package graph

import (
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"time"

	cmodel "github.com/open-falcon/common/model"
	cutils "github.com/open-falcon/common/utils"
	"github.com/samuel/go-zookeeper/zk"
	rings "github.com/toolkits/consistent/rings"
	nset "github.com/toolkits/container/set"
	spool "github.com/toolkits/pool/simple_conn_pool"
	"github.com/jdjr/drrs/golang/sdk"

	"github.com/open-falcon/query/g"
)

// 连接池
// node_address -> connection_pool
var (
	GraphConnPools *spool.SafeRpcConnPools
)

// 服务节点的一致性哈希环
// pk -> node
var (
	GraphNodeRing *rings.ConsistentHashNodeRing
	DrrsNodeRing  *rings.ConsistentHashNodeRing //drrs
)

var drrs_master_list []string //drrs

func Start() {
	initNodeRings()
	initConnPools()
	log.Println("graph.Start ok")
}

//监听zk中的master节点发生变化
func watchZNode(ch <-chan zk.Event) {
	cfg := g.Config()
	drrsConfig := cfg.Drrs
	for {
		e := <-ch
		if e.Type == zk.EventNodeChildrenChanged {
			var master_list []string
			c, _, err := zk.Connect([]string{drrsConfig.Zk.Ip}, time.Second*time.Duration(drrsConfig.Zk.Timeout))
			if err != nil {
				drrs_master_list = nil
				DrrsNodeRing = nil
				log.Fatalln("[DRRS FATALL] watchZNode: ZK connection error: ", err)
			}
			children, stat, zkChannel, err := c.ChildrenW(drrsConfig.Zk.Addr)
			if err != nil {
				drrs_master_list = nil
				DrrsNodeRing = nil
				log.Fatalln("[DRRS FATALL] watchZNode: ZK get children error: ", err)
			}
			nzk := stat.NumChildren
			if nzk <= 0 {
				drrs_master_list = nil
				DrrsNodeRing = nil
				log.Fatalln("[DRRS FATALL] watchZNode: ZK contents error: ", zk.ErrNoChildrenForEphemerals)
			}
			for i := range children {
				absAddr := fmt.Sprintf("%s/%s", drrsConfig.Zk.Addr, children[i])
				data_get, _, err := c.Get(absAddr)
				if err != nil {
					drrs_master_list = nil
					DrrsNodeRing = nil
					log.Fatalln("[DRRS FATALL] watchZNode: ZK get data error: ", err)
				}
				data := string(data_get)
				if data == "" {
					drrs_master_list = nil
					DrrsNodeRing = nil
					log.Fatalln("[DRRS FATALL] watchZNode: ZK data error: ", zk.ErrInvalidPath)
				}
				master_list = append(master_list, data)
			}
			drrs_master_list = master_list
			DrrsNodeRing = rings.NewConsistentHashNodesRing(cfg.Drrs.Replicas, drrs_master_list)
			go watchZNode(zkChannel)
			break
		}
	}
}

func initDrrsMasterList(drrsConfig *g.DrrsConfig) error { //drrs
	if !drrsConfig.Enabled {
		drrs_master_list = nil
		return nil
	}
	if !drrsConfig.UseZk {
		drrs_master_list = append(drrs_master_list, drrsConfig.Dest)
		return nil
	}

	c, _, err := zk.Connect([]string{drrsConfig.Zk.Ip}, time.Second*time.Duration(drrsConfig.Zk.Timeout))
	if err != nil {
		drrs_master_list = nil
		return err
	}
	children, stat, zkChannel, err := c.ChildrenW(drrsConfig.Zk.Addr)
	if err != nil {
		drrs_master_list = nil
		return err
	}
	go watchZNode(zkChannel)

	nzk := stat.NumChildren
	if nzk <= 0 {
		drrs_master_list = nil
		return zk.ErrNoChildrenForEphemerals
	}
	for i := range children {
		absAddr := fmt.Sprintf("%s/%s", drrsConfig.Zk.Addr, children[i])
		data_get, _, err := c.Get(absAddr)
		if err != nil {
			return err
		}
		data := string(data_get)
		if data == "" {
			return zk.ErrInvalidPath
		}
		drrs_master_list = append(drrs_master_list, data)
	}
	return nil
}

func QueryOne(para cmodel.GraphQueryParam) (resp *cmodel.GraphQueryResponse, err error) {

	cfg := g.Config()

	start, end := para.Start, para.End
	endpoint, counter := para.Endpoint, para.Counter

	if cfg.Drrs.Enabled { //drrs
		md5 := Md5(endpoint + "/" + counter)
		filename := RrdFileName(md5)
		drrs_master, err := DrrsNodeRing.GetNode(filename)
		res := cmodel.GraphQueryResponse{Endpoint: endpoint, Counter: counter}
		datas, err := Fetch(filename, para.ConsolFun, start, end, drrs_master)
		if err != nil {
			//fetch出错，重试两次，看是不是master挂了，尝试ck分配新的master。
			ok := false
			for i := 0; i < 2; i++ {
				time.Sleep(time.Second * 10)
				drrs_master, err = DrrsNodeRing.GetNode(filename)
				if err != nil {
					log.Println("[DRRS ERROR] DRRS GET NODE RING ERROR:", err)
					break
				}
				datas, err = Fetch(filename, para.ConsolFun, start, end, drrs_master)
				if err == nil {
					ok = true
					break
				}
			}
			if !ok {
				log.Println("[DRRS ERROR] RRD FETCH ERROR: ", err)
				return nil, err
			}
		}
		res.Values = datas
		return &res, nil
	}

	pool, addr, err := selectPool(endpoint, counter)
	if err != nil {
		return nil, err
	}

	conn, err := pool.Fetch()
	if err != nil {
		return nil, err
	}

	rpcConn := conn.(spool.RpcClient)
	if rpcConn.Closed() {
		pool.ForceClose(conn)
		return nil, errors.New("conn closed")
	}

	type ChResult struct {
		Err  error
		Resp *cmodel.GraphQueryResponse
	}

	ch := make(chan *ChResult, 1)
	go func() {
		resp := &cmodel.GraphQueryResponse{}
		err := rpcConn.Call("Graph.Query", para, resp)
		ch <- &ChResult{Err: err, Resp: resp}
	}()

	select {
	case <-time.After(time.Duration(g.Config().Graph.CallTimeout) * time.Millisecond):
		pool.ForceClose(conn)
		return nil, fmt.Errorf("%s, call timeout. proc: %s", addr, pool.Proc())
	case r := <-ch:
		if r.Err != nil {
			pool.ForceClose(conn)
			return r.Resp, fmt.Errorf("%s, call failed, err %v. proc: %s", addr, r.Err, pool.Proc())
		} else {
			pool.Release(conn)

			if len(r.Resp.Values) < 1 {
				return r.Resp, nil
			}

			// TODO query不该做这些事情, 说明graph没做好
			fixed := []*cmodel.RRDData{}
			for _, v := range r.Resp.Values {
				if v == nil || !(v.Timestamp >= start && v.Timestamp <= end) {
					continue
				}
				//FIXME: 查询数据的时候，把所有的负值都过滤掉，因为transfer之前在设置最小值的时候为U
				if (r.Resp.DsType == "DERIVE" || r.Resp.DsType == "COUNTER") && v.Value < 0 {
					fixed = append(fixed, &cmodel.RRDData{Timestamp: v.Timestamp, Value: cmodel.JsonFloat(math.NaN())})
				} else {
					fixed = append(fixed, v)
				}
			}
			r.Resp.Values = fixed
		}
		return r.Resp, nil
	}
}

func Info(para cmodel.GraphInfoParam) (resp *cmodel.GraphFullyInfo, err error) {
	endpoint, counter := para.Endpoint, para.Counter

	pool, addr, err := selectPool(endpoint, counter)
	if err != nil {
		return nil, err
	}

	conn, err := pool.Fetch()
	if err != nil {
		return nil, err
	}

	rpcConn := conn.(spool.RpcClient)
	if rpcConn.Closed() {
		pool.ForceClose(conn)
		return nil, errors.New("conn closed")
	}

	type ChResult struct {
		Err  error
		Resp *cmodel.GraphInfoResp
	}
	ch := make(chan *ChResult, 1)
	go func() {
		resp := &cmodel.GraphInfoResp{}
		err := rpcConn.Call("Graph.Info", para, resp)
		ch <- &ChResult{Err: err, Resp: resp}
	}()

	select {
	case <-time.After(time.Duration(g.Config().Graph.CallTimeout) * time.Millisecond):
		pool.ForceClose(conn)
		return nil, fmt.Errorf("%s, call timeout. proc: %s", addr, pool.Proc())
	case r := <-ch:
		if r.Err != nil {
			pool.ForceClose(conn)
			return nil, fmt.Errorf("%s, call failed, err %v. proc: %s", addr, r.Err, pool.Proc())
		} else {
			pool.Release(conn)
			fullyInfo := cmodel.GraphFullyInfo{
				Endpoint:  endpoint,
				Counter:   counter,
				ConsolFun: r.Resp.ConsolFun,
				Step:      r.Resp.Step,
				Filename:  r.Resp.Filename,
				Addr:      addr,
			}
			return &fullyInfo, nil
		}
	}
}

func Last(para cmodel.GraphLastParam) (r *cmodel.GraphLastResp, err error) {
	endpoint, counter := para.Endpoint, para.Counter

	pool, addr, err := selectPool(endpoint, counter)
	if err != nil {
		return nil, err
	}

	conn, err := pool.Fetch()
	if err != nil {
		return nil, err
	}

	rpcConn := conn.(spool.RpcClient)
	if rpcConn.Closed() {
		pool.ForceClose(conn)
		return nil, errors.New("conn closed")
	}

	type ChResult struct {
		Err  error
		Resp *cmodel.GraphLastResp
	}
	ch := make(chan *ChResult, 1)
	go func() {
		resp := &cmodel.GraphLastResp{}
		err := rpcConn.Call("Graph.Last", para, resp)
		ch <- &ChResult{Err: err, Resp: resp}
	}()

	select {
	case <-time.After(time.Duration(g.Config().Graph.CallTimeout) * time.Millisecond):
		pool.ForceClose(conn)
		return nil, fmt.Errorf("%s, call timeout. proc: %s", addr, pool.Proc())
	case r := <-ch:
		if r.Err != nil {
			pool.ForceClose(conn)
			return r.Resp, fmt.Errorf("%s, call failed, err %v. proc: %s", addr, r.Err, pool.Proc())
		} else {
			pool.Release(conn)
			return r.Resp, nil
		}
	}
}

func LastRaw(para cmodel.GraphLastParam) (r *cmodel.GraphLastResp, err error) {
	endpoint, counter := para.Endpoint, para.Counter

	pool, addr, err := selectPool(endpoint, counter)
	if err != nil {
		return nil, err
	}

	conn, err := pool.Fetch()
	if err != nil {
		return nil, err
	}

	rpcConn := conn.(spool.RpcClient)
	if rpcConn.Closed() {
		pool.ForceClose(conn)
		return nil, errors.New("conn closed")
	}

	type ChResult struct {
		Err  error
		Resp *cmodel.GraphLastResp
	}
	ch := make(chan *ChResult, 1)
	go func() {
		resp := &cmodel.GraphLastResp{}
		err := rpcConn.Call("Graph.LastRaw", para, resp)
		ch <- &ChResult{Err: err, Resp: resp}
	}()

	select {
	case <-time.After(time.Duration(g.Config().Graph.CallTimeout) * time.Millisecond):
		pool.ForceClose(conn)
		return nil, fmt.Errorf("%s, call timeout. proc: %s", addr, pool.Proc())
	case r := <-ch:
		if r.Err != nil {
			pool.ForceClose(conn)
			return r.Resp, fmt.Errorf("%s, call failed, err %v. proc: %s", addr, r.Err, pool.Proc())
		} else {
			pool.Release(conn)
			return r.Resp, nil
		}
	}
}

func selectPool(endpoint, counter string) (rpool *spool.ConnPool, raddr string, rerr error) {
	pkey := cutils.PK2(endpoint, counter)
	node, err := GraphNodeRing.GetNode(pkey)
	if err != nil {
		return nil, "", err
	}

	addr, found := g.Config().Graph.Cluster[node]
	if !found {
		return nil, "", errors.New("node not found")
	}

	pool, found := GraphConnPools.Get(addr)
	if !found {
		return nil, addr, errors.New("addr not found")
	}

	return pool, addr, nil
}

// internal functions
func initConnPools() {
	cfg := g.Config()

	// TODO 为了得到Slice,这里做的太复杂了
	graphInstances := nset.NewSafeSet()
	for _, address := range cfg.Graph.Cluster {
		graphInstances.Add(address)
	}
	GraphConnPools = spool.CreateSafeRpcConnPools(cfg.Graph.MaxConns, cfg.Graph.MaxIdle,
		cfg.Graph.ConnTimeout, cfg.Graph.CallTimeout, graphInstances.ToSlice())

	if cfg.Drrs.Enabled { //drrs
		if drrs_master_list != nil {
			var addrs []*net.TCPAddr
			for _, addr := range drrs_master_list {
				//初始化drrs
				tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
				if err != nil {
					log.Fatalln("[DRRS FATALL] config file:", cfg, "is not correct, cannot resolve drrs master tcp address. err:", err)
				}
				addrs = append(addrs, tcpAddr)
			}
			err := sdk.DRRSInit(addrs)
			if err != nil {
				log.Fatalln("[DRRS FATALL] StartSendTasks: DRRS init error: ", err)
				return
			}
		}
	}
}

func initNodeRings() {
	cfg := g.Config()

	err := initDrrsMasterList(cfg.Drrs) //drrs
	if err != nil {                     //drrs
		log.Fatalln("[DRRS FATALL] init drrs zookeeper list acorrding to config file:", cfg, "fail:", err)
	}

	GraphNodeRing = rings.NewConsistentHashNodesRing(cfg.Graph.Replicas, cutils.KeysOfMap(cfg.Graph.Cluster))
	if cfg.Drrs.Enabled { //drrs
		if drrs_master_list != nil {
			DrrsNodeRing = rings.NewConsistentHashNodesRing(cfg.Drrs.Replicas, drrs_master_list)
		} else {
			DrrsNodeRing = nil
		}
	} else {
		DrrsNodeRing = nil
	}
}
