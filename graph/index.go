package graph

import (
	"errors"
	"fmt"
	"time"

	cmodel "github.com/open-falcon/common/model"
	spool "github.com/toolkits/pool/simple_conn_pool"

	"github.com/open-falcon/query/g"
)

func DeleteIndex(para *cmodel.GraphCounter) (*cmodel.SimpleRpcResponse, error) {
	endpoint, counter := para.Endpoint, para.Counter
	if len(endpoint) < 1 || len(counter) < 1 {
		return nil, fmt.Errorf("empty")
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
		Resp *cmodel.SimpleRpcResponse
	}
	ch := make(chan *ChResult, 1)
	go func() {
		resp := &cmodel.SimpleRpcResponse{}
		err := rpcConn.Call("Graph.DeleteIndex", para, resp)
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
			return r.Resp, nil
		}
	}
}
