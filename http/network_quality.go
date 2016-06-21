package http

import (
	"github.com/astaxie/beego/orm"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Nqm_node struct {
	Id         int
	Nid        string
	Pid        string
	Pname      string
	Pname_abbr string
	Cid        string
	Cname      string
	Cname_abbr string
	Iid        int
	Iname      string
	Status     int
	Note       string
	Addtime    int
}

func getNQMNodes(rw http.ResponseWriter, req *http.Request) {
	var nodes = make(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	items := []interface{}{}
	o := orm.NewOrm()
	o.Using("gz_nqm")
	var NQMNodes []*Nqm_node
	_, err := o.Raw("SELECT nid, pname, cname, status, note FROM gz_nqm.nqm_node ORDER BY nid ASC").QueryRows(&NQMNodes)
	if err != nil {
		setError(err.Error(), result)
	} else {
		for _, node := range NQMNodes {
			idc := map[string]string{
				"idc":      node.Nid,
				"status":   strconv.Itoa(node.Status),
				"province": node.Pname,
				"city":     node.Cname,
				"note":     node.Note,
			}
			items = append(items, idc)
		}
	}
	result["items"] = items
	nodes["result"] = result
	nodes["count"] = len(items)
	setResponse(rw, nodes)
}

func getLatestTimestamp(tableName string, result map[string]interface{}) string {
	timestamp := ""
	o := orm.NewOrm()
	o.Using("gz_nqm")
	sqlcmd := "SELECT mtime FROM gz_nqm." + tableName + " ORDER BY mtime DESC LIMIT 1"
	var rows []orm.Params
	num, err := o.Raw(sqlcmd).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		timestamp = rows[0]["mtime"].(string)
	}
	return timestamp
}

func getSum(slice []float64) float64 {
	sum := float64(0)
	for _, number := range slice {
		sum += number
	}
	return sum
}

func getPacketLossAndAveragePingTime(tableName string, timestamp string, result map[string]interface{}) map[string]interface{} {
	idc := map[string]interface{}{}
	sends := []float64{}
	receives := []float64{}
	averages := []float64{}

	o := orm.NewOrm()
	o.Using("gz_nqm")
	sqlcmd := "SELECT send, receive, avg FROM gz_nqm." + tableName + " WHERE mtime = ?"
	var rows []orm.Params
	num, err := o.Raw(sqlcmd, timestamp).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		for _, row := range rows {
			send, err := strconv.ParseFloat(row["send"].(string), 64)
			if err != nil {
				setError(err.Error(), result)
			} else {
				sends = append(sends, send)
			}
			receive, err := strconv.ParseFloat(row["receive"].(string), 64)
			if err != nil {
				setError(err.Error(), result)
			} else {
				receives = append(receives, receive)
			}
			avg, err := strconv.ParseFloat(row["avg"].(string), 64)
			if err != nil {
				setError(err.Error(), result)
			} else {
				averages = append(averages, avg)
			}
		}
	}
	divider := float64(len(sends))
	packetLossRate := 1 - (getSum(receives) / getSum(sends))
	averagePingTimeMilliseconds := getSum(averages) / divider

	timestampInt, err := strconv.Atoi(timestamp)
	if err != nil {
		setError(err.Error(), result)
	} else {
		idc = map[string]interface{}{
			"packetLossRate":              packetLossRate,
			"averagePingTimeMilliseconds": averagePingTimeMilliseconds,
			"time": time.Unix(int64(timestampInt), 0).Format("2006-01-02 15:04"),
		}
	}
	return idc
}

func getNQMPacketLoss(rw http.ResponseWriter, req *http.Request) {
	var nodes = make(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	items := []interface{}{}

	tablesMap := map[string]interface{}{}
	o := orm.NewOrm()
	o.Using("gz_nqm")
	sqlcmd := "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'nqm_log_prober_%'"
	var rows []orm.Params
	num, err := o.Raw(sqlcmd).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else if num > 0 {
		for _, row := range rows {
			tablesMap[row["table_name"].(string)] = ""
		}
	}

	nids := []string{}
	pidMap := map[string]interface{}{}
	var NQMNodes []*Nqm_node
	_, err = o.Raw("SELECT nid, pid, status FROM gz_nqm.nqm_node ORDER BY nid ASC").QueryRows(&NQMNodes)
	if err != nil {
		setError(err.Error(), result)
	} else {
		for _, node := range NQMNodes {
			if node.Status > 0 {
				nids = append(nids, node.Nid)
				pidMap[node.Nid] = node.Pid
			}
		}
	}
	idcNames := []string{}
	for _, nid := range nids {
		if _, ok := tablesMap["nqm_log_prober_"+strings.Replace(nid, "-", "_", -1)]; ok {
			idcNames = append(idcNames, nid)
		}
	}
	sort.Strings(idcNames)
	for _, idcName := range idcNames {
		tableName := "nqm_log_prober_" + strings.Replace(idcName, "-", "_", -1)
		timestamp := getLatestTimestamp(tableName, result)
		if timestamp != "" {
			idc := getPacketLossAndAveragePingTime(tableName, timestamp, result)
			idc["nodeName"] = idcName
			idc["pid"] = pidMap[idcName]
			items = append(items, idc)
		}
	}
	result["items"] = items
	nodes["result"] = result
	nodes["count"] = len(items)
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	setResponse(rw, nodes)
}

func configNQMRoutes() {
	http.HandleFunc("/api/nqm/nodes", getNQMNodes)
	http.HandleFunc("/api/nqm/loss", getNQMPacketLoss)
}
