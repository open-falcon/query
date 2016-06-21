package http

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	cmodel "github.com/Cepave/common/model"
	"github.com/Cepave/query/g"
	"github.com/Cepave/query/graph"
	"github.com/Cepave/query/proc"
	"github.com/astaxie/beego/orm"
	"github.com/bitly/go-simplejson"
)

type Tag struct {
	StrategyId int
	Name       string
	Value      string
	CreateAt   string
	UpdateAt   string
}

/**
 * @function name:   func postByJson(rw http.ResponseWriter, req *http.Request, url string)
 * @description:     This function sends a POST request in JSON format.
 * @related issues:  OWL-171
 * @param:           rw http.ResponseWriter
 * @param:           req *http.Request
 * @param:           url string
 * @return:          void
 * @author:          Don Hsieh
 * @since:           11/12/2015
 * @last modified:   11/13/2015
 * @called by:       func queryInfo(rw http.ResponseWriter, req *http.Request)
 *                   func queryHistory(rw http.ResponseWriter, req *http.Request)
 */
func postByJSON(rw http.ResponseWriter, req *http.Request, url string) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(req.Body)
	s := buf.String()
	reqPost, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(s)))
	if err != nil {
		log.Println("Error =", err.Error())
	}
	reqPost.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(reqPost)
	if err != nil {
		log.Println("Error =", err.Error())
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	rw.Header().Set("Content-Type", "application/json; charset=UTF-8")
	rw.Write(body)
}

/**
 * @function name:   func queryInfo(rw http.ResponseWriter, req *http.Request)
 * @description:     This function handles /graph/info API request.
 * @related issues:  OWL-171
 * @param:           rw http.ResponseWriter
 * @param:           req *http.Request
 * @return:          void
 * @author:          Don Hsieh
 * @since:           11/12/2015
 * @last modified:   11/13/2015
 * @called by:       func configApiRoutes()
 */
func queryInfo(rw http.ResponseWriter, req *http.Request) {
	url := g.Config().Api.Query + "/graph/info"
	postByJSON(rw, req, url)
}

/**
 * @function name:   func queryHistory(rw http.ResponseWriter, req *http.Request)
 * @description:     This function handles /graph/history API request.
 * @related issues:  OWL-171
 * @param:           rw http.ResponseWriter
 * @param:           req *http.Request
 * @return:          void
 * @author:          Don Hsieh
 * @since:           11/12/2015
 * @last modified:   11/13/2015
 * @called by:       func configApiRoutes()
 */
func queryHistory(rw http.ResponseWriter, req *http.Request) {
	url := g.Config().Api.Query + "/graph/history"
	postByJSON(rw, req, url)
}

/**
 * @function name:   func getRequest(rw http.ResponseWriter, url string)
 * @description:     This function sends GET request to given URL.
 * @related issues:  OWL-159
 * @param:           rw http.ResponseWriter
 * @param:           url string
 * @return:          void
 * @author:          Don Hsieh
 * @since:           11/24/2015
 * @last modified:   11/24/2015
 * @called by:       func dashboardEndpoints(rw http.ResponseWriter, req *http.Request)
 *                    in query/http/api.go
 * @called by:       func dashboardEndpoints(rw http.ResponseWriter, req *http.Request)
 *                    in query/http/api.go
 */
func getRequest(rw http.ResponseWriter, url string) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Println("Error =", err.Error())
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Error =", err.Error())
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	rw.Header().Set("Content-Type", "application/json; charset=UTF-8")
	rw.Write(body)
}

/**
 * @function name:   func dashboardEndpoints(rw http.ResponseWriter, req *http.Request)
 * @description:     This function handles /api/endpoints API request.
 * @related issues:  OWL-159, OWL-171
 * @param:           rw http.ResponseWriter
 * @param:           req *http.Request
 * @return:          void
 * @author:          Don Hsieh
 * @since:           11/12/2015
 * @last modified:   11/24/2015
 * @called by:       func configApiRoutes()
 */
func dashboardEndpoints(rw http.ResponseWriter, req *http.Request) {
	url := g.Config().Api.Dashboard + req.URL.RequestURI()
	getRequest(rw, url)
}

/**
 * @function name:   func postByForm(rw http.ResponseWriter, req *http.Request, url string)
 * @description:     This function sends a POST request in Form format.
 * @related issues:  OWL-171
 * @param:           rw http.ResponseWriter
 * @param:           req *http.Request
 * @param:           url string
 * @return:          void
 * @author:          Don Hsieh
 * @since:           11/12/2015
 * @last modified:   11/13/2015
 * @called by:       func dashboardCounters(rw http.ResponseWriter, req *http.Request)
 *                   func dashboardChart(rw http.ResponseWriter, req *http.Request)
 */
func postByForm(rw http.ResponseWriter, req *http.Request, url string) {
	req.ParseForm()
	client := &http.Client{}
	resp, err := client.PostForm(url, req.PostForm)
	if err != nil {
		log.Println("Error =", err.Error())
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	rw.Header().Set("Content-Type", "application/json; charset=UTF-8")
	rw.Write(body)
}

/**
 * @function name:   func dashboardCounters(rw http.ResponseWriter, req *http.Request)
 * @description:     This function handles /api/counters API request.
 * @related issues:  OWL-171
 * @param:           rw http.ResponseWriter
 * @param:           req *http.Request
 * @return:          void
 * @author:          Don Hsieh
 * @since:           11/13/2015
 * @last modified:   11/13/2015
 * @called by:       func configApiRoutes()
 */
func dashboardCounters(rw http.ResponseWriter, req *http.Request) {
	url := g.Config().Api.Dashboard + "/api/counters"
	postByForm(rw, req, url)
}

/**
 * @function name:   func dashboardChart(rw http.ResponseWriter, req *http.Request)
 * @description:     This function handles /api/chart API request.
 * @related issues:  OWL-171
 * @param:           rw http.ResponseWriter
 * @param:           req *http.Request
 * @return:          void
 * @author:          Don Hsieh
 * @since:           11/13/2015
 * @last modified:   11/13/2015
 * @called by:       func configApiRoutes()
 */
func dashboardChart(rw http.ResponseWriter, req *http.Request) {
	url := g.Config().Api.Dashboard + "/chart"
	postByForm(rw, req, url)
}

func getAgentAliveData(hostnames []string, versions map[string]string, result map[string]interface{}) []cmodel.GraphLastResp {
	var queries []cmodel.GraphLastParam
	o := orm.NewOrm()
	var hosts []*Host
	_, err := o.Raw("SELECT hostname, agent_version FROM falcon_portal.host ORDER BY hostname ASC").QueryRows(&hosts)
	if err != nil {
		setError(err.Error(), result)
	} else {
		for _, host := range hosts {
			var query cmodel.GraphLastParam
			if !strings.Contains(host.Hostname, ".") && strings.Contains(host.Hostname, "-") {
				hostnames = append(hostnames, host.Hostname)
				versions[host.Hostname] = host.Agent_version
				query.Endpoint = host.Hostname
				query.Counter = "agent.alive"
				queries = append(queries, query)
			}
		}
	}
	s, err := json.Marshal(queries)
	if err != nil {
		setError(err.Error(), result)
	}
	url := g.Config().Api.Query + "/graph/last"
	reqPost, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(s)))
	if err != nil {
		setError(err.Error(), result)
	}
	reqPost.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(reqPost)
	if err != nil {
		setError(err.Error(), result)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	data := []cmodel.GraphLastResp{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		setError(err.Error(), result)
	}
	return data
}

func processAgentAliveData(data []cmodel.GraphLastResp, hostnames []string, versions map[string]string, result map[string]interface{}) {
	name := ""
	version := ""
	status := ""
	alive := 0
	countOfNormal := 0
	countOfWarn := 0
	countOfDead := 0
	anomalies := []interface{}{}
	items := []interface{}{}
	for key, row := range data {
		name = row.Endpoint
		var diff int64
		diff = 0
		var timestamp int64
		timestamp = 0
		status = "dead"
		alive = 0
		if name == "" {
			name = hostnames[key]
		} else {
			alive = int(row.Value.Value)
			timestamp = row.Value.Timestamp
			now := time.Now().Unix()
			diff = now - timestamp
		}
		version = versions[name]
		if alive > 0 {
			if diff > 3600 {
				status = "warm"
				countOfWarn++
			} else {
				status = "normal"
				countOfNormal++
			}
		} else {
			countOfDead++
		}
		item := map[string]interface{}{}
		item["id"] = strconv.Itoa(key + 1)
		item["hostname"] = name
		item["agent_version"] = version
		item["alive"] = alive
		item["timestamp"] = timestamp
		item["diff"] = diff
		item["status"] = status
		items = append(items, item)
		if diff > 60*60*24 && timestamp > 0 {
			anomalies = append(anomalies, item)
		}
	}
	var count = make(map[string]interface{})
	count["all"] = len(data)
	count["normal"] = countOfNormal
	count["warn"] = countOfWarn
	count["dead"] = countOfDead
	result["count"] = count
	result["items"] = items
}

func getAlive(rw http.ResponseWriter, req *http.Request) {
	var nodes = make(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors

	data := []cmodel.GraphLastResp{}
	hostnames := []string{}
	var versions = make(map[string]string)
	data = getAgentAliveData(hostnames, versions, result)
	processAgentAliveData(data, hostnames, versions, result)
	nodes["result"] = result
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	setResponse(rw, nodes)
}

func setStrategyTags(rw http.ResponseWriter, req *http.Request) {
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	buf := new(bytes.Buffer)
	buf.ReadFrom(req.Body)
	json, err := simplejson.NewJson(buf.Bytes())
	if err != nil {
		setError(err.Error(), result)
	}

	var nodes = make(map[string]interface{})
	nodes, _ = json.Map()
	strategyID := ""
	tagName := ""
	tagValue := ""
	if value, ok := nodes["strategyID"]; ok {
		strategyID = value.(string)
		delete(nodes, "strategyID")
	}
	if value, ok := nodes["tagName"]; ok {
		tagName = value.(string)
		delete(nodes, "tagName")
	}
	if value, ok := nodes["tagValue"]; ok {
		tagValue = value.(string)
		delete(nodes, "tagValue")
	}

	if len(strategyID) > 0 && len(tagName) > 0 && len(tagValue) > 0 {
		strategyIDint, err := strconv.Atoi(strategyID)
		if err != nil {
			setError(err.Error(), result)
		} else {
			o := orm.NewOrm()
			var tag Tag
			sqlcmd := "SELECT * FROM falcon_portal.tags WHERE strategy_id=?"
			err = o.Raw(sqlcmd, strategyIDint).QueryRow(&tag)
			if err == orm.ErrNoRows {
				log.Println("tag not found")
				sql := "INSERT INTO tags(strategy_id, name, value, create_at) VALUES(?, ?, ?, ?)"
				res, err := o.Raw(sql, strategyIDint, tagName, tagValue, getNow()).Exec()
				if err != nil {
					setError(err.Error(), result)
				} else {
					num, _ := res.RowsAffected()
					log.Println("mysql row affected nums =", num)
					result["strategyID"] = strategyID
					result["action"] = "create"
				}
			} else if err != nil {
				setError(err.Error(), result)
			} else {
				log.Println("tag existed =", tag)
				sql := "UPDATE tags SET name = ?, value = ? WHERE strategy_id = ?"
				res, err := o.Raw(sql, tagName, tagValue, strategyIDint).Exec()
				if err != nil {
					setError(err.Error(), result)
				} else {
					num, _ := res.RowsAffected()
					log.Println("mysql row affected nums =", num)
					result["strategyID"] = strategyID
					result["action"] = "update"
				}
			}
		}
	} else {
		setError("Input value errors.", result)
	}
	nodes["result"] = result
	setResponse(rw, nodes)
}

func getTemplateStrategies(rw http.ResponseWriter, req *http.Request) {
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	items := []interface{}{}
	countOfStrategies := 0
	arguments := strings.Split(req.URL.Path, "/")
	if arguments[len(arguments)-1] == "strategies" {
		templateID, err := strconv.Atoi(arguments[len(arguments)-2])
		if err != nil {
			setError(err.Error(), result)
		}
		o := orm.NewOrm()
		var strategyIDs []int64
		num, err := o.Raw("SELECT id FROM falcon_portal.strategy WHERE tpl_id = ? ORDER BY id ASC", templateID).QueryRows(&strategyIDs)
		if err != nil {
			setError(err.Error(), result)
		} else if num > 0 {
			countOfStrategies = int(num)
			var strategies = make(map[string]interface{})
			sids := ""
			for key, strategyID := range strategyIDs {
				sid := strconv.Itoa(int(strategyID))
				item := map[string]string{}
				item["templateID"] = strconv.Itoa(templateID)
				item["strategyID"] = sid
				strategies[sid] = item
				if key == 0 {
					sids = sid
				} else {
					sids += ", " + sid
				}
			}
			sqlcmd := "SELECT strategy_id, name, value FROM falcon_portal.tags WHERE strategy_id IN ("
			sqlcmd += sids
			sqlcmd += ") ORDER BY strategy_id ASC"
			var tags []*Tag
			_, err = o.Raw(sqlcmd).QueryRows(&tags)
			if err != nil {
				setError(err.Error(), result)
			} else {
				for _, tag := range tags {
					strategyID := strconv.Itoa(int(tag.StrategyId))
					strategy := strategies[strategyID].(map[string]string)
					strategy["tagName"] = tag.Name
					strategy["tagValue"] = tag.Value
					strategies[strategyID] = strategy
				}
			}
			for _, strategy := range strategies {
				items = append(items, strategy)
			}
		}
	}
	result["items"] = items
	result["count"] = countOfStrategies
	var nodes = make(map[string]interface{})
	nodes["result"] = result
	setResponse(rw, nodes)
}

func getPlatformJSON(nodes map[string]interface{}, result map[string]interface{}) {
	fcname := g.Config().Api.Name
	fctoken := getFctoken()
	url := g.Config().Api.Map + "/fcname/" + fcname + "/fctoken/" + fctoken
	url += "/show_active/yes/hostname/yes/pop_id/yes.json"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		setError(err.Error(), result)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		setError(err.Error(), result)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &nodes); err != nil {
		setError(err.Error(), result)
	}
}

func setGraphQueries(hostnames []string, hostnamesExisted []string, versions map[string]string, result map[string]interface{}) []*cmodel.GraphLastParam {
	var queries []*cmodel.GraphLastParam
	o := orm.NewOrm()
	var hosts []*Host
	hostnamesStr := strings.Join(hostnames, "','")
	sqlcommand := "SELECT hostname, agent_version FROM falcon_portal.host WHERE hostname IN ('"
	sqlcommand += hostnamesStr + "') ORDER BY hostname ASC"
	_, err := o.Raw(sqlcommand).QueryRows(&hosts)
	if err != nil {
		setError(err.Error(), result)
	} else {
		for _, host := range hosts {
			var query cmodel.GraphLastParam
			if !strings.Contains(host.Hostname, ".") && strings.Contains(host.Hostname, "-") {
				hostnamesExisted = append(hostnamesExisted, host.Hostname)
				versions[host.Hostname] = host.Agent_version
				query.Endpoint = host.Hostname
				query.Counter = "agent.alive"
				queries = append(queries, &query)
			}
		}
	}
	return queries
}

func queryAgentAlive(queries []*cmodel.GraphLastParam, reqHost string, result map[string]interface{}) []cmodel.GraphLastResp {
	data := []cmodel.GraphLastResp{}
	if len(queries) > 0 {
		if strings.Index(g.Config().Api.Query, reqHost) >= 0 {
			proc.LastRequestCnt.Incr()
			for _, param := range queries {
				if param == nil {
					continue
				}
				last, err := graph.Last(*param)
				if err != nil {
					log.Printf("graph.last fail, resp: %v, err: %v", last, err)
				}
				if last == nil {
					continue
				}
				data = append(data, *last)
			}
			proc.LastRequestItemCnt.IncrBy(int64(len(data)))
		} else {
			s, err := json.Marshal(queries)
			if err != nil {
				setError(err.Error(), result)
			}
			url := g.Config().Api.Query + "/graph/last"
			reqPost, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(s)))
			if err != nil {
				setError(err.Error(), result)
			}
			reqPost.Header.Set("Content-Type", "application/json")

			client := &http.Client{}
			resp, err := client.Do(reqPost)
			if err != nil {
				setError(err.Error(), result)
			}
			defer resp.Body.Close()

			body, _ := ioutil.ReadAll(resp.Body)

			err = json.Unmarshal(body, &data)
			if err != nil {
				setError(err.Error(), result)
			}
		}
	}
	return data
}

func classifyAgentAliveResponse(data []cmodel.GraphLastResp, hostnamesExisted []string, versions map[string]string, result map[string]interface{}) {
	name := ""
	version := ""
	status := ""
	alive := 0
	var diff int64
	var timestamp int64
	items := map[string]interface{}{}
	for key, row := range data {
		name = row.Endpoint
		alive = 0
		diff = 0
		timestamp = 0
		status = "error"
		if name == "" {
			name = hostnamesExisted[key]
		} else {
			alive = int(row.Value.Value)
			timestamp = row.Value.Timestamp
			now := time.Now().Unix()
			diff = now - timestamp
		}
		version = versions[name]
		if alive > 0 {
			if diff > 3600 {
				status = "warm"
			} else {
				status = "normal"
			}
		}
		item := map[string]interface{}{}
		item["version"] = version
		item["status"] = status
		items[name] = item
	}
	result["items"] = items
}

func getAnomalies(errorHosts []interface{}, result map[string]interface{}) map[string]interface{} {
	anomalies := map[string]interface{}{}
	pop_ids := map[string]string{}
	for _, errorHost := range errorHosts {
		pop_id := errorHost.(map[string]string)["pop_id"]
		pop_ids[pop_id] = pop_id
	}
	arr := []string{}
	for _, pop_id := range pop_ids {
		arr = append(arr, pop_id)
	}
	sort.Strings(arr)

	sqlcmd := "SELECT pop_id, name, province, city FROM grafana.idc WHERE pop_id IN ('"
	sqlcmd += strings.Join(arr, "','") + "')"
	idcs := map[string]interface{}{}
	var rows []orm.Params
	o := orm.NewOrm()
	o.Using("grafana")
	_, err := o.Raw(sqlcmd).Values(&rows)
	if err != nil {
		setError(err.Error(), result)
	} else {
		for _, row := range rows {
			idc := map[string]string{
				"idc":      row["name"].(string),
				"province": row["province"].(string),
				"city":     row["city"].(string),
			}
			idcs[row["pop_id"].(string)] = idc
		}
	}

	anomalies2 := map[string]interface{}{}
	for _, errorHost := range errorHosts {
		pop_id := errorHost.(map[string]string)["pop_id"]
		idc := idcs[pop_id]
		errorHost.(map[string]string)["idc"] = idc.(map[string]string)["idc"]
		errorHost.(map[string]string)["city"] = idc.(map[string]string)["city"]

		provinceName := idc.(map[string]string)["province"]
		if provinceName == "特区" {
			provinceName = idc.(map[string]string)["city"]
		}
		errorHost.(map[string]string)["province"] = provinceName
		delete(errorHost.(map[string]string), "pop_id")
		delete(errorHost.(map[string]string), "id")

		if province, ok := anomalies2[provinceName]; ok {
			province = append(province.([]map[string]string), errorHost.(map[string]string))
			anomalies2[provinceName] = province
		} else {
			anomalies2[provinceName] = []map[string]string{
				errorHost.(map[string]string),
			}
		}
	}

	for provinceName, hosts := range anomalies2 {
		count := len(hosts.([]map[string]string))
		anomalies[provinceName] = map[string]interface{}{
			"count": count,
			"hosts": hosts,
		}
	}
	return anomalies
}

func completeAgentAliveData(groups map[string]interface{}, groupNames []string, result map[string]interface{}) {
	errorHosts := []interface{}{}
	platforms := []interface{}{}
	count := map[string]int{}
	countOfNormalSum := 0
	countOfWarnSum := 0
	countOfErrorSum := 0
	countOfMissSum := 0
	countOfDeactivatedSum := 0
	hostId := 1
	name := ""
	activate := ""
	version := ""
	pop_id := ""
	status := ""
	items := result["items"].(map[string]interface{})
	for _, groupName := range groupNames {
		platform := map[string]interface{}{}
		hosts := []interface{}{}
		count := map[string]int{}
		countOfNormal := 0
		countOfWarn := 0
		countOfError := 0
		countOfMiss := 0
		countOfDeactivated := 0
		group := groups[groupName].([]interface{})
		for _, agent := range group {
			name = agent.(map[string]interface{})["name"].(string)
			activate = agent.(map[string]interface{})["activate"].(string)
			pop_id = agent.(map[string]interface{})["pop_id"].(string)
			status = ""
			version = ""
			if activate == "1" {
				if item, ok := items[name]; ok {
					status = item.(map[string]interface{})["status"].(string)
					version = item.(map[string]interface{})["version"].(string)
				} else {
					status = "miss"
					countOfMiss++
				}
			} else {
				status = "deactivated"
				countOfDeactivated++
			}
			if status == "normal" {
				countOfNormal++
			} else if status == "warm" {
				countOfWarn++
			} else if status == "error" {
				countOfError++
			}
			host := map[string]string{
				"id":       strconv.Itoa(hostId),
				"name":     name,
				"platform": groupName,
				"pop_id":   pop_id,
				"status":   status,
				"version":  version,
			}
			if host["status"] == "error" {
				errorHosts = append(errorHosts, host)
			} else {
				delete(host, "pop_id")
			}
			hosts = append(hosts, host)
			hostId++
		}
		count["normal"] = countOfNormal
		count["warn"] = countOfWarn
		count["error"] = countOfError
		count["miss"] = countOfMiss
		count["deactivated"] = countOfDeactivated
		count["all"] = countOfNormal + countOfWarn + countOfError + countOfMiss + countOfDeactivated
		platform["platformName"] = groupName
		platform["platformCount"] = count
		platform["hosts"] = hosts
		platforms = append(platforms, platform)
		countOfNormalSum += countOfNormal
		countOfWarnSum += countOfWarn
		countOfErrorSum += countOfError
		countOfMissSum += countOfMiss
		countOfDeactivatedSum += countOfDeactivated
	}
	count["normal"] = countOfNormalSum
	count["warn"] = countOfWarnSum
	count["error"] = countOfErrorSum
	count["miss"] = countOfMissSum
	count["deactivated"] = countOfDeactivatedSum
	count["all"] = countOfNormalSum + countOfWarnSum + countOfErrorSum + countOfMissSum + countOfDeactivatedSum
	result["count"] = count
	result["anomalies"] = getAnomalies(errorHosts, result)
	result["items"] = platforms
}

func getPlatforms(rw http.ResponseWriter, req *http.Request) {
	var nodes = make(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	getPlatformJSON(nodes, result)
	groups := map[string]interface{}{}
	groupNames := []string{}
	hostnames := []string{}
	hostnamesMap := map[string]int{}
	if nodes["status"] != nil && int(nodes["status"].(float64)) == 1 {
		hostname := ""
		for _, platform := range nodes["result"].([]interface{}) {
			groupName := platform.(map[string]interface{})["platform"].(string)
			groupNames = append(groupNames, groupName)
			group := []interface{}{}
			for _, device := range platform.(map[string]interface{})["ip_list"].([]interface{}) {
				hostname = device.(map[string]interface{})["hostname"].(string)
				if _, ok := hostnamesMap[hostname]; !ok {
					hostnames = append(hostnames, hostname)
					host := map[string]interface{}{
						"name":     hostname,
						"activate": device.(map[string]interface{})["ip_status"].(string),
						"pop_id":   device.(map[string]interface{})["pop_id"].(string),
					}
					group = append(group, host)
					hostnamesMap[hostname] = 1
				}
			}
			groups[groupName] = group
		}
		sort.Strings(hostnames)
		sort.Strings(groupNames)

		hostnamesExisted := []string{}
		var versions = make(map[string]string)
		queries := setGraphQueries(hostnames, hostnamesExisted, versions, result)
		data := queryAgentAlive(queries, req.Host, result)
		classifyAgentAliveResponse(data, hostnamesExisted, versions, result)
		completeAgentAliveData(groups, groupNames, result)
	}
	if _, ok := nodes["info"]; ok {
		delete(nodes, "info")
	}
	if _, ok := nodes["status"]; ok {
		delete(nodes, "status")
	}
	nodes["result"] = result
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	setResponse(rw, nodes)
}

func getMetricsByMetricType(metricType string) []string {
	metrics := []string{}
	if metricType == "bandwidths" {
		metrics = []string{
			"net.if.in.bits/iface=eth_all",
			"net.if.out.bits/iface=eth_all",
		}
	} else if metricType == "cpu" {
		metrics = []string{
			"cpu.idle",
			"cpu.system",
			"cpu.softirq",
			"cpu.user",
		}
	} else if metricType == "resources" {
		metrics = []string{
			"cpu.idle",
			"disk.io.util.max",
			"load.1min",
			"mem.memfree.percent",
			"mem.swapused.percent",
			"vmstat.procs.b",
			"vmstat.procs.r",
		}
	} else if metricType == "services" {
		metrics = []string{
			"http.response.time",
			"https.response.time",
			"ss.close.wait",
			"ss.established",
			"ss.syn.recv",
			"vfcc.squid.response.time",
		}
	} else if metricType == "all" || metricType == "aggregate" {
		metrics = []string{
			"net.if.in.bits/iface=eth_all",
			"net.if.out.bits/iface=eth_all",
			"cpu.idle",
			"disk.io.util.max",
			"load.1min",
			"mem.memfree.percent",
			"mem.swapused.percent",
			"http.response.time",
			"https.response.time",
			"ss.close.wait",
			"ss.established",
			"ss.syn.recv",
			"vfcc.squid.response.time",
			"vmstat.procs.b",
			"vmstat.procs.r",
		}
	}
	return metrics
}

func convertDurationToPoint(duration string, result map[string]interface{}) (timestampFrom int64, timestampTo int64) {
	if strings.Index(duration, ",") > -1 {
		var e error
		timestampFrom, e = strconv.ParseInt(strings.Split(duration, ",")[0], 10, 64)
		if e != nil {
			setError(e.Error(), result)
		}
		timestampTo, e = strconv.ParseInt(strings.Split(duration, ",")[1], 10, 64)
		if e != nil {
			setError(e.Error(), result)
		}
		if timestampFrom >= timestampTo {
			setError("Value of timestampFrom should be less than value of timestampTo.", result)
		}
		if timestampTo > time.Now().Unix() {
			setError("Value of timestampTo should be equal to or less than value of now.", result)
		}
	} else if strings.Index(duration, "d") > -1 || strings.Index(duration, "min") > -1 {
		unit := ""
		seconds := int64(0)
		if strings.Index(duration, "d") > -1 {
			unit = "d"
			seconds = int64(86400)
		} else {
			unit = "min"
			seconds = int64(60)
		}
		multiplier, err := strconv.Atoi(strings.Split(duration, unit)[0])
		if err != nil {
			setError(err.Error(), result)
		}
		offset := int64(multiplier) * seconds
		now := time.Now().Unix()
		timestampFrom = now - offset
		timestampTo = now
	}
	return
}

func getGraphQueryResponse(metrics []string, duration string, hostnames []string, result map[string]interface{}) []*cmodel.GraphQueryResponse {
	data := []*cmodel.GraphQueryResponse{}
	start, end := convertDurationToPoint(duration, result)

	proc.HistoryRequestCnt.Incr()
	for _, hostname := range hostnames {
		for _, metric := range metrics {
			request := cmodel.GraphQueryParam{
				Start:     start,
				End:       end,
				ConsolFun: "AVERAGE",
				Endpoint:  hostname,
				Counter:   metric,
			}
			response, err := graph.QueryOne(request)
			if err != nil {
				setError("graph.queryOne fail, "+err.Error(), result)
			}
			if result == nil {
				continue
			}
			data = append(data, response)
		}
	}

	proc.HistoryResponseCounterCnt.IncrBy(int64(len(data)))
	for _, item := range data {
		proc.HistoryResponseItemCnt.IncrBy(int64(len(item.Values)))
	}
	return data
}

func getHostMetricValues(rw http.ResponseWriter, req *http.Request) {
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	items := []interface{}{}
	arguments := strings.Split(req.URL.Path, "/")
	hostname := ""
	metricType := ""
	duration := ""
	if len(arguments) == 6 {
		hostname = arguments[len(arguments)-3]
		metricType = arguments[len(arguments)-2]
		duration = arguments[len(arguments)-1]
	} else if len(arguments) == 5 {
		hostname = arguments[len(arguments)-2]
		metricType = arguments[len(arguments)-1]
		duration = "3d"
	}
	metrics := getMetricsByMetricType(metricType)
	if len(metrics) > 0 && strings.Index(duration, "d") > -1 {
		data := getGraphQueryResponse(metrics, duration, []string{hostname}, result)

		for _, series := range data {
			values := []interface{}{}
			for _, rrdObj := range series.Values {
				value := []interface{}{
					rrdObj.Timestamp * 1000,
					rrdObj.Value,
				}
				values = append(values, value)
			}
			item := map[string]interface{}{
				"host":   series.Endpoint,
				"metric": series.Counter,
				"data":   values,
			}
			items = append(items, item)
		}
	}
	result["items"] = items
	var nodes = make(map[string]interface{})
	nodes["result"] = result
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	setResponse(rw, nodes)
}

func getApolloFiltersJSON(nodes map[string]interface{}, result map[string]interface{}) {
	fcname := g.Config().Api.Name
	fctoken := getFctoken()
	url := g.Config().Api.Map + "/fcname/" + fcname + "/fctoken/" + fctoken
	url += "/show_active/yes/hostname/yes/pop_id/yes.json"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		setError(err.Error(), result)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		setError(err.Error(), result)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &nodes); err != nil {
		setError(err.Error(), result)
	}
}

func getExistedHostnames(hostnames []string, result map[string]interface{}) []string {
	hostnamesExisted := []string{}
	o := orm.NewOrm()
	var hosts []*Host
	hostnamesStr := strings.Join(hostnames, "','")
	sqlcommand := "SELECT hostname, agent_version FROM falcon_portal.host WHERE hostname IN ('"
	sqlcommand += hostnamesStr + "') ORDER BY hostname ASC"
	_, err := o.Raw(sqlcommand).QueryRows(&hosts)
	if err != nil {
		setError(err.Error(), result)
	} else {
		for _, host := range hosts {
			if !strings.Contains(host.Hostname, ".") && strings.Contains(host.Hostname, "-") {
				hostnamesExisted = append(hostnamesExisted, host.Hostname)
			}
		}
	}
	return hostnamesExisted
}

func appendUniqueString(slice []string, s string) []string {
	sliceStr := strings.Join(slice, "','")
	if !strings.Contains(sliceStr, s) {
		slice = append(slice, s)
		sort.Strings(slice)
	}
	return slice
}

func appendUnique(slice []int, num int) []int {
	existed := false
	for _, element := range slice {
		if element == num {
			existed = true
		}
	}
	if !existed {
		slice = append(slice, num)
		sort.Ints(slice)
	}
	return slice
}

func getExistedHosts(hosts []interface{}, hostnamesExisted []string, result map[string]interface{}) map[string]interface{} {
	hostsExisted := map[string]interface{}{}
	for key, hostname := range hostnamesExisted {
		host := map[string]interface{}{
			"id":   key + 1,
			"name": hostname,
		}
		hostsExisted[hostname] = host
	}
	idcMap := map[string]string{}
	o := orm.NewOrm()
	var idcs []*Idc
	sqlcommand := "SELECT pop_id, name FROM grafana.idc ORDER BY pop_id ASC"
	_, err := o.Raw(sqlcommand).QueryRows(&idcs)
	if err != nil {
		setError(err.Error(), result)
	} else {
		for _, idc := range idcs {
			idcMap[strconv.Itoa(idc.Pop_id)] = idc.Name
		}
	}
	for _, host := range hosts {
		hostname := host.(map[string]interface{})["name"].(string)
		if _, ok := hostsExisted[hostname]; ok {
			hostExisted := hostsExisted[hostname]
			isp := strings.Split(hostname, "-")[0]
			province := strings.Split(hostname, "-")[1]
			popID := host.(map[string]interface{})["popID"].(string)
			idc := idcMap[popID]
			platform := host.(map[string]interface{})["platform"].(string)
			if _, ok := hostExisted.(map[string]interface{})["platform"]; ok {
				hostExisted.(map[string]interface{})["platform"] = appendUniqueString(hostExisted.(map[string]interface{})["platform"].([]string), platform)
			} else {
				hostExisted.(map[string]interface{})["platform"] = []string{platform}
			}
			hostExisted.(map[string]interface{})["isp"] = isp
			hostExisted.(map[string]interface{})["province"] = province
			hostExisted.(map[string]interface{})["idc"] = idc
			hostsExisted[hostname] = hostExisted
		}
	}
	return hostsExisted
}

func completeApolloFiltersData(hostsExisted map[string]interface{}, result map[string]interface{}) {
	hosts := map[string]interface{}{}
	keywords := map[string]interface{}{}
	for _, host := range hostsExisted {
		id := host.(map[string]interface{})["id"].(int)
		platform := host.(map[string]interface{})["platform"].([]string)
		for _, s := range platform {
			if _, ok := keywords[s]; ok {
				keywords[s] = appendUnique(keywords[s].([]int), id)
			} else {
				keywords[s] = []int{id}
			}
		}

		isp := host.(map[string]interface{})["isp"].(string)
		if _, ok := keywords[isp]; ok {
			keywords[isp] = appendUnique(keywords[isp].([]int), id)
		} else {
			keywords[isp] = []int{id}
		}

		province := host.(map[string]interface{})["province"].(string)
		if _, ok := keywords[province]; ok {
			keywords[province] = appendUnique(keywords[province].([]int), id)
		} else {
			keywords[province] = []int{id}
		}

		name := host.(map[string]interface{})["name"].(string)
		fragments := strings.Split(name, "-")
		if len(fragments) == 6 {
			fragments := fragments[2:]
			for _, fragment := range fragments {
				if _, ok := keywords[fragment]; ok {
					keywords[fragment] = appendUnique(keywords[fragment].([]int), id)
				} else {
					keywords[fragment] = []int{id}
				}
			}
		}

		idc := host.(map[string]interface{})["idc"].(string)
		if _, ok := keywords[idc]; ok {
			keywords[idc] = appendUnique(keywords[idc].([]int), id)
		} else {
			keywords[idc] = []int{id}
		}

		delete(host.(map[string]interface{}), "id")
		delete(host.(map[string]interface{}), "isp")
		delete(host.(map[string]interface{}), "province")
		hosts[strconv.Itoa(id)] = host
	}
	result["hosts"] = hosts
	result["keywords"] = keywords
}

func getApolloFilters(rw http.ResponseWriter, req *http.Request) {
	var nodes = make(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	count := 0
	getApolloFiltersJSON(nodes, result)
	hosts := []interface{}{}
	hostnames := []string{}
	if nodes["status"] != nil && int(nodes["status"].(float64)) == 1 {
		hostname := ""
		for _, platform := range nodes["result"].([]interface{}) {
			groupName := platform.(map[string]interface{})["platform"].(string)
			for _, device := range platform.(map[string]interface{})["ip_list"].([]interface{}) {
				hostname = device.(map[string]interface{})["hostname"].(string)
				popID := device.(map[string]interface{})["pop_id"].(string)
				if device.(map[string]interface{})["ip_status"].(string) == "1" {
					hostnames = append(hostnames, hostname)
					host := map[string]interface{}{
						"name":     hostname,
						"platform": groupName,
						"popID":    popID,
					}
					hosts = append(hosts, host)
					hostnames = append(hostnames, hostname)
				}
			}
		}
		sort.Strings(hostnames)
		hostnamesExisted := getExistedHostnames(hostnames, result)
		sort.Strings(hostnamesExisted)
		hostsExisted := getExistedHosts(hosts, hostnamesExisted, result)
		count = len(hostsExisted)
		completeApolloFiltersData(hostsExisted, result)
	}
	if _, ok := nodes["info"]; ok {
		delete(nodes, "info")
	}
	if _, ok := nodes["status"]; ok {
		delete(nodes, "status")
	}
	nodes["count"] = count
	nodes["result"] = result
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	setResponse(rw, nodes)
}

func getApolloCharts(rw http.ResponseWriter, req *http.Request) {
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	items := []interface{}{}
	arguments := strings.Split(req.URL.Path, "/")
	metricType := arguments[4]
	hostnames := strings.Split(arguments[5], ",")
	metrics := getMetricsByMetricType(metricType)
	duration := "1d"
	if len(arguments) > 6 {
		duration = arguments[6]
	}
	data := getGraphQueryResponse(metrics, duration, hostnames, result)
	for _, series := range data {
		values := []interface{}{}
		for _, rrdObj := range series.Values {
			value := []interface{}{
				rrdObj.Timestamp * 1000,
				rrdObj.Value,
			}
			values = append(values, value)
		}
		item := map[string]interface{}{
			"host":   series.Endpoint,
			"metric": series.Counter,
			"data":   values,
		}
		items = append(items, item)
	}
	result["items"] = items
	var nodes = make(map[string]interface{})
	nodes["result"] = result
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	setResponse(rw, nodes)
}

func getIPFromHostname(hostname string, result map[string]interface{}) string {
	ip := ""
	fragments := strings.Split(hostname, "-")
	slice := []string{}
	if len(fragments) == 6 {
		fragments := fragments[2:]
		for _, fragment := range fragments {
			num, err := strconv.Atoi(fragment)
			if err != nil {
				setError(err.Error(), result)
			} else {
				slice = append(slice, strconv.Itoa(num))
			}
		}
		if len(slice) == 4 {
			ip = strings.Join(slice, ".")
		}
	}
	return ip
}

func getBandwidthsFiveMinutesAverage(metricType string, duration string, hostnames []string, result map[string]interface{}) []interface{} {
	items := []interface{}{}
	sort.Strings(hostnames)
	metrics := getMetricsByMetricType(metricType)
	hostMap := map[string]interface{}{}
	if len(metrics) > 0 && len(hostnames) > 0 {
		data := getGraphQueryResponse(metrics, duration, hostnames, result)
		for _, series := range data {
			values := []interface{}{}
			for _, rrdObj := range series.Values {
				if !math.IsNaN(float64(rrdObj.Value)) {
					value := []interface{}{
						float64(rrdObj.Timestamp),
						float64(rrdObj.Value),
					}
					values = append(values, value)
				}
			}
			average := float64(0)
			sum := float64(0)
			divider := float64(0)
			timestamp := float64(0)
			for _, value := range values {
				timestamp = value.([]interface{})[0].(float64)
				sum += value.([]interface{})[1].(float64)
				divider++
			}
			if divider > 0 {
				average = sum / divider
			}
			item := map[string]interface{}{
				"host": series.Endpoint,
				"ip":   getIPFromHostname(series.Endpoint, result),
				"net.in.bits.5min.avg":  0,
				"net.out.bits.5min.avg": 0,
				"time":                  "",
			}
			if value, ok := hostMap[series.Endpoint]; ok {
				item = value.(map[string]interface{})
			}
			if series.Counter == "net.if.in.bits/iface=eth_all" {
				item["net.in.bits.5min.avg"] = int(average)
			} else if series.Counter == "net.if.out.bits/iface=eth_all" {
				item["net.out.bits.5min.avg"] = int(average)
			}
			if item["time"] == "" && average > 0 {
				item["time"] = time.Unix(int64(timestamp), 0).Format("2006-01-02 15:04:05")
			}
			hostMap[series.Endpoint] = item
		}
	}
	for _, hostname := range hostnames {
		host := hostMap[hostname]
		if host.(map[string]interface{})["host"].(string) != "" {
			items = append(items, host)
		}
	}
	return items
}

func getPlatformBandwidthsFiveMinutesAverage(platformName string, metricType string, rw http.ResponseWriter) map[string]interface{} {
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	duration := "6min"
	var nodes = make(map[string]interface{})
	getApolloFiltersJSON(nodes, result)
	hostnames := []string{}
	if nodes["status"] != nil && int(nodes["status"].(float64)) == 1 {
		hostname := ""
		for _, platform := range nodes["result"].([]interface{}) {
			groupName := platform.(map[string]interface{})["platform"].(string)
			if groupName == platformName {
				for _, device := range platform.(map[string]interface{})["ip_list"].([]interface{}) {
					hostname = device.(map[string]interface{})["hostname"].(string)
					if device.(map[string]interface{})["ip_status"].(string) == "1" {
						hostnames = append(hostnames, hostname)
					}
				}
			}
		}
	}
	items := getBandwidthsFiveMinutesAverage(metricType, duration, hostnames, result)
	if _, ok := nodes["info"]; ok {
		delete(nodes, "info")
	}
	if _, ok := nodes["status"]; ok {
		delete(nodes, "status")
	}
	result["items"] = items
	nodes["result"] = result
	nodes["count"] = len(items)
	nodes["platform"] = platformName
	return nodes
}

func getPlatformContact(platformName string, rw http.ResponseWriter, nodes map[string]interface{}) {
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	var platformMap = make(map[string]interface{})
	fcname := g.Config().Api.Name
	fctoken := getFctoken()
	url := g.Config().Api.Contact
	params := map[string]string{
		"fcname":       fcname,
		"fctoken":      fctoken,
		"platform_key": platformName,
	}
	s, err := json.Marshal(params)
	if err != nil {
		setError(err.Error(), result)
	}
	reqPost, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(s)))
	if err != nil {
		setError(err.Error(), result)
	}
	reqPost.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(reqPost)
	if err != nil {
		setError(err.Error(), result)
	} else {
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		err = json.Unmarshal(body, &nodes)
		if err != nil {
			setError(err.Error(), result)
		} else if nodes["status"] != nil && int(nodes["status"].(float64)) == 1 {
			for _, name := range strings.Split(platformName, ",") {
				if platform, ok := nodes["result"].(map[string]interface{})[name].([]interface{}); ok {
					items := []interface{}{}
					for _, contact := range platform {
						item := map[string]interface{}{
							"name":  contact.(map[string]interface{})["realname"].(string),
							"phone": contact.(map[string]interface{})["cell"].(string),
							"email": contact.(map[string]interface{})["email"].(string),
						}
						items = append(items, item)
					}
					platformMap[name] = items
				} else {
					platformMap[name] = "BOSS 沒有平台負責人資訊"
				}
			}
		}
	}
	if _, ok := nodes["info"]; ok {
		delete(nodes, "info")
	}
	if _, ok := nodes["status"]; ok {
		delete(nodes, "status")
	}
	result["items"] = platformMap
	nodes["result"] = result
	nodes["count"] = len(platformMap)
	nodes["platform"] = platformName
}

func parsePlatformArguments(rw http.ResponseWriter, req *http.Request) {
	var nodes = make(map[string]interface{})
	arguments := strings.Split(req.URL.Path, "/")
	if len(arguments) == 6 && arguments[len(arguments)-2] == "bandwidths" && arguments[len(arguments)-1] == "average" {
		platformName := arguments[len(arguments)-3]
		metricType := arguments[len(arguments)-2]
		nodes = getPlatformBandwidthsFiveMinutesAverage(platformName, metricType, rw)
	} else if len(arguments) == 5 && arguments[len(arguments)-1] == "contact" {
		platformName := arguments[len(arguments)-2]
		getPlatformContact(platformName, rw, nodes)
	}
	setResponse(rw, nodes)
}

func getHostsBandwidthsFiveMinutesAverage(rw http.ResponseWriter, req *http.Request) {
	var nodes = make(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	arguments := strings.Split(req.URL.Path, "/")
	hostnames := []string{}
	metricType := ""
	duration := "6min"
	if len(arguments) == 5 && arguments[len(arguments)-1] == "bandwidths" {
		hostnames = strings.Split(arguments[len(arguments)-2], ",")
		metricType = arguments[len(arguments)-1]
	}
	items := getBandwidthsFiveMinutesAverage(metricType, duration, hostnames, result)
	if _, ok := nodes["info"]; ok {
		delete(nodes, "info")
	}
	if _, ok := nodes["status"]; ok {
		delete(nodes, "status")
	}
	result["items"] = items
	nodes["result"] = result
	nodes["count"] = len(items)
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	setResponse(rw, nodes)
}

func getIDCsHosts(rw http.ResponseWriter, req *http.Request) {
	var nodes = make(map[string]interface{})
	idcsMap := map[string]interface{}{}
	idcIDs := []string{}
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	getPlatformJSON(nodes, result)
	hosts := map[string]interface{}{}
	hostnames := []string{}
	hostnamesMap := map[string]int{}
	if nodes["status"] != nil && int(nodes["status"].(float64)) == 1 {
		hostname := ""
		for _, platform := range nodes["result"].([]interface{}) {
			for _, device := range platform.(map[string]interface{})["ip_list"].([]interface{}) {
				hostname = device.(map[string]interface{})["hostname"].(string)
				if _, ok := hostnamesMap[hostname]; !ok {
					ip := device.(map[string]interface{})["ip"].(string)
					if ip == getIPFromHostname(hostname, result) {
						hostnames = append(hostnames, hostname)
						idcID := device.(map[string]interface{})["pop_id"].(string)
						host := map[string]interface{}{
							"activate": device.(map[string]interface{})["ip_status"].(string),
							"hostname": hostname,
							"idcID":    idcID,
							"ip":       ip,
						}
						hostnamesMap[hostname] = 1
						hosts[hostname] = host
						idcIDs = appendUniqueString(idcIDs, idcID)
					}
				}
			}
		}
		sort.Strings(hostnames)
		sort.Strings(idcIDs)
		for _, hostname := range hostnames {
			host := hosts[hostname].(map[string]interface{})
			idcID := host["idcID"].(string)
			if _, ok := idcsMap[idcID]; ok {
				idcsMap[idcID] = append(idcsMap[idcID].([]map[string]interface{}), host)
			} else {
				idcsMap[idcID] = []map[string]interface{}{
					host,
				}
			}
		}
		idcNamesMap := map[string]string{}
		idcNames := []string{}
		o := orm.NewOrm()
		var idcs []*Idc
		sqlcommand := "SELECT pop_id, name FROM grafana.idc ORDER BY pop_id ASC"
		_, err := o.Raw(sqlcommand).QueryRows(&idcs)
		if err != nil {
			setError(err.Error(), result)
		} else {
			for _, idc := range idcs {
				idcNamesMap[idc.Name] = strconv.Itoa(idc.Pop_id)
				idcNames = appendUniqueString(idcNames, idc.Name)
			}
		}
		sort.Strings(idcNames)
		for _, idcName := range idcNames {
			idcID := idcNamesMap[idcName]
			if _, ok := idcsMap[idcID]; ok {
				idc := idcsMap[idcID]
				idcsMap[idcName] = idc
				delete(idcsMap, idcID)
			}
		}
	}
	if _, ok := nodes["info"]; ok {
		delete(nodes, "info")
	}
	if _, ok := nodes["status"]; ok {
		delete(nodes, "status")
	}
	result["items"] = idcsMap
	nodes["result"] = result
	nodes["count"] = len(idcIDs)
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	setResponse(rw, nodes)
}

func getIDCsBandwidthsUpperLimit(rw http.ResponseWriter, req *http.Request) {
	var nodes = make(map[string]interface{})
	errors := []string{}
	var result = make(map[string]interface{})
	result["error"] = errors
	arguments := strings.Split(req.URL.Path, "/")
	idcName := ""
	upperLimitSum := float64(0)
	if len(arguments) == 6 && arguments[len(arguments)-2] == "bandwidths" && arguments[len(arguments)-1] == "limit" {
		idcName = arguments[len(arguments)-3]
	}
	fcname := g.Config().Api.Name
	fctoken := getFctoken()
	url := g.Config().Api.Uplink
	params := map[string]string{
		"fcname":   fcname,
		"fctoken":  fctoken,
		"pop_name": idcName,
	}
	s, err := json.Marshal(params)
	if err != nil {
		setError(err.Error(), result)
	}
	reqPost, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(s)))
	if err != nil {
		setError(err.Error(), result)
	}
	reqPost.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(reqPost)
	if err != nil {
		setError(err.Error(), result)
	} else {
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		err = json.Unmarshal(body, &nodes)
		if err != nil {
			setError(err.Error(), result)
		}

		if nodes["status"] != nil && int(nodes["status"].(float64)) == 1 {
			for _, uplink := range nodes["result"].([]interface{}) {
				upperLimit := uplink.(map[string]interface{})["all_uplink_top"].(float64)
				upperLimitSum += upperLimit
			}
		}
	}
	items := map[string]interface{}{
		"idcName":      idcName,
		"upperLimitMB": upperLimitSum,
	}

	if _, ok := nodes["info"]; ok {
		delete(nodes, "info")
	}
	if _, ok := nodes["status"]; ok {
		delete(nodes, "status")
	}
	result["items"] = items
	nodes["result"] = result
	setResponse(rw, nodes)
}

func configAPIRoutes() {
	http.HandleFunc("/api/info", queryInfo)
	http.HandleFunc("/api/history", queryHistory)
	http.HandleFunc("/api/endpoints", dashboardEndpoints)
	http.HandleFunc("/api/counters", dashboardCounters)
	http.HandleFunc("/api/chart", dashboardChart)
	http.HandleFunc("/api/alive", getAlive)
	http.HandleFunc("/api/tags/update", setStrategyTags)
	http.HandleFunc("/api/templates/", getTemplateStrategies)
	http.HandleFunc("/api/alive/platforms", getPlatforms)
	http.HandleFunc("/api/metrics.health/", getHostMetricValues)
	http.HandleFunc("/api/apollo/filters", getApolloFilters)
	http.HandleFunc("/api/apollo/charts/", getApolloCharts)
	http.HandleFunc("/api/platforms/", parsePlatformArguments)
	http.HandleFunc("/api/hosts/", getHostsBandwidthsFiveMinutesAverage)
	http.HandleFunc("/api/idcs/hosts", getIDCsHosts)
	http.HandleFunc("/api/idcs/", getIDCsBandwidthsUpperLimit)
}
