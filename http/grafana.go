package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Cepave/query/g"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func doHTTPQuery(url string) map[string]interface{} {
	reqGet, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Println("Error =", err.Error())
	}
	client := &http.Client{}
	resp, err := client.Do(reqGet)
	if err != nil {
		log.Println("Error =", err.Error())
	}
	defer resp.Body.Close()
	var nodes = make(map[string]interface{})
	if resp.Status == "200 OK" {
		body, _ := ioutil.ReadAll(resp.Body)
		if err := json.Unmarshal(body, &nodes); err != nil {
			log.Println(err.Error())
		}
	}
	return nodes
}
func getHosts(reqHost string, hostKeyword string) []interface{} {
	if len(hostKeyword) == 0 {
		hostKeyword = ".+"
	}
	rand.Seed(time.Now().UTC().UnixNano())
	random64 := rand.Float64()
	_r := strconv.FormatFloat(random64, 'f', -1, 32)
	maxQuery := g.Config().Api.Max
	url := fmt.Sprintf("/api/endpoints?q=%s&tags&limit=%d&_r=%s&regex_query=1", hostKeyword, maxQuery, _r)
	if strings.Index(g.Config().Api.Query, reqHost) >= 0 {
		url = "http://localhost:9966" + url
	} else {
		url = g.Config().Api.Query + url
	}
	nodes := doHTTPQuery(url)
	result := []interface{}{}
	chart := map[string]interface{}{
		"text":       "chart",
		"expandable": true,
	}
	result = append(result, chart)

	for _, host := range nodes["data"].([]interface{}) {
		item := map[string]interface{}{
			"text":       host,
			"expandable": true,
		}
		result = append(result, item)
	}
	return result
}

func getNextCounterSegment(metric string, counter string) string {
	segment := ""
	if len(metric) > 0 {
		metric += "."
	}
	if counter+"." == metric {
		//when the counter metric are the same, will retrun "$" as the ending chartacter of query
		segment = "$"
	} else {
		log.Println("metric = ", metric, "counter = ", counter)
		counter = strings.Replace(counter, metric, "", 1)
		segment = strings.Split(counter, ".")[0]
	}
	return segment
}

func checkSegmentExpandable(segment string, counter string) bool {
	if segment == "$" {
		return false
	}
	segments := strings.Split(counter, ".")
	expandable := !(segment == segments[len(segments)-1])
	return expandable
}

func doHTTPPost(target string, endpoints string, metric string, maxQuery string, _r string) map[string]interface{} {
	form := url.Values{}
	form.Set("endpoints", endpoints)
	form.Add("q", metric)
	form.Add("limit", maxQuery)
	form.Add("_r", _r)

	reqPost, err := http.NewRequest("POST", target, strings.NewReader(form.Encode()))
	if err != nil {
		log.Println("Error =", err.Error())
	}
	reqPost.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(reqPost)
	if err != nil {
		log.Println("Error =", err.Error())
	}
	defer resp.Body.Close()
	var nodes = make(map[string]interface{})
	if resp.Status == "200 OK" {
		body, _ := ioutil.ReadAll(resp.Body)
		if err := json.Unmarshal(body, &nodes); err != nil {
			log.Println(err.Error())
		}
	}
	return nodes
}

func getMetrics(reqHost string, query string) []interface{} {
	result := []interface{}{}
	regx, _ := regexp.Compile("(#?\\.\\*$|\\.\\$)")
	query = regx.ReplaceAllString(query, "")
	arrQuery := strings.Split(query, "#")
	host, arrMetric := arrQuery[0], arrQuery[1:]
	maxQuery := strconv.Itoa(g.Config().Api.Max)
	if host == "chart" {
		chartBar := map[string]interface{}{
			"text":       "bar",
			"expandable": false,
		}
		result = append(result, chartBar)

		chartMap := map[string]interface{}{
			"text":       "map",
			"expandable": false,
		}
		result = append(result, chartMap)

		chartRose := map[string]interface{}{
			"text":       "rose",
			"expandable": false,
		}
		result = append(result, chartRose)
	} else if len(arrMetric) > 0 && arrMetric[len(arrMetric)-1] == "%" {
		result = append(result, map[string]interface{}{
			"text":       "%",
			"expandable": false,
		})
	} else {
		metric := strings.Join(arrMetric, ".")
		reg, _ := regexp.Compile("(^{|}$)")
		host = reg.ReplaceAllString(host, "")
		host = strings.Replace(host, ",", "\",\"", -1)
		endpoints := "[\"" + host + "\"]"
		rand.Seed(time.Now().UTC().UnixNano())
		random64 := rand.Float64()
		_r := strconv.FormatFloat(random64, 'f', -1, 32)
		target := "/api/counters"
		if strings.Index(g.Config().Api.Query, reqHost) >= 0 {
			target = "http://localhost:9966" + target
		} else {
			target = g.Config().Api.Query + target
		}
		log.Println("target =", target)
		nodes := doHTTPPost(target, endpoints, metric, maxQuery, _r)
		var segmentPool = make(map[string]bool)
		for _, data := range nodes["data"].([]interface{}) {
			counter := data.([]interface{})[0].(string)
			segment := getNextCounterSegment(metric, counter)
			expandable := checkSegmentExpandable(segment, counter)
			if _, ok := segmentPool[segment]; !ok {
				segmentPool[segment] = expandable
			} else if segmentPool[segment] == false {
				//for solve issue of mertice has 2 different type of expandable
				//ex. ["used"] and ["used.percent"]
				segmentPool[segment] = expandable
			}
		}
		expandCounter := 0
		for key, value := range segmentPool {
			if value == false {
				expandCounter += 1
			}
			item := map[string]interface{}{
				"text":       key,
				"expandable": value,
			}
			result = append(result, item)
		}
		//add wildcard support
		if expandCounter >= 2 {
			result = append(result, map[string]interface{}{
				"text":       "%",
				"expandable": false,
			})
		}
	}
	return result
}

func setQueryEditor(rw http.ResponseWriter, req *http.Request) {
	query := req.URL.Query().Get("query")
	replacer := strings.NewReplacer(".%", "",
		".undefined", "",
		".select metric", "")
	query = replacer.Replace(query)
	if !strings.Contains(query, "#") {
		RenderJson(rw, getHosts(req.Host, query))
	} else {
		RenderJson(rw, getMetrics(req.Host, query))
	}
}

func getMetricValues(req *http.Request, host string, metrics []string, result []interface{}) []interface{} {
	endpointCounters := []interface{}{}
	if strings.Contains(host, "{") {
		host = strings.Replace(host, "{", "", -1)
		host = strings.Replace(host, "}", "", -1)
		hosts := strings.Split(host, ",")
		for _, host := range hosts {
			for _, metric := range metrics {
				item := map[string]string{
					"endpoint": host,
					"counter":  metric,
				}
				endpointCounters = append(endpointCounters, item)
			}
		}
	} else {
		for _, metric := range metrics {
			item := map[string]string{
				"endpoint": host,
				"counter":  metric,
			}
			endpointCounters = append(endpointCounters, item)
		}
	}

	if len(endpointCounters) > 0 {
		from, err := strconv.ParseInt(req.PostForm["from"][0], 10, 64)
		until, err := strconv.ParseInt(req.PostForm["until"][0], 10, 64)
		var step int64 = 60
		postmap := req.Form
		if postmap.Get("step") != "" {
			step, _ = strconv.ParseInt(req.PostForm["step"][0], 10, 64)
		}
		cf := "AVERAGE"
		if postmap.Get("cf") != "" {
			cf = req.PostForm["cf"][0]
		}
		url := "/graph/history"
		if strings.Index(g.Config().Api.Query, req.Host) >= 0 {
			url = "http://localhost:9966" + url
		} else {
			url = g.Config().Api.Query + url
		}

		args := map[string]interface{}{
			"start":             from,
			"end":               until,
			"cf":                cf,
			"step":              step,
			"endpoint_counters": endpointCounters,
		}
		bs, err := json.Marshal(args)
		if err != nil {
			log.Println("Error =", err.Error())
		}

		reqPost, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(bs)))
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

		if resp.Status == "200 OK" {
			body, _ := ioutil.ReadAll(resp.Body)
			nodes := []interface{}{}
			if err := json.Unmarshal(body, &nodes); err != nil {
				log.Println(err.Error())
			}

			for _, node := range nodes {
				if _, ok := node.(map[string]interface{})["Values"]; ok {
					result = append(result, node)
				}
			}
		}
	}
	return result
}

func getValues(rw http.ResponseWriter, req *http.Request) {
	result := []interface{}{}
	req.ParseForm()
	for _, target := range req.PostForm["target"] {
		if !strings.Contains(target, ".select metric") {
			targets := strings.Split(target, "#")
			host, targets := targets[0], targets[1:]
			var metrics = make([]string, 0)
			if host == "chart" {
				chartType := targets[len(targets)-1]
				chartValues := getChartOptions(chartType)
				result = append(result, chartValues)
			} else if targets[len(targets)-1] == "%" {
				regx, _ := regexp.Compile("#%$")
				query := regx.ReplaceAllString(target, "")
				counter_tmp := strings.Split(query, "#")[1:]
				counter_perfix := strings.Join(counter_tmp, ".") + "."
				for _, item := range getMetrics(req.Host, query) {
					i := item.(map[string]interface{})
					if i["expandable"].(bool) == false {
						metrics = append(metrics, counter_perfix+i["text"].(string))
					}
				}
				log.Printf("metrics:: => %v", metrics)
				result = getMetricValues(req, host, metrics, result)
			} else {
				metrics = append(metrics, strings.Join(targets, "."))
				result = getMetricValues(req, host, metrics, result)
			}
		}
	}
	RenderJson(rw, result)
}

func grafanaAPIParser(rw http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		setQueryEditor(rw, req)
	} else if req.Method == "POST" {
		getValues(rw, req)
	}
}

func configGrafanaRoutes() {
	http.HandleFunc("/api/grafana/", grafanaAPIParser)
}
