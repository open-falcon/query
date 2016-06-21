package http

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"regexp"

	cmodel "github.com/Cepave/common/model"
	"github.com/Cepave/query/graph"
	"github.com/Cepave/query/proc"
)

type GraphHistoryParam struct {
	Start            int                     `json:"start"`
	End              int                     `json:"end"`
	CF               string                  `json:"cf"`
	Step             int                     `json:"step"`
	EndpointCounters []cmodel.GraphInfoParam `json:"endpoint_counters"`
}

func graphQueryOne(ec cmodel.GraphInfoParam, body GraphHistoryParam, endpoint string, counter string) *cmodel.GraphQueryResponse {
	if endpoint == "" {
		endpoint = ec.Endpoint
	}
	if counter == "" {
		counter = ec.Counter
	}
	request := cmodel.GraphQueryParam{
		Start:     int64(body.Start),
		End:       int64(body.End),
		ConsolFun: body.CF,
		Step:      body.Step,
		Endpoint:  endpoint,
		Counter:   counter,
	}
	result, err := graph.QueryOne(request)
	if err != nil {
		log.Printf("graph.queryOne fail, %v, (endpoint, counter) = (%s, %s)", err, endpoint, counter)
	}
	return result
}

func makeOneFakeResult(body GraphHistoryParam) *cmodel.GraphQueryResponse {
	for _, ec := range body.EndpointCounters {
		if !strings.Contains(ec.Counter, "packet-loss-rate") && !strings.Contains(ec.Counter, "average") {
			fakeResult := graphQueryOne(ec, body, "", "")
			for i := range fakeResult.Values {
				fakeResult.Values[i].Value = cmodel.JsonFloat(0.0)
			}
			return fakeResult
		}
	}
	return nil
}

func detectCounter(counter string, body GraphHistoryParam) bool {
	for _, ec := range body.EndpointCounters {
		if strings.Contains(ec.Counter, counter) {
			return true
		}
	}
	return false
}

func nqmData(body GraphHistoryParam, nqmDataCounter string, rawDataCounter string) []*cmodel.GraphQueryResponse {

	result := makeOneFakeResult(body)
	if result == nil {
		return nil
	}
	data := []*cmodel.GraphQueryResponse{}
	packetSentCount := make([]cmodel.JsonFloat, len(result.Values))
	for i := range result.Values {
		packetSentCount[i] = cmodel.JsonFloat(0.0)
	}
	for _, ec := range body.EndpointCounters {
		if !strings.Contains(ec.Counter, rawDataCounter) {
			continue
		}

		resultRaw := graphQueryOne(ec, body, "", "")
		if resultRaw == nil {
			continue
		}
		data = append(data, resultRaw)

		if rawDataCounter == "packets-sent" {
			counter := strings.Replace(ec.Counter, "packets-sent", "packets-received", 1)
			if resultAdditional := graphQueryOne(ec, body, "", counter); resultAdditional != nil {
				data = append(data, resultAdditional)
				for i := range resultRaw.Values {
					packetLossCount := (resultRaw.Values[i].Value - resultAdditional.Values[i].Value)
					result.Values[i].Value += packetLossCount
					packetSentCount[i] += resultRaw.Values[i].Value
				}
			}
		} else if rawDataCounter == "transmission-time" {
			counter := strings.Replace(ec.Counter, "transmission-time", "packets-sent", 1)
			if resultAdditional := graphQueryOne(ec, body, "", counter); resultAdditional != nil {
				data = append(data, resultAdditional)
				for i := range resultAdditional.Values {
					result.Values[i].Value += resultAdditional.Values[i].Value * resultRaw.Values[i].Value
					packetSentCount[i] += resultAdditional.Values[i].Value
				}
			}
		}
	}
	for i := range result.Values {
		result.Values[i].Value = result.Values[i].Value / packetSentCount[i]
	}
	result.Endpoint = "all-endpoints"
	result.Counter = nqmDataCounter
	data = append(data, result)
	return data
}

func configGraphRoutes() {

	// method:post
	http.HandleFunc("/graph/history", func(w http.ResponseWriter, r *http.Request) {
		// statistics
		proc.HistoryRequestCnt.Incr()

		var body GraphHistoryParam
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&body)
		if err != nil {
			StdRender(w, "", err)
			return
		}

		if len(body.EndpointCounters) == 0 {
			StdRender(w, "", errors.New("empty_payload"))
			return
		}

		data := []*cmodel.GraphQueryResponse{}

		isPacketLossRate := detectCounter("packet-loss-rate", body)
		isAverage := detectCounter("average", body)

		if isPacketLossRate || isAverage {
			// NQM Case
			if isPacketLossRate {
				data = append(data, nqmData(body, "packet-loss-rate", "packets-sent")...)
			} else if isAverage {
				data = append(data, nqmData(body, "average", "transmission-time")...)
			}
		} else {
			for _, ec := range body.EndpointCounters {
				regx, _ := regexp.Compile("(\\.\\$\\s*|\\s*)$")
				endpoint := regx.ReplaceAllString(ec.Endpoint, "")
				counter := regx.ReplaceAllString(ec.Counter, "")
				result := graphQueryOne(ec, body, endpoint, counter)
				if result == nil {
					continue
				}
				data = append(data, result)
			}
		}

		// statistics
		proc.HistoryResponseCounterCnt.IncrBy(int64(len(data)))
		for _, item := range data {
			proc.HistoryResponseItemCnt.IncrBy(int64(len(item.Values)))
		}

		StdRender(w, data, nil)
	})

	// post, info
	http.HandleFunc("/graph/info", func(w http.ResponseWriter, r *http.Request) {
		// statistics
		proc.InfoRequestCnt.Incr()

		var body []*cmodel.GraphInfoParam
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&body)
		if err != nil {
			StdRender(w, "", err)
			return
		}

		if len(body) == 0 {
			StdRender(w, "", errors.New("empty"))
			return
		}

		data := []*cmodel.GraphFullyInfo{}
		for _, param := range body {
			if param == nil {
				continue
			}
			info, err := graph.Info(*param)
			if err != nil {
				log.Printf("graph.info fail, resp: %v, err: %v", info, err)
			}
			if info == nil {
				continue
			}
			data = append(data, info)
		}

		StdRender(w, data, nil)
	})

	// post, last
	http.HandleFunc("/graph/last", func(w http.ResponseWriter, r *http.Request) {
		// statistics
		proc.LastRequestCnt.Incr()

		var body []*cmodel.GraphLastParam
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&body)
		if err != nil {
			StdRender(w, "", err)
			return
		}

		if len(body) == 0 {
			StdRender(w, "", errors.New("empty"))
			return
		}

		data := []*cmodel.GraphLastResp{}
		for _, param := range body {
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
			data = append(data, last)
		}

		// statistics
		proc.LastRequestItemCnt.IncrBy(int64(len(data)))

		StdRender(w, data, nil)
	})

	// post, last/raw
	http.HandleFunc("/graph/last/raw", func(w http.ResponseWriter, r *http.Request) {
		// statistics
		proc.LastRawRequestCnt.Incr()

		var body []*cmodel.GraphLastParam
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&body)
		if err != nil {
			StdRender(w, "", err)
			return
		}

		if len(body) == 0 {
			StdRender(w, "", errors.New("empty"))
			return
		}

		data := []*cmodel.GraphLastResp{}
		for _, param := range body {
			if param == nil {
				continue
			}
			last, err := graph.LastRaw(*param)
			if err != nil {
				log.Printf("graph.last.raw fail, resp: %v, err: %v", last, err)
			}
			if last == nil {
				continue
			}
			data = append(data, last)
		}
		// statistics
		proc.LastRawRequestItemCnt.IncrBy(int64(len(data)))
		StdRender(w, data, nil)
	})

}
