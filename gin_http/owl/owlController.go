package owl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/Cepave/query/g"
)

type EndpointCounters struct {
	Endpoint string `json:"endpoint"`
	Counter  string `json:"counter"`
}

type PostData struct {
	Start            int64              `json:"start"`
	End              int64              `json:"end"`
	CF               string             `json:"cf"`
	EndpointCounters []EndpointCounters `json:"endpoint_counters"`
}

func gethost() (endpoints []string) {
	dat, _ := ioutil.ReadFile("./test/owl_endpoints")
	endpoints = strings.Split(string(dat), ",")
	return
}

func generatePostData(endpoints []string, counter string, sts int64, ets int64) PostData {
	pd := PostData{
		Start: sts,
		End:   ets,
		CF:    "AVERAGE",
	}
	var ec []EndpointCounters
	for _, enp := range endpoints {
		ec = append(ec, EndpointCounters{enp, counter})
	}
	pd.EndpointCounters = ec
	return pd
}

func DoPost() {
	conf := g.Config()
	url := fmt.Sprintf("%s%s", conf.Http.Listen, "/graph/history")
	// host := []string{"endpointA", "endpointB"}
	host := gethost()
	dd := generatePostData(host, "cpu.idle", 1464761471, 1464847858)
	data, _ := json.Marshal(&dd)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(data)))
	if err != nil {
		log.Println(err.Error())
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("response Body:", string(body))
}
