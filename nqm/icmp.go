package nqm

import (
	"fmt"
	"encoding/json"
	"github.com/Cepave/query/g"
	"github.com/Cepave/query/jsonrpc"
	"github.com/bitly/go-simplejson"
)

// As the DSL arguments for calling JSONRPC
type IcmpDslArgs struct {
	Dsl *NqmDsl `json:"dsl"`
}

// The raw result returned from JSONRPC
type IcmpResult struct {
	grouping []int32
	metrics *Metrics
}
// Used to unmarshal JSON with specific structure of IcmpResult(because of reusing struct)
func (icmpResult *IcmpResult) UnmarshalJSON(p []byte) error {
	jsonObj, err := simplejson.NewJson(p)
	if err != nil {
		return err
	}

	icmpResult.grouping = toGroupingIds(jsonObj.Get("grouping").MustArray())
	icmpResult.metrics = &Metrics{
		Max: int16(jsonObj.Get("max").MustInt()),
		Min: int16(jsonObj.Get("min").MustInt()),
		Avg: float32(jsonObj.Get("avg").MustFloat64()),
		Med: int16(jsonObj.Get("med").MustInt()),
		Mdev: float32(jsonObj.Get("mdev").MustFloat64()),
		Loss: float32(jsonObj.Get("loss").MustFloat64()),
		Count: int32(jsonObj.Get("count").MustInt()),
		NumberOfSentPackets: uint64(jsonObj.Get("number_of_sent_packets").MustUint64()),
		NumberOfReceivedPackets: uint64(jsonObj.Get("number_of_received_packets").MustUint64()),
	}

	return nil
}

func toGroupingIds(srcArray []interface{}) []int32 {
	result := make([]int32, len(srcArray))

	for i, v := range srcArray {
		number, e := v.(json.Number).Int64()
		if e != nil {
			panic(e)
		}
		result[i] = int32(number)
	}

	return result
}

var rpcServiceCaller *jsonrpc.JsonRpcService = nil
func initIcmp() {
	config := g.Config()
	rpcServiceCaller = jsonrpc.NewService(config.NqmLog.JsonrpcUrl)
}

// Retrieves the statistics of ICMP log by DSL
func getStatisticsOfIcmpByDsl(queryParams *NqmDsl) ([]IcmpResult, error) {
	var result = make([]IcmpResult, 0)

	httpInfo, err := rpcServiceCaller.CallMethod("NqmEndpoint.QueryIcmpByDsl", &IcmpDslArgs{ Dsl: queryParams }, &result)
	if err != nil {
		return nil, fmt.Errorf("Call JSONRPC \"NqmEndpoint.QueryIcmpByDsl\" has error: %v. HTTP: %v ", err, httpInfo)
	}

	return result, nil
}
