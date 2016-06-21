package nqm

import (
	"net/http"
    "github.com/gorilla/rpc/v2"
	"github.com/Cepave/query/g"
	"github.com/bitly/go-simplejson"
	rpctest "github.com/Cepave/query/test"
	. "gopkg.in/check.v1"
)

type TestNqmLogSuite struct{}

var _ = Suite(&TestNqmLogSuite{})

type queryIcmpByDslTestCase struct {
	sampleIdOfAgentProvince Id2Bytes
	expectedNumberOfResult int
}
// Tests the query(calling of JSONRPC) by DSL for ICMP log
func (suite *TestNqmLogSuite) TestGetStatisticsOfIcmpByDsl(c *C) {
	testCases := []queryIcmpByDslTestCase {
		{ 15, 1 },
		{ 20, 2 },
		{ 23, 0 }, // No data
	}

	for _, testCase := range testCases {
		icmpParams := &NqmDsl {
			GroupingColumns: []string { "ib_ag_pv_id" },
			StartTime: 1328407200,
			EndTime: 1328493600,
			IdsOfAgentProvinces: []Id2Bytes { testCase.sampleIdOfAgentProvince },
			IdsOfAgentIsps: []Id2Bytes { 16 },
			IdsOfTargetProvinces: []Id2Bytes { 31 },
			IdsOfTargetIsps: []Id2Bytes { 87 },
			ProvinceRelation: -1,
		}

		testedResult, err := getStatisticsOfIcmpByDsl(icmpParams)

		/**
		 * Asserts the content of data
		 */
		c.Assert(err, IsNil)
		c.Assert(len(testedResult), Equals, testCase.expectedNumberOfResult)
		if testCase.expectedNumberOfResult > 0 {
			c.Assert(testedResult[0].grouping, DeepEquals, []int32 { 20, 40 })
			c.Assert(testedResult[0].metrics.Avg, Equals, float32(45.78))
			c.Assert(testedResult[0].metrics.Max, Equals, int16(88))
		}
		// :~)
	}
}

func (s *TestNqmLogSuite) SetUpSuite(c *C) {
	rpctest.StartMockJsonRpcServer(
		c,
		func(server *rpc.Server) {
			server.RegisterService(new(mockNqmService), "NqmEndpoint")
		},
	)

	g.SetConfig(
		&g.GlobalConfig {
			NqmLog: &g.NqmLogConfig {
				JsonrpcUrl: rpctest.GetUrlOfMockedServer(),
			},
		},
	)
	initIcmp()
}
func (s *TestNqmLogSuite) TearDownSuite(c *C) {
	rpctest.StopMockJsonRpcServer(c)
}

type mockNqmService struct {}
func (srv *mockNqmService) QueryIcmpByDsl(r *http.Request, args *IcmpDslArgs, replyResult **[]*simplejson.Json) error {
	jsonIcmpStatistics := simplejson.New()

	jsonIcmpStatistics.Set("grouping", []int32 { 20, 40 })
	jsonIcmpStatistics.Set("max", 88)
	jsonIcmpStatistics.Set("min", 33)
	jsonIcmpStatistics.Set("avg", 45.78)
	jsonIcmpStatistics.Set("mdev", 35.22)
	jsonIcmpStatistics.Set("med", 55)
	jsonIcmpStatistics.Set("count", 100)
	jsonIcmpStatistics.Set("loss", 0.1)
	jsonIcmpStatistics.Set("number_of_sent_packets",9000)
	jsonIcmpStatistics.Set("number_of_received_packets",8933)

	dsl := args.Dsl

	resultList := make([]*simplejson.Json, 0)
	switch dsl.IdsOfAgentProvinces[0] {
		case 15:
			resultList = append(resultList, jsonIcmpStatistics)
		case 20:
			resultList = append(resultList, jsonIcmpStatistics)
			resultList = append(resultList, jsonIcmpStatistics)
	}

	*replyResult = &resultList

	return nil
}
