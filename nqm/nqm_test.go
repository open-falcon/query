package nqm

import (
	"testing"
	"encoding/json"
	dsl "github.com/Cepave/query/dsl/nqm_parser" // As NQM intermediate representation
	qtest "github.com/Cepave/query/test"
	"github.com/bitly/go-simplejson"
	. "gopkg.in/check.v1"
	"sort"
)

func Test(t *testing.T) { TestingT(t) }

type TestNqmSuite struct{}

var _ = Suite(&TestNqmSuite{})

func (s *TestNqmSuite) SetUpSuite(c *C) {
	qtest.InitOrm()
}

func (s *TestNqmSuite) TearDownSuite(c *C) {}

// Tests the merging of data for provinces(by mocked data source)
func (suite *TestNqmSuite) TestListByProvincesByMockData(c *C) {
	srv := ServiceController {
		GetStatisticsOfIcmpByDsl: func(nqmDsl *NqmDsl) ([]IcmpResult, error) {
			return []IcmpResult {
				IcmpResult {
					grouping: []int32 { 11 },
					metrics: &Metrics { Max: 31 },
				},
				IcmpResult {
					grouping: []int32 { 12 },
					metrics: &Metrics { Max: 32 },
				},
				IcmpResult {
					grouping: []int32 { 13 },
					metrics: &Metrics { Max: 33 },
				},
			}, nil
		},
		GetProvinceById: func(provinceId int16) *Province {
			switch provinceId {
				case 11:
					return &Province{ Id: 11, Name: "PV-11" }
				case 12:
					return &Province{ Id: 12, Name: "PV-12" }
			}

			return &Province{ Id: 99, Name: "PV-99" }
		},
	}

	testedResult := srv.ListByProvinces(&dsl.QueryParams{})

	/**
	 * Asserts the joined data and metric
	 */
	c.Assert(testedResult, HasLen, 3)
	c.Assert(testedResult[1].Province.Id, Equals, int16(12))
	c.Assert(testedResult[1].Province.Name, Equals, "PV-12")
	c.Assert(testedResult[1].Metrics.Max, Equals, int16(32))
	// :~)
}

// Tests the merging of data for targets(by mocked data source)
func (suite *TestNqmSuite) TestListTargetsWithCityDetail(c *C) {
	srv := ServiceController {
		GetStatisticsOfIcmpByDsl: func(nqmDsl *NqmDsl) ([]IcmpResult, error) {
			switch len(nqmDsl.GroupingColumns) {
				/**
				 * Mocks the statistics of city
				 */
				case 1:
					return []IcmpResult {
						IcmpResult {
							grouping: []int32 { 41 },
							metrics: &Metrics { Max: 87 },
						},
						IcmpResult {
							grouping: []int32 { 42 },
							metrics: &Metrics { Max: 62 },
						},
					}, nil
				// :~)
				/**
				 * Mocks the statistic of target
				 */
				case 3:
					return []IcmpResult {
						IcmpResult {
							grouping: []int32 { 2001, 41, 81 },
							metrics: &Metrics { Max: 79 },
						},
						IcmpResult {
							grouping: []int32 { 2002, 41, 81 },
							metrics: &Metrics { Max: 62 },
						},
						IcmpResult {
							grouping: []int32 { 2003, 42, 81 },
							metrics: &Metrics { Max: 71 },
						},
						IcmpResult {
							grouping: []int32 { 2004, 42, 81 },
							metrics: &Metrics { Max: 68 },
						},
					}, nil
					// :~)
				default:
					c.Error("Unknown DSL for mocking \"GetStatisticsOfIcmpByDsl\"")
					return nil, nil
			}
		},
		GetCityById: func(cityId int16) *City {
			switch cityId {
				case 41:
					return &City{ Id: cityId, Name: "葡萄城市" }
				case 42:
					return &City{ Id: cityId, Name: "香蕉城市" }
			}

			return &City{ Id: 99, Name: "PV-99" }
		},
		GetIspById: func(ispId int16) *Isp {
			switch ispId {
				case 81:
					return &Isp{ Id: ispId, Name: "金牌快網" }
			}

			return &Isp{ Id: 99, Name: "ISP-99" }
		},
		GetTargetById: func(targetId int32) *Target {
			switch targetId {
				case 2001:
					return &Target { Id: targetId, Host: "98.20.50.1" }
				case 2002:
					return &Target { Id: targetId, Host: "98.20.50.2" }
				case 2003:
					return &Target { Id: targetId, Host: "98.20.50.3" }
				case 2004:
					return &Target { Id: targetId, Host: "98.20.50.4" }
			}

			return &Target{ Id: 99, Host: "UNKNOWN_TARGET" }
		},
	}

	testedResult := srv.ListTargetsWithCityDetail(&dsl.QueryParams{})

	c.Assert(len(testedResult), Equals, 2) // Asserts 2 cities

	testedCity := testedResult[0]
	/**
	 * Asserts data of city
	 */
	c.Assert(testedCity.City.Name, Equals, "葡萄城市")
	c.Assert(testedCity.Metrics.Max, Equals, int16(87))
	// :~)

	/**
	 * Asserts data of target
	 */
	c.Assert(len(testedCity.Targets), Equals, 2)

	testedTarget := testedCity.Targets[0]
	c.Assert(testedTarget.Id, Equals, int32(2001))
	c.Assert(testedTarget.Host, Equals, "98.20.50.1")
	c.Assert(testedTarget.Isp.Id, Equals, int16(81))
	c.Assert(testedTarget.Isp.Name, Equals, "金牌快網")
	c.Assert(testedTarget.Metrics.Max, Equals, int16(79))
	// :~)
}

// Tests the content of JSON for metric grouping with provinces
func (suite *TestNqmSuite) TestJsonOfProvinceMetric(c *C) {
	sampleData := []ProvinceMetric {
		ProvinceMetric{
			Province: &Province{ Id: 20, Name: "Dog-1" },
			Metrics: &Metrics{ Max: 40, Min: 30, Avg: 33.45 },
		},
		ProvinceMetric{
			Province: &Province{ Id: 21, Name: "Dog-2" },
			Metrics: &Metrics{ Max: 50, Min: 43, Avg: 44.5 },
		},
	}

	rawJson, objectToJsonErr := json.MarshalIndent(sampleData, "", "  ")
	c.Assert(objectToJsonErr, IsNil)
	c.Logf("JSON: %v", string(rawJson))

	testedJson, toSimpleJsonError := simplejson.NewJson(rawJson)

	c.Assert(toSimpleJsonError, IsNil)
	c.Assert(testedJson.GetIndex(0).GetPath("province", "name").MustString(), Equals, "Dog-1")
	c.Assert(testedJson.GetIndex(1).GetPath("metrics", "min").MustInt(), Equals, 43)
}

// Tests the content of JSON for metric grouping with cities
func (suite *TestNqmSuite) TestJsonOfCityMetric(c *C) {
	sampleData := []CityMetric {
		CityMetric {
			City: &City{ Id: 51, Name: "小黃瓜城市" },
			Metrics: &Metrics { Max: 82, Min: 33, Avg: 62.81 },
			Targets: []TargetMetric {
				TargetMetric {
					Id: 4021, Host: "h1.ping.org", Isp: &Isp{ Id: 91, Name: "山東網路" },
					Metrics: &Metrics{ Max: 101, Min: 63, Avg: 77.3 },
				},
				TargetMetric {
					Id: 4022, Host: "h2.ping.org", Isp: &Isp{ Id: 91, Name: "山東網路" },
					Metrics: &Metrics{ Max: 93, Min: 77, Avg: 82.5 },
				},
			},
		},
		CityMetric {
			City: &City{ Id: 52, Name: "高麗菜城市" },
			Metrics: &Metrics { Max: 32, Min: 12, Avg: 22.3 },
			Targets: []TargetMetric {
				TargetMetric {
					Id: 4031, Host: "g1.ping.org", Isp: &Isp{ Id: 91, Name: "山東網路" },
					Metrics: &Metrics{ Max: 62, Min: 37, Avg: 40.25 },
				},
				TargetMetric {
					Id: 4032, Host: "g2.ping.org", Isp: &Isp{ Id: 91, Name: "山東網路" },
					Metrics: &Metrics{ Max: 35, Min: 22, Avg: 29.1 },
				},
			},
		},
	}

	rawJson, objectToJsonErr := json.MarshalIndent(sampleData, "", "  ")
	c.Assert(objectToJsonErr, IsNil)
	c.Logf("JSON: %v", string(rawJson))

	testedJson, toSimpleJsonError := simplejson.NewJson(rawJson)

	/**
	 * Asserts the content of city
	 */
	c.Assert(toSimpleJsonError, IsNil)
	c.Assert(testedJson.GetIndex(0).GetPath("city", "name").MustString(), Equals, "小黃瓜城市")
	c.Assert(testedJson.GetIndex(1).GetPath("metrics", "min").MustInt(), Equals, 12)
	// :~)

	/**
	 * Asserts the content of target
	 */
	testedJsonTargets := testedJson.GetIndex(1).GetPath("targets")
	c.Assert(testedJsonTargets.GetIndex(0).Get("id").MustInt(), Equals, 4031)
	c.Assert(testedJsonTargets.GetIndex(1).Get("host").MustString(), Equals, "g2.ping.org")
	c.Assert(testedJsonTargets.GetIndex(0).GetPath("metrics", "avg").MustFloat64(), Equals, 40.25)
	// :~)
}

// Tests the convertion from IR of NQM DSL to query parameters on ICMP log(Cassandra)
func (suite *TestNqmSuite) TestToNqmDsl(c *C) {
	sampleQueryParam := dsl.QueryParams {
		AgentFilter: dsl.NodeFilter {
			MatchProvinces: []string { "湖北", "青海", "不存在" },
			MatchIsps: []string { "电信通", "大陆其它", "不存在" },
		},
		AgentFilterById: dsl.NodeFilterById {
			MatchProvinces: []int16 { 34, 34 },
			MatchIsps: []int16 { 51, 52 },
		},
		TargetFilter: dsl.NodeFilter {
			MatchProvinces: []string { "宁夏", "山东", "不存在" },
			MatchIsps: []string { "天威", "台湾中华电信", "不存在" },
		},
		TargetFilterById: dsl.NodeFilterById {
			MatchProvinces: []int16 { 36, 36 },
			MatchIsps: []int16 { 53, 54 },
		},
	}

	testedDslParams := toNqmDsl(&sampleQueryParam)

	sort.Sort(idArray(testedDslParams.IdsOfAgentProvinces))
	c.Assert(testedDslParams.IdsOfAgentProvinces, DeepEquals, []Id2Bytes { UNKNOWN_ID_FOR_QUERY, 18, 29, 34 })
	sort.Sort(idArray(testedDslParams.IdsOfAgentIsps))
	c.Assert(testedDslParams.IdsOfAgentIsps, DeepEquals, []Id2Bytes { UNKNOWN_ID_FOR_QUERY, 7, 29, 51, 52 })
	sort.Sort(idArray(testedDslParams.IdsOfTargetProvinces))
	c.Assert(testedDslParams.IdsOfTargetProvinces, DeepEquals, []Id2Bytes { UNKNOWN_ID_FOR_QUERY, 11, 28, 36 })
	sort.Sort(idArray(testedDslParams.IdsOfTargetIsps))
	c.Assert(testedDslParams.IdsOfTargetIsps, DeepEquals, []Id2Bytes { UNKNOWN_ID_FOR_QUERY, 18, 23, 53, 54 })
}

type idArray []Id2Bytes
func (a idArray) Len() int           { return len(a) }
func (a idArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a idArray) Less(i, j int) bool { return a[i] < a[j] }

func (s *TestNqmSuite) SetUpTest(c *C) {
	switch c.TestName() {
	case "TestNqmSuite.TestToNqmDsl":
		if !qtest.HasDefaultOrmOnPortal(c) {
			return
		}
	}
}
