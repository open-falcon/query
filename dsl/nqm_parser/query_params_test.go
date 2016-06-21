package nqm_parser

import (
	. "gopkg.in/check.v1"
	"sort"
	"time"
)

type TestQueryParamsSuite struct{}

var _ = Suite(&TestQueryParamsSuite{})

// Tests the adding values for node's properties
func (suite *TestQueryParamsSuite) TestAddValuesToNodeProperties(c *C) {
	var testedParam = &QueryParams{}

	testedParam.addProvinceOfAgent("p1", "p2")
	testedParam.addIspOfAgent("i1", "i2")
	testedParam.addCityOfAgent("c1", "c2")
	testedParam.addProvinceOfTarget("p3", "p4")
	testedParam.addIspOfTarget("i3", "i4")
	testedParam.addCityOfTarget("c3", "c4")

	c.Assert(testedParam.AgentFilter.MatchIsps, DeepEquals, []string{ "i1", "i2" })
	c.Assert(testedParam.AgentFilter.MatchProvinces, DeepEquals, []string{ "p1", "p2" })
	c.Assert(testedParam.AgentFilter.MatchCities, DeepEquals, []string{ "c1", "c2" })
	c.Assert(testedParam.TargetFilter.MatchIsps, DeepEquals, []string{ "i3", "i4" })
	c.Assert(testedParam.TargetFilter.MatchProvinces, DeepEquals, []string{ "p3", "p4" })
	c.Assert(testedParam.TargetFilter.MatchCities, DeepEquals, []string{ "c3", "c4" })
}

// Tests the error for both "province" and "city" are set
type locationErrorTestCase struct {
	params *QueryParams
	matchError string
}
func (suite *TestQueryParamsSuite) TestCheckParamsWithLocationError(c *C) {
	testCases := []*locationErrorTestCase {
		&locationErrorTestCase {
			&QueryParams {
				AgentFilter: NodeFilter {
					MatchProvinces: []string{ "p1", "p2" },
					MatchCities: []string{ "c1", "c2" },
				},
			},
			".*Agent.*",
		},
		&locationErrorTestCase {
			&QueryParams {
				TargetFilter: NodeFilter {
					MatchProvinces: []string{ "p1", "p2" },
					MatchCities: []string{ "c1", "c2" },
				},
			},
			".*Target.*",
		},
	}

	for _, testCase := range testCases {
		err := testCase.params.checkParams()

		c.Logf("Error for check parameters: %v", err)
		c.Assert(err, ErrorMatches, testCase.matchError)
	}
}

// Tests the elimination for duplicated values
func (suite *TestQueryParamsSuite) TestCheckParamsWithDuplicatedValue(c *C) {
	testedParams_1 := &QueryParams{
		AgentFilter: NodeFilter {
			MatchProvinces: []string{ "p1", "p2", "p1", "p2" },
			MatchIsps: []string{ "i1", "i2", "i1", "i2" },
		},
		TargetFilter: NodeFilter {
			MatchProvinces: []string{ "p3", "p4", "p3", "p4" },
			MatchIsps: []string{ "i3", "i4", "i3", "i4" },
		},
	}
	testedParams_2 := &QueryParams{
		AgentFilter: NodeFilter {
			MatchCities: []string{ "c1", "c2", "c1", "c2" },
		},
		TargetFilter: NodeFilter {
			MatchCities: []string{ "c3", "c4", "c3", "c4" },
		},
	}

	c.Assert(testedParams_1.checkParams(), IsNil)
	c.Assert(testedParams_2.checkParams(), IsNil)

	sort.Strings(testedParams_1.AgentFilter.MatchProvinces)
	sort.Strings(testedParams_2.AgentFilter.MatchCities)
	sort.Strings(testedParams_1.AgentFilter.MatchIsps)
	sort.Strings(testedParams_1.TargetFilter.MatchProvinces)
	sort.Strings(testedParams_2.TargetFilter.MatchCities)
	sort.Strings(testedParams_1.TargetFilter.MatchIsps)

	c.Assert(testedParams_1.AgentFilter.MatchProvinces, DeepEquals, []string{ "p1", "p2" })
	c.Assert(testedParams_2.AgentFilter.MatchCities, DeepEquals, []string{ "c1", "c2" })
	c.Assert(testedParams_1.AgentFilter.MatchIsps, DeepEquals, []string{ "i1", "i2" })
	c.Assert(testedParams_1.TargetFilter.MatchProvinces, DeepEquals, []string{ "p3", "p4" })
	c.Assert(testedParams_2.TargetFilter.MatchCities, DeepEquals, []string{ "c3", "c4" })
	c.Assert(testedParams_1.TargetFilter.MatchIsps, DeepEquals, []string{ "i3", "i4" })
}

type checkRationalOfParametersTestCase struct {
	startTime time.Time
	endTime time.Time
	assertion func(error)
}
// Tests the rational meaning of query parameters
func (suite *TestQueryParamsSuite) TestCheckRationalOfParameters(c *C) {
	testCases := []checkRationalOfParametersTestCase {
		/**
		 * Nothing failed
		 */
		checkRationalOfParametersTestCase {
			startTime: time.Now(),
			endTime: time.Now().Add(1 * time.Minute),
			assertion: func(err error) {
				c.Assert(err, IsNil)
			},
		},
		// :~)
		/**
		 * The time range is meaningless
		 */
		checkRationalOfParametersTestCase {
			startTime: time.Now(),
			endTime: time.Now().Add(-1 * time.Minute),
			assertion: func(err error) {
				c.Assert(err, ErrorMatches, "Start time is not valid.*")
			},
		},
		// :~)
	}

	for _, testCase := range testCases {
		testedParams := &QueryParams{
			StartTime: testCase.startTime,
			EndTime: testCase.endTime,
		}

		testCase.assertion(testedParams.CheckRationalOfParameters())
	}
}
