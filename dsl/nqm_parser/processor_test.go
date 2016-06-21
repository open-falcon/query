package nqm_parser

import (
	. "gopkg.in/check.v1"
	"time"
)

type TestParseProcessorSuite struct{}

var _ = Suite(&TestParseProcessorSuite{})

// Tests the parsing of Epoch time
func (suite *TestParseProcessorSuite) TestParseUnixTime(c *C) {
	testedResult, err := parseUnixTime(buildCurrent("1273053600"))

	c.Assert(err, IsNil)

	c.Logf("Parse UnixTime: %v", testedResult);

	c.Assert(testedResult.Unix(), Equals, int64(1273053600));
}

// Tests the combination for string literals
type combineStringLiteralsTestCase struct {
	first string
	rest []interface{}
	expectedResult []string
}
func (suite *TestParseProcessorSuite) TestCombineStringLiterals(c *C) {
	testCases := []*combineStringLiteralsTestCase {
		&combineStringLiteralsTestCase{ "a1", []interface{}{ "b1", "b2" }, []string{ "a1", "b1", "b2" } },
		&combineStringLiteralsTestCase{ "a9", nil, []string{ "a9" } },
	}

	for _, testCase := range testCases {
		c.Assert(combineStringLiterals(testCase.first, testCase.rest), DeepEquals, testCase.expectedResult)
	}
}

// Tests the setting for parameters for query
func (suite *TestParseProcessorSuite) TestSetParams(c *C) {
	var testedParams QueryParams

	addedDays, _ := time.ParseDuration("72h")
	sampleStartTime, sampleEndTime := time.Now(), time.Now().Add(addedDays)

	setParams(
		&testedParams,
		[]interface{} {
			buildParamContent("starttime", sampleStartTime),
			buildParamContent("endtime", sampleEndTime),
			buildParamContent("agent.isp", []string{ "i1", "i2" }),
			buildParamContent("agent.province", []string{ "p1", "p2" }),
			buildParamContent("agent.city", []string{ "c1", "c2" }),
			buildParamContent("target.isp", []string{ "i3", "i4" }),
			buildParamContent("target.province", []string{ "p3", "p4" }),
			buildParamContent("target.city", []string{ "c3", "c4" }),
		},
	)

	c.Assert(testedParams.StartTime, Equals, sampleStartTime)
	c.Assert(testedParams.EndTime, Equals, sampleEndTime)
	c.Assert(testedParams.AgentFilter.MatchIsps, DeepEquals, []string { "i1", "i2" })
	c.Assert(testedParams.AgentFilter.MatchProvinces, DeepEquals, []string { "p1", "p2" })
	c.Assert(testedParams.AgentFilter.MatchCities, DeepEquals, []string { "c1", "c2" })
	c.Assert(testedParams.TargetFilter.MatchIsps, DeepEquals, []string { "i3", "i4" })
	c.Assert(testedParams.TargetFilter.MatchProvinces, DeepEquals, []string { "p3", "p4" })
	c.Assert(testedParams.TargetFilter.MatchCities, DeepEquals, []string { "c3", "c4" })
}

// Tests the parsing for ISO8601 with various format of input
type parseIso8601Case struct {
	sampleValue string
	expectedYear int
	expectedHour int
}
func (suite *TestParseProcessorSuite) TestParseIso8601(c *C) {
	testCases := []parseIso8601Case {
		{ "2012-10-10T14:10+04:00", 2012, 14 },
		{ "2008-02-15T07:10", 2008, 7 },
		{ "2009-03-07T08", 2009, 8 },
		{ "2011-04-02", 2011, 0 },
	}

	for _, testCase := range testCases {
		testedResult, err := parseIso8601(buildCurrent(testCase.sampleValue))

		c.Assert(err, IsNil)

		c.Logf("Parse ISO8601: \"%v\" Result: \"%v\"", testCase.sampleValue, testedResult);
		c.Assert(testedResult.Year(), Equals, testCase.expectedYear)
		c.Assert(testedResult.Hour(), Equals, testCase.expectedHour)
	}
}

func buildCurrent(value string) *current {
	return &current {
		pos: position{},
		text: []byte(value),
	}
}
