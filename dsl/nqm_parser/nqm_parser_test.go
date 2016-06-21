package nqm_parser

import (
	"fmt"
	"testing"
	. "gopkg.in/check.v1"
	"log"
	"strings"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type TestNqmDslParserSuite struct{}

var _ = Suite(&TestNqmDslParserSuite{})

// Tests the parsing for time parameters
type timeParamsTestCase struct {
	sampleStartTime string
	sampleEndTime string
	expectedStartTime time.Time
	expectedEndTime time.Time
}
func (suite *TestNqmDslParserSuite) TestTimeParams(c *C) {
	testCases := []*timeParamsTestCase {
		&timeParamsTestCase{ "1273053600", "1273312800", time.Unix(1273053600, 0), time.Unix(1273312800, 0) },
		&timeParamsTestCase{ "2011-10-01", "2011-10-03", parseTime("2011-10-01T00:00:00+08:00"), parseTime("2011-10-03T00:00:00+08:00") },
		&timeParamsTestCase{ "2011-08-20T10", "2011-08-20T16", parseTime("2011-08-20T10:00:00+08:00"), parseTime("2011-08-20T16:00:00+08:00") },
		&timeParamsTestCase{ "2011-07-11T10:30", "2011-07-11T11:30", parseTime("2011-07-11T10:30:00+08:00"), parseTime("2011-07-11T11:30:00+08:00") },
		&timeParamsTestCase{ "2011-06-03T10:00+04:00", "2011-06-03T12:00+04:00", parseTime("2011-06-03T10:00:00+04:00"), parseTime("2011-06-03T12:00:00+04:00") },
	}

	for _, testCase := range testCases {
		testedQueryParams, err := doParse(
			fmt.Sprintf("starttime=%s endtime=%s", testCase.sampleStartTime, testCase.sampleEndTime),
		)

		c.Assert(err, IsNil)

		c.Assert(testedQueryParams.StartTime.Unix(), Equals, testCase.expectedStartTime.Unix())
		c.Assert(testedQueryParams.EndTime.Unix(), Equals, testCase.expectedEndTime.Unix())
	}
}

// Tests the paring for node parameters
type getCheckedValue func(testedQueryParam *QueryParams) []string
type nodeParamsTestCase struct {
	dsl string
	expectedNodeValue []string
	getCheckedValue getCheckedValue
}
func (suite *TestParseProcessorSuite) TestNodeParams(c *C) {
	testCases := []*nodeParamsTestCase {
		&nodeParamsTestCase{
			"agent.province=廣東,浙江,山東", []string{ "廣東", "浙江", "山東"},
			func (testedQueryParam *QueryParams) []string { return testedQueryParam.AgentFilter.MatchProvinces },
		},
		&nodeParamsTestCase{
			"agent.isp=i1,i2", []string{ "i1", "i2" },
			func (testedQueryParam *QueryParams) []string { return testedQueryParam.AgentFilter.MatchIsps },
		},
		&nodeParamsTestCase{
			"agent.city=c1,c2", []string{ "c1", "c2" },
			func (testedQueryParam *QueryParams) []string { return testedQueryParam.AgentFilter.MatchCities },
		},
		&nodeParamsTestCase{
			"target.province=北京,台北", []string{ "北京", "台北" },
			func (testedQueryParam *QueryParams) []string { return testedQueryParam.TargetFilter.MatchProvinces },
		},
		&nodeParamsTestCase{
			"target.isp=i3,i4", []string{ "i3", "i4" },
			func (testedQueryParam *QueryParams) []string { return testedQueryParam.TargetFilter.MatchIsps },
		},
		&nodeParamsTestCase{
			"target.city=c3,c4", []string{ "c3", "c4" },
			func (testedQueryParam *QueryParams) []string { return testedQueryParam.TargetFilter.MatchCities },
		},
	}

	for _, testCase := range testCases {
		result, err := doParse(testCase.dsl)

		c.Assert(err, IsNil)
		c.Assert(testCase.getCheckedValue(result), DeepEquals, testCase.expectedNodeValue)
	}
}

func parseTime(timeStr string) time.Time {
	time, err := time.Parse(time.RFC3339, timeStr)

	if err != nil {
		log.Fatalf("Parse time \"%v\" has error. Error: %v", timeStr, err)
	}

	return time
}

// Test the error for unknown parameters
type unknownParamTestCase struct {
	sampleDsl string
	errorMatch string
}
func (suite *TestNqmDslParserSuite) TestUnknownParam(c *C) {
	testCases := []*unknownParamTestCase {
		&unknownParamTestCase{ "starttime", ".*missed.*" },
		&unknownParamTestCase{ "starttime=", ".*need set.*" },
		&unknownParamTestCase{ "starttime=ggaa", ".*cannot accept.*" },
		&unknownParamTestCase{ "target.isp", ".*missed.*" },
		&unknownParamTestCase{ "agent.isp=a1,,=2j", ".*,=2j.*" },
		&unknownParamTestCase{ "agent.isp==,", ".*=,.*" },
		&unknownParamTestCase{ "param1=20", ".*param1.*" },
		&unknownParamTestCase{ "agent.gogo", ".*agent.gogo.*" },
		&unknownParamTestCase{ "starttime9=33 starttime=10 endtime=20", ".*starttime9.*" },
		&unknownParamTestCase{ "starttime=10 endtime=20 endtime9=22", ".*endtime9.*" },
	}

	for _, testCase := range testCases {
		_, err := doParse(testCase.sampleDsl)

		c.Logf("Error Content: %v", err)

		c.Assert(err, ErrorMatches, testCase.errorMatch)
	}
}

func doParse(sampleDsl string) (*QueryParams, error) {
	result, err := ParseReader(
		"TestNqmDslParserSuite.file",
		strings.NewReader(sampleDsl),
	)

	if result == nil {
		return nil, err
	}

	return result.(*QueryParams), err
}
