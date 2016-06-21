package nqm_parser

import (
	"fmt"
	"strconv"
	"time"
)

const unknown_type = -1

const (
	_ = iota

	param_start_time = iota
	param_end_time = iota
	param_agent_isp = iota
	param_agent_province = iota
	param_agent_city = iota
	param_target_isp = iota
	param_target_province = iota
	param_target_city = iota

	parse_iso8601_minute = "2006-01-02T15:04Z07:00"
)

var currentTimeZone = time.Now().Format("Z07:00")

type paramContent struct {
	paramType int
	paramValue interface{}
	setQueryParams func (*QueryParams, interface{})
}

var emptyParamContent = &paramContent{ unknown_type, nil, func(p *QueryParams, v interface{}) {} }

func parseValidPramName(paramName interface{}, assignedValue interface{}) (*paramContent, error) {
	if assignedValue == nil {
		return emptyParamContent, fmt.Errorf("\"%v\" missed \"=<value>\" ?", paramName);
	}

	if assignedValue != nil {
		paramValue := assignedValue.([]interface{})[1]

		if paramValue != nil {
			return emptyParamContent, fmt.Errorf("\"%v\" cannot accept \"%v\"", paramName, paramValue)
		}

		return emptyParamContent, fmt.Errorf("\"%v=\" need set value", paramName)
	}

	return emptyParamContent, nil
}

// Builds paramContent by name of param
func buildParamContent(paramName interface{}, srcParamValue interface{}) *paramContent {
	var resultParamType int = unknown_type
	var implSetParam func (*QueryParams, interface{})

	switch paramName.(string) {
	case "starttime":
		resultParamType = param_start_time
		implSetParam = func (p *QueryParams, v interface{}) {
			p.StartTime = v.(time.Time)
		}
	case "endtime":
		resultParamType = param_end_time
		implSetParam = func (p *QueryParams, v interface{}) {
			p.EndTime = v.(time.Time)
		}
	case "agent.isp":
		resultParamType = param_agent_isp
		implSetParam = func (p *QueryParams, v interface{}) {
			p.addIspOfAgent(v.([]string)...)
		}
	case "agent.province":
		resultParamType = param_agent_province
		implSetParam = func (p *QueryParams, v interface{}) {
			p.addProvinceOfAgent(v.([]string)...)
		}
	case "agent.city":
		resultParamType = param_agent_city
		implSetParam = func (p *QueryParams, v interface{}) {
			p.addCityOfAgent(v.([]string)...)
		}
	case "target.isp":
		resultParamType = param_target_isp
		implSetParam = func (p *QueryParams, v interface{}) {
			p.addIspOfTarget(v.([]string)...)
		}
	case "target.province":
		resultParamType = param_target_province
		implSetParam = func (p *QueryParams, v interface{}) {
			p.addProvinceOfTarget(v.([]string)...)
		}
	case "target.city":
		resultParamType = param_target_city
		implSetParam = func (p *QueryParams, v interface{}) {
			p.addCityOfTarget(v.([]string)...)
		}
	}

	return &paramContent {
		paramType: resultParamType,
		paramValue: srcParamValue,
		setQueryParams: implSetParam,
	}
}

// Sets the parameters for query with "starttime" or "endtime"
//
// If there are multiple "starttime"s or "endtime"s, the last one is the final value.
func setParams(queryParams *QueryParams, params interface{}) error {
	for _, param := range params.([]interface{}) {
		paramContent := param.(*paramContent)
		paramContent.setQueryParams(queryParams, paramContent.paramValue)
	}

	return nil;
}

func combineStringLiterals(first interface{}, rest interface{}) []string {
	allRests := rest.([]interface{})

	result := make([]string, 0, len(allRests) + 1)
	result = append(result, first.(string))

	for _, v := range allRests {
		result = append(result, v.(string))
	}

	return result
}

// Parses the unix time by string representation of "1213213"(Epoch tiem)
func parseUnixTime(c *current) (time.Time, error) {
	unixTimeInt64, parseErr := strconv.ParseInt(string(c.text), 0, 64)

	if parseErr != nil {
		return time.Unix(0, 0), parseErr
	}

	return time.Unix(unixTimeInt64, 0), nil
}

// Parses the string representation of ISO-8601 format
//
// For example(assumes local timezone is "+08:00"):
//
// 2010-05-05 -> 2010-05-05T00:00+08:00
// 2010-04-12T02 -> 2010-04-12T02:00+08:00
// 2010-03-05T10:30 -> 2010-03-05T10:30+08:00
// 2012-01-10T10:30+02:00 -> 2012-01-10T10:30+02:00
func parseIso8601(c *current) (time.Time, error) {
	timeStr := string(c.text)

	switch (len(timeStr)) {
	case 10:
		timeStr = fmt.Sprintf("%sT00:00%s", timeStr, currentTimeZone)
	case 13:
		timeStr = fmt.Sprintf("%s:00%s", timeStr, currentTimeZone)
	case 16:
		timeStr = fmt.Sprintf("%s%s", timeStr, currentTimeZone)
	}

	return time.Parse(parse_iso8601_minute, timeStr)
}
