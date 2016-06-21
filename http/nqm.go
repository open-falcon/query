package http

import (
	"fmt"
	dsl "github.com/Cepave/query/dsl/nqm_parser"
	"github.com/Cepave/query/nqm"
	"github.com/Cepave/query/g"
	"github.com/astaxie/beego"
	"github.com/astaxie/beego/context"
	"github.com/bitly/go-simplejson"
	"log"
	"net/http"
	"strings"
	"time"
	"runtime/debug"
	"strconv"
)

const (
	jsonIndent = false
	jsonCoding = false
)

var nqmService nqm.ServiceController
func configNqmRoutes() {
	nqmService = nqm.GetDefaultServiceController()
	nqmService.Init()

	/**
	 * Registers the handler of RESTful service on beego
	 */
	serviceController := beego.NewControllerRegister()
	setupUrlMappingAndHandler(serviceController)
	// :~)

	http.Handle("/nqm/", serviceController)
}

func setupUrlMappingAndHandler(serviceRegister *beego.ControllerRegister) {
	serviceRegister.AddMethod(
		"get", "/nqm/icmp/list/by-provinces",
		listIcmpByProvinces,
	)
	serviceRegister.AddMethod(
		"get", "/nqm/icmp/province/:province_id([0-9]+)/list/by-targets",
		listIcmpByTargetsForAProvince,
	)
}

type resultWithDsl struct {
	queryParams *dsl.QueryParams
	resultData interface{}
}

func (result *resultWithDsl) MarshalJSON() ([]byte, error) {
	jsonObject := simplejson.New()

	jsonObject.SetPath([]string{ "dsl", "start_time" }, result.queryParams.StartTime.Unix())
	jsonObject.SetPath([]string{ "dsl", "end_time" }, result.queryParams.EndTime.Unix())
	jsonObject.Set("result", result.resultData)

	return jsonObject.MarshalJSON()
}

// Lists statistics data of ICMP, which would be grouped by provinces
func listIcmpByProvinces(ctx *context.Context) {
	defer outputJsonForPanic(ctx)

	dslParams, isValid := processDslAndOutputError(ctx, ctx.Input.Query("dsl"))
	if !isValid {
		return
	}

	listResult := nqmService.ListByProvinces(dslParams)

	ctx.Output.JSON(&resultWithDsl{ queryParams: dslParams, resultData: listResult }, jsonIndent, jsonCoding)
}

// Lists data of targets, which would be grouped by cities
func listIcmpByTargetsForAProvince(ctx *context.Context) {
	defer outputJsonForPanic(ctx)

	dslParams, isValid := processDslAndOutputError(ctx, ctx.Input.Query("dsl"))
	if !isValid {
		return
	}

	provinceId, _ := strconv.ParseInt(ctx.Input.Param(":province_id"), 10, 16)

	dslParams.AgentFilter.MatchProvinces = make([]string, 0) // Ignores the province of agent
	dslParams.AgentFilterById.MatchProvinces = []int16 { int16(provinceId) } // Use the id as the filter of agent
	listResult := nqmService.ListTargetsWithCityDetail(dslParams)
	ctx.Output.JSON(&resultWithDsl{ queryParams: dslParams, resultData: listResult }, jsonIndent, jsonCoding)
}

type jsonDslError struct {
	Code int `json:"error_code"`
	Message string `json:"error_message"`
}
func outputDslError(ctx *context.Context, err error) {
	ctx.Output.SetStatus(http.StatusBadRequest)
	ctx.Output.JSON(
		&jsonDslError {
			Code: 1,
			Message: err.Error(),
		}, jsonIndent, jsonCoding,
	)
}

// Used to output JSON message even if the execution is panic
func outputJsonForPanic(ctx *context.Context) {
	r := recover()
	if r == nil {
		return
	}

	if g.Config().Debug {
		debug.PrintStack()
	}

	log.Printf("Error on HTTP Request[%v/%v]. Error: %v", ctx.Input.Method(), ctx.Input.URI(), r)

	ctx.Output.SetStatus(http.StatusBadRequest)
	ctx.Output.JSON(&jsonDslError{
		Code: -1,
		Message: fmt.Sprintf("%v", r),
	}, jsonIndent, jsonCoding)
}

const (
	defaultDaysForTimeRange = 7
	after7Days = defaultDaysForTimeRange * 24 * time.Hour
	before7Days = after7Days * -1
)

// Process DSL and output error
// Returns: true if the DSL is valid
func processDslAndOutputError(ctx *context.Context, dslText string) (*dsl.QueryParams, bool) {
	dslParams, err := processDsl(dslText)

	if err != nil {
		outputDslError(ctx, err)
		return nil, false
	}

	return dslParams, true
}

// The query of DSL would be inner-province(used for phase 1)
func processDsl(dslParams string) (*dsl.QueryParams, error) {
	strNqmDsl := strings.TrimSpace(dslParams)

	/**
	 * If any of errors for parsing DSL
	 */
	result, parseError := dsl.Parse(
		"Query.nqmdsl", []byte(strNqmDsl),
	)
	if parseError != nil {
		return nil, parseError
	}
	// :~)

	resultDsl := result.(*dsl.QueryParams)

	setupTimeRange(resultDsl)
	setupInnerProvince(resultDsl)

	paramsError := resultDsl.CheckRationalOfParameters()
	if paramsError != nil {
		return nil, paramsError
	}

	return resultDsl, nil
}

// Sets-up the time range with provided-or-not value of parameters
// 1. Without any parameter of time range
// 2. Has only start time
// 3. Has only end time
func setupTimeRange(queryParams *dsl.QueryParams) {
	if queryParams.StartTime.IsZero() && queryParams.EndTime.IsZero() {
		now := time.Now()

		queryParams.StartTime = now.Add(before7Days)
		queryParams.EndTime = now
		return
	}

	if queryParams.StartTime.IsZero() && !queryParams.EndTime.IsZero() {
		queryParams.StartTime = queryParams.EndTime.Add(before7Days)
		return
	}

	if !queryParams.StartTime.IsZero() && queryParams.EndTime.IsZero() {
		queryParams.EndTime = queryParams.StartTime.Add(after7Days)
		return
	}
}

/**
 * !IMPORTANT!
 * This default value is just used in phase 1 funcion of NQM reporting(inner-province)
 */
func setupInnerProvince(queryParams *dsl.QueryParams) {
	queryParams.ProvinceRelation = dsl.SAME_VALUE
}
