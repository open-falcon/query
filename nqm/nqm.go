package nqm

import (
	"fmt"
	dsl "github.com/Cepave/query/dsl/nqm_parser" // As NQM intermediate representation
)

/**
 * Aliases of type for DSL
 */
type Id2Bytes int16
type EpochTime int64
// :~)

// This value is used to indicate the non-existing id for data(province, city, or ISP)
// Instead of -1(e.x. A agent doesn't has information of ISP), this value is used in query.
const UNKNOWN_ID_FOR_QUERY = -2
const UNKNOWN_NAME_FOR_QUERY = "<UNKNOWN>"

// Represents the DSL for query over Icmp log
type NqmDsl struct {
	GroupingColumns []string `json:"grouping_columns"`

    StartTime EpochTime `json:"start_time"`
	EndTime EpochTime `json:"end_time"`

	IdsOfAgentProvinces []Id2Bytes `json:"ids_of_agent_provinces"`
	IdsOfAgentIsps []Id2Bytes `json:"ids_of_agent_isps"`
	IdsOfTargetProvinces []Id2Bytes `json:"ids_of_target_provinces"`
	IdsOfTargetIsps []Id2Bytes `json:"ids_of_target_isps"`

	ProvinceRelation dsl.HostRelation `json:"province_relation"`
}

// The data used for reporting of ICMP statistics(grouping by provinces of agents)
type ProvinceMetric struct {
	Province *Province `json:"province"`
	Metrics *Metrics `json:"metrics"`
}

// The data used for reporting of ICMP statistics, which contains detail of target node(grouping by city)
type CityMetric struct {
	City *City `json:"city"`
	Metrics *Metrics `json:"metrics"`
	Targets []TargetMetric `json:"targets"`
}

// The data used for reporting of ICMP statistics target node
type TargetMetric struct {
	Id int32 `json:"id"`
	Host string `json:"host"`
	Isp *Isp `json:"isp"`
	Metrics *Metrics `json:"metrics"`
}

/**
 * 1. Main controller for NQM reporting
 * 2. Makes the unit test more easily(by replacing lambda used in method)
 */
type ServiceController struct {
	GetStatisticsOfIcmpByDsl func(*NqmDsl) ([]IcmpResult, error)
	GetProvinceById func(int16) *Province
	GetProvinceByName func(string) *Province
	GetIspById func(int16) *Isp
	GetIspByName func(string) *Isp
	GetCityById func(int16) *City
	GetCityByName func(string) *City
	GetTargetById func(int32) *Target
	GetTargetByHost func(string) *Target
}

var defaultServiceController = ServiceController{
	GetStatisticsOfIcmpByDsl: getStatisticsOfIcmpByDsl,
	GetProvinceById: getProvinceById,
	GetProvinceByName: getProvinceByName,
	GetIspById: getIspById,
	GetIspByName: getIspByName,
	GetCityById: getCityById,
	GetCityByName: getCityByName,
	GetTargetById: getTargetById,
	GetTargetByHost: getTargetByHost,
}
func GetDefaultServiceController() ServiceController {
	return defaultServiceController
}
// :~)

// Initilaize the service
func (srv ServiceController) Init() {
	initIcmp()
}

// Query data for provinces
func (srv ServiceController) ListByProvinces(dslParams *dsl.QueryParams) []ProvinceMetric {
	/**
	 * 1. Set-up the grouping column
	 * 2. Only for inter-province
	 */
	nqmDsl := toNqmDsl(dslParams)
	nqmDsl.GroupingColumns = []string { "ib_ag_pv_id" }
	nqmDsl.ProvinceRelation = dsl.SAME_VALUE
	// :~)

	/**
	 * Loads statistics of ICMP
	 */
	rawIcmpData, err := srv.GetStatisticsOfIcmpByDsl(nqmDsl)
	if err != nil {
		panic(err)
	}
	// :~)

	/**
	 * Join data of provinces
	 */
	result := make([]ProvinceMetric, len(rawIcmpData))
	for i, v := range rawIcmpData {
		currentMetric := ProvinceMetric{}

		province := srv.GetProvinceById(int16(v.grouping[0]))

		currentMetric.Province = province
		currentMetric.Metrics = v.metrics

		result[i] = currentMetric
	}
	// :~)

	return result
}

// Query data for detail of city
func (srv ServiceController) ListTargetsWithCityDetail(dslParams *dsl.QueryParams) []CityMetric {
	/**
	 * Loads data with grouping by id of cities
	 */
	dslGroupByCity := toNqmDsl(dslParams)
	dslGroupByCity.GroupingColumns = []string { "ib_tg_ct_id" }
	dslGroupByCity.ProvinceRelation = dsl.SAME_VALUE
	rawIcmpGroupByCity, errForCityReport := srv.GetStatisticsOfIcmpByDsl(dslGroupByCity)
	if errForCityReport != nil {
		panic(errForCityReport)
	}
	// :~)

	result := make([]CityMetric, 0, len(rawIcmpGroupByCity))

	/**
	 * Initialize map for city metrics
	 */
	idToCityMetrics := make(map[int16]*CityMetric)

	for i, rowByCity := range rawIcmpGroupByCity {
		cityId := int16(rowByCity.grouping[0])

		result = append(
			result,
			CityMetric{
				City: srv.GetCityById(cityId),
				Metrics: rowByCity.metrics,
				Targets: make([]TargetMetric, 0),
			},
		)
		idToCityMetrics[cityId] = &result[i]
	}
	// :~)

	/**
	 * Loads data with grouping by id of targets
	 */
	dslGroupByTarget := toNqmDsl(dslParams)
	dslGroupByTarget.GroupingColumns = []string { "ib_tg_id", "ib_tg_ct_id", "ib_tg_isp_id" }
	rawIcmpGroupByTarget, errForTargetReport := srv.GetStatisticsOfIcmpByDsl(dslGroupByTarget)
	if errForTargetReport != nil {
		panic(errForTargetReport)
	}
	// :~)

	/**
	 * Collects the list of targets into matched city record
	 */
	for _, rowByTarget := range rawIcmpGroupByTarget {
		targetId := rowByTarget.grouping[0]
		cityId := int16(rowByTarget.grouping[1])
		ispId := int16(rowByTarget.grouping[2])

		cityRow, assertExisting := idToCityMetrics[cityId]
		if !assertExisting {
			panic(fmt.Errorf("Cannot find city[id: %v] for target row[target id: %v]", cityId, targetId))
		}

		// Loads data of target
		targetNode := srv.GetTargetById(targetId)

		/**
		 * Appends the found target
		 */
		cityRow.Targets = append(
			cityRow.Targets,
			TargetMetric {
				Id: targetNode.Id,
				Host: targetNode.Host,
				Isp: srv.GetIspById(ispId),
				Metrics: rowByTarget.metrics,
			},
		)
		// :~)
	}
	// :~)

	return result
}

// Converts the IR of DSL to specific data for query on Cassandra
func toNqmDsl(queryParams *dsl.QueryParams) *NqmDsl {
	return &NqmDsl{
		IdsOfAgentProvinces: loadIds(queryParams.AgentFilter.MatchProvinces, getIdOfProvinceByName, queryParams.AgentFilterById.MatchProvinces),
		IdsOfAgentIsps: loadIds(queryParams.AgentFilter.MatchIsps, getIdOfIspByName, queryParams.AgentFilterById.MatchIsps),
		IdsOfTargetProvinces: loadIds(queryParams.TargetFilter.MatchProvinces, getIdOfProvinceByName, queryParams.TargetFilterById.MatchProvinces),
		IdsOfTargetIsps: loadIds(queryParams.TargetFilter.MatchIsps, getIdOfIspByName, queryParams.TargetFilterById.MatchIsps),
		StartTime: EpochTime(queryParams.StartTime.Unix()),
		EndTime: EpochTime(queryParams.EndTime.Unix()),
		ProvinceRelation: queryParams.ProvinceRelation,
	}
}

func loadIds(
	queryNames []string, loadIdFunc getIdFunc,
	additionalIds []int16,
) []Id2Bytes {
	uniqueIds := make(map[Id2Bytes]bool)

	/**
	 * Loads ids from text of search condition
	 */
	for _, name := range queryNames {
		uniqueIds[loadIdFunc(name)] = true
	}
	// :~)

	/**
	 * Loads ids from explicit value of id
	 */
	for _, id := range additionalIds {
		uniqueIds[Id2Bytes(id)] = true
	}
	// :~)

	/**
	 * Transfers the map(unique ids) to result
	 */
	result := make([]Id2Bytes, 0, len(uniqueIds))
	for id, _ := range uniqueIds {
		result = append(result, id)
	}
	// :~)

	return result
}

/**
 * Quick method for loader
 */
type getIdFunc func(string) Id2Bytes

func getIdOfProvinceByName(name string) Id2Bytes {
	return Id2Bytes(getProvinceByName(name).Id)
}
func getIdOfIspByName(name string) Id2Bytes {
	return Id2Bytes(getIspByName(name).Id)
}
// :~)
