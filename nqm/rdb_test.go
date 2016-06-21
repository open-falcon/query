package nqm

import (
	qtest "github.com/Cepave/query/test"
	. "gopkg.in/check.v1"
)

type TestNqmRdbSuite struct{}

var _ = Suite(&TestNqmRdbSuite{})

func (s *TestNqmRdbSuite) SetUpSuite(c *C) {
	qtest.InitOrm()
	qtest.InitDb()
}

func (s *TestNqmRdbSuite) TearDownSuite(c *C) {
	qtest.ReleaseDb()
}

// Tests the getting of data for province by name
type getProvinceByNameTestCase struct {
	searchText string
	expectedId int32
	expectedName string
}
func (suite *TestNqmRdbSuite) TestGetProvinceByName(c *C) {
	testCases := []getProvinceByNameTestCase {
		{ "天津", 12, "天津" },
		{ "天", 12, "天津" },
		{ "無此省份", UNKNOWN_ID_FOR_QUERY, "無此省份" },
		{ "無此省份2", UNKNOWN_ID_FOR_QUERY, "無此省份2" },
		{ "贵州", 22, "贵州" },
	}

	for _, testCase := range testCases {
		testedProvince := getProvinceByName(testCase.searchText)

		c.Logf("Test load province by name \"%v\". Got: %v", testCase.searchText, testedProvince)

		c.Assert(testedProvince, NotNil)
		c.Assert(testedProvince.Id, Equals, int16(testCase.expectedId))
		c.Assert(testedProvince.Name, Equals, testCase.expectedName)
	}
}

// Tests the getting of data for province by id
type getProvinceByIdTestCase struct {
	searchId int16
	expectedId int16
	expectedName string
}
func (suite *TestNqmRdbSuite) TestGetProvinceById(c *C) {
	testCases := []getProvinceByIdTestCase {
		{ 12, 12, "天津" },
		{ 23, 23, "海南" },
		{ -1, -1, "<UNDEFINED>" },
		{ -919, -919, UNKNOWN_NAME_FOR_QUERY },
	}

	for _, testCase := range testCases {
		testedProvince := getProvinceById(testCase.searchId)

		c.Logf("Test load province by id \"%d\". Got: %v", testCase.searchId, testedProvince)

		c.Assert(testedProvince, NotNil)
		c.Assert(testedProvince.Id, Equals, int16(testCase.expectedId))
		c.Assert(testedProvince.Id, Equals, testCase.expectedId)
	}
}

// Tests the getting of data for ISP by name
type getIspByNameTestCase struct {
	searchText string
	expectedId int32
	expectedName string
}
func (suite *TestNqmRdbSuite) TestGetIspByName(c *C) {
	testCases := []getIspByNameTestCase {
		{ "方正", 8, "方正宽带" },
		{ "方正宽带", 8, "方正宽带" },
		{ "無此ISP", UNKNOWN_ID_FOR_QUERY, "無此ISP" },
		{ "無此ISP2", UNKNOWN_ID_FOR_QUERY, "無此ISP2" },
		{ "中信网络", 21, "中信网络" },
	}

	for _, testCase := range testCases {
		testedIsp := getIspByName(testCase.searchText)

		c.Logf("Test load ISP by name \"%v\". Got: %v", testCase.searchText, testedIsp)

		c.Assert(testedIsp, NotNil)
		c.Assert(testedIsp.Id, Equals, int16(testCase.expectedId))
		c.Assert(testedIsp.Name, Equals, testCase.expectedName)
	}
}

// Tests the getting of data for ISP by id
type getIspByIdTestCase struct {
	searchId int16
	expectedId int16
	expectedName string
}
func (suite *TestNqmRdbSuite) TestGetIspById(c *C) {
	testCases := []getIspByIdTestCase {
		{ 8, 8, "方正宽带" },
		{ 11, 11, "长城宽带"},
		{ -1, -1, "<UNDEFINED>" },
		{ -919, -919, UNKNOWN_NAME_FOR_QUERY},
	}

	for _, testCase := range testCases {
		testedIsp := getIspById(testCase.searchId)

		c.Logf("Test load ISP by id \"%d\". Got: %v", testCase.searchId, testedIsp)

		c.Assert(testedIsp, NotNil)
		c.Assert(testedIsp.Id, Equals, int16(testCase.expectedId))
		c.Assert(testedIsp.Id, Equals, testCase.expectedId)
	}
}

// Tests the getting of data for city by name
type getCityByNameTestCase struct {
	searchText string
	expectedId int32
	expectedName string
}
func (suite *TestNqmRdbSuite) TestGetCityByName(c *C) {
	testCases := []getCityByNameTestCase {
		{ "茂名", 20, "茂名市" },
		{ "株洲市", 32, "株洲市" },
		{ "無此city", UNKNOWN_ID_FOR_QUERY, "無此city" },
		{ "無此city2", UNKNOWN_ID_FOR_QUERY, "無此city2" },
	}

	for _, testCase := range testCases {
		testedCity := getCityByName(testCase.searchText)

		c.Logf("Test load city by name \"%v\". Got: %v", testCase.searchText, testedCity)

		c.Assert(testedCity, NotNil)
		c.Assert(testedCity.Id, Equals, int16(testCase.expectedId))
		c.Assert(testedCity.Name, Equals, testCase.expectedName)
	}
}

// Tests the getting of data for city by id
type getCityByIdTestCase struct {
	searchId int16
	expectedId int16
	expectedName string
}
func (suite *TestNqmRdbSuite) TestGetCityById(c *C) {
	testCases := []getCityByIdTestCase {
		{ 48, 48, "荆州市" },
		{ 33, 33, "娄底市"},
		{ -1, -1, "<UNDEFINED>" },
		{ -919, -919, UNKNOWN_NAME_FOR_QUERY},
	}

	for _, testCase := range testCases {
		testedCity := getCityById(testCase.searchId)

		c.Logf("Test load city by id \"%d\". Got: %v", testCase.searchId, testedCity)

		c.Assert(testedCity, NotNil)
		c.Assert(testedCity.Id, Equals, int16(testCase.expectedId))
		c.Assert(testedCity.Id, Equals, testCase.expectedId)
	}
}

// Tests the getting of data for target by id
type getTargetByIdTestCase struct {
	searchId int32
	expectedId int32
	expectedHost string
}
func (suite *TestNqmRdbSuite) TestGetTargetById(c *C) {
	testCases := []getTargetByIdTestCase {
		{ 19203, 19203, "100.20.50.3" },
		{ 19202, 19202, "100.20.50.2" },
		{ 28001, 28001, UNKNOWN_NAME_FOR_QUERY},
	}

	for _, testCase := range testCases {
		testedTarget := getTargetById(testCase.searchId)

		c.Logf("Test load target by id \"%v\". Got: %v", testCase.searchId, testedTarget)

		c.Assert(testedTarget, NotNil)
		c.Assert(testedTarget.Id, Equals, testCase.expectedId)
		c.Assert(testedTarget.Host, Equals, testCase.expectedHost)
	}
}

// Tests the getting of data for target by host
type getTargetByHostTestCase struct {
	searchText string
	expectedId int32
	expectedHost string
}
func (suite *TestNqmRdbSuite) TestGetTargetByHost(c *C) {
	testCases := []getTargetByHostTestCase {
		{ "100.20.50.1", 19201, "100.20.50.1" },
		{ "100.20.50.2", 19202, "100.20.50.2" },
		{ "無此target", UNKNOWN_ID_FOR_QUERY, "無此target" },
	}

	for _, testCase := range testCases {
		testedTarget := getTargetByHost(testCase.searchText)

		c.Logf("Test load target by host \"%v\". Got: %v", testCase.searchText, testedTarget)

		c.Assert(testedTarget, NotNil)
		c.Assert(testedTarget.Id, Equals, testCase.expectedId)
		c.Assert(testedTarget.Host, Equals, testCase.expectedHost)
	}
}

func (s *TestNqmRdbSuite) SetUpTest(c *C) {
	if !qtest.HasDefaultOrmOnPortal(c) {
		return
	}

	switch c.TestName() {
	case "TestNqmRdbSuite.TestGetTargetByHost",
		"TestNqmRdbSuite.TestGetTargetById":
		qtest.ExecuteQueriesOrFailInTx(
			`
			INSERT INTO nqm_target(tg_id, tg_name, tg_host) VALUES
			(19201, 'target-1', '100.20.50.1'),
			(19202, 'target-2', '100.20.50.2'),
			(19203, 'target-3', '100.20.50.3')
			`,
		)
	}
}

func (s *TestNqmRdbSuite) TearDownTest(c *C) {
	if !qtest.HasTestDbConfig() {
		return
	}

	switch c.TestName() {
	case "TestNqmRdbSuite.TestGetTargetByHost",
		"TestNqmRdbSuite.TestGetTargetById":
		qtest.ExecuteQueriesOrFailInTx(
			`DELETE FROM nqm_target WHERE tg_id IN (19201, 19202, 19203)`,
		)
	}
}
