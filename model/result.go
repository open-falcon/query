package model

type Result struct {
	Dstype   string
	Step     int
	Endpoint string
	Counter  string
	Values   []*TimeSeriesData
}
