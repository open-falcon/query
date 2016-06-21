package model

import (
	"time"

	"fmt"

	"github.com/Cepave/query/database"
	"github.com/Cepave/query/g"
)

type Endpoint struct {
	Id       int64     `json:"id"`
	Endpoint string    `json:"endpoint"`
	Ts       int64     `json:"ts"`
	TCreate  time.Time `json:"-"`
	TModify  time.Time `json:"-"`
	Ipv4     string    `json:"-"`
}

func EndpointQuery() (endpointList []string) {
	database.Init()
	db := database.DBConn()
	gconf := g.Config()
	var enps []Endpoint
	var sqlStr string
	if gconf.GraphDB.Limit == -1 {
		sqlStr = "SELECT * from graph.endpoint"
	} else {
		sqlStr = fmt.Sprintf("SELECT * from graph.endpoint limit %v", gconf.GraphDB.Limit)
	}
	db.Raw(sqlStr).Scan(&enps)
	if len(enps) != 0 {
		for _, host := range enps {
			endpointList = append(endpointList, host.Endpoint)
		}
	}
	return
}
