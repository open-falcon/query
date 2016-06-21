package database

import (
	"log"

	"github.com/Cepave/query/g"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

var (
	db  *gorm.DB
	err error
)

func DBConn() *gorm.DB {
	return db
}

func Init() {
	conf := g.Config()
	db, err = gorm.Open("mysql", conf.GraphDB.Addr)
	if err != nil {
		log.Println(err.Error())
	}
}
