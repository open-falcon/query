package conf

import (
	"log"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestConfRead(t *testing.T) {
	ReadConf("./lambdaSetup.json")
	c := Config()
	testfunname := "avgCompare"
	Convey("ReadFuncation", t, func() {
		Convey("ReadJsonConfTest", func() {
			So(c["avgCompare"].FuncationName, ShouldNotBeEmpty)
			log.Printf("%v", c["top"].Params)
			So(len(c[testfunname].Params), ShouldBeGreaterThan, -1)
			So(c["top"].Params[0], ShouldEqual, "limit:int")
		})
	})

	Convey("JsReadFuncation", t, func() {
		Convey("ReadJsonFileTest", func() {
			contain := jsFileReader("../js/avgCompare.js")
			So(contain, ShouldNotBeEmpty)
		})
		Convey("GetAvaibleFun", func() {
			funcations := GetAvaibleFun()
			So(len(funcations), ShouldBeGreaterThan, 0)
		})
	})

}
