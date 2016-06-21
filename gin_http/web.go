package ginHttp

import (
	"time"

	"log"

	cmodel "github.com/Cepave/common/model"

	"github.com/Cepave/query/g"
	"github.com/Cepave/query/gin_http/computeFunc"
	"github.com/Cepave/query/gin_http/openFalcon"
	"github.com/gin-gonic/gin"
)

type QueryInput struct {
	StartTs       time.Time
	EndTs         time.Time
	ComputeMethod string
	Endpoint      string
	Counter       string
}

//this function will generate query string obj for QueryRRDtool
func getq(q QueryInput) cmodel.GraphQueryParam {
	request := cmodel.GraphQueryParam{
		Start:     q.StartTs.Unix(),
		End:       q.EndTs.Unix(),
		ConsolFun: q.ComputeMethod,
		Endpoint:  q.Endpoint,
		Counter:   q.Counter,
	}
	return request
}

//accept cross domain request
func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func StartWeb(ginMsg chan<- string) {

	defer func() {
		ginMsg <- "gin"
	}()

	handler := gin.Default()
	handler.Use(CORSMiddleware())
	compute := handler.Group("/func")
	conf := g.Config()
	compute.GET("/compute", computeFunc.Compute)
	compute.GET("/funcations", computeFunc.GetAvaibleFun)
	compute.GET("/smapledata", computeFunc.GetTestData)
	openfalcon := handler.Group("/owl")
	openfalcon.GET("/endpoints", openFalcon.GetEndpoints)
	openfalcon.GET("/queryrrd", openFalcon.QueryData)
	log.Println("open gin port on:", conf.GinHttp.Listen)
	handler.Run(conf.GinHttp.Listen)
}
