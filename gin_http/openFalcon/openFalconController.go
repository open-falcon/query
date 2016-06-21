package openFalcon

import (
	"time"

	"strconv"

	"fmt"

	cmodel "github.com/Cepave/common/model"
	"github.com/Cepave/query/graph"
	"github.com/Cepave/query/logger"
	"github.com/Cepave/query/model"
	"github.com/gin-gonic/gin"
)

func GetEndpoints(c *gin.Context) {
	enps := model.EndpointQuery()
	c.JSON(200, gin.H{
		"status": "ok",
		"data": map[string][]string{
			"endpoints": enps,
		},
	})
}

func QDataGet(c *gin.Context) []*cmodel.GraphQueryResponse {
	log := logger.Logger()
	startTmp := c.DefaultQuery("startTs", string(time.Now().Unix()-(86400)))
	startTmp2, _ := strconv.Atoi(startTmp)
	startTs := int64(startTmp2)
	endTmp := c.DefaultQuery("endTs", string(time.Now().Unix()))
	endTmp2, _ := strconv.Atoi(endTmp)
	endTs := int64(endTmp2)
	consolFun := c.DefaultQuery("consolFun", "AVERAGE")
	stepTmp := c.DefaultQuery("step", "60")
	step, _ := strconv.Atoi(stepTmp)
	counter := c.DefaultQuery("counter", "cpu.idle")
	endpoints := model.EndpointQuery()
	var result []*cmodel.GraphQueryResponse
	for _, enp := range endpoints {
		q := cmodel.GraphQueryParam{
			Start:     startTs,
			End:       endTs,
			ConsolFun: consolFun,
			Step:      step,
			Endpoint:  enp,
			Counter:   counter,
		}
		res, _ := graph.QueryOne(q)
		log.Debug(fmt.Sprintf("%v, %v, %v", res.Counter, res.Endpoint, len(res.Values)))
		result = append(result, res)
	}
	log.Debug(fmt.Sprintf("%s: %d", "openfaclon query got", len(result)))
	return result
}

func QueryData(c *gin.Context) {
	result := QDataGet(c)
	c.JSON(200, gin.H{
		"status": "ok",
		"data":   result,
	})
}
