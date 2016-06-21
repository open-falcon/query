package grpc

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"regexp"

	cmodel "github.com/Cepave/common/model"
	"github.com/Cepave/query/g"
	"github.com/Cepave/query/graph"
	pb "github.com/Cepave/query/grpc/proto/owlquery"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type server struct{}

//this function will generate query string obj for QueryRRDtool
func getq(q *pb.QueryInput) cmodel.GraphQueryParam {
	request := cmodel.GraphQueryParam{
		Start:     int64(q.StartTs),
		End:       int64(q.EndTs),
		ConsolFun: q.ComputeMethod,
		Endpoint:  q.Endpoint,
		Counter:   q.Counter,
	}
	return request
}

func stringToarry(str string) (res []string, err error) {
	if check, _ := regexp.Match("^\\s*\\[[\\w\\W]+\\]\\s*$", []byte(str)); check {
		repat1, _ := regexp.Compile("(^\\s*\\[\\s*\"|\"\\s*\\]\\s*$)")
		splpat, _ := regexp.Compile("\"\\s*,\\s*\"")
		str = repat1.ReplaceAllString(str, "")
		res = splpat.Split(str, -1)
	} else {
		err = fmt.Errorf("can not cast %v to array", str)
	}
	return
}

func getEndPoints(endpointVal string) (endpointList []string, err error) {
	if check, _ := regexp.Match("^\\s*\\[[\\w\\W]+\\]\\s*$", []byte(endpointVal)); check {
		endpointList, err = stringToarry(endpointVal)
		if err != nil {
			log.Printf("%v", err.Error())
		}
	} else {
		endpointList = append(endpointList, endpointVal)
	}
	return
}

func getCounter(counterVal string) (counterList []string, err error) {
	if check, _ := regexp.Match("^\\s*\\[[\\w\\W]+\\]\\s*$", []byte(counterVal)); check {
		counterList, err = stringToarry(counterVal)
		if err != nil {
			log.Printf("%v", err.Error())
		}
	} else {
		counterList = append(counterList, counterVal)
	}
	return
}

func rrdQuery(in *pb.QueryInput) (resp []*cmodel.GraphQueryResponse) {
	queryParams := getq(in)
	endpointList, _ := getEndPoints(queryParams.Endpoint)
	counterList, _ := getCounter(queryParams.Counter)
	for _, enp := range endpointList {
		queryParams.Endpoint = enp
		for _, con := range counterList {
			queryParams.Counter = con
			res, err := graph.QueryOne(queryParams)
			if err != nil {
				log.Printf("%v", err.Error())
			}
			resp = append(resp, res)
		}
	}
	return
}

func (s *server) Query(ctx context.Context, in *pb.QueryInput) (*pb.QueryReply, error) {
	resTmp := rrdQuery(in)

	// When Values is empty will generate null in jsonMarshal
	// This will terms "null" into "[]"
	for idx, result := range resTmp {
		if result.Values == nil {
			result.Values = []*cmodel.RRDData{}
		}
		resTmp[idx] = result
	}

	res, _ := json.Marshal(resTmp)
	return &pb.QueryReply{Result: string(res)}, nil
}

func Start(grpcMsg chan<- string) {

	defer func() {
		grpcMsg <- "grpc"
	}()

	port := fmt.Sprintf(":%v", g.Config().Grpc.Port)
	log.Printf("start grpc in port %v ..", port)
	//queryrrd(1452806153, 1452827753, "AVERAGE", "docker-agent", "cpu.idle")
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterOwlQueryServer(s, &server{})
	s.Serve(lis)
}
