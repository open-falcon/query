package test

import (
	"errors"
	"flag"
	"log"
	"net/http/httptest"
    "github.com/gorilla/rpc/v2"
    "github.com/gorilla/rpc/v2/json2"
    "github.com/Cepave/query/jsonrpc"
	rpchttp "github.com/gorilla/http"
	. "gopkg.in/check.v1"
	"net"
)

var mockRpcPort = flag.String("test.mockrpc.port", "6173", "HTTP port for mock JSONRPC of NQM log")
var testHttpServer *httptest.Server = nil
var rpcServiceCaller *jsonrpc.JsonRpcService = nil

// Initialize mock JSONRPC server for NQM log
// You may use "test.mockrpc.port" to customize port of HTTP for it
func StartMockJsonRpcServer(c *C, jsonrpcServiceSetupFunc func(*rpc.Server)) {
	if testHttpServer != nil {
		return
	}

	flag.Parse()

	jsonrpcService := rpc.NewServer()
	jsonrpcService.RegisterCodec(json2.NewCodec(), "application/json")
	jsonrpcServiceSetupFunc(jsonrpcService)

	/**
	 * Set-up HTTP server for testing
	 */
	testHttpServer = httptest.NewUnstartedServer(jsonrpcService)
	listener, err := net.Listen("tcp", "127.0.0.1:" + *mockRpcPort)
	if err != nil {
		panic(err)
	}
	testHttpServer.Listener = listener
	testHttpServer.Start()
	// :~)

	rpcServiceCaller = jsonrpc.NewService(testHttpServer.URL)

	c.Logf("Test HTTP Server: \"%v\"", testHttpServer.URL);
}

var httpClient = &rpchttp.DefaultClient

// Gets URL of mocked server
func GetUrlOfMockedServer() string {
	if testHttpServer == nil {
		panic("Un-initialized mocked HTTP server")
	}

	return testHttpServer.URL
}

// Calls the JSONRPC service provided by StartMockJsonRpcServer
func CallMockJsonRpc(
	method string, params interface{}, reply interface{},
) error {
	if testHttpServer == nil {
		return errors.New("The HTTP Server for JSONRPC is not initialized")
	}

	httpInfo, err := rpcServiceCaller.CallMethod(
		method, params, reply,
	)
	if err != nil {
		log.Printf("JSONRPC has error: %v. HTTP Status: [%v]. Headers: %v", err, httpInfo.Status, httpInfo.Headers)
	}

	return err
}

// Stops the HTTP server for mocked JSONRPC services
func StopMockJsonRpcServer(c *C) {
	if testHttpServer != nil {
		testHttpServer.Close()
		testHttpServer = nil
	}
}
