package jsonrpc

import (
	"fmt"
	rpchttp "github.com/gorilla/http"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/gorilla/http/client"
	"strings"
)

// Represents the service information of JSONRPC(2.0)
type JsonRpcService struct {
	url string
	httpClient *rpchttp.Client
}

// Represents the information of HTTP responsed on JSONRPC service
type JsonRpcHttpInfo struct {
	Status client.Status
	Headers map[string][]string
}

// Initialize a service for JSONRPC(2.0)
func NewService(url string) *JsonRpcService {
	return &JsonRpcService {
		url: url,
		httpClient: &rpchttp.DefaultClient,
	}
}

// Calls method of JSONRPC
func (service *JsonRpcService) CallMethod(method string, params interface{}, reply interface{}) (*JsonRpcHttpInfo, error) {
	jsonrpcRequest, jsonrpcErr := json2.EncodeClientRequest(method, params)
	if jsonrpcErr != nil {
		return nil, fmt.Errorf("Encode request of JSONRPC(2.0) has error: %v", jsonrpcErr)
	}

	/**
	 * Calls the method to service
	 */
	status, headers, body, err := service.httpClient.Post(
		service.url,
		map[string][]string { "Content-Type": []string { "application/json" } },
		strings.NewReader(string(jsonrpcRequest)),
	)

	if body != nil {
		defer body.Close()
	}

	/**
	 * Binds the information of HTTP response
	 */
	httpInfo := JsonRpcHttpInfo {
		Status: status,
		Headers: headers,
	}
	// :~)

	if err != nil {
		return &httpInfo, fmt.Errorf("Call method[%v, %v] has error: %v", method, service.url, err)
	}
	// :~)

	/**
	 * Unmarshal the response of JSONRPC
	 */
	err = json2.DecodeClientResponse(body, reply)
	if err != nil {
		return &httpInfo, fmt.Errorf("Decode response has error: %v. Method[%v, %v]", err, method, service.url)
	}
	// :~)

	return &httpInfo, nil
}
