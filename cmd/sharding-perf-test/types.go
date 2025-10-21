package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type JSONRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      int         `json:"id"`
}

type JSONRPCResponse struct {
	JSONRPC string        `json:"jsonrpc"`
	Result  interface{}   `json:"result,omitempty"`
	Error   *JSONRPCError `json:"error,omitempty"`
	ID      int           `json:"id"`
}

type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

// Use the public API types

type GetBlockResponse struct {
	Block Block `json:"block"`
}

type Block struct {
	Index     string `json:"index"`
	Timestamp string `json:"timestamp"`
}

type GetBlockCommitmentsRequest struct {
	BlockNumber string `json:"blockNumber"`
}

type GetBlockCommitmentsResponse struct {
	Commitments []AggregatorRecord `json:"commitments"`
}

type AggregatorRecord struct {
	RequestID   string `json:"requestId"`
	BlockNumber string `json:"blockNumber"`
}

type GetBlockHeightResponse struct {
	BlockNumber string `json:"blockNumber"`
}

type requestInfo struct {
	URL   string
	Found int32 // 0 for not found, 1 for found
}

// ShardMetrics holds all the counters and data for a single shard.
type ShardMetrics struct {
	totalRequests         int64
	successfulRequests    int64
	failedRequests        int64
	requestIdExistsErr    int64
	blockCommitmentCounts []int
	totalBlockCommitments int64
	mutex                 sync.RWMutex
}

// Metrics holds all the metrics for the performance test.
type Metrics struct {
	startTime            time.Time
	startingBlockNumbers map[string]int64
	submittedRequestIDs  sync.Map
	shardMetrics         map[string]*ShardMetrics
	mutex                sync.RWMutex
}

func (sm *ShardMetrics) addBlockCommitmentCount(count int) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.blockCommitmentCounts = append(sm.blockCommitmentCounts, count)
}

type JSONRPCClient struct {
	httpClient *http.Client
	url        string
	authHeader string
	requestID  int64
}

func NewJSONRPCClient(url string, authHeader string) *JSONRPCClient {
	// Configure transport with higher connection limits
	transport := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		MaxConnsPerHost:     1000,
		IdleConnTimeout:     90 * time.Second,
	}

	return &JSONRPCClient{
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
		url:        url,
		authHeader: authHeader,
	}
}

func (c *JSONRPCClient) call(method string, params interface{}) (*JSONRPCResponse, error) {
	id := atomic.AddInt64(&c.requestID, 1)

	request := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      int(id),
	}

	reqBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", c.url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Add auth header if provided
	if c.authHeader != "" {
		req.Header.Set("Authorization", c.authHeader)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	var response JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}
