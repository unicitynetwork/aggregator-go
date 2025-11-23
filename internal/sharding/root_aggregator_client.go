package sharding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type (
	RootAggregatorClient struct {
		rpcURL     string
		httpClient *http.Client
		stateIDC   *atomic.Int64
	}

	jsonRpcRequest struct {
		JsonRpc string      `json:"jsonrpc"`
		Method  string      `json:"method"`
		Params  interface{} `json:"params"`
		ID      int64       `json:"id"`
	}

	jsonRpcResponse[T any] struct {
		JsonRpc string        `json:"jsonrpc"`
		Result  *T            `json:"result,omitempty"`
		Error   *jsonRpcError `json:"error,omitempty"`
		ID      int64         `json:"id"`
	}

	jsonRpcError struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    string `json:"data,omitempty"`
	}
)

func NewRootAggregatorClient(rpcURL string) *RootAggregatorClient {
	return &RootAggregatorClient{
		rpcURL:     rpcURL,
		httpClient: &http.Client{},
		stateIDC:   new(atomic.Int64),
	}
}

func (c *RootAggregatorClient) SubmitShardRoot(ctx context.Context, req *api.SubmitShardRootRequest) error {
	result, err := doRpcRequest[api.SubmitShardRootResponse](ctx, c, "submit_shard_root", req)
	if err != nil {
		return fmt.Errorf("failed to submit shard root: %w", err)
	}
	if result.Status != api.ShardRootStatusSuccess {
		return fmt.Errorf("unexpected status: %s", result.Status)
	}
	return nil
}

func (c *RootAggregatorClient) GetShardProof(ctx context.Context, req *api.GetShardProofRequest) (*api.RootShardInclusionProof, error) {
	response, err := doRpcRequest[api.RootShardInclusionProof](ctx, c, "get_shard_proof", req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch shard proof: %w", err)
	}
	return response, nil
}

func doRpcRequest[T any](ctx context.Context, c *RootAggregatorClient, method string, params interface{}) (*T, error) {
	rpcReq := jsonRpcRequest{
		JsonRpc: "2.0",
		Method:  method,
		Params:  params,
		ID:      c.stateIDC.Add(1),
	}

	reqBody, err := json.Marshal(rpcReq)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.rpcURL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("http error: %s, body: %s", resp.Status, body)
	}

	var rpcResp jsonRpcResponse[T]
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("rpc error: %s", rpcResp.Error.Message)
	}
	if rpcResp.Result == nil {
		return nil, fmt.Errorf("rpc error: result is nil")
	}
	return rpcResp.Result, nil
}
