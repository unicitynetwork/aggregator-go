package sharding

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestRootAggregatorClient_SubmitShardRoot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRpcRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		require.Equal(t, "2.0", req.JsonRpc)
		require.Equal(t, int64(1), req.ID)
		require.Equal(t, "submit_shard_root", req.Method)

		params := req.Params.(map[string]interface{})
		require.Equal(t, float64(4), params["shardId"])
		require.Equal(t, "010203", params["rootHash"])

		resp := jsonRpcResponse[api.SubmitShardRootResponse]{
			JsonRpc: "2.0",
			Result:  &api.SubmitShardRootResponse{Status: api.ShardRootStatusSuccess},
			ID:      req.ID,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewRootAggregatorClient(server.URL)
	rootHash, err := api.NewHexBytesFromString("010203")
	require.NoError(t, err)
	req := &api.SubmitShardRootRequest{
		ShardID:  4,
		RootHash: rootHash,
	}
	err = client.SubmitShardRoot(context.Background(), req)
	require.NoError(t, err)
}

func TestRootAggregatorClient_GetShardProof(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRpcRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)

		require.Equal(t, "2.0", req.JsonRpc)
		require.Equal(t, "get_shard_proof", req.Method)

		params := req.Params.(map[string]interface{})
		require.Equal(t, float64(4), params["shardId"])

		proof := &api.RootShardInclusionProof{
			MerkleTreePath:     &api.MerkleTreePath{Root: "0x1234"},
			UnicityCertificate: api.HexBytes("0xabcdef"),
		}

		resp := jsonRpcResponse[api.RootShardInclusionProof]{
			JsonRpc: "2.0",
			Result:  proof,
			ID:      req.ID,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewRootAggregatorClient(server.URL)
	req := &api.GetShardProofRequest{
		ShardID: 4,
	}
	proof, err := client.GetShardProof(context.Background(), req)

	require.NoError(t, err)
	require.NotNil(t, proof)
	require.Equal(t, "0x1234", proof.MerkleTreePath.Root)
	require.Equal(t, api.HexBytes("0xabcdef"), proof.UnicityCertificate)
}

func TestRootAggregatorClient_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	client := NewRootAggregatorClient(server.URL)
	_, err := doRpcRequest[api.RootShardInclusionProof](context.Background(), client, "get_shard_proof", &api.GetShardProofRequest{ShardID: 1})
	require.ErrorContains(t, err, "http error")
}

func TestRootAggregatorClient_RPCErrorResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := jsonRpcResponse[api.SubmitShardRootResponse]{
			JsonRpc: "2.0",
			Error: &jsonRpcError{
				Code:    -32000,
				Message: "invalid shard",
				Data:    "shard not found",
			},
			ID: 1,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewRootAggregatorClient(server.URL)
	err := client.SubmitShardRoot(context.Background(), &api.SubmitShardRootRequest{ShardID: 99})
	require.ErrorContains(t, err, "rpc error")
}

func TestRootAggregatorClient_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("{not-json"))
	}))
	defer server.Close()

	client := NewRootAggregatorClient(server.URL)
	_, err := client.GetShardProof(context.Background(), &api.GetShardProofRequest{ShardID: 5})
	require.ErrorContains(t, err, "decode response")
}
