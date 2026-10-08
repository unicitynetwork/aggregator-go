package sharding

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

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
			ParentFragment: &api.ParentInclusionFragment{
				CertificateBytes: api.NewHexBytes(make([]byte, api.BitmapSize)),
				ShardLeafValue:   api.HexBytes("0x1234"),
			},
			BlockNumber:        9,
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
	require.Equal(t, api.HexBytes("0x1234"), proof.ParentFragment.ShardLeafValue)
	require.Equal(t, uint64(9), proof.BlockNumber)
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

func TestRootAggregatorClient_CheckHealth_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/health", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewRootAggregatorClient(server.URL)
	require.NoError(t, client.CheckHealth(context.Background()))
}

func TestRootAggregatorClient_CheckHealth_Failure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "parent down", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewRootAggregatorClient(server.URL)
	err := client.CheckHealth(context.Background())
	require.ErrorContains(t, err, "parent health")
}

// The stub parent's proofs carry the reference time of the round they certify and, in the seal, the time the child's next round pins.
// By default successive proofs advance a second; a same-second step reproduces a live parent certifying within one second.
func TestRootAggregatorClientStub_ReferenceStep(t *testing.T) {
	proofTimes := func(stub *RootAggregatorClientStub) (inputs, seals []uint64) {
		require.NoError(t, stub.SubmitShardRoot(t.Context(), &api.SubmitShardRootRequest{ShardID: 4, RootHash: api.NewHexBytes([]byte{1, 2, 3})}))
		for i := 0; i < 3; i++ {
			proof, err := stub.GetShardProof(t.Context(), &api.GetShardProofRequest{ShardID: 4})
			require.NoError(t, err)
			var uc types.UnicityCertificate
			require.NoError(t, types.Cbor.Unmarshal(proof.UnicityCertificate, &uc))
			inputs, seals = append(inputs, uc.InputRecord.Timestamp), append(seals, uc.UnicitySeal.Timestamp)
		}
		return inputs, seals
	}

	inputs, seals := proofTimes(NewRootAggregatorClientStub())
	for i := range inputs {
		require.Equal(t, inputs[i]+1, seals[i], "default: the seal is one second past the round")
		if i > 0 {
			require.Equal(t, inputs[i-1]+1, inputs[i], "default: each proof advances one second")
		}
	}

	inputs, seals = proofTimes(NewRootAggregatorClientStub().WithReferenceStep(func(p uint64) uint64 { return p }))
	for i := range inputs {
		require.Equal(t, inputs[0], inputs[i], "same second: every proof pins the same reference time")
		require.Equal(t, inputs[i], seals[i], "same second: the seal names the same second")
	}
}
