package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

func batchServer(t *testing.T, mode config.ShardingMode, h jsonrpc.HandlerFunc) *jsonrpc.Server {
	var srv *Server
	rpc := newRPCTestServer(t, fixedV2Service{resp: v2Fixture()}, func(s *Server) {
		s.config = &config.Config{}
		s.config.Sharding.Mode = mode
		s.SetBatchProofHandler(h)
		srv = s
	})
	_ = srv
	return rpc
}

func echoHandler(called *int) jsonrpc.HandlerFunc {
	return func(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
		*called++
		return &api.GetInclusionProofsResponseV1{Proofs: []*api.GetInclusionProofResponseV2{v2Fixture()}}, nil
	}
}

func TestBatchRPC_SuccessEnvelopeAndV2Element(t *testing.T) {
	var n int
	rpc := batchServer(t, config.ShardingModeStandalone, echoHandler(&n))
	_, body := postRPC(t, rpc, `{"jsonrpc":"2.0","id":7,"method":"get_inclusion_proofs.v1","params":{"stateIds":["`+strings.Repeat("ab", 32)+`"]}}`)
	var env struct {
		Result struct {
			Proofs []json.RawMessage `json:"proofs"`
		} `json:"result"`
		ID int `json:"id"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &env))
	require.Equal(t, 7, env.ID)
	require.Len(t, env.Result.Proofs, 1)
	// the element is exactly what get_inclusion_proof.v2 returns as its result
	_, v2 := postRPC(t, rpc, `{"jsonrpc":"2.0","id":7,"method":"get_inclusion_proof.v2","params":{"stateId":"`+strings.Repeat("ab", 32)+`"}}`)
	require.Equal(t, goldenV2Success, v2)
	want := strings.TrimSuffix(strings.TrimPrefix(goldenV2Success, `{"jsonrpc":"2.0","result":`), `,"id":7}`+"\n")
	require.Equal(t, want, string(env.Result.Proofs[0]))
	require.NotContains(t, body, "partitionView")
}

func TestBatchRPC_RequestAndIDLimits(t *testing.T) {
	var n int
	rpc := batchServer(t, config.ShardingModeStandalone, echoHandler(&n))
	ids := `"` + strings.Repeat("ab", 32) + `"`

	// whole request larger than 64 KiB (params themselves under 32 KiB)
	pad := strings.Repeat(" ", api.InclusionProofsMaxRequestBytes)
	_, body := postRPC(t, rpc, `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proofs.v1",`+pad+`"params":{"stateIds":[`+ids+`]}}`)
	require.Contains(t, body, `"code":-32007`)
	require.Contains(t, body, `"detail":"request_bytes"`)
	require.Contains(t, body, fmt.Sprintf(`"limit":%d`, api.InclusionProofsMaxRequestBytes))

	// params larger than 32 KiB inside a request below... (params dominate)
	big := strings.Repeat("a", api.InclusionProofsMaxParamsBytes)
	_, body = postRPC(t, rpc, `{"jsonrpc":"2.0","id":2,"method":"get_inclusion_proofs.v1","params":{"stateIds":[`+ids+`],"p":"`+big+`"}}`)
	require.Contains(t, body, `"code":-32007`)
	require.Contains(t, body, fmt.Sprintf(`"limit":%d`, api.InclusionProofsMaxParamsBytes))

	// ID over 128 encoded bytes is invalid and not echoed
	_, body = postRPC(t, rpc, `{"jsonrpc":"2.0","id":"`+strings.Repeat("x", 200)+`","method":"get_inclusion_proofs.v1","params":{"stateIds":[`+ids+`]}}`)
	require.Contains(t, body, `"code":-32602`)
	require.Contains(t, body, `"detail":"invalid_id"`)
	require.NotContains(t, body, "xxxx")
	require.Zero(t, n, "the handler never ran for rejected requests")

	// a 128-byte encoded ID is accepted
	_, body = postRPC(t, rpc, `{"jsonrpc":"2.0","id":"`+strings.Repeat("x", 126)+`","method":"get_inclusion_proofs.v1","params":{"stateIds":[`+ids+`]}}`)
	require.Contains(t, body, `"result"`)
}

func TestBatchRPC_ResponseLimitBuffersAndReplaces(t *testing.T) {
	huge := func(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
		return map[string]string{"proofs": strings.Repeat("a", api.InclusionProofsMaxResponseBytes)}, nil
	}
	rpc := batchServer(t, config.ShardingModeStandalone, huge)
	code, body := postRPC(t, rpc, `{"jsonrpc":"2.0","id":9,"method":"get_inclusion_proofs.v1","params":{"stateIds":["`+strings.Repeat("ab", 32)+`"]}}`)
	require.Equal(t, 200, code)
	require.Equal(t, `{"jsonrpc":"2.0","error":{"code":-32007,"message":"Limit exceeded","data":{"reason":"limit_exceeded","detail":"response_bytes","retryable":false,"limit":8388608}},"id":9}`+"\n", body)
}

func TestBatchRPC_UnsupportedConfigurationAndUnregistered(t *testing.T) {
	var n int
	for _, mode := range []config.ShardingMode{config.ShardingModeParent, config.ShardingModeChild} {
		rpc := batchServer(t, mode, echoHandler(&n))
		_, body := postRPC(t, rpc, `{"jsonrpc":"2.0","id":3,"method":"get_inclusion_proofs.v1","params":{}}`)
		require.Equal(t, `{"jsonrpc":"2.0","error":{"code":-32008,"message":"Proof unavailable","data":{"reason":"proof_unavailable","detail":"unsupported_configuration","retryable":false}},"id":3}`+"\n", body, mode)
	}
	require.Zero(t, n)

	// without the publication wiring the method is simply not served
	rpc := newRPCTestServer(t, fixedV2Service{resp: v2Fixture()}, nil)
	_, body := postRPC(t, rpc, `{"jsonrpc":"2.0","id":4,"method":"get_inclusion_proofs.v1","params":{}}`)
	require.Contains(t, body, `"code":-32601`)
}

// Other methods keep the unbounded generic path.
func TestBatchRPC_OtherMethodsUnaffectedByLimits(t *testing.T) {
	var n int
	rpc := batchServer(t, config.ShardingModeStandalone, echoHandler(&n))
	pad := strings.Repeat(" ", 2*api.InclusionProofsMaxRequestBytes)
	_, body := postRPC(t, rpc, `{"jsonrpc":"2.0","id":8,`+pad+`"method":"get_inclusion_proof.v2","params":{"stateId":"`+strings.Repeat("ab", 32)+`"}}`)
	require.Equal(t, goldenV2Success[:len(goldenV2Success)-len(`,"id":7}`+"\n")]+`,"id":8}`+"\n", body)
}
