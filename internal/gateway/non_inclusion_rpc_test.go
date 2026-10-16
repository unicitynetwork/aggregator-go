package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

func niServer(t *testing.T, mode config.ShardingMode, h jsonrpc.HandlerFunc) *jsonrpc.Server {
	return newRPCTestServer(t, fixedV2Service{resp: v2Fixture()}, func(s *Server) {
		s.config = &config.Config{}
		s.config.Sharding.Mode = mode
		s.SetNonInclusionHandler(h)
	})
}

func niResponse(t *testing.T) *api.GetNonInclusionProofResponseV1 {
	var c api.NonInclusionCert
	b := make([]byte, 96)
	b[32] = 1
	require.NoError(t, c.UnmarshalBinary(b))
	return &api.GetNonInclusionProofResponseV1{BlockNumber: 7, Proof: &api.NonInclusionProofV1{Certificate: &c, UnicityCertificate: types.RawCBOR{0xf6}}}
}

// The method answers with ONE JSON string: the hex of [block, 39034([1, certificate, UC])].
func TestNonInclusionRPC_ResultIsAHexString(t *testing.T) {
	resp := niResponse(t)
	rpc := niServer(t, config.ShardingModeStandalone, func(ctx context.Context, p json.RawMessage) (interface{}, *jsonrpc.Error) {
		req, err := api.ParseGetNonInclusionProofRequestV1(p)
		require.NoError(t, err)
		require.Len(t, req.StateID, 32)
		return resp, nil
	})
	_, body := postRPC(t, rpc, `{"jsonrpc":"2.0","id":7,"method":"get_non_inclusion_proof.v1","params":{"stateId":"`+strings.Repeat("ab", 32)+`"}}`)
	var env struct {
		Result string `json:"result"`
		ID     int    `json:"id"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &env))
	raw, err := hex.DecodeString(env.Result)
	require.NoError(t, err)
	want, err := resp.MarshalCBOR()
	require.NoError(t, err)
	require.True(t, bytes.Equal(want, raw))
	require.Equal(t, 7, env.ID)
}

func TestNonInclusionRPC_BoundsErrorsAndUnsupported(t *testing.T) {
	called := 0
	h := func(context.Context, json.RawMessage) (interface{}, *jsonrpc.Error) {
		called++
		return nil, jsonrpc.NewDetailedError(api.NonInclusionCodeStateIncluded, "State included", "state_included", "state_included", false, nil)
	}
	rpc := niServer(t, config.ShardingModeStandalone, h)
	// the code and structured data pass through unchanged (HTTP 200 JSON-RPC error)
	code, body := postRPC(t, rpc, `{"jsonrpc":"2.0","id":3,"method":"get_non_inclusion_proof.v1","params":{"stateId":"`+strings.Repeat("ab", 32)+`"}}`)
	require.Equal(t, 200, code)
	require.Equal(t, `{"jsonrpc":"2.0","error":{"code":-32022,"message":"State included","data":{"reason":"state_included","detail":"state_included","retryable":false}},"id":3}`+"\n", body)
	// params bigger than one state ID are refused before the handler runs
	_, body = postRPC(t, rpc, `{"jsonrpc":"2.0","id":4,"method":"get_non_inclusion_proof.v1","params":{"stateId":"`+strings.Repeat("ab", 32)+`","x":"`+strings.Repeat("a", 2048)+`"}}`)
	require.Contains(t, body, `"error"`)
	require.Equal(t, 1, called)
	// an oversized request id is refused too
	_, body = postRPC(t, rpc, `{"jsonrpc":"2.0","id":"`+strings.Repeat("x", 200)+`","method":"get_non_inclusion_proof.v1","params":{"stateId":"`+strings.Repeat("ab", 32)+`"}}`)
	require.Contains(t, body, `"error"`)
	require.Equal(t, 1, called)

	for _, mode := range []config.ShardingMode{config.ShardingModeParent, config.ShardingModeChild} {
		rpc := niServer(t, mode, h)
		_, body := postRPC(t, rpc, `{"jsonrpc":"2.0","id":5,"method":"get_non_inclusion_proof.v1","params":{}}`)
		require.Equal(t, `{"jsonrpc":"2.0","error":{"code":-32020,"message":"No certified snapshot","data":{"reason":"proof_unavailable","detail":"unsupported_configuration","retryable":false}},"id":5}`+"\n", body, mode)
	}
	require.Equal(t, 1, called)

	// not registered until the publication wiring: not served
	plain := newRPCTestServer(t, fixedV2Service{resp: v2Fixture()}, nil)
	_, body = postRPC(t, plain, `{"jsonrpc":"2.0","id":6,"method":"get_non_inclusion_proof.v1","params":{}}`)
	require.Contains(t, body, `"code":-32601`)
}
