package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/service"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

type countingV2Service struct {
	fixedV2Service
	calls *int
}

func (s countingV2Service) GetInclusionProofV2(ctx context.Context, r *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error) {
	*s.calls++
	return s.fixedV2Service.GetInclusionProofV2(ctx, r)
}

type paramsFixture struct {
	Errors map[string]struct {
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	} `json:"errors"`
	Cases []struct {
		Name   string  `json:"name"`
		Method string  `json:"method"`
		Params string  `json:"params"`
		Valid  bool    `json:"valid"`
		WaitMs *uint64 `json:"waitMs"`
		Error  string  `json:"error"`
	} `json:"cases"`
}

// The reserved waitMs is validated by both handlers over the wire and otherwise ignored: valid
// requests are answered exactly like the same request without it (no echo yet), invalid ones with
// the method's existing validation convention, and the service is not reached.
func TestLongPollParams_ReservedOverTheWire(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/longpoll/params.json")
	require.NoError(t, err)
	var fx paramsFixture
	require.NoError(t, json.Unmarshal(raw, &fx))

	var v2Calls, batchCalls int
	rpc := newRPCTestServer(t, countingV2Service{fixedV2Service: fixedV2Service{resp: v2Fixture()}, calls: &v2Calls}, func(s *Server) {
		s.config = &config.Config{}
		s.config.Sharding.Mode = config.ShardingModeStandalone
		s.SetBatchProofHandler(func(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
			batchCalls++
			if _, err := api.ParseGetInclusionProofsRequestV1(params); err != nil {
				return nil, service.ParamsBatchError(err).RPCError()
			}
			return &api.GetInclusionProofsResponseV1{Proofs: []*api.GetInclusionProofResponseV2{v2Fixture()}}, nil
		})
	})
	for _, c := range fx.Cases {
		t.Run(c.Name, func(t *testing.T) {
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":3,"method":%q,"params":%s}`, c.Method, c.Params)
			before := v2Calls
			beforeBatch := batchCalls
			_, got := postRPC(t, rpc, body)
			if !c.Valid {
				if c.Method == "get_inclusion_proof.v2" {
					require.Equal(t, before, v2Calls, "the service must not be reached")
					want := fx.Errors[c.Error].Error.Code
					require.Contains(t, got, fmt.Sprintf(`"error":{"code":%d,`, want))
				} else {
					require.Contains(t, got, `"error":{"code":-32602,"message":"Invalid params","data":{"reason":"invalid_request","detail":"malformed_params","retryable":false}}`)
				}
				return
			}
			require.NotContains(t, got, `"error"`)
			// no echo and no metadata: the result is the unchanged legacy shape
			if c.Method == "get_inclusion_proof.v2" {
				require.Equal(t, before+1, v2Calls)
				if strings.Contains(c.Params, `"includeStatus":true`) {
					require.True(t, strings.HasPrefix(got, `{"jsonrpc":"2.0","result":{"proof":"`), got)
					require.NotContains(t, got, "waitedMs")
				} else {
					require.True(t, strings.HasPrefix(got, `{"jsonrpc":"2.0","result":"`), got)
				}
			} else {
				require.Equal(t, beforeBatch+1, batchCalls)
				require.NotContains(t, got, "waitedMs")
			}
		})
	}
}
