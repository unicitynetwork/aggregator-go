package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

type statusV2Service struct {
	fixedV2Service
	hint  api.ProofStatus
	asked int
}

func (s *statusV2Service) ProofStatusHint(api.StateID) api.ProofStatus {
	s.asked++
	return s.hint
}

// the empty-proof shape of a state which is not (yet) included
func emptyV2Fixture() *api.GetInclusionProofResponseV2 {
	return &api.GetInclusionProofResponseV2{
		BlockNumber:    42,
		InclusionProof: &api.InclusionProofV2{UnicityCertificate: types.RawCBOR{0x01}},
	}
}

const stateIDHex = "abababababababababababababababababababababababababababababababab"

func rpcBody(id int, params string) string {
	return `{"jsonrpc":"2.0","id":` + string(rune('0'+id)) + `,"method":"get_inclusion_proof.v2","params":` + params + `}`
}

func decodeResult(t *testing.T, body string) json.RawMessage {
	var env struct {
		Result json.RawMessage `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &env))
	return env.Result
}

// Golden: the default result of the empty-proof shape stays byte-identical, with or without
// a status-capable service behind it.
const goldenV2Empty = `{"jsonrpc":"2.0","result":"82182ad998798501f6f6f601","id":7}` + "\n"

func TestV2StatusDefaultResultIsByteIdentical(t *testing.T) {
	for name, tc := range map[string]struct {
		resp   *api.GetInclusionProofResponseV2
		golden string
	}{"included": {v2Fixture(), goldenV2Success}, "empty proof": {emptyV2Fixture(), goldenV2Empty}} {
		for svcName, svc := range map[string]Service{
			"plain service":  fixedV2Service{resp: tc.resp},
			"status service": &statusV2Service{fixedV2Service: fixedV2Service{resp: tc.resp}, hint: api.ProofStatusPending},
		} {
			rpc := newRPCTestServer(t, svc, nil)
			_, body := postRPC(t, rpc, rpcBody(7, `{"stateId":"`+stateIDHex+`"}`))
			require.Equal(t, tc.golden, body, name+"/"+svcName)
			_, body = postRPC(t, rpc, rpcBody(7, `{"stateId":"`+stateIDHex+`","includeStatus":false}`))
			require.Equal(t, tc.golden, body, name+"/"+svcName+"/includeStatus=false")
		}
	}
}

func TestV2StatusOptIn(t *testing.T) {
	served := func(s string) float64 {
		return testutil.ToFloat64(metrics.ProofStatusServedTotal.WithLabelValues("get_inclusion_proof.v2", s))
	}
	for _, tc := range []struct {
		name   string
		resp   *api.GetInclusionProofResponseV2
		hint   api.ProofStatus
		status string
		golden string
	}{
		{"included", v2Fixture(), api.ProofStatusPending, "included", goldenV2Success}, // stale pending mark: the proof wins
		{"pending", emptyV2Fixture(), api.ProofStatusPending, "pending", goldenV2Empty},
		{"expired", emptyV2Fixture(), api.ProofStatusExpired, "expired", goldenV2Empty},
		{"unknown", emptyV2Fixture(), api.ProofStatusUnknown, "unknown", goldenV2Empty},
	} {
		before := served(tc.status)
		svc := &statusV2Service{fixedV2Service: fixedV2Service{resp: tc.resp}, hint: tc.hint}
		rpc := newRPCTestServer(t, svc, nil)
		_, body := postRPC(t, rpc, rpcBody(7, `{"stateId":"`+stateIDHex+`","includeStatus":true}`))

		var res struct {
			Proof  string `json:"proof"`
			Status string `json:"status"`
		}
		require.NoError(t, json.Unmarshal(decodeResult(t, body), &res), tc.name)
		require.Equal(t, tc.status, res.Status, tc.name)
		// proof is byte-identical to the default result in every case
		var def string
		require.NoError(t, json.Unmarshal(decodeResult(t, tc.golden), &def))
		require.Equal(t, def, res.Proof, tc.name)
		require.Equal(t, `{"proof":"`+def+`","status":"`+tc.status+`"}`, string(decodeResult(t, body)), "exact object shape: "+tc.name)
		require.Equal(t, 1.0, served(tc.status)-before, tc.name)
	}
}

// A service without status support answers unknown, never an error.
func TestV2StatusWithoutProviderIsUnknown(t *testing.T) {
	rpc := newRPCTestServer(t, fixedV2Service{resp: emptyV2Fixture()}, nil)
	_, body := postRPC(t, rpc, rpcBody(7, `{"stateId":"`+stateIDHex+`","includeStatus":true}`))
	require.Contains(t, string(decodeResult(t, body)), `"status":"unknown"`)
}

// The hint is read once, before the proof is built (a state finalizing in between returns
// its proof rather than "unknown"), and only on request.
func TestV2StatusHintOnlyOnRequest(t *testing.T) {
	svc := &statusV2Service{fixedV2Service: fixedV2Service{resp: emptyV2Fixture()}, hint: api.ProofStatusPending}
	rpc := newRPCTestServer(t, svc, nil)
	postRPC(t, rpc, rpcBody(7, `{"stateId":"`+stateIDHex+`"}`))
	require.Zero(t, svc.asked, "no lookup without the opt-in")
	postRPC(t, rpc, rpcBody(7, `{"stateId":"`+stateIDHex+`","includeStatus":true}`))
	require.Equal(t, 1, svc.asked, "one O(1) lookup per query")
}

// Old servers decode the params with plain json.Unmarshal into a struct without the field and
// answer with the plain string: a string result means "status unsupported".
func TestV2StatusOldServerCompatibility(t *testing.T) {
	type oldRequest struct {
		StateID api.StateID `json:"stateId"`
	}
	var old oldRequest
	require.NoError(t, json.Unmarshal([]byte(`{"stateId":"`+stateIDHex+`","includeStatus":true}`), &old))
	require.Equal(t, stateIDHex, old.StateID.String()[len(old.StateID.String())-64:])

	// an SDK distinguishes the two shapes by the JSON type of the result
	rpc := newRPCTestServer(t, fixedV2Service{resp: v2Fixture()}, nil)
	_, plain := postRPC(t, rpc, rpcBody(7, `{"stateId":"`+stateIDHex+`"}`))
	require.True(t, strings.HasPrefix(string(decodeResult(t, plain)), `"`), "string result: no status support / not requested")
	_, withStatus := postRPC(t, newRPCTestServer(t, &statusV2Service{fixedV2Service: fixedV2Service{resp: v2Fixture()}, hint: api.ProofStatusUnknown}, nil),
		rpcBody(7, `{"stateId":"`+stateIDHex+`","includeStatus":true}`))
	require.True(t, bytes.HasPrefix(decodeResult(t, withStatus), []byte(`{`)), "object result: status supported")

	// errors keep their shape with the opt-in
	_, bad := postRPC(t, rpc, rpcBody(8, `{"stateId":"abcd","includeStatus":true}`))
	require.Contains(t, bad, "stateId must be exactly 32 bytes")
	_ = context.Background
}

type notYetService struct{ fixedV2Service }

func (notYetService) GetInclusionProofV2(context.Context, *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error) {
	return nil, fmt.Errorf("%w: no block found with root hash 00", api.ErrProofNotYetAvailable)
}

// Before the first block the answer is the typed retryable proof_unavailable/no_finalized_view
// of the batch method, never an internal error and never an empty-tree proof.
func TestV2BeforeFirstBlockIsTypedRetryable(t *testing.T) {
	rpc := newRPCTestServer(t, notYetService{}, nil)
	for _, params := range []string{`{"stateId":"` + stateIDHex + `"}`, `{"stateId":"` + stateIDHex + `","includeStatus":true}`} {
		_, body := postRPC(t, rpc, rpcBody(7, params))
		var env struct {
			Result json.RawMessage `json:"result"`
			Error  *jsonrpc.Error  `json:"error"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &env))
		require.Nil(t, env.Result)
		require.NotNil(t, env.Error)
		require.Equal(t, jsonrpc.ProofUnavailableCode, env.Error.Code)
		data, err := json.Marshal(env.Error.Data)
		require.NoError(t, err)
		require.JSONEq(t, `{"reason":"proof_unavailable","detail":"no_finalized_view","retryable":true}`, string(data))
	}
}
