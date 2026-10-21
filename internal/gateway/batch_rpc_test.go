package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"testing/iotest"

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

func postRPCReader(t *testing.T, h http.Handler, body io.Reader) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Body.String()
}

// Regression: the 64 KiB limit counts the whole body, not the first JSON value.
func TestBatchRPC_RequestLimitCountsCompleteBody(t *testing.T) {
	var n int
	rpc := batchServer(t, config.ShardingModeStandalone, echoHandler(&n))
	valid := `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proofs.v1","params":{"stateIds":["` + strings.Repeat("ab", 32) + `"]}}`
	limitErr := fmt.Sprintf(`{"jsonrpc":"2.0","error":{"code":-32007,"message":"Limit exceeded","data":{"reason":"limit_exceeded","detail":"request_bytes","retryable":false,"limit":%d}},"id":1}`+"\n", api.InclusionProofsMaxRequestBytes)

	// trailing whitespace pushing the body over the limit
	require.Equal(t, limitErr, postRPCReader(t, rpc, strings.NewReader(valid+strings.Repeat(" ", api.InclusionProofsMaxRequestBytes))))
	require.Zero(t, n)
	// the same body delivered in one-byte chunks (no Content-Length reliance)
	require.Equal(t, limitErr, postRPCReader(t, rpc, iotest.OneByteReader(strings.NewReader(valid+strings.Repeat("\n", api.InclusionProofsMaxRequestBytes)))))
	require.Zero(t, n)
	// trailing whitespace within the limit is fine
	require.Contains(t, postRPCReader(t, rpc, iotest.OneByteReader(strings.NewReader(valid+"  \n\t"))), `"result"`)
	require.Equal(t, 1, n)

	// trailing non-whitespace, including a second request, is rejected
	invalid := `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request","data":{"reason":"invalid_request","detail":"invalid_envelope","retryable":false}},"id":1}` + "\n"
	require.Equal(t, invalid, postRPCReader(t, rpc, strings.NewReader(valid+valid)))
	require.Equal(t, invalid, postRPCReader(t, rpc, strings.NewReader(valid+" x")))
	require.Equal(t, 1, n)
}

// Regression: the raw ID is validated by its original encoding and echoed verbatim.
func TestBatchRPC_RawIDValidationAndEcho(t *testing.T) {
	var n int
	rpc := batchServer(t, config.ShardingModeStandalone, echoHandler(&n))
	call := func(id string) string {
		idField := ""
		if id != "<absent>" {
			idField = `"id":` + id + `,`
		}
		_, body := postRPC(t, rpc, `{"jsonrpc":"2.0",`+idField+`"method":"get_inclusion_proofs.v1","params":{"stateIds":["`+strings.Repeat("ab", 32)+`"]}}`)
		return body
	}
	invalidID := `{"jsonrpc":"2.0","error":{"code":-32602,"message":"Invalid params","data":{"reason":"invalid_request","detail":"invalid_id","retryable":false}},"id":null}` + "\n"

	// escapes: 30 x \u0061 decodes to 30 bytes but is 182 encoded bytes
	require.Equal(t, invalidID, call(`"`+strings.Repeat(`\u0061`, 30)+`"`))
	for _, id := range []string{`{}`, `[]`, `true`, `false`, `null`, `<absent>`} {
		require.Equal(t, invalidID, call(id), id)
	}
	require.Zero(t, n, "the handler never ran for invalid IDs")

	// precision and form are preserved
	for _, id := range []string{`9007199254740993`, `-12`, `1.50`, `1e2`, `"a\u0061b"`, `"plain"`} {
		body := call(id)
		require.True(t, strings.HasSuffix(body, `,"id":`+id+"}\n"), "id %s echoed in %s", id, body)
	}
	require.Equal(t, 6, n)
	// exactly 128 encoded bytes passes, 129 does not
	require.Contains(t, call(`"`+strings.Repeat("x", 126)+`"`), `"result"`)
	require.Equal(t, invalidID, call(`"`+strings.Repeat("x", 127)+`"`))
}

// Regression: an identifiable batch method gets the structured envelope error.
func TestBatchRPC_InvalidEnvelopeIsStructured(t *testing.T) {
	var n int
	rpc := batchServer(t, config.ShardingModeStandalone, echoHandler(&n))
	_, body := postRPC(t, rpc, `{"jsonrpc":"1.0","id":5,"method":"get_inclusion_proofs.v1","params":{}}`)
	require.Equal(t, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request","data":{"reason":"invalid_request","detail":"invalid_envelope","retryable":false}},"id":5}`+"\n", body)
	_, body = postRPC(t, rpc, `{"id":5,"method":"get_inclusion_proofs.v1"}`)
	require.Contains(t, body, `"detail":"invalid_envelope"`)
	require.Zero(t, n)

	// legacy methods keep the bare error
	_, body = postRPC(t, rpc, `{"jsonrpc":"1.0","id":5,"method":"get_inclusion_proof.v2","params":{}}`)
	require.Equal(t, `{"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"},"id":5}`+"\n", body)
	// legacy IDs still decode as before (float conversion included)
	_, body = postRPC(t, rpc, `{"jsonrpc":"2.0","id":9007199254740993,"method":"nope","params":{}}`)
	require.Contains(t, body, `"id":9007199254740992`)
}

func bigProofs(n, witnessBytes int) []*api.GetInclusionProofResponseV2 {
	proofs := make([]*api.GetInclusionProofResponseV2, n)
	for i := range proofs {
		p := v2Fixture()
		p.InclusionProof.CertificationData.Witness = bytes.Repeat([]byte{byte(i)}, witnessBytes)
		proofs[i] = p
	}
	return proofs
}

// Regression: an oversized result is rejected while it is being assembled, with
// output capped; the previous writer encoded all of it first.
func TestBatchRPC_OversizedResponseIsRejectedWithBoundedAllocation(t *testing.T) {
	// 256 proofs with ~128 KiB witnesses: ~64 MiB of hex JSON against the 8 MiB limit
	resp := &api.GetInclusionProofsResponseV1{Proofs: bigProofs(256, 128<<10), PartitionView: []byte{1, 2, 3}}
	handler := func(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) { return resp, nil }
	rpc := batchServer(t, config.ShardingModeStandalone, handler)
	body := `{"jsonrpc":"2.0","id":9,"method":"get_inclusion_proofs.v1","params":{"stateIds":["` + strings.Repeat("ab", 32) + `"]}}`

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, got := postRPC(t, rpc, body)
	runtime.ReadMemStats(&after)
	boundedAllocated := after.TotalAlloc - before.TotalAlloc
	require.Equal(t, `{"jsonrpc":"2.0","error":{"code":-32007,"message":"Limit exceeded","data":{"reason":"limit_exceeded","detail":"response_bytes","retryable":false,"limit":8388608}},"id":9}`+"\n", got)

	// encoding the whole result first (the previous writer) costs several times more
	runtime.GC()
	runtime.ReadMemStats(&before)
	full, err := json.Marshal(jsonrpc.NewResponse(resp, 9))
	require.NoError(t, err)
	runtime.ReadMemStats(&after)
	fullAllocated := after.TotalAlloc - before.TotalAlloc
	require.Greater(t, len(full), 4*api.InclusionProofsMaxResponseBytes)

	allocated := boundedAllocated
	require.Less(t, allocated*4, fullAllocated, "bounded writer allocated %d bytes, full encoding %d", allocated, fullAllocated)
}

// The bounded writer is byte-identical to json.Marshal of the response, and the
// limit counts the envelope, ID and newline exactly.
func TestBatchRPC_BoundedWriterMatchesMarshalAndLimitIsExact(t *testing.T) {
	resp := &api.GetInclusionProofsResponseV1{Proofs: bigProofs(3, 100), PartitionView: []byte{9, 8, 7}}
	handler := func(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) { return resp, nil }
	rpc := batchServer(t, config.ShardingModeStandalone, handler)
	call := `{"jsonrpc":"2.0","id":"x","method":"get_inclusion_proofs.v1","params":{"stateIds":["` + strings.Repeat("ab", 32) + `"]}}`
	_, got := postRPC(t, rpc, call)
	want, err := json.Marshal(jsonrpc.NewResponse(resp, json.RawMessage(`"x"`)))
	require.NoError(t, err)
	require.Equal(t, string(want)+"\n", got)
	require.Contains(t, got, `"partitionView":"090807"`)
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// Regression (security): an outer body cap applies before the envelope is
// decoded, for every method, so an oversized body is never read into memory.
func TestRPC_OuterBodyCapIsEnforcedBeforeDecoding(t *testing.T) {
	var n int
	rpc := batchServer(t, config.ShardingModeStandalone, echoHandler(&n))
	rpc.SetMaxRequestBodyBytes(4096)
	huge := strings.Repeat("a", 8<<20)

	for _, method := range []string{"get_inclusion_proof.v2", "get_inclusion_proofs.v1", "unknown_method"} {
		cr := &countingReader{r: strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":{"x":"` + huge + `"}}`)}
		req := httptest.NewRequest(http.MethodPost, "/", cr)
		rec := httptest.NewRecorder()
		rpc.ServeHTTP(rec, req)
		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, method)
		require.Equal(t, `{"jsonrpc":"2.0","error":{"code":-32007,"message":"Limit exceeded","data":{"reason":"limit_exceeded","detail":"request_bytes","retryable":false,"limit":4096}},"id":null}`+"\n", rec.Body.String(), method)
		require.LessOrEqual(t, cr.n, int64(4096+1), "%s: only the ceiling (plus one byte) may be read, read %d", method, cr.n)
	}
	require.Zero(t, n)

	// a trailing tail on a batch request cannot slip past the ceiling either
	valid := `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proofs.v1","params":{"stateIds":["` + strings.Repeat("ab", 32) + `"]}}`
	cr := &countingReader{r: strings.NewReader(valid + strings.Repeat(" ", 8<<20))}
	rec := httptest.NewRecorder()
	rpc.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", cr))
	require.Contains(t, rec.Body.String(), `"detail":"request_bytes"`)
	require.LessOrEqual(t, cr.n, int64(4096+1))

	// requests under the ceiling are unaffected
	require.Contains(t, postRPCReader(t, rpc, strings.NewReader(valid)), `"result"`)
}

func TestRPC_DefaultOuterBodyCeiling(t *testing.T) {
	require.EqualValues(t, 16<<20, jsonrpc.DefaultMaxRequestBodyBytes)
	rpc := batchServer(t, config.ShardingModeStandalone, echoHandler(new(int)))
	// larger than the batch limits, far below the ceiling: still the batch-specific error, not 413
	pad := strings.Repeat(" ", 1<<20)
	rec := httptest.NewRecorder()
	rpc.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proofs.v1","params":{}}`+pad)))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"detail":"request_bytes"`)
	// the legacy v2 method accepts a large (1 MiB) request as before
	_, body := postRPC(t, rpc, `{"jsonrpc":"2.0","id":7,`+pad+`"method":"get_inclusion_proof.v2","params":{"stateId":"`+strings.Repeat("ab", 32)+`"}}`)
	require.Equal(t, goldenV2Success, body)
}
