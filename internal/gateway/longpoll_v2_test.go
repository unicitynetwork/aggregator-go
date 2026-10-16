package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// waitService serves the empty proof with a hint until included flips.
type waitService struct {
	fixedV2Service
	included atomic.Bool
	hint     atomic.Value // api.ProofStatus
	err      error
	reads    atomic.Int32
}

func (s *waitService) ProofStatusHint(api.StateID) api.ProofStatus {
	if h, ok := s.hint.Load().(api.ProofStatus); ok {
		return h
	}
	return api.ProofStatusUnknown
}

func (s *waitService) GetInclusionProofV2(context.Context, *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error) {
	s.reads.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	if s.included.Load() {
		return v2Fixture(), nil
	}
	return emptyV2Fixture(), nil
}

func lpServer(t *testing.T, svc Service, waiters, c int) (*Server, *longpoll.Registry, func(string) string) {
	t.Helper()
	var srv *Server
	rpc := newRPCTestServer(t, svc, func(s *Server) { srv = s })
	_ = rpc
	reg := longpoll.New(longpoll.Config{MaxWaiters: waiters, Concurrency: c})
	srv.SetLongPoll(&longpoll.Coordinator{Reg: reg, MaxWait: 3 * time.Second, Budget: 30 * time.Second,
		Reserve: 100 * time.Millisecond, ResumeReserveOverride: 200 * time.Millisecond})
	return srv, reg, func(body string) string { _, b := postRPC(t, srv.rpcServer, body); return b }
}

func v2req(id int, params string) string {
	return `{"jsonrpc":"2.0","id":` + string(rune('0'+id)) + `,"method":"get_inclusion_proof.v2","params":` + params + `}`
}

func TestV2Waited_EnvelopeShapes(t *testing.T) {
	sid := `"stateId":"` + stateIDHex + `"`
	svc := &waitService{}
	svc.included.Store(true)
	svc.hint.Store(api.ProofStatusPending)
	_, _, post := lpServer(t, svc, 8, 8)

	var def string
	require.NoError(t, json.Unmarshal(decodeResult(t, goldenV2Success), &def))

	// no waitMs: the bare string, and includeStatus alone keeps its legacy object
	require.Equal(t, goldenV2Success, post(v2req(7, `{`+sid+`}`)))
	require.Equal(t, `{"proof":"`+def+`","status":"included"}`, string(decodeResult(t, post(v2req(7, `{`+sid+`,"includeStatus":true}`)))))

	// waitMs alone: {proof, waitedMs}; zero is explicit
	require.Equal(t, `{"proof":"`+def+`","waitedMs":0}`, string(decodeResult(t, post(v2req(7, `{`+sid+`,"waitMs":0}`)))))
	require.Equal(t, `{"proof":"`+def+`","waitedMs":0}`, string(decodeResult(t, post(v2req(7, `{`+sid+`,"waitMs":2000}`)))), "included at once: nothing waited")
	// waitMs + includeStatus
	require.Equal(t, `{"proof":"`+def+`","status":"included","waitedMs":0}`, string(decodeResult(t, post(v2req(7, `{`+sid+`,"waitMs":2000,"includeStatus":true}`)))))
	// includeStatus:false is not status
	require.Equal(t, `{"proof":"`+def+`","waitedMs":0}`, string(decodeResult(t, post(v2req(7, `{`+sid+`,"waitMs":0,"includeStatus":false}`)))))
}

func TestV2Waited_NotPendingStatesEndTheWaitAtOnce(t *testing.T) {
	sid := `"stateId":"` + stateIDHex + `"`
	for _, h := range []api.ProofStatus{api.ProofStatusUnknown, api.ProofStatusExpired} {
		svc := &waitService{}
		svc.hint.Store(h)
		_, reg, post := lpServer(t, svc, 8, 8)
		start := time.Now()
		got := post(v2req(7, `{`+sid+`,"waitMs":2500,"includeStatus":true}`))
		require.Less(t, time.Since(start), time.Second, string(h))
		require.Contains(t, string(decodeResult(t, got)), `"status":"`+string(h)+`","waitedMs":0}`)
		require.EqualValues(t, 1, svc.reads.Load(), "no park, no recheck")
		require.Zero(t, reg.Stats().Reserved)
	}
}

func TestV2Waited_PendingWakesOnPublication(t *testing.T) {
	sid := `"stateId":"` + stateIDHex + `"`
	svc := &waitService{}
	svc.hint.Store(api.ProofStatusPending)
	_, reg, post := lpServer(t, svc, 8, 8)
	got := make(chan string, 1)
	go func() { got <- post(v2req(7, `{`+sid+`,"waitMs":2500,"includeStatus":true}`)) }()
	require.Eventually(t, func() bool { return reg.Stats().Reserved == 1 }, 2*time.Second, time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	svc.included.Store(true)
	svc.hint.Store(api.ProofStatusUnknown) // the hint is cleared by finalization: the proof wins
	reg.Notify()
	body := <-got
	var res struct {
		Proof    string `json:"proof"`
		Status   string `json:"status"`
		WaitedMs uint64 `json:"waitedMs"`
	}
	require.NoError(t, json.Unmarshal(decodeResult(t, body), &res))
	require.Equal(t, "included", res.Status)
	require.GreaterOrEqual(t, res.WaitedMs, uint64(40))
	require.NotEmpty(t, res.Proof)
}

func TestV2Waited_TimeoutReturnsThePendingOutcomeWithEcho(t *testing.T) {
	sid := `"stateId":"` + stateIDHex + `"`
	svc := &waitService{}
	svc.hint.Store(api.ProofStatusPending)
	srv, _, post := lpServer(t, svc, 8, 8)
	srv.longPoll.MaxWait = 200 * time.Millisecond
	body := post(v2req(7, `{`+sid+`,"waitMs":9000,"includeStatus":true}`)) // clamped to the configured maximum
	var res struct {
		Status   string `json:"status"`
		WaitedMs uint64 `json:"waitedMs"`
	}
	require.NoError(t, json.Unmarshal(decodeResult(t, body), &res))
	require.Equal(t, "pending", res.Status, "a timeout is the ordinary pending outcome, not a new error")
	require.InDelta(t, 200, float64(res.WaitedMs), 100)
	require.NotContains(t, body, `"error"`)
}

func TestV2Waited_CapZeroAndSmallCServeImmediatelyWithZeroEcho(t *testing.T) {
	sid := `"stateId":"` + stateIDHex + `"`
	for name, tc := range map[string][2]int{"cap 0": {0, 8}, "C=1": {8, 1}} {
		svc := &waitService{}
		svc.hint.Store(api.ProofStatusPending)
		_, _, post := lpServer(t, svc, tc[0], tc[1])
		start := time.Now()
		body := post(v2req(7, `{`+sid+`,"waitMs":2500,"includeStatus":true}`))
		require.Less(t, time.Since(start), time.Second, name)
		require.Contains(t, string(decodeResult(t, body)), `"status":"pending","waitedMs":0}`, name)
	}
}

func TestV2Waited_ErrorsEchoInErrorData(t *testing.T) {
	sid := `"stateId":"` + stateIDHex + `"`
	// pre-first-block
	svc := &waitService{err: api.ErrProofNotYetAvailable}
	_, _, post := lpServer(t, svc, 8, 8)
	require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32008,"message":"Proof unavailable","data":{"reason":"proof_unavailable","detail":"no_finalized_view","retryable":true,"waitedMs":0}},"id":7}`,
		post(v2req(7, `{`+sid+`,"waitMs":1000}`)))
	// internal error: non-object data is kept as originalData
	svc.err = errors.New("db down")
	require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32603,"message":"Failed to get inclusion proof","data":{"originalData":"db down","waitedMs":0}},"id":7}`,
		post(v2req(7, `{`+sid+`,"waitMs":1000}`)))
	// validation errors of a recognized wait-bearing object echo zero
	require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32000,"message":"stateId must be exactly 32 bytes (v2 wire format), got 2","data":{"waitedMs":0}},"id":7}`,
		post(v2req(7, `{"stateId":"abcd","waitMs":1000}`)))
	require.JSONEq(t, `{"jsonrpc":"2.0","error":{"code":-32000,"message":"stateId is required","data":{"waitedMs":0}},"id":7}`,
		post(v2req(7, `{"waitMs":1000}`)))
	// an invalid waitMs is not recognized: the legacy error, no echo
	bad := post(v2req(7, `{`+sid+`,"waitMs":-1}`))
	require.Contains(t, bad, `"code":-32000`)
	require.NotContains(t, bad, "waitedMs")
	dup := post(v2req(7, `{`+sid+`,"waitMs":1,"waitMs":2}`))
	require.NotContains(t, dup, "waitedMs")
}

// Without the coordinator (long-poll not wired) the reservation behavior stays: waitMs validated,
// ignored, no echo.
func TestV2Waited_NotWiredKeepsTheReservation(t *testing.T) {
	rpc := newRPCTestServer(t, fixedV2Service{resp: v2Fixture()}, nil)
	_, body := postRPC(t, rpc, v2req(7, `{"stateId":"`+stateIDHex+`","waitMs":500}`))
	require.Equal(t, goldenV2Success, body)
}

func TestV2Waited_OptInRequestBoundsApplyOnlyToWaitRequests(t *testing.T) {
	svc := &waitService{}
	svc.included.Store(true)
	srv, _, post := lpServer(t, svc, 8, 8)
	pad := strings.Repeat("x", api.InclusionProofsMaxParamsBytes)
	// legacy request: unbounded legacy decoding, unchanged
	legacy := post(v2req(7, `{"stateId":"`+stateIDHex+`","pad":"`+pad+`"}`))
	require.Contains(t, legacy, `"result":"`)
	// opt-in request: 32 KiB params bound
	opt := post(v2req(7, `{"stateId":"`+stateIDHex+`","waitMs":1000,"pad":"`+pad+`"}`))
	require.Contains(t, opt, `"code":-32007`)
	require.Contains(t, opt, `"detail":"request_bytes"`)
	// opt-in with a long ID: 128 byte bound; the legacy long ID is still echoed
	longID := strings.Repeat("i", 200)
	_, body := postRPC(t, srv.rpcServer, `{"jsonrpc":"2.0","id":"`+longID+`","method":"get_inclusion_proof.v2","params":{"stateId":"`+stateIDHex+`","waitMs":1000}}`)
	require.Contains(t, body, `"detail":"invalid_id"`)
	_, body = postRPC(t, srv.rpcServer, `{"jsonrpc":"2.0","id":"`+longID+`","method":"get_inclusion_proof.v2","params":{"stateId":"`+stateIDHex+`"}}`)
	require.Contains(t, body, `"result":"`)
	// a whole opt-in request over 64 KiB (trailing whitespace counts)
	_, body = postRPC(t, srv.rpcServer, v2req(7, `{"stateId":"`+stateIDHex+`","waitMs":1000}`)+strings.Repeat(" ", api.InclusionProofsMaxRequestBytes))
	require.Contains(t, body, `"detail":"request_bytes"`)
}
