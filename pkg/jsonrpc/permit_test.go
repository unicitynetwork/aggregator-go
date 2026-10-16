package jsonrpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
)

func testServer(t *testing.T, limit int) *Server {
	t.Helper()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	return NewServer(log, limit)
}

func TestPermit_ReleaseCloseAreIdempotentAndNeverStealTokens(t *testing.T) {
	s := testServer(t, 2)
	a, ok := s.newPermit()
	require.True(t, ok)
	b, ok := s.newPermit()
	require.True(t, ok)
	require.Equal(t, 2, s.ActiveRequests())
	_, ok = s.newPermit()
	require.False(t, ok, "saturated: fresh requests are rejected exactly as before")

	require.True(t, a.Release())
	require.False(t, a.Release(), "second release is a no-op")
	require.Equal(t, 1, s.ActiveRequests())
	a.Close() // the deferred owner release after a park must not remove b's token
	a.Close()
	require.Equal(t, 1, s.ActiveRequests())
	require.True(t, b.Held())

	b.Close()
	b.Close()
	require.Equal(t, 0, s.ActiveRequests())
}

func TestPermit_ReacquireRespectsHeadroomAndClose(t *testing.T) {
	s := testServer(t, 2)
	a, _ := s.newPermit()
	a.Release()
	require.True(t, a.TryReacquire(1), "one of two tokens is free and one stays for fresh requests")
	require.False(t, a.TryReacquire(1), "already held")

	b, ok := s.newPermit()
	require.True(t, ok, "headroom was kept for a fresh request")
	a.Release()
	require.False(t, a.TryReacquire(1), "no token may be taken that leaves no headroom")
	b.Close()
	require.True(t, a.TryReacquire(1))

	a.Release()
	a.Close()
	require.False(t, a.TryReacquire(0), "a closed permit never holds a token again")
	require.Equal(t, 0, s.ActiveRequests())
}

func TestPermit_ConcurrentReleaseAndClose(t *testing.T) {
	s := testServer(t, 4)
	for i := 0; i < 200; i++ {
		p, _ := s.newPermit()
		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p.Release()
				p.TryReacquire(0)
				p.Close()
			}()
		}
		wg.Wait()
		require.Equal(t, 0, s.ActiveRequests())
	}
}

func TestPermit_ReleaseHookRunsOutsideLocks(t *testing.T) {
	s := testServer(t, 2)
	calls := 0
	s.SetReleaseHook(func() {
		calls++
		_ = s.ActiveRequests() // would deadlock if called under the pool lock
	})
	p, _ := s.newPermit()
	p.Close()
	require.Equal(t, 1, calls)
}

// A worker which outlives the timeout middleware keeps the owned permit until it stops; a worker
// parked without a token cannot get one back after the response; a request which did not opt in
// keeps the legacy accounting (token freed when the response is written).
func TestTimeoutMiddleware_OwnedPermitWaitsForWorker(t *testing.T) {
	s := testServer(t, 2)
	mw := TimeoutMiddleware(30*time.Millisecond, s.logger)
	run := func(owned bool) (*Permit, chan struct{}, chan struct{}) {
		p, _ := s.newPermit()
		if owned {
			p.owned = true
		}
		ctx := ContextWithPermit(context.Background(), p)
		started, finish := make(chan struct{}), make(chan struct{})
		resp := mw(ctx, &Request{ID: 1}, func(ctx context.Context, r *Request) *Response {
			close(started)
			<-finish
			return NewResponse("late", r.ID)
		})
		<-started
		require.NotNil(t, resp.Error, "deadline response")
		p.Close() // what ServeHTTP does when the response is written
		return p, started, finish
	}

	p, _, finish := run(true)
	require.True(t, p.Held(), "proof work still runs: token stays")
	require.Equal(t, 1, s.ActiveRequests())
	close(finish)
	require.Eventually(t, func() bool { return s.ActiveRequests() == 0 }, time.Second, time.Millisecond)

	p, _, finish = run(false)
	require.Equal(t, 0, s.ActiveRequests(), "legacy request: released with the response")
	close(finish)
}

func TestTimeoutMiddleware_ParkedWorkerCannotReacquireAfterResponse(t *testing.T) {
	s := testServer(t, 2)
	mw := TimeoutMiddleware(30*time.Millisecond, s.logger)
	p, _ := s.newPermit()
	p.owned = true
	got := make(chan bool, 1)
	release := make(chan struct{})
	resp := mw(ContextWithPermit(context.Background(), p), &Request{ID: 1}, func(ctx context.Context, r *Request) *Response {
		p.Release() // parked
		<-release
		got <- p.TryReacquire(0)
		return NewResponse("late", r.ID)
	})
	require.NotNil(t, resp.Error)
	p.Close()
	close(release)
	require.False(t, <-got)
	require.Eventually(t, func() bool { return s.ActiveRequests() == 0 }, time.Second, time.Millisecond)
}

// A JSON-RPC array at the top level stays unsupported, and one request uses one slot.
func TestServeHTTP_TopLevelArrayRejectedAndOneSlotPerRequest(t *testing.T) {
	s := testServer(t, 1)
	var seen int
	s.RegisterMethod("m", func(ctx context.Context, _ json.RawMessage) (interface{}, *Error) {
		seen = s.ActiveRequests()
		require.NotNil(t, PermitFromContext(ctx))
		return "ok", nil
	})
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`[{"jsonrpc":"2.0","id":1,"method":"m"},{"jsonrpc":"2.0","id":2,"method":"m"}]`)))
	require.Contains(t, rec.Body.String(), `"error"`)
	require.Zero(t, seen)
	require.Zero(t, s.ActiveRequests())

	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"m"}`)))
	require.Contains(t, rec.Body.String(), `"result":"ok"`)
	require.Equal(t, 1, seen)
	require.Zero(t, s.ActiveRequests())
}

func TestServeHTTP_SaturatedRejectsAsBefore(t *testing.T) {
	s := testServer(t, 1)
	hold, _ := s.newPermit()
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"m"}`)))
	require.Contains(t, rec.Body.String(), `-32006`)
	hold.Close()
	s0 := testServer(t, 0)
	rec = httptest.NewRecorder()
	s0.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"m"}`)))
	require.Contains(t, rec.Body.String(), `-32006`, "limit 0 rejects everything, as the channel semaphore did")
}

func TestServeHTTP_OwnershipPredicateMarksOnlyOptInRequests(t *testing.T) {
	s := testServer(t, 2)
	s.SetOwnershipPredicate("m", func(p []byte) bool { return strings.Contains(string(p), "waitMs") })
	var owned []bool
	s.RegisterMethod("m", func(ctx context.Context, _ json.RawMessage) (interface{}, *Error) {
		owned = append(owned, PermitFromContext(ctx).Owned())
		return "ok", nil
	})
	for _, p := range []string{`{}`, `{"waitMs":0}`} {
		s.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"m","params":`+p+`}`)))
	}
	require.Equal(t, []bool{false, true}, owned)
}

func TestError_WithWaited(t *testing.T) {
	enc := func(e *Error) string { b, err := json.Marshal(e); require.NoError(t, err); return string(b) }
	require.JSONEq(t, `{"code":-32008,"message":"m","data":{"reason":"r","detail":"d","retryable":true,"waitedMs":7}}`,
		enc(NewDetailedError(-32008, "m", "r", "d", true, nil).WithWaited(7)))
	require.Equal(t, `{"code":-32008,"message":"m","data":{"reason":"r","detail":"d","retryable":true,"indexes":{"pending":[0]},"waitedMs":8000}}`,
		enc(NewDetailedErrorWithIndexes(-32008, "m", "r", "d", true, map[string][]int{"pending": {0}}).WithWaited(8000)), "existing members first, echo last")
	require.Equal(t, `{"code":-32000,"message":"m","data":{"waitedMs":0}}`, enc(NewValidationError("m").WithWaited(0)), "absent data becomes an object")
	require.Equal(t, `{"code":-32603,"message":"m","data":{"originalData":"db down","waitedMs":0}}`, enc(NewError(-32603, "m", "db down").WithWaited(0)))
	require.Equal(t, `{"code":-32603,"message":"m","data":{"originalData":[1,2],"waitedMs":3}}`, enc(NewError(-32603, "m", []int{1, 2}).WithWaited(3)))
	require.Equal(t, `{"code":1,"message":"m","data":{"waitedMs":4}}`, enc(NewError(1, "m", map[string]int{}).WithWaited(4)), "empty object")
	// shared predefined errors are not modified
	_ = ErrConcurrencyLimit.WithWaited(9)
	require.Nil(t, ErrConcurrencyLimit.Data)
	require.Equal(t, `{"code":-32006,"message":"Concurrency limit exceeded","data":{"waitedMs":5}}`, enc(ErrConcurrencyLimit.WithWaited(5).WithWaited(5)))
	var nilErr *Error
	require.Nil(t, nilErr.WithWaited(1))
}

type boundedEcho struct{ ms uint64 }

func (b boundedEcho) AppendJSONBounded(dst []byte, max int) ([]byte, error) {
	return nil, ErrResponseTooLarge
}
func (b boundedEcho) WaitedEcho() (uint64, bool) { return b.ms, true }

// The response_bytes replacement of an oversized result keeps the result's wait echo.
func TestResponseTooLargeFallbackKeepsTheEcho(t *testing.T) {
	s := testServer(t, 2)
	s.RegisterMethodWithLimits("m", func(ctx context.Context, _ json.RawMessage) (interface{}, *Error) {
		return boundedEcho{ms: 7}, nil
	}, MethodLimits{MaxResponseBytes: 1000})
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"m"}`)))
	require.Contains(t, rec.Body.String(), `"detail":"response_bytes","retryable":false,"limit":1000,"waitedMs":7}`)
}
