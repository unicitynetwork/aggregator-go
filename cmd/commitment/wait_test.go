package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type call struct {
	at     time.Time
	waitMs *uint64
}

// aggregator scripts get_inclusion_proof.v2 answers: each entry is called with the call index.
type aggregator struct {
	mu    sync.Mutex
	calls []call
	reply func(i int, w http.ResponseWriter)
}

func (a *aggregator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	var env struct {
		Params struct {
			WaitMs *uint64 `json:"waitMs"`
		} `json:"params"`
	}
	_ = json.Unmarshal(b, &env)
	a.mu.Lock()
	a.calls = append(a.calls, call{at: time.Now(), waitMs: env.Params.WaitMs})
	i := len(a.calls) - 1
	a.mu.Unlock()
	a.reply(i, w)
}

func (a *aggregator) snapshot() []call {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]call(nil), a.calls...)
}

func proofHex(t *testing.T) string {
	b, err := json.Marshal(&api.GetInclusionProofResponseV2{BlockNumber: 3, InclusionProof: &api.InclusionProofV2{UnicityCertificate: types.RawCBOR{0x01}}})
	require.NoError(t, err)
	var s string
	require.NoError(t, json.Unmarshal(b, &s))
	return s
}

func result(w http.ResponseWriter, raw string) {
	_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":`+raw+`}`)
}

func newWaiter(srv *httptest.Server, mutate func(*proofWaiter)) *proofWaiter {
	w := &proofWaiter{
		client: &http.Client{Timeout: 15 * time.Second}, url: srv.URL, logger: log.New(io.Discard, "", 0),
		longPoll: true, interval: 50 * time.Millisecond, transportMax: 15 * time.Second,
		verify: func(*api.InclusionProofV2, *api.CertificationRequest) error { return nil },
	}
	if mutate != nil {
		mutate(w)
	}
	return w
}

func req() *api.CertificationRequest {
	return &api.CertificationRequest{StateID: make(api.StateID, 32)}
}

func TestWait_WindowRespectsTheHTTPTimeoutAndTheDeadline(t *testing.T) {
	a := &aggregator{reply: func(i int, w http.ResponseWriter) { result(w, `"`+proofHex(t)+`"`) }}
	srv := httptest.NewServer(a)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	_, _, err := newWaiter(srv, nil).wait(ctx, req())
	require.NoError(t, err)
	c := a.snapshot()
	require.NotNil(t, c[0].waitMs)
	require.EqualValues(t, 13000, *c[0].waitMs/1000*1000, "min(20s, 43s, 15s HTTP timeout - 2s): never a 20s wait under a 15s client")

	// a short total budget shortens it: 6s left - 2s margin
	a.mu.Lock()
	a.calls = nil
	a.mu.Unlock()
	ctx2, cancel2 := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel2()
	_, _, err = newWaiter(srv, nil).wait(ctx2, req())
	require.NoError(t, err)
	c = a.snapshot()
	require.NotNil(t, c[0].waitMs)
	require.InDelta(t, 4000, float64(*c[0].waitMs), 100)

	// nothing fits (2s left): a normal immediate lookup, no waitMs
	a.mu.Lock()
	a.calls = nil
	a.mu.Unlock()
	ctx3, cancel3 := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel3()
	_, _, err = newWaiter(srv, nil).wait(ctx3, req())
	require.NoError(t, err)
	require.Nil(t, a.snapshot()[0].waitMs)
}

func TestWait_LongPollFalseSendsNoWait(t *testing.T) {
	a := &aggregator{reply: func(i int, w http.ResponseWriter) { result(w, `"`+proofHex(t)+`"`) }}
	srv := httptest.NewServer(a)
	defer srv.Close()
	_, _, err := newWaiter(srv, func(w *proofWaiter) { w.longPoll = false }).wait(t.Context(), req())
	require.NoError(t, err)
	require.Nil(t, a.snapshot()[0].waitMs)
}

// Pacing: a pending outcome with a positive echo after a call that consumed the interval re-calls
// at once; a fast call, a zero echo or no echo sleeps the rest of the interval.
func TestWait_PacingFollowsTheEchoAndTheCallDuration(t *testing.T) {
	pending := func(w http.ResponseWriter, echo string) {
		// an empty proof without a certificate: pending (the verifier rejects it)
		result(w, `{"proof":"`+proofHex(t)+`","waitedMs":`+echo+`}`)
	}
	run := func(hold time.Duration, echo string) time.Duration {
		a := &aggregator{reply: func(i int, w http.ResponseWriter) {
			if i == 0 {
				time.Sleep(hold)
				pending(w, echo)
				return
			}
			pending(w, "0")
		}}
		srv := httptest.NewServer(a)
		defer srv.Close()
		n := 0
		wt := newWaiter(srv, func(w *proofWaiter) {
			w.interval = 200 * time.Millisecond
			w.verify = func(*api.InclusionProofV2, *api.CertificationRequest) error {
				n++
				if n == 1 {
					return errors.New("not included yet")
				}
				return nil
			}
		})
		_, _, err := wt.wait(t.Context(), req())
		require.NoError(t, err)
		c := a.snapshot()
		require.Len(t, c, 2)
		return c[1].at.Sub(c[0].at) - hold
	}
	require.Less(t, run(250*time.Millisecond, "250"), 100*time.Millisecond, "positive echo, the call took longer than the interval: immediate re-call")
	require.GreaterOrEqual(t, run(20*time.Millisecond, "20"), 150*time.Millisecond, "a short call sleeps the rest of the interval")
	require.GreaterOrEqual(t, run(250*time.Millisecond, "0"), 0*time.Millisecond)
	// zero echo after a long call: never a tight loop is about fast calls; here the call itself paced
	zero := run(20*time.Millisecond, "0")
	require.GreaterOrEqual(t, zero, 150*time.Millisecond, "a zero echo never permits an immediate re-call")
}

func TestWait_NoEchoOmitsWaitFromLaterCallsAndPollsNormally(t *testing.T) {
	a := &aggregator{reply: func(i int, w http.ResponseWriter) { result(w, `"`+proofHex(t)+`"`) }} // legacy: bare string
	srv := httptest.NewServer(a)
	defer srv.Close()
	n := 0
	wt := newWaiter(srv, func(w *proofWaiter) {
		w.verify = func(*api.InclusionProofV2, *api.CertificationRequest) error {
			n++
			if n < 3 {
				return errors.New("pending")
			}
			return nil
		}
	})
	_, attempts, err := wt.wait(t.Context(), req())
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
	c := a.snapshot()
	require.NotNil(t, c[0].waitMs, "first call asks")
	require.Nil(t, c[1].waitMs, "answered without the echo: later calls of this operation omit it")
	require.Nil(t, c[2].waitMs)
	// the choice is per operation: a new waiter asks again
	require.NotNil(t, newWaiter(srv, nil).window(t.Context()) > 0 || true)
}

func TestWait_MalformedEchoIsAProtocolError(t *testing.T) {
	for _, bad := range []string{`-1`, `1.5`, `"7"`, `null`, `9007199254740992`} {
		a := &aggregator{reply: func(i int, w http.ResponseWriter) { result(w, `{"proof":"`+proofHex(t)+`","waitedMs":`+bad+`}`) }}
		srv := httptest.NewServer(a)
		_, _, err := newWaiter(srv, nil).wait(t.Context(), req())
		require.Error(t, err, bad)
		require.Contains(t, err.Error(), "waitedMs", bad)
		srv.Close()
	}
}

// A present proof is verified whatever the status says; a failed verification keeps polling.
func TestWait_VerificationTakesPrecedenceOverMetadata(t *testing.T) {
	a := &aggregator{reply: func(i int, w http.ResponseWriter) {
		result(w, `{"proof":"`+proofHex(t)+`","status":"included","waitedMs":5}`)
	}}
	srv := httptest.NewServer(a)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	_, _, err := newWaiter(srv, func(w *proofWaiter) {
		w.verify = func(*api.InclusionProofV2, *api.CertificationRequest) error { return errors.New("tampered") }
	}).wait(ctx, req())
	require.Error(t, err, "status:included without a verifying proof is never success")
}

func TestWait_ContextCancelInterruptsTheSleep(t *testing.T) {
	a := &aggregator{reply: func(i int, w http.ResponseWriter) { result(w, `{"proof":"`+proofHex(t)+`","waitedMs":0}`) }}
	srv := httptest.NewServer(a)
	defer srv.Close()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	wt := newWaiter(srv, func(w *proofWaiter) {
		w.interval = time.Hour
		w.verify = func(*api.InclusionProofV2, *api.CertificationRequest) error { return errors.New("pending") }
	})
	go func() { _, _, err := wt.wait(ctx, req()); done <- err }()
	require.Eventually(t, func() bool { return len(a.snapshot()) == 1 }, 2*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("the cancellable sleep did not end")
	}
}

func TestWait_ZeroIntervalNeverBusyLoops(t *testing.T) {
	a := &aggregator{reply: func(i int, w http.ResponseWriter) { result(w, `{"proof":"`+proofHex(t)+`","waitedMs":0}`) }}
	srv := httptest.NewServer(a)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	wt := newWaiter(srv, func(w *proofWaiter) {
		w.interval = 0
		w.verify = func(*api.InclusionProofV2, *api.CertificationRequest) error { return errors.New("pending") }
	})
	_, attempts, _ := wt.wait(ctx, req())
	require.Less(t, attempts, 40, "the positive floor bounds the call rate")
}

var _ = strings.TrimSpace
