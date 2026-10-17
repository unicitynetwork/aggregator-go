package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// lpBatchRig is a batch environment behind the production gateway with long-poll wired.
type lpBatchRig struct {
	e     *batchEnv
	reg   *longpoll.Registry
	url   string
	ids   []api.StateID // included in the pinned view
	later []api.StateID // stored, but only in the next view
	st    *lockedStatus
}

// lockedStatus is a status source the tests may change while requests are served.
type lockedStatus struct {
	mu sync.Mutex
	m  map[string]api.ProofStatus
}

func (s *lockedStatus) ProofStatus(id api.StateID) api.ProofStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.m[id.String()]; ok {
		return st
	}
	return api.ProofStatusUnknown
}

func (s *lockedStatus) set(id api.StateID, st api.ProofStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[id.String()] = st
}

func (s *lockedStatus) clear(id api.StateID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, id.String())
}

func newLPBatchRig(t *testing.T, waiters, c int) *lpBatchRig {
	t.Helper()
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(4, 1000)
	e.finalize([]byte("pv1"))
	later := e.addRecords(16, 1000) // in the tree and the record store, not in the pinned view
	st := &lockedStatus{m: map[string]api.ProofStatus{}}
	e.svc.status = st
	reg := longpoll.New(longpoll.Config{MaxWaiters: waiters, Concurrency: c})
	co := &longpoll.Coordinator{Reg: reg, MaxWait: 3 * time.Second, Budget: 30 * time.Second,
		Reserve: 100 * time.Millisecond, ResumeReserveOverride: 300 * time.Millisecond}
	e.svc.longPoll = co
	url := goldenGateway(t, &AggregatorService{}, e.svc.HandleRPC, co)
	return &lpBatchRig{e: e, reg: reg, url: url, ids: ids, later: later, st: st}
}

func (r *lpBatchRig) call(t *testing.T, ids []api.StateID, extra string) string {
	t.Helper()
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":5,"method":"get_inclusion_proofs.v1","params":{"stateIds":[%s]%s}}`, batchIDsJSON(ids...), extra)
	return goldenPost(t, r.url, body)
}

// publish makes the stored-but-unpublished states servable and wakes the waiters, the way a
// finalization does (view published, then the signal).
func (r *lpBatchRig) publish() {
	r.e.finalize([]byte("pv2"))
	r.reg.Notify()
}

func waitedMsOf(t *testing.T, body string) (uint64, bool) {
	var env struct {
		Result struct {
			WaitedMs *uint64 `json:"waitedMs"`
		} `json:"result"`
		Error struct {
			Data struct {
				WaitedMs *uint64 `json:"waitedMs"`
			} `json:"data"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &env))
	if env.Result.WaitedMs != nil {
		return *env.Result.WaitedMs, true
	}
	if env.Error.Data.WaitedMs != nil {
		return *env.Error.Data.WaitedMs, true
	}
	return 0, false
}

func TestLongPollBatch_NoWaitUnchangedAndZeroIsExplicit(t *testing.T) {
	r := newLPBatchRig(t, 8, 100)
	plain := r.call(t, r.ids[:2], "")
	require.NotContains(t, plain, "waitedMs")
	zero := r.call(t, r.ids[:2], `,"waitMs":0`)
	require.Contains(t, zero, `"waitedMs":0}`, "present zero opts into metadata")
	// the same proofs; the echo is the only difference, appended last
	require.Equal(t, plain, strings.Replace(zero, `,"waitedMs":0`, "", 1))
}

func TestLongPollBatch_PendingWakesOnPublicationAndSucceeds(t *testing.T) {
	r := newLPBatchRig(t, 8, 100)
	req := append([]api.StateID{r.ids[0]}, r.later[0])
	got := make(chan string, 1)
	go func() { got <- r.call(t, req, `,"waitMs":3000`) }()
	require.Eventually(t, func() bool {
		return r.reg.Stats().Reserved == 1 && r.e.catalog.Stats().ActivePins == 0 && len(r.e.svc.slots) == 0
	}, 2*time.Second, time.Millisecond, "once the first read returned, the parked wait pins no view, lease or batch slot")
	for i := 0; i < 20; i++ { // and keeps holding none while it sleeps
		require.Zero(t, r.e.catalog.Stats().ActivePins)
		require.Zero(t, len(r.e.svc.slots))
		time.Sleep(3 * time.Millisecond)
	}
	r.publish()
	body := <-got
	require.Contains(t, body, `"result":{"proofs":[`)
	require.NotContains(t, body, `"error"`)
	w, ok := waitedMsOf(t, body)
	require.True(t, ok)
	require.GreaterOrEqual(t, w, uint64(60))
	require.Contains(t, body, `"partitionView":"`)
	require.Zero(t, r.e.catalog.Stats().ActivePins)
	require.Zero(t, r.reg.Stats().Reserved)
}

func TestLongPollBatch_TimeoutIsTheOrdinaryPendingErrorWithEcho(t *testing.T) {
	r := newLPBatchRig(t, 8, 100)
	r.e.svc.longPoll.MaxWait = 250 * time.Millisecond
	start := time.Now()
	body := r.call(t, []api.StateID{r.later[0], r.ids[1], r.later[1]}, `,"waitMs":9000`)
	require.Less(t, time.Since(start), 2*time.Second)
	var env struct {
		Error struct {
			Code int `json:"code"`
			Data struct {
				Reason   string           `json:"reason"`
				Detail   string           `json:"detail"`
				Retry    bool             `json:"retryable"`
				Indexes  map[string][]int `json:"indexes"`
				WaitedMs uint64           `json:"waitedMs"`
			} `json:"data"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &env))
	require.Equal(t, -32008, env.Error.Code)
	require.Equal(t, "state_pending", env.Error.Data.Detail)
	require.True(t, env.Error.Data.Retry)
	require.Equal(t, map[string][]int{"pending": {0, 2}}, env.Error.Data.Indexes)
	require.InDelta(t, 250, float64(env.Error.Data.WaitedMs), 120)
	require.Zero(t, r.e.catalog.Stats().ActivePins)
}

func TestLongPollBatch_UnknownOrExpiredEndsTheWaitAtOnceEvenWithPendingEntries(t *testing.T) {
	r := newLPBatchRig(t, 8, 100)
	gone := unseen(r.e, 2)
	r.st.set(gone[1], api.ProofStatusExpired)

	start := time.Now()
	unknown := r.call(t, []api.StateID{r.later[0], gone[0]}, `,"waitMs":3000`)
	require.Less(t, time.Since(start), time.Second)
	require.Contains(t, unknown, `"detail":"state_unknown"`)
	require.Contains(t, unknown, `"indexes":{"pending":[0],"unknown":[1]},"waitedMs":0}`)

	expired := r.call(t, []api.StateID{r.later[0], gone[1]}, `,"waitMs":3000`)
	require.Contains(t, expired, `"detail":"state_expired"`)
	require.Contains(t, expired, `"waitedMs":0}`)
	require.Zero(t, r.reg.Stats().Reserved)
}

func TestLongPollBatch_AParkedWaitEndsWhenAnEntryTurnsExpiredOrUnknown(t *testing.T) {
	for name, st := range map[string]api.ProofStatus{"expired": api.ProofStatusExpired, "unknown": api.ProofStatusUnknown} {
		t.Run(name, func(t *testing.T) {
			r := newLPBatchRig(t, 8, 100)
			gone := unseen(r.e, 1)
			r.st.set(gone[0], api.ProofStatusPending) // admitted here, nothing stored yet
			got := make(chan string, 1)
			go func() { got <- r.call(t, []api.StateID{r.later[0], gone[0]}, `,"waitMs":3000`) }()
			require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 1 }, 2*time.Second, time.Millisecond)

			// an irrelevant publication does not finish the wait
			r.reg.Notify()
			time.Sleep(60 * time.Millisecond)
			select {
			case b := <-got:
				t.Fatalf("finished by an irrelevant event: %s", b)
			default:
			}
			r.st.clear(gone[0])
			if st == api.ProofStatusExpired {
				r.st.set(gone[0], st)
			}
			r.reg.Notify() // the tracker's drop/sweep signal
			body := <-got
			require.Contains(t, body, `"detail":"state_`+name+`"`)
			require.Contains(t, body, `"pending":[0]`, "the other pending entry is still reported")
		})
	}
}

func TestLongPollBatch_CapacityZeroAndFullServeImmediatelyWithZeroEcho(t *testing.T) {
	for name, tc := range map[string][2]int{"cap 0": {0, 100}, "C=1": {8, 1}} {
		r := newLPBatchRig(t, tc[0], tc[1])
		start := time.Now()
		body := r.call(t, []api.StateID{r.later[0]}, `,"waitMs":3000`)
		require.Less(t, time.Since(start), time.Second, name)
		require.Contains(t, body, `"detail":"state_pending"`, name)
		require.Contains(t, body, `"waitedMs":0}`, name)
	}
	r := newLPBatchRig(t, 1, 100)
	got := make(chan string, 1)
	go func() { got <- r.call(t, []api.StateID{r.later[0]}, `,"waitMs":3000`) }()
	require.Eventually(t, func() bool { return r.reg.Stats().Reserved == 1 }, 2*time.Second, time.Millisecond)
	full := r.call(t, []api.StateID{r.later[1]}, `,"waitMs":3000`)
	require.Contains(t, full, `"detail":"state_pending"`)
	require.Contains(t, full, `"waitedMs":0}`, "cap shedding is an ordinary immediate outcome, never an error of its own")
	r.publish()
	<-got
}

func TestLongPollBatch_ValidationErrorsAndEcho(t *testing.T) {
	r := newLPBatchRig(t, 8, 100)
	one := []api.StateID{r.ids[0]}
	root := strings.Repeat("cd", 32)
	bad := func(extra string) string { return r.call(t, one, extra) }

	malformed := `"detail":"malformed_params","retryable":false`
	// a recognized wait-bearing object echoes zero, an invalid waitMs does not
	withEcho := bad(`,"waitMs":1000,"foo":1`)
	require.Contains(t, withEcho, malformed+`,"waitedMs":0}`)
	exactRoot := bad(`,"stateRoot":"` + root + `","waitMs":0`)
	require.Contains(t, exactRoot, malformed+`,"waitedMs":0}`)
	for _, extra := range []string{`,"waitMs":"5"`, `,"waitMs":-1`, `,"waitMs":1.5`, `,"waitMs":null`, `,"waitMs":1,"waitMs":2`} {
		b := bad(extra)
		require.Contains(t, b, malformed, extra)
		require.NotContains(t, b, "waitedMs", extra)
	}
	// invalid state ID with a valid wait: the specific validation error, echoed
	b := goldenPost(t, r.url, `{"jsonrpc":"2.0","id":5,"method":"get_inclusion_proofs.v1","params":{"stateIds":["abcd"],"waitMs":1000}}`)
	require.Contains(t, b, `"detail":"invalid_state_id"`)
	require.Contains(t, b, `"waitedMs":0}`)
	// no-wait validation error: legacy bytes
	b = goldenPost(t, r.url, `{"jsonrpc":"2.0","id":5,"method":"get_inclusion_proofs.v1","params":{"stateIds":["abcd"]}}`)
	require.NotContains(t, b, "waitedMs")
}

// The bounded encoder is byte-identical to encoding/json with the echo, and counts its bytes.
func TestLongPollBatch_BoundedEncoderEqualsJSONWithEcho(t *testing.T) {
	r := newLPBatchRig(t, 8, 100)
	resp, berr := r.e.svc.GetInclusionProofsV1(t.Context(), r.e.request(r.ids, nil))
	require.Nil(t, berr)
	for _, ms := range []uint64{0, 7, 20000} {
		m := ms
		resp.WaitedMs = &m
		want, err := json.Marshal(resp)
		require.NoError(t, err)
		got, err := resp.AppendJSONBounded(nil, len(want))
		require.NoError(t, err)
		require.Equal(t, string(want), string(got))
		_, err = resp.AppendJSONBounded(nil, len(want)-1)
		require.Error(t, err, "the echo counts against the limit")
	}
	resp.WaitedMs = nil
	plain, _ := json.Marshal(resp)
	require.NotContains(t, string(plain), "waitedMs", "never inserted on no-wait calls")
	require.NotContains(t, string(plain), "waitedMs")
}
