package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

type mapStatus struct{ m map[string]api.ProofStatus }

func (s mapStatus) ProofStatus(id api.StateID) api.ProofStatus {
	if st, ok := s.m[id.String()]; ok {
		return st
	}
	return api.ProofStatusUnknown
}

// unseen returns n distinct state IDs which have no stored record at this node.
func unseen(e *batchEnv, n int) []api.StateID {
	var out []api.StateID
	for i := 0; i < n; i++ {
		id := make(api.StateID, 32)
		id[0], id[1], id[31] = 0xee, byte(len(e.ids)), byte(i)
		out = append(out, id)
	}
	return out
}

func statusEnv(t *testing.T) (*batchEnv, []api.StateID, mapStatus) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(6, 1000)
	e.finalize([]byte("pv"))
	st := mapStatus{m: map[string]api.ProofStatus{}}
	e.svc.status = st
	return e, ids, st
}

func requireIndexes(t *testing.T, err *BatchError, want map[string][]int) {
	t.Helper()
	require.Equal(t, want, err.Indexes)
	data, jerr := json.Marshal(err.RPCError().Data)
	require.NoError(t, jerr)
	var decoded struct {
		Indexes map[string][]int `json:"indexes"`
	}
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, want, decoded.Indexes, "the offending indexes are in the error data")
}

func TestBatchStatus_LatestPendingIsRetryable(t *testing.T) {
	e, ids, st := statusEnv(t)
	gone := unseen(e, 2)
	st.m[gone[0].String()], st.m[gone[1].String()] = api.ProofStatusPending, api.ProofStatusPending
	req := []api.StateID{ids[0], gone[0], ids[1], gone[1]}

	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(req, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_pending")
	require.True(t, err.Retryable)
	requireIndexes(t, err, map[string][]int{"pending": {1, 3}})
}

func TestBatchStatus_ExpiredAndUnknownAreNotRetryable(t *testing.T) {
	e, ids, st := statusEnv(t)
	gone := unseen(e, 2)
	st.m[gone[0].String()] = api.ProofStatusExpired

	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{ids[0], gone[0]}, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_expired")
	require.False(t, err.Retryable)
	requireIndexes(t, err, map[string][]int{"expired": {1}})

	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{gone[1], ids[0]}, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_unknown")
	require.False(t, err.Retryable)
	requireIndexes(t, err, map[string][]int{"unknown": {0}})
}

// Mixed outcomes: unknown takes precedence over expired, which takes precedence over pending.
func TestBatchStatus_MixedOutcomePrecedence(t *testing.T) {
	e, ids, st := statusEnv(t)
	g := unseen(e, 3)
	st.m[g[0].String()], st.m[g[1].String()] = api.ProofStatusPending, api.ProofStatusExpired // g[2] unknown

	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{g[0], ids[0], g[1], g[2]}, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_unknown")
	require.False(t, err.Retryable)
	requireIndexes(t, err, map[string][]int{"pending": {0}, "expired": {2}, "unknown": {3}})

	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{g[0], g[1]}, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_expired")
	requireIndexes(t, err, map[string][]int{"pending": {0}, "expired": {1}})
}

// A finalized record the pinned view does not contain yet is pending (retry), not unknown;
// the status memory cannot make it expired.
func TestBatchStatus_RecordNewerThanThePinnedViewIsPending(t *testing.T) {
	e, ids, st := statusEnv(t)
	later := e.addRecordNoLeaf(1000) // stored record, leaf not in the pinned root

	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{ids[0], later, ids[1]}, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_pending")
	require.True(t, err.Retryable)
	requireIndexes(t, err, map[string][]int{"pending": {1}})

	// the (volatile, advisory) status memory never overrules a stored finalized record
	for _, wrong := range []api.ProofStatus{api.ProofStatusExpired, api.ProofStatusUnknown} {
		st.m[later.String()] = wrong
		_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{later}, nil))
		requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_pending")
		require.True(t, err.Retryable)
	}
}

func TestBatchStatus_ExactRootAbsentLeafIsNotInRoot(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	old := e.addRecords(4, 1000)
	oldRoot := e.finalize([]byte("pv1"))
	more := e.addRecords(2, 1000) // in the next root only
	e.finalize([]byte("pv2"))
	e.svc.status = mapStatus{m: map[string]api.ProofStatus{more[0].String(): api.ProofStatusPending}}
	gone := unseen(e, 1)

	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{old[0], more[0], old[1], more[1], gone[0]}, oldRoot))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_not_in_root")
	require.False(t, err.Retryable, "waiting cannot make a leaf appear in a fixed root")
	requireIndexes(t, err, map[string][]int{"not_in_root": {1, 3, 4}})
}

// No status source (or no knowledge): unknown, never a silent success.
func TestBatchStatus_NoSourceMeansUnknown(t *testing.T) {
	e, ids, _ := statusEnv(t)
	e.svc.status = nil
	g := unseen(e, 1)
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{ids[0], g[0]}, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_unknown")
}

// Classification is memory only: the batch keeps its constant database round trips, and
// a failing batch uses no more than the successful one.
func TestBatchStatus_NoExtraDatabaseCommands(t *testing.T) {
	e, ids, st := statusEnv(t)
	ids = append(ids, e.addRecords(250, 1000)...)
	e.finalize([]byte("pv3"))
	g := unseen(e, 3)
	for _, id := range g {
		st.m[id.String()] = api.ProofStatusPending
	}
	later := e.addRecordNoLeaf(1000)

	cmds := func() int { e.recs.mu.Lock(); defer e.recs.mu.Unlock(); return e.recs.sizeQ + e.recs.calls }
	reset := func() { e.recs.mu.Lock(); e.recs.sizeQ, e.recs.calls = 0, 0; e.recs.mu.Unlock() }

	reset()
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids[:256], nil))
	require.Nil(t, err)
	require.Equal(t, 2, cmds(), "a successful batch of 256 is exactly 2 commands")

	reset() // only states without a record: classified from the size projection alone
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(g, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_pending")
	require.Equal(t, 1, cmds())

	reset() // mixed: the present states are examined too (all offending indexes), still 2 commands
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(append(append([]api.StateID{}, ids[:200]...), g...), nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_pending")
	require.Equal(t, 2, cmds())

	reset() // records newer than the view: still the same two commands
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(append(append([]api.StateID{}, ids[:255]...), later), nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_pending")
	require.Equal(t, 2, cmds())
}

func TestBatchStatus_ServedCounters(t *testing.T) {
	e, ids, st := statusEnv(t)
	g := unseen(e, 3)
	st.m[g[0].String()], st.m[g[1].String()] = api.ProofStatusPending, api.ProofStatusExpired
	c := func(s string) float64 {
		return testutil.ToFloat64(metrics.ProofStatusServedTotal.WithLabelValues("get_inclusion_proofs.v1", s))
	}
	p, x, u := c("pending"), c("expired"), c("unknown")
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{ids[0], g[0], g[1], g[2]}, nil))
	require.NotNil(t, err)
	require.Equal(t, 1.0, c("pending")-p)
	require.Equal(t, 1.0, c("expired")-x)
	require.Equal(t, 1.0, c("unknown")-u)
}

func TestBatchStatus_RPCErrorShape(t *testing.T) {
	e, ids, st := statusEnv(t)
	g := unseen(e, 1)
	st.m[g[0].String()] = api.ProofStatusPending
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{ids[0], g[0]}, nil))
	raw, jerr := json.Marshal(err.RPCError())
	require.NoError(t, jerr)
	require.JSONEq(t, `{"code":-32008,"message":"Proof unavailable","data":{"reason":"proof_unavailable","detail":"state_pending","retryable":true,"indexes":{"pending":[1]}}}`, string(raw))
}

// clearingStatus reports pending until the record query has run, then unknown: what the status
// tracker does when the state finalizes (markProofsReady clears the hint) while the request runs.
type clearingStatus struct {
	cleared *bool
	id      api.StateID
}

func (s clearingStatus) ProofStatus(id api.StateID) api.ProofStatus {
	if id.String() == s.id.String() && !*s.cleared {
		return api.ProofStatusPending
	}
	return api.ProofStatusUnknown
}

type clearAfterSizes struct {
	BatchRecordSource
	cleared *bool
}

func (c clearAfterSizes) GetStoredSizesByStateIDs(ctx context.Context, ids []api.StateID) (map[string]int64, error) {
	out, err := c.BatchRecordSource.GetStoredSizesByStateIDs(ctx, ids)
	*c.cleared = true // the state finalizes right after the record query found nothing
	return out, err
}

// A state which finalizes while the request runs was pending when its hint was read: the batch
// is retryable state_pending, never the non-retryable state_unknown from a hint cleared by that
// very finalization.
func TestBatchStatus_HintIsSnapshottedBeforeTheRecordQuery(t *testing.T) {
	e, ids, _ := statusEnv(t)
	gone := unseen(e, 1)[0]
	cleared := false
	e.svc.status = clearingStatus{cleared: &cleared, id: gone}
	e.svc.records = clearAfterSizes{BatchRecordSource: e.svc.records, cleared: &cleared}

	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{ids[0], gone}, nil))
	require.True(t, cleared, "the hook ran")
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_pending")
	require.True(t, err.Retryable)
	requireIndexes(t, err, map[string][]int{"pending": {1}})
}
