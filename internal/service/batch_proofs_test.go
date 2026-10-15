package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// batchEnv is a real SMT with n finalized records plus fakes for the view
// source (a real proofview.Catalog over frozen SMT read views), records and shard.
type batchEnv struct {
	t       *testing.T
	tree    *smt.ThreadSafeSMT
	records map[string]*models.AggregatorRecord
	ids     []api.StateID
	catalog *proofview.Catalog
	svc     *BatchProofService
	shard   *fakeShard
	recs    *fakeRecords
	prev    *smt.ReadView
	roots   [][]byte
	blocks  uint64
}

type fakeShard struct{ reject map[string]bool }

func (f *fakeShard) ValidateShardID(id api.StateID) error {
	if f.reject[string(id)] {
		return errors.New("wrong shard")
	}
	return nil
}

type fakeRecords struct {
	sizes   map[string]int64 // overrides the computed stored size, by StateID.String()
	loaded  int              // records returned by GetFinalizedByStateIDs (full materializations)
	mu      sync.Mutex
	records map[string]*models.AggregatorRecord // by string(id)
	err     error
	delay   time.Duration
	calls   int // GetFinalizedByStateIDs calls
	sizeQ   int // GetStoredSizesByStateIDs calls
}

func (f *fakeRecords) GetStoredSizesByStateIDs(ctx context.Context, ids []api.StateID) (map[string]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sizeQ++
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]int64, len(ids))
	for _, id := range ids {
		r := f.records[string(id)]
		if r == nil {
			continue
		}
		if n, ok := f.sizes[id.String()]; ok {
			out[id.String()] = n
			continue
		}
		out[id.String()] = int64(200 + len(r.CertificationData.Witness) + len(r.CertificationData.OwnerPredicate.Params))
	}
	return out, nil
}

func (f *fakeRecords) GetFinalizedByStateIDs(ctx context.Context, ids []api.StateID) (map[string]*models.AggregatorRecord, error) {
	f.mu.Lock()
	f.calls++
	d, err := f.delay, f.err
	out := make(map[string]*models.AggregatorRecord, len(ids))
	for _, id := range ids {
		if r := f.records[string(id)]; r != nil {
			out[id.String()] = r
		}
	}
	f.loaded += len(out)
	f.mu.Unlock()
	if d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return out, err
}

// readerView adapts a frozen SMT view to backend.ReadView.
type readerView struct{ v *smt.ReadView }

func (r readerView) RootHash() []byte      { return r.v.RootHashRaw() }
func (r readerView) RetainedBytes() uint64 { return smt.ReadViewOverheadBytes + r.v.Introduced() }
func (r readerView) Close() error          { return nil }
func (r readerView) GetInclusionCert(ctx context.Context, key []byte) (*api.InclusionCert, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return r.v.GetInclusionCert(key)
}

func newBatchEnv(t *testing.T, limits proofview.Limits) *batchEnv {
	e := &batchEnv{t: t, tree: smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)),
		records: map[string]*models.AggregatorRecord{}, shard: &fakeShard{reject: map[string]bool{}}}
	var err error
	e.catalog, err = proofview.NewCatalog(limits)
	require.NoError(t, err)
	e.recs = &fakeRecords{records: e.records}
	e.svc = NewBatchProofService(CatalogViewSource(e.catalog), e.recs, e.shard, BatchProofConfig{})
	t.Cleanup(func() { _ = e.catalog.Close() })
	return e
}

func (e *batchEnv) addRecords(n int, refTime uint64) []api.StateID {
	var ids []api.StateID
	for range n {
		i := len(e.ids)
		cd := models.CertificationData{
			OwnerPredicate:  api.Predicate{Engine: 1, Code: []byte{1}, Params: bytes.Repeat([]byte{2}, 33)},
			SourceStateHash: bytes.Repeat([]byte{byte(i), byte(i >> 8), 3}, 11)[:32],
			TransactionHash: api.RequireNewImprintV2(strings.Repeat(fmt.Sprintf("%02x", byte(i+1)), 32)),
			Witness:         bytes.Repeat([]byte{4}, 65),
		}
		id, err := cd.ToAPI().CreateStateID()
		require.NoError(e.t, err)
		path, err := id.GetPath()
		require.NoError(e.t, err)
		require.NoError(e.t, e.tree.AddLeaf(path, api.LeafValue(cd.TransactionHash.DataBytes(), refTime)))
		e.recs.mu.Lock()
		e.records[string(id)] = &models.AggregatorRecord{Version: 2, StateID: id, CertificationData: cd, ReferenceTime: refTime,
			BlockNumber: api.NewBigIntFromUint64(e.blocks + 1)}
		e.recs.mu.Unlock()
		e.ids = append(e.ids, id)
		ids = append(ids, id)
	}
	return ids
}

// finalize publishes the current tree as the next block, with an optional partition view.
func (e *batchEnv) finalize(view []byte) []byte {
	e.blocks++
	var prevSeq uint64
	if e.prev != nil {
		prevSeq = e.prev.Seq()
	}
	rv := e.tree.FreezeReadView(prevSeq)
	e.prev = rv
	root := rv.RootHashRaw()
	uc := types.UnicityCertificate{Version: 1, InputRecord: &types.InputRecord{Version: 1, RoundNumber: e.blocks, Hash: root, SummaryValue: []byte{}}}
	ucBytes, err := types.Cbor.Marshal(&uc)
	require.NoError(e.t, err)
	require.NoError(e.t, e.catalog.Publish(context.Background(), proofview.PublishInput{
		Block: proofview.BlockIdentity{Number: api.NewBigIntFromUint64(e.blocks), ConfigLineage: []byte("cfg")},
		Root:  root, Reader: readerView{rv}, Cert: proofview.Certification{UC: ucBytes, PartitionView: view},
	}))
	e.roots = append(e.roots, root)
	return root
}

func (e *batchEnv) request(ids []api.StateID, root []byte) *api.GetInclusionProofsRequestV1 {
	return &api.GetInclusionProofsRequestV1{StateIDs: ids, Root: root}
}

func requireBatchErr(t *testing.T, err *BatchError, code int, reason, detail string) {
	t.Helper()
	require.NotNil(t, err)
	require.Equal(t, code, err.Code, err.Error())
	require.Equal(t, reason, err.Reason)
	require.Equal(t, detail, err.Detail)
}

func TestBatch_LatestAllInOrderAndDecodesAsV2(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(5, 1000)
	root := e.finalize([]byte{0xA1, 0x01})

	// reversed request order is preserved
	req := []api.StateID{ids[4], ids[0], ids[3], ids[1], ids[2]}
	resp, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(req, nil))
	require.Nil(t, err)
	require.Len(t, resp.Proofs, 5)
	require.Equal(t, api.HexBytes{0xA1, 0x01}, resp.PartitionView)

	raw, jerr := json.Marshal(resp)
	require.NoError(t, jerr)
	var wire struct {
		Proofs        []json.RawMessage `json:"proofs"`
		PartitionView string            `json:"partitionView"`
	}
	require.NoError(t, json.Unmarshal(raw, &wire))
	require.Equal(t, "a101", wire.PartitionView)
	for i, el := range wire.Proofs {
		// every element decodes with the existing v2 decoder
		var p api.GetInclusionProofResponseV2
		require.NoError(t, json.Unmarshal(el, &p))
		require.EqualValues(t, 1, p.BlockNumber)
		// and equals the element a v2 single call would serialize
		single := &api.GetInclusionProofResponseV2{BlockNumber: p.BlockNumber, InclusionProof: resp.Proofs[i].InclusionProof}
		want, merr := json.Marshal(single)
		require.NoError(t, merr)
		require.JSONEq(t, string(want), string(el))

		// the proof is for the i-th requested state and folds to the pinned root
		rec := e.records[string(req[i])]
		require.Equal(t, rec.CertificationData.TransactionHash, p.InclusionProof.CertificationData.TransactionHash)
		var cert api.InclusionCert
		require.NoError(t, cert.UnmarshalBinary(p.InclusionProof.CertificateBytes))
		key, kerr := req[i].GetTreeKey()
		require.NoError(t, kerr)
		require.NoError(t, cert.Verify(key, api.LeafValue(rec.CertificationData.TransactionHash.DataBytes(), 1000), root, api.SHA256))
	}
}

func TestBatch_ExactRootOmitsViewAndNeverSubstitutes(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	first := e.addRecords(2, 1000)
	root1 := e.finalize(nil) // exact-root view without sidecar
	second := e.addRecords(2, 1001)
	root2 := e.finalize([]byte{1})

	resp, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(first, root1))
	require.Nil(t, err)
	require.Empty(t, resp.PartitionView)
	raw, _ := json.Marshal(resp)
	require.NotContains(t, string(raw), "partitionView", "exact responses omit the field")

	// a state finalized after the exact root is not in that view: all-or-none
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(append(append([]api.StateID{}, first...), second[0]), root1))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_not_in_root")

	// an unknown root is not retained and nothing newer is substituted
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(first, bytes.Repeat([]byte{9}, 32)))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "view_not_retained")

	// latest serves the newer view including all four
	resp, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(append(append([]api.StateID{}, first...), second...), nil))
	require.Nil(t, err)
	require.Len(t, resp.Proofs, 4)
	_ = root2
}

func TestBatch_LatestRequiresViewAndNeverFallsBack(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(1, 1000)
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "no_finalized_view")

	e.finalize([]byte{1})
	e.addRecords(1, 1001)
	e.finalize(nil) // newest certification has no sidecar
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "partition_view_unavailable")
	require.True(t, err.Retryable)
}

func TestBatch_MissingStateFailsWholeCall(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(3, 1000)
	e.finalize([]byte{1})
	missing := api.StateID(bytes.Repeat([]byte{7}, 32))
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request([]api.StateID{ids[0], missing, ids[1]}, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_unknown")
	require.False(t, err.Retryable == false && err.Reason == "")
}

func TestBatch_WrongShardAndLimits(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(2, 1000)
	e.finalize([]byte{1})
	e.shard.reject[string(ids[1])] = true
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	requireBatchErr(t, err, jsonrpc.InvalidParamsCode, "invalid_request", "wrong_shard")
	require.False(t, err.Retryable)

	many := make([]api.StateID, 257)
	for i := range many {
		many[i] = api.StateID(bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 16))
	}
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(many, nil))
	requireBatchErr(t, err, jsonrpc.LimitExceededCode, "limit_exceeded", "state_count")
	require.Equal(t, 256, *err.Limit)

	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(nil, nil))
	requireBatchErr(t, err, jsonrpc.InvalidParamsCode, "invalid_request", "malformed_params")
}

func TestBatch_ResponseBytesLimit(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(20, 1000)
	e.finalize([]byte{1})
	e.svc.maxBytes = 2000
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	requireBatchErr(t, err, jsonrpc.LimitExceededCode, "limit_exceeded", "response_bytes")
	require.Equal(t, 2000, *err.Limit)
}

func TestBatch_BusyMapping(t *testing.T) {
	limits := proofview.DefaultLimits()
	limits.MaxConcurrentPins = 1
	e := newBatchEnv(t, limits)
	ids := e.addRecords(1, 1000)
	e.finalize([]byte{1})

	// pins exhausted: another lease is held
	held, aerr := e.catalog.AcquireLatest(context.Background())
	require.NoError(t, aerr)
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	requireBatchErr(t, err, jsonrpc.ConcurrencyLimitCode, "busy", "pinned_views")
	require.True(t, err.Retryable)
	held.Release()

	// admission slots exhausted
	e.svc.slots = make(chan struct{}, 1)
	e.svc.slots <- struct{}{}
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	requireBatchErr(t, err, jsonrpc.ConcurrencyLimitCode, "busy", "active_requests")
	<-e.svc.slots

	// deadline: slow storage under a short execution bound; the lease is released
	e.svc.timeout = 20 * time.Millisecond
	e.recs.delay = time.Second
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	requireBatchErr(t, err, jsonrpc.ConcurrencyLimitCode, "busy", "deadline")
	e.recs.delay = 0
	e.svc.timeout = BatchMaxExecution
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	require.Nil(t, err, "the lease was released on every exit path")
	require.Zero(t, e.catalog.Stats().ActivePins)
}

func TestBatch_DatabaseErrorIsNotAbsence(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(1, 1000)
	e.finalize([]byte{1})
	e.recs.err = errors.New("mongo: connection reset secret-host")
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	require.NotNil(t, err)
	require.Equal(t, jsonrpc.DatabaseErrorCode, err.Code)
	rpc := err.RPCError()
	require.NotContains(t, fmt.Sprint(rpc.Message, rpc.Data), "secret-host")
	require.Nil(t, rpc.Data)
}

// One pinned view per batch: later publications do not leak into a running batch.
func TestBatch_OnePinnedViewUnderConcurrentWrites(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(10, 1000)
	root := e.finalize([]byte{1})

	started := make(chan struct{})
	var once sync.Once
	e.recs.delay = 0
	slow := &hookRecords{inner: e.recs, onCall: func() {
		once.Do(func() { close(started) })
		time.Sleep(5 * time.Millisecond)
	}}
	e.svc.records = slow

	type result struct {
		resp *api.GetInclusionProofsResponseV1
		err  *BatchError
	}
	done := make(chan result, 1)
	go func() {
		r, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
		done <- result{r, err}
	}()
	<-started
	for i := 0; i < 3; i++ { // newer finalized views while the batch runs
		e.addRecords(2, 2000+uint64(i))
		e.finalize([]byte{byte(i)})
	}
	res := <-done
	require.Nil(t, res.err)
	for i, p := range res.resp.Proofs {
		var uc types.UnicityCertificate
		require.NoError(t, types.Cbor.Unmarshal(p.InclusionProof.UnicityCertificate, &uc))
		require.Equal(t, root, []byte(uc.InputRecord.Hash), "element %d", i)
		require.EqualValues(t, 1, p.BlockNumber)
	}
}

type hookRecords struct {
	inner  BatchRecordSource
	onCall func()
}

func (h *hookRecords) GetStoredSizesByStateIDs(ctx context.Context, ids []api.StateID) (map[string]int64, error) {
	return h.inner.GetStoredSizesByStateIDs(ctx, ids)
}

func (h *hookRecords) GetFinalizedByStateIDs(ctx context.Context, ids []api.StateID) (map[string]*models.AggregatorRecord, error) {
	h.onCall()
	return h.inner.GetFinalizedByStateIDs(ctx, ids)
}

func TestBatch_RPCErrorShapes(t *testing.T) {
	limit := 256
	for name, tc := range map[string]struct {
		e    *BatchError
		want string
	}{
		"unavailable": {unavailable("view_not_retained"), `{"code":-32008,"message":"Proof unavailable","data":{"reason":"proof_unavailable","detail":"view_not_retained","retryable":true}}`},
		"invalid":     {invalidParams("duplicate_state_id"), `{"code":-32602,"message":"Invalid params","data":{"reason":"invalid_request","detail":"duplicate_state_id","retryable":false}}`},
		"limit":       {limitExceeded("state_count", limit), `{"code":-32007,"message":"Limit exceeded","data":{"reason":"limit_exceeded","detail":"state_count","retryable":false,"limit":256}}`},
		"busy":        {busy("deadline"), `{"code":-32006,"message":"Concurrency limit exceeded","data":{"reason":"busy","detail":"deadline","retryable":true}}`},
		"unsupported": {UnsupportedConfiguration(), `{"code":-32008,"message":"Proof unavailable","data":{"reason":"proof_unavailable","detail":"unsupported_configuration","retryable":false}}`},
	} {
		t.Run(name, func(t *testing.T) {
			b, err := json.Marshal(tc.e.RPCError())
			require.NoError(t, err)
			require.Equal(t, tc.want, string(b))
		})
	}
}

// Regression: a record whose stored size cannot fit the remaining response
// budget is rejected from the size projection alone; it is never loaded.
func TestBatch_OversizedStoredRecordIsNeverMaterialized(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(4, 1000)
	e.finalize([]byte("pv"))
	e.recs.sizes = map[string]int64{ids[2].String(): 64 << 20} // huge stored witness/predicate

	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	requireBatchErr(t, err, jsonrpc.LimitExceededCode, "limit_exceeded", "response_bytes")
	e.recs.mu.Lock()
	loaded := e.recs.loaded
	e.recs.mu.Unlock()
	require.Zero(t, loaded, "the oversized record is rejected from the size projection; no record is loaded at all")
}

func TestBatch_SizeProjectionErrorsAndMisses(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(2, 1000)
	e.finalize([]byte("pv"))

	e.recs.err = errors.New("mongo: secret connection text")
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	require.NotNil(t, err)
	require.Equal(t, jsonrpc.DatabaseErrorCode, err.RPCError().Code)
	require.Nil(t, err.RPCError().Data, "no database text is exposed")
	e.recs.err = nil

	e.recs.mu.Lock()
	delete(e.records, string(ids[1]))
	e.recs.mu.Unlock()
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_unknown")
}

// Regression: a finalized record whose leaf is absent from the pinned view is an
// availability outcome, not an internal error.
func TestBatch_AbsentLeafInPinnedViewIsStateNotIncluded(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(3, 1000)
	e.finalize([]byte("pv"))
	// a record finalized later than the pinned view: stored, but its leaf is not in the root
	extra := e.addRecordNoLeaf(1000)
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(append([]api.StateID{ids[0]}, extra), nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_pending")
	require.True(t, err.Retryable)
}

// addRecordNoLeaf stores a record without adding its leaf to the tree.
func (e *batchEnv) addRecordNoLeaf(refTime uint64) api.StateID {
	i := len(e.ids) + 1000
	cd := models.CertificationData{
		OwnerPredicate:  api.Predicate{Engine: 1, Code: []byte{1}, Params: bytes.Repeat([]byte{2}, 33)},
		SourceStateHash: bytes.Repeat([]byte{byte(i), byte(i >> 8), 5}, 11)[:32],
		TransactionHash: api.RequireNewImprintV2(strings.Repeat(fmt.Sprintf("%02x", byte(i+1)), 32)),
		Witness:         bytes.Repeat([]byte{4}, 65),
	}
	id, err := cd.ToAPI().CreateStateID()
	require.NoError(e.t, err)
	e.recs.mu.Lock()
	e.records[string(id)] = &models.AggregatorRecord{Version: 2, StateID: id, CertificationData: cd, ReferenceTime: refTime,
		BlockNumber: api.NewBigIntFromUint64(e.blocks)}
	e.recs.mu.Unlock()
	return id
}

// Regression: lease cancellation (pressure or lease deadline) interrupts an
// in-flight database call; the request context alone previously bounded it.
func TestBatch_LeaseCancellationInterruptsInFlightDatabaseCall(t *testing.T) {
	limits := proofview.DefaultLimits()
	limits.MaxLeaseDuration = 150 * time.Millisecond
	e := newBatchEnv(t, limits)
	ids := e.addRecords(2, 1000)
	e.finalize([]byte("pv"))
	e.recs.delay = 10 * time.Second // the database call would outlast the 5 s request timeout

	start := time.Now()
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	requireBatchErr(t, err, jsonrpc.ConcurrencyLimitCode, "busy", "deadline")
	require.Less(t, time.Since(start), 2*time.Second, "the call was cut by the lease, not by the request timeout")
}

func TestBatch_LeaseCancelledByPressureInterruptsDatabaseCall(t *testing.T) {
	limits := proofview.DefaultLimits()
	limits.MaxViews = 1
	e := newBatchEnv(t, limits)
	ids := e.addRecords(2, 1000)
	e.finalize([]byte("pv"))
	started := make(chan struct{})
	var once sync.Once
	e.svc.records = &hookRecords{inner: e.recs, onCall: func() { once.Do(func() { close(started) }) }}
	e.recs.delay = 10 * time.Second

	done := make(chan *BatchError, 1)
	go func() {
		_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
		done <- err
	}()
	<-started
	// closing the catalog cancels every lease, as pressure cancellation does
	require.NoError(t, e.catalog.Close())

	select {
	case err := <-done:
		requireBatchErr(t, err, jsonrpc.ConcurrencyLimitCode, "busy", "deadline")
	case <-time.After(3 * time.Second):
		t.Fatal("database call was not interrupted by lease cancellation")
	}
}

// The number of database round trips per batch is constant: one size projection
// and one record load, whatever the number of IDs; none is loaded when the batch
// is rejected by the preflight.
func TestBatch_ConstantDatabaseRoundTripsPerBatch(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(256, 1000)
	e.finalize([]byte("pv"))

	for _, n := range []int{1, 2, 16, 100, 256} {
		e.recs.mu.Lock()
		e.recs.sizeQ, e.recs.calls = 0, 0
		e.recs.mu.Unlock()
		resp, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids[:n], nil))
		require.Nil(t, err, n)
		require.Len(t, resp.Proofs, n)
		e.recs.mu.Lock()
		require.Equal(t, 1, e.recs.sizeQ, "%d IDs: one size query", n)
		require.Equal(t, 1, e.recs.calls, "%d IDs: one record query", n)
		e.recs.mu.Unlock()
	}

	// over-budget or missing: a single size query, no load
	e.recs.mu.Lock()
	e.recs.sizeQ, e.recs.calls = 0, 0
	e.recs.sizes = map[string]int64{ids[200].String(): 64 << 20}
	e.recs.mu.Unlock()
	_, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	requireBatchErr(t, err, jsonrpc.LimitExceededCode, "limit_exceeded", "response_bytes")
	e.recs.mu.Lock()
	require.Equal(t, 1, e.recs.sizeQ)
	require.Zero(t, e.recs.calls)
	e.recs.mu.Unlock()
}

func TestBatch_RequestOrderAndAllOrNoneWithBatchedLookups(t *testing.T) {
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(6, 1000)
	e.finalize([]byte("pv"))
	req := []api.StateID{ids[5], ids[0], ids[3], ids[1]}
	resp, err := e.svc.GetInclusionProofsV1(context.Background(), e.request(req, nil))
	require.Nil(t, err)
	for i, id := range req {
		want := e.records[string(id)]
		require.Equal(t, want.CertificationData.ToAPI().SourceStateHash, resp.Proofs[i].InclusionProof.CertificationData.SourceStateHash, "position %d", i)
	}
	// one absent ID fails the whole call; the present IDs are still examined (one record
	// load) so the error lists every offending index
	absent := e.addRecordNoLeaf(1000)
	delete(e.records, string(absent))
	e.recs.mu.Lock()
	e.recs.calls = 0
	e.recs.mu.Unlock()
	_, err = e.svc.GetInclusionProofsV1(context.Background(), e.request(append(req, absent), nil))
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_unknown")
	require.Equal(t, map[string][]int{"unknown": {4}}, err.Indexes)
	e.recs.mu.Lock()
	require.Equal(t, 1, e.recs.calls)
	e.recs.mu.Unlock()
}
