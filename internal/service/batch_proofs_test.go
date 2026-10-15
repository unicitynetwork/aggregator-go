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
	mu      sync.Mutex
	records map[string]*models.AggregatorRecord
	err     error
	delay   time.Duration
	calls   int
}

func (f *fakeRecords) GetByStateID(ctx context.Context, id api.StateID) (*models.AggregatorRecord, error) {
	f.mu.Lock()
	f.calls++
	r, err, d := f.records[string(id)], f.err, f.delay
	f.mu.Unlock()
	if d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return r, err
}

// readerView adapts a frozen SMT view to backend.ReadView.
type readerView struct{ v *smt.ReadView }

func (r readerView) RootHash() []byte      { return r.v.RootHashRaw() }
func (r readerView) RetainedBytes() uint64 { return r.v.RetainedBytes() }
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
	rv := e.tree.FreezeReadView(e.prev)
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
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_not_included")

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
	requireBatchErr(t, err, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_not_included")
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

func (h *hookRecords) GetByStateID(ctx context.Context, id api.StateID) (*models.AggregatorRecord, error) {
	h.onCall()
	return h.inner.GetByStateID(ctx, id)
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
