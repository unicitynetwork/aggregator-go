package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// batchView is a retained view with a batch path (like the RocksDB view), counting calls.
type batchView struct {
	readerView
	singles, batches atomic.Int64
	batchKeys        atomic.Int64
	batchErr         error
}

func (b *batchView) GetInclusionCert(ctx context.Context, key []byte) (*api.InclusionCert, error) {
	b.singles.Add(1)
	return b.readerView.GetInclusionCert(ctx, key)
}

func (b *batchView) GetInclusionCerts(ctx context.Context, keys [][]byte) ([]*api.InclusionCert, error) {
	b.batches.Add(1)
	b.batchKeys.Add(int64(len(keys)))
	if b.batchErr != nil {
		return nil, b.batchErr
	}
	out := make([]*api.InclusionCert, len(keys))
	for i, k := range keys {
		c, err := b.readerView.GetInclusionCert(ctx, k)
		if err != nil {
			if errors.Is(err, smt.ErrLeafNotFound) {
				continue // absent: nil entry, like the disk view
			}
			return nil, err
		}
		out[i] = c
	}
	return out, nil
}

var _ backend.BatchReadView = (*batchView)(nil)

// publishWith publishes the current tree with the given reader.
func (e *batchEnv) publishWith(mk func(readerView) backend.ReadView) []byte {
	e.blocks++
	rv := e.tree.FreezeReadView(0)
	root := rv.RootHashRaw()
	uc := types.UnicityCertificate{Version: 1, InputRecord: &types.InputRecord{Version: 1, RoundNumber: e.blocks, Hash: root, SummaryValue: []byte{}}}
	ucBytes, err := types.Cbor.Marshal(&uc)
	require.NoError(e.t, err)
	require.NoError(e.t, e.catalog.Publish(context.Background(), proofview.PublishInput{
		Block: proofview.BlockIdentity{Number: api.NewBigIntFromUint64(e.blocks), ConfigLineage: []byte("cfg")},
		Root:  root, Reader: mk(readerView{rv}), Cert: proofview.Certification{UC: ucBytes, PartitionView: []byte("pv")},
	}))
	return root
}

func TestBatch_UsesOneBatchBuildOnThePinnedViewAndMatchesPerKeyProofs(t *testing.T) {
	var bv *batchView
	e1 := newBatchEnv(t, proofview.DefaultLimits())
	ids := e1.addRecords(12, 1000)
	e1.publishWith(func(r readerView) backend.ReadView { bv = &batchView{readerView: r}; return bv })

	// the same state served from a view without a batch path (per-key builds)
	e2 := newBatchEnv(t, proofview.DefaultLimits())
	e2.addRecords(12, 1000)
	e2.publishWith(func(r readerView) backend.ReadView { return r })

	req := []api.StateID{ids[5], ids[0], ids[11], ids[3]}
	got, berr := e1.svc.GetInclusionProofsV1(context.Background(), e1.request(req, nil))
	require.Nil(t, berr)
	want, berr := e2.svc.GetInclusionProofsV1(context.Background(), e2.request(req, nil))
	require.Nil(t, berr)

	require.EqualValues(t, 1, bv.batches.Load(), "one union build for the whole batch")
	require.EqualValues(t, len(req), bv.batchKeys.Load())
	require.Zero(t, bv.singles.Load(), "no per-key materialization")
	a, _ := json.Marshal(got)
	b, _ := json.Marshal(want)
	require.JSONEq(t, string(b), string(a), "identical response")
}

func TestBatch_SingleStateKeepsPerKeyPath(t *testing.T) {
	var bv *batchView
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(3, 1000)
	e.publishWith(func(r readerView) backend.ReadView { bv = &batchView{readerView: r}; return bv })
	_, berr := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids[:1], nil))
	require.Nil(t, berr)
	require.Zero(t, bv.batches.Load())
	require.EqualValues(t, 1, bv.singles.Load())
}

// An absent leaf is a per-key outcome of the one union build: state_not_included at that
// state, in request order, with zero per-key materializations (no fallback path a client
// could force).
func TestBatch_AbsentLeafIsPerKeyOutcomeWithoutFallback(t *testing.T) {
	var bv *batchView
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(4, 1000)
	e.publishWith(func(r readerView) backend.ReadView { bv = &batchView{readerView: r}; return bv })
	extra := e.addRecordNoLeaf(1000)

	_, berr := e.svc.GetInclusionProofsV1(context.Background(), e.request(append(append([]api.StateID{}, ids[:3]...), extra), nil))
	requireBatchErr(t, berr, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_not_included")
	require.EqualValues(t, 1, bv.batches.Load())
	require.Zero(t, bv.singles.Load(), "no per-key fallback materialization")
}

// The older-root scenario: many IDs included in R_old followed by one finalized after
// R_old. The worst case stays one union build, not one build per key.
func TestBatch_OlderRootWithIDFinalizedAfterIt(t *testing.T) {
	var oldView, newView *batchView
	e := newBatchEnv(t, proofview.DefaultLimits())
	oldIDs := e.addRecords(40, 1000)
	oldRoot := e.publishWith(func(r readerView) backend.ReadView { oldView = &batchView{readerView: r}; return oldView })
	later := e.addRecords(1, 1000) // finalized after oldRoot: record in block 2, leaf only in the newer root
	e.records[string(later[0])].BlockNumber = api.NewBigIntFromUint64(2)
	e.publishWith(func(r readerView) backend.ReadView { newView = &batchView{readerView: r}; return newView })

	req := append(append([]api.StateID{}, oldIDs...), later[0])
	_, berr := e.svc.GetInclusionProofsV1(context.Background(), e.request(req, oldRoot))
	requireBatchErr(t, berr, jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_not_included")
	require.EqualValues(t, 1, oldView.batches.Load())
	require.Zero(t, oldView.singles.Load(), "one union build, no per-key builds")
	require.Zero(t, newView.batches.Load()+newView.singles.Load())

	// the same batch against the newer root succeeds, still one batch build
	root2 := e.catalogLatestRoot()
	resp, berr := e.svc.GetInclusionProofsV1(context.Background(), e.request(req, root2))
	require.Nil(t, berr)
	require.Len(t, resp.Proofs, len(req))
	require.EqualValues(t, 1, newView.batches.Load())
	require.Zero(t, newView.singles.Load())
}

func (e *batchEnv) catalogLatestRoot() []byte {
	l, err := e.catalog.AcquireLatest(context.Background())
	require.NoError(e.t, err)
	defer l.Release()
	return append([]byte(nil), l.Root()...)
}

func TestBatch_BatchBuildFailureIsInternalAndLeaksNothing(t *testing.T) {
	var bv *batchView
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(4, 1000)
	e.publishWith(func(r readerView) backend.ReadView { bv = &batchView{readerView: r}; return bv })
	bv.batchErr = errors.New("rocksdb: corruption at /var/lib/secret")

	_, berr := e.svc.GetInclusionProofsV1(context.Background(), e.request(ids, nil))
	require.NotNil(t, berr)
	require.Equal(t, jsonrpc.InternalErrorCode, berr.Code)
	rpc := berr.RPCError()
	data, _ := json.Marshal(rpc)
	require.NotContains(t, string(data), "secret")
}
