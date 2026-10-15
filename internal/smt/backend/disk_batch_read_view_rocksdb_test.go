//go:build rocksdb

package backend

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestDiskReadViewBatchCertsMatchSingleAndNeverReadLiveState(t *testing.T) {
	f := newRVFixture(t)
	root1 := f.commit(0, 150)
	first := append([]LeafInput(nil), f.leaves...)
	v1, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	defer v1.Close()
	bv, ok := v1.(BatchReadView)
	require.True(t, ok, "the disk view has a batch path")

	// later commits and compaction change the live store; the batch reads the pinned snapshot
	f.commit(150, 400)
	require.NoError(t, f.store.CompactNodes())

	keys := make([][]byte, len(first))
	for i, l := range first {
		keys[i] = l.Key
	}
	leavesBefore := testutil.ToFloat64(metrics.SMTDiskBatchProofLeaves)
	readsBefore := testutil.ToFloat64(metrics.SMTDiskBatchNodeReads)
	uniqueBefore := testutil.ToFloat64(metrics.SMTDiskBatchUniqueNodes)

	certs, err := bv.GetInclusionCerts(f.ctx, keys)
	require.NoError(t, err)
	require.Len(t, certs, len(keys))
	var singleNodes int
	for i, l := range first {
		require.NoError(t, certs[i].Verify(l.Key, l.Value, root1, api.InclusionProofV2HashAlgorithm))
		single, err := v1.GetInclusionCert(f.ctx, l.Key)
		require.NoError(t, err)
		require.Equal(t, single.Bitmap, certs[i].Bitmap)
		require.Equal(t, single.Siblings, certs[i].Siblings)
		singleNodes += len(single.Siblings)
	}

	require.Equal(t, float64(len(keys)), testutil.ToFloat64(metrics.SMTDiskBatchProofLeaves)-leavesBefore)
	reads := testutil.ToFloat64(metrics.SMTDiskBatchNodeReads) - readsBefore
	unique := testutil.ToFloat64(metrics.SMTDiskBatchUniqueNodes) - uniqueBefore
	require.Equal(t, reads, unique, "every node of the union is read once")
	require.Less(t, reads, float64(singleNodes), "far fewer node reads than L independent proofs")
	t.Logf("L=%d node reads=%.0f unique=%.0f Σ(single path nodes)=%d", len(keys), reads, unique, singleNodes)
}

func TestDiskReadViewBatchCertsErrors(t *testing.T) {
	f := newRVFixture(t)
	f.commit(0, 30)
	v, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	bv := v.(BatchReadView)

	t.Run("a key not in the retained state", func(t *testing.T) {
		f.commit(30, 40)
		_, err := bv.GetInclusionCerts(f.ctx, [][]byte{f.leaves[0].Key, f.leaves[35].Key})
		require.ErrorIs(t, err, ErrPublishedProofLeafNotFound)
	})
	t.Run("empty key list", func(t *testing.T) {
		certs, err := bv.GetInclusionCerts(f.ctx, nil)
		require.NoError(t, err)
		require.Empty(t, certs)
	})
	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := canceledContext()
		defer cancel()
		_, err := bv.GetInclusionCerts(ctx, [][]byte{f.leaves[0].Key})
		require.Error(t, err)
	})
	t.Run("closed view", func(t *testing.T) {
		require.NoError(t, v.Close())
		_, err := bv.GetInclusionCerts(f.ctx, [][]byte{f.leaves[0].Key})
		require.ErrorIs(t, err, ErrReadViewClosed)
	})
	t.Run("empty root", func(t *testing.T) {
		g := newRVFixture(t)
		ev, err := g.backend.RetainReadView(g.ctx)
		require.NoError(t, err)
		defer ev.Close()
		_, err = ev.(BatchReadView).GetInclusionCerts(g.ctx, [][]byte{rvLeaf(1).Key})
		require.ErrorIs(t, err, ErrPublishedProofLeafNotFound)
	})
}

func canceledContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx, cancel
}
