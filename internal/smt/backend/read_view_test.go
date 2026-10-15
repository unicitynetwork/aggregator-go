package backend

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func spreadInput(n uint32) LeafInput {
	key := make([]byte, api.StateTreeKeyLengthBytes)
	binary.BigEndian.PutUint32(key[:4], n*2654435761) // spread across the key space
	binary.BigEndian.PutUint32(key[len(key)-4:], n)
	value := make([]byte, 32)
	binary.BigEndian.PutUint32(value[28:], n+1)
	return LeafInput{Key: key, Value: value}
}

func spreadRange(from, to uint32) []LeafInput {
	out := make([]LeafInput, 0, to-from)
	for n := from; n < to; n++ {
		out = append(out, spreadInput(n))
	}
	return out
}

func commitBatch(t testing.TB, b *MemoryBackend, block uint64, inputs []LeafInput) []byte {
	t.Helper()
	ctx := context.Background()
	snap, err := b.CreateSnapshot(ctx)
	require.NoError(t, err)
	res, err := snap.AddLeavesClassified(ctx, inputs)
	require.NoError(t, err)
	require.NoError(t, res.ValidateAllAccepted(len(inputs)))
	require.NoError(t, snap.Commit(ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(block), RootHash: res.CandidateRoot}))
	return res.CandidateRoot
}

func newMemory() *MemoryBackend {
	return NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
}

func requireProves(t testing.TB, v ReadView, in LeafInput) {
	t.Helper()
	cert, err := v.GetInclusionCert(context.Background(), in.Key)
	require.NoError(t, err)
	require.NoError(t, cert.Verify(in.Key, in.Value, v.RootHash(), api.SHA256))
}

func TestMemoryReadViewServesFrozenRootAcrossLaterCommits(t *testing.T) {
	ctx := context.Background()
	b := newMemory()
	first := spreadRange(0, 50)
	root1 := commitBatch(t, b, 1, first)

	v1, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	require.Equal(t, root1, v1.RootHash())

	later := spreadRange(50, 400)
	root2 := commitBatch(t, b, 2, later)
	require.NotEqual(t, root1, root2)

	// The old view still proves the old leaves against the old root, and does
	// not see leaves committed afterwards.
	require.Equal(t, root1, v1.RootHash())
	for _, in := range first {
		requireProves(t, v1, in)
	}
	_, err = v1.GetInclusionCert(ctx, later[0].Key)
	require.Error(t, err)

	// A view frozen now proves everything against the new root.
	v2, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	require.Equal(t, root2, v2.RootHash())
	for _, in := range append(append([]LeafInput{}, first...), later...) {
		requireProves(t, v2, in)
	}
}

func TestMemoryReadViewIsolatedFromSnapshotForksAndAbandonedWrites(t *testing.T) {
	ctx := context.Background()
	b := newMemory()
	root1 := commitBatch(t, b, 1, spreadRange(0, 100))
	v, err := b.RetainReadView(ctx)
	require.NoError(t, err)

	// A pipelined chain: snapshot, fork, fork-of-fork, never committed.
	snap, err := b.CreateSnapshot(ctx)
	require.NoError(t, err)
	_, err = snap.AddLeavesClassified(ctx, spreadRange(100, 200))
	require.NoError(t, err)
	fork, err := snap.Fork(ctx)
	require.NoError(t, err)
	_, err = fork.AddLeavesClassified(ctx, spreadRange(200, 300))
	require.NoError(t, err)
	snap.Discard(ctx)
	fork.Discard(ctx)

	require.Equal(t, root1, v.RootHash())
	for _, in := range spreadRange(0, 100) {
		requireProves(t, v, in)
	}
	_, err = v.GetInclusionCert(ctx, spreadInput(150).Key)
	require.Error(t, err)
}

func TestMemoryReadViewConcurrentReadersWithWritersAndForks(t *testing.T) {
	ctx := context.Background()
	b := newMemory()
	initial := spreadRange(0, 200)
	commitBatch(t, b, 1, initial)
	v, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	rootAtFreeze := v.RootHash()

	stop := make(chan struct{})
	var readers sync.WaitGroup
	var reads atomic.Int64
	for r := 0; r < 8; r++ {
		readers.Add(1)
		go func(r int) {
			defer readers.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				in := initial[(i*7+r)%len(initial)]
				cert, err := v.GetInclusionCert(ctx, in.Key)
				if err != nil {
					t.Errorf("read: %v", err)
					return
				}
				if err := cert.Verify(in.Key, in.Value, rootAtFreeze, api.SHA256); err != nil {
					t.Errorf("verify: %v", err)
					return
				}
				reads.Add(1)
			}
		}(r)
	}

	// Writers: commit chained blocks, fork pipelines, and retain further views.
	next := uint32(200)
	prevView := v
	for block := uint64(2); block < 40; block++ {
		batch := spreadRange(next, next+25)
		next += 25
		snap, err := b.CreateSnapshot(ctx)
		require.NoError(t, err)
		res, err := snap.AddLeavesClassified(ctx, batch)
		require.NoError(t, err)
		child, err := snap.Fork(ctx)
		require.NoError(t, err)
		require.NoError(t, snap.Commit(ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(block), RootHash: res.CandidateRoot}))
		_, err = child.AddLeavesClassified(ctx, spreadRange(next+10000, next+10010))
		require.NoError(t, err)
		child.Discard(ctx)
		nv, err := b.RetainReadView(ctx)
		require.NoError(t, err)
		if block%5 == 0 {
			require.NoError(t, prevView.Close()) // retire some views mid-flight (not v)
		}
		prevView = nv
	}
	close(stop)
	readers.Wait()
	require.Positive(t, reads.Load())
	require.Equal(t, rootAtFreeze, v.RootHash())
}

func TestMemoryReadViewRetainedBytesChargeReplacedNodesToPredecessor(t *testing.T) {
	ctx := context.Background()
	b := newMemory()
	commitBatch(t, b, 1, spreadRange(0, 1000))
	v1, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	fresh := v1.RetainedBytes()
	require.NotZero(t, fresh)

	// No change: retaining the same state again charges nothing more.
	same, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	require.Equal(t, fresh, v1.RetainedBytes())
	require.Equal(t, v1.RootHash(), same.RootHash())

	// A small block replaces only a few paths: the predecessor is charged a
	// small, nonzero amount, far below the cost of the whole tree.
	commitBatch(t, b, 2, spreadRange(1000, 1010))
	v2, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	small := same.RetainedBytes() - fresh
	require.NotZero(t, small)

	// A much larger block costs proportionally more.
	commitBatch(t, b, 3, spreadRange(1010, 1510))
	_, err = b.RetainReadView(ctx)
	require.NoError(t, err)
	large := v2.RetainedBytes() - fresh
	require.Greater(t, large, small*5)

	// Closing a view drops its accounting but keeps successor charging sound.
	require.NoError(t, v1.Close())
	require.Zero(t, v1.RetainedBytes())
}

func TestMemoryReadViewClosePreventsFurtherUse(t *testing.T) {
	ctx := context.Background()
	b := newMemory()
	in := spreadRange(0, 5)
	commitBatch(t, b, 1, in)
	v, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	requireProves(t, v, in[0])
	require.NoError(t, v.Close())
	require.NoError(t, v.Close()) // idempotent
	_, err = v.GetInclusionCert(ctx, in[0].Key)
	require.ErrorIs(t, err, ErrReadViewClosed)

	// A later view still charges its predecessor without touching freed state.
	commitBatch(t, b, 2, spreadRange(5, 10))
	_, err = b.RetainReadView(ctx)
	require.NoError(t, err)
}

func TestMemoryReadViewHonorsCancelledContext(t *testing.T) {
	b := newMemory()
	commitBatch(t, b, 1, spreadRange(0, 3))
	v, err := b.RetainReadView(context.Background())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = v.GetInclusionCert(ctx, spreadInput(0).Key)
	require.ErrorIs(t, err, context.Canceled)
	_, err = b.RetainReadView(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestMemoryReadViewOfEmptyTree(t *testing.T) {
	b := newMemory()
	v, err := b.RetainReadView(context.Background())
	require.NoError(t, err)
	require.Equal(t, emptyRoot(t), v.RootHash())
	_, err = v.GetInclusionCert(context.Background(), spreadInput(1).Key)
	require.Error(t, err)
}

// Regression: closing a duplicate handle must not strand the charge for nodes
// an older surviving view still holds.
func TestMemoryReadViewChargeSurvivesDuplicateHandleClose(t *testing.T) {
	ctx := context.Background()
	b := newMemory()
	commitBatch(t, b, 1, spreadRange(0, 1000))
	a, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	dup, err := b.RetainReadView(ctx) // same state, as a retried publication
	require.NoError(t, err)
	require.NoError(t, dup.Close())

	commitBatch(t, b, 2, spreadRange(1000, 2000))
	nb, err := b.RetainReadView(ctx)
	require.NoError(t, err)

	require.Greater(t, a.RetainedBytes(), uint64(50_000), "A keeps ~1000 replaced paths alive")
	require.EqualValues(t, 256, nb.RetainedBytes(), "newest view holds nothing beyond live state")
}

// Regression: with an older view still pinned, closing an intermediate view
// moves its share of the charge onto the survivor instead of dropping it.
func TestMemoryReadViewChargeMovesToSurvivorWhenIntermediateCloses(t *testing.T) {
	ctx := context.Background()
	b := newMemory()
	commitBatch(t, b, 1, spreadRange(0, 1000))
	a, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	commitBatch(t, b, 2, spreadRange(1000, 1500))
	m, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	commitBatch(t, b, 3, spreadRange(1500, 2000))
	head, err := b.RetainReadView(ctx)
	require.NoError(t, err)

	aBefore := a.RetainedBytes()
	mBefore := m.RetainedBytes()
	require.Greater(t, mBefore, uint64(256))
	require.NoError(t, m.Close())
	require.Zero(t, m.RetainedBytes())
	require.Equal(t, aBefore+mBefore-256, a.RetainedBytes(), "A absorbs M's share")
	require.EqualValues(t, 256, head.RetainedBytes())

	// With A closed too, nothing older than head remains to charge.
	require.NoError(t, a.Close())
	commitBatch(t, b, 4, spreadRange(2000, 2100))
	_, err = b.RetainReadView(ctx)
	require.NoError(t, err)
	require.Greater(t, head.RetainedBytes(), uint64(256))
}
