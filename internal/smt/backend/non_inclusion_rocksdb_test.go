//go:build rocksdb

package backend

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/nifixture"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func newNIDiskBackend(t *testing.T) Backend {
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	b, err := NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// The RocksDB retained views produce exactly the oracle's certificates on random trees across commits,
// the same bytes as the memory backend.
func TestDiskBackendNonInclusionDifferential(t *testing.T) {
	RunNonInclusionDifferential(t, newNIDiskBackend, 25)
}

// Identical content in both backends yields identical certificates and roots.
func TestMemoryAndDiskNonInclusionAgree(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(99))
	for iter := 0; iter < 10; iter++ {
		leaves := niLeaves(rng, 10+rng.Intn(60))
		mem, disk := newNIMemoryBackend(), newNIDiskBackend(t)
		rm, rd := niCommit(t, mem, 1, leaves), niCommit(t, disk, 1, leaves)
		require.Equal(t, rm, rd)
		mv, err := mem.(ReadViewRetainer).RetainReadView(ctx)
		require.NoError(t, err)
		dv, err := disk.(ReadViewRetainer).RetainReadView(ctx)
		require.NoError(t, err)
		for k := 0; k < 30; k++ {
			q := niQuery(rng, leaves, k)
			mc, merr := NonInclusionCert(ctx, mv, q[:])
			dc, derr := NonInclusionCert(ctx, dv, q[:])
			require.Equal(t, errors.Is(merr, api.ErrStateIncluded), errors.Is(derr, api.ErrStateIncluded))
			if merr == nil {
				require.NoError(t, derr)
				mb, _ := mc.MarshalBinary()
				db, _ := dc.MarshalBinary()
				require.Equal(t, mb, db)
			}
		}
		_ = mv.Close()
		_ = dv.Close()
	}
}

// The view reads only its pinned snapshot: the backend's live store is swapped out under the view and
// the view's certificate does not change; a node the snapshot cannot read fails the call (never an absence).
func TestDiskNonInclusionReadsOnlyThePinnedSnapshotAndFailsClosed(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(3))
	b := newNIDiskBackend(t)
	leaves := niLeaves(rng, 40)
	root := niCommit(t, b, 1, leaves[:20])
	v, err := b.(ReadViewRetainer).RetainReadView(ctx)
	require.NoError(t, err)
	defer v.Close()
	oracle, _ := nifixture.Build(leaves[:20])
	q := leaves[25].Key // inserted later
	before, err := NonInclusionCert(ctx, v, q[:])
	require.NoError(t, err)
	bb, _ := before.MarshalBinary()
	require.Equal(t, oracle.Certificate(q), bb)

	niCommit(t, b, 2, leaves[20:])
	after, err := NonInclusionCert(ctx, v, q[:])
	require.NoError(t, err)
	ab, _ := after.MarshalBinary()
	require.Equal(t, bb, ab, "later commits do not change the retained view")
	live, err := b.(ReadViewRetainer).RetainReadView(ctx)
	require.NoError(t, err)
	defer live.Close()
	_, err = NonInclusionCert(ctx, live, q[:])
	require.ErrorIs(t, err, api.ErrStateIncluded)

	// fail closed: a reader which cannot find nodes produces an error, not a certificate
	var reads atomic.Int64
	dv := v.(*diskReadView)
	_, err = persist.BuildNonInclusionCert(disk.Hash(root), &missingNodes{reads: &reads}, q[:])
	require.Error(t, err)
	require.Positive(t, reads.Load())
	_ = dv
	require.False(t, errors.Is(err, api.ErrStateIncluded))
	// corrupted bytes under the right key do not authenticate: the hash check rejects them
	_, err = persist.BuildNonInclusionCert(disk.Hash(root), &garbageNodes{}, q[:])
	require.Error(t, err)
}

type missingNodes struct{ reads *atomic.Int64 }

func (m *missingNodes) GetNode(disk.NodeKey) ([]byte, bool, error) {
	m.reads.Add(1)
	return nil, false, nil
}
func (m *missingNodes) GetNodes(keys []disk.NodeKey, _ bool) ([]storage.NodeReadResult, error) {
	m.reads.Add(1)
	return make([]storage.NodeReadResult, len(keys)), nil
}

type garbageNodes struct{}

func (garbageNodes) GetNode(disk.NodeKey) ([]byte, bool, error) { return []byte{1, 2, 3, 4}, true, nil }
func (garbageNodes) GetNodes(keys []disk.NodeKey, _ bool) ([]storage.NodeReadResult, error) {
	out := make([]storage.NodeReadResult, len(keys))
	for i := range out {
		out[i] = storage.NodeReadResult{Found: true, Value: []byte{1, 2, 3, 4}}
	}
	return out, nil
}

type niCountingStore struct {
	storage.ReadStore
	reads atomic.Int64
}

func (c *niCountingStore) GetNode(k disk.NodeKey) ([]byte, bool, error) {
	c.reads.Add(1)
	return c.ReadStore.GetNode(k)
}
func (c *niCountingStore) GetNodes(ks []disk.NodeKey, p bool) ([]storage.NodeReadResult, error) {
	c.reads.Add(int64(len(ks)))
	return c.ReadStore.GetNodes(ks, p)
}

// Cost: a certificate reads one node per level of its path (root, then one per junction), whatever the
// size of the tree; the work is the path height, not N.
func TestDiskNonInclusionNodeReadsAreThePathHeight(t *testing.T) {
	rng := rand.New(rand.NewSource(8))
	var perQuery []float64
	for _, n := range []int{64, 512, 4096} {
		store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
		require.NoError(t, err)
		b, err := NewDiskBackend(store, persist.DefaultOptions())
		require.NoError(t, err)
		leaves := niLeaves(rng, n)
		niCommit(t, b, 1, leaves)
		reader, closeSnap, err := store.NewReadSnapshot()
		require.NoError(t, err)
		root, err := b.RootHashRaw(context.Background())
		require.NoError(t, err)
		cs := &niCountingStore{ReadStore: reader}
		var reads, junctions float64
		for k := 0; k < 100; k++ {
			var q [32]byte
			rng.Read(q[:])
			before := cs.reads.Load()
			cert, err := persist.BuildNonInclusionCert(disk.Hash(root), cs, q[:])
			require.NoError(t, err)
			reads += float64(cs.reads.Load() - before)
			junctions += float64(cert.Junctions())
			require.LessOrEqual(t, cs.reads.Load()-before, int64(cert.Junctions())+1, "one node per level")
		}
		perQuery = append(perQuery, reads/100)
		t.Logf("N=%d: %.1f node reads, %.1f junctions per query", n, reads/100, junctions/100)
		closeSnap()
		require.NoError(t, b.Close())
	}
	// 64x more leaves add about log2(64)=6 levels, not a multiple
	require.Less(t, perQuery[2], perQuery[0]+10)
}
