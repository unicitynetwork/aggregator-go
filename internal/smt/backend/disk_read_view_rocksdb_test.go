//go:build rocksdb

package backend

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type rvFixture struct {
	t       *testing.T
	ctx     context.Context
	store   *rocksstore.Store
	backend *DiskBackend
	block   uint64
	leaves  []LeafInput // all committed so far
}

func newRVFixture(t *testing.T) *rvFixture {
	t.Helper()
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	b, err := NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	f := &rvFixture{t: t, ctx: context.Background(), store: store, backend: b}
	t.Cleanup(func() { _ = b.Close() })
	return f
}

// rvLeaf spreads keys over the key space so commits rewrite shared branch nodes.
func rvLeaf(i int) LeafInput {
	key := make([]byte, api.StateTreeKeyLengthBytes)
	key[0] = byte(i * 37)
	key[1] = byte(i)
	key[31] = byte(i >> 8)
	value := make([]byte, 32)
	value[0], value[31] = byte(i), byte(i>>8)
	return LeafInput{Key: key, Value: value}
}

func (f *rvFixture) commit(from, to int) []byte {
	f.t.Helper()
	var inputs []LeafInput
	for i := from; i < to; i++ {
		inputs = append(inputs, rvLeaf(i))
	}
	snap, err := f.backend.CreateSnapshot(f.ctx)
	require.NoError(f.t, err)
	res, err := snap.AddLeavesClassified(f.ctx, inputs)
	require.NoError(f.t, err)
	require.Empty(f.t, res.Rejected)
	f.block++
	require.NoError(f.t, snap.Commit(f.ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(f.block), RootHash: res.CandidateRoot}))
	f.leaves = append(f.leaves, inputs...)
	return res.CandidateRoot
}

func (f *rvFixture) verify(v ReadView, root []byte, leaves []LeafInput) {
	f.t.Helper()
	require.Equal(f.t, root, v.RootHash())
	for _, l := range leaves {
		cert, err := v.GetInclusionCert(f.ctx, l.Key)
		require.NoError(f.t, err)
		require.NoError(f.t, cert.Verify(l.Key, l.Value, root, api.InclusionProofV2HashAlgorithm))
	}
}

func TestDiskReadViewServesRetainedStateAfterLaterCommitsAndCompaction(t *testing.T) {
	f := newRVFixture(t)
	root1 := f.commit(0, 20)
	first := append([]LeafInput(nil), f.leaves...)
	v1, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), f.store.NumSnapshots())

	// later commits rewrite and tombstone shared nodes; compaction would drop old versions
	root2 := f.commit(20, 60)
	root3 := f.commit(60, 200)
	require.NotEqual(t, root1, root3)
	require.NoError(t, f.store.CompactNodes())

	f.verify(v1, root1, first)
	// leaves committed later are not in the retained state
	for _, l := range f.leaves[len(first):] {
		_, err := v1.GetInclusionCert(f.ctx, l.Key)
		require.ErrorIs(t, err, ErrPublishedProofLeafNotFound)
	}

	// a view of the newest state sees everything, the older view is unaffected
	v3, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	f.verify(v3, root3, f.leaves)
	f.verify(v1, root1, first)
	_ = root2

	require.Equal(t, uint64(2), f.store.NumSnapshots())
	require.NoError(t, v1.Close())
	require.Equal(t, uint64(1), f.store.NumSnapshots())
	require.NoError(t, v3.Close())
	require.Zero(t, f.store.NumSnapshots())
}

// the live store must never be consulted by a view, only its own snapshot
type liveReadCountingStore struct {
	storage.Store
	snapshotter storage.ReadSnapshotter
	volume      storage.WriteVolumeReporter
	liveReads   atomic.Int64
}

func (s *liveReadCountingStore) GetNode(k disk.NodeKey) ([]byte, bool, error) {
	s.liveReads.Add(1)
	return s.Store.GetNode(k)
}

func (s *liveReadCountingStore) GetNodes(k []disk.NodeKey, sorted bool) ([]storage.NodeReadResult, error) {
	s.liveReads.Add(1)
	return s.Store.GetNodes(k, sorted)
}

func (s *liveReadCountingStore) NewReadSnapshot() (storage.ReadStore, func(), error) {
	return s.snapshotter.NewReadSnapshot()
}

func (s *liveReadCountingStore) WrittenBytes() uint64 { return s.volume.WrittenBytes() }

func TestDiskReadViewNeverReadsLiveStore(t *testing.T) {
	ctx := context.Background()
	rs, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	store := &liveReadCountingStore{Store: rs, snapshotter: rs, volume: rs}
	b, err := NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	defer b.Close()
	f := &rvFixture{t: t, ctx: ctx, store: rs, backend: b}
	root := f.commit(0, 30)
	leaves := append([]LeafInput(nil), f.leaves...)
	v, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	defer v.Close()
	f.commit(30, 40)

	before := store.liveReads.Load()
	f.verify(v, root, leaves)
	require.Equal(t, before, store.liveReads.Load(), "view must read through its snapshot only")
}

func TestDiskReadViewCloseSemantics(t *testing.T) {
	f := newRVFixture(t)
	root := f.commit(0, 5)
	v, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	require.Equal(t, root, v.RootHash())
	require.NoError(t, v.Close())
	require.NoError(t, v.Close(), "idempotent")
	require.Zero(t, f.store.NumSnapshots(), "no leaked or double released snapshot")
	_, err = v.GetInclusionCert(f.ctx, rvLeaf(0).Key)
	require.ErrorIs(t, err, ErrReadViewClosed)
	require.Zero(t, v.RetainedBytes())

	// canceled context
	v2, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	defer v2.Close()
	cctx, cancel := context.WithCancel(f.ctx)
	cancel()
	_, err = v2.GetInclusionCert(cctx, rvLeaf(0).Key)
	require.ErrorIs(t, err, context.Canceled)
	_, err = f.backend.RetainReadView(cctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDiskReadViewBackendCloseReleasesViews(t *testing.T) {
	f := newRVFixture(t)
	f.commit(0, 5)
	v, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	v2, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), f.store.NumSnapshots())

	done := make(chan error)
	go func() { done <- f.backend.Close() }()
	require.NoError(t, <-done, "close must not wait for views forever")
	_, err = v.GetInclusionCert(f.ctx, rvLeaf(0).Key)
	require.ErrorIs(t, err, ErrReadViewClosed)
	require.NoError(t, v2.Close())
	_, err = f.backend.RetainReadView(f.ctx)
	require.Error(t, err)
}

func TestDiskReadViewEmptyRoot(t *testing.T) {
	f := newRVFixture(t)
	v, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	require.Zero(t, f.store.NumSnapshots(), "empty root needs no snapshot")
	_, err = v.GetInclusionCert(f.ctx, rvLeaf(0).Key)
	require.ErrorIs(t, err, ErrPublishedProofLeafNotFound)
	f.commit(0, 10)
	require.Zero(t, v.RetainedBytes(), "nothing is pinned by an empty view")
	require.NoError(t, v.Close())
}

func TestDiskReadViewRetainedBytesLineage(t *testing.T) {
	f := newRVFixture(t)
	f.commit(0, 50)
	v1, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	require.Zero(t, v1.RetainedBytes(), "nothing written since capture")

	f.commit(50, 100)
	b1 := v1.RetainedBytes()
	require.Positive(t, b1, "versions written after capture are charged")
	f.commit(100, 150)
	b2 := v1.RetainedBytes()
	require.Greater(t, b2, b1, "charge grows while it is the newest view")

	v2, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	// the successor fixes the predecessor's charge: shared history is counted once
	require.Equal(t, v1.RetainedBytes(), v1.RetainedBytes())
	fixed := v1.RetainedBytes()
	require.GreaterOrEqual(t, fixed, b2)
	f.commit(150, 200)
	require.Equal(t, fixed, v1.RetainedBytes())
	require.Positive(t, v2.RetainedBytes())

	// total over the retained views covers everything written since the oldest
	require.Equal(t, fixed+v2.RetainedBytes(), f.store.WrittenBytes()-startOf(t, f, v1))

	require.NoError(t, v1.Close())
	require.Zero(t, v1.RetainedBytes())
	require.Positive(t, v2.RetainedBytes())
	require.NoError(t, v2.Close())
}

// startOf returns the write volume at the capture of v (white box).
func startOf(t *testing.T, f *rvFixture, v ReadView) uint64 {
	return v.(*diskReadView).lineageStart
}

func TestDiskReadViewUnsupportedStores(t *testing.T) {
	ctx := context.Background()
	rs, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)

	// snapshots but no write volume accounting: the retention budget could not be honored
	noVolume := &countingSnapshotStore{Store: rs, snapshotter: rs}
	b, err := NewDiskBackend(noVolume, persist.DefaultOptions())
	require.NoError(t, err)
	defer b.Close()
	_, err = b.RetainReadView(ctx)
	require.ErrorIs(t, err, ErrReadViewUnsupported)

	// no snapshots at all
	rs2, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	plain := struct{ storage.Store }{rs2}
	b2, err := NewDiskBackend(plain, persist.DefaultOptions())
	require.NoError(t, err)
	defer b2.Close()
	_, err = b2.RetainReadView(ctx)
	require.ErrorIs(t, err, ErrReadViewUnsupported)
}

// A view retained while commits run always matches its own root: root and
// snapshot are captured atomically with respect to commits.
func TestDiskReadViewConsistentUnderConcurrentCommits(t *testing.T) {
	f := newRVFixture(t)
	f.commit(0, 5)

	type captured struct {
		view ReadView
		n    int // leaves committed in this root
	}
	var mu sync.Mutex
	rootToCount := map[string]int{string(f.mustRoot()): 5}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var views []captured
	var nCaptured atomic.Int64 // published by the reader after each capture
	const maxViews = 60
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if len(views) >= maxViews {
				time.Sleep(time.Millisecond)
				continue
			}
			v, err := f.backend.RetainReadView(f.ctx)
			if err != nil {
				return
			}
			time.Sleep(time.Millisecond)
			mu.Lock()
			n, ok := rootToCount[string(v.RootHash())]
			mu.Unlock()
			views = append(views, captured{v, n})
			nCaptured.Store(int64(len(views)))
			_ = ok
		}
	}()
	// The reader goroutine may be scheduled only after the commits are done. Require a capture
	// before the first commit and one more after every commit (while the reader is under its cap),
	// so views taken while the root moves are always exercised and the test cannot pass vacuously.
	waitCaptures := func(atLeast int64) {
		require.Eventually(t, func() bool { return nCaptured.Load() >= atLeast }, 30*time.Second, time.Millisecond,
			"reader did not capture a view")
	}
	waitCaptures(1)
	for i := 5; i < 65; i += 2 {
		snap, err := f.backend.CreateSnapshot(f.ctx)
		require.NoError(t, err)
		in := []LeafInput{rvLeaf(i), rvLeaf(i + 1)}
		res, err := snap.AddLeavesClassified(f.ctx, in)
		require.NoError(t, err)
		mu.Lock()
		rootToCount[string(res.CandidateRoot)] = i + 2
		mu.Unlock()
		f.block++
		require.NoError(t, snap.Commit(f.ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(f.block), RootHash: res.CandidateRoot}))
		if before := nCaptured.Load(); before < maxViews {
			waitCaptures(before + 1)
		}
	}
	close(stop)
	wg.Wait()
	require.NotEmpty(t, views)
	for _, c := range views {
		if c.n == 0 {
			// captured between AddLeaves and Commit registration of the next root: root is still a committed one
			c.n = 5
		}
		root := c.view.RootHash()
		for i := 0; i < c.n; i++ {
			l := rvLeaf(i)
			cert, err := c.view.GetInclusionCert(f.ctx, l.Key)
			require.NoError(t, err)
			require.NoError(t, cert.Verify(l.Key, l.Value, root, api.InclusionProofV2HashAlgorithm))
		}
		require.NoError(t, c.view.Close())
	}
	require.Zero(t, f.store.NumSnapshots())
}

func (f *rvFixture) mustRoot() []byte {
	r, err := f.backend.RootHashRaw(f.ctx)
	require.NoError(f.t, err)
	return r
}

func TestDiskReadViewConcurrentReadersWritersAndClose(t *testing.T) {
	f := newRVFixture(t)
	root := f.commit(0, 40)
	leaves := append([]LeafInput(nil), f.leaves...)
	v, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				l := leaves[i%len(leaves)]
				cert, err := v.GetInclusionCert(f.ctx, l.Key)
				if err != nil {
					require.ErrorIs(t, err, ErrReadViewClosed)
					return
				}
				require.NoError(t, cert.Verify(l.Key, l.Value, root, api.InclusionProofV2HashAlgorithm))
				_ = v.RetainedBytes()
			}
		}()
	}
	for i := 40; i < 120; i += 8 {
		f.commit(i, i+8)
		if i == 80 {
			require.NoError(t, f.store.CompactNodes())
		}
	}
	require.NoError(t, v.Close()) // waits for reads in flight
	wg.Wait()
	require.Zero(t, f.store.NumSnapshots())
}

func totalRetained(views ...ReadView) uint64 {
	var sum uint64
	for _, v := range views {
		sum += v.RetainedBytes()
	}
	return sum
}

// A redundant same-root reader (discarded by the catalog) must not end the
// charging of the view which survives.
func TestDiskReadViewDiscardedDuplicateKeepsPredecessorCharged(t *testing.T) {
	f := newRVFixture(t)
	f.commit(0, 20)
	a, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	defer a.Close()
	start := a.(*diskReadView).lineageStart
	b, err := f.backend.RetainReadView(f.ctx) // same root
	require.NoError(t, err)
	require.NoError(t, b.Close())

	f.commit(20, 100)
	require.Equal(t, f.store.WrittenBytes()-start, a.RetainedBytes(), "A pins everything written since it was retained")
	require.Positive(t, a.RetainedBytes())
	// a new capture after the close continues the lineage from A
	c, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	defer c.Close()
	f.commit(100, 120)
	require.Equal(t, f.store.WrittenBytes()-start, totalRetained(a, c))
}

// An intermediate view closing while an older one is pinned transfers its interval.
func TestDiskReadViewIntermediateCloseTransfersAccounting(t *testing.T) {
	f := newRVFixture(t)
	f.commit(0, 20)
	a, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	defer a.Close()
	start := a.(*diskReadView).lineageStart
	f.commit(20, 50)
	b, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	f.commit(50, 80)
	c, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	defer c.Close()
	f.commit(80, 110)

	before := totalRetained(a, b, c)
	require.Equal(t, f.store.WrittenBytes()-start, before)
	aBefore, cBefore := a.RetainedBytes(), c.RetainedBytes()

	require.NoError(t, b.Close())
	require.Zero(t, b.RetainedBytes())
	require.Equal(t, before, totalRetained(a, c), "closing B must not lower the total charge of the survivors")
	require.Greater(t, a.RetainedBytes(), aBefore, "A inherits B's interval")
	require.Equal(t, cBefore, c.RetainedBytes())

	// ongoing writes are charged to the newest survivor
	f.commit(110, 140)
	require.Equal(t, f.store.WrittenBytes()-start, totalRetained(a, c))
	require.Greater(t, c.RetainedBytes(), cBefore)
}

// When the newest view closes the newest survivor resumes charging the ongoing churn.
func TestDiskReadViewNewestCloseResumesPredecessor(t *testing.T) {
	f := newRVFixture(t)
	f.commit(0, 20)
	a, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	defer a.Close()
	start := a.(*diskReadView).lineageStart
	f.commit(20, 40)
	b, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	f.commit(40, 60)
	require.NoError(t, b.Close())
	f.commit(60, 120)
	require.Equal(t, f.store.WrittenBytes()-start, a.RetainedBytes())

	// all closed: nothing charged, a later capture starts a fresh lineage
	require.NoError(t, a.Close())
	d, err := f.backend.RetainReadView(f.ctx)
	require.NoError(t, err)
	defer d.Close()
	require.Zero(t, d.RetainedBytes())
	f.commit(120, 130)
	require.Equal(t, f.store.WrittenBytes()-d.(*diskReadView).lineageStart, d.RetainedBytes())
}

// The backend total is O(1) (no list walk) and equals the sum of the charges of the open
// views through random captures, out-of-order closes and accounting retirements.
func TestDiskReadViewTotalIsTheSumOfCharges(t *testing.T) {
	f := newRVFixture(t)
	rng := rand.New(rand.NewSource(11))
	var open []ReadView
	next := 0
	for step := 0; step < 120; step++ {
		switch op := rng.Intn(10); {
		case op < 3:
			f.commit(next, next+1+rng.Intn(6))
			next += 6
		case op < 6:
			v, err := f.backend.RetainReadView(f.ctx)
			require.NoError(t, err)
			open = append(open, v)
		case op < 8 && len(open) > 0:
			i := rng.Intn(len(open))
			require.NoError(t, open[i].Close())
			open = append(open[:i], open[i+1:]...)
		case len(open) > 0:
			i := rng.Intn(len(open))
			open[i].(AccountingRetirer).RetireAccounting()
			require.Zero(t, open[i].RetainedBytes())
			defer open[i].Close()
			open = append(open[:i], open[i+1:]...)
		}
		var sum uint64
		for _, v := range open {
			sum += v.RetainedBytes()
		}
		require.Equal(t, sum, f.backend.RetainedBytesTotal(), "step %d", step)
	}
	for _, v := range open {
		require.NoError(t, v.Close())
	}
	require.Zero(t, f.backend.RetainedBytesTotal())
}
