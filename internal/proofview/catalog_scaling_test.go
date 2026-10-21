package proofview

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ---- invariants of the maintained structures ------------------------------------------------

// checkInvariants recomputes everything the catalog maintains incrementally (totals,
// indexes, heap membership) from its lists and compares.
func checkInvariants(t testing.TB, c *Catalog) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	var live, n int
	var prev *view
	var evictable, pinned int
	for v := c.head; v != nil; v = v.next {
		n++
		require.True(t, v.live)
		require.Same(t, prev, v.prev)
		require.Same(t, v, c.byID[v.idKey])
		var attBytes uint64
		cnt := 0
		var pa *attachment
		for a := v.attHead; a != nil; a = a.next {
			require.Same(t, pa, a.prev)
			require.Same(t, a, v.attIdx.m[a.digest])
			attBytes += a.bytes
			cnt++
			pa = a
		}
		require.Same(t, pa, v.attTail)
		require.Equal(t, cnt, v.attN)
		require.Len(t, v.attIdx.m, cnt)
		require.Equal(t, attBytes, v.attBytes)
		live += int(attBytes)
		require.Equal(t, v.refs, len(v.leases))
		for l := range v.pending {
			require.Contains(t, v.leases, l, "a pending lease is a lease of the view")
		}
		wantEvictable := v.refs == 0 && v != c.tail
		require.Equal(t, wantEvictable, v.evictIdx >= 0, "evictable membership")
		require.Equal(t, v.refs > 0, v.pinIdx >= 0, "pinned membership")
		if wantEvictable {
			evictable++
		}
		if v.refs > 0 {
			pinned++
		}
		// newest live view of its root
		nv := v
		for nv.rootNewer != nil {
			nv = nv.rootNewer
		}
		require.Same(t, nv, c.byRoot[string(v.root)])
		prev = v
	}
	require.Same(t, prev, c.tail)
	require.Equal(t, n, c.nViews)
	require.Len(t, c.byID, n)
	require.Len(t, c.evictable.items, evictable)
	require.Len(t, c.pinned.items, pinned)
	require.Equal(t, uint64(live), c.liveAttBytes)
	// the digest index holds exactly the attachments of the live views
	indexed := 0
	for d, vs := range c.byDigest {
		require.NotEmpty(t, vs)
		for v := range vs {
			require.True(t, v.live, "a retired view is not indexed")
			require.Contains(t, v.attIdx.m, d)
			indexed++
		}
	}
	var held int
	for v := c.head; v != nil; v = v.next {
		held += v.attIdx.size()
	}
	require.Equal(t, held, indexed)
	// the composite index holds exactly the live views with a certification, under their latest one
	wantIn, wantRoots := 0, map[string]int{}
	for v := c.head; v != nil; v = v.next {
		if v.attTail == nil {
			require.False(t, v.compatIn)
			continue
		}
		wantIn++
		wantRoots[string(v.root)]++
		require.True(t, v.compatIn)
		require.Equal(t, compatKey{root: string(v.root), digest: v.attTail.compat}, v.compat)
		cs := c.byCompat[v.compat]
		require.NotNil(t, cs)
		require.Contains(t, cs.views, v)
		for o := range cs.views {
			require.LessOrEqual(t, o.seq, cs.newest.seq, "newest is the newest holder")
		}
	}
	gotIn := 0
	for _, cs := range c.byCompat {
		require.NotEmpty(t, cs.views)
		gotIn += len(cs.views)
	}
	require.Equal(t, wantIn, gotIn)
	require.Equal(t, wantRoots, c.rootCertified)
	var retiredViews, retiredAtts uint64
	for v := range c.retired {
		require.True(t, v.retired)
		require.False(t, v.live)
		retiredViews += v.attBytes
		require.Equal(t, len(v.pending) > 0, func() bool { _, ok := c.retiredLive[v]; return ok }(), "retiredLive tracks retired views with pending leases")
	}
	require.LessOrEqual(t, len(c.retiredLive), len(c.retired))
	for a := range c.retiredAtts {
		retiredAtts += a.bytes
	}
	require.Equal(t, retiredViews, c.retiredViewAttBytes)
	require.Equal(t, retiredAtts, c.retiredAttBytes)
	// heap order
	for i := 1; i < len(c.evictable.items); i++ {
		require.Less(t, c.evictable.items[(i-1)/2].seq, c.evictable.items[i].seq+1)
	}
	for i, v := range c.evictable.items {
		require.Equal(t, i, v.evictIdx)
	}
	for i, v := range c.pinned.items {
		require.Equal(t, i, v.pinIdx)
	}
}

// ---- reader that counts how often the catalog queries accounting ------------------------------

type countingWorld struct {
	mu      sync.Mutex
	readers map[*cReader]struct{}
	// queries is the hardware independent metric: calls into reader/backend accounting
	readerQueries atomic.Int64
	totalQueries  atomic.Int64
}

type cReader struct {
	w       *countingWorld
	root    []byte
	charge  atomic.Uint64
	retired atomic.Bool
	closed  atomic.Bool
}

func (w *countingWorld) newReader(root []byte, charge uint64) *cReader {
	r := &cReader{w: w, root: root}
	r.charge.Store(charge)
	w.mu.Lock()
	if w.readers == nil {
		w.readers = map[*cReader]struct{}{}
	}
	w.readers[r] = struct{}{}
	w.mu.Unlock()
	return r
}

func (r *cReader) RootHash() []byte { return append([]byte(nil), r.root...) }
func (r *cReader) RetainedBytes() uint64 {
	r.w.readerQueries.Add(1)
	if r.retired.Load() || r.closed.Load() {
		return 0
	}
	return r.charge.Load()
}
func (r *cReader) Close() error { r.closed.Store(true); return nil }
func (r *cReader) GetInclusionCert(ctx context.Context, key []byte) (*api.InclusionCert, error) {
	return &api.InclusionCert{}, ctx.Err()
}
func (r *cReader) RetireAccounting() { r.retired.Store(true) }

// RetainedBytesTotal is the backend's O(1) total (the fake sums internally; the call is what is counted).
func (w *countingWorld) RetainedBytesTotal() uint64 {
	w.totalQueries.Add(1)
	w.mu.Lock()
	defer w.mu.Unlock()
	var s uint64
	for r := range w.readers {
		if !r.retired.Load() && !r.closed.Load() {
			s += r.charge.Load()
		}
	}
	return s
}

func (w *countingWorld) queries() int64 { return w.readerQueries.Load() + w.totalQueries.Load() }

func pub(t testing.TB, c *Catalog, w *countingWorld, n int, round uint64, charge uint64) *cReader {
	t.Helper()
	root := make([]byte, 32)
	root[30], root[31] = byte(n>>8), byte(n)
	r := w.newReader(root, charge)
	require.NoError(t, c.Publish(context.Background(), PublishInput{
		Block: BlockIdentity{Number: api.NewBigIntFromUint64(uint64(n)), ConfigLineage: []byte("cfg")}, Root: root, Reader: r,
		Cert: Certification{UC: ucFor(t, root, round)},
	}))
	return r
}

func rootN(n int) []byte {
	root := make([]byte, 32)
	root[30], root[31] = byte(n>>8), byte(n)
	return root
}

func scalingLimits(v int) Limits {
	l := DefaultLimits()
	l.MaxViews = v
	l.MaxAttachmentsPerView = 4
	l.RetentionBudgetBytes = 1 << 40
	l.MaxConcurrentPins = 8
	return l
}

// Ordinary operations (publish with one eviction, acquire latest/root, release, stats) query
// reader accounting a constant number of times, however many views are retained: the work per
// operation is independent of V. The linear-scan reference model grows with V.
func TestCatalogOrdinaryOperationsAreIndependentOfViewCount(t *testing.T) {
	type sample struct{ indexed, reference float64 }
	var samples []sample
	for _, v := range []int{16, 32, 64, 128, 256} {
		// indexed catalog with the backend total
		w := &countingWorld{}
		c, err := NewCatalog(scalingLimits(v), WithAccountant(w))
		require.NoError(t, err)
		for i := 1; i <= v; i++ {
			pub(t, c, w, i, uint64(i), 100)
		}
		before := w.queries()
		const ops = 20
		for k := 0; k < ops; k++ {
			pub(t, c, w, v+1+k, uint64(v+1+k), 100) // evicts the oldest
			l, err := c.AcquireLatest(context.Background())
			require.NoError(t, err)
			l2, err := c.AcquireRoot(context.Background(), rootN(v-5+k+1))
			require.NoError(t, err)
			l.Release()
			l2.Release()
			_ = c.Stats()
		}
		perOp := float64(w.queries()-before) / ops
		checkInvariants(t, c)

		// reference model (quadratic accounting) with plain readers
		rw := &countingWorld{}
		rc, err := newRefCatalog(scalingLimits(v))
		require.NoError(t, err)
		for i := 1; i <= v; i++ {
			root := rootN(i)
			require.NoError(t, rc.Publish(context.Background(), PublishInput{Block: BlockIdentity{Number: api.NewBigIntFromUint64(uint64(i)), ConfigLineage: []byte("cfg")},
				Root: root, Reader: rw.newReader(root, 100), Cert: Certification{UC: ucFor(t, root, uint64(i))}}))
		}
		rbefore := rw.queries()
		for k := 0; k < ops; k++ {
			root := rootN(v + 1 + k)
			require.NoError(t, rc.Publish(context.Background(), PublishInput{Block: BlockIdentity{Number: api.NewBigIntFromUint64(uint64(v + 1 + k)), ConfigLineage: []byte("cfg")},
				Root: root, Reader: rw.newReader(root, 100), Cert: Certification{UC: ucFor(t, root, uint64(v+1+k))}}))
			l, err := rc.AcquireLatest(context.Background())
			require.NoError(t, err)
			l2, err := rc.AcquireRoot(context.Background(), rootN(v-5+k+1))
			require.NoError(t, err)
			l.Release()
			l2.Release()
			_ = rc.Stats()
		}
		samples = append(samples, sample{perOp, float64(rw.queries()-rbefore) / ops})
	}
	for _, s := range samples {
		require.LessOrEqual(t, s.indexed, 12.0, "accounting queries per operation batch must not depend on V")
	}
	// flat: the largest V costs no more than the smallest
	require.InDelta(t, samples[0].indexed, samples[len(samples)-1].indexed, 1.0)
	// the reference model is superlinear: doubling V at least doubles its queries
	for i := 1; i < len(samples); i++ {
		require.Greater(t, samples[i].reference, 1.8*samples[i-1].reference, "reference model must scale with V or the test proves nothing")
	}
}

// Evicting Θ(V) views under pressure (a big attachment forcing the budget) is linear in V with the
// indexes; the reference model re-sums the readers per eviction and is quadratic.
func TestCatalogFullEvictionIsLinear(t *testing.T) {
	var indexed, reference []int64
	vs := []int{16, 32, 64, 128, 256}
	for _, v := range vs {
		l := scalingLimits(v)
		l.RetentionBudgetBytes = uint64(v)*600 + 1000

		w := &countingWorld{}
		c, err := NewCatalog(l, WithAccountant(w))
		require.NoError(t, err)
		var last *cReader
		for i := 1; i <= v; i++ {
			last = pub(t, c, w, i, uint64(i), 100)
		}
		before := w.queries()
		// the newest view gets a certification so large that all history has to go
		big := ucFor(t, last.root, uint64(v)+10)
		err = c.AttachCertification(context.Background(), BlockIdentity{Number: api.NewBigIntFromUint64(uint64(v)), ConfigLineage: []byte("cfg")}, last.root,
			Certification{UC: big, PartitionView: make([]byte, l.RetentionBudgetBytes)})
		_ = err
		indexed = append(indexed, w.queries()-before)
		require.Equal(t, 1, c.Stats().Views, "all history evicted")
		checkInvariants(t, c)

		rw := &countingWorld{}
		rc, err := newRefCatalog(l)
		require.NoError(t, err)
		var rlast *cReader
		for i := 1; i <= v; i++ {
			root := rootN(i)
			rlast = rw.newReader(root, 100)
			require.NoError(t, rc.Publish(context.Background(), PublishInput{Block: BlockIdentity{Number: api.NewBigIntFromUint64(uint64(i)), ConfigLineage: []byte("cfg")},
				Root: root, Reader: rlast, Cert: Certification{UC: ucFor(t, root, uint64(i))}}))
		}
		rbefore := rw.queries()
		_ = rc.AttachCertification(context.Background(), BlockIdentity{Number: api.NewBigIntFromUint64(uint64(v)), ConfigLineage: []byte("cfg")}, rlast.root,
			Certification{UC: ucFor(t, rlast.root, uint64(v)+10), PartitionView: make([]byte, l.RetentionBudgetBytes)})
		reference = append(reference, rw.queries()-rbefore)
	}
	for i := 1; i < len(vs); i++ {
		// linear: doubling V at most doubles (plus a constant) the work
		require.LessOrEqual(t, indexed[i], 2*indexed[i-1]+8, "V=%d: %v", vs[i], indexed)
		// superlinear reference
		require.Greater(t, float64(reference[i]), 3.0*float64(reference[i-1]), "reference %v", reference)
	}
}

// ---- exact equivalence with the reference model -------------------------------------------------

type twin struct {
	a, b *cReader // indexed catalog's reader, reference catalog's reader
}

type diffWorld struct {
	t   *testing.T
	rng *rand.Rand
	c   *Catalog
	r   *refCatalog
	wa  *countingWorld
	wb  *countingWorld

	readers []twin
	leases  []struct {
		a *Lease
		b *refLease
	}
	nextN int
}

func (d *diffWorld) block(n int) BlockIdentity {
	return BlockIdentity{Number: api.NewBigIntFromUint64(uint64(n)), ConfigLineage: []byte("cfg")}
}

func (d *diffWorld) compare(step string) {
	t := d.t
	t.Helper()
	require.Equal(t, d.r.Stats(), d.c.Stats(), step)
	for _, tw := range d.readers {
		require.Equal(t, tw.b.closed.Load(), tw.a.closed.Load(), "%s: reader closed", step)
	}
	for _, l := range d.leases {
		require.Equal(t, l.b.ctx.Err() != nil, l.a.ctx.Err() != nil, "%s: lease cancelled", step)
	}
	checkInvariants(t, d.c)
}

func errClass(err error) string {
	for _, e := range []error{ErrNoFinalizedView, ErrViewNotRetained, ErrBusyPins, ErrBusyBudget, ErrBudget, ErrInvalidPublication, ErrClosed} {
		if err != nil && errorsIs(err, e) {
			return e.Error()
		}
	}
	if err != nil {
		return "other"
	}
	return ""
}

func errorsIs(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func (d *diffWorld) randomN() int {
	if d.nextN == 0 || d.rng.Intn(4) == 0 {
		return 1 + d.rng.Intn(max(d.nextN, 1))
	}
	return d.nextN
}

func (d *diffWorld) step() {
	t := d.t
	op := d.rng.Intn(12)
	switch {
	case op < 3: // publish a new view (sometimes an already known identity)
		n := d.nextN + 1
		if d.nextN > 0 && d.rng.Intn(5) == 0 {
			n = d.randomN() // duplicate delivery of a known block
		} else {
			d.nextN = n
		}
		root := rootN(n)
		charge := uint64(d.rng.Intn(400))
		round := uint64(n)*3 + uint64(d.rng.Intn(3))
		var pv []byte
		if d.rng.Intn(3) == 0 {
			pv = make([]byte, d.rng.Intn(1500))
		}
		ra, rb := d.wa.newReader(root, charge), d.wb.newReader(root, charge)
		d.readers = append(d.readers, twin{ra, rb})
		ea := d.c.Publish(context.Background(), PublishInput{Block: d.block(n), Root: root, Reader: ra, Cert: Certification{UC: ucFor(t, root, round), PartitionView: pv}})
		eb := d.r.Publish(context.Background(), PublishInput{Block: d.block(n), Root: root, Reader: rb, Cert: Certification{UC: ucFor(t, root, round), PartitionView: pv}})
		require.Equal(t, errClass(eb), errClass(ea), "publish %d", n)
	case op < 5: // attach a certification
		n := d.randomN()
		if n == 0 {
			return
		}
		root := rootN(n)
		round := uint64(d.rng.Intn(n*3 + 8))
		var pv []byte
		if d.rng.Intn(2) == 0 {
			pv = make([]byte, d.rng.Intn(2500))
		}
		ea := d.c.AttachCertification(context.Background(), d.block(n), root, Certification{UC: ucFor(t, root, round), PartitionView: pv})
		eb := d.r.AttachCertification(context.Background(), d.block(n), root, Certification{UC: ucFor(t, root, round), PartitionView: pv})
		require.Equal(t, errClass(eb), errClass(ea), "attach %d", n)
	case op < 7: // acquire
		var la *Lease
		var lb *refLease
		var ea, eb error
		if d.rng.Intn(2) == 0 {
			la, ea = d.c.AcquireLatest(context.Background())
			lb, eb = d.r.AcquireLatest(context.Background())
		} else {
			root := rootN(d.randomN())
			la, ea = d.c.AcquireRoot(context.Background(), root)
			lb, eb = d.r.AcquireRoot(context.Background(), root)
		}
		require.Equal(t, errClass(eb), errClass(ea), "acquire")
		if ea == nil {
			require.Equal(t, lb.v.root, la.Root())
			require.Equal(t, lb.att.uc, la.UC())
			require.Equal(t, lb.att.view, la.PartitionView())
			require.True(t, lb.v.block.equal(la.Block()))
			d.leases = append(d.leases, struct {
				a *Lease
				b *refLease
			}{la, lb})
		}
	case op < 9 && len(d.leases) > 0: // release in any order
		i := d.rng.Intn(len(d.leases))
		d.leases[i].a.Release()
		d.leases[i].b.Release()
		d.leases = append(d.leases[:i], d.leases[i+1:]...)
	case op < 11 && len(d.readers) > 0: // the backend charge of a reader changes (writes arrive)
		tw := d.readers[d.rng.Intn(len(d.readers))]
		c := uint64(d.rng.Intn(600))
		tw.a.charge.Store(c)
		tw.b.charge.Store(c)
		// the next operation observes it
		_ = d.c.Stats()
		_ = d.r.Stats()
	default:
		_ = d.c.Stats()
	}
}

// Through random publications (including duplicates and rejected ones), attachments, acquisitions,
// out-of-order releases and charge changes, the indexed catalog is observably identical to the
// reference model (stats, reader closes, lease cancellation, served material, errors) and every
// maintained total and index equals its recomputation.
func TestCatalogMatchesReferenceModel(t *testing.T) {
	for seed := int64(1); seed <= 24; seed++ {
		rng := rand.New(rand.NewSource(seed))
		l := DefaultLimits()
		l.MaxViews = 2 + rng.Intn(5)
		l.MaxAttachmentsPerView = 1 + rng.Intn(4)
		l.RetentionBudgetBytes = uint64(500 + rng.Intn(4000))
		l.MaxConcurrentPins = 1 + rng.Intn(5)
		c, err := NewCatalog(l)
		require.NoError(t, err)
		r, err := newRefCatalog(l)
		require.NoError(t, err)
		d := &diffWorld{t: t, rng: rng, c: c, r: r, wa: &countingWorld{}, wb: &countingWorld{}}
		for i := 0; i < 250; i++ {
			d.step()
			d.compare(fmt.Sprintf("seed %d step %d", seed, i))
		}
		// teardown: release everything, close
		for _, ls := range d.leases {
			ls.a.Release()
			ls.b.Release()
		}
		d.compare(fmt.Sprintf("seed %d released", seed))
		require.NoError(t, c.Close())
		require.NoError(t, r.Close())
		for _, tw := range d.readers {
			require.Equal(t, tw.b.closed.Load(), tw.a.closed.Load(), "seed %d: reader closed after Close", seed)
		}
	}
}

// ---- accelerated accounting with the real memory backend ---------------------------------------

// With the backend total (O(1)) the catalog's retained bytes equal the slow recomputation (sum over
// every held reader and attachment) through out-of-order lease releases, evictions, rejected
// publications and duplicate readers.
func TestCatalogTotalsMatchSlowRecomputationWithMemoryBackend(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		rng := rand.New(rand.NewSource(seed))
		ctx := context.Background()
		be := backend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
		l := DefaultLimits()
		l.MaxViews = 3 + rng.Intn(4)
		l.RetentionBudgetBytes = uint64(20000 + rng.Intn(60000))
		l.MaxConcurrentPins = 4
		c, err := NewCatalog(l, WithAccountant(be))
		require.NoError(t, err)

		var leases []*Lease
		next, blockNo := 0, 0
		commit := func() {
			snap, err := be.CreateSnapshot(ctx)
			require.NoError(t, err)
			var in []backend.LeafInput
			for i := 0; i < 1+rng.Intn(20); i++ {
				next++
				key := make([]byte, api.StateTreeKeyLengthBytes)
				key[0], key[1], key[2] = byte(next*37), byte(next), byte(next>>8)
				in = append(in, backend.LeafInput{Key: key, Value: make([]byte, 32)})
			}
			res, err := snap.AddLeavesClassified(ctx, in)
			require.NoError(t, err)
			blockNo++
			require.NoError(t, snap.Commit(ctx, backend.CommitMetadata{BlockNumber: api.NewBigIntFromUint64(uint64(blockNo)), RootHash: res.CandidateRoot}))
		}
		slow := func() uint64 {
			c.mu.Lock()
			defer c.mu.Unlock()
			var total uint64
			for v := c.head; v != nil; v = v.next {
				total += v.reader.RetainedBytes()
				for a := v.attHead; a != nil; a = a.next {
					total += a.bytes
				}
			}
			for v := range c.retired {
				total += v.reader.RetainedBytes()
				for a := v.attHead; a != nil; a = a.next {
					total += a.bytes
				}
			}
			for a := range c.retiredAtts {
				total += a.bytes
			}
			return total
		}
		var lastRoot []byte
		for step := 0; step < 300; step++ {
			switch op := rng.Intn(12); {
			case op < 3:
				commit()
			case op < 6:
				v, err := be.RetainReadView(ctx)
				require.NoError(t, err)
				root := v.RootHash()
				round := uint64(step + 1)
				var pv []byte
				if rng.Intn(3) == 0 {
					pv = make([]byte, rng.Intn(3000))
				}
				blk := blockNo
				if len(lastRoot) > 0 && string(lastRoot) == string(root) && rng.Intn(2) == 0 {
					round += 1000 // same state again: certification of a repeat
				}
				lastRoot = root
				err = c.Publish(ctx, PublishInput{Block: BlockIdentity{Number: api.NewBigIntFromUint64(uint64(blk)), ConfigLineage: []byte("cfg")}, Root: root, Reader: v,
					Cert: Certification{UC: ucFor(t, root, round), PartitionView: pv}})
				if err != nil {
					require.ErrorIs(t, err, ErrBudget)
				}
			case op < 8:
				var ls *Lease
				var err error
				if rng.Intn(2) == 0 {
					ls, err = c.AcquireLatest(ctx)
				} else if len(lastRoot) > 0 {
					ls, err = c.AcquireRoot(ctx, lastRoot)
				}
				if err == nil && ls != nil {
					leases = append(leases, ls)
				}
			case op < 10 && len(leases) > 0:
				i := rng.Intn(len(leases))
				leases[i].Release()
				leases = append(leases[:i], leases[i+1:]...)
			default:
				_ = c.Stats()
			}
			checkInvariants(t, c)
			require.Equal(t, slow(), c.Stats().RetainedBytes, "seed %d step %d", seed, step)
		}
		for _, ls := range leases {
			ls.Release()
		}
		require.Equal(t, slow(), c.Stats().RetainedBytes)
		require.NoError(t, c.Close())
		require.Zero(t, be.RetainedBytesTotal(), "all readers closed")
	}
}

// Under sustained retention pressure each lease is looked at by a pressure pass once, not by every later
// pass: P pinned leases on views which retention pressure retires, then released one by one while the
// catalog stays over budget, cost O(P) lease visits in total; the previous passes re-scanned every
// lease (and, per pass, all views x leases) and cost Θ(P²) and worse.
func TestPressurePassesVisitEachLeaseOnce(t *testing.T) {
	ctx := context.Background()
	var indexed, reference []uint64
	ps := []int{8, 16, 32, 64, 128}
	for _, p := range ps {
		l := scalingLimits(2) // the view limit retires the pinned views as newer ones are published
		l.MaxConcurrentPins = p
		l.RetentionBudgetBytes = uint64(p+6) * 400 // fits the history while charges are small
		w, rw := &countingWorld{}, &countingWorld{}
		c, err := NewCatalog(l, WithAccountant(w))
		require.NoError(t, err)
		rc, err := newRefCatalog(l)
		require.NoError(t, err)
		var readers, rreaders []*cReader
		var leases []*Lease
		var rleases []*refLease
		publishBoth := func(i int) {
			readers = append(readers, pub(t, c, w, i, uint64(i), 1))
			root := rootN(i)
			rr := rw.newReader(root, 1)
			rreaders = append(rreaders, rr)
			require.NoError(t, rc.Publish(ctx, PublishInput{Block: BlockIdentity{Number: api.NewBigIntFromUint64(uint64(i)), ConfigLineage: []byte("cfg")},
				Root: root, Reader: rr, Cert: Certification{UC: ucFor(t, root, uint64(i))}}))
		}
		for i := 1; i <= p; i++ {
			publishBoth(i)
			ls, err := c.AcquireLatest(ctx)
			require.NoError(t, err)
			rls, err := rc.AcquireLatest(ctx)
			require.NoError(t, err)
			leases, rleases = append(leases, ls), append(rleases, rls)
		}
		// the charges grow (writes arrive): from now on the retired pinned history is over budget
		for i := range readers {
			readers[i].charge.Store(800)
			rreaders[i].charge.Store(800)
		}
		require.Equal(t, p-2, c.Stats().RetiredPinned)
		// the pins drain one by one, in a scrambled order, while pressure persists
		for _, i := range rand.New(rand.NewSource(int64(p))).Perm(len(leases)) {
			leases[i].Release()
			rleases[i].Release()
		}
		checkInvariants(t, c)
		indexed = append(indexed, c.leaseVisits)
		reference = append(reference, rc.leaseVisits)
		require.Equal(t, rc.Stats(), c.Stats(), "p=%d: identical observable behavior", p)
	}
	for i, p := range ps {
		require.LessOrEqual(t, indexed[i], uint64(3*p), "p=%d: %v", p, indexed)
	}
	last := len(ps) - 1
	require.Greater(t, reference[last], 8*indexed[last], "reference %v indexed %v", reference, indexed)
	for i := 1; i < len(ps); i++ {
		require.Greater(t, float64(reference[i]), 3.0*float64(reference[i-1]), "the reference is superlinear: %v", reference)
	}
}

// The constructor wiring: the backends are accountants, other types silently are not.
func TestAccountantForWiresBackendsWhichCanTotal(t *testing.T) {
	be := backend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	c, err := NewCatalog(DefaultLimits(), AccountantFor(be))
	require.NoError(t, err)
	require.NotNil(t, c.acc, "the memory backend is an O(1) accountant")
	c, err = NewCatalog(DefaultLimits(), AccountantFor(struct{}{}), AccountantFor(nil))
	require.NoError(t, err)
	require.Nil(t, c.acc, "no accountant: the catalog uses the linear fallback")
	// ... and the wired catalog behaves identically to the fallback one
	w := &countingWorld{}
	c, err = NewCatalog(scalingLimits(4), AccountantFor(w))
	require.NoError(t, err)
	require.NotNil(t, c.acc)
}

// The digest lookup of the publication path is an index probe: every lookup in a view's attachment index is
// counted wherever the loop lives, so a lookup which scanned the retained views would show V probes. Per
// successful lookup exactly one view is probed, flat across V; the reference scan (kept here as the model of the
// previous implementation, going through the same counted index) probes ~V/2 and grows with V.
func TestDigestLookupDoesNotScanTheRetainedViews(t *testing.T) {
	var indexed, scan []float64
	for _, v := range []int{16, 32, 64, 128, 256} {
		w := &countingWorld{}
		c, err := NewCatalog(scalingLimits(v), WithAccountant(w))
		require.NoError(t, err)
		digests := map[int][32]byte{}
		for i := 1; i <= v; i++ {
			pub(t, c, w, i, uint64(i), 100)
			digests[i] = sha256.Sum256(ucFor(t, rootN(i), uint64(i)))
		}
		scanOnce := func(d [32]byte) bool { // the previous implementation: newest view first
			c.mu.Lock()
			defer c.mu.Unlock()
			for vw := c.tail; vw != nil; vw = vw.prev {
				if vw.attIdx.get(d) != nil {
					return true
				}
			}
			return false
		}
		const lookups = 50
		probesBefore := c.attProbes
		for k := 0; k < lookups; k++ {
			i := 1 + (k*7)%v
			block, root, ok := c.ViewForDigest(digests[i])
			require.True(t, ok)
			require.Equal(t, rootN(i), root)
			require.Equal(t, uint64(i), block.Number.Uint64())
			_, ok = c.PartitionViewForDigest(digests[i])
			require.True(t, ok)
		}
		indexed = append(indexed, float64(c.attProbes-probesBefore)/(2*lookups))
		require.InDelta(t, 1.0, indexed[len(indexed)-1], 1e-9, "exactly one view probed per successful lookup: %v", indexed)

		probesBefore = c.attProbes
		for k := 0; k < lookups; k++ {
			require.True(t, scanOnce(digests[1+(k*7)%v]))
		}
		scan = append(scan, float64(c.attProbes-probesBefore)/lookups)

		// a digest nobody holds probes no view at all
		probesBefore = c.attProbes
		_, _, ok := c.ViewForDigest([32]byte{9})
		require.False(t, ok)
		require.Equal(t, probesBefore, c.attProbes)
		checkInvariants(t, c)
		// evicted views are not found
		pub(t, c, w, v+1, uint64(v+1), 100)
		_, _, ok = c.ViewForDigest(digests[1])
		require.False(t, ok, "the oldest view was evicted")
	}
	for i := 1; i < len(scan); i++ {
		require.Greater(t, scan[i], 1.7*scan[i-1], "the reference scan grows with V (otherwise the counter proves nothing): %v", scan)
	}
	require.Greater(t, scan[len(scan)-1], 50*indexed[len(indexed)-1], "scan %v indexed %v", scan, indexed)
}
