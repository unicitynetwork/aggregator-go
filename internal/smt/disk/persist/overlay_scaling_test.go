package persist

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ---- in-memory store (no cgo) ---------------------------------------------------------------

type memStore struct {
	mu    sync.Mutex
	nodes map[disk.NodeKey][]byte
	root  disk.Hash
	block *api.BigInt
	reads int
}

func newMemStore() *memStore {
	return &memStore{nodes: map[disk.NodeKey][]byte{}, root: disk.EmptyRootHash()}
}

func (m *memStore) GetNode(k disk.NodeKey) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	v, ok := m.nodes[k]
	return append([]byte(nil), v...), ok, nil
}

func (m *memStore) GetNodes(keys []disk.NodeKey, _ bool) ([]storage.NodeReadResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]storage.NodeReadResult, len(keys))
	for i, k := range keys {
		m.reads++
		if v, ok := m.nodes[k]; ok {
			out[i] = storage.NodeReadResult{Value: append([]byte(nil), v...), Found: true}
		}
	}
	return out, nil
}

func (m *memStore) Close() error { return nil }
func (m *memStore) CommittedState() (storage.CommittedState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return storage.CommittedState{RootHash: m.root, BlockNumber: m.block}, nil
}
func (m *memStore) NewBatch() storage.Batch    { return &memBatch{m: m, sets: map[disk.NodeKey][]byte{}} }
func (m *memStore) Counters() storage.Counters { return storage.Counters{} }
func (m *memStore) Metrics() storage.Metrics   { return storage.Metrics{} }
func (m *memStore) node(k disk.NodeKey) []byte { m.mu.Lock(); defer m.mu.Unlock(); return m.nodes[k] }
func (m *memStore) has(k disk.NodeKey) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.nodes[k]
	return ok
}

type memBatch struct {
	m     *memStore
	sets  map[disk.NodeKey][]byte
	dels  []disk.NodeKey
	root  *disk.Hash
	block *api.BigInt
}

func (b *memBatch) SetNode(k disk.NodeKey, v []byte) error {
	b.sets[k] = append([]byte(nil), v...)
	return nil
}
func (b *memBatch) DeleteNode(k disk.NodeKey) error { b.dels = append(b.dels, k); return nil }
func (b *memBatch) SetCommittedState(r disk.Hash, n *api.BigInt) error {
	b.root, b.block = &r, n
	return nil
}
func (b *memBatch) Commit() error {
	b.m.mu.Lock()
	defer b.m.mu.Unlock()
	for _, k := range b.dels {
		delete(b.m.nodes, k)
	}
	for k, v := range b.sets {
		b.m.nodes[k] = v
	}
	if b.root != nil {
		b.m.root, b.m.block = *b.root, b.block
	}
	return nil
}
func (b *memBatch) Close() error { return nil }

func openMem(t testing.TB, s *memStore) *Tree {
	tree, err := Open(s)
	require.NoError(t, err)
	return tree
}

func randLeaf(rng *rand.Rand) disk.LeafInput {
	k := make([]byte, 32)
	rng.Read(k)
	v := make([]byte, 8+rng.Intn(24))
	rng.Read(v)
	return disk.LeafInput{Key: k, Value: v}
}

func randLeaves(rng *rand.Rand, n int) []disk.LeafInput {
	out := make([]disk.LeafInput, n)
	for i := range out {
		out[i] = randLeaf(rng)
	}
	return out
}

// walkNodes calls fn for every materialized node of the tree with its node key.
func walkNodes(b *disk.Branch, key disk.NodeKey, prefix disk.PrefixBits, startBit int, fn func(*disk.Branch, disk.NodeKey)) error {
	if b == nil || b.Kind == disk.BranchKindStub {
		return nil
	}
	fn(b, key)
	if b.Kind != disk.BranchKindInternal {
		return nil
	}
	for dir := byte(0); dir < 2; dir++ {
		ck, cp, cs, err := childNodeKey(prefix, startBit, b.Internal.Path, dir)
		if err != nil {
			return err
		}
		child := b.Internal.Left
		if dir == 1 {
			child = b.Internal.Right
		}
		if err := walkNodes(child, ck, cp, cs, fn); err != nil {
			return err
		}
	}
	return nil
}

func encodeNode(t testing.TB, b *disk.Branch) []byte {
	var enc []byte
	var err error
	if b.Kind == disk.BranchKindLeaf {
		enc, err = disk.MarshalLeaf(b.Leaf)
	} else {
		enc, err = disk.MarshalInternal(b.Internal)
	}
	require.NoError(t, err)
	return enc
}

// requireOverlayConsistent checks the invariant of the overlay: whatever the snapshot would
// persist for every materialized node (its overlay entry, else what the store already holds) is
// exactly the node's encoding.
func requireOverlayConsistent(t testing.TB, s *Snapshot, store *memStore, step string) {
	t.Helper()
	require.NoError(t, walkNodes(s.tree.Root(), disk.RootNodeKey(), disk.PrefixBits{}, 0, func(b *disk.Branch, key disk.NodeKey) {
		want := encodeNode(t, b)
		if e, ok := s.ownOverlay[key]; ok {
			require.Equal(t, want, e.value, "%s: overlay entry at %x", step, key.Bytes())
			return
		}
		if e, ok := s.parentOverlay.get(key); ok {
			require.Equal(t, want, e.value, "%s: inherited entry at %x", step, key.Bytes())
			return
		}
		require.Equal(t, want, store.node(key), "%s: unchanged node at %x must already be in the store", step, key.Bytes())
	}))
	// the write list mirrors the overlay exactly, one entry per key
	require.Len(t, s.overlayWrites, len(s.ownOverlay), step)
	seen := map[disk.NodeKey]bool{}
	for i, w := range s.overlayWrites {
		require.False(t, seen[w.Key], step)
		seen[w.Key] = true
		require.Equal(t, i, s.writeIdx[w.Key])
		require.Equal(t, s.ownOverlay[w.Key].value, w.Value)
	}
}

func certsOf(t testing.TB, root disk.Hash, store storage.ReadStore, leaves []disk.LeafInput) []*api.InclusionCert {
	keys := make([][]byte, len(leaves))
	for i, l := range leaves {
		keys[i] = l.Key
	}
	certs, err := BuildInclusionCerts(root, store, keys)
	require.NoError(t, err)
	for i, c := range certs {
		require.NoError(t, c.Verify(leaves[i].Key, leaves[i].Value, root[:], api.SHA256), "leaf %d", i)
	}
	return certs
}

// ---- equivalence with the full re-collection -------------------------------------------------

// Through random blocks of many small and large updates, with reopening between blocks (so nodes are
// loaded from the store and move when edges split), the incrementally collected overlay is always
// consistent, and committing it yields the same root and the same readable tree as the reference
// which re-collects the whole materialized tree after every update.
func TestIncrementalOverlayMatchesFullCollection(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		rng := rand.New(rand.NewSource(seed))
		incStore, refStore := newMemStore(), newMemStore()
		var all []disk.LeafInput
		for block := 1; block <= 6; block++ {
			inc, ref := openMem(t, incStore), openMem(t, refStore)
			a, err := inc.CreateSnapshot()
			require.NoError(t, err)
			b, err := ref.CreateSnapshot()
			require.NoError(t, err)
			calls := 1 + rng.Intn(8)
			for c := 0; c < calls; c++ {
				n := 1 + rng.Intn(1+rng.Intn(40))
				in := randLeaves(rng, n)
				if rng.Intn(4) == 0 && len(all) > 0 { // duplicates of committed leaves are accepted as no-ops
					in = append(in, all[rng.Intn(len(all))])
				}
				ra, err := a.AddLeaves(in)
				require.NoError(t, err)
				rb, err := b.AddLeaves(in)
				require.NoError(t, err)
				require.NoError(t, b.rebuildOverlay()) // the reference: collect everything again
				require.Equal(t, rb.CandidateRoot, ra.CandidateRoot)
				for _, i := range ra.AcceptedIndexes {
					all = append(all, in[i])
				}
				step := fmt.Sprintf("seed %d block %d call %d", seed, block, c)
				requireOverlayConsistent(t, a, incStore, step)
				requireOverlayConsistent(t, b, refStore, step)
			}
			// an explicit full rebuild of the incremental snapshot changes nothing observable
			if rng.Intn(3) == 0 {
				before := map[disk.NodeKey][]byte{}
				for k, e := range a.ownOverlay {
					before[k] = e.value
				}
				require.NoError(t, a.rebuildOverlay())
				requireOverlayConsistent(t, a, incStore, "after rebuild")
				for k, v := range before {
					if e, ok := a.ownOverlay[k]; ok {
						require.Equal(t, v, e.value)
					}
				}
			}
			require.NoError(t, a.Commit(api.NewBigIntFromUint64(uint64(block))))
			require.NoError(t, b.Commit(api.NewBigIntFromUint64(uint64(block))))
			require.Equal(t, refStore.root, incStore.root)
		}
		certsInc := certsOf(t, incStore.root, incStore, all)
		certsRef := certsOf(t, refStore.root, refStore, all)
		for i := range certsInc {
			require.Equal(t, certsRef[i], certsInc[i])
		}
	}
}

// ---- work of collection ----------------------------------------------------------------------

type collectWork struct{ visited, encoded int }

func workOf(s *Snapshot) collectWork { return collectWork{s.collectVisited, s.collectEncoded} }

// With a fixed number B of new leaves, the nodes encoded by the incremental collection are the
// nodes the updates create (O(B·depth)) however the leaves are split into calls; the full
// re-collection re-encodes the accumulated materialized tree after every call and grows with the
// number of calls. Counted, not timed.
func TestCollectionWorkDoesNotDependOnTheNumberOfUpdateCalls(t *testing.T) {
	const prefill, total = 4096, 256
	var incremental, reference []int
	calls := []int{1, 2, 4, 8, 16, 32, 64, 128, 256}
	for _, c := range calls {
		for _, mode := range []string{"incremental", "reference"} {
			rng := rand.New(rand.NewSource(99)) // same data for every run
			store := newMemStore()
			tree := openMem(t, store)
			snap, err := tree.CreateSnapshot()
			require.NoError(t, err)
			_, err = snap.AddLeaves(randLeaves(rng, prefill))
			require.NoError(t, err)
			require.NoError(t, snap.Commit(api.NewBigIntFromUint64(1)))

			tree = openMem(t, store)
			snap, err = tree.CreateSnapshot()
			require.NoError(t, err)
			leaves := randLeaves(rng, total)
			per := total / c
			for i := 0; i < c; i++ {
				_, err := snap.AddLeaves(leaves[i*per : (i+1)*per])
				require.NoError(t, err)
				if mode == "reference" {
					require.NoError(t, snap.rebuildOverlay())
				}
			}
			w := workOf(snap)
			if mode == "incremental" {
				incremental = append(incremental, w.encoded)
				// every examined node is either encoded or the unchanged child of an encoded node
				require.LessOrEqual(t, w.visited, 2*w.encoded+c, "c=%d", c)
			} else {
				reference = append(reference, w.encoded)
			}
		}
	}
	depth := math.Log2(prefill+total) + 3
	for i, c := range calls {
		// the bound of the nodes B updates create or move: B·depth, for any split into calls
		require.LessOrEqual(t, float64(incremental[i]), 3*float64(total)*depth, "c=%d: %v", c, incremental)
		// ... and it does not grow with the number of calls: at most 2x of the single-call count + one path per call
		require.LessOrEqual(t, incremental[i], 2*incremental[0]+c*int(depth), "c=%d: %v", c, incremental)
	}
	// the reference model grows with the number of calls and the incremental one stays below it
	last := len(calls) - 1
	require.Greater(t, reference[last], 4*incremental[last], "reference %v incremental %v", reference, incremental)
	for i := 1; i < len(calls); i++ {
		require.GreaterOrEqual(t, reference[i], reference[i-1], "reference %v", reference)
	}
}

// ---- persistent overlay -----------------------------------------------------------------------

// refFlatten is the previous Fork: it copies the inherited overlay into every child.
func refFlatten(base, own nodeOverlay) (nodeOverlay, int) {
	out := make(nodeOverlay, len(base)+len(own))
	copies := 0
	for k, e := range base {
		out[k] = e
		copies++
	}
	for k, e := range own {
		out[k] = e
		copies++
	}
	return out, copies
}

// Chains of speculative snapshots: every lookup through the persistent chain equals the lookup in the
// flattened overlay of the previous implementation, and the committed result equals the sequential one.
func TestForkChainLookupsMatchFlatteningReference(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		rng := rand.New(rand.NewSource(seed))
		store := newMemStore()
		tree := openMem(t, store)
		root, err := tree.CreateSnapshot()
		require.NoError(t, err)
		snaps := []*Snapshot{root}
		var all [][]disk.LeafInput
		cur := root
		depth := 2 + rng.Intn(10)
		var flat nodeOverlay // the previous implementation's inherited overlay of the next child
		for i := 0; i < depth; i++ {
			in := randLeaves(rng, 1+rng.Intn(20))
			_, err := cur.AddLeaves(in)
			require.NoError(t, err)
			all = append(all, in)
			next, err := cur.Fork()
			require.NoError(t, err)
			flat, _ = refFlatten(flat, cur.ownOverlay)
			// every key of the flattened reference resolves identically through the chain; others do not resolve
			for k, want := range flat {
				got, ok := next.parentOverlay.get(k)
				require.True(t, ok)
				require.Equal(t, want.value, got.value)
			}
			for j := 0; j < 20; j++ {
				k, _ := disk.NewNodeKey(1+rng.Intn(20), disk.PrefixBits{byte(rng.Intn(256)), byte(rng.Intn(256))})
				_, inFlat := flat[k]
				_, inChain := next.parentOverlay.get(k)
				require.Equal(t, inFlat, inChain)
			}
			require.LessOrEqual(t, next.parentOverlay.depth(), 2+int(math.Log2(float64(len(flat)+1))), "layers stay logarithmic")
			snaps = append(snaps, next)
			cur = next
		}
		// the leaves of the whole chain, applied to the plain tree, give the root of the last snapshot
		plain := disk.NewTree()
		var flatLeaves []disk.LeafInput
		for _, in := range all {
			flatLeaves = append(flatLeaves, in...)
		}
		res, err := plain.ApplyLeaves(flatLeaves)
		require.NoError(t, err)
		// the speculative root of the last-but-one snapshot equals it (the last has no own leaves)
		require.Equal(t, res.CandidateRoot, snaps[len(snaps)-2].RootHash())
		// commit the chain in order and compare with the sequential result
		for i, s := range snaps[:len(snaps)-1] {
			if i > 0 {
				require.NoError(t, s.SetCommitTarget(tree))
			}
			require.NoError(t, s.Commit(api.NewBigIntFromUint64(uint64(i+1))))
		}
		require.Equal(t, res.CandidateRoot, tree.RootHash())
		certsOf(t, tree.RootHash(), store, flatLeaves)
	}
}

// Forking a chain of g speculative snapshots with Δ new entries each copies O(Δ log g) entries per fork
// with the persistent chain, whereas flattening the inherited overlay copies Θ(gΔ) per fork. The copied
// entries are counted: the total is Θ(g²Δ) for the flattening and Θ(gΔ log g) for the chain.
func TestForkCopyingIsNearlyLinearInChainDepth(t *testing.T) {
	const delta = 24
	var chain, flatTotals []float64
	gs := []int{4, 8, 16, 32, 64, 128}
	for _, g := range gs {
		rng := rand.New(rand.NewSource(5))
		store := newMemStore()
		tree := openMem(t, store)
		cur, err := tree.CreateSnapshot()
		require.NoError(t, err)
		var flat nodeOverlay
		flatCopies := 0
		var ownTotal int
		for i := 0; i < g; i++ {
			_, err := cur.AddLeaves(randLeaves(rng, delta))
			require.NoError(t, err)
			next, err := cur.Fork()
			require.NoError(t, err)
			var c int
			flat, c = refFlatten(flat, cur.ownOverlay)
			flatCopies += c + len(cur.ownOverlay) // the old Fork cloned the own overlay as well
			ownTotal += len(cur.ownOverlay)
			cur = next
		}
		chain = append(chain, float64(tree.overlayCopies.Load()))
		flatTotals = append(flatTotals, float64(flatCopies))
		// the chain's cost: each entry is copied once when frozen and O(log) times when merged
		require.LessOrEqual(t, chain[len(chain)-1], float64(ownTotal)*(2+math.Log2(float64(g))), "g=%d", g)
	}
	for i := 1; i < len(gs); i++ {
		// flattening: doubling g more than triples the copies (quadratic, slightly damped by shared keys)
		require.Greater(t, flatTotals[i]/flatTotals[i-1], 3.1, "flattening copies %v", flatTotals)
	}
	// per g² the chain's cost falls over the sweep (n log n is below every quadratic)
	gFirst, gLast := float64(gs[0]), float64(gs[len(gs)-1])
	require.Less(t, (chain[len(gs)-1]/(gLast*gLast))/(chain[0]/(gFirst*gFirst)), 0.25, "chain copies %v", chain)
	// the merge pattern is bumpy at powers of two, so the widening gap is checked end to end
	gap := func(i int) float64 { return flatTotals[i] / chain[i] }
	require.Greater(t, gap(len(gs)-1), 2*gap(2), "the gap widens with g: chain %v flatten %v", chain, flatTotals)
	require.Greater(t, flatTotals[len(gs)-1], 3*chain[len(gs)-1], "chain %v flatten %v", chain, flatTotals)
}

func TestOverlayChainBasics(t *testing.T) {
	var c *overlayChain
	k := func(i int) disk.NodeKey { nk, _ := disk.NewNodeKey(8, disk.PrefixBits{byte(i)}); return nk }
	_, ok := c.get(k(1))
	require.False(t, ok)
	require.Equal(t, 0, c.depth())
	c = c.push(nodeOverlay{k(1): {value: []byte("old")}, k(2): {value: []byte("two")}}, nil)
	c = c.push(nodeOverlay{k(1): {value: []byte("new")}}, nil)
	e, ok := c.get(k(1))
	require.True(t, ok)
	require.True(t, bytes.Equal([]byte("new"), e.value), "the newest layer wins")
	e, _ = c.get(k(2))
	require.Equal(t, []byte("two"), e.value)
	// pushing never changes the chain it was derived from
	base := c
	derived := base.push(nodeOverlay{k(1): {value: []byte("newer")}, k(3): {value: []byte("three")}}, nil)
	e, _ = base.get(k(1))
	require.Equal(t, []byte("new"), e.value)
	_, ok = base.get(k(3))
	require.False(t, ok)
	e, _ = derived.get(k(1))
	require.Equal(t, []byte("newer"), e.value)
	// an empty layer is not pushed
	require.Equal(t, derived.depth(), derived.push(nil, nil).depth())
}

// The number of layers is logarithmic in the entries for EVERY sequence of layer sizes, in particular
// for decreasing ones (a pipeline whose blocks taper off), which a "merge while the lower layer is not
// larger" rule never merges.
func TestOverlayChainLayersAreLogarithmicForAnyLayerSizes(t *testing.T) {
	k := func(i int) disk.NodeKey {
		nk, err := disk.NewNodeKey(16, disk.PrefixBits{byte(i >> 8), byte(i)})
		require.NoError(t, err)
		return nk
	}
	layerOf := func(start, n int) nodeOverlay {
		l := make(nodeOverlay, n)
		for i := 0; i < n; i++ {
			l[k(start+i)] = overlayEntry{value: []byte{byte(i)}}
		}
		return l
	}
	check := func(name string, sizes []int) {
		var c *overlayChain
		var copies atomic.Int64
		total, next := 0, 0
		for _, n := range sizes {
			c = c.push(layerOf(next, n), &copies)
			next += n
			total += n
			require.LessOrEqual(t, c.depth(), bits.Len(uint(total)), "%s: %d layers for %d entries", name, c.depth(), total)
		}
		// copied entries: each pushed entry is copied O(log total) times
		require.LessOrEqual(t, copies.Load(), int64(total)*int64(bits.Len(uint(total))+1), name)
		// every key is still found, at its newest layer
		for i := 0; i < next; i++ {
			_, ok := c.get(k(i))
			require.True(t, ok, "%s: key %d", name, i)
		}
	}
	dec := make([]int, 0, 64)
	for i := 64; i >= 1; i-- {
		dec = append(dec, i)
	}
	check("decreasing 64..1", dec)
	inc := make([]int, 0, 64)
	for i := 1; i <= 64; i++ {
		inc = append(inc, i)
	}
	check("increasing", inc)
	check("equal", func() []int {
		s := make([]int, 100)
		for i := range s {
			s[i] = 7
		}
		return s
	}())
	rng := rand.New(rand.NewSource(3))
	for seed := 0; seed < 20; seed++ {
		sizes := make([]int, 50+rng.Intn(100))
		for i := range sizes {
			sizes[i] = 1 + rng.Intn(1<<uint(rng.Intn(9)))
		}
		check(fmt.Sprintf("random %d", seed), sizes)
	}
	// geometric decrease: the worst case for a "lower layer not larger" rule
	var geo []int
	for n := 1 << 12; n >= 1; n /= 2 {
		geo = append(geo, n)
	}
	check("halving", geo)
}

// A real chain of speculative snapshots with shrinking blocks keeps a logarithmic number of layers.
func TestForkChainOfShrinkingBlocksHasLogarithmicLayers(t *testing.T) {
	rng := rand.New(rand.NewSource(8))
	store := newMemStore()
	tree := openMem(t, store)
	cur, err := tree.CreateSnapshot()
	require.NoError(t, err)
	entries := 0
	for n := 64; n >= 1; n-- {
		_, err := cur.AddLeaves(randLeaves(rng, n))
		require.NoError(t, err)
		entries += len(cur.ownOverlay)
		next, err := cur.Fork()
		require.NoError(t, err)
		require.LessOrEqual(t, next.parentOverlay.depth(), bits.Len(uint(entries)), "after %d blocks (%d entries)", 65-n, entries)
		cur = next
	}
}
