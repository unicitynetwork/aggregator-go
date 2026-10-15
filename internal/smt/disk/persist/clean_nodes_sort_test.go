package persist

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

var prefilledLeaves = map[*memStore][]disk.LeafInput{}

func prefilled(t testing.TB, n int, opts Options) (*memStore, *rand.Rand) {
	rng := rand.New(rand.NewSource(int64(n)))
	store := newMemStore()
	tree, err := OpenWithOptions(store, opts)
	require.NoError(t, err)
	snap, err := tree.CreateSnapshot()
	require.NoError(t, err)
	leaves := randLeaves(rng, n)
	_, err = snap.AddLeaves(leaves)
	require.NoError(t, err)
	require.NoError(t, snap.Commit(api.NewBigIntFromUint64(1)))
	prefilledLeaves[store] = leaves
	return store, rng
}

// A loaded node which is unchanged is persisted already: the update writes the nodes it created (or
// moved), however many nodes the paths of its leaves loaded; the full re-collection writes every loaded
// node again.
func TestLoadedUnchangedNodesAreNotRewritten(t *testing.T) {
	const b, dups = 16, 48 // new leaves, and resubmitted leaves which are only looked up (duplicates and a modification)
	type row struct{ n, created, written, materialized, refWritten int }
	var rows []row
	for _, n := range []int{1 << 8, 1 << 10, 1 << 12, 1 << 14} {
		store, rng := prefilled(t, n, DefaultOptions())
		leaves := randLeaves(rng, b)
		// resubmitted committed leaves: their paths are loaded to classify them, nothing about them changes
		old := prefilledLeaves[store]
		for i := 0; i < dups; i++ {
			l := old[rng.Intn(len(old))]
			if i == 0 {
				l = disk.LeafInput{Key: l.Key, Value: []byte("a different value")} // rejected modification
			}
			leaves = append(leaves, l)
		}

		tree, err := Open(store)
		require.NoError(t, err)
		inc, err := tree.CreateSnapshot()
		require.NoError(t, err)
		res, err := inc.AddLeaves(leaves)
		require.NoError(t, err)

		ref, err := tree.CreateSnapshot()
		require.NoError(t, err)
		_, err = ref.AddLeaves(leaves)
		require.NoError(t, err)
		require.NoError(t, ref.rebuildOverlay()) // the full collection writes everything materialized

		requireOverlayConsistent(t, inc, store, fmt.Sprint("n=", n))
		rows = append(rows, row{n, res.Stats.OverlayEntries, len(inc.overlayWrites), res.Stats.MaterializedNodes, len(ref.overlayWrites)})
		// the overlay is exactly what was written: no clean node among it
		require.Equal(t, inc.collectEncoded, len(inc.overlayWrites))
		// every written node is new or moved: it is not what the store already holds at that key
		var same int
		for _, w := range inc.overlayWrites {
			if old := store.node(w.Key); string(old) == string(w.Value) {
				same++
			}
		}
		require.Zero(t, same, "n=%d: a node identical to the stored one was written", n)
	}
	for _, r := range rows {
		// the writes are bounded by the nodes the update created or moved: independent of the tree size
		require.LessOrEqual(t, r.written, 2*r.created, "%+v", r)
		require.Less(t, r.written, r.refWritten, "the full collection writes the loaded clean nodes as well: %+v", r)
		// the clean paths of the resubmitted leaves dominate what the full collection writes
		require.GreaterOrEqual(t, r.refWritten-r.written, dups, "%+v", r)
	}
	for _, r := range rows {
		// the nodes of B new leaves: B paths of depth O(log N), not the (B+D) loaded paths
		require.LessOrEqual(t, float64(r.written), 2*b*(math.Log2(float64(r.n))+3), "%+v", r)
	}
	first, last := rows[0], rows[len(rows)-1]
	// writes follow B log N; the loaded paths follow (B + D) log N
	grow := (math.Log2(float64(last.n)) + 3) / (math.Log2(float64(first.n)) + 3)
	require.LessOrEqual(t, float64(last.written), 1.5*grow*float64(first.written), "%+v vs %+v", first, last)
	require.Greater(t, last.refWritten-last.written, first.refWritten-first.written, "the clean paths the full collection rewrites grow with the tree")
}

// An update sorts its input once. Waves of the frontier materialization are put in node key order
// without comparison sorts and the existing leaf which collides with a batch is merged in without one.
func TestUpdateSortsOncePerCall(t *testing.T) {
	for _, mode := range []string{MaterializePath, MaterializeFrontier, MaterializeParallel} {
		opts := DefaultOptions()
		opts.MaterializeMode = mode
		store, rng := prefilled(t, 4096, opts)
		for _, b := range []int{2, 8, 64, 512} {
			tree, err := OpenWithOptions(store, opts)
			require.NoError(t, err)
			snap, err := tree.CreateSnapshot()
			require.NoError(t, err)
			res, err := snap.AddLeaves(randLeaves(rng, b))
			require.NoError(t, err)
			require.Equal(t, 1, res.Stats.SortCalls, "%s b=%d: one comparison sort", mode, b)
			require.Equal(t, b, res.Stats.SortedElements, "%s b=%d", mode, b)
			if mode == MaterializeFrontier {
				// the tree is several waves deep: before, each wave sorted its loads
				require.GreaterOrEqual(t, res.Stats.WaveOrderings, 3, "b=%d", b)
			}
			requireOverlayConsistent(t, snap, store, fmt.Sprint(mode, b))
		}
		// several calls per block: one sort per call, never one per wave
		tree, err := OpenWithOptions(store, opts)
		require.NoError(t, err)
		snap, err := tree.CreateSnapshot()
		require.NoError(t, err)
		const calls, per = 6, 32
		var sorts, elems int
		for c := 0; c < calls; c++ {
			res, err := snap.AddLeaves(randLeaves(rng, per))
			require.NoError(t, err)
			sorts += res.Stats.SortCalls
			elems += res.Stats.SortedElements
		}
		require.Equal(t, calls, sorts, mode)
		require.Equal(t, calls*per, elems, mode)
	}
}

// The ordering of a frontier wave: waves in tree order (as the materialization generates them) are ordered
// by node key with no comparison sort and the same result as a comparison sort; disorder which breaks the
// assumption is still handled (and counted).
func TestOrderFrontierLoads(t *testing.T) {
	mk := func(depth int, b0, b1 byte) frontierLoadReq {
		var p disk.PrefixBits
		p[0], p[1] = b0, b1
		k, err := disk.NewNodeKey(depth, p)
		require.NoError(t, err)
		return frontierLoadReq{req: frontierReq{nodeKey: k}}
	}
	sortedRef := func(in []frontierLoadReq) []frontierLoadReq {
		out := append([]frontierLoadReq(nil), in...)
		sort.SliceStable(out, func(i, j int) bool { return nodeKeyLess(out[i].req.nodeKey, out[j].req.nodeKey) })
		return out
	}
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 200; iter++ {
		// a wave in tree order: nodes at various depths, increasing prefixes among equal depths
		var wave []frontierLoadReq
		prefix := 0
		for i := 0; i < 1+rng.Intn(60); i++ {
			prefix += 1 + rng.Intn(500)
			depth := []int{3, 5, 8, 9, 12, 16}[rng.Intn(6)]
			p := prefix >> uint(16-depth) << uint(16-depth) // an absolute prefix with only depth bits
			wave = append(wave, mk(depth, byte(p>>8), byte(p)))
		}
		// the generator above can produce equal-depth nodes out of prefix order after masking: keep a valid tree-order wave
		sort.SliceStable(wave, func(i, j int) bool {
			a, b := wave[i].req.nodeKey.Prefix(), wave[j].req.nodeKey.Prefix()
			return string(a[:2]) < string(b[:2]) || (string(a[:2]) == string(b[:2]) && wave[i].req.nodeKey.DepthBits() < wave[j].req.nodeKey.DepthBits())
		})
		var st disk.ApplyStats
		got := orderFrontierLoads(append([]frontierLoadReq(nil), wave...), &st)
		require.Equal(t, sortedRef(wave), got)
		require.Zero(t, st.SortCalls, "no comparison sort for a wave in tree order")
		require.Equal(t, 1, st.WaveOrderings)
	}
	// adversarial disorder inside one depth: still sorted, and the comparison sort is counted
	bad := []frontierLoadReq{mk(8, 0x80, 0), mk(8, 0x40, 0), mk(8, 0x20, 0), mk(4, 0x10, 0)}
	var st disk.ApplyStats
	got := orderFrontierLoads(append([]frontierLoadReq(nil), bad...), &st)
	require.Equal(t, sortedRef(bad), got)
	require.Equal(t, 1, st.SortCalls)
	require.Equal(t, len(bad), st.SortedElements)
}

// The ordered update is the plain update: results classified identically (accepted, duplicate, invalid, in
// input order) and the same root as applying the inputs one at a time.
func TestOrderedUpdateMatchesSequentialApplication(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		rng := rand.New(rand.NewSource(seed))
		var in []disk.LeafInput
		for i := 0; i < 5+rng.Intn(80); i++ {
			switch rng.Intn(8) {
			case 0:
				in = append(in, disk.LeafInput{Key: make([]byte, 5), Value: []byte("bad key")})
			case 1:
				if len(in) > 0 {
					in = append(in, in[rng.Intn(len(in))]) // duplicate
					continue
				}
				fallthrough
			default:
				in = append(in, randLeaf(rng))
			}
		}
		batch := disk.NewTree()
		got, err := batch.ApplyLeaves(in)
		require.NoError(t, err)
		seq := disk.NewTree()
		var accepted, dup, rejected []int
		for i, l := range in {
			r, err := seq.ApplyLeaves([]disk.LeafInput{l})
			require.NoError(t, err)
			accepted = append(accepted, offset(r.AcceptedIndexes, i)...)
			dup = append(dup, offset(r.DuplicateIndexes, i)...)
			for _, rj := range r.Rejected {
				rejected = append(rejected, rj.Index+i)
			}
		}
		require.Equal(t, seq.RootHash(), batch.RootHash(), "seed %d", seed)
		require.ElementsMatch(t, accepted, got.AcceptedIndexes, "seed %d", seed)
		require.ElementsMatch(t, dup, got.DuplicateIndexes, "seed %d", seed)
		var gotRejected []int
		for _, rj := range got.Rejected {
			gotRejected = append(gotRejected, rj.Index)
		}
		require.ElementsMatch(t, rejected, gotRejected, "seed %d", seed)
		require.Equal(t, 1, got.Stats.SortCalls)
		// a second batch colliding with existing leaves (the leaf-merge path) gives the same root as sequential
		more := randLeaves(rng, 30)
		more = append(more, in[:min(len(in), 5)]...)
		_, err = batch.ApplyLeaves(more)
		require.NoError(t, err)
		for _, l := range more {
			_, err := seq.ApplyLeaves([]disk.LeafInput{l})
			require.NoError(t, err)
		}
		require.Equal(t, seq.RootHash(), batch.RootHash(), "seed %d after the second batch", seed)
	}
}

func offset(idx []int, by int) []int {
	out := make([]int, len(idx))
	for i, v := range idx {
		out[i] = v + by
	}
	return out
}
