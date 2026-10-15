//go:build rocksdb

package persist

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Batch builds read the union of the proof paths once: node reads equal distinct nodes,
// round trips depend on the tree depth and not on L, and reads grow sublinearly in L
// (shared prefixes) compared with L independent single-proof builds.
func TestBatchProofNodeReadsAreSublinearInLeaves(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)

	const n = 4000
	inputs := make([]disk.LeafInput, 0, n)
	keys := make([][]byte, 0, n)
	values := map[disk.Key][]byte{}
	for i := 0; i < n; i++ {
		var key disk.Key
		_, err := rng.Read(key[:])
		require.NoError(t, err)
		v := make([]byte, 8)
		binary.LittleEndian.PutUint64(v, uint64(i))
		inputs = append(inputs, leafInput(key, v))
		k := key
		keys = append(keys, k[:])
		values[key] = v
	}
	snap, err := tree.CreateSnapshot()
	require.NoError(t, err)
	res, err := snap.AddLeaves(inputs)
	require.NoError(t, err)
	require.NoError(t, snap.Commit(api.NewBigIntFromUint64(1)))
	root, err := disk.HashFromBytes(res.CandidateRoot[:])
	require.NoError(t, err)

	reader, closeSnap, err := store.NewReadSnapshot()
	require.NoError(t, err)
	defer closeSnap()

	singleReads := func(ks [][]byte) (sum uint64) {
		for _, k := range ks {
			_, st, err := BuildInclusionCertsWithStats(root, reader, [][]byte{k})
			require.NoError(t, err)
			sum += st.NodeReads
		}
		return sum
	}

	prev := float64(1 << 30)
	for _, l := range []int{1, 8, 64, 512, 2048} {
		batch := keys[:l]
		certs, st, err := BuildInclusionCertsWithStats(root, reader, batch)
		require.NoError(t, err)
		require.Len(t, certs, l)
		require.Equal(t, st.NodeReads, st.UniqueNodes, "union build reads each node once (L=%d)", l)
		require.LessOrEqual(t, st.Frontiers, uint64(40), "round trips follow the depth, not L (L=%d)", l)
		perLeaf := float64(st.NodeReads) / float64(l)
		require.LessOrEqual(t, perLeaf, prev, "node reads per proven leaf must not grow with L (L=%d)", l)
		prev = perLeaf
		if l >= 8 {
			indep := singleReads(batch)
			require.Less(t, st.NodeReads, indep, "batch reads must be below L independent builds (L=%d: %d vs %d)", l, st.NodeReads, indep)
			if l >= 512 {
				require.Less(t, st.NodeReads*2, indep, "at large L the union is under half of L independent builds (L=%d: %d vs %d)", l, st.NodeReads, indep)
			}
		}
		t.Logf("L=%4d nodeReads=%5d unique=%5d roundTrips=%2d perLeaf=%.2f", l, st.NodeReads, st.UniqueNodes, st.Frontiers, perLeaf)

		// parity with the single-proof path
		for _, i := range []int{0, l / 2, l - 1} {
			single, err := BuildInclusionCert(root, reader, batch[i])
			require.NoError(t, err)
			require.Equal(t, single.Bitmap, certs[i].Bitmap)
			require.Equal(t, single.Siblings, certs[i].Siblings)
			var key disk.Key
			copy(key[:], batch[i])
			require.NoError(t, certs[i].Verify(batch[i], values[key], res.CandidateRoot[:], api.SHA256))
		}
	}

	// a shared-prefix subset (one first byte) shares even more of the path
	var shared [][]byte
	for _, k := range keys {
		if k[0] < 4 {
			shared = append(shared, k)
		}
	}
	require.Greater(t, len(shared), 40)
	_, stShared, err := BuildInclusionCertsWithStats(root, reader, shared)
	require.NoError(t, err)
	_, stSpread, err := BuildInclusionCertsWithStats(root, reader, keys[:len(shared)])
	require.NoError(t, err)
	require.Less(t, stShared.NodeReads, stSpread.NodeReads, "keys sharing prefixes read fewer nodes")
}

func TestBatchProofMissingLeafIsNotFound(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	tree := openPersistTree(t, store)
	snap, err := tree.CreateSnapshot()
	require.NoError(t, err)
	var present, absent disk.Key
	present[0], absent[0] = 0x10, 0xf0
	res, err := snap.AddLeaves([]disk.LeafInput{leafInput(present, []byte("v")), leafInput(keyWithFirstByte(0x20), []byte("w"))})
	require.NoError(t, err)
	require.NoError(t, snap.Commit(api.NewBigIntFromUint64(1)))
	root, _ := disk.HashFromBytes(res.CandidateRoot[:])
	reader, closeSnap, err := store.NewReadSnapshot()
	require.NoError(t, err)
	defer closeSnap()
	_, _, err = BuildInclusionCertsWithStats(root, reader, [][]byte{present[:], absent[:]})
	require.ErrorIs(t, err, ErrProofLeafNotFound)
}
