package smt

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/nifixture"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func randomLeaves(rng *rand.Rand, n int) []nifixture.Leaf {
	var leaves []nifixture.Leaf
	seen := map[[32]byte]bool{}
	for len(leaves) < n {
		var l nifixture.Leaf
		rng.Read(l.Key[:])
		switch rng.Intn(4) {
		case 0: // dense prefix sharing: differ from an existing key in one bit
			if len(leaves) > 0 {
				l.Key = leaves[rng.Intn(len(leaves))].Key
				b := rng.Intn(256)
				l.Key[b/8] ^= 0x80 >> uint(b%8)
			}
		case 1: // the extreme depths
			if len(leaves) > 0 {
				l.Key = leaves[rng.Intn(len(leaves))].Key
				l.Key[31] ^= 1
			}
		}
		rng.Read(l.Value[:])
		if !seen[l.Key] {
			seen[l.Key] = true
			leaves = append(leaves, l)
		}
	}
	return leaves
}

func buildMemoryTree(t testing.TB, rng *rand.Rand, leaves []nifixture.Leaf) *SparseMerkleTree {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	order := rng.Perm(len(leaves))
	for _, i := range order {
		path, err := api.FixedBytesToPath(leaves[i].Key[:], api.StateTreeKeyLengthBits)
		require.NoError(t, err)
		require.NoError(t, tree.AddLeaf(path, leaves[i].Value[:]))
	}
	return tree
}

// The memory tree's non-inclusion certificate equals, byte for byte, the independent oracle's, for absent
// queries; for present ones it reports the state as included; the root equals the oracle root.
func TestMemoryNonInclusionCertEqualsTheOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for iter := 0; iter < 250; iter++ {
		n := rng.Intn(70)
		if iter%25 == 0 {
			n = 0
		}
		leaves := randomLeaves(rng, n)
		oracle, err := nifixture.Build(leaves)
		require.NoError(t, err)
		tree := buildMemoryTree(t, rng, leaves)
		root, ok := oracle.Root()
		if ok {
			require.Equal(t, root[:], tree.GetRootHashRaw())
		}
		for k := 0; k < 12; k++ {
			var q [32]byte
			rng.Read(q[:])
			switch {
			case n > 0 && k%3 == 0:
				q = leaves[rng.Intn(n)].Key
			case n > 0 && k%3 == 1: // near an existing key
				q = leaves[rng.Intn(n)].Key
				b := rng.Intn(256)
				q[b/8] ^= 0x80 >> uint(b%8)
			}
			cert, err := tree.GetNonInclusionCert(q[:])
			if oracle.Contains(q) {
				require.ErrorIs(t, err, api.ErrStateIncluded)
				require.Nil(t, cert)
				// the shared walk: the inclusion certificate has the same junctions
				inc, err := tree.GetInclusionCert(q[:])
				require.NoError(t, err)
				want, _ := oracle.InclusionCertificate(q)
				ib, _ := inc.MarshalBinary()
				require.Equal(t, want, ib)
				continue
			}
			require.NoError(t, err)
			got, err := cert.MarshalBinary()
			require.NoError(t, err)
			require.Equal(t, oracle.Certificate(q), got)
			var rb []byte
			if ok {
				rb = root[:]
			}
			require.NoError(t, cert.VerifyAgainstTrustedRoot(q[:], rb, api.SHA256))
		}
	}
}

func TestMemoryNonInclusionBoundaryDepths(t *testing.T) {
	for _, depth := range []int{0, 1, 7, 8, 254, 255} {
		var a, b [32]byte
		a[0] = 0x55
		b = a
		b[depth/8] ^= 0x80 >> uint(depth%8)
		leaves := []nifixture.Leaf{{Key: a, Value: [32]byte{1}}, {Key: b, Value: [32]byte{2}}}
		tree := buildMemoryTree(t, rand.New(rand.NewSource(1)), leaves)
		oracle, _ := nifixture.Build(leaves)
		q := a
		q[(depth+1)%256/8] ^= 0x80 >> uint((depth+1)%256%8) // leaves the pair at the next bit, if any
		if oracle.Contains(q) {
			continue
		}
		cert, err := tree.GetNonInclusionCert(q[:])
		require.NoError(t, err, depth)
		got, _ := cert.MarshalBinary()
		require.Equal(t, oracle.Certificate(q), got, depth)
	}
}

// A frozen view keeps serving its root: later insertions (including of the queried key) change the
// tree but not the view.
func TestReadViewNonInclusionIsImmutable(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	leaves := randomLeaves(rng, 20)
	ts := NewThreadSafeSMT(NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))
	for _, l := range leaves {
		path, _ := api.FixedBytesToPath(l.Key[:], api.StateTreeKeyLengthBits)
		require.NoError(t, ts.AddLeaf(path, l.Value[:]))
	}
	view := ts.FreezeReadView(0)
	var q [32]byte
	rng.Read(q[:])
	before, err := view.GetNonInclusionCert(q[:])
	require.NoError(t, err)
	bb, _ := before.MarshalBinary()

	path, _ := api.FixedBytesToPath(q[:], api.StateTreeKeyLengthBits)
	require.NoError(t, ts.AddLeaf(path, bytes.Repeat([]byte{9}, 32)))
	after, err := view.GetNonInclusionCert(q[:])
	require.NoError(t, err)
	ab, _ := after.MarshalBinary()
	require.Equal(t, bb, ab, "the frozen root is unchanged by later writes")
	// the live tree now reports the state as included, the view still proves absence
	live := ts.FreezeReadView(view.Seq())
	_, err = live.GetNonInclusionCert(q[:])
	require.ErrorIs(t, err, api.ErrStateIncluded)
	view.Release()
	_, err = view.GetNonInclusionCert(q[:])
	require.ErrorIs(t, err, ErrReadViewReleased)
}

func TestMemoryNonInclusionRejectsParentTreesAndBadKeys(t *testing.T) {
	tree := NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	_, err := tree.GetNonInclusionCert(make([]byte, 31))
	require.ErrorIs(t, err, api.ErrCertKeyLength)
	empty, err := tree.GetNonInclusionCert(make([]byte, 32))
	require.NoError(t, err)
	require.True(t, empty.IsEmptyTree())
}
