package backend

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/nifixture"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func newNIMemoryBackend() Backend {
	return NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
}

func niLeaves(rng *rand.Rand, n int) []nifixture.Leaf {
	var leaves []nifixture.Leaf
	seen := map[[32]byte]bool{}
	for len(leaves) < n {
		var l nifixture.Leaf
		rng.Read(l.Key[:])
		if len(leaves) > 0 && rng.Intn(3) > 0 {
			l.Key = leaves[rng.Intn(len(leaves))].Key
			b := rng.Intn(256)
			if rng.Intn(4) == 0 {
				b = []int{0, 1, 254, 255}[rng.Intn(4)]
			}
			l.Key[b/8] ^= 0x80 >> uint(b%8)
		}
		rng.Read(l.Value[:])
		if !seen[l.Key] {
			seen[l.Key] = true
			leaves = append(leaves, l)
		}
	}
	return leaves
}

func niCommit(t testing.TB, b Backend, block uint64, leaves []nifixture.Leaf) []byte {
	ctx := context.Background()
	snap, err := b.CreateSnapshot(ctx)
	require.NoError(t, err)
	var in []LeafInput
	for _, l := range leaves {
		in = append(in, LeafInput{Key: bytes.Clone(l.Key[:]), Value: bytes.Clone(l.Value[:])})
	}
	res, err := snap.AddLeavesClassified(ctx, in)
	require.NoError(t, err)
	require.Empty(t, res.Rejected)
	require.NoError(t, snap.Commit(ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(block), RootHash: res.CandidateRoot}))
	return res.CandidateRoot
}

func niQuery(rng *rand.Rand, leaves []nifixture.Leaf, k int) (q [32]byte) {
	rng.Read(q[:])
	switch {
	case len(leaves) > 0 && k%3 == 0:
		q = leaves[rng.Intn(len(leaves))].Key
	case len(leaves) > 0 && k%3 == 1:
		q = leaves[rng.Intn(len(leaves))].Key
		b := rng.Intn(256)
		q[b/8] ^= 0x80 >> uint(b%8)
	}
	return q
}

// RunNonInclusionDifferential checks a backend's retained views against the independent oracle on
// random trees committed in several rounds: every retained view keeps proving its own root's relation
// byte for byte after later commits, including commits which insert the queried key.
func RunNonInclusionDifferential(t *testing.T, newBackend func(t *testing.T) Backend, iterations int) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(21))
	for iter := 0; iter < iterations; iter++ {
		b := newBackend(t)
		retainer := b.(ReadViewRetainer)
		all := niLeaves(rng, 6+rng.Intn(50))
		cut := []int{0, len(all) / 3, 2 * len(all) / 3, len(all)}
		type retained struct {
			v      ReadView
			leaves []nifixture.Leaf
			root   []byte
		}
		var views []retained
		// the empty state is retained first
		ev, err := retainer.RetainReadView(ctx)
		require.NoError(t, err)
		views = append(views, retained{v: ev})
		for r := 1; r < len(cut); r++ {
			root := niCommit(t, b, uint64(r), all[cut[r-1]:cut[r]])
			v, err := retainer.RetainReadView(ctx)
			require.NoError(t, err)
			views = append(views, retained{v: v, leaves: all[:cut[r]], root: root})
		}
		defer func() {
			for _, r := range views {
				_ = r.v.Close()
			}
		}()
		// check after all commits: older views still serve their own state
		for _, r := range views {
			oracle, err := nifixture.Build(r.leaves)
			require.NoError(t, err)
			if root, ok := oracle.Root(); ok {
				require.Equal(t, root[:], r.v.RootHash())
			}
			for k := 0; k < 14; k++ {
				q := niQuery(rng, all, k)
				cert, err := NonInclusionCert(ctx, r.v, q[:])
				if oracle.Contains(q) {
					require.ErrorIs(t, err, api.ErrStateIncluded)
					require.Nil(t, cert)
					continue
				}
				require.NoError(t, err)
				got, err := cert.MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, oracle.Certificate(q), got)
				var rb []byte
				if root, ok := oracle.Root(); ok {
					rb = root[:]
				}
				require.NoError(t, cert.VerifyAgainstTrustedRoot(q[:], rb, api.SHA256))
			}
		}
	}
}

func TestMemoryBackendNonInclusionDifferential(t *testing.T) {
	RunNonInclusionDifferential(t, func(*testing.T) Backend { return newNIMemoryBackend() }, 40)
}

func TestMemoryReadViewNonInclusionLifecycle(t *testing.T) {
	ctx := context.Background()
	b := newNIMemoryBackend()
	niCommit(t, b, 1, niLeaves(rand.New(rand.NewSource(1)), 5))
	v, err := b.(ReadViewRetainer).RetainReadView(ctx)
	require.NoError(t, err)
	q := make([]byte, 32)
	q[0] = 9
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = NonInclusionCert(cancelled, v, q)
	require.ErrorIs(t, err, context.Canceled)
	_, err = NonInclusionCert(ctx, v, q[:5])
	require.ErrorIs(t, err, api.ErrCertKeyLength)
	require.NoError(t, v.Close())
	_, err = NonInclusionCert(ctx, v, q)
	require.ErrorIs(t, err, ErrReadViewClosed)
	// a view without the capability is reported as such, never as an absence
	_, err = NonInclusionCert(ctx, struct{ ReadView }{v}, q)
	require.ErrorIs(t, err, ErrNonInclusionUnsupported)
}
