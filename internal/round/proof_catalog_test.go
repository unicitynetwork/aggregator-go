package round

import (
	"context"
	"crypto/sha256"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// countingView counts how often the catalog asks a retained view for its charge (the hardware independent
// cost of the accounting) and forwards everything else, including leaving the accounting.
type countingView struct {
	smtbackend.ReadView
	reads *atomic.Int64
}

func (c *countingView) RetainedBytes() uint64 {
	c.reads.Add(1)
	return c.ReadView.RetainedBytes()
}

func (c *countingView) RetireAccounting() {
	if r, ok := c.ReadView.(smtbackend.AccountingRetirer); ok {
		r.RetireAccounting()
	}
}

func newMemoryBackend() *smtbackend.MemoryBackend {
	return smtbackend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
}

func ucOf(t testing.TB, root []byte, round uint64) []byte {
	uc := types.UnicityCertificate{
		Version:     1,
		InputRecord: &types.InputRecord{Version: 1, RoundNumber: round, Hash: root, SummaryValue: []byte{}},
		UnicitySeal: &types.UnicitySeal{Version: 1, RootChainRoundNumber: round},
	}
	b, err := types.Cbor.Marshal(&uc)
	require.NoError(t, err)
	return b
}

// commitLeaf commits one more leaf to the backend and returns the new root.
func commitLeaf(t testing.TB, be smtbackend.Backend, n int) []byte {
	ctx := context.Background()
	snap, err := be.CreateSnapshot(ctx)
	require.NoError(t, err)
	key := make([]byte, api.StateTreeKeyLengthBytes)
	key[0], key[1], key[2] = byte(n*37), byte(n), byte(n>>8)
	res, err := snap.AddLeavesClassified(ctx, []smtbackend.LeafInput{{Key: key, Value: make([]byte, 32)}})
	require.NoError(t, err)
	require.NoError(t, snap.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: api.NewBigIntFromUint64(uint64(n)), RootHash: res.CandidateRoot}))
	return res.CandidateRoot
}

// The production constructor wires the backend's O(1) retained-byte total into the catalog: the
// accounting reads per catalog operation do not grow with the number of retained views (they do when the
// accountant is omitted, which is what production did before). Counted through the real constructor.
func TestProductionCatalogConstructorWiresTheAccountant(t *testing.T) {
	ctx := context.Background()
	run := func(v int, production bool) float64 {
		be := newMemoryBackend()
		limits := proofview.DefaultLimits()
		limits.MaxViews = v
		var catalog *proofview.Catalog
		var err error
		if production {
			catalog, err = newProofViewCatalog(limits, be)
			require.True(t, catalog.UsesAccountant(), "the memory backend is an accountant")
		} else {
			catalog, err = proofview.NewCatalog(limits) // the previous production constructor
		}
		require.NoError(t, err)
		var reads atomic.Int64
		publish := func(n int) {
			root := commitLeaf(t, be, n)
			rv, err := be.RetainReadView(ctx)
			require.NoError(t, err)
			require.NoError(t, catalog.Publish(ctx, proofview.PublishInput{
				Block: proofview.BlockIdentity{Number: api.NewBigIntFromUint64(uint64(n)), ConfigLineage: []byte("cfg")},
				Root:  root, Reader: &countingView{ReadView: rv, reads: &reads}, Cert: proofview.Certification{UC: ucOf(t, root, uint64(n))},
			}))
		}
		for n := 1; n <= v; n++ {
			publish(n)
		}
		before := reads.Load()
		const ops = 12
		for k := 0; k < ops; k++ {
			publish(v + 1 + k) // admission with one eviction
			l, err := catalog.AcquireLatest(ctx)
			require.NoError(t, err)
			l.Release()
			_ = catalog.Stats()
		}
		return float64(reads.Load()-before) / ops
	}
	var wired, unwired []float64
	vs := []int{8, 16, 32, 64}
	for _, v := range vs {
		wired = append(wired, run(v, true))
		unwired = append(unwired, run(v, false))
	}
	for _, w := range wired {
		require.LessOrEqual(t, w, 12.0, "production constructor: accounting reads per operation batch must not depend on V: %v", wired)
	}
	require.InDelta(t, wired[0], wired[len(wired)-1], 1.0, "flat in V: %v", wired)
	// without the accountant the same operations scale with V (so this test would catch a regression)
	require.Greater(t, unwired[len(unwired)-1], 4*unwired[0], "fallback scales with V: %v", unwired)
	require.Greater(t, unwired[len(unwired)-1], 10*wired[len(wired)-1], "wired %v unwired %v", wired, unwired)
}

// A backend which cannot total its views gets the (correct) linear fallback instead of failing.
func TestProductionCatalogConstructorWithoutAccountantBackend(t *testing.T) {
	c, err := newProofViewCatalog(proofview.DefaultLimits(), nil)
	require.NoError(t, err)
	require.False(t, c.UsesAccountant())
	_ = sha256.Size
}
