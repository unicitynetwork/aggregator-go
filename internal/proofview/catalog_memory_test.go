package proofview

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// memChain drives a real MemoryBackend through the catalog.
type memChain struct {
	wrap  func(backend.ReadView) backend.ReadView // optional: wraps each retained reader (instrumentation)
	t     *testing.T
	b     *backend.MemoryBackend
	c     *Catalog
	next  uint32
	block uint64
}

func newMemChain(t *testing.T, l Limits) *memChain {
	return &memChain{t: t, c: newCatalog(t, l),
		b: backend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))}
}

// publishBlock commits n new leaves and publishes the finalized state.
func (m *memChain) publishBlock(n int) []byte {
	ctx := context.Background()
	inputs := make([]backend.LeafInput, n)
	for i := range inputs {
		key := make([]byte, api.StateTreeKeyLengthBytes)
		binary.BigEndian.PutUint32(key[:4], m.next*2654435761)
		binary.BigEndian.PutUint32(key[len(key)-4:], m.next)
		inputs[i] = backend.LeafInput{Key: key, Value: append(make([]byte, 28), byte(m.next>>24), byte(m.next>>16), byte(m.next>>8), byte(m.next))}
		m.next++
	}
	snap, err := m.b.CreateSnapshot(ctx)
	require.NoError(m.t, err)
	res, err := snap.AddLeavesClassified(ctx, inputs)
	require.NoError(m.t, err)
	m.block++
	require.NoError(m.t, snap.Commit(ctx, backend.CommitMetadata{BlockNumber: api.NewBigIntFromUint64(m.block), RootHash: res.CandidateRoot}))
	rv, err := m.b.RetainReadView(ctx)
	require.NoError(m.t, err)
	var reader backend.ReadView = rv
	if m.wrap != nil {
		reader = m.wrap(rv)
	}
	require.NoError(m.t, m.c.Publish(ctx, PublishInput{
		Block: block(m.block), Root: rv.RootHash(), Reader: reader,
		Cert: Certification{UC: ucFor(m.t, rv.RootHash(), m.block)},
	}))
	return rv.RootHash()
}

// Regression: evicting an unpinned intermediate view moves its charge onto the
// pinned older survivor only when its reader closes. Enforcement must be
// re-run after that, cancelling the over-budget survivor's lease.
func TestEvictedIntermediateChargeTransfersToPinnedSurvivorAndCancelsIt(t *testing.T) {
	l := testLimits()
	l.RetentionBudgetBytes = 800_000
	m := newMemChain(t, l)
	ctx := context.Background()

	rootA := m.publishBlock(1000)
	pinA, err := m.c.AcquireRoot(ctx, rootA)
	require.NoError(t, err)
	defer pinA.Release()
	m.publishBlock(500) // B, unpinned
	require.NoError(t, pinA.Context().Err())
	m.publishBlock(500) // C: B is evicted, its charge lands on A at close

	st := m.c.Stats()
	require.Greater(t, st.RetainedBytes, l.RetentionBudgetBytes, "A now carries B's share: over budget")
	require.Error(t, pinA.Context().Err(), "the over-budget pinned survivor must be cancelled")
	require.EqualValues(t, 1, st.PressureKills)

	// New pins stay refused until the pressure drains; releasing A drains it.
	_, err = m.c.AcquireLatest(ctx)
	require.ErrorIs(t, err, ErrBusyBudget)
	pinA.Release()
	require.LessOrEqual(t, m.c.Stats().RetainedBytes, l.RetentionBudgetBytes)
	lease, err := m.c.AcquireLatest(ctx)
	require.NoError(t, err)
	lease.Release()
}
