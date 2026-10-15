//go:build rocksdb

package round

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
)

func newDiskBackend(t testing.TB) smtbackend.Backend {
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	be, err := smtbackend.NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	return be
}

// The same production path with the RocksDB backend: initProofViews installs the accountant, the accounting
// questions per batch do not depend on the number of retained views, and the maintained total equals the slow
// per-reader summation after every operation.
func TestInitProofViewsWiresTheAccountantRocksDBBackend(t *testing.T) {
	var wired []float64
	for _, v := range []int{4, 8, 16, 32} {
		var reads, totals atomic.Int64
		be := &accountingBackend{Backend: newDiskBackend(t), reads: &reads, totals: &totals}
		rm := proofRoundManager(t, be, v)
		require.True(t, rm.ProofViewCatalog().UsesAccountant(), "initProofViews must install the accountant for the RocksDB backend")
		wired = append(wired, runCatalogScenario(t, rm, be, be.queries, v))
		require.Equal(t, rm.ProofViewCatalog().RetainedBytesBySummation(), rm.ProofViewCatalog().Stats().RetainedBytes)
	}
	for _, w := range wired {
		require.LessOrEqual(t, w, 40.0, "accounting questions per batch must not depend on V: %v", wired)
	}
	require.InDelta(t, wired[0], wired[len(wired)-1], 3.0, "flat in V: %v", wired)
}
