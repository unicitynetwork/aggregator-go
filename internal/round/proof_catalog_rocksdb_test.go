//go:build rocksdb

package round

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
)

// The RocksDB backend is wired as the accountant by the production constructor as well.
func TestProductionCatalogConstructorWiresTheAccountantForRocksDB(t *testing.T) {
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	be, err := smtbackend.NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	defer be.Close()
	c, err := newProofViewCatalog(proofview.DefaultLimits(), be)
	require.NoError(t, err)
	require.True(t, c.UsesAccountant(), "the RocksDB backend is an accountant")
}
