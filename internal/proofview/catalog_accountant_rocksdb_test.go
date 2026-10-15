//go:build rocksdb

package proofview

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
)

func TestAccountantForWiresTheDiskBackend(t *testing.T) {
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	be, err := backend.NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	defer be.Close()
	c, err := NewCatalog(DefaultLimits(), AccountantFor(be))
	require.NoError(t, err)
	require.NotNil(t, c.acc, "the RocksDB backend is an O(1) accountant")
}
