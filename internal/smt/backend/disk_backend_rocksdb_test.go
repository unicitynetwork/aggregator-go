//go:build rocksdb

package backend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestDiskBackendRocksDBRootParity(t *testing.T) {
	ctx := context.Background()
	inputs := []LeafInput{
		testLeafInput(1, 11),
		testLeafInput(2, 22),
		testLeafInput(3, 33),
	}
	expectedRoot := legacyRoot(t, inputs)

	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{
		DisableWAL:   true,
		NoSyncWrites: true,
	})
	require.NoError(t, err)
	backend, err := NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close()) }()

	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, inputs)
	require.NoError(t, err)
	require.NoError(t, result.ValidateAllAccepted(len(inputs)))
	require.Equal(t, expectedRoot, result.CandidateRoot)

	require.NoError(t, snapshot.Commit(ctx, CommitMetadata{
		BlockNumber: api.NewBigIntFromUint64(7),
		RootHash:    expectedRoot,
	}))

	actualRoot, err := backend.RootHashRaw(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedRoot, actualRoot)
}
