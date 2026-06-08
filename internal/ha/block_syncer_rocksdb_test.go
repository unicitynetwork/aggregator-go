//go:build rocksdb

package ha

import (
	"context"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestBlockSyncerRocksDBReplaysNonEmptyAndEmptyBlocks(t *testing.T) {
	ctx := t.Context()
	fixture := newBlockSyncerFixture(t)
	root1 := fixture.addBlock(t, 1, 2)
	root2 := fixture.addBlock(t, 2, 1)
	root3 := fixture.addBlock(t, 3, 0)
	require.Equal(t, root2.String(), root3.String())

	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{
		DisableWAL:   true,
		NoSyncWrites: true,
	})
	require.NoError(t, err)
	backend, err := smtbackend.NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close()) }()

	commitFixtureBlockToBackend(t, ctx, backend, fixture.storage, 1)
	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), state.BlockNumber.Int64())
	require.Equal(t, root1.String(), api.HexBytes(state.RootHash).String())

	syncer := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, fixture.storage, backend, 0, time.Second, nil)
	require.NoError(t, syncer.SyncToLatestBlock(ctx))

	state, err = backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), state.BlockNumber.Int64())
	require.Equal(t, root3.String(), api.HexBytes(state.RootHash).String())

	root, err := backend.RootHashRaw(ctx)
	require.NoError(t, err)
	require.Equal(t, root3.String(), api.HexBytes(root).String())
}

func TestBlockSyncerRocksDBRefreshesPublishedViewAfterReplay(t *testing.T) {
	ctx := t.Context()
	fixture := newBlockSyncerFixture(t)
	fixture.addBlock(t, 1, 1)

	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{
		DisableWAL:   true,
		NoSyncWrites: true,
	})
	require.NoError(t, err)
	backend, err := smtbackend.NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close()) }()

	wrapped := &countingProofViewBackend{Backend: backend}
	syncer := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, fixture.storage, wrapped, 0, time.Second, nil)
	require.NoError(t, syncer.SyncToLatestBlock(ctx))

	require.Equal(t, int32(1), wrapped.refreshes.Load())
}

type countingProofViewBackend struct {
	smtbackend.Backend
	refreshes atomic.Int32
}

func (b *countingProofViewBackend) IsDiskBackedSMT() bool {
	return true
}

func (b *countingProofViewBackend) RefreshPublishedProofView(ctx context.Context, expectedRoot []byte) error {
	b.refreshes.Add(1)
	return b.Backend.(smtbackend.ProofViewPublisher).RefreshPublishedProofView(ctx, expectedRoot)
}

func (b *countingProofViewBackend) CommittedState(ctx context.Context) (smtbackend.CommittedState, error) {
	state, err := b.Backend.CommittedState(ctx)
	if state.BlockNumber != nil {
		state.BlockNumber = api.NewBigInt(new(big.Int).Set(state.BlockNumber.Int))
	}
	return state, err
}

var (
	_ smtbackend.DiskBacked         = (*countingProofViewBackend)(nil)
	_ smtbackend.ProofViewPublisher = (*countingProofViewBackend)(nil)
)
