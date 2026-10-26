package ha

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/bft-go-base/types"
)

type transientRetainBackend struct {
	*fakeDiskBackend
	fail       bool
	memory     bool
	cancel     context.CancelFunc
	calls      int
	stateReads int
}

func (b *transientRetainBackend) IsDiskBackedSMT() bool { return !b.memory }

func (b *transientRetainBackend) CommittedState(ctx context.Context) (backend.CommittedState, error) {
	b.stateReads++
	return b.MemoryBackend.CommittedState(ctx)
}

func (b *transientRetainBackend) RetainReadView(ctx context.Context) (backend.ReadView, error) {
	b.calls++
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}
	if b.fail {
		return nil, errors.New("temporary read-snapshot allocation failure")
	}
	return b.MemoryBackend.RetainReadView(ctx)
}

func TestBlockSyncer_RetriesUnavailableCurrentView(t *testing.T) {
	for name, memory := range map[string]bool{"disk": false, "memory": true} {
		t.Run(name, func(t *testing.T) { testBlockSyncerRetriesUnavailableCurrentView(t, memory) })
	}
}

func testBlockSyncerRetriesUnavailableCurrentView(t *testing.T, memory bool) {
	ctx := t.Context()
	f := newBlockSyncerFixture(t)
	root := f.addBlock(t, 1, 2)
	block := f.storage.blocks.byNumber["1"]
	auth := prooftest.New(t, 2, 1, root, 81)
	uc, err := types.Cbor.Marshal(auth.UC)
	require.NoError(t, err)
	block.UnicityCertificate = api.HexBytes(uc)
	be := &transientRetainBackend{fakeDiskBackend: newFakeDiskBackend(), fail: true, memory: memory}
	limits := proofview.DefaultLimits()
	limits.MaxConcurrentPins = 1
	cat, err := proofview.NewCatalog(limits)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cat.Close()) })
	pub := proofpub.New(cat, be, nil, testBlockSyncerLogger(t))
	s := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, f.storage, be, 0, time.Second, state.NewSyncStateTracker())
	s.SetViewPublisher(pub)
	wakes := 0
	s.SetNotify(func() { wakes++ })
	for i := 1; i <= 2; i++ {
		require.NoError(t, s.SyncToLatestBlock(ctx))
		_, err = cat.AcquireLatest(ctx)
		require.ErrorIs(t, err, proofview.ErrViewNotRetained)
		require.Equal(t, i, be.calls)
		require.Equal(t, 1, wakes, "unchanged publication failure does not wake waiters again")
	}
	be.fail = false
	require.NoError(t, s.SyncToLatestBlock(ctx))
	require.Equal(t, 3, be.calls)
	require.Equal(t, 2, wakes, "recovery wakes held proof reads")
	l, err := cat.AcquireLatest(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, l.Block().Number.Uint64())
	for i := 0; i < 3; i++ {
		reads := be.stateReads
		require.NoError(t, s.SyncToLatestBlock(ctx))
		if memory {
			require.Equal(t, reads, be.stateReads, "memory retries check the root without reading disk state")
		} else {
			require.Equal(t, reads+2, be.stateReads, "one cursor read and one committed-state check")
		}
	}
	require.Equal(t, 3, be.calls, "a healthy view is not retained again, even with all pins occupied")
	require.Equal(t, 2, wakes)
	l.Release()

	root2 := f.addBlock(t, 2, 0)
	block2 := f.storage.blocks.byNumber["2"]
	auth2 := prooftest.New(t, 2, 2, root2, 82)
	uc2, err := types.Cbor.Marshal(auth2.UC)
	require.NoError(t, err)
	block2.UnicityCertificate = api.HexBytes(uc2)
	be.fail = true
	require.NoError(t, s.SyncToLatestBlock(ctx))
	_, err = cat.AcquireLatest(ctx)
	require.ErrorIs(t, err, proofview.ErrViewNotRetained, "never fall back to the previous block")
	be.fail = false
	require.NoError(t, s.SyncToLatestBlock(ctx))
	l, err = cat.AcquireLatest(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, l.Block().Number.Uint64())
	l.Release()
	require.Equal(t, 5, be.calls)
	require.Equal(t, 4, wakes)
}

func TestBlockSyncer_MemoryPublicationRecoversAfterCancellation(t *testing.T) {
	f := newBlockSyncerFixture(t)
	root := f.addBlock(t, 1, 2)
	block := f.storage.blocks.byNumber["1"]
	auth := prooftest.New(t, 2, 1, root, 81)
	uc, err := types.Cbor.Marshal(auth.UC)
	require.NoError(t, err)
	block.UnicityCertificate = api.HexBytes(uc)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	be := &transientRetainBackend{fakeDiskBackend: newFakeDiskBackend(), memory: true, cancel: cancel}
	cat, err := proofview.NewCatalog(proofview.DefaultLimits())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cat.Close()) })
	pub := proofpub.New(cat, be, nil, testBlockSyncerLogger(t))
	tracker := state.NewSyncStateTracker()
	syncer := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, f.storage, be, 0, time.Second, tracker)
	syncer.SetViewPublisher(pub)
	require.NoError(t, syncer.SyncToLatestBlock(ctx))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.EqualValues(t, 1, tracker.GetLastSyncedBlock().Uint64())
	_, err = cat.AcquireLatest(t.Context())
	require.ErrorIs(t, err, proofview.ErrViewNotRetained)
	require.NoError(t, syncer.SyncToLatestBlock(t.Context()))
	lease, err := cat.AcquireLatest(t.Context())
	require.NoError(t, err)
	defer lease.Release()
	require.EqualValues(t, 1, lease.Block().Number.Uint64())
	require.Equal(t, 2, be.calls)

	block.RootHash = api.HexBytes(prooftest.Hash("wrong root"))
	require.ErrorContains(t, syncer.SyncToLatestBlock(t.Context()), "does not match latest block root hash")
	require.Equal(t, 2, be.calls, "a mismatching committed state must not be republished")
}
