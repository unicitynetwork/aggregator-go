//go:build rocksdb

package round

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	rocksstore "github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestDiskSMTStartupFreshNoBlocks(t *testing.T) {
	ctx := context.Background()
	rm := newDiskStartupRoundManager(t, newTestDiskBackendForStartup(t), &diskStartupStorage{})

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Nil(t, block)
}

func TestDiskSMTStartupMatchingLatestBlock(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf := diskStartupLeaf(1, 11)
	root := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf})
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(diskStartupBlock(1, root)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), block.Uint64())
}

func TestDiskSMTStartupRootMismatchFails(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf := diskStartupLeaf(1, 11)
	root := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf})
	wrongRoot := append([]byte(nil), root...)
	wrongRoot[0] ^= 0xff
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(diskStartupBlock(1, wrongRoot)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	_, err := rm.verifyDiskSMTStartup(ctx)
	require.ErrorContains(t, err, "root mismatch")
}

func TestDiskSMTStartupEmptyDiskWithHistoryFails(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(diskStartupBlock(1, diskStartupLeaf(1, 11).Value)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	_, err := rm.verifyDiskSMTStartup(ctx)
	require.ErrorContains(t, err, "disk SMT is empty")
}

func TestDiskSMTStartupEmptyDiskWithHistoryHAStartsAsStaleFollower(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(diskStartupBlock(1, diskStartupLeaf(1, 11).Value)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)
	rm.config.HA.Enabled = true

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Nil(t, block)

	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Nil(t, state.BlockNumber, "stale follower leaves disk empty for BlockSyncer to catch up")
}

func TestDiskSMTStartEmptyDiskHAFollowerSkipsRecovery(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	// Finalized history exists, plus an in-flight unfinalized block, while the
	// local RocksDB is empty. A stale follower must not try to recover the
	// unfinalized block onto an empty tree; startup succeeds and defers to sync.
	finalized := diskStartupBlock(1, diskStartupLeaf(1, 11).Value)
	unfinalized := diskStartupBlock(2, diskStartupLeaf(2, 22).Value)
	unfinalized.Finalized = false
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(finalized, unfinalized),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)
	rm.config.HA.Enabled = true
	rm.commitmentQueue = &diskStartupCommitmentQueue{}

	require.NoError(t, rm.Start(ctx))

	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Nil(t, state.BlockNumber, "stale follower must leave disk empty after startup")
}

func TestDiskSMTStartupPaginatedReplay(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf1 := diskStartupLeaf(1, 11)
	root1 := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf1})

	leaf2 := diskStartupLeaf(2, 22)
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, []smtbackend.LeafInput{leaf2})
	require.NoError(t, err)
	root2 := result.CandidateRoot
	snapshot.Discard(ctx)

	block2 := diskStartupBlock(2, root2)
	storage := &diskStartupStorage{
		blocks:     newDiskStartupBlockStorage(diskStartupBlock(1, root1), block2),
		smt:        newDiskStartupSMTStorage(models.NewSmtNode(leaf2.Key, leaf2.Value)),
		aggregator: newDiskStartupAggregatorRecordStorage(diskStartupAggregatorRecord(block2, leaf2, 22, 0)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.Uint64())

	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.BlockNumber.Uint64())
	require.Equal(t, root2, state.RootHash)
}

func TestDiskSMTStartupPaginatedReplayIgnoresLatestAdvanceDuringReplay(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf1 := diskStartupLeaf(1, 11)
	root1 := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf1})

	leaf2 := diskStartupLeaf(2, 22)
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, []smtbackend.LeafInput{leaf2})
	require.NoError(t, err)
	root2 := result.CandidateRoot
	snapshot.Discard(ctx)

	leaf3 := diskStartupLeaf(3, 33)
	snapshot, err = backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err = snapshot.AddLeavesClassified(ctx, []smtbackend.LeafInput{leaf2, leaf3})
	require.NoError(t, err)
	root3 := result.CandidateRoot
	snapshot.Discard(ctx)

	block2 := diskStartupBlock(2, root2)
	block3 := diskStartupBlock(3, root3)
	blockStorage := newDiskStartupBlockStorage(diskStartupBlock(1, root1), block2)
	blockStorage.blocks[block3.Index.String()] = block3
	blockStorage.afterGetFinalizedPage = func() {
		blockStorage.latest = block3
	}
	storage := &diskStartupStorage{
		blocks:     blockStorage,
		smt:        newDiskStartupSMTStorage(models.NewSmtNode(leaf2.Key, leaf2.Value)),
		aggregator: newDiskStartupAggregatorRecordStorage(diskStartupAggregatorRecord(block2, leaf2, 22, 0)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.Uint64())

	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.BlockNumber.Uint64())
	require.Equal(t, root2, state.RootHash)
	require.Equal(t, block3, blockStorage.latest, "test setup must advance latest while replay is in progress")
}

func TestDiskSMTStartupPaginatedReplaySkipsMissingRepeatUCRounds(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf1 := diskStartupLeaf(1, 11)
	root1 := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf1})

	leaf3 := diskStartupLeaf(3, 33)
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, []smtbackend.LeafInput{leaf3})
	require.NoError(t, err)
	root3 := result.CandidateRoot
	snapshot.Discard(ctx)

	block3 := diskStartupBlock(3, root3)
	storage := &diskStartupStorage{
		blocks:     newDiskStartupBlockStorage(diskStartupBlock(1, root1), block3),
		smt:        newDiskStartupSMTStorage(models.NewSmtNode(leaf3.Key, leaf3.Value)),
		aggregator: newDiskStartupAggregatorRecordStorage(diskStartupAggregatorRecord(block3, leaf3, 33, 0)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), block.Uint64())

	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), state.BlockNumber.Uint64())
	require.Equal(t, root3, state.RootHash)
}

func TestDiskSMTStartupPaginatedReplayBeyondFormerLimit(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	root := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{diskStartupLeaf(1, 11)})

	blocks := make([]*models.Block, 0, diskSMTStartupReplayPageSize+2)
	blocks = append(blocks, diskStartupBlock(1, root))
	for i := 2; i <= diskSMTStartupReplayPageSize+2; i++ {
		blocks = append(blocks, diskStartupBlock(uint64(i), root))
	}
	blockStorage := newDiskStartupBlockStorage(blocks...)
	storage := &diskStartupStorage{
		blocks: blockStorage,
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(diskSMTStartupReplayPageSize+2), block.Uint64())
	require.Equal(t, 2, blockStorage.finalizedPageCalls)

	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(diskSMTStartupReplayPageSize+2), state.BlockNumber.Uint64())
	require.Equal(t, root, state.RootHash)
}

func TestDiskSMTStartupPaginatedReplayResumesAfterPageFetchFailure(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	root := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{diskStartupLeaf(1, 11)})

	blocks := make([]*models.Block, 0, diskSMTStartupReplayPageSize+2)
	blocks = append(blocks, diskStartupBlock(1, root))
	for i := 2; i <= diskSMTStartupReplayPageSize+2; i++ {
		blocks = append(blocks, diskStartupBlock(uint64(i), root))
	}
	blockStorage := newDiskStartupBlockStorage(blocks...)
	blockStorage.finalizedPageErrorOnCall = 2
	rm := newDiskStartupRoundManager(t, backend, &diskStartupStorage{blocks: blockStorage})

	_, err := rm.verifyDiskSMTStartup(ctx)
	require.ErrorContains(t, err, "injected finalized page failure")

	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(diskSMTStartupReplayPageSize+1), state.BlockNumber.Uint64())

	blockStorage.finalizedPageErrorOnCall = 0
	blockStorage.finalizedPageCalls = 0
	blockStorage.finalizedPageAfter = nil

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(diskSMTStartupReplayPageSize+2), block.Uint64())
	require.Equal(t, []string{strconv.Itoa(diskSMTStartupReplayPageSize + 1)}, blockStorage.finalizedPageAfter)
}

func TestDiskSMTStartupPaginatedReplayCancellationResumesWithoutFailureMetric(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	backend := newTestDiskBackendForStartup(t)
	root := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{diskStartupLeaf(1, 11)})

	blocks := make([]*models.Block, 0, diskSMTStartupReplayPageSize+2)
	blocks = append(blocks, diskStartupBlock(1, root))
	for i := 2; i <= diskSMTStartupReplayPageSize+2; i++ {
		blocks = append(blocks, diskStartupBlock(uint64(i), root))
	}
	blockStorage := newDiskStartupBlockStorage(blocks...)
	blockStorage.afterGetFinalizedPage = func() {
		if blockStorage.finalizedPageCalls == 2 {
			cancel()
		}
	}
	rm := newDiskStartupRoundManager(t, backend, &diskStartupStorage{blocks: blockStorage})
	failuresBefore := diskSMTStartupFailureMetric(t)

	_, err := rm.verifyDiskSMTStartup(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, failuresBefore, diskSMTStartupFailureMetric(t))

	state, err := backend.CommittedState(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(diskSMTStartupReplayPageSize+1), state.BlockNumber.Uint64())

	blockStorage.afterGetFinalizedPage = nil
	blockStorage.finalizedPageCalls = 0
	blockStorage.finalizedPageAfter = nil

	block, err := rm.verifyDiskSMTStartup(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(diskSMTStartupReplayPageSize+2), block.Uint64())
	require.Equal(t, []string{strconv.Itoa(diskSMTStartupReplayPageSize + 1)}, blockStorage.finalizedPageAfter)
}

func TestDiskSMTStartupPaginatedReplayRejectsBadRootBeforeCommit(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	root := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{diskStartupLeaf(1, 11)})
	wrongRoot := append([]byte(nil), root...)
	wrongRoot[0] ^= 0xff
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(
			diskStartupBlock(1, root),
			diskStartupBlock(2, wrongRoot),
		),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	_, err := rm.verifyDiskSMTStartup(ctx)
	require.ErrorContains(t, err, "replay root mismatch at block 2")

	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.BlockNumber.Uint64())
	require.Equal(t, root, state.RootHash)
}

func TestDiskSMTStartupDiskAheadFails(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	root := commitDiskStartupLeaves(t, ctx, backend, 2, []smtbackend.LeafInput{diskStartupLeaf(1, 11)})
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(diskStartupBlock(1, root)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	_, err := rm.verifyDiskSMTStartup(ctx)
	require.ErrorContains(t, err, "ahead of latest finalized block")
}

func TestDiskSMTStartupAllowsDiskAtUnfinalizedNextBlock(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf1 := diskStartupLeaf(1, 11)
	root1 := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf1})
	leaf2 := diskStartupLeaf(2, 22)
	root2 := commitDiskStartupLeaves(t, ctx, backend, 2, []smtbackend.LeafInput{leaf2})

	unfinalized := diskStartupBlock(2, root2)
	unfinalized.Finalized = false
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(diskStartupBlock(1, root1), unfinalized),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	block, err := rm.verifyDiskSMTStartup(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), block.Uint64())
}

func TestDiskSMTStartReplaysFinalizedBeforeRecoveringUnfinalizedBlock(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf1 := diskStartupLeaf(1, 11)
	root1 := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf1})
	leaf2 := diskStartupLeaf(2, 22)
	leaf3 := diskStartupLeaf(3, 33)

	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, []smtbackend.LeafInput{leaf2})
	require.NoError(t, err)
	root2 := result.CandidateRoot
	snapshot.Discard(ctx)

	snapshot, err = backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err = snapshot.AddLeavesClassified(ctx, []smtbackend.LeafInput{leaf2, leaf3})
	require.NoError(t, err)
	root3 := result.CandidateRoot
	snapshot.Discard(ctx)

	block3 := diskStartupBlock(3, root3)
	block3.Finalized = false
	block2 := diskStartupBlock(2, root2)
	storage := &diskStartupStorage{
		blocks: newDiskStartupBlockStorage(
			diskStartupBlock(1, root1),
			block2,
			block3,
		),
		smt:        newDiskStartupSMTStorage(models.NewSmtNode(leaf2.Key, leaf2.Value), models.NewSmtNode(leaf3.Key, leaf3.Value)),
		aggregator: newDiskStartupAggregatorRecordStorage(diskStartupAggregatorRecord(block2, leaf2, 22, 0), diskStartupAggregatorRecord(block3, leaf3, 33, 0)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)
	rm.commitmentQueue = &diskStartupCommitmentQueue{}

	require.NoError(t, rm.Start(ctx))
	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), state.BlockNumber.Uint64())
	require.Equal(t, root3, state.RootHash)
	latest, err := storage.BlockStorage().GetLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), latest.Index.Uint64())
	require.Equal(t, int64(3), rm.stateTracker.GetLastSyncedBlock().Int64())
}

func TestDiskSMTStartFinalizesAlreadyCommittedUnfinalizedBlock(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf1 := diskStartupLeaf(1, 11)
	root1 := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf1})
	leaf2 := diskStartupLeaf(2, 22)
	root2 := commitDiskStartupLeaves(t, ctx, backend, 2, []smtbackend.LeafInput{leaf2})

	block2 := diskStartupBlock(2, root2)
	block2.Finalized = false
	storage := &diskStartupStorage{
		blocks:     newDiskStartupBlockStorage(diskStartupBlock(1, root1), block2),
		smt:        newDiskStartupSMTStorage(models.NewSmtNode(leaf2.Key, leaf2.Value)),
		aggregator: newDiskStartupAggregatorRecordStorage(diskStartupAggregatorRecord(block2, leaf2, 22, 0)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)
	rm.commitmentQueue = &diskStartupCommitmentQueue{}

	require.NoError(t, rm.Start(ctx))
	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.BlockNumber.Uint64())
	require.Equal(t, root2, state.RootHash)
	finalized, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigIntFromUint64(2))
	require.NoError(t, err)
	require.NotNil(t, finalized)
}

func TestDiskSMTStartFinalizesAlreadyCommittedFirstBlock(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf := diskStartupLeaf(1, 11)
	root := commitDiskStartupLeaves(t, ctx, backend, 1, []smtbackend.LeafInput{leaf})

	block := diskStartupBlock(1, root)
	block.Finalized = false
	storage := &diskStartupStorage{
		blocks:     newDiskStartupBlockStorage(block),
		smt:        newDiskStartupSMTStorage(models.NewSmtNode(leaf.Key, leaf.Value)),
		aggregator: newDiskStartupAggregatorRecordStorage(diskStartupAggregatorRecord(block, leaf, 11, 0)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)
	rm.commitmentQueue = &diskStartupCommitmentQueue{}

	require.NoError(t, rm.Start(ctx))
	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.BlockNumber.Uint64())
	require.Equal(t, root, state.RootHash)
	finalized, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigIntFromUint64(1))
	require.NoError(t, err)
	require.NotNil(t, finalized)
	require.Equal(t, int64(1), rm.stateTracker.GetLastSyncedBlock().Int64())
}

func TestDiskSMTStartAppliesUnfinalizedFirstBlockFromEmptyDisk(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf := diskStartupLeaf(1, 11)

	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, []smtbackend.LeafInput{leaf})
	require.NoError(t, err)
	root := result.CandidateRoot
	snapshot.Discard(ctx)

	block := diskStartupBlock(1, root)
	block.Finalized = false
	storage := &diskStartupStorage{
		blocks:     newDiskStartupBlockStorage(block),
		smt:        newDiskStartupSMTStorage(models.NewSmtNode(leaf.Key, leaf.Value)),
		aggregator: newDiskStartupAggregatorRecordStorage(diskStartupAggregatorRecord(block, leaf, 11, 0)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)
	rm.commitmentQueue = &diskStartupCommitmentQueue{}

	require.NoError(t, rm.Start(ctx))
	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.BlockNumber.Uint64())
	require.Equal(t, root, state.RootHash)
	finalized, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigIntFromUint64(1))
	require.NoError(t, err)
	require.NotNil(t, finalized)
	require.Equal(t, int64(1), rm.stateTracker.GetLastSyncedBlock().Int64())
}

func TestLoadRecoveredNodesIntoBackendRejectsRecoveredRootMismatch(t *testing.T) {
	ctx := context.Background()
	backend := newTestDiskBackendForStartup(t)
	leaf := diskStartupLeaf(1, 11)
	wrongRoot := append([]byte(nil), leaf.Value...)
	wrongRoot[0] ^= 0xff
	blockNumber := api.NewBigIntFromUint64(1)
	block := diskStartupBlock(1, wrongRoot)
	storage := &diskStartupStorage{
		blocks:     newDiskStartupBlockStorage(block),
		aggregator: newDiskStartupAggregatorRecordStorage(diskStartupAggregatorRecord(block, leaf, 11, 0)),
	}
	rm := newDiskStartupRoundManager(t, backend, storage)

	err := LoadRecoveredNodesIntoBackend(ctx, rm.logger, storage, backend, blockNumber, []api.StateID{api.StateID(leaf.Key)})
	require.ErrorContains(t, err, "recovered SMT root mismatch")
}

func TestDiskSMTRecoveredFinalizationReconciliationErrorIsReturned(t *testing.T) {
	ctx := context.Background()
	rm := newDiskStartupRoundManager(t, newTestDiskBackendForStartup(t), &diskStartupStorage{})
	rm.proofPending = map[string]struct{}{"pending-state": {}}

	err := rm.reconcileRecoveredFinalization(ctx, &RecoveryResult{
		Recovered:   true,
		BlockNumber: api.NewBigIntFromUint64(99),
	})
	require.ErrorContains(t, err, "recovered block 99 is not finalized")

	rm.proofCacheMu.RLock()
	_, stillPending := rm.proofPending["pending-state"]
	rm.proofCacheMu.RUnlock()
	require.True(t, stillPending, "proof-pending state must not be cleared after failed disk SMT reconciliation")
}

func newTestDiskBackendForStartup(t *testing.T) *smtbackend.DiskBackend {
	t.Helper()
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	backend, err := smtbackend.NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })
	return backend
}

func newDiskStartupRoundManager(t *testing.T, backend smtbackend.Backend, storage interfaces.Storage) *RoundManager {
	t.Helper()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	return &RoundManager{
		config:       &config.Config{},
		logger:       log,
		storage:      storage,
		smtBackend:   backend,
		stateTracker: state.NewSyncStateTracker(),
	}
}

func diskStartupLeaf(keyByte, valueByte byte) smtbackend.LeafInput {
	key := make([]byte, api.StateTreeKeyLengthBytes)
	key[len(key)-1] = keyByte
	return smtbackend.LeafInput{
		Key:   key,
		Value: api.LeafValue(diskStartupTransactionHash(valueByte).DataBytes(), 1755000000),
	}
}

func diskStartupTransactionHash(valueByte byte) api.TransactionHash {
	value := make([]byte, api.StateTreeKeyLengthBytes)
	value[len(value)-1] = valueByte
	return api.TransactionHash(value)
}

func commitDiskStartupLeaves(t *testing.T, ctx context.Context, backend smtbackend.Backend, blockNumber uint64, leaves []smtbackend.LeafInput) []byte {
	t.Helper()
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, leaves)
	require.NoError(t, err)
	require.NoError(t, result.ValidateAllAccepted(len(leaves)))
	require.NoError(t, snapshot.Commit(ctx, smtbackend.CommitMetadata{
		BlockNumber: api.NewBigIntFromUint64(blockNumber),
		RootHash:    result.CandidateRoot,
	}))
	return result.CandidateRoot
}

func diskStartupBlock(number uint64, root []byte) *models.Block {
	block := models.NewBlock(
		api.NewBigIntFromUint64(number),
		"test-chain",
		0,
		"1",
		"test",
		api.HexBytes(append([]byte(nil), root...)),
		nil,
		nil,
		1755000000,
	)
	block.Finalized = true
	block.Status = models.FinalityStatusFinalized
	block.ProposalID = "proposal-" + block.Index.String()
	return block
}

func diskStartupAggregatorRecord(block *models.Block, leaf smtbackend.LeafInput, transactionHashByte byte, leafIndex uint64) *models.AggregatorRecord {
	return &models.AggregatorRecord{
		StateID:       api.StateID(append([]byte(nil), leaf.Key...)),
		ReferenceTime: 1755000000,
		CertificationData: models.CertificationData{
			TransactionHash: diskStartupTransactionHash(transactionHashByte),
		},
		BlockNumber: block.Index,
		LeafIndex:   api.NewBigIntFromUint64(leafIndex),
		ProposalID:  block.ProposalID,
	}
}

type diskStartupStorage struct {
	blocks     *diskStartupBlockStorage
	smt        *diskStartupSMTStorage
	aggregator *diskStartupAggregatorRecordStorage
}

func (s *diskStartupStorage) AggregatorRecordStorage() interfaces.AggregatorRecordStorage {
	if s.aggregator == nil {
		return newDiskStartupAggregatorRecordStorage()
	}
	return s.aggregator
}
func (s *diskStartupStorage) BlockStorage() interfaces.BlockStorage {
	if s.blocks == nil {
		return newDiskStartupBlockStorage()
	}
	return s.blocks
}
func (s *diskStartupStorage) SmtStorage() interfaces.SmtStorage {
	if s.smt == nil {
		return newDiskStartupSMTStorage()
	}
	return s.smt
}
func (s *diskStartupStorage) LeadershipStorage() interfaces.LeadershipStorage { return nil }
func (s *diskStartupStorage) TrustBaseStorage() interfaces.TrustBaseStorage   { return nil }
func (s *diskStartupStorage) Initialize(context.Context) error                { return nil }
func (s *diskStartupStorage) Ping(context.Context) error                      { return nil }
func (s *diskStartupStorage) Close(context.Context) error                     { return nil }
func (s *diskStartupStorage) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

type diskStartupBlockStorage struct {
	blocks                   map[string]*models.Block
	latest                   *models.Block
	afterGetFinalizedPage    func()
	finalizedPageCalls       int
	finalizedPageErrorOnCall int
	finalizedPageAfter       []string
}

func newDiskStartupBlockStorage(blocks ...*models.Block) *diskStartupBlockStorage {
	out := &diskStartupBlockStorage{blocks: make(map[string]*models.Block)}
	for _, block := range blocks {
		out.blocks[block.Index.String()] = block
		if block.Finalized && (out.latest == nil || block.Index.Cmp(out.latest.Index.Int) > 0) {
			out.latest = block
		}
	}
	return out
}

func (s *diskStartupBlockStorage) Store(_ context.Context, block *models.Block) error {
	s.blocks[block.Index.String()] = block
	if block.Finalized && (s.latest == nil || block.Index.Cmp(s.latest.Index.Int) > 0) {
		s.latest = block
	}
	return nil
}
func (s *diskStartupBlockStorage) GetByNumber(_ context.Context, blockNumber *api.BigInt) (*models.Block, error) {
	block := s.blocks[blockNumber.String()]
	if block == nil || !block.Finalized {
		return nil, nil
	}
	return block, nil
}
func (s *diskStartupBlockStorage) GetLatest(context.Context) (*models.Block, error) {
	return s.latest, nil
}
func (s *diskStartupBlockStorage) GetLatestNumber(context.Context) (*api.BigInt, error) {
	if s.latest == nil {
		return nil, nil
	}
	return s.latest.Index, nil
}
func (s *diskStartupBlockStorage) GetLatestByRootHash(context.Context, api.HexBytes) (*models.Block, error) {
	return nil, nil
}
func (s *diskStartupBlockStorage) Count(context.Context) (int64, error) {
	return int64(len(s.blocks)), nil
}
func (s *diskStartupBlockStorage) GetFinalizedPage(ctx context.Context, afterBlock, toBlock *api.BigInt, limit int) ([]*models.Block, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.finalizedPageCalls++
	s.finalizedPageAfter = append(s.finalizedPageAfter, afterBlock.String())
	if s.finalizedPageErrorOnCall == s.finalizedPageCalls {
		return nil, errors.New("injected finalized page failure")
	}
	if limit <= 0 {
		return nil, errors.New("finalized block page limit must be positive")
	}
	var blocks []*models.Block
	for _, block := range s.blocks {
		if !block.Finalized || block.Index.Cmp(afterBlock.Int) <= 0 || block.Index.Cmp(toBlock.Int) > 0 {
			continue
		}
		blocks = append(blocks, block)
	}
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Index.Cmp(blocks[j].Index.Int) < 0
	})
	if len(blocks) > limit {
		blocks = blocks[:limit]
	}
	if s.afterGetFinalizedPage != nil {
		s.afterGetFinalizedPage()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return blocks, nil
}
func (s *diskStartupBlockStorage) GetNextFinalizedAfter(_ context.Context, afterBlock, toBlock *api.BigInt) (*models.Block, error) {
	var next *models.Block
	for _, block := range s.blocks {
		if !block.Finalized || block.Index.Cmp(afterBlock.Int) <= 0 || block.Index.Cmp(toBlock.Int) > 0 {
			continue
		}
		if next == nil || block.Index.Cmp(next.Index.Int) < 0 {
			next = block
		}
	}
	return next, nil
}
func (s *diskStartupBlockStorage) SetFinalized(_ context.Context, blockNumber *api.BigInt, finalized bool) error {
	block := s.blocks[blockNumber.String()]
	if block == nil {
		return errors.New("block not found")
	}
	block.Finalized = finalized
	s.latest = nil
	for _, candidate := range s.blocks {
		if candidate.Finalized && (s.latest == nil || candidate.Index.Cmp(s.latest.Index.Int) > 0) {
			s.latest = candidate
		}
	}
	return nil
}
func (s *diskStartupBlockStorage) GetUnfinalized(context.Context) ([]*models.Block, error) {
	blocks := make([]*models.Block, 0)
	for _, block := range s.blocks {
		if !block.Finalized {
			blocks = append(blocks, block)
		}
	}
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Index.Cmp(blocks[j].Index.Int) < 0
	})
	return blocks, nil
}

func diskSMTStartupFailureMetric(t *testing.T) float64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, metrics.SMTStartupRecoveryActions.WithLabelValues("fail").Write(metric))
	return metric.GetCounter().GetValue()
}

type diskStartupAggregatorRecordStorage struct {
	records map[string]*models.AggregatorRecord
}

func newDiskStartupAggregatorRecordStorage(records ...*models.AggregatorRecord) *diskStartupAggregatorRecordStorage {
	out := &diskStartupAggregatorRecordStorage{records: make(map[string]*models.AggregatorRecord)}
	for _, record := range records {
		out.records[record.StateID.String()] = record
	}
	return out
}

func (s *diskStartupAggregatorRecordStorage) Store(_ context.Context, record *models.AggregatorRecord) error {
	s.records[record.StateID.String()] = record
	return nil
}

func (s *diskStartupAggregatorRecordStorage) StoreBatch(_ context.Context, records []*models.AggregatorRecord) error {
	for _, record := range records {
		s.records[record.StateID.String()] = record
	}
	return nil
}

func (s *diskStartupAggregatorRecordStorage) GetByStateID(_ context.Context, stateID api.StateID) (*models.AggregatorRecord, error) {
	return s.records[stateID.String()], nil
}

func (s *diskStartupAggregatorRecordStorage) GetByBlockNumber(_ context.Context, blockNumber *api.BigInt) ([]*models.AggregatorRecord, error) {
	records := make([]*models.AggregatorRecord, 0, len(s.records))
	for _, record := range s.records {
		if record.BlockNumber != nil && record.BlockNumber.Cmp(blockNumber.Int) == 0 {
			records = append(records, record)
		}
	}
	sortAggregatorRecordsByLeafIndex(records)
	return records, nil
}

func (s *diskStartupAggregatorRecordStorage) Count(context.Context) (int64, error) {
	return int64(len(s.records)), nil
}

func (s *diskStartupAggregatorRecordStorage) EstimatedCount(context.Context) (int64, error) {
	return int64(len(s.records)), nil
}

func (s *diskStartupAggregatorRecordStorage) GetExistingStateIDs(_ context.Context, stateIDs []string) (map[string]bool, error) {
	existing := make(map[string]bool, len(stateIDs))
	for _, stateID := range stateIDs {
		if s.records[stateID] != nil {
			existing[stateID] = true
		}
	}
	return existing, nil
}

type diskStartupSMTStorage struct {
	nodes map[string]*models.SmtNode
}

func newDiskStartupSMTStorage(nodes ...*models.SmtNode) *diskStartupSMTStorage {
	out := &diskStartupSMTStorage{nodes: make(map[string]*models.SmtNode)}
	for _, node := range nodes {
		out.nodes[node.Key.String()] = node
	}
	return out
}

func (s *diskStartupSMTStorage) Store(context.Context, *models.SmtNode) error { return nil }
func (s *diskStartupSMTStorage) StoreBatch(context.Context, []*models.SmtNode) error {
	return nil
}
func (s *diskStartupSMTStorage) UpsertBatch(context.Context, []*models.SmtNode) error {
	return nil
}
func (s *diskStartupSMTStorage) GetByKey(_ context.Context, key api.HexBytes) (*models.SmtNode, error) {
	return s.nodes[key.String()], nil
}
func (s *diskStartupSMTStorage) GetByKeys(_ context.Context, keys []api.HexBytes) ([]*models.SmtNode, error) {
	nodes := make([]*models.SmtNode, 0, len(keys))
	for _, key := range keys {
		node := s.nodes[key.String()]
		if node != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}
func (s *diskStartupSMTStorage) Delete(context.Context, api.HexBytes) error        { return nil }
func (s *diskStartupSMTStorage) DeleteBatch(context.Context, []api.HexBytes) error { return nil }
func (s *diskStartupSMTStorage) Count(context.Context) (int64, error) {
	return int64(len(s.nodes)), nil
}
func (s *diskStartupSMTStorage) EstimatedCount(context.Context) (int64, error) {
	return int64(len(s.nodes)), nil
}
func (s *diskStartupSMTStorage) GetAll(context.Context) ([]*models.SmtNode, error) {
	return nil, errors.New("not implemented")
}
func (s *diskStartupSMTStorage) GetChunked(context.Context, int, int) ([]*models.SmtNode, error) {
	return nil, errors.New("not implemented")
}
func (s *diskStartupSMTStorage) GetExistingKeys(_ context.Context, keys []string) (map[string]bool, error) {
	existing := make(map[string]bool, len(keys))
	for _, key := range keys {
		if s.nodes[key] != nil {
			existing[key] = true
		}
	}
	return existing, nil
}

type diskStartupCommitmentQueue struct{}

func (q *diskStartupCommitmentQueue) Store(context.Context, *models.CertificationRequest) error {
	return nil
}

func (q *diskStartupCommitmentQueue) GetByStateID(context.Context, api.StateID) (*models.CertificationRequest, error) {
	return nil, nil
}

func (q *diskStartupCommitmentQueue) GetUnprocessedBatch(context.Context, int) ([]*models.CertificationRequest, error) {
	return nil, nil
}

func (q *diskStartupCommitmentQueue) GetUnprocessedBatchWithCursor(context.Context, string, int) ([]*models.CertificationRequest, string, error) {
	return nil, "", nil
}

func (q *diskStartupCommitmentQueue) StreamCertificationRequests(context.Context, chan<- *models.CertificationRequest) error {
	return nil
}

func (q *diskStartupCommitmentQueue) MarkProcessed(context.Context, []interfaces.CertificationRequestAck) error {
	return nil
}

func (q *diskStartupCommitmentQueue) Delete(context.Context, []api.StateID) error {
	return nil
}

func (q *diskStartupCommitmentQueue) Count(context.Context) (int64, error) {
	return 0, nil
}

func (q *diskStartupCommitmentQueue) CountUnprocessed(context.Context) (int64, error) {
	return 0, nil
}

func (q *diskStartupCommitmentQueue) GetAllPending(context.Context) ([]*models.CertificationRequest, error) {
	return nil, nil
}

func (q *diskStartupCommitmentQueue) GetByStateIDs(context.Context, []api.StateID) (map[string]*models.CertificationRequest, error) {
	return nil, nil
}

func (q *diskStartupCommitmentQueue) Initialize(context.Context) error {
	return nil
}

func (q *diskStartupCommitmentQueue) Close(context.Context) error {
	return nil
}

var (
	_ interfaces.Storage                 = (*diskStartupStorage)(nil)
	_ interfaces.BlockStorage            = (*diskStartupBlockStorage)(nil)
	_ interfaces.SmtStorage              = (*diskStartupSMTStorage)(nil)
	_ interfaces.AggregatorRecordStorage = (*diskStartupAggregatorRecordStorage)(nil)
	_ interfaces.CommitmentQueue         = (*diskStartupCommitmentQueue)(nil)
)
