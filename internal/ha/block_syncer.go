package ha

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type (
	LeaderSelector interface {
		IsLeader(ctx context.Context) (bool, error)
	}

	// BlockSyncer updates the node's state tree using the blocks from storage, if in follower mode.
	// Needs to be started with the Start method and stopped with the Stop method.
	// Should not be started in standalone mode.
	BlockSyncer struct {
		logger         *logger.Logger
		leaderSelector LeaderSelector
		storage        interfaces.Storage
		smtBackend     smtbackend.Backend
		shardID        api.ShardID
		syncInterval   time.Duration
		stateTracker   *state.Tracker

		syncSem chan struct{}
		wg      sync.WaitGroup
		cancel  context.CancelFunc
	}
)

var ErrDiskSMTDiverged = errors.New("disk SMT diverged from finalized chain")

func NewBlockSyncer(
	logger *logger.Logger,
	leaderSelector LeaderSelector,
	storage interfaces.Storage,
	smtBackend smtbackend.Backend,
	shardID api.ShardID,
	syncInterval time.Duration,
	stateTracker *state.Tracker,
) *BlockSyncer {
	syncSem := make(chan struct{}, 1)
	syncSem <- struct{}{}
	return &BlockSyncer{
		logger:         logger,
		leaderSelector: leaderSelector,
		storage:        storage,
		smtBackend:     smtBackend,
		shardID:        shardID,
		syncInterval:   syncInterval,
		stateTracker:   stateTracker,
		syncSem:        syncSem,
	}
}

func (bs *BlockSyncer) Start(ctx context.Context) {
	ctx, bs.cancel = context.WithCancel(ctx)
	bs.wg.Go(func() {
		bs.runLoop(ctx)
	})
}

func (bs *BlockSyncer) Stop() {
	if bs.cancel != nil {
		bs.cancel()
		bs.cancel = nil
	}
	bs.wg.Wait()
}

func (bs *BlockSyncer) runLoop(ctx context.Context) {
	ticker := time.NewTicker(bs.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := bs.onTick(ctx); err != nil {
				bs.logger.WithContext(ctx).Error("error on block sync tick", "error", err.Error())
			}
		}
	}
}

func (bs *BlockSyncer) onTick(ctx context.Context) error {
	isLeader, err := bs.leaderSelector.IsLeader(ctx)
	if err != nil {
		return fmt.Errorf("failed to query leader status: %w", err)
	}
	if !isLeader {
		if err := bs.SyncToLatestBlock(ctx); err != nil {
			return fmt.Errorf("failed to sync smt to latest block: %w", err)
		}
	}
	return nil
}

func (bs *BlockSyncer) SyncToLatestBlock(ctx context.Context) error {
	release, err := bs.acquireSync(ctx)
	if err != nil {
		return err
	}
	defer release()

	latest, err := bs.storage.BlockStorage().GetLatest(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch latest finalized block: %w", err)
	}
	return bs.syncToBlock(ctx, latest)
}

// SyncToLatestFinalizedBlock samples the current finalized head and syncs exactly to that block.
// It is used on leader promotion so activation starts from a fixed, verified target.
func (bs *BlockSyncer) SyncToLatestFinalizedBlock(ctx context.Context) (*models.Block, error) {
	release, err := bs.acquireSync(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	latest, err := bs.storage.BlockStorage().GetLatest(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch latest finalized block: %w", err)
	}
	if err := bs.syncToBlock(ctx, latest); err != nil {
		return latest, err
	}
	return latest, nil
}

func (bs *BlockSyncer) syncToBlock(ctx context.Context, target *models.Block) error {
	if target == nil {
		return nil
	}
	if bs.usesDiskSMTBackend() {
		if err := bs.syncDiskToBlock(ctx, target.Index.Int); err != nil {
			return err
		}
		return bs.verifyDiskCommittedBlock(ctx, target)
	}
	return bs.syncMemoryToBlock(ctx, target.Index.Int)
}

func (bs *BlockSyncer) acquireSync(ctx context.Context) (func(), error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-bs.syncSem:
		return func() {
			bs.syncSem <- struct{}{}
		}, nil
	}
}

func (bs *BlockSyncer) syncMemoryToBlock(ctx context.Context, endBlock *big.Int) error {
	// fetch last synced smt block number and last stored block number
	currBlock := bs.stateTracker.GetLastSyncedBlock()
	for currBlock.Cmp(endBlock) < 0 {
		// fetch the next block record
		b, err := bs.storage.BlockRecordsStorage().GetNextBlock(ctx, api.NewBigInt(currBlock))
		if err != nil {
			return fmt.Errorf("failed to fetch next block: %w", err)
		}
		if b == nil {
			return fmt.Errorf("next block record not found block: %s", currBlock.String())
		}

		// skip empty blocks
		if len(b.StateIDs) == 0 {
			bs.logger.WithContext(ctx).Debug("skipping block sync (empty block)", "blockNumber", b.BlockNumber.String())
			currBlock = b.BlockNumber.Int
			bs.stateTracker.SetLastSyncedBlock(currBlock)
			continue
		}
		bs.logger.WithContext(ctx).Debug("updating SMT for round", "blockNumber", b.BlockNumber.String())

		// apply changes from block record to SMT
		if err := bs.updateSMTForBlock(ctx, b); err != nil {
			return fmt.Errorf("failed to update SMT: %w", err)
		}

		currBlock = b.BlockNumber.Int
		bs.stateTracker.SetLastSyncedBlock(currBlock)
		bs.logger.Info("SMT updated for round", "roundNumber", currBlock)
	}
	return nil
}

func (bs *BlockSyncer) syncDiskToBlock(ctx context.Context, endBlock *big.Int) error {
	state, err := bs.smtBackend.CommittedState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read disk SMT committed state: %w", err)
	}
	currBlock := big.NewInt(0)
	if state.BlockNumber != nil {
		currBlock = new(big.Int).Set(state.BlockNumber.Int)
	}

	for currBlock.Cmp(endBlock) < 0 {
		b, err := bs.storage.BlockRecordsStorage().GetNextBlock(ctx, api.NewBigInt(currBlock))
		if err != nil {
			return fmt.Errorf("failed to fetch next block: %w", err)
		}
		if b == nil {
			return fmt.Errorf("next block record not found block: %s", currBlock.String())
		}
		if b.BlockNumber.Int.Cmp(endBlock) > 0 {
			return fmt.Errorf("next block record %s is after latest finalized block %s", b.BlockNumber.String(), endBlock.String())
		}

		if len(b.StateIDs) == 0 {
			bs.logger.WithContext(ctx).Debug("advancing disk SMT metadata for empty block", "blockNumber", b.BlockNumber.String())
		} else {
			bs.logger.WithContext(ctx).Debug("updating disk SMT for round", "blockNumber", b.BlockNumber.String())
		}

		if err := bs.updateSMTForBlock(ctx, b); err != nil {
			return fmt.Errorf("failed to update disk SMT: %w", err)
		}

		currBlock = new(big.Int).Set(b.BlockNumber.Int)
		if bs.stateTracker != nil {
			bs.stateTracker.SetLastSyncedBlock(currBlock)
		}
		bs.logger.Info("Disk SMT updated for round", "roundNumber", currBlock)
	}
	return nil
}

func (bs *BlockSyncer) verifyDiskCommittedBlock(ctx context.Context, target *models.Block) error {
	state, err := bs.smtBackend.CommittedState(ctx)
	if err != nil {
		return fmt.Errorf("failed to read disk SMT committed state: %w", err)
	}
	if state.BlockNumber == nil {
		return fmt.Errorf("%w: disk SMT has no committed block after syncing to %s", ErrDiskSMTDiverged, target.Index.String())
	}
	if state.BlockNumber.Cmp(target.Index.Int) != 0 {
		return fmt.Errorf("%w: disk SMT committed block %s does not match target block %s", ErrDiskSMTDiverged, state.BlockNumber.String(), target.Index.String())
	}
	if !bytes.Equal(state.RootHash, target.RootHash) {
		return fmt.Errorf("%w: disk SMT root %s does not match target block root %s", ErrDiskSMTDiverged,
			api.HexBytes(state.RootHash).String(), target.RootHash.String())
	}
	return nil
}

func (bs *BlockSyncer) usesDiskSMTBackend() bool {
	diskBacked, ok := bs.smtBackend.(smtbackend.DiskBacked)
	return ok && diskBacked.IsDiskBackedSMT()
}

func (bs *BlockSyncer) verifySMTForBlock(ctx context.Context, smtRootHash api.HexBytes, blockNumber *api.BigInt) error {
	block, err := bs.storage.BlockStorage().GetByNumber(ctx, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to fetch block: %w", err)
	}
	if block == nil {
		return fmt.Errorf("block not found for block number: %s", blockNumber.String())
	}
	expectedRootHash := block.RootHash
	if !bytes.Equal(smtRootHash, expectedRootHash) {
		return fmt.Errorf("smt root hash %s does not match latest block root hash %s",
			smtRootHash.String(), expectedRootHash.String())
	}
	return nil
}

func (bs *BlockSyncer) updateSMTForBlock(ctx context.Context, blockRecord *models.BlockRecords) error {
	leaves, err := bs.replayLeavesForStateIDs(ctx, blockRecord.StateIDs)
	if err != nil {
		return err
	}

	// apply changes to smt snapshot
	snapshot, err := bs.smtBackend.CreateSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SMT snapshot: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			snapshot.Discard(ctx)
		}
	}()
	result, err := snapshot.AddLeavesClassified(ctx, leaves)
	if err != nil {
		return fmt.Errorf("failed to apply SMT updates for block %s: %w", blockRecord.BlockNumber.String(), err)
	}
	if err := result.ValidateAllAccepted(len(leaves)); err != nil {
		return fmt.Errorf("failed to apply SMT updates for block %s: %w", blockRecord.BlockNumber.String(), err)
	}
	smtRootHash := api.HexBytes(result.CandidateRoot)
	// verify smt root hash matches the raw 32-byte block root hash
	if err := bs.verifySMTForBlock(ctx, smtRootHash, blockRecord.BlockNumber); err != nil {
		if bs.usesDiskSMTBackend() {
			return fmt.Errorf("%w: %w", ErrDiskSMTDiverged, err)
		}
		return fmt.Errorf("failed to verify SMT: %w", err)
	}
	// commit smt snapshot
	if err := snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: blockRecord.BlockNumber, RootHash: smtRootHash}); err != nil {
		return fmt.Errorf("failed to commit SMT snapshot: %w", err)
	}
	committed = true
	if err := bs.refreshProofView(ctx, smtRootHash); err != nil {
		return fmt.Errorf("failed to refresh disk SMT proof view: %w", err)
	}

	return nil
}

func (bs *BlockSyncer) replayLeavesForStateIDs(ctx context.Context, stateIDs []api.StateID) ([]smtbackend.LeafInput, error) {
	if len(stateIDs) == 0 {
		return nil, nil
	}

	uniqueStateIds := make(map[string]struct{}, len(stateIDs))
	leafIDs := make([]api.HexBytes, 0, len(stateIDs))
	for _, stateID := range stateIDs {
		key := stateID.String()
		if _, exists := uniqueStateIds[key]; exists {
			continue
		}
		uniqueStateIds[key] = struct{}{}

		keyBytes, err := stateID.GetTreeKey()
		if err != nil {
			return nil, fmt.Errorf("failed to get SMT key: %w", err)
		}
		leafIDs = append(leafIDs, api.NewHexBytes(keyBytes))
	}

	smtNodes, err := bs.storage.SmtStorage().GetByKeys(ctx, leafIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to load smt nodes by keys: %w", err)
	}
	if len(smtNodes) != len(leafIDs) {
		return nil, fmt.Errorf("expected %d SMT leaves, found %d", len(leafIDs), len(smtNodes))
	}

	leaves := make([]smtbackend.LeafInput, 0, len(smtNodes))
	for _, smtNode := range smtNodes {
		leaves = append(leaves, smtbackend.LeafInput{
			Key:   smtNode.Key,
			Value: smtNode.Value,
		})
	}
	return leaves, nil
}

func (bs *BlockSyncer) refreshProofView(ctx context.Context, expectedRoot []byte) error {
	publisher, ok := bs.smtBackend.(smtbackend.ProofViewPublisher)
	if !ok {
		return nil
	}
	return publisher.RefreshPublishedProofView(ctx, expectedRoot)
}
