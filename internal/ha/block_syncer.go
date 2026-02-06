package ha

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
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
		smt            *smt.ThreadSafeSMT
		shardID        api.ShardID
		syncInterval   time.Duration
		stateTracker   *state.Tracker

		wg     sync.WaitGroup
		cancel context.CancelFunc
	}
)

func NewBlockSyncer(
	logger *logger.Logger,
	leaderSelector LeaderSelector,
	storage interfaces.Storage,
	smt *smt.ThreadSafeSMT,
	shardID api.ShardID,
	syncInterval time.Duration,
	stateTracker *state.Tracker,
) *BlockSyncer {
	return &BlockSyncer{
		logger:         logger,
		leaderSelector: leaderSelector,
		storage:        storage,
		smt:            smt,
		shardID:        shardID,
		syncInterval:   syncInterval,
		stateTracker:   stateTracker,
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
	// fetch last synced smt block number and last stored block number
	currBlock := bs.stateTracker.GetLastSyncedBlock()
	endBlock, err := bs.getLastStoredBlockRecordNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch last stored block number: %w", err)
	}
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

func (bs *BlockSyncer) verifySMTForBlock(ctx context.Context, smtRootHash string, blockNumber *api.BigInt) error {
	block, err := bs.storage.BlockStorage().GetByNumber(ctx, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to fetch block: %w", err)
	}
	if block == nil {
		return fmt.Errorf("block not found for block number: %s", blockNumber.String())
	}
	expectedRootHash := block.RootHash.String()
	if smtRootHash != expectedRootHash {
		return fmt.Errorf("smt root hash %s does not match latest block root hash %s",
			smtRootHash, expectedRootHash)
	}
	return nil
}

func (bs *BlockSyncer) updateSMTForBlock(ctx context.Context, blockRecord *models.BlockRecords) error {
	// build leaf ids while filtering duplicate blockRecord.StateIDs
	uniqueStateIds := make(map[string]struct{}, len(blockRecord.StateIDs))
	leafIDs := make([]api.HexBytes, 0, len(blockRecord.StateIDs))
	for _, stateID := range blockRecord.StateIDs {
		// skip duplicates
		key := stateID.String()
		if _, exists := uniqueStateIds[key]; exists {
			continue
		}
		uniqueStateIds[key] = struct{}{}

		path, err := stateID.GetPath()
		if err != nil {
			return fmt.Errorf("failed to get path: %w", err)
		}
		leafIDs = append(leafIDs, api.NewHexBytes(path.Bytes()))
	}
	// load smt nodes by ids
	smtNodes, err := bs.storage.SmtStorage().GetByKeys(ctx, leafIDs)
	if err != nil {
		return fmt.Errorf("failed to load smt nodes by keys: %w", err)
	}
	// convert smt nodes to leaves
	leaves := make([]*smt.Leaf, 0, len(smtNodes))
	for _, smtNode := range smtNodes {
		leaves = append(leaves, smt.NewLeaf(new(big.Int).SetBytes(smtNode.Key), smtNode.Value))
	}

	// apply changes to smt snapshot
	snapshot := bs.smt.CreateSnapshot()
	smtRootHash, err := snapshot.AddLeaves(leaves)
	if err != nil {
		return fmt.Errorf("failed to apply SMT updates for block %s: %w", blockRecord.BlockNumber.String(), err)
	}
	// verify smt root hash matches block store root hash
	if err := bs.verifySMTForBlock(ctx, smtRootHash, blockRecord.BlockNumber); err != nil {
		return fmt.Errorf("failed to verify SMT: %w", err)
	}
	// commit smt snapshot
	snapshot.Commit(bs.smt)

	return nil
}

func (bs *BlockSyncer) getLastStoredBlockRecordNumber(ctx context.Context) (*big.Int, error) {
	// Use BlockStorage which filters on finalized=true
	// This ensures we only sync up to the latest finalized block
	latestNumber, err := bs.storage.BlockStorage().GetLatestNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch latest finalized block number: %w", err)
	}
	if latestNumber == nil {
		return big.NewInt(0), nil
	}
	return latestNumber.Int, nil
}
