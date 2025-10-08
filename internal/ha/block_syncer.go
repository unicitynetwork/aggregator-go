package ha

import (
	"context"
	"fmt"
	"math/big"

	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// blockSyncer helper struct to update the SMT with data from commited blocks.
type blockSyncer struct {
	logger       *logger.Logger
	storage      interfaces.Storage
	smt          *smt.ThreadSafeSMT
	stateTracker *state.Tracker
}

func newBlockSyncer(logger *logger.Logger, storage interfaces.Storage, smt *smt.ThreadSafeSMT, stateTracker *state.Tracker) *blockSyncer {
	return &blockSyncer{
		logger:       logger,
		storage:      storage,
		smt:          smt,
		stateTracker: stateTracker,
	}
}

func (bs *blockSyncer) syncToLatestBlock(ctx context.Context) error {
	// fetch last synced smt block number and last stored block number
	currBlock := bs.stateTracker.GetLastSyncedBlock()
	endBlock, err := bs.getLastStoredBlockRecordNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch last stored block number: %w", err)
	}
	bs.logger.WithContext(ctx).Debug("block sync", "from", currBlock, "to", endBlock)
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
		if len(b.RequestIDs) == 0 {
			bs.logger.WithContext(ctx).Debug("skipping block sync (empty block)", "nextBlock", b.BlockNumber.String())
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

func (bs *blockSyncer) verifySMTForBlock(ctx context.Context, smtRootHash string, blockNumber *api.BigInt) error {
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

func (bs *blockSyncer) updateSMTForBlock(ctx context.Context, blockRecord *models.BlockRecords) error {
	// build leaf ids while filtering duplicate blockRecord.RequestIDs
	uniqueRequestIds := make(map[string]struct{}, len(blockRecord.RequestIDs))
	leafIDs := make([]api.HexBytes, 0, len(blockRecord.RequestIDs))
	for _, reqID := range blockRecord.RequestIDs {
		// skip duplicates
		key := string(reqID)
		if _, exists := uniqueRequestIds[key]; exists {
			continue
		}
		uniqueRequestIds[key] = struct{}{}

		path, err := reqID.GetPath()
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

func (bs *blockSyncer) getLastStoredBlockRecordNumber(ctx context.Context) (*big.Int, error) {
	blockRecord, err := bs.storage.BlockRecordsStorage().GetLatestBlock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch latest block number: %w", err)
	}
	if blockRecord == nil {
		return big.NewInt(0), nil
	}
	return blockRecord.BlockNumber.Int, nil
}
