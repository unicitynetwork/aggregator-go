package round

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func (rm *RoundManager) restoreOrVerifySMT(ctx context.Context) (*api.BigInt, error) {
	if _, ok := rm.smtBackend.(*smtbackend.DiskBackend); ok {
		return rm.verifyDiskSMTStartup(ctx)
	}
	return rm.restoreSmtFromStorage(ctx)
}

func (rm *RoundManager) verifyDiskSMTStartup(ctx context.Context) (*api.BigInt, error) {
	rm.logger.Info("Starting disk SMT startup verification")

	state, err := rm.smtBackend.CommittedState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read disk SMT committed state: %w", err)
	}
	latestBlock, err := rm.storage.BlockStorage().GetLatest(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block for disk SMT verification: %w", err)
	}

	if latestBlock == nil {
		if !bytes.Equal(state.RootHash, emptyStateRoot()) {
			return nil, fmt.Errorf("disk SMT has root %s but no finalized block exists", api.HexBytes(state.RootHash).String())
		}
		rm.logger.Info("Disk SMT verified against empty finalized history")
		return nil, nil
	}

	if state.BlockNumber == nil {
		return nil, fmt.Errorf("disk SMT is empty but finalized history exists at block %s; full bootstrap is out of scope for this phase", latestBlock.Index.String())
	}

	switch state.BlockNumber.Cmp(latestBlock.Index.Int) {
	case 0:
		if !bytes.Equal(state.RootHash, latestBlock.RootHash) {
			return nil, fmt.Errorf("disk SMT root mismatch at block %s: disk=%s finalized=%s",
				latestBlock.Index.String(), api.HexBytes(state.RootHash).String(), latestBlock.RootHash.String())
		}
		if rm.stateTracker != nil {
			rm.stateTracker.SetLastSyncedBlock(latestBlock.Index.Int)
		}
		rm.logger.Info("Disk SMT verified against latest finalized block",
			"blockNumber", latestBlock.Index.String(),
			"rootHash", latestBlock.RootHash.String())
		return latestBlock.Index, nil
	case -1:
		return rm.replayDiskSMTGap(ctx, state.BlockNumber, latestBlock.Index)
	default:
		return nil, fmt.Errorf("disk SMT block %s is ahead of latest finalized block %s; recovery of local-ahead state is out of scope for this phase",
			state.BlockNumber.String(), latestBlock.Index.String())
	}
}

func (rm *RoundManager) replayDiskSMTGap(ctx context.Context, fromBlock, latestBlock *api.BigInt) (*api.BigInt, error) {
	diff := new(big.Int).Sub(latestBlock.Int, fromBlock.Int)
	limit := big.NewInt(int64(rm.config.SMT.StartupReplayLimitBlocks))
	if diff.Sign() < 0 {
		return nil, fmt.Errorf("invalid disk SMT replay range from %s to %s", fromBlock.String(), latestBlock.String())
	}
	if diff.Cmp(limit) > 0 {
		return nil, fmt.Errorf("disk SMT is behind by %s blocks, exceeding SMT_STARTUP_REPLAY_LIMIT_BLOCKS=%d; explicit rebuild required",
			diff.String(), rm.config.SMT.StartupReplayLimitBlocks)
	}

	rm.logger.Info("Disk SMT is behind finalized history, starting bounded replay",
		"fromBlock", fromBlock.String(),
		"latestBlock", latestBlock.String(),
		"blocksBehind", diff.String())

	for n := new(big.Int).Add(fromBlock.Int, big.NewInt(1)); n.Cmp(latestBlock.Int) <= 0; n.Add(n, big.NewInt(1)) {
		blockNumber := api.NewBigInt(new(big.Int).Set(n))
		if err := rm.replayDiskSMTBlock(ctx, blockNumber); err != nil {
			return nil, err
		}
	}

	state, err := rm.smtBackend.CommittedState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read disk SMT state after replay: %w", err)
	}
	if state.BlockNumber == nil || state.BlockNumber.Cmp(latestBlock.Int) != 0 {
		return nil, fmt.Errorf("disk SMT replay ended at block %v, expected %s", state.BlockNumber, latestBlock.String())
	}
	latest, err := rm.storage.BlockStorage().GetLatest(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to reload latest block after disk SMT replay: %w", err)
	}
	if latest == nil || !bytes.Equal(state.RootHash, latest.RootHash) {
		return nil, fmt.Errorf("disk SMT replay root mismatch: disk=%s latest=%v",
			api.HexBytes(state.RootHash).String(), latest)
	}
	if rm.stateTracker != nil {
		rm.stateTracker.SetLastSyncedBlock(latestBlock.Int)
	}
	rm.logger.Info("Disk SMT bounded replay complete",
		"blockNumber", latestBlock.String(),
		"rootHash", api.HexBytes(state.RootHash).String())
	return latestBlock, nil
}

func (rm *RoundManager) replayDiskSMTBlock(ctx context.Context, blockNumber *api.BigInt) error {
	block, err := rm.storage.BlockStorage().GetByNumber(ctx, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to load block %s for disk SMT replay: %w", blockNumber.String(), err)
	}
	if block == nil || !block.Finalized {
		return fmt.Errorf("finalized block %s not found for disk SMT replay", blockNumber.String())
	}

	blockRecords, err := rm.storage.BlockRecordsStorage().GetByBlockNumber(ctx, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to load block records for disk SMT replay block %s: %w", blockNumber.String(), err)
	}
	if blockRecords == nil {
		return fmt.Errorf("block records for disk SMT replay block %s not found", blockNumber.String())
	}

	leaves, err := rm.replayLeavesForStateIDs(ctx, blockRecords.StateIDs)
	if err != nil {
		return fmt.Errorf("failed to load replay leaves for block %s: %w", blockNumber.String(), err)
	}

	snapshot, err := rm.smtBackend.CreateSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("failed to create disk SMT replay snapshot for block %s: %w", blockNumber.String(), err)
	}
	result, err := snapshot.AddLeavesClassified(ctx, leaves)
	if err != nil {
		snapshot.Discard(ctx)
		return fmt.Errorf("failed to replay disk SMT leaves for block %s: %w", blockNumber.String(), err)
	}
	if err := result.ValidateAllAccepted(len(leaves)); err != nil {
		snapshot.Discard(ctx)
		return fmt.Errorf("failed to replay disk SMT leaves for block %s: %w", blockNumber.String(), err)
	}
	if !bytes.Equal(result.CandidateRoot, block.RootHash) {
		snapshot.Discard(ctx)
		return fmt.Errorf("disk SMT replay root mismatch at block %s: candidate=%s block=%s",
			blockNumber.String(), api.HexBytes(result.CandidateRoot).String(), block.RootHash.String())
	}
	if err := snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: block.Index, RootHash: block.RootHash}); err != nil {
		snapshot.Discard(ctx)
		return fmt.Errorf("failed to commit disk SMT replay block %s: %w", blockNumber.String(), err)
	}
	return nil
}

func (rm *RoundManager) replayLeavesForStateIDs(ctx context.Context, stateIDs []api.StateID) ([]smtbackend.LeafInput, error) {
	if len(stateIDs) == 0 {
		return nil, nil
	}

	seen := make(map[string]struct{}, len(stateIDs))
	keys := make([]api.HexBytes, 0, len(stateIDs))
	for _, stateID := range stateIDs {
		key := string(stateID)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keyBytes, err := stateID.GetTreeKey()
		if err != nil {
			return nil, fmt.Errorf("stateID %s tree key: %w", stateID.String(), err)
		}
		keys = append(keys, api.HexBytes(keyBytes))
	}

	nodes, err := rm.storage.SmtStorage().GetByKeys(ctx, keys)
	if err != nil {
		return nil, err
	}
	if len(nodes) != len(keys) {
		return nil, fmt.Errorf("expected %d SMT nodes, found %d", len(keys), len(nodes))
	}

	leaves := make([]smtbackend.LeafInput, len(nodes))
	for i, node := range nodes {
		leaves[i] = smtbackend.LeafInput{
			Key:   node.Key,
			Value: node.Value,
		}
	}
	return leaves, nil
}

func emptyStateRoot() []byte {
	root := disk.EmptyRootHash()
	out := make([]byte, len(root))
	copy(out, root[:])
	return out
}
