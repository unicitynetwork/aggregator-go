package round

import (
	"context"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type RecoveryResult struct {
	Recovered   bool
	BlockNumber *api.BigInt
	StateIDs    []api.StateID
}

// indexedStateID tracks a state ID with its original position in the block.
type indexedStateID struct {
	stateID   api.StateID
	leafIndex int
}

// RecoverUnfinalizedBlock checks for and completes any unfinalized blocks.
// Must be called before starting the round manager.
func RecoverUnfinalizedBlock(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	commitmentQueue interfaces.CommitmentQueue,
) (*RecoveryResult, error) {
	log.WithContext(ctx).Info("Checking for unfinalized blocks...")

	unfinalizedBlocks, err := storage.BlockStorage().GetUnfinalized(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get unfinalized blocks: %w", err)
	}

	if len(unfinalizedBlocks) > 1 {
		return nil, fmt.Errorf("FATAL: found %d unfinalized blocks - data corruption", len(unfinalizedBlocks))
	}

	if len(unfinalizedBlocks) == 0 {
		log.WithContext(ctx).Info("No unfinalized blocks found")

		// Cleanup pending commitments that are already processed
		// (handles case where block was finalized but MarkProcessed failed)
		if err := CleanupProcessedPendingCommitments(ctx, log, storage, commitmentQueue); err != nil {
			return nil, fmt.Errorf("failed to cleanup pending commitments: %w", err)
		}

		return &RecoveryResult{Recovered: false}, nil
	}

	block := unfinalizedBlocks[0]
	log.WithContext(ctx).Info("Found unfinalized block, starting recovery",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String())

	stateIDs, err := recoverBlock(ctx, log, storage, commitmentQueue, block)
	if err != nil {
		return nil, fmt.Errorf("failed to recover block %s: %w", block.Index.String(), err)
	}

	log.WithContext(ctx).Info("Block recovery completed successfully", "blockNumber", block.Index.String())
	return &RecoveryResult{
		Recovered:   true,
		BlockNumber: block.Index,
		StateIDs:    stateIDs,
	}, nil
}

func recoverBlock(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	commitmentQueue interfaces.CommitmentQueue,
	block *models.Block,
) ([]api.StateID, error) {
	blockRecords, err := storage.BlockRecordsStorage().GetByBlockNumber(ctx, block.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to get block records: %w", err)
	}
	if blockRecords == nil {
		return nil, fmt.Errorf("FATAL: block records not found for block %s", block.Index.String())
	}

	stateIDs := blockRecords.StateIDs
	log.WithContext(ctx).Info("Block records found", "stateCount", len(stateIDs))

	existingRecordIDs, err := storage.AggregatorRecordStorage().GetExistingStateIDs(ctx, stateIDsToStrings(stateIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to check existing records: %w", err)
	}

	smtKeyStrings := make([]string, len(stateIDs))
	for i, stateID := range stateIDs {
		keyBytes, err := stateID.GetTreeKey()
		if err != nil {
			return nil, fmt.Errorf("failed to get SMT key for stateID: %w", err)
		}
		smtKeyStrings[i] = api.HexBytes(keyBytes).String()
	}
	existingSmtKeys, err := storage.SmtStorage().GetExistingKeys(ctx, smtKeyStrings)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing SMT nodes: %w", err)
	}

	var missingRecords []indexedStateID
	var missingSmtKeys []api.StateID
	for i, stateID := range stateIDs {
		if !existingRecordIDs[stateID.String()] {
			missingRecords = append(missingRecords, indexedStateID{stateID: stateID, leafIndex: i})
		}
		if !existingSmtKeys[smtKeyStrings[i]] {
			missingSmtKeys = append(missingSmtKeys, stateID)
		}
	}

	log.WithContext(ctx).Info("Recovery status",
		"totalStateIDs", len(stateIDs),
		"existingRecords", len(existingRecordIDs),
		"existingSmtNodes", len(existingSmtKeys),
		"missingRecords", len(missingRecords),
		"missingSmtNodes", len(missingSmtKeys))

	if len(missingRecords) > 0 || len(missingSmtKeys) > 0 {
		if err := recoverMissingData(ctx, log, storage, commitmentQueue, block.Index, missingRecords, missingSmtKeys); err != nil {
			return nil, err
		}
	}

	if err := storage.BlockStorage().SetFinalized(ctx, block.Index, true); err != nil {
		return nil, fmt.Errorf("failed to set block as finalized: %w", err)
	}

	// Ack certification requests in Redis - fetch only the ones we need by state ID.
	commitmentMap, err := commitmentQueue.GetByStateIDs(ctx, stateIDs)
	if err != nil {
		log.WithContext(ctx).Warn("Failed to get commitments for acking, they may be re-processed", "error", err)
	} else if len(commitmentMap) > 0 {
		ackEntries := make([]interfaces.CertificationRequestAck, 0, len(commitmentMap))
		for _, c := range commitmentMap {
			ackEntries = append(ackEntries, interfaces.CertificationRequestAck{
				StateID:  c.StateID,
				StreamID: c.StreamID,
			})
		}
		if err := commitmentQueue.MarkProcessed(ctx, ackEntries); err != nil {
			log.WithContext(ctx).Warn("Failed to ack commitments, they may be re-processed", "error", err)
		} else {
			log.WithContext(ctx).Info("Acked commitments", "count", len(ackEntries))
		}
	}

	return stateIDs, nil
}

func recoverMissingData(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	commitmentQueue interfaces.CommitmentQueue,
	blockNumber *api.BigInt,
	missingRecords []indexedStateID,
	missingSmtKeys []api.StateID,
) error {
	// Collect all needed state IDs.
	neededIDsMap := make(map[string]api.StateID, len(missingRecords)+len(missingSmtKeys))
	for _, missing := range missingRecords {
		neededIDsMap[missing.stateID.String()] = missing.stateID
	}
	for _, stateID := range missingSmtKeys {
		neededIDsMap[stateID.String()] = stateID
	}
	neededIDs := make([]api.StateID, 0, len(neededIDsMap))
	for _, stateID := range neededIDsMap {
		neededIDs = append(neededIDs, stateID)
	}

	// Fetch only the certification requests we need (streams in batches internally).
	commitmentMap, err := commitmentQueue.GetByStateIDs(ctx, neededIDs)
	if err != nil {
		return fmt.Errorf("failed to get commitments: %w", err)
	}

	if len(missingRecords) > 0 {
		var records []*models.AggregatorRecord
		for _, missing := range missingRecords {
			commitment, ok := commitmentMap[missing.stateID.String()]
			if !ok {
				existingRecord, err := storage.AggregatorRecordStorage().GetByStateID(ctx, missing.stateID)
				if err != nil {
					return fmt.Errorf("failed to check existing record: %w", err)
				}
				if existingRecord != nil {
					continue
				}
				return fmt.Errorf("FATAL: certification request not found for stateID %s", missing.stateID)
			}
			leafIndex := api.NewBigInt(nil)
			leafIndex.SetInt64(int64(missing.leafIndex))
			records = append(records, models.NewAggregatorRecord(commitment, blockNumber, leafIndex))
		}

		if len(records) > 0 {
			if err := storage.AggregatorRecordStorage().StoreBatch(ctx, records); err != nil {
				return fmt.Errorf("failed to store missing aggregator records: %w", err)
			}
			log.WithContext(ctx).Info("Stored missing aggregator records", "count", len(records))
		}
	}

	if len(missingSmtKeys) > 0 {
		var nodes []*models.SmtNode
		for _, stateID := range missingSmtKeys {
			commitment, ok := commitmentMap[stateID.String()]
			if !ok {
				keyBytes, err := stateID.GetTreeKey()
				if err != nil {
					return fmt.Errorf("failed to get SMT key for stateID: %w", err)
				}
				existingNode, err := storage.SmtStorage().GetByKey(ctx, keyBytes)
				if err != nil {
					return fmt.Errorf("failed to check existing SMT node: %w", err)
				}
				if existingNode != nil {
					continue
				}
				return fmt.Errorf("FATAL: certification request not found for SMT key %s", stateID)
			}

			keyBytes, err := commitment.StateID.GetTreeKey()
			if err != nil {
				return fmt.Errorf("failed to get SMT key for commitment: %w", err)
			}
			leafValue, err := commitment.LeafValue()
			if err != nil {
				return fmt.Errorf("failed to create leaf value: %w", err)
			}
			nodes = append(nodes, models.NewSmtNode(keyBytes, leafValue))
		}

		if len(nodes) > 0 {
			if err := storage.SmtStorage().StoreBatch(ctx, nodes); err != nil {
				return fmt.Errorf("failed to store missing SMT nodes: %w", err)
			}
			log.WithContext(ctx).Info("Stored missing SMT nodes", "count", len(nodes))
		}
	}

	return nil
}

// LoadRecoveredNodesIntoBackend loads SMT nodes for a recovered block into the active SMT backend.
// Used in HA mode when a follower becomes leader.
func LoadRecoveredNodesIntoBackend(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	backend smtbackend.Backend,
	blockNumber *api.BigInt,
	stateIDs []api.StateID,
) error {
	if backend == nil {
		return fmt.Errorf("SMT backend not initialized")
	}
	if len(stateIDs) == 0 {
		if blockNumber == nil {
			return nil
		}
		state, err := backend.CommittedState(ctx)
		if err != nil {
			return fmt.Errorf("failed to read SMT backend state: %w", err)
		}
		snapshot, err := backend.CreateSnapshot(ctx)
		if err != nil {
			return fmt.Errorf("failed to create SMT snapshot: %w", err)
		}
		if err := snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: blockNumber, RootHash: state.RootHash}); err != nil {
			return fmt.Errorf("failed to advance recovered SMT metadata: %w", err)
		}
		log.WithContext(ctx).Info("Advanced recovered SMT metadata", "blockNumber", blockNumber.String())
		return nil
	}

	log.WithContext(ctx).Info("Loading recovered SMT nodes", "count", len(stateIDs))

	// Deduplicate state IDs (duplicates can occur when the same certification
	// request is submitted twice in the same round).
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
			return fmt.Errorf("failed to get SMT key for stateID %s: %w", stateID, err)
		}
		keys = append(keys, api.HexBytes(keyBytes))
	}

	nodes, err := storage.SmtStorage().GetByKeys(ctx, keys)
	if err != nil {
		return fmt.Errorf("failed to get SMT nodes: %w", err)
	}

	if len(nodes) != len(keys) {
		return fmt.Errorf("FATAL: expected %d SMT nodes but found %d - data corruption", len(keys), len(nodes))
	}

	leaves := make([]smtbackend.LeafInput, len(nodes))
	for i, node := range nodes {
		leaves[i] = smtbackend.LeafInput{
			Key:   node.Key,
			Value: node.Value,
		}
	}

	snapshot, err := backend.CreateSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SMT snapshot: %w", err)
	}
	result, err := snapshot.AddLeavesClassified(ctx, leaves)
	if err != nil {
		return fmt.Errorf("failed to add recovered nodes to SMT: %w", err)
	}
	if err := result.ValidateAllAccepted(len(leaves)); err != nil {
		return fmt.Errorf("failed to add recovered nodes to SMT: %w", err)
	}
	if err := snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: blockNumber, RootHash: result.CandidateRoot}); err != nil {
		return fmt.Errorf("failed to commit recovered nodes to SMT: %w", err)
	}

	log.WithContext(ctx).Info("Loaded recovered SMT nodes", "count", len(nodes))
	return nil
}
func stateIDsToStrings(stateIDs []api.StateID) []string {
	result := make([]string, len(stateIDs))
	for i, stateID := range stateIDs {
		result[i] = stateID.String()
	}
	return result
}

// CleanupProcessedPendingCommitments ACKs pending commitments that are already in AggregatorRecords.
// This handles the case where a block was finalized but MarkProcessed failed (e.g., Redis was down).
func CleanupProcessedPendingCommitments(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	commitmentQueue interfaces.CommitmentQueue,
) error {
	pendingCommitments, err := commitmentQueue.GetAllPending(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending commitments: %w", err)
	}
	if len(pendingCommitments) == 0 {
		return nil
	}

	stateIDs := make([]string, len(pendingCommitments))
	for i, c := range pendingCommitments {
		stateIDs[i] = c.StateID.String()
	}

	existingIDs, err := storage.AggregatorRecordStorage().GetExistingStateIDs(ctx, stateIDs)
	if err != nil {
		return fmt.Errorf("failed to check existing records: %w", err)
	}

	var ackEntries []interfaces.CertificationRequestAck
	for _, c := range pendingCommitments {
		if existingIDs[c.StateID.String()] {
			ackEntries = append(ackEntries, interfaces.CertificationRequestAck{
				StateID:  c.StateID,
				StreamID: c.StreamID,
			})
		}
	}

	if len(ackEntries) > 0 {
		if err := commitmentQueue.MarkProcessed(ctx, ackEntries); err != nil {
			return fmt.Errorf("failed to ack processed commitments: %w", err)
		}
		log.WithContext(ctx).Info("Cleaned up already-processed pending commitments", "count", len(ackEntries))
	}

	return nil
}
