package round

import (
	"context"
	"fmt"
	"math/big"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type RecoveryResult struct {
	Recovered   bool
	BlockNumber *api.BigInt
	RequestIDs  []api.RequestID
}

// indexedRequestID tracks a request ID with its original position in the block
type indexedRequestID struct {
	reqID     api.RequestID
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

	requestIDs, err := recoverBlock(ctx, log, storage, commitmentQueue, block)
	if err != nil {
		return nil, fmt.Errorf("failed to recover block %s: %w", block.Index.String(), err)
	}

	log.WithContext(ctx).Info("Block recovery completed successfully", "blockNumber", block.Index.String())
	return &RecoveryResult{
		Recovered:   true,
		BlockNumber: block.Index,
		RequestIDs:  requestIDs,
	}, nil
}

func recoverBlock(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	commitmentQueue interfaces.CommitmentQueue,
	block *models.Block,
) ([]api.RequestID, error) {
	blockRecords, err := storage.BlockRecordsStorage().GetByBlockNumber(ctx, block.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to get block records: %w", err)
	}
	if blockRecords == nil {
		return nil, fmt.Errorf("FATAL: block records not found for block %s", block.Index.String())
	}

	requestIDs := blockRecords.RequestIDs
	log.WithContext(ctx).Info("Block records found", "requestCount", len(requestIDs))

	existingRecordIDs, err := storage.AggregatorRecordStorage().GetExistingRequestIDs(ctx, requestIDsToStrings(requestIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to check existing records: %w", err)
	}

	smtKeyStrings := make([]string, len(requestIDs))
	for i, reqID := range requestIDs {
		path, err := reqID.GetPath()
		if err != nil {
			return nil, fmt.Errorf("failed to get path for requestID: %w", err)
		}
		smtKeyStrings[i] = api.HexBytes(path.Bytes()).String()
	}
	existingSmtKeys, err := storage.SmtStorage().GetExistingKeys(ctx, smtKeyStrings)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing SMT nodes: %w", err)
	}

	var missingRecords []indexedRequestID
	var missingSmtKeys []api.RequestID
	for i, reqID := range requestIDs {
		if !existingRecordIDs[string(reqID)] {
			missingRecords = append(missingRecords, indexedRequestID{reqID: reqID, leafIndex: i})
		}
		if !existingSmtKeys[smtKeyStrings[i]] {
			missingSmtKeys = append(missingSmtKeys, reqID)
		}
	}

	log.WithContext(ctx).Info("Recovery status",
		"totalRequestIDs", len(requestIDs),
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

	// Ack commitments in Redis - fetch only the ones we need by request ID
	commitmentMap, err := commitmentQueue.GetByRequestIDs(ctx, requestIDs)
	if err != nil {
		log.WithContext(ctx).Warn("Failed to get commitments for acking, they may be re-processed", "error", err)
	} else if len(commitmentMap) > 0 {
		ackEntries := make([]interfaces.CommitmentAck, 0, len(commitmentMap))
		for _, c := range commitmentMap {
			ackEntries = append(ackEntries, interfaces.CommitmentAck{
				RequestID: c.RequestID,
				StreamID:  c.StreamID,
			})
		}
		if err := commitmentQueue.MarkProcessed(ctx, ackEntries); err != nil {
			log.WithContext(ctx).Warn("Failed to ack commitments, they may be re-processed", "error", err)
		} else {
			log.WithContext(ctx).Info("Acked commitments", "count", len(ackEntries))
		}
	}

	return requestIDs, nil
}

func recoverMissingData(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	commitmentQueue interfaces.CommitmentQueue,
	blockNumber *api.BigInt,
	missingRecords []indexedRequestID,
	missingSmtKeys []api.RequestID,
) error {
	// Collect all needed request IDs
	neededIDsMap := make(map[string]api.RequestID, len(missingRecords)+len(missingSmtKeys))
	for _, missing := range missingRecords {
		neededIDsMap[string(missing.reqID)] = missing.reqID
	}
	for _, reqID := range missingSmtKeys {
		neededIDsMap[string(reqID)] = reqID
	}
	neededIDs := make([]api.RequestID, 0, len(neededIDsMap))
	for _, reqID := range neededIDsMap {
		neededIDs = append(neededIDs, reqID)
	}

	// Fetch only the commitments we need (streams in batches internally)
	commitmentMap, err := commitmentQueue.GetByRequestIDs(ctx, neededIDs)
	if err != nil {
		return fmt.Errorf("failed to get commitments: %w", err)
	}

	if len(missingRecords) > 0 {
		var records []*models.AggregatorRecord
		for _, missing := range missingRecords {
			commitment, ok := commitmentMap[string(missing.reqID)]
			if !ok {
				existingRecord, err := storage.AggregatorRecordStorage().GetByRequestID(ctx, missing.reqID)
				if err != nil {
					return fmt.Errorf("failed to check existing record: %w", err)
				}
				if existingRecord != nil {
					continue
				}
				return fmt.Errorf("FATAL: commitment not found for requestID %s", missing.reqID)
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
		for _, reqID := range missingSmtKeys {
			commitment, ok := commitmentMap[string(reqID)]
			if !ok {
				path, err := reqID.GetPath()
				if err != nil {
					return fmt.Errorf("failed to get path for reqID: %w", err)
				}
				existingNode, err := storage.SmtStorage().GetByKey(ctx, api.HexBytes(path.Bytes()))
				if err != nil {
					return fmt.Errorf("failed to check existing SMT node: %w", err)
				}
				if existingNode != nil {
					continue
				}
				return fmt.Errorf("FATAL: commitment not found for SMT key %s", reqID)
			}

			path, err := commitment.RequestID.GetPath()
			if err != nil {
				return fmt.Errorf("failed to get path for commitment: %w", err)
			}
			leafValue, err := commitment.CreateLeafValue()
			if err != nil {
				return fmt.Errorf("failed to create leaf value: %w", err)
			}
			nodes = append(nodes, models.NewSmtNode(api.HexBytes(path.Bytes()), leafValue))
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

// LoadRecoveredNodesIntoSMT loads SMT nodes for a recovered block into the in-memory SMT.
// Used in HA mode when follower becomes leader.
func LoadRecoveredNodesIntoSMT(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	smtTree *smt.ThreadSafeSMT,
	requestIDs []api.RequestID,
) error {
	if len(requestIDs) == 0 {
		return nil
	}

	log.WithContext(ctx).Info("Loading recovered SMT nodes", "count", len(requestIDs))

	keys := make([]api.HexBytes, len(requestIDs))
	for i, reqID := range requestIDs {
		path, err := reqID.GetPath()
		if err != nil {
			return fmt.Errorf("failed to get path for requestID %s: %w", reqID, err)
		}
		keys[i] = api.HexBytes(path.Bytes())
	}

	nodes, err := storage.SmtStorage().GetByKeys(ctx, keys)
	if err != nil {
		return fmt.Errorf("failed to get SMT nodes: %w", err)
	}

	leaves := make([]*smt.Leaf, len(nodes))
	for i, node := range nodes {
		path := new(big.Int).SetBytes(node.Key)
		leaves[i] = &smt.Leaf{
			Path:  path,
			Value: node.Value,
		}
	}

	if _, err := smtTree.AddLeaves(leaves); err != nil {
		return fmt.Errorf("failed to add recovered nodes to SMT: %w", err)
	}

	log.WithContext(ctx).Info("Loaded recovered SMT nodes", "count", len(nodes))
	return nil
}
func requestIDsToStrings(requestIDs []api.RequestID) []string {
	result := make([]string, len(requestIDs))
	for i, reqID := range requestIDs {
		result[i] = string(reqID)
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

	requestIDs := make([]string, len(pendingCommitments))
	for i, c := range pendingCommitments {
		requestIDs[i] = string(c.RequestID)
	}

	existingIDs, err := storage.AggregatorRecordStorage().GetExistingRequestIDs(ctx, requestIDs)
	if err != nil {
		return fmt.Errorf("failed to check existing records: %w", err)
	}

	var ackEntries []interfaces.CommitmentAck
	for _, c := range pendingCommitments {
		if existingIDs[string(c.RequestID)] {
			ackEntries = append(ackEntries, interfaces.CommitmentAck{
				RequestID: c.RequestID,
				StreamID:  c.StreamID,
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
