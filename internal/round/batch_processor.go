package round

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// processMiniBatch processes a small batch of commitments into the SMT for efficiency
// NOTE: The caller is expected to hold rm.roundMutex when calling this function
func (rm *RoundManager) processMiniBatch(ctx context.Context, commitments []*models.Commitment) error {
	if len(commitments) == 0 {
		return nil
	}

	// Convert commitments to SMT leaves
	leaves := make([]*smt.Leaf, 0, len(commitments))
	for _, commitment := range commitments {
		// Generate leaf path from requestID
		path, err := commitment.RequestID.GetPath()
		if err != nil {
			rm.logger.WithContext(ctx).Error("Failed to get path for commitment",
				"requestID", commitment.RequestID.String(),
				"error", err.Error())
			continue
		}

		// Create leaf value (hash of commitment data)
		leafValue, err := commitment.CreateLeafValue()
		if err != nil {
			rm.logger.WithContext(ctx).Error("Failed to create leaf value",
				"requestID", commitment.RequestID.String(),
				"error", err.Error())
			continue
		}

		leaves = append(leaves, &smt.Leaf{
			Path:  path,
			Value: leafValue,
		})
	}

	// Add leaves to the current round's SMT snapshot
	if rm.currentRound != nil && rm.currentRound.Snapshot != nil {
		_, err := rm.currentRound.Snapshot.AddLeaves(leaves)
		if err != nil {
			return fmt.Errorf("failed to add leaves to SMT snapshot: %w", err)
		}

		rm.currentRound.PendingLeaves = append(rm.currentRound.PendingLeaves, leaves...)
	}

	return nil
}

// ProposeBlock creates and proposes a new block with the given data
func (rm *RoundManager) proposeBlock(ctx context.Context, blockNumber *api.BigInt, rootHash string) error {
	rm.logger.WithContext(ctx).Info("proposeBlock called",
		"blockNumber", blockNumber.String(),
		"rootHash", rootHash)

	rm.roundMutex.Lock()
	if rm.currentRound != nil {
		rm.logger.WithContext(ctx).Debug("Changing round state to finalizing",
			"roundNumber", rm.currentRound.Number.String(),
			"previousState", rm.currentRound.State.String())
		rm.currentRound.State = RoundStateFinalizing
	}
	rm.roundMutex.Unlock()

	rm.logger.WithContext(ctx).Info("Creating block proposal",
		"blockNumber", blockNumber.String(),
		"rootHash", rootHash)

	// Get parent block hash
	var parentHash api.HexBytes
	if blockNumber.Cmp(big.NewInt(1)) > 0 {
		// Get previous block
		prevBlockNumber := api.NewBigInt(nil)
		prevBlockNumber.Set(blockNumber.Int)
		prevBlockNumber.Sub(prevBlockNumber.Int, big.NewInt(1))

		prevBlock, err := rm.storage.BlockStorage().GetByNumber(ctx, prevBlockNumber)
		if err != nil {
			return fmt.Errorf("failed to get previous block %s: %w", prevBlockNumber.String(), err)
		}
		if prevBlock != nil {
			// Use the block's root hash as the "hash" for now
			parentHash = prevBlock.RootHash
		}
	}

	// Create block (simplified for now)
	rootHashBytes, err := api.NewHexBytesFromString(rootHash)
	if err != nil {
		return fmt.Errorf("failed to parse root hash %s: %w", rootHash, err)
	}

	switch rm.config.Sharding.Mode {
	case config.ShardingModeStandalone:
		block := models.NewBlock(
			blockNumber,
			"unicity",
			0,
			"1.0",
			"mainnet",
			rootHashBytes,
			parentHash,
			nil,
			nil,
		)
		rm.logger.WithContext(ctx).Info("Sending certification request to BFT client",
			"blockNumber", blockNumber.String(),
			"bftClientType", fmt.Sprintf("%T", rm.bftClient))
		if err := rm.bftClient.CertificationRequest(ctx, block); err != nil {
			rm.logger.WithContext(ctx).Error("Failed to send certification request",
				"blockNumber", blockNumber.String(),
				"error", err.Error())
			return fmt.Errorf("failed to send certification request: %w", err)
		}
		rm.logger.WithContext(ctx).Info("Certification request sent successfully",
			"blockNumber", blockNumber.String())
		return nil
	case config.ShardingModeChild:
		rm.logger.WithContext(ctx).Info("Submitting root hash to parent shard", "rootHash", rootHash)

		// Strip algorithm prefix (first 2 bytes) before sending to parent
		// Parent SMT stores raw 32-byte hashes, not the full 34-byte format with algorithm ID
		// This is required for JoinPaths to work correctly when combining child and parent proofs
		if len(rootHashBytes) < 2 {
			return fmt.Errorf("root hash too short: expected at least 2 bytes for algorithm prefix, got %d", len(rootHashBytes))
		}
		rootHashRaw := rootHashBytes[2:] // Remove algorithm identifier
		if len(rootHashRaw) != 32 {
			return fmt.Errorf("child root hash has invalid length after stripping prefix: expected 32 bytes, got %d", len(rootHashRaw))
		}

		request := &api.SubmitShardRootRequest{
			ShardID:  rm.config.Sharding.Child.ShardID,
			RootHash: rootHashRaw,
		}
		if err := rm.rootClient.SubmitShardRoot(ctx, request); err != nil {
			return fmt.Errorf("failed to submit root hash to parent shard: %w", err)
		}
		rm.logger.WithContext(ctx).Info("Root hash submitted to parent, polling for inclusion proof...", "rootHash", rootHash)
		proof, err := rm.pollInclusionProof(ctx, rootHash)
		if err != nil {
			return fmt.Errorf("failed to poll for parent shard inclusion proof: %w", err)
		}
		block := models.NewBlock(
			blockNumber,
			"unicity",
			request.ShardID,
			"1.0",
			"mainnet",
			rootHashBytes,
			parentHash,
			proof.UnicityCertificate,
			proof.MerkleTreePath,
		)
		if err := rm.FinalizeBlock(ctx, block); err != nil {
			return fmt.Errorf("failed to finalize block: %w", err)
		}
		nextRoundNumber := big.NewInt(0).Add(blockNumber.Int, big.NewInt(1))
		if err := rm.StartNewRound(ctx, api.NewBigInt(nextRoundNumber)); err != nil {
			rm.logger.WithContext(ctx).Error("Failed to start new round after finalization.", "error", err.Error())
		}
		rm.logger.WithContext(ctx).Info("Block finalized and new round started", "blockNumber", blockNumber.String())
		return nil
	default:
		return fmt.Errorf("invalid sharding mode: %s", rm.config.Sharding.Mode)
	}
}

func (rm *RoundManager) pollInclusionProof(ctx context.Context, rootHash string) (*api.RootShardInclusionProof, error) {
	pollingCtx, cancel := context.WithTimeout(ctx, rm.config.Sharding.Child.ParentPollTimeout)
	defer cancel()

	ticker := time.NewTicker(rm.config.Sharding.Child.ParentPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pollingCtx.Done():
			return nil, fmt.Errorf("timed out waiting for parent shard inclusion proof %s", rootHash)
		case <-ticker.C:
			request := &api.GetShardProofRequest{ShardID: rm.config.Sharding.Child.ShardID}
			proof, err := rm.rootClient.GetShardProof(pollingCtx, request)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch parent shard inclusion proof: %w", err)
			}
			if proof == nil || !proof.IsValid() {
				rm.logger.WithContext(ctx).Debug("Parent shard inclusion proof not found, retrying...")
				continue
			}
			rm.logger.WithContext(ctx).Info("Successfully received shard proof from parent")
			return proof, nil
		}
	}
}

// FinalizeBlock creates and persists a new block with the given data
func (rm *RoundManager) FinalizeBlock(ctx context.Context, block *models.Block) error {
	rm.logger.WithContext(ctx).Info("FinalizeBlock called",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String(),
		"hasUnicityCertificate", block.UnicityCertificate != nil)

	finalizationStartTime := time.Now()
	var proposalTime time.Time
	var processingTime time.Duration
	var markProcessedStart, storeBlockStart, persistDataStart, commitSnapshotStart time.Time
	var markProcessedTime, storeBlockTime, persistDataTime, commitSnapshotTime time.Duration

	// Get timing metrics from the round if available
	rm.roundMutex.Lock()
	if rm.currentRound != nil && rm.currentRound.Number.String() == block.Index.String() {
		proposalTime = rm.currentRound.ProposalTime
		processingTime = rm.currentRound.ProcessingTime
	}
	rm.roundMutex.Unlock()

	// CRITICAL: Store all commitment data BEFORE storing the block to prevent race conditions
	// where API returns partial block data

	// First, collect all request IDs that will be in this block
	rm.roundMutex.Lock()
	requestIds := make([]api.RequestID, 0)
	if rm.currentRound != nil {
		requestIds = make([]api.RequestID, 0, len(rm.currentRound.Commitments))
		for _, commitment := range rm.currentRound.Commitments {
			requestIds = append(requestIds, commitment.RequestID)
		}
	}
	rm.roundMutex.Unlock()

	rm.roundMutex.Lock()
	if rm.currentRound != nil && len(rm.currentRound.Commitments) > 0 {
		rm.logger.WithContext(ctx).Debug("Preparing commitment data before block storage",
			"roundNumber", rm.currentRound.Number.String(),
			"commitmentCount", len(rm.currentRound.Commitments),
			"recordCount", len(rm.currentRound.PendingRecords))

		// Extract data we need
		requestIDs := make([]api.RequestID, len(rm.currentRound.Commitments))
		for i, commitment := range rm.currentRound.Commitments {
			requestIDs[i] = commitment.RequestID
		}
		rm.roundMutex.Unlock()

		// Note: Aggregator records are now stored during processBatch, not here
		rm.logger.WithContext(ctx).Debug("Aggregator records already stored during batch processing",
			"commitmentCount", len(requestIDs))

		// Mark commitments as processed BEFORE storing the block
		markProcessedStart = time.Now()
		if err := rm.commitmentQueue.MarkProcessed(ctx, requestIDs); err != nil {
			rm.logger.WithContext(ctx).Error("Failed to mark commitments as processed",
				"error", err.Error(),
				"blockNumber", block.Index.String())
			return fmt.Errorf("failed to mark commitments as processed: %w", err)
		}
		markProcessedTime = time.Since(markProcessedStart)

		rm.logger.WithContext(ctx).Info("Successfully prepared commitment data",
			"count", len(requestIDs),
			"blockNumber", block.Index.String())

	} else {
		rm.roundMutex.Unlock()
	}

	// Store the block first - before committing the snapshot
	rm.logger.WithContext(ctx).Debug("Storing block in database",
		"blockNumber", block.Index.String())

	storeBlockStart = time.Now()
	if err := rm.storage.BlockStorage().Store(ctx, block); err != nil {
		rm.logger.WithContext(ctx).Error("Failed to store block",
			"blockNumber", block.Index.String(),
			"error", err.Error())
		return fmt.Errorf("failed to store block: %w", err)
	}

	// Store block records mapping
	if err := rm.storage.BlockRecordsStorage().Store(ctx, models.NewBlockRecords(block.Index, requestIds)); err != nil {
		return fmt.Errorf("failed to store block record: %w", err)
	}
	storeBlockTime = time.Since(storeBlockStart)

	// Now that block is stored with unicity certificate, persist SMT nodes and aggregator records
	rm.roundMutex.Lock()
	var pendingLeaves []*smt.Leaf
	var commitments []*models.Commitment
	snapshot := rm.currentRound.Snapshot
	if rm.currentRound != nil {
		pendingLeaves = rm.currentRound.PendingLeaves
		commitments = rm.currentRound.Commitments
	}
	rm.roundMutex.Unlock()

	// Store SMT nodes and aggregator records if we have commitments
	if len(commitments) > 0 {
		persistDataStart = time.Now()
		var wg sync.WaitGroup
		var smtPersistErr, aggregatorRecordErr error

		// Only persist SMT nodes if we have pending leaves
		if len(pendingLeaves) > 0 {
			wg.Go(func() {
				smtPersistErr = rm.persistSmtNodes(ctx, pendingLeaves)
			})
		}

		// Always persist aggregator records if we have commitments
		wg.Go(func() {
			aggregatorRecordErr = rm.persistAggregatorRecords(ctx, commitments, block.Index)
		})

		wg.Wait()
		persistDataTime = time.Since(persistDataStart)

		if smtPersistErr != nil {
			return fmt.Errorf("failed to persist SMT nodes: %w", smtPersistErr)
		}
		if aggregatorRecordErr != nil {
			return fmt.Errorf("failed to store aggregator records: %w", aggregatorRecordErr)
		}

		rm.logger.WithContext(ctx).Info("Successfully persisted data",
			"blockNumber", block.Index.String(),
			"leafCount", len(pendingLeaves),
			"recordCount", len(commitments))
	}

	// CRITICAL: Commit the snapshot to the main SMT AFTER storing the block successfully
	// This ensures the SMT state only reflects successfully persisted blocks

	if snapshot != nil {
		commitSnapshotStart = time.Now()
		rm.logger.WithContext(ctx).Info("Committing snapshot to main SMT after successful block storage",
			"blockNumber", block.Index.String())

		snapshot.Commit(rm.smt)
		commitSnapshotTime = time.Since(commitSnapshotStart)

		rm.logger.WithContext(ctx).Info("Successfully committed snapshot to main SMT",
			"blockNumber", block.Index.String())
	}

	// Update current round with finalized block
	rm.roundMutex.Lock()
	if rm.currentRound != nil {
		rm.currentRound.Block = block
		// Clear pending data as it's now finalized
		rm.currentRound.PendingRecords = nil
		rm.currentRound.PendingRootHash = ""
		rm.currentRound.PendingLeaves = nil
		// Clear the snapshot as it has been committed to the main SMT
		rm.currentRound.Snapshot = nil
	}
	rm.roundMutex.Unlock()

	// Calculate actual finalization metrics
	actualFinalizationTime := time.Since(finalizationStartTime)
	var totalRoundTime time.Duration
	var bftWaitTime time.Duration

	if !proposalTime.IsZero() {
		// Calculate time from proposal to actual finalization
		bftWaitTime = finalizationStartTime.Sub(proposalTime)

		// Update the average finalization time with the actual measurement
		rm.avgFinalizationTime = (rm.avgFinalizationTime*4 + actualFinalizationTime + bftWaitTime) / 5

		// Calculate total round time (processing + waiting for UC + finalization)
		totalRoundTime = processingTime + bftWaitTime + actualFinalizationTime

		rm.logger.WithContext(ctx).Debug("Finalization timing metrics",
			"blockNumber", block.Index.String(),
			"processingTime", processingTime,
			"proposalToUCTime", bftWaitTime,
			"finalizationTime", actualFinalizationTime,
			"totalRoundTime", totalRoundTime,
			"avgFinalizationTime", rm.avgFinalizationTime)
	}

	// Get commitment count for performance summary
	rm.roundMutex.RLock()
	commitmentCount := 0
	if rm.currentRound != nil {
		commitmentCount = len(rm.currentRound.Commitments)
	}
	rm.roundMutex.RUnlock()

	// Log comprehensive performance summary
	rm.logger.WithContext(ctx).Info("PERF: Round completed",
		"blockNumber", block.Index.String(),
		"commitments", commitmentCount,
		"totalRoundTime", totalRoundTime.String(),
		"processingTime", processingTime.String(),
		"bftWaitTime", bftWaitTime.String(),
		"finalizationTime", actualFinalizationTime.String(),
		"markProcessedTime", markProcessedTime.String(),
		"storeBlockTime", storeBlockTime.String(),
		"persistDataTime", persistDataTime.String(),
		"commitSnapshotTime", commitSnapshotTime.String())

	rm.logger.WithContext(ctx).Info("Block finalized and stored successfully",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String())

	rm.stateTracker.SetLastSyncedBlock(block.Index.Int)

	return nil
}

// persistSmtNodes persists SMT leaves to storage for permanent retention
func (rm *RoundManager) persistSmtNodes(ctx context.Context, leaves []*smt.Leaf) error {
	if len(leaves) == 0 {
		return nil
	}

	// Convert SMT leaves to storage models
	smtNodes := make([]*models.SmtNode, len(leaves))
	for i, leaf := range leaves {
		// Create the key from the path (convert big.Int to bytes)
		keyBytes := leaf.Path.Bytes()
		key := api.NewHexBytes(keyBytes)

		// Create the value
		value := api.NewHexBytes(leaf.Value)

		smtNodes[i] = models.NewSmtNode(key, value)
	}

	// Store batch to database
	if err := rm.storage.SmtStorage().StoreBatch(ctx, smtNodes); err != nil {
		return fmt.Errorf("failed to store SMT nodes batch: %w", err)
	}

	return nil
}

// persistAggregatorRecords generates aggregator records and stores them to database
func (rm *RoundManager) persistAggregatorRecords(ctx context.Context, commitments []*models.Commitment, blockIndex *api.BigInt) error {
	if len(commitments) == 0 {
		return nil
	}

	records := make([]*models.AggregatorRecord, 0, len(commitments))

	for i, commitment := range commitments {
		leafIndex := api.NewBigInt(big.NewInt(int64(i)))
		records = append(records, models.NewAggregatorRecord(commitment, blockIndex, leafIndex))
	}

	if err := rm.storage.AggregatorRecordStorage().StoreBatch(ctx, records); err != nil {
		return fmt.Errorf("failed to store aggregator records batch: %w", err)
	}

	rm.logger.WithContext(ctx).Debug("Successfully stored aggregator records batch",
		"recordCount", len(records))
	return nil
}
