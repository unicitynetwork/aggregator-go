package round

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
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
			rm.config.Chain.ID,
			0,
			rm.config.Chain.Version,
			rm.config.Chain.ForkID,
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
		submitStart := time.Now()
		if err := rm.submitShardRootWithRetry(ctx, request); err != nil {
			return fmt.Errorf("failed to submit root hash to parent shard: %w", err)
		}
		submissionDuration := time.Since(submitStart)
		rm.logger.WithContext(ctx).Info("Root hash submitted to parent, polling for inclusion proof...",
			"rootHash", rootHashRaw.String(),
			"submissionDuration", submissionDuration)

		proofWaitStart := time.Now()
		proof, err := rm.pollInclusionProof(ctx, rootHashRaw.String())
		if err != nil {
			return fmt.Errorf("failed to poll for parent shard inclusion proof: %w", err)
		}
		proofWait := time.Since(proofWaitStart)
		rm.logger.WithContext(ctx).Info("Parent shard proof received",
			"rootHash", rootHashRaw.String(),
			"proofWait", proofWait,
			"submissionToProof", submissionDuration+proofWait)
		rm.roundMutex.Lock()
		if rm.currentRound != nil {
			rm.currentRound.SubmissionDuration = submissionDuration
			rm.currentRound.ProofWaitDuration = proofWait
		}
		rm.roundMutex.Unlock()
		block := models.NewBlock(
			blockNumber,
			rm.config.Chain.ID,
			request.ShardID,
			rm.config.Chain.Version,
			rm.config.Chain.ForkID,
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
			if proof == nil || !proof.IsValid(rootHash) {
				rm.logger.WithContext(ctx).Debug("Parent shard inclusion proof not found, retrying...")
				continue
			}
			return proof, nil
		}
	}
}

func (rm *RoundManager) submitShardRootWithRetry(ctx context.Context, req *api.SubmitShardRootRequest) error {
	if rm.rootClient == nil {
		return fmt.Errorf("root client not configured")
	}

	var attempt int
	for {
		attempt++
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := rm.rootClient.SubmitShardRoot(ctx, req); err == nil {
			if attempt > 1 {
				rm.logger.WithContext(ctx).Info("Shard root submission succeeded after retries",
					"attempt", attempt)
			}
			return nil
		} else {
			rm.logger.WithContext(ctx).Warn("Failed to submit shard root to parent, retrying...",
				"attempt", attempt,
				"error", err.Error())
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
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
	commitmentCount := 0

	// Get timing metrics from the round if available
	rm.roundMutex.Lock()
	if rm.currentRound != nil && rm.currentRound.Number.String() == block.Index.String() {
		proposalTime = rm.currentRound.ProposalTime
		processingTime = rm.currentRound.ProcessingTime
	}
	rm.roundMutex.Unlock()

	// CRITICAL: Store all commitment data BEFORE storing the block to prevent race conditions
	// where API returns partial block data

	// First, collect all request IDs and stream metadata that will be in this block
	rm.roundMutex.Lock()
	requestIds := make([]api.RequestID, 0)
	ackEntries := make([]interfaces.CommitmentAck, 0)
	pendingRecordCount := 0
	roundNumber := block.Index.String()
	var proofTimes []time.Duration

	if rm.currentRound != nil {
		if rm.currentRound.Number != nil {
			roundNumber = rm.currentRound.Number.String()
		}
		commitmentCount = len(rm.currentRound.Commitments)
		pendingRecordCount = len(rm.currentRound.PendingRecords)

		requestIds = make([]api.RequestID, commitmentCount)
		ackEntries = make([]interfaces.CommitmentAck, commitmentCount)

		now := time.Now()
		for i, commitment := range rm.currentRound.Commitments {
			requestIds[i] = commitment.RequestID
			ackEntries[i] = interfaces.CommitmentAck{RequestID: commitment.RequestID, StreamID: commitment.StreamID}

			if commitment.CreatedAt != nil {
				proofReadyTime := now.Sub(commitment.CreatedAt.Time)
				if proofReadyTime > 0 {
					proofTimes = append(proofTimes, proofReadyTime)
				}
			}
		}
	}
	rm.roundMutex.Unlock()

	// Get pending data before transaction
	rm.roundMutex.Lock()
	var pendingLeaves []*smt.Leaf
	var commitments []*models.Commitment
	snapshot := rm.currentRound.Snapshot
	if rm.currentRound != nil {
		pendingLeaves = rm.currentRound.PendingLeaves
		commitments = rm.currentRound.Commitments
	}
	rm.roundMutex.Unlock()

	// Use MongoDB transaction to atomically store block, block records, SMT nodes, and aggregator records
	// This prevents data inconsistency if MongoDB crashes during finalization
	storeBlockStart = time.Now()
	persistDataStart = time.Now()

	err := rm.storage.WithTransaction(ctx, func(txCtx context.Context) error {
		// 1. Store block
		rm.logger.WithContext(txCtx).Debug("Storing block in database",
			"blockNumber", block.Index.String())
		if err := rm.storage.BlockStorage().Store(txCtx, block); err != nil {
			rm.logger.WithContext(txCtx).Error("Failed to store block",
				"blockNumber", block.Index.String(),
				"error", err.Error())
			return fmt.Errorf("failed to store block: %w", err)
		}

		// 2. Store block records mapping
		if err := rm.storage.BlockRecordsStorage().Store(txCtx, models.NewBlockRecords(block.Index, requestIds)); err != nil {
			return fmt.Errorf("failed to store block record: %w", err)
		}

		// 3. Store SMT nodes and aggregator records if we have commitments
		if len(commitments) > 0 {
			var wg sync.WaitGroup
			var smtPersistErr, aggregatorRecordErr error

			// Persist SMT nodes if we have pending leaves
			if len(pendingLeaves) > 0 {
				wg.Go(func() {
					smtPersistErr = rm.persistSmtNodes(txCtx, pendingLeaves)
				})
			}

			// Persist aggregator records
			wg.Go(func() {
				aggregatorRecordErr = rm.persistAggregatorRecords(txCtx, commitments, block.Index)
			})

			wg.Wait()

			if smtPersistErr != nil {
				return fmt.Errorf("failed to persist SMT nodes: %w", smtPersistErr)
			}
			if aggregatorRecordErr != nil {
				return fmt.Errorf("failed to store aggregator records: %w", aggregatorRecordErr)
			}

			rm.logger.WithContext(txCtx).Info("Successfully persisted data in transaction",
				"blockNumber", block.Index.String(),
				"leafCount", len(pendingLeaves),
				"recordCount", len(commitments))
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to finalize block in transaction: %w", err)
	}

	storeBlockTime = time.Since(storeBlockStart)
	persistDataTime = time.Since(persistDataStart)

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

	// After all data is safely persisted and snapshot committed, mark commitments processed
	if len(ackEntries) > 0 {
		rm.logger.WithContext(ctx).Debug("Marking commitments as processed",
			"roundNumber", roundNumber,
			"commitmentCount", len(ackEntries),
			"recordCount", pendingRecordCount)

		markProcessedStart = time.Now()

		if err := rm.commitmentQueue.MarkProcessed(ctx, ackEntries); err != nil {
			rm.logger.WithContext(ctx).Error("Failed to mark commitments as processed",
				"error", err.Error(),
				"blockNumber", block.Index.String())
			return fmt.Errorf("failed to mark commitments as processed: %w", err)
		}
		markProcessedTime = time.Since(markProcessedStart)

		rm.logger.WithContext(ctx).Info("Successfully marked commitments as processed",
			"count", len(ackEntries),
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
	finalizationWorkDuration := markProcessedTime + storeBlockTime + persistDataTime + commitSnapshotTime
	var totalRoundTime time.Duration
	var bftWaitTime time.Duration
	if !proposalTime.IsZero() {
		bftWaitTime = finalizationStartTime.Sub(proposalTime)
		rm.avgFinalizationTime = (rm.avgFinalizationTime*4 + actualFinalizationTime + bftWaitTime) / 5
		totalRoundTime = processingTime + bftWaitTime + actualFinalizationTime
	}

	// Get commitment count for performance summary
	rm.roundMutex.RLock()
	commitmentCount = 0
	proofWaitDuration := time.Duration(0)
	if rm.currentRound != nil {
		commitmentCount = len(rm.currentRound.Commitments)
		proofWaitDuration = rm.currentRound.ProofWaitDuration
	}
	rm.roundMutex.RUnlock()

	if totalRoundTime == 0 {
		totalRoundTime = processingTime + actualFinalizationTime
	}

	// For child shards, ensure minimum round duration
	// This prevents rounds from completing too quickly when there are no commitments
	if rm.config.Sharding.Mode.IsChild() {
		rm.roundMutex.RLock()
		roundStartTime := rm.currentRound.StartTime
		rm.roundMutex.RUnlock()

		elapsed := time.Since(roundStartTime)
		minRoundDuration := rm.roundDuration
		if elapsed < minRoundDuration {
			delay := minRoundDuration - elapsed
			time.Sleep(delay)
			// Recalculate totalRoundTime to include the delay
			totalRoundTime = time.Since(roundStartTime)
		}
	}

	shortDur := func(d time.Duration) string {
		if d <= 0 {
			return "0ms"
		}
		return fmt.Sprintf("%dms", d.Milliseconds())
	}

	logFields := []interface{}{
		"block", block.Index.String(),
		"commitments", commitmentCount,
		"roundTime", shortDur(totalRoundTime),
		"processing", shortDur(processingTime),
		"bftWait", shortDur(bftWaitTime),
		"finalization", shortDur(finalizationWorkDuration),
	}

	if len(proofTimes) > 0 {
		sorted := make([]time.Duration, len(proofTimes))
		copy(sorted, proofTimes)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i] < sorted[j]
		})
		median := sorted[len(sorted)/2]
		p95 := sorted[len(sorted)*95/100]
		p99 := sorted[len(sorted)*99/100]

		logFields = append(logFields,
			"proofReadyMedian", shortDur(median),
			"proofReadyP95", shortDur(p95),
			"proofReadyP99", shortDur(p99),
		)
	}

	redisTotal, _ := rm.commitmentQueue.Count(ctx)
	redisPending, _ := rm.commitmentQueue.CountUnprocessed(ctx)

	logFields = append(logFields,
		"redisTotal", redisTotal,
		"redisPending", redisPending,
	)
	if proofWaitDuration > 0 {
		logFields = append(logFields,
			"proofWait", shortDur(proofWaitDuration),
		)
	}

	rm.logger.WithContext(ctx).Info("PERF: Round completed", logFields...)

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
