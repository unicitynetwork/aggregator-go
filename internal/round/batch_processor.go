package round

import (
	"context"
	"errors"
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
func (rm *RoundManager) processMiniBatch(ctx context.Context, commitments []*models.CertificationRequest) error {
	if len(commitments) == 0 {
		return nil
	}

	// Convert commitments to SMT leaves
	leaves := make([]*smt.Leaf, 0, len(commitments))
	for _, commitment := range commitments {
		// Generate leaf path from stateID
		path, err := commitment.StateID.GetPath()
		if err != nil {
			rm.logger.WithContext(ctx).Error("Failed to get path for commitment",
				"stateID", commitment.StateID.String(),
				"error", err.Error())
			continue
		}

		// Create leaf value (hash of certification request data)
		leafValue, err := commitment.LeafValue()
		if err != nil {
			rm.logger.WithContext(ctx).Error("Failed to create leaf value",
				"stateID", commitment.StateID.String(),
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

		var proof *api.RootShardInclusionProof
		proofWaitStart := time.Now()

		// Initialize pre-collection: create chained snapshot for next round
		rm.initPreCollection(ctx)

		// Poll for proof while pre-collecting commitments for next round
		proof, err = rm.pollWithPreCollection(ctx, rootHashRaw.String())
		if err != nil {
			rm.clearPreCollection()
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
		if err := rm.FinalizeBlockWithRetry(ctx, block); err != nil {
			rm.clearPreCollection()
			return fmt.Errorf("failed to finalize block after retries: %w", err)
		}

		nextRoundNumber := big.NewInt(0).Add(blockNumber.Int, big.NewInt(1))

		rm.preCollectionMutex.Lock()
		preSnapshot := rm.preCollectionSnapshot
		preCommitments := rm.preCollectedCommitments
		preLeaves := rm.preCollectedLeaves
		rm.preCollectionSnapshot = nil
		rm.preCollectedCommitments = nil
		rm.preCollectedLeaves = nil
		rm.preCollectionMutex.Unlock()

		if preSnapshot != nil && len(preCommitments) > 0 {
			preSnapshot.SetCommitTarget(rm.smt)
			_ = rm.StartNewRoundWithSnapshot(ctx, api.NewBigInt(nextRoundNumber), preSnapshot, preCommitments, preLeaves)
		} else {
			if err := rm.StartNewRound(ctx, api.NewBigInt(nextRoundNumber)); err != nil {
				rm.logger.WithContext(ctx).Error("Failed to start new round after finalization.", "error", err.Error())
			}
		}

		rm.logger.WithContext(ctx).Info("Block finalized and new round started", "blockNumber", blockNumber.String())
		return nil
	default:
		return fmt.Errorf("invalid sharding mode: %s", rm.config.Sharding.Mode)
	}
}

// pollWithPreCollection polls for parent proof while pre-collecting commitments for next round.
func (rm *RoundManager) pollWithPreCollection(ctx context.Context, rootHash string) (*api.RootShardInclusionProof, error) {
	pollingCtx, cancel := context.WithTimeout(ctx, rm.config.Sharding.Child.ParentPollTimeout)
	defer cancel()

	ticker := time.NewTicker(rm.config.Sharding.Child.ParentPollInterval)
	defer ticker.Stop()

	preCollectCount := 0
	maxPreCollect := rm.config.Processing.MaxCommitmentsPerRound
	if maxPreCollect <= 0 {
		maxPreCollect = 10000
	}

	// Buffer for batching pre-collection
	pendingCommitments := make([]*models.CertificationRequest, 0, miniBatchSize)
	flushBatch := func() {
		if len(pendingCommitments) == 0 {
			return
		}
		if err := rm.addBatchToPreCollection(ctx, pendingCommitments); err != nil {
			rm.logger.WithContext(ctx).Error("Failed to add batch to pre-collection", "error", err.Error())
			return // Don't clear - will retry on next flush
		}
		preCollectCount += len(pendingCommitments)
		pendingCommitments = pendingCommitments[:0]
	}

	pollStart := time.Now()

	for {
		select {
		case <-pollingCtx.Done():
			flushBatch()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			rm.logger.WithContext(ctx).Warn("Poll with pre-collection timed out",
				"rootHash", rootHash,
				"pollDuration", time.Since(pollStart))
			return nil, fmt.Errorf("timed out waiting for parent shard inclusion proof %s", rootHash)

		case <-ticker.C:
			flushBatch()
			request := &api.GetShardProofRequest{ShardID: rm.config.Sharding.Child.ShardID}
			proof, err := rm.rootClient.GetShardProof(pollingCtx, request)
			if err != nil {
				rm.logger.WithContext(ctx).Warn("Failed to fetch parent shard inclusion proof, retrying",
					"rootHash", rootHash,
					"error", err.Error())
				continue
			}
			if proof == nil || !proof.IsValid(rootHash) {
				continue
			}
			return proof, nil

		case commitment := <-rm.commitmentStream:
			if preCollectCount+len(pendingCommitments) < maxPreCollect {
				pendingCommitments = append(pendingCommitments, commitment)
				if len(pendingCommitments) >= miniBatchSize {
					flushBatch()
				}
			}
		}
	}
}

// addToPreCollection adds a single commitment (convenience wrapper for tests)
func (rm *RoundManager) addToPreCollection(ctx context.Context, commitment *models.CertificationRequest) error {
	return rm.addBatchToPreCollection(ctx, []*models.CertificationRequest{commitment})
}

func (rm *RoundManager) addBatchToPreCollection(ctx context.Context, commitments []*models.CertificationRequest) error {
	if len(commitments) == 0 {
		return nil
	}

	leaves := make([]*smt.Leaf, 0, len(commitments))
	validCommitments := make([]*models.CertificationRequest, 0, len(commitments))

	for _, commitment := range commitments {
		path, err := commitment.StateID.GetPath()
		if err != nil {
			rm.logger.WithContext(ctx).Error("Failed to get path for commitment",
				"requestID", commitment.StateID.String(), "error", err.Error())
			continue
		}

		leafValue, err := commitment.LeafValue()
		if err != nil {
			rm.logger.WithContext(ctx).Error("Failed to create leaf value",
				"requestID", commitment.StateID.String(), "error", err.Error())
			continue
		}

		leaves = append(leaves, &smt.Leaf{Path: path, Value: leafValue})
		validCommitments = append(validCommitments, commitment)
	}

	if len(leaves) == 0 {
		return nil
	}

	rm.preCollectionMutex.Lock()
	defer rm.preCollectionMutex.Unlock()

	if rm.preCollectionSnapshot == nil {
		return fmt.Errorf("pre-collection snapshot not initialized")
	}

	if _, err := rm.preCollectionSnapshot.AddLeaves(leaves); err != nil {
		// Fall back to adding leaves one by one, skipping bad ones
		rm.addLeavesOneByOne(ctx, leaves, validCommitments)
		return nil
	}

	rm.preCollectedCommitments = append(rm.preCollectedCommitments, validCommitments...)
	rm.preCollectedLeaves = append(rm.preCollectedLeaves, leaves...)

	return nil
}

// addLeavesOneByOne adds leaves individually, skipping any that fail.
// Must be called with preCollectionMutex held.
func (rm *RoundManager) addLeavesOneByOne(ctx context.Context, leaves []*smt.Leaf, commitments []*models.CertificationRequest) {
	var rejected []interfaces.CertificationRequestAck
	for i, leaf := range leaves {
		if err := rm.preCollectionSnapshot.AddLeaf(leaf.Path, leaf.Value); err != nil {
			rm.logger.WithContext(ctx).Warn("Rejected commitment during pre-collection",
				"requestID", commitments[i].StateID.String(),
				"error", err.Error())
			rejected = append(rejected, interfaces.CertificationRequestAck{
				StateID: commitments[i].StateID,
				StreamID:  commitments[i].StreamID,
			})
			continue
		}
		rm.preCollectedCommitments = append(rm.preCollectedCommitments, commitments[i])
		rm.preCollectedLeaves = append(rm.preCollectedLeaves, leaf)
	}

	if len(rejected) == 0 {
		return
	}

	if rm.commitmentQueue == nil {
		rm.logger.WithContext(ctx).Warn("Commitment queue not configured, rejected commitments not marked processed",
			"count", len(rejected))
		return
	}

	if err := rm.commitmentQueue.MarkProcessed(ctx, rejected); err != nil {
		rm.logger.WithContext(ctx).Error("Failed to mark rejected commitments as processed",
			"count", len(rejected),
			"error", err.Error())
	}
}

func (rm *RoundManager) initPreCollection(ctx context.Context) {
	rm.preCollectionMutex.Lock()
	defer rm.preCollectionMutex.Unlock()

	rm.roundMutex.RLock()
	currentSnapshot := rm.currentRound.Snapshot
	rm.roundMutex.RUnlock()

	if currentSnapshot == nil {
		rm.logger.WithContext(ctx).Warn("Cannot initialize pre-collection: current round has no snapshot")
		return
	}

	rm.preCollectionSnapshot = currentSnapshot.CreateSnapshot()
	rm.preCollectedCommitments = make([]*models.CertificationRequest, 0)
	rm.preCollectedLeaves = make([]*smt.Leaf, 0)
}

func (rm *RoundManager) clearPreCollection() {
	rm.preCollectionMutex.Lock()
	defer rm.preCollectionMutex.Unlock()

	rm.preCollectionSnapshot = nil
	rm.preCollectedCommitments = nil
	rm.preCollectedLeaves = nil
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

const (
	maxFinalizeRetries = 3
	finalizeRetryDelay = 1000 * time.Millisecond
)

// FinalizeBlockWithRetry retries finalization and uses recovery if block was partially stored.
func (rm *RoundManager) FinalizeBlockWithRetry(ctx context.Context, block *models.Block) error {
	for attempt := 1; attempt <= maxFinalizeRetries; attempt++ {
		err := rm.FinalizeBlock(ctx, block)
		if err == nil {
			return nil
		}

		rm.logger.Error("FinalizeBlock failed",
			"attempt", attempt,
			"maxAttempts", maxFinalizeRetries,
			"blockNumber", block.Index.String(),
			"error", err.Error())

		unfinalizedBlocks, checkErr := rm.storage.BlockStorage().GetUnfinalized(ctx)
		if checkErr != nil {
			rm.logger.Error("Failed to check for unfinalized blocks", "error", checkErr.Error())
		} else if len(unfinalizedBlocks) > 0 {
			rm.logger.Info("Found unfinalized block, attempting recovery",
				"blockNumber", unfinalizedBlocks[0].Index.String())
			_, recoverErr := RecoverUnfinalizedBlock(ctx, rm.logger, rm.storage, rm.commitmentQueue)
			if recoverErr != nil {
				return fmt.Errorf("recovery failed: %w", recoverErr)
			}
			rm.logger.Info("Recovery completed successfully")
			return nil
		}

		if attempt < maxFinalizeRetries {
			rm.logger.Info("Retrying FinalizeBlock", "attempt", attempt)
			time.Sleep(finalizeRetryDelay)
		}
	}
	return fmt.Errorf("FinalizeBlock failed after %d attempts", maxFinalizeRetries)
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
	var markProcessedStart, persistDataStart, commitSnapshotStart time.Time
	var markProcessedTime, persistDataTime, commitSnapshotTime time.Duration
	commitmentCount := 0

	rm.roundMutex.Lock()
	if rm.currentRound != nil && rm.currentRound.Number.String() == block.Index.String() {
		proposalTime = rm.currentRound.ProposalTime
		processingTime = rm.currentRound.ProcessingTime
	}
	rm.roundMutex.Unlock()

	rm.roundMutex.Lock()
	requestIds := make([]api.StateID, 0)
	ackEntries := make([]interfaces.CertificationRequestAck, 0)
	var proofTimes []time.Duration

	if rm.currentRound != nil {
		commitmentCount = len(rm.currentRound.Commitments)
		requestIds = make([]api.StateID, commitmentCount)
		ackEntries = make([]interfaces.CertificationRequestAck, commitmentCount)

		now := time.Now()
		for i, commitment := range rm.currentRound.Commitments {
			requestIds[i] = commitment.StateID
			ackEntries[i] = interfaces.CertificationRequestAck{StateID: commitment.StateID, StreamID: commitment.StreamID}

			if commitment.CreatedAt != nil {
				proofReadyTime := now.Sub(commitment.CreatedAt.Time)
				if proofReadyTime > 0 {
					proofTimes = append(proofTimes, proofReadyTime)
				}
			}
		}
	}
	rm.roundMutex.Unlock()

	rm.roundMutex.Lock()
	var pendingLeaves []*smt.Leaf
	var commitments []*models.CertificationRequest
	snapshot := rm.currentRound.Snapshot
	if rm.currentRound != nil {
		pendingLeaves = rm.currentRound.PendingLeaves
		commitments = rm.currentRound.Commitments
	}
	rm.roundMutex.Unlock()

	persistDataStart = time.Now()
	smtNodes := rm.convertLeavesToNodes(pendingLeaves)
	records := rm.convertCommitmentsToRecords(commitments, block.Index)

	block.Finalized = false
	if err := rm.storeBlockAndRecords(ctx, block, requestIds); err != nil {
		if !errors.Is(err, interfaces.ErrDuplicateKey) {
			return fmt.Errorf("failed to store block and records: %w", err)
		}
		rm.logger.WithContext(ctx).Info("Block already exists, continuing with remaining steps",
			"blockNumber", block.Index.String())
	}

	if err := rm.storeDataParallel(ctx, block.Index, smtNodes, records); err != nil {
		return fmt.Errorf("failed to store SMT nodes and aggregator records: %w", err)
	}
	persistDataTime = time.Since(persistDataStart)

	rm.finalizationMu.Lock()
	if snapshot != nil {
		commitSnapshotStart = time.Now()
		snapshot.Commit(rm.smt)
		commitSnapshotTime = time.Since(commitSnapshotStart)
	}

	if err := rm.storage.BlockStorage().SetFinalized(ctx, block.Index, true); err != nil {
		rm.finalizationMu.Unlock()
		return fmt.Errorf("failed to set block as finalized: %w", err)
	}
	rm.finalizationMu.Unlock()
	block.Finalized = true

	if len(ackEntries) > 0 {
		markProcessedStart = time.Now()
		if err := rm.commitmentQueue.MarkProcessed(ctx, ackEntries); err != nil {
			return fmt.Errorf("failed to mark commitments as processed: %w", err)
		}
		markProcessedTime = time.Since(markProcessedStart)
	}

	rm.roundMutex.Lock()
	if rm.currentRound != nil {
		rm.currentRound.Block = block
		rm.currentRound.PendingRecords = nil
		rm.currentRound.PendingRootHash = ""
		rm.currentRound.PendingLeaves = nil
		rm.currentRound.Snapshot = nil
	}
	rm.roundMutex.Unlock()

	actualFinalizationTime := time.Since(finalizationStartTime)
	finalizationWorkDuration := markProcessedTime + persistDataTime + commitSnapshotTime
	var totalRoundTime time.Duration
	var bftWaitTime time.Duration
	if !proposalTime.IsZero() {
		bftWaitTime = finalizationStartTime.Sub(proposalTime)
		rm.avgFinalizationTime = (rm.avgFinalizationTime*4 + actualFinalizationTime + bftWaitTime) / 5
		totalRoundTime = processingTime + bftWaitTime + actualFinalizationTime
	}

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

	// Ensure minimum round duration for child shards
	if rm.config.Sharding.Mode.IsChild() {
		rm.roundMutex.RLock()
		roundStartTime := rm.currentRound.StartTime
		rm.roundMutex.RUnlock()

		elapsed := time.Since(roundStartTime)
		if elapsed < rm.roundDuration {
			time.Sleep(rm.roundDuration - elapsed)
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

// convertLeavesToNodes converts SMT leaves to storage models
func (rm *RoundManager) convertLeavesToNodes(leaves []*smt.Leaf) []*models.SmtNode {
	if len(leaves) == 0 {
		return nil
	}

	smtNodes := make([]*models.SmtNode, len(leaves))
	for i, leaf := range leaves {
		keyBytes := leaf.Path.Bytes()
		key := api.NewHexBytes(keyBytes)
		value := api.NewHexBytes(leaf.Value)
		smtNodes[i] = models.NewSmtNode(key, value)
	}
	return smtNodes
}

// convertCommitmentsToRecords converts commitments to aggregator records
func (rm *RoundManager) convertCommitmentsToRecords(commitments []*models.CertificationRequest, blockIndex *api.BigInt) []*models.AggregatorRecord {
	if len(commitments) == 0 {
		return nil
	}

	records := make([]*models.AggregatorRecord, len(commitments))
	for i, commitment := range commitments {
		leafIndex := api.NewBigInt(big.NewInt(int64(i)))
		records[i] = models.NewAggregatorRecord(commitment, blockIndex, leafIndex)
	}
	return records
}

// executeBlockTransaction executes the block finalization transaction.
// storeBlockAndRecords stores the block and block records in a mini-transaction.
// The block is stored with finalized=false.
func (rm *RoundManager) storeBlockAndRecords(ctx context.Context, block *models.Block, requestIds []api.StateID) error {
	return rm.storage.WithTransaction(ctx, func(txCtx context.Context) error {
		if err := rm.storage.BlockStorage().Store(txCtx, block); err != nil {
			return fmt.Errorf("failed to store block: %w", err)
		}
		if err := rm.storage.BlockRecordsStorage().Store(txCtx, models.NewBlockRecords(block.Index, requestIds)); err != nil {
			return fmt.Errorf("failed to store block records: %w", err)
		}
		return nil
	})
}

// storeDataParallel stores SMT nodes and aggregator records in parallel.
// StoreBatch handles duplicates internally (ignores duplicate key errors).
func (rm *RoundManager) storeDataParallel(
	ctx context.Context,
	blockNumber *api.BigInt,
	smtNodes []*models.SmtNode,
	records []*models.AggregatorRecord,
) error {
	start := time.Now()

	var smtErr, recordsErr error
	var smtTime, recordsTime time.Duration

	// Run SMT and AggregatorRecords storage in parallel
	var wg sync.WaitGroup

	if len(smtNodes) > 0 {
		wg.Go(func() {
			t := time.Now()
			smtErr = rm.storage.SmtStorage().StoreBatch(ctx, smtNodes)
			smtTime = time.Since(t)
		})
	}

	if len(records) > 0 {
		wg.Go(func() {
			t := time.Now()
			recordsErr = rm.storage.AggregatorRecordStorage().StoreBatch(ctx, records)
			recordsTime = time.Since(t)
		})
	}

	wg.Wait()

	rm.logger.WithContext(ctx).Debug("PARALLEL_TIMING",
		"block", blockNumber.String(),
		"storeSmtNodes", smtTime.Milliseconds(),
		"storeAggRecords", recordsTime.Milliseconds(),
		"smtCount", len(smtNodes),
		"recordCount", len(records),
		"totalMs", time.Since(start).Milliseconds())

	if smtErr != nil {
		return fmt.Errorf("failed to store SMT nodes: %w", smtErr)
	}
	if recordsErr != nil {
		return fmt.Errorf("failed to store aggregator records: %w", recordsErr)
	}

	return nil
}
