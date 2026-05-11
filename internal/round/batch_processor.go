package round

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
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

	// Convert commitments to SMT leaves, tracking valid commitments
	leaves := make([]*smt.Leaf, 0, len(commitments))
	validCommitments := make([]*models.CertificationRequest, 0, len(commitments))
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

		leaves = append(leaves, smt.NewLeaf(path, leafValue))
		validCommitments = append(validCommitments, commitment)
	}

	// Add leaves to the current round's SMT snapshot
	if rm.currentRound != nil && rm.currentRound.Snapshot != nil {
		smtStart := time.Now()
		_, err := rm.currentRound.Snapshot.AddLeaves(leaves)
		metrics.SMTAddLeavesDuration.Observe(time.Since(smtStart).Seconds())
		if err != nil {
			result := tryAddLeavesOneByOne(ctx, rm.logger, rm.commitmentQueue, rm.currentRound.Snapshot, leaves, validCommitments)
			rm.currentRound.PendingLeaves = append(rm.currentRound.PendingLeaves, result.successLeaves...)
			rm.currentRound.PendingCommitments = append(rm.currentRound.PendingCommitments, result.successCommitments...)
			rm.markProofsPending(result.successCommitments)
		} else {
			rm.currentRound.PendingLeaves = append(rm.currentRound.PendingLeaves, leaves...)
			rm.currentRound.PendingCommitments = append(rm.currentRound.PendingCommitments, validCommitments...)
			rm.markProofsPending(validCommitments)
		}
	}

	return nil
}

// leafAddResult holds results of adding leaves one-by-one to an SMT snapshot.
type leafAddResult struct {
	successLeaves      []*smt.Leaf
	successCommitments []*models.CertificationRequest
	rejected           []interfaces.CertificationRequestAck
}

// ProposeBlock creates and proposes a new block with the given data.
// rootHash is the raw 32-byte SMT root (no algorithm-id prefix) — the
// block, UC.IR.h and V2 proof wire all bind against this raw form.
func (rm *RoundManager) proposeBlock(ctx context.Context, blockNumber *api.BigInt, rootHash api.HexBytes) error {
	rm.logger.WithContext(ctx).Info("proposeBlock called",
		"blockNumber", blockNumber.String(),
		"rootHash", rootHash.String())

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
		"rootHash", rootHash.String())

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

	switch rm.config.Sharding.Mode {
	case config.ShardingModeStandalone, config.ShardingModeBFTShard:
		block := models.NewBlock(
			blockNumber,
			rm.config.Chain.ID,
			0,
			rm.config.Chain.Version,
			rm.config.Chain.ForkID,
			rootHash,
			parentHash,
			nil,
		)
		rm.roundMutex.RLock()
		if rm.currentRound != nil && !rm.currentRound.StartTime.IsZero() {
			metrics.RoundPreparationDuration.Observe(time.Since(rm.currentRound.StartTime).Seconds())
		}
		rm.roundMutex.RUnlock()
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
		rm.logger.WithContext(ctx).Info("Submitting root hash to parent shard", "rootHash", rootHash.String())

		if len(rootHash) != api.StateTreeKeyLengthBytes {
			return fmt.Errorf("child root hash has invalid length: expected %d bytes, got %d",
				api.StateTreeKeyLengthBytes, len(rootHash))
		}

		request := &api.SubmitShardRootRequest{
			ShardID:  rm.config.Sharding.Child.ShardID,
			RootHash: rootHash,
		}
		submitStart := time.Now()
		if err := rm.submitShardRootWithRetry(ctx, request); err != nil {
			return fmt.Errorf("failed to submit root hash to parent shard: %w", err)
		}
		submissionDuration := time.Since(submitStart)
		metrics.ParentRootSubmissionDuration.Observe(submissionDuration.Seconds())
		rm.logger.WithContext(ctx).Info("Root hash submitted to parent, polling for inclusion proof...",
			"rootHash", rootHash.String(),
			"submissionDuration", submissionDuration)

		proofWaitStart := time.Now()
		var (
			proof    *api.RootShardInclusionProof
			parentUC *types.UnicityCertificate
			err      error
		)
		for {
			proof, parentUC, err = rm.pollForParentProof(ctx, rootHash)
			if err == nil {
				break
			}
			if errors.Is(err, ErrParentProofPollTimeout) {
				rm.logger.WithContext(ctx).Warn("Parent shard proof poll timed out, continuing to poll",
					"rootHash", rootHash.String(),
					"timeout", rm.config.Sharding.Child.ParentPollTimeout)
				continue
			}
			return fmt.Errorf("failed to poll for parent shard inclusion proof: %w", err)
		}
		proofWait := time.Since(proofWaitStart)
		rm.logger.WithContext(ctx).Info("Parent shard proof received",
			"rootHash", rootHash.String(),
			"proofWait", proofWait,
			"submissionToProof", submissionDuration+proofWait)
		rm.roundMutex.Lock()
		if rm.currentRound != nil {
			rm.currentRound.SubmissionDuration = submissionDuration
			rm.currentRound.ProofWaitDuration = proofWait
		}
		rm.roundMutex.Unlock()

		if proof.ParentFragment == nil {
			return fmt.Errorf("parent shard proof missing native parent fragment")
		}

		block := models.NewChildBlock(
			blockNumber,
			rm.config.Chain.ID,
			request.ShardID,
			rm.config.Chain.Version,
			rm.config.Chain.ForkID,
			rootHash,
			parentHash,
			proof.UnicityCertificate,
			proof.ParentFragment,
			proof.BlockNumber,
		)
		if err := rm.FinalizeBlockWithRetry(ctx, block); err != nil {
			return fmt.Errorf("failed to finalize block after retries: %w", err)
		}
		rm.acceptParentUC(parentUC)

		nextRoundNumber := big.NewInt(0).Add(blockNumber.Int, big.NewInt(1))

		// Snapshot precollector ref under lock to avoid data race with Deactivate.
		rm.roundMutex.RLock()
		cp := rm.precollector
		rm.roundMutex.RUnlock()

		if cp != nil {
			preResult, advErr := cp.AdvanceRound()
			if advErr == nil {
				preResult.snapshot.SetCommitTarget(rm.smt)
				// StartNewRoundWithSnapshot atomically checks precollectorDisabled
				// under roundMutex — no race with concurrent Deactivate.
				if err := rm.StartNewRoundWithSnapshot(ctx, api.NewBigInt(nextRoundNumber), preResult.snapshot, preResult.commitments, preResult.leaves); err != nil && !errors.Is(err, ErrDeactivated) {
					rm.logger.WithContext(ctx).Error("Failed to start new round with snapshot.", "error", err.Error())
				}
			} else {
				rm.logger.WithContext(ctx).Warn("Failed to advance precollector", "error", advErr.Error())
				if err := rm.StartNewRound(ctx, api.NewBigInt(nextRoundNumber)); err != nil && !errors.Is(err, ErrDeactivated) {
					rm.logger.WithContext(ctx).Error("Failed to start new round after finalization.", "error", err.Error())
				}
			}
		} else {
			if err := rm.StartNewRound(ctx, api.NewBigInt(nextRoundNumber)); err != nil && !errors.Is(err, ErrDeactivated) {
				rm.logger.WithContext(ctx).Error("Failed to start new round after finalization.", "error", err.Error())
			}
		}

		rm.logger.WithContext(ctx).Info("Block finalized and new round started", "blockNumber", blockNumber.String())
		return nil
	default:
		return fmt.Errorf("invalid sharding mode: %s", rm.config.Sharding.Mode)
	}
}

func (rm *RoundManager) pollForParentProof(ctx context.Context, rootHash api.HexBytes) (*api.RootShardInclusionProof, *types.UnicityCertificate, error) {
	pollingCtx, cancel := context.WithTimeout(ctx, rm.config.Sharding.Child.ParentPollTimeout)
	defer cancel()

	ticker := time.NewTicker(rm.config.Sharding.Child.ParentPollInterval)
	defer ticker.Stop()
	pollStart := time.Now()

	for {
		select {
		case <-pollingCtx.Done():
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			metrics.ParentProofErrorsTotal.Inc()
			rm.logger.WithContext(ctx).Warn("Timed out waiting for parent shard inclusion proof",
				"rootHash", rootHash.String(),
				"pollDuration", time.Since(pollStart))
			return nil, nil, fmt.Errorf("%w: %s", ErrParentProofPollTimeout, rootHash.String())
		case <-ticker.C:
			request := &api.GetShardProofRequest{ShardID: rm.config.Sharding.Child.ShardID}
			proof, err := rm.rootClient.GetShardProof(pollingCtx, request)
			if err != nil {
				metrics.ParentProofErrorsTotal.Inc()
				rm.logger.WithContext(ctx).Warn("Failed to fetch parent shard inclusion proof, retrying",
					"rootHash", rootHash.String(),
					"error", err.Error())
				continue
			}
			if proof == nil || !proof.IsValid(rm.config.Sharding.Child.ShardID, rm.config.Sharding.ShardIDLength, rootHash) {
				continue
			}

			parentUC, err := decodeUnicityCertificate(proof.UnicityCertificate)
			if err != nil {
				rm.logger.WithContext(ctx).Warn("Failed to decode parent shard proof UC, retrying",
					"rootHash", rootHash.String(),
					"error", err.Error())
				continue
			}

			lastParentRound := rm.lastAcceptedParentUC()
			if parentUC.GetRoundNumber() <= lastParentRound {
				rm.logger.WithContext(ctx).Debug("Ignoring stale parent shard proof",
					"rootHash", rootHash.String(),
					"proofParentRound", parentUC.GetRoundNumber(),
					"lastAcceptedParentRound", lastParentRound)
				continue
			}

			metrics.ParentProofWaitDuration.Observe(time.Since(pollStart).Seconds())
			return proof, parentUC, nil
		}
	}
}

// ErrParentProofPollTimeout marks a single poll window timeout while waiting for a parent proof.
var ErrParentProofPollTimeout = errors.New("parent shard inclusion proof poll timeout")

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
	if err := rm.validateBlockForMode(block); err != nil {
		return err
	}

	rm.logger.WithContext(ctx).Info("FinalizeBlock called",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String(),
		"hasUnicityCertificate", block.UnicityCertificate != nil)

	finalizationStartTime := time.Now()
	var proposalTime time.Time
	var processingTime time.Duration
	commitmentCount := 0

	rm.roundMutex.Lock()
	if rm.currentRound != nil && rm.currentRound.Number.String() == block.Index.String() {
		proposalTime = rm.currentRound.ProposalTime
		processingTime = rm.currentRound.ProcessingTime
	}
	rm.roundMutex.Unlock()

	rm.roundMutex.Lock()
	var pendingLeaves []*smt.Leaf
	var pendingCommitments []*models.CertificationRequest
	var snapshot *smt.ThreadSafeSmtSnapshot
	if rm.currentRound != nil {
		pendingLeaves = rm.currentRound.PendingLeaves
		pendingCommitments = rm.currentRound.PendingCommitments
		snapshot = rm.currentRound.Snapshot
	}
	rm.roundMutex.Unlock()

	commitmentCount = len(pendingCommitments)
	stateIDs := make([]api.StateID, commitmentCount)
	ackEntries := make([]interfaces.CertificationRequestAck, commitmentCount)

	finalizationScanStart := time.Now()
	for i, commitment := range pendingCommitments {
		stateIDs[i] = commitment.StateID
		ackEntries[i] = interfaces.CertificationRequestAck{StateID: commitment.StateID, StreamID: commitment.StreamID}
	}
	finalizationScanDuration := time.Since(finalizationScanStart)

	finalizationConvertStart := time.Now()
	smtNodes, err := rm.convertLeavesToNodes(pendingLeaves)
	if err != nil {
		return fmt.Errorf("failed to convert leaves to storage nodes: %w", err)
	}
	records := rm.convertCommitmentsToRecords(pendingCommitments, block.Index)
	finalizationConvertDuration := time.Since(finalizationConvertStart)

	block.Finalized = false
	storeBlockStart := time.Now()
	if err := rm.storeBlockAndRecords(ctx, block, stateIDs); err != nil {
		if !errors.Is(err, interfaces.ErrDuplicateKey) {
			return fmt.Errorf("failed to store block and records: %w", err)
		}
		rm.logger.WithContext(ctx).Info("Block already exists, continuing with remaining steps",
			"blockNumber", block.Index.String())
	}
	storeBlockDuration := time.Since(storeBlockStart)

	storeDataTiming, err := rm.storeDataParallel(ctx, block.Index, smtNodes, records)
	if err != nil {
		return fmt.Errorf("failed to store SMT nodes and aggregator records: %w", err)
	}

	lockWaitStart := time.Now()
	rm.finalizationMu.Lock()
	finalizationLockWaitDuration := time.Since(lockWaitStart)

	smtCommitStart := time.Now()
	if snapshot != nil {
		snapshot.Commit(rm.smt)
	}
	smtCommitDuration := time.Since(smtCommitStart)

	setFinalizedStart := time.Now()
	if err := rm.storage.BlockStorage().SetFinalized(ctx, block.Index, true); err != nil {
		rm.finalizationMu.Unlock()
		return fmt.Errorf("failed to set block as finalized: %w", err)
	}
	setFinalizedDuration := time.Since(setFinalizedStart)
	block.Finalized = true
	rm.markProofsReady(block, stateIDs)
	rm.finalizationMu.Unlock()

	// Proofs are requestable only after the SMT snapshot is committed and the block is visible as finalized.
	// Redis ACK is recovery bookkeeping.
	proofReadyAt := time.Now()
	metrics.RoundFinalizationDuration.Observe(proofReadyAt.Sub(finalizationStartTime).Seconds())

	ackDuration := time.Duration(0)
	if len(ackEntries) > 0 {
		ackStart := time.Now()
		if err := rm.commitmentQueue.MarkProcessed(ctx, ackEntries); err != nil {
			return fmt.Errorf("failed to mark commitments as processed: %w", err)
		}
		ackDuration = time.Since(ackStart)
	}

	rm.roundMutex.Lock()
	if rm.currentRound != nil {
		rm.currentRound.Block = block
		rm.currentRound.PendingRootHash = nil
		rm.currentRound.PendingLeaves = nil
		rm.currentRound.PendingCommitments = nil
		rm.currentRound.Snapshot = nil
	}
	rm.roundMutex.Unlock()

	actualFinalizationTime := time.Since(finalizationStartTime)
	proofTimes := make([]time.Duration, 0, len(pendingCommitments))
	for _, commitment := range pendingCommitments {
		if commitment.CreatedAt == nil {
			continue
		}
		proofReadyTime := proofReadyAt.Sub(commitment.CreatedAt.Time)
		if proofReadyTime > 0 {
			metrics.ProofReadinessDuration.Observe(proofReadyTime.Seconds())
			proofTimes = append(proofTimes, proofReadyTime)
		}
	}

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
		"finalization", shortDur(actualFinalizationTime),
		"finalizeScan", shortDur(finalizationScanDuration),
		"finalizeConvert", shortDur(finalizationConvertDuration),
		"finalizeStoreBlock", shortDur(storeBlockDuration),
		"finalizeStoreData", shortDur(storeDataTiming.total),
		"finalizeStoreSmt", shortDur(storeDataTiming.smt),
		"finalizeStoreRecords", shortDur(storeDataTiming.records),
		"finalizeLockWait", shortDur(finalizationLockWaitDuration),
		"finalizeSmtCommit", shortDur(smtCommitDuration),
		"finalizeSetFinalized", shortDur(setFinalizedDuration),
		"finalizeAck", shortDur(ackDuration),
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
		metrics.ProofReadinessMedian.Set(median.Seconds())
		metrics.ProofReadinessP95.Set(p95.Seconds())
	}

	redisTotal, _ := rm.commitmentQueue.Count(ctx)
	redisPending, _ := rm.commitmentQueue.CountUnprocessed(ctx)

	logFields = append(logFields,
		"redisTotal", redisTotal,
		"redisPending", redisPending,
		"collectPhaseDuration", rm.config.Processing.CollectPhaseDuration.String(),
		"streamChannelSize", len(rm.commitmentStream),
		"streamChannelCapacity", cap(rm.commitmentStream),
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

	rm.roundMutex.RLock()
	var roundStartTime time.Time
	finalizedCommitments := len(pendingCommitments)
	if rm.currentRound != nil {
		roundStartTime = rm.currentRound.StartTime
	}
	rm.roundMutex.RUnlock()

	metrics.SetBlockHeight(block.Index.Int)
	metrics.CommitmentsProcessedTotal.Add(float64(finalizedCommitments))
	metrics.RoundCommitments.Observe(float64(finalizedCommitments))
	if !roundStartTime.IsZero() {
		metrics.BlockCreationDuration.Observe(time.Since(roundStartTime).Seconds())
	}

	return nil
}

func (rm *RoundManager) validateBlockForMode(block *models.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}
	if rm.config.Sharding.Mode.IsChild() {
		if block.ParentFragment == nil {
			return errors.New("child-mode block missing parent fragment")
		}
		if block.ParentBlockNumber == 0 {
			return errors.New("child-mode block missing parent block number")
		}
	}
	return nil
}

// convertLeavesToNodes converts SMT leaves to storage models
func (rm *RoundManager) convertLeavesToNodes(leaves []*smt.Leaf) ([]*models.SmtNode, error) {
	if len(leaves) == 0 {
		return nil, nil
	}

	keyLength := rm.smt.GetKeyLength()
	smtNodes := make([]*models.SmtNode, 0, len(leaves))
	for _, leaf := range leaves {
		keyBytes, err := api.PathToFixedBytes(leaf.Path, keyLength)
		if err != nil {
			return nil, fmt.Errorf("failed to convert leaf path %s to SMT storage key: %w", leaf.Path.String(), err)
		}
		key := api.NewHexBytes(keyBytes)
		value := api.NewHexBytes(leaf.Value)
		smtNodes = append(smtNodes, models.NewSmtNode(key, value))
	}
	return smtNodes, nil
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
func (rm *RoundManager) storeBlockAndRecords(ctx context.Context, block *models.Block, stateIDs []api.StateID) error {
	return rm.storage.WithTransaction(ctx, func(txCtx context.Context) error {
		if err := rm.storage.BlockStorage().Store(txCtx, block); err != nil {
			return fmt.Errorf("failed to store block: %w", err)
		}
		if err := rm.storage.BlockRecordsStorage().Store(txCtx, models.NewBlockRecords(block.Index, stateIDs)); err != nil {
			return fmt.Errorf("failed to store block records: %w", err)
		}
		return nil
	})
}

type storeDataTiming struct {
	total   time.Duration
	smt     time.Duration
	records time.Duration
}

// storeDataParallel stores SMT nodes and aggregator records in parallel.
// StoreBatch handles duplicates internally (ignores duplicate key errors).
func (rm *RoundManager) storeDataParallel(
	ctx context.Context,
	blockNumber *api.BigInt,
	smtNodes []*models.SmtNode,
	records []*models.AggregatorRecord,
) (storeDataTiming, error) {
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

	timing := storeDataTiming{
		total:   time.Since(start),
		smt:     smtTime,
		records: recordsTime,
	}

	rm.logger.WithContext(ctx).Debug("PARALLEL_TIMING",
		"block", blockNumber.String(),
		"storeSmtNodes", smtTime.Milliseconds(),
		"storeAggRecords", recordsTime.Milliseconds(),
		"smtCount", len(smtNodes),
		"recordCount", len(records),
		"totalMs", timing.total.Milliseconds())

	if smtErr != nil {
		return timing, fmt.Errorf("failed to store SMT nodes: %w", smtErr)
	}
	if recordsErr != nil {
		return timing, fmt.Errorf("failed to store aggregator records: %w", recordsErr)
	}

	return timing, nil
}
