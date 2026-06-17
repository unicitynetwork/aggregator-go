package round

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"sort"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/bft"
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	diskpersist "github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	diskstorage "github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// processMiniBatch processes a small batch of commitments into the SMT for efficiency
// NOTE: The caller is expected to hold rm.roundMutex when calling this function
// and ACK returned dropped entries after releasing it.
func (rm *RoundManager) processMiniBatch(ctx context.Context, commitments []*models.CertificationRequest) ([]interfaces.CertificationRequestAck, error) {
	return rm.processMiniBatchForRound(ctx, rm.currentRound, commitments)
}

func (rm *RoundManager) processMiniBatchForRound(ctx context.Context, round *Round, commitments []*models.CertificationRequest) ([]interfaces.CertificationRequestAck, error) {
	if len(commitments) == 0 {
		return nil, nil
	}

	// Convert commitments to backend leaf inputs, tracking valid commitments.
	leaves := make([]smtbackend.LeafInput, 0, len(commitments))
	validCommitments := make([]*models.CertificationRequest, 0, len(commitments))
	for _, commitment := range commitments {
		leaf, err := commitmentLeafInput(commitment)
		if err != nil {
			rm.logger.WithContext(ctx).Error("Failed to create leaf input",
				"stateID", commitment.StateID.String(),
				"error", err.Error())
			continue
		}

		leaves = append(leaves, leaf)
		validCommitments = append(validCommitments, commitment)
	}

	// Add leaves to the current round's SMT snapshot
	if round != nil && round.Snapshot != nil {
		smtStart := time.Now()
		addedCommitments, addedLeaves, dropped, err := addCommitmentLeaves(ctx, rm.logger, round.Snapshot, leaves, validCommitments)
		if err != nil {
			return nil, err
		}
		metrics.SMTAddLeavesDuration.Observe(time.Since(smtStart).Seconds())
		round.PendingLeaves = append(round.PendingLeaves, addedLeaves...)
		round.PendingCommitments = append(round.PendingCommitments, addedCommitments...)
		rm.markProofsPending(addedCommitments)
		return dropped, nil
	}

	return nil, nil
}

// ProposeBlock creates and proposes a new block with the given data.
// rootHash is the raw 32-byte SMT root (no algorithm-id prefix) — the
// block, UC.IR.h and V2 proof wire all bind against this raw form.
func (rm *RoundManager) proposeBlock(ctx context.Context, round *Round, blockNumber *api.BigInt, rootHash api.HexBytes) error {
	rm.logger.WithContext(ctx).Info("proposeBlock called",
		"blockNumber", blockNumber.String(),
		"rootHash", rootHash.String())

	if round == nil {
		return bft.ErrStaleCertificationRound
	}

	rm.roundMutex.Lock()
	if rm.currentRound != round {
		rm.roundMutex.Unlock()
		return bft.ErrStaleCertificationRound
	}
	rm.logger.WithContext(ctx).Debug("Changing round state to finalizing",
		"roundNumber", round.Number.String(),
		"previousState", round.State.String())
	round.State = RoundStateFinalizing
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
		block.ProposalID = round.ProposalID
		if err := rm.ensureDurableProposal(ctx, round, block); err != nil {
			return err
		}
		if round != nil && !round.StartTime.IsZero() {
			metrics.RoundPreparationDuration.Observe(time.Since(round.StartTime).Seconds())
		}
		// A repeat UC could have superseded this round during the storage work
		// above; do not send a stale proposal to BFT.
		rm.roundMutex.RLock()
		superseded := rm.currentRound != round
		rm.roundMutex.RUnlock()
		if superseded {
			if err := rm.AbandonDurableProposal(ctx, block.Index, block.RootHash); err != nil {
				return fmt.Errorf("failed to abandon superseded durable proposal: %w", err)
			}
			return bft.ErrStaleCertificationRound
		}
		rm.logger.WithContext(ctx).Info("Sending certification request to BFT client",
			"blockNumber", blockNumber.String(),
			"bftClientType", fmt.Sprintf("%T", rm.bftClient))
		if err := rm.bftClient.CertificationRequest(ctx, block); err != nil {
			if errors.Is(err, bft.ErrStaleCertificationRound) {
				if abandonErr := rm.AbandonDurableProposal(ctx, block.Index, block.RootHash); abandonErr != nil {
					return fmt.Errorf("failed to abandon stale durable proposal: %w", abandonErr)
				}
			}
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
		block.ProposalID = round.ProposalID
		if err := rm.ensureDurableProposal(ctx, round, block); err != nil {
			return err
		}
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
			preResult, advErr := rm.advancePrecollectorForHandoff(cp)
			if advErr == nil {
				nextRound := api.NewBigInt(nextRoundNumber)
				if err := validatePrecollectorBlockNumber(preResult, nextRound); err != nil {
					preResult.snapshot.Discard(ctx)
					rm.logger.WithContext(ctx).Error("Precollector block number mismatch.", "error", err.Error())
					return err
				}
				if err := preResult.snapshot.SetCommitTarget(ctx, rm.smtBackend); err != nil {
					preResult.snapshot.Discard(ctx)
					rm.logger.WithContext(ctx).Error("Failed to set precollector commit target.", "error", err.Error())
					return err
				}
				// StartNewRoundWithSnapshot atomically checks precollectorDisabled
				// under roundMutex — no race with concurrent Deactivate.
				if err := rm.StartNewRoundWithSnapshot(ctx, nextRound, preResult.snapshot, preResult.commitments, preResult.leaves, preResult.recordsStaged, preResult.proposalID); err != nil {
					preResult.snapshot.Discard(ctx)
					if !errors.Is(err, ErrDeactivated) {
						rm.logger.WithContext(ctx).Error("Failed to start new round with snapshot.", "error", err.Error())
					}
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

func (rm *RoundManager) ensureDurableProposal(ctx context.Context, round *Round, block *models.Block) error {
	records, err := rm.proposalRecordsForRound(round, block.Index)
	if err != nil {
		return err
	}
	if err := rm.storeProposedBlockAndRecords(ctx, block, records); err != nil {
		if !errors.Is(err, interfaces.ErrDuplicateKey) {
			return fmt.Errorf("failed to store durable proposal: %w", err)
		}
		if _, err := rm.validateStoredDurableProposalBlock(ctx, block); err != nil {
			return err
		}
		rm.logger.WithContext(ctx).Info("Durable proposal already exists",
			"blockNumber", block.Index.String())
	}
	return nil
}

func (rm *RoundManager) proposalRecordsForRound(round *Round, blockNumber *api.BigInt) ([]*models.AggregatorRecord, error) {
	if round == nil {
		return nil, bft.ErrStaleCertificationRound
	}
	rm.roundMutex.RLock()
	if rm.currentRound != round {
		rm.roundMutex.RUnlock()
		return nil, bft.ErrStaleCertificationRound
	}
	pendingCommitments := append([]*models.CertificationRequest(nil), round.PendingCommitments...)
	recordsStaged := round.ProposalRecordsStaged
	rm.roundMutex.RUnlock()

	if recordsStaged {
		return nil, nil
	}
	records := rm.convertCommitmentsToRecords(pendingCommitments, blockNumber)
	for _, record := range records {
		record.ProposalID = round.ProposalID
	}
	return records, nil
}

func (rm *RoundManager) stageProposedCommitments(ctx context.Context, blockNumber *api.BigInt, proposalID string, leafIndexOffset int, commitments []*models.CertificationRequest) error {
	if blockNumber == nil || len(commitments) == 0 {
		return nil
	}
	records := rm.convertCommitmentsToRecordsFrom(commitments, blockNumber, leafIndexOffset)
	for _, record := range records {
		record.ProposalID = proposalID
	}
	if err := rm.storage.AggregatorRecordStorage().StoreBatch(ctx, records); err != nil {
		return fmt.Errorf("failed to stage proposed aggregator records: %w", err)
	}
	return nil
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
			recoveryResult, recoverErr := recoverUnfinalizedBlock(ctx, rm.logger, rm.storage, rm.commitmentQueue, !rm.usesDiskSMTBackend())
			if recoverErr != nil {
				return fmt.Errorf("recovery failed: %w", recoverErr)
			}
			if recoveryResult != nil && recoveryResult.Recovered {
				if err := rm.reconcileRecoveredFinalization(ctx, recoveryResult); err != nil {
					return fmt.Errorf("failed to reconcile recovered finalization: %w", err)
				}
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

func (rm *RoundManager) reconcileRecoveredFinalization(ctx context.Context, recoveryResult *RecoveryResult) error {
	if recoveryResult == nil || !recoveryResult.Recovered {
		return nil
	}
	blockNumber := recoveryResult.BlockNumber
	var snapshot smtbackend.Snapshot

	rm.roundMutex.RLock()
	if blockNumber != nil &&
		rm.currentRound != nil &&
		rm.currentRound.Number != nil &&
		rm.currentRound.Snapshot != nil &&
		rm.currentRound.Number.Cmp(blockNumber.Int) == 0 {
		snapshot = rm.currentRound.Snapshot
	}
	rm.roundMutex.RUnlock()

	rm.finalizationMu.Lock()
	defer rm.finalizationMu.Unlock()

	if rm.usesDiskSMTBackend() {
		if err := rm.syncDiskSMTAfterRecoveredBlock(ctx, recoveryResult); err != nil {
			return err
		}
		rm.clearProofPending()
		return nil
	}

	if snapshot != nil {
		if err := snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: blockNumber}); err != nil {
			return fmt.Errorf("failed to commit recovered SMT snapshot: %w", err)
		}
	}

	rm.clearProofPending()
	return nil
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
	round := rm.currentRound
	if round == nil || round.Number == nil {
		rm.roundMutex.Unlock()
		return fmt.Errorf("cannot finalize block %s: no active round", block.Index.String())
	}
	if round.Number.Cmp(block.Index.Int) != 0 {
		roundNumber := round.Number.String()
		rm.roundMutex.Unlock()
		return fmt.Errorf("cannot finalize block %s: block number does not match active round %s", block.Index.String(), roundNumber)
	}

	proposalTime = round.ProposalTime
	processingTime = round.ProcessingTime

	var pendingLeaves []smtbackend.LeafInput
	var pendingCommitments []*models.CertificationRequest
	var snapshot smtbackend.Snapshot
	if len(round.PendingLeaves) > 0 {
		pendingLeaves = append([]smtbackend.LeafInput(nil), round.PendingLeaves...)
	}
	if len(round.PendingCommitments) > 0 {
		pendingCommitments = append([]*models.CertificationRequest(nil), round.PendingCommitments...)
	}
	snapshot = round.Snapshot
	rm.roundMutex.Unlock()

	if snapshot != nil {
		snapshotRoot, err := snapshot.RootHashRaw(ctx)
		if err != nil {
			return fmt.Errorf("failed to get SMT snapshot root: %w", err)
		}
		if !bytes.Equal(snapshotRoot, block.RootHash) {
			return fmt.Errorf("cannot finalize block %s: snapshot root %s does not match block root %s",
				block.Index.String(), api.HexBytes(snapshotRoot).String(), block.RootHash.String())
		}
	}

	commitmentCount = len(pendingCommitments)
	stateIDs := make([]api.StateID, commitmentCount)
	ackEntries := make([]interfaces.CertificationRequestAck, commitmentCount)

	finalizationScanStart := time.Now()
	for i, commitment := range pendingCommitments {
		stateIDs[i] = commitment.StateID
		ackEntries[i] = interfaces.CertificationRequestAck{StateID: commitment.StateID, StreamID: commitment.StreamID}
	}
	finalizationScanDuration := time.Since(finalizationScanStart)

	var smtNodesToStore []*models.SmtNode
	var finalizationConvertDuration time.Duration
	if !rm.usesDiskSMTBackend() {
		finalizationConvertStart := time.Now()
		var err error
		smtNodesToStore, err = rm.convertLeavesToNodes(pendingLeaves)
		if err != nil {
			return fmt.Errorf("failed to convert leaves to storage nodes: %w", err)
		}
		finalizationConvertDuration = time.Since(finalizationConvertStart)
	}

	storeBlockStart := time.Now()
	var existingBlock *models.Block
	var records []*models.AggregatorRecord
	var err error
	existingBlock, err = rm.validateStoredDurableProposalBlock(ctx, block)
	if err != nil {
		return err
	}
	records = rm.convertCommitmentsToRecords(pendingCommitments, block.Index)
	for _, record := range records {
		record.ProposalID = existingBlock.ProposalID
	}
	block.ProposalID = existingBlock.ProposalID
	if existingBlock.Finalized {
		localRoot, err := rm.smtBackend.RootHashRaw(ctx)
		if err != nil {
			return fmt.Errorf("failed to validate local SMT root for finalized duplicate block %s: %w", block.Index.String(), err)
		}
		if bytes.Equal(localRoot, block.RootHash) {
			if publisher, ok := rm.smtBackend.(smtbackend.ProofViewPublisher); ok {
				if err := publisher.RefreshPublishedProofView(ctx, block.RootHash); err != nil {
					return fmt.Errorf("failed to refresh proof view for finalized duplicate block %s: %w", block.Index.String(), err)
				}
			}
			block.Finalized = true
			rm.markProofsReady(block, stateIDs, records)
			if snapshot != nil {
				snapshot.Discard(ctx)
			}
			rm.clearFinalizedRound(round, block)
			rm.logger.WithContext(ctx).Info("Block already finalized with matching local SMT root, skipping duplicate finalization",
				"blockNumber", block.Index.String())
			return nil
		}
	}
	storeBlockDuration := time.Since(storeBlockStart)

	var storeDataTiming storeDataTiming
	var smtCommitDuration time.Duration
	var smtCommitTiming smtbackend.CommitTiming
	var finalizationLockWaitDuration time.Duration
	var preparedProofView smtbackend.PreparedProofView

	diskFinalize := rm.usesDiskSMTBackend() && snapshot != nil
	if !diskFinalize {
		var err error
		storeDataTiming, err = rm.storeDataParallel(ctx, smtNodesToStore)
		if err != nil {
			return fmt.Errorf("failed to store SMT nodes: %w", err)
		}

		lockWaitStart := time.Now()
		rm.finalizationMu.Lock()
		finalizationLockWaitDuration = time.Since(lockWaitStart)

		smtCommitStart := time.Now()
		if snapshot != nil {
			if err := snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: block.Index, RootHash: block.RootHash}); err != nil {
				rm.finalizationMu.Unlock()
				return fmt.Errorf("failed to commit SMT snapshot: %w", err)
			}
		}
		smtCommitDuration = time.Since(smtCommitStart)
	}
	proofViewPublished := false
	defer func() {
		if preparedProofView != nil && !proofViewPublished {
			preparedProofView.Discard(ctx)
		}
	}()

	setFinalizedStart := time.Now()
	if err := setBlockFinalizedWithCertificate(ctx, rm.storage, block); err != nil {
		if !diskFinalize {
			rm.finalizationMu.Unlock()
		}
		return err
	}
	setFinalizedDuration := time.Since(setFinalizedStart)
	block.Finalized = true
	block.Status = models.FinalityStatusFinalized

	if diskFinalize {
		var err error
		smtCommitDuration, smtCommitTiming, preparedProofView, err = rm.commitDiskSnapshotForFinalizedBlock(ctx, block, snapshot)
		if err != nil {
			return err
		}
	}
	metrics.SMTCommitDuration.Observe(smtCommitDuration.Seconds())

	precomputeProofDuration := time.Duration(0)
	precomputeProofTiming := precomputeProofTiming{}
	if rm.config.SMT.PrecomputeProofs {
		precomputeProofStart := time.Now()
		timing, err := rm.storePrecomputedProofResponses(ctx, block, records)
		precomputeProofTiming = timing
		if err != nil {
			rm.logger.WithContext(ctx).Warn("Failed to store precomputed proof responses; falling back to live proof generation",
				"blockNumber", block.Index.String(),
				"records", len(records),
				"error", err.Error())
		}
		precomputeProofDuration = time.Since(precomputeProofStart)
	}
	rm.markProofsReady(block, stateIDs, records)
	if preparedProofView != nil {
		if err := preparedProofView.Publish(ctx); err != nil {
			return fmt.Errorf("failed to publish disk SMT proof view: %w", err)
		}
		proofViewPublished = true
	}
	if !diskFinalize {
		rm.finalizationMu.Unlock()
	}

	// Proofs are requestable only after the SMT snapshot is committed and the block is visible as finalized.
	// Redis ACK is recovery bookkeeping.
	proofReadyAt := time.Now()
	metrics.RoundFinalizationDuration.Observe(proofReadyAt.Sub(finalizationStartTime).Seconds())

	ackDuration := time.Duration(0)
	if len(ackEntries) > 0 {
		ackStart := time.Now()
		if err := rm.commitmentQueue.MarkProcessed(ctx, ackEntries); err != nil {
			rm.logger.WithContext(ctx).Warn("Failed to mark finalized commitments as processed; recovery will retry cleanup",
				"blockNumber", block.Index.String(),
				"commitments", len(ackEntries),
				"error", err.Error())
		}
		ackDuration = time.Since(ackStart)
	}

	rm.clearFinalizedRound(round, block)

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

	shortDur := shortPerfDuration

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
		"finalizeSmtCommitCollect", shortDur(smtCommitTiming.CollectDuration),
		"finalizeSmtCommitTombstone", shortDur(smtCommitTiming.TombstoneDuration),
		"finalizeSmtCommitBatchBuild", shortDur(smtCommitTiming.BatchBuildDuration),
		"finalizeSmtCommitRootHash", shortDur(smtCommitTiming.RootHashDuration),
		"finalizeSmtCommitEngineWrite", shortDur(smtCommitTiming.EngineWriteDuration),
		"finalizeSmtCommitCacheUpdate", shortDur(smtCommitTiming.CacheUpdateDuration),
		"finalizeSmtCommitNodeWrites", smtCommitTiming.NodeWrites,
		"finalizeSmtCommitNodeDeletes", smtCommitTiming.NodeDeletes,
		"finalizeSetFinalized", shortDur(setFinalizedDuration),
		"finalizePrecomputeProofs", shortDur(precomputeProofDuration),
		"finalizePrecomputeKeys", shortDur(precomputeProofTiming.keys),
		"finalizePrecomputeCerts", shortDur(precomputeProofTiming.certs),
		"finalizePrecomputeResponses", shortDur(precomputeProofTiming.responses),
		"finalizePrecomputeStore", shortDur(precomputeProofTiming.store),
		"finalizeAck", shortDur(ackDuration),
	}
	if !proposalTime.IsZero() {
		logFields = append(logFields,
			"proposalToProofReady", shortDur(proofReadyAt.Sub(proposalTime)),
		)
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
	logFields = rm.appendMemoryDiagnosticFields(ctx, logFields)
	if proofWaitDuration > 0 {
		logFields = append(logFields,
			"proofWait", shortDur(proofWaitDuration),
		)
	}

	rm.logger.WithContext(ctx).Info("Round completed", logFields...)

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

func shortPerfDuration(d time.Duration) string {
	if d <= 0 {
		return "0ms"
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

// convertLeavesToNodes converts backend leaf inputs to storage models.
func (rm *RoundManager) convertLeavesToNodes(leaves []smtbackend.LeafInput) ([]*models.SmtNode, error) {
	if len(leaves) == 0 {
		return nil, nil
	}

	smtNodes := make([]*models.SmtNode, 0, len(leaves))
	for _, leaf := range leaves {
		if len(leaf.Key) != api.StateTreeKeyLengthBytes {
			return nil, fmt.Errorf("invalid SMT leaf key length: got %d, want %d", len(leaf.Key), api.StateTreeKeyLengthBytes)
		}
		key := api.NewHexBytes(leaf.Key)
		value := api.NewHexBytes(leaf.Value)
		smtNodes = append(smtNodes, models.NewSmtNode(key, value))
	}
	return smtNodes, nil
}

// convertCommitmentsToRecords converts commitments to aggregator records
func (rm *RoundManager) convertCommitmentsToRecords(commitments []*models.CertificationRequest, blockIndex *api.BigInt) []*models.AggregatorRecord {
	return rm.convertCommitmentsToRecordsFrom(commitments, blockIndex, 0)
}

func (rm *RoundManager) convertCommitmentsToRecordsFrom(commitments []*models.CertificationRequest, blockIndex *api.BigInt, leafIndexOffset int) []*models.AggregatorRecord {
	if len(commitments) == 0 {
		return nil
	}

	records := make([]*models.AggregatorRecord, len(commitments))
	for i, commitment := range commitments {
		leafIndex := api.NewBigInt(big.NewInt(int64(leafIndexOffset + i)))
		records[i] = models.NewAggregatorRecord(commitment, blockIndex, leafIndex)
	}
	return records
}

func (rm *RoundManager) storePrecomputedProofResponses(ctx context.Context, block *models.Block, records []*models.AggregatorRecord) (precomputeProofTiming, error) {
	var timing precomputeProofTiming
	if len(records) == 0 {
		return timing, nil
	}
	if block == nil {
		return timing, fmt.Errorf("missing finalized block")
	}
	writer, ok := rm.smtBackend.(smtbackend.PrecomputedProofWriter)
	if !ok {
		return timing, nil
	}

	keysStart := time.Now()
	keys := make([][]byte, len(records))
	for i, record := range records {
		key, err := record.StateID.GetTreeKey()
		if err != nil {
			return timing, fmt.Errorf("record %d has invalid state ID: %w", i, err)
		}
		keys[i] = key
	}
	timing.keys = time.Since(keysStart)

	certsStart := time.Now()
	certs := make([]*api.InclusionCert, len(keys))
	if batchBackend, ok := rm.smtBackend.(smtbackend.BatchInclusionCertBackend); ok {
		batchCerts, err := batchBackend.GetInclusionCerts(ctx, keys)
		if err != nil {
			return timing, err
		}
		certs = batchCerts
	} else {
		for i, key := range keys {
			cert, err := rm.smtBackend.GetInclusionCert(ctx, key)
			if err != nil {
				return timing, fmt.Errorf("build inclusion cert %d: %w", i, err)
			}
			certs[i] = cert
		}
	}
	timing.certs = time.Since(certsStart)

	responsesStart := time.Now()
	responseBlockNumber, err := precomputedProofBlockNumber(rm.config.Sharding.Mode, block)
	if err != nil {
		return timing, err
	}
	proofs := make([]smtbackend.PrecomputedProofResponse, len(records))
	for i, record := range records {
		cert := certs[i]
		if cert == nil {
			return timing, fmt.Errorf("nil inclusion cert %d", i)
		}
		if rm.config.Sharding.Mode == config.ShardingModeChild {
			if block.ParentFragment == nil {
				return timing, fmt.Errorf("child block %s is missing parent fragment", block.Index.String())
			}
			cert, err = api.ComposeInclusionCert(block.ParentFragment, cert, block.RootHash)
			if err != nil {
				return timing, fmt.Errorf("compose inclusion cert %d: %w", i, err)
			}
		}
		certBytes, err := cert.MarshalBinary()
		if err != nil {
			return timing, fmt.Errorf("marshal inclusion cert %d: %w", i, err)
		}
		proofs[i] = smtbackend.PrecomputedProofResponse{
			StateID: record.StateID,
			Response: &api.GetInclusionProofResponseV2{
				BlockNumber: responseBlockNumber,
				InclusionProof: &api.InclusionProofV2{
					CertificationData:  record.CertificationData.ToAPI(),
					CertificateBytes:   certBytes,
					UnicityCertificate: types.RawCBOR(block.UnicityCertificate),
				},
			},
		}
	}
	timing.responses = time.Since(responsesStart)

	storeStart := time.Now()
	err = writer.StorePrecomputedProofResponses(ctx, proofs)
	timing.store = time.Since(storeStart)
	return timing, err
}

func precomputedProofBlockNumber(mode config.ShardingMode, block *models.Block) (uint64, error) {
	if block == nil {
		return 0, fmt.Errorf("missing block for precomputed proof")
	}
	if mode != config.ShardingModeChild {
		return block.Index.Uint64(), nil
	}
	if block.ParentBlockNumber == 0 {
		return 0, fmt.Errorf("child block %s is missing parent block number", block.Index.String())
	}
	return block.ParentBlockNumber, nil
}

func (rm *RoundManager) storeProposedBlockAndRecords(
	ctx context.Context,
	block *models.Block,
	records []*models.AggregatorRecord,
) error {
	block.Finalized = false
	block.Status = models.FinalityStatusProposed

	return rm.storage.WithTransaction(ctx, func(txCtx context.Context) error {
		if err := rm.storage.BlockStorage().Store(txCtx, block); err != nil {
			return fmt.Errorf("failed to store proposed block: %w", err)
		}
		if len(records) > 0 {
			if serial, ok := rm.storage.AggregatorRecordStorage().(aggregatorRecordSerialBatchStorage); ok {
				if err := serial.StoreBatchSerial(txCtx, records); err != nil {
					return fmt.Errorf("failed to store proposed aggregator records: %w", err)
				}
			} else {
				for _, record := range records {
					if err := rm.storage.AggregatorRecordStorage().Store(txCtx, record); err != nil {
						return fmt.Errorf("failed to store proposed aggregator record: %w", err)
					}
				}
			}
		}
		return nil
	})
}

func (rm *RoundManager) clearFinalizedRound(round *Round, block *models.Block) {
	rm.roundMutex.Lock()
	defer rm.roundMutex.Unlock()
	if rm.currentRound == round {
		rm.currentRound.Block = block
		rm.currentRound.PendingRootHash = nil
		rm.currentRound.PendingLeaves = nil
		rm.currentRound.PendingCommitments = nil
		rm.currentRound.Snapshot = nil
	}
}

func (rm *RoundManager) validateStoredDurableProposalBlock(ctx context.Context, block *models.Block) (*models.Block, error) {
	existingBlock, err := getBlockAnyFinalization(ctx, rm.storage, block.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to load durable proposal block %s: %w", block.Index.String(), err)
	}
	if existingBlock == nil {
		return nil, fmt.Errorf("durable proposal block %s not found before finalization", block.Index.String())
	}
	if !bytes.Equal(existingBlock.RootHash, block.RootHash) {
		return nil, fmt.Errorf("durable proposal block %s root mismatch: existing %s new %s",
			block.Index.String(), existingBlock.RootHash.String(), block.RootHash.String())
	}
	return existingBlock, nil
}

func sameStateIDs(a, b []api.StateID) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, stateID := range a {
		counts[stateID.String()]++
	}
	for _, stateID := range b {
		key := stateID.String()
		if counts[key] == 0 {
			return false
		}
		counts[key]--
	}
	return true
}

type storeDataTiming struct {
	total   time.Duration
	smt     time.Duration
	records time.Duration
}

type precomputeProofTiming struct {
	keys      time.Duration
	certs     time.Duration
	responses time.Duration
	store     time.Duration
}

func (rm *RoundManager) appendMemoryDiagnosticFields(ctx context.Context, fields []interface{}) []interface{} {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	proofPending, proofRecords, proofBlocks := rm.GetProofCacheStats()
	fields = append(fields,
		"memGoHeapAllocMB", bytesToMB(mem.HeapAlloc),
		"memGoHeapInuseMB", bytesToMB(mem.HeapInuse),
		"memGoHeapSysMB", bytesToMB(mem.HeapSys),
		"memGoStackInuseMB", bytesToMB(mem.StackInuse),
		"memGoSysMB", bytesToMB(mem.Sys),
		"memGoNextGCMB", bytesToMB(mem.NextGC),
		"memGoHeapObjects", mem.HeapObjects,
		"memGoNumGC", mem.NumGC,
		"memGoGoroutines", runtime.NumGoroutine(),
		"proofPending", proofPending,
		"proofCacheRecords", proofRecords,
		"proofCacheBlocks", proofBlocks,
		"proofCacheRecordCapacity", rm.config.SMT.ProofMetadataCacheEntries,
	)

	if rm.smtBackend == nil {
		return fields
	}

	stats := rm.smtBackend.Stats(ctx)
	storeMetrics, ok := stats.Raw["store_metrics"].(diskstorage.Metrics)
	if ok {
		fields = append(fields,
			"memRocksDBConfiguredCacheMB", rm.config.SMT.RocksDBCacheMB,
			"memRocksDBBlockCacheMB", signedBytesToMB(storeMetrics.BlockCacheSize),
			"memRocksDBMemTableMB", bytesToMB(storeMetrics.MemTableSize),
			"rocksDBPendingCompactionMB", bytesToMB(storeMetrics.CompactEstimatedDebt),
			"rocksDBL0Files", storeMetrics.L0NumFiles,
			"rocksDBBlockCacheHits", storeMetrics.BlockCacheHits,
			"rocksDBBlockCacheMisses", storeMetrics.BlockCacheMisses,
			"rocksDBBlockCacheDataHits", storeMetrics.BlockCacheDataHits,
			"rocksDBBlockCacheDataMisses", storeMetrics.BlockCacheDataMisses,
		)
	}

	nodeCache, ok := stats.Raw["node_cache"].(diskpersist.NodeCacheStats)
	if ok {
		fields = append(fields,
			"memDiskNodeCacheEnabled", nodeCache.Enabled,
			"memDiskNodeCacheEntries", nodeCache.Entries,
			"memDiskNodeCacheMB", signedBytesToMB(nodeCache.Bytes),
		)
	}

	return fields
}

func bytesToMB(bytes uint64) uint64 {
	return bytes / (1024 * 1024)
}

func signedBytesToMB(bytes int64) int64 {
	return bytes / (1024 * 1024)
}

// commitDiskSnapshotForFinalizedBlock advances the local RocksDB committed
// state only after Mongo has persisted the UC/finalized block. If the process
// crashes between the Mongo finalize and this commit, startup can replay the
// finalized block from Mongo. The reverse ordering can leave RocksDB ahead of
// Mongo with only an uncertified proposed block, which is not recoverable.
//
// Disk proof reads do not use finalizationMu: they read from the last published
// proof view. On success this returns a proof view captured at the RocksDB
// commit point; the caller publishes it only after Mongo finalization and proof
// metadata are ready.
func (rm *RoundManager) commitDiskSnapshotForFinalizedBlock(
	ctx context.Context,
	block *models.Block,
	snapshot smtbackend.Snapshot,
) (time.Duration, smtbackend.CommitTiming, smtbackend.PreparedProofView, error) {
	smtCommitStart := time.Now()
	var preparedProofView smtbackend.PreparedProofView
	var commitErr error
	if publishable, ok := snapshot.(smtbackend.ProofViewPreparingSnapshot); ok {
		preparedProofView, commitErr = publishable.CommitAndPrepareProofView(ctx, smtbackend.CommitMetadata{BlockNumber: block.Index, RootHash: block.RootHash})
	} else {
		commitErr = snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: block.Index, RootHash: block.RootHash})
	}
	smtCommitDuration := time.Since(smtCommitStart)
	var smtCommitTiming smtbackend.CommitTiming
	if timingProvider, ok := snapshot.(smtbackend.CommitTimingProvider); ok {
		smtCommitTiming = timingProvider.LastCommitTiming()
	}

	if commitErr != nil {
		if preparedProofView != nil {
			preparedProofView.Discard(ctx)
		}
		return smtCommitDuration, smtCommitTiming, nil, fmt.Errorf("failed to commit SMT snapshot: %w", commitErr)
	}

	return smtCommitDuration, smtCommitTiming, preparedProofView, nil
}

// storeDataParallel stores SMT nodes. Aggregator records are written once as a
// durable proposal before certification, never during finalization.
func (rm *RoundManager) storeDataParallel(
	ctx context.Context,
	smtNodes []*models.SmtNode,
) (storeDataTiming, error) {
	start := time.Now()

	var smtErr error
	var smtTime time.Duration

	if len(smtNodes) > 0 {
		t := time.Now()
		smtErr = rm.storage.SmtStorage().StoreBatch(ctx, smtNodes)
		smtTime = time.Since(t)
	}

	timing := storeDataTiming{
		total:   time.Since(start),
		smt:     smtTime,
		records: 0,
	}

	if smtErr != nil {
		return timing, fmt.Errorf("failed to store SMT nodes: %w", smtErr)
	}

	return timing, nil
}
