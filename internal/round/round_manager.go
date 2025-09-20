package round

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/bft"
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// RoundState represents the current state of a round
type RoundState int

const (
	RoundStateCollecting RoundState = iota // Collecting commitments
	RoundStateProcessing                   // Processing batch and computing SMT
	RoundStateFinalizing                   // Finalizing block
)

func (rs RoundState) String() string {
	switch rs {
	case RoundStateCollecting:
		return "collecting"
	case RoundStateProcessing:
		return "processing"
	case RoundStateFinalizing:
		return "finalizing"
	default:
		return "unknown"
	}
}

// Round represents a single aggregation round
type Round struct {
	Number      *api.BigInt
	StartTime   time.Time
	State       RoundState
	Commitments []*models.Commitment
	Block       *models.Block
	// Track commitments that have been added to SMT but not yet finalized in a block
	PendingRecords  []*models.AggregatorRecord
	PendingRootHash string
	// SMT snapshot for this round - allows accumulating changes before committing
	Snapshot *ThreadSafeSmtSnapshot
	// Store data for persistence during FinalizeBlock
	PendingLeaves []*smt.Leaf
}

// RoundManager handles the creation of blocks and processing of commitments
type RoundManager struct {
	config    *config.Config
	logger    *logger.Logger
	storage   interfaces.Storage
	smt       *ThreadSafeSMT
	bftClient bft.BFTClient

	// Round management
	currentRound *Round
	roundMutex   sync.RWMutex
	roundTimer   *time.Timer
	stopChan     chan struct{}
	wg           sync.WaitGroup

	// Round duration (configurable, default 1 second)
	roundDuration time.Duration

	// Metrics
	totalRounds      int64
	totalCommitments int64
}

// NewRoundManager creates a new round manager
func NewRoundManager(cfg *config.Config, logger *logger.Logger, storage interfaces.Storage) (*RoundManager, error) {
	// Initialize SMT with empty tree - will be replaced with restored tree in Start()
	smtInstance := smt.NewSparseMerkleTree(api.SHA256)
	threadSafeSMT := NewThreadSafeSMT(smtInstance)

	rm := &RoundManager{
		config:        cfg,
		logger:        logger,
		storage:       storage,
		smt:           threadSafeSMT,
		stopChan:      make(chan struct{}),
		roundDuration: cfg.Processing.RoundDuration, // Configurable round duration (default 1s)
	}

	if cfg.BFT.Enabled {
		var err error
		rm.bftClient, err = bft.NewBFTClient(context.Background(), &cfg.BFT, logger, rm)
		if err != nil {
			return nil, fmt.Errorf("failed to create BFT client: %w", err)
		}
	} else {
		rm.bftClient = bft.NewBFTClientStub(logger, rm)
	}

	return rm, nil
}

// Start begins the round manager operation
func (rm *RoundManager) Start(ctx context.Context) error {
	rm.logger.WithContext(ctx).Info("Starting Round Manager",
		"roundDuration", rm.roundDuration.String(),
		"batchLimit", rm.config.Processing.BatchLimit)

	// Restore SMT from storage - this will populate the existing empty SMT
	if err := rm.restoreSmtFromStorage(ctx); err != nil {
		return fmt.Errorf("failed to restore SMT from storage: %w", err)
	}

	// Ensure any previous timers are stopped
	rm.roundMutex.Lock()
	if rm.roundTimer != nil {
		rm.roundTimer.Stop()
		rm.roundTimer = nil
	}
	rm.roundMutex.Unlock()

	// Get latest block number to determine starting round
	latestBlockNumber, err := rm.storage.BlockStorage().GetLatestNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block number: %w", err)
	}

	// Initialize first round (start from next block number)
	nextRoundNumber := api.NewBigInt(nil)
	if latestBlockNumber != nil && latestBlockNumber.Int != nil {
		// If blocks exist, start from latest + 1
		nextRoundNumber.Set(latestBlockNumber.Int)
		nextRoundNumber.Add(nextRoundNumber.Int, big.NewInt(1))
		rm.logger.WithContext(ctx).Info("Starting from existing blockchain state",
			"latestBlock", latestBlockNumber.String(),
			"nextRound", nextRoundNumber.String())
	} else {
		// If no blocks exist, start from 1 (not 0)
		nextRoundNumber.SetInt64(1)
		rm.logger.WithContext(ctx).Info("No existing blocks found, starting from block 1")
	}

	// Keep checking until we find a block number that doesn't exist
	for {
		existingBlock, err := rm.storage.BlockStorage().GetByNumber(ctx, nextRoundNumber)
		if err != nil {
			return fmt.Errorf("failed to check if block %s exists: %w", nextRoundNumber.String(), err)
		}
		if existingBlock == nil {
			// Found a gap - this is our next block number
			break
		}

		rm.logger.WithContext(ctx).Debug("Block already exists, incrementing to find next available number",
			"blockNumber", nextRoundNumber.String())
		nextRoundNumber.Add(nextRoundNumber.Int, big.NewInt(1))
	}

	rm.logger.WithContext(ctx).Info("Found next available block number for new round",
		"finalRoundNumber", nextRoundNumber.String())

	if err := rm.bftClient.Start(ctx, nextRoundNumber); err != nil {
		return fmt.Errorf("failed to start BFT client: %w", err)
	}

	// Start the round processing goroutine
	rm.wg.Add(1)
	go rm.roundProcessor(ctx)

	return nil
}

// Stop gracefully stops the round manager
func (rm *RoundManager) Stop(ctx context.Context) error {
	rm.logger.WithContext(ctx).Info("Stopping Round Manager")

	// Signal stop
	close(rm.stopChan)

	// Stop current round timer
	rm.roundMutex.Lock()
	if rm.roundTimer != nil {
		rm.roundTimer.Stop()
	}
	rm.roundMutex.Unlock()

	// Wait for goroutines to finish
	rm.wg.Wait()

	rm.logger.WithContext(ctx).Info("Round Manager stopped")
	return nil
}

// GetCurrentRound returns information about the current round
func (rm *RoundManager) GetCurrentRound() *Round {
	rm.roundMutex.RLock()
	defer rm.roundMutex.RUnlock()

	if rm.currentRound == nil {
		return nil
	}

	// Return a copy to avoid race conditions
	return &Round{
		Number:      rm.currentRound.Number,
		StartTime:   rm.currentRound.StartTime,
		State:       rm.currentRound.State,
		Commitments: append([]*models.Commitment(nil), rm.currentRound.Commitments...),
		Block:       rm.currentRound.Block,
	}
}

// GetSMT returns the thread-safe SMT instance for inclusion proof generation
func (rm *RoundManager) GetSMT() *ThreadSafeSMT {
	return rm.smt
}

// GetStats returns round manager statistics
func (rm *RoundManager) GetStats() map[string]interface{} {
	rm.roundMutex.RLock()
	defer rm.roundMutex.RUnlock()

	stats := map[string]interface{}{
		"totalRounds":      rm.totalRounds,
		"totalCommitments": rm.totalCommitments,
		"roundDuration":    rm.roundDuration.String(),
	}

	if rm.currentRound != nil {
		stats["currentRound"] = map[string]interface{}{
			"number":          rm.currentRound.Number.String(),
			"state":           rm.currentRound.State.String(),
			"startTime":       rm.currentRound.StartTime,
			"commitmentCount": len(rm.currentRound.Commitments),
			"age":             time.Since(rm.currentRound.StartTime).String(),
		}
	}

	return stats
}

// StartNewRound initializes a new round
func (rm *RoundManager) StartNewRound(ctx context.Context, roundNumber *api.BigInt) error {
	rm.logger.WithContext(ctx).Info("StartNewRound called",
		"roundNumber", roundNumber.String())

	rm.roundMutex.Lock()
	defer rm.roundMutex.Unlock()

	// Log previous round state if exists
	if rm.currentRound != nil {
		rm.logger.WithContext(ctx).Info("Previous round state",
			"previousRoundNumber", rm.currentRound.Number.String(),
			"previousRoundState", rm.currentRound.State.String(),
			"previousRoundAge", time.Since(rm.currentRound.StartTime).String())

		// Check if we're skipping rounds (root chain timeout scenario)
		if rm.currentRound.Number != nil && roundNumber.Cmp(rm.currentRound.Number.Int) > 1 {
			skippedRounds := new(big.Int).Sub(roundNumber.Int, rm.currentRound.Number.Int)
			skippedRounds.Sub(skippedRounds, big.NewInt(1))
			rm.logger.WithContext(ctx).Warn("Skipping rounds due to root chain timeout",
				"currentRound", rm.currentRound.Number.String(),
				"newRound", roundNumber.String(),
				"skippedRounds", skippedRounds.String())
		}
	}

	// Stop any existing timer
	if rm.roundTimer != nil {
		rm.logger.WithContext(ctx).Debug("Stopping existing round timer")
		rm.roundTimer.Stop()
	}

	rm.currentRound = &Round{
		Number:      roundNumber,
		StartTime:   time.Now(),
		State:       RoundStateCollecting,
		Commitments: make([]*models.Commitment, 0),
		Snapshot:    rm.smt.CreateSnapshot(), // Create snapshot for this round
	}

	// Start round timer
	rm.roundTimer = time.AfterFunc(rm.roundDuration, func() {
		rm.logger.WithContext(ctx).Info("Round timer fired",
			"roundNumber", roundNumber.String(),
			"elapsed", rm.roundDuration.String())
		if err := rm.processCurrentRound(ctx); err != nil {
			rm.logger.WithContext(ctx).Error("Failed to process round",
				"roundNumber", roundNumber.String(),
				"error", err.Error())
		}
	})

	rm.logger.WithContext(ctx).Info("Started new round",
		"roundNumber", roundNumber.String(),
		"duration", rm.roundDuration.String(),
		"timerSet", rm.roundTimer != nil)

	return nil
}

// processCurrentRound processes the current round and creates a block
func (rm *RoundManager) processCurrentRound(ctx context.Context) error {
	rm.logger.WithContext(ctx).Info("processCurrentRound called")

	rm.roundMutex.Lock()
	if rm.currentRound == nil {
		rm.roundMutex.Unlock()
		rm.logger.WithContext(ctx).Error("No current round to process")
		return fmt.Errorf("no current round to process")
	}

	// Log current round state
	rm.logger.WithContext(ctx).Info("Current round state before processing",
		"roundNumber", rm.currentRound.Number.String(),
		"state", rm.currentRound.State.String(),
		"age", time.Since(rm.currentRound.StartTime).String())

	// Check if round is already being processed
	if rm.currentRound.State != RoundStateCollecting {
		rm.roundMutex.Unlock()
		rm.logger.WithContext(ctx).Warn("Round already being processed, skipping",
			"roundNumber", rm.currentRound.Number.String(),
			"state", rm.currentRound.State.String())
		return nil
	}

	// Change state to processing
	rm.currentRound.State = RoundStateProcessing
	roundNumber := rm.currentRound.Number
	rm.logger.WithContext(ctx).Info("Changed round state to processing",
		"roundNumber", roundNumber.String())

	// Get any unprocessed commitments from storage
	var err error
	rm.currentRound.Commitments, err = rm.storage.CommitmentStorage().GetUnprocessedBatch(ctx, rm.config.Processing.BatchLimit)
	if err != nil {
		rm.logger.WithContext(ctx).Error("Failed to get unprocessed commitments",
			"roundNumber", roundNumber.String(),
			"error", err.Error())
		rm.currentRound.Commitments = []*models.Commitment{}
	}

	rm.logger.WithContext(ctx).Info("Processing round",
		"roundNumber", roundNumber.String(),
		"commitmentCount", len(rm.currentRound.Commitments),
		"batchLimit", rm.config.Processing.BatchLimit)
	rm.roundMutex.Unlock()

	// Process commitments in batch
	var rootHash string
	if len(rm.currentRound.Commitments) > 0 {
		// Process batch and add to SMT, but DO NOT mark as processed yet
		rootHash, err = rm.processBatch(ctx, rm.currentRound.Commitments, roundNumber)
		if err != nil {
			rm.logger.WithContext(ctx).Error("Failed to process batch", "error", err.Error())
			return fmt.Errorf("failed to process batch: %w", err)
		}

		// Store pending data in the round for later finalization
		rm.roundMutex.Lock()
		rm.currentRound.PendingRootHash = rootHash
		rm.roundMutex.Unlock()

		rm.logger.WithContext(ctx).Info("Batch processed and added to SMT, awaiting finalization",
			"roundNumber", roundNumber.String(),
			"commitmentCount", len(rm.currentRound.Commitments),
			"rootHash", rootHash)
	} else {
		// Empty round - use previous root hash or empty hash
		rootHash = rm.smt.GetRootHash()
	}

	// Create and propose block
	rm.logger.WithContext(ctx).Info("Proposing block",
		"roundNumber", roundNumber.String(),
		"rootHash", rootHash)

	if err := rm.proposeBlock(ctx, roundNumber, rootHash); err != nil {
		rm.logger.WithContext(ctx).Error("Failed to propose block",
			"roundNumber", roundNumber.String(),
			"error", err.Error())
		return fmt.Errorf("failed to propose block: %w", err)
	}

	// Update stats
	rm.totalRounds++
	rm.totalCommitments += int64(len(rm.currentRound.Commitments))

	rm.logger.WithContext(ctx).Info("Round processing completed successfully",
		"roundNumber", roundNumber.String(),
		"totalRounds", rm.totalRounds,
		"totalCommitments", rm.totalCommitments)

	return nil
}

// roundProcessor is the main goroutine that handles round processing
func (rm *RoundManager) roundProcessor(ctx context.Context) {
	defer rm.wg.Done()

	rm.logger.WithContext(ctx).Info("Round processor started")

	for {
		select {
		case <-rm.stopChan:
			rm.logger.WithContext(ctx).Info("Round processor stopping")
			return
		case <-ctx.Done():
			rm.logger.WithContext(ctx).Info("Round processor context cancelled")
			return
		}
	}
}

// restoreSmtFromStorage restores the SMT tree from persisted nodes in storage
func (rm *RoundManager) restoreSmtFromStorage(ctx context.Context) error {
	rm.logger.Info("Starting SMT restoration from storage")

	// Get total count for progress tracking
	totalCount, err := rm.storage.SmtStorage().Count(ctx)
	if err != nil {
		return fmt.Errorf("failed to get SMT node count: %w", err)
	}

	if totalCount == 0 {
		rm.logger.Info("No SMT nodes found in storage, starting with empty tree")
		return nil
	}

	rm.logger.Info("Found SMT nodes in storage, starting restoration", "totalNodes", totalCount)

	const chunkSize = 1000
	offset := 0
	restoredCount := 0

	for {
		// Load chunk of nodes
		nodes, err := rm.storage.SmtStorage().GetChunked(ctx, offset, chunkSize)
		if err != nil {
			return fmt.Errorf("failed to load SMT chunk at offset %d: %w", offset, err)
		}

		if len(nodes) == 0 {
			break // No more data
		}

		// Convert storage nodes to SMT leaves
		leaves := make([]*smt.Leaf, len(nodes))
		for i, node := range nodes {
			// Convert key bytes back to big.Int path
			path := new(big.Int).SetBytes(node.Key)
			leaves[i] = &smt.Leaf{
				Path:  path,
				Value: node.Value,
			}
		}

		if _, err := rm.smt.AddLeaves(leaves); err != nil {
			return fmt.Errorf("failed to restore SMT leaves at offset %d: %w", offset, err)
		}

		restoredCount += len(nodes)
		rm.logger.Info("Restored SMT chunk",
			"offset", offset,
			"chunkSize", len(nodes),
			"restoredCount", restoredCount,
			"totalCount", totalCount,
			"progress", fmt.Sprintf("%.1f%%", float64(restoredCount)/float64(totalCount)*100))

		offset += len(nodes)

		if len(nodes) < chunkSize {
			break // Last chunk
		}
	}

	// Log final state
	finalRootHash := rm.smt.GetRootHash()
	rm.logger.Info("SMT restoration complete",
		"restoredNodes", restoredCount,
		"totalNodes", totalCount,
		"finalRootHash", finalRootHash)

	if restoredCount != int(totalCount) {
		rm.logger.Warn("SMT restoration count mismatch",
			"expected", totalCount,
			"restored", restoredCount)
	}

	// Verify restored SMT root hash matches latest block's root hash
	latestBlock, err := rm.storage.BlockStorage().GetLatest(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block for SMT verification: %w", err)
	} else if latestBlock == nil {
		rm.logger.Info("No latest block found, skipping SMT verification")
	} else {
		expectedRootHash := latestBlock.RootHash.String()
		if finalRootHash != expectedRootHash {
			rm.logger.Error("SMT restoration verification failed - root hash mismatch",
				"restoredRootHash", finalRootHash,
				"expectedRootHash", expectedRootHash,
				"latestBlockNumber", latestBlock.Index.String())
			return fmt.Errorf("SMT restoration verification failed: restored root hash %s does not match latest block root hash %s",
				finalRootHash, expectedRootHash)
		}
		rm.logger.Info("SMT restoration verified successfully - root hash matches latest block",
			"rootHash", finalRootHash,
			"latestBlockNumber", latestBlock.Index.String())
	}

	return nil
}
