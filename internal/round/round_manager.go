package round

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
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
	Number      *models.BigInt
	StartTime   time.Time
	State       RoundState
	Commitments []*models.Commitment
	Block       *models.Block
}

// RoundManager handles the creation of blocks and processing of commitments
type RoundManager struct {
	config       *config.Config
	logger       *logger.Logger
	storage      interfaces.Storage
	smt          *ThreadSafeSMT
	
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
func NewRoundManager(cfg *config.Config, logger *logger.Logger, storage interfaces.Storage) *RoundManager {
	// Initialize SMT with thread-safe wrapper
	smtInstance := smt.NewSparseMerkleTree(smt.SHA256)
	threadSafeSMT := NewThreadSafeSMT(smtInstance)

	return &RoundManager{
		config:        cfg,
		logger:        logger,
		storage:       storage,
		smt:           threadSafeSMT,
		stopChan:      make(chan struct{}),
		roundDuration: time.Second, // 1 second rounds
	}
}

// Start begins the round manager operation
func (rm *RoundManager) Start(ctx context.Context) error {
	rm.logger.WithContext(ctx).Info("Starting Round Manager")

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
	nextRoundNumber := models.NewBigInt(nil)
	if latestBlockNumber != nil && latestBlockNumber.Int != nil {
		// If blocks exist, start from latest + 1
		nextRoundNumber.Set(latestBlockNumber.Int)
		nextRoundNumber.Add(nextRoundNumber.Int, big.NewInt(1))
		rm.logger.WithContext(ctx).
			WithField("latestBlock", latestBlockNumber.String()).
			WithField("nextRound", nextRoundNumber.String()).
			Info("Starting from existing blockchain state")
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
		
		rm.logger.WithContext(ctx).
			WithField("blockNumber", nextRoundNumber.String()).
			Debug("Block already exists, incrementing to find next available number")
		nextRoundNumber.Add(nextRoundNumber.Int, big.NewInt(1))
	}
	
	rm.logger.WithContext(ctx).
		WithField("finalRoundNumber", nextRoundNumber.String()).
		Info("Found next available block number for new round")
	
	if err := rm.startNewRound(ctx, nextRoundNumber); err != nil {
		return fmt.Errorf("failed to start initial round: %w", err)
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

// AddCommitment adds a commitment to the current round
func (rm *RoundManager) AddCommitment(ctx context.Context, commitment *models.Commitment) error {
	rm.roundMutex.Lock()
	defer rm.roundMutex.Unlock()

	if rm.currentRound == nil {
		return fmt.Errorf("no active round")
	}

	if rm.currentRound.State != RoundStateCollecting {
		return fmt.Errorf("round %s is not collecting commitments (state: %s)", 
			rm.currentRound.Number.String(), rm.currentRound.State.String())
	}

	// Add commitment to current round
	rm.currentRound.Commitments = append(rm.currentRound.Commitments, commitment)
	
	rm.logger.WithContext(ctx).
		WithField("roundNumber", rm.currentRound.Number.String()).
		WithField("commitmentCount", len(rm.currentRound.Commitments)).
		WithField("requestId", commitment.RequestID.String()).
		Debug("Added commitment to round")

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

// startNewRound initializes a new round
func (rm *RoundManager) startNewRound(ctx context.Context, roundNumber *models.BigInt) error {
	rm.roundMutex.Lock()
	defer rm.roundMutex.Unlock()

	// Stop any existing timer
	if rm.roundTimer != nil {
		rm.roundTimer.Stop()
	}

	rm.currentRound = &Round{
		Number:      roundNumber,
		StartTime:   time.Now(),
		State:       RoundStateCollecting,
		Commitments: make([]*models.Commitment, 0),
	}

	// Start round timer
	rm.roundTimer = time.AfterFunc(rm.roundDuration, func() {
		if err := rm.processCurrentRound(ctx); err != nil {
			rm.logger.WithContext(ctx).WithError(err).Error("Failed to process round")
		}
	})

	rm.logger.WithContext(ctx).
		WithField("roundNumber", roundNumber.String()).
		WithField("duration", rm.roundDuration.String()).
		Info("Started new round")

	return nil
}

// processCurrentRound processes the current round and creates a block
func (rm *RoundManager) processCurrentRound(ctx context.Context) error {
	rm.roundMutex.Lock()
	if rm.currentRound == nil {
		rm.roundMutex.Unlock()
		return fmt.Errorf("no current round to process")
	}

	// Check if round is already being processed
	if rm.currentRound.State != RoundStateCollecting {
		rm.roundMutex.Unlock()
		rm.logger.WithContext(ctx).
			WithField("roundNumber", rm.currentRound.Number.String()).
			WithField("state", rm.currentRound.State.String()).
			Debug("Round already being processed, skipping")
		return nil
	}

	// Change state to processing
	rm.currentRound.State = RoundStateProcessing
	commitments := append([]*models.Commitment(nil), rm.currentRound.Commitments...)
	roundNumber := rm.currentRound.Number
	rm.roundMutex.Unlock()

	rm.logger.WithContext(ctx).
		WithField("roundNumber", roundNumber.String()).
		WithField("commitmentCount", len(commitments)).
		Info("Processing round")

	// Get any unprocessed commitments from storage in addition to round commitments
	const batchLimit = 1000 // Limit to prevent memory issues
	unprocessedCommitments, err := rm.storage.CommitmentStorage().GetUnprocessedBatch(ctx, batchLimit)
	if err != nil {
		rm.logger.WithContext(ctx).WithError(err).Warn("Failed to get unprocessed commitments")
		unprocessedCommitments = []*models.Commitment{}
	}
	
	// Combine round commitments with unprocessed commitments
	allCommitments := append(commitments, unprocessedCommitments...)
	
	rm.logger.WithContext(ctx).
		WithField("roundCommitments", len(commitments)).
		WithField("unprocessedCommitments", len(unprocessedCommitments)).
		WithField("totalCommitments", len(allCommitments)).
		Info("Processing commitments batch")

	// Process commitments in batch
	var rootHash string
	var records []*models.AggregatorRecord

	if len(allCommitments) > 0 {
		// Mark commitments as processed FIRST to prevent duplicate processing
		requestIDs := make([]models.RequestID, len(allCommitments))
		for i, commitment := range allCommitments {
			requestIDs[i] = commitment.RequestID
		}
		
		if err := rm.storage.CommitmentStorage().MarkProcessed(ctx, requestIDs); err != nil {
			rm.logger.WithContext(ctx).WithError(err).Error("Failed to mark commitments as processed")
			return fmt.Errorf("failed to mark commitments as processed: %w", err)
		}
		
		rootHash, records, err = rm.processBatch(ctx, allCommitments, roundNumber)
		if err != nil {
			rm.logger.WithContext(ctx).WithError(err).Error("Failed to process batch, but commitments are already marked as processed")
			return fmt.Errorf("failed to process batch: %w", err)
		}
	} else {
		// Empty round - use previous root hash or empty hash
		rootHash = rm.smt.GetRootHash()
		records = make([]*models.AggregatorRecord, 0)
	}

	// Create and finalize block
	if err := rm.finalizeBlock(ctx, roundNumber, rootHash, records); err != nil {
		return fmt.Errorf("failed to finalize block: %w", err)
	}

	// Update stats
	rm.totalRounds++
	rm.totalCommitments += int64(len(allCommitments))

	// Start next round
	nextRoundNumber := models.NewBigInt(nil)
	nextRoundNumber.Set(roundNumber.Int)
	nextRoundNumber.Add(nextRoundNumber.Int, big.NewInt(1))
	
	return rm.startNewRound(ctx, nextRoundNumber)
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