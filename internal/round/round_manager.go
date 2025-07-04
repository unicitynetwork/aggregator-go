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
	// Initialize SMT with thread-safe wrapper
	smtInstance := smt.NewSparseMerkleTree(smt.SHA256)
	threadSafeSMT := NewThreadSafeSMT(smtInstance)

	rm := &RoundManager{
		config:        cfg,
		logger:        logger,
		storage:       storage,
		smt:           threadSafeSMT,
		stopChan:      make(chan struct{}),
		roundDuration: time.Second, // 1 second rounds
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
			rm.logger.WithContext(ctx).Error("Failed to process round", "error", err.Error())
		}
	})

	rm.logger.WithContext(ctx).Info("Started new round",
		"roundNumber", roundNumber.String(),
		"duration", rm.roundDuration.String())

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
		rm.logger.WithContext(ctx).Debug("Round already being processed, skipping",
			"roundNumber", rm.currentRound.Number.String(),
			"state", rm.currentRound.State.String())
		return nil
	}

	// Change state to processing
	rm.currentRound.State = RoundStateProcessing
	roundNumber := rm.currentRound.Number

	// Get any unprocessed commitments from storage
	var err error
	rm.currentRound.Commitments, err = rm.storage.CommitmentStorage().GetUnprocessedBatch(ctx, rm.config.Processing.BatchLimit)
	if err != nil {
		rm.logger.WithContext(ctx).Warn("Failed to get unprocessed commitments", "error", err.Error())
		rm.currentRound.Commitments = []*models.Commitment{}
	}

	rm.logger.WithContext(ctx).Info("Processing round",
		"roundNumber", roundNumber.String(),
		"commitmentCount", len(rm.currentRound.Commitments))
	rm.roundMutex.Unlock()

	// Process commitments in batch
	var rootHash string
	var records []*models.AggregatorRecord

	if len(rm.currentRound.Commitments) > 0 {
		// Mark commitments as processed FIRST to prevent duplicate processing
		requestIDs := make([]api.RequestID, len(rm.currentRound.Commitments))
		for i, commitment := range rm.currentRound.Commitments {
			requestIDs[i] = commitment.RequestID
		}

		if err := rm.storage.CommitmentStorage().MarkProcessed(ctx, requestIDs); err != nil {
			rm.logger.WithContext(ctx).Error("Failed to mark commitments as processed", "error", err.Error())
			return fmt.Errorf("failed to mark commitments as processed: %w", err)
		}

		rootHash, records, err = rm.processBatch(ctx, rm.currentRound.Commitments, roundNumber)
		if err != nil {
			rm.logger.WithContext(ctx).Error("Failed to process batch, but commitments are already marked as processed", "error", err.Error())
			return fmt.Errorf("failed to process batch: %w", err)
		}
	} else {
		// Empty round - use previous root hash or empty hash
		rootHash = rm.smt.GetRootHash()
		records = make([]*models.AggregatorRecord, 0)
	}

	// Create and propose block
	if err := rm.proposeBlock(ctx, roundNumber, rootHash, records); err != nil {
		return fmt.Errorf("failed to propose block: %w", err)
	}

	// Update stats
	rm.totalRounds++
	rm.totalCommitments += int64(len(rm.currentRound.Commitments))

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
