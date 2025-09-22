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
	// Timing metrics for this round
	ProcessingTime      time.Duration
	ProposalTime        time.Time // When block was proposed to BFT
	FinalizationTime    time.Time // When block was actually finalized
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

	// Streaming support
	commitmentStream chan *models.Commitment
	streamMutex      sync.RWMutex
	lastFetchedID    string // Cursor for pagination

	// Adaptive throughput tracking
	avgProcessingRate float64 // commitments per millisecond
	lastRoundMetrics  RoundMetrics

	// Adaptive timing
	avgFinalizationTime time.Duration // Running average of finalization time
	avgSMTUpdateTime    time.Duration // Running average of SMT update time per batch
	processingRatio     float64       // Ratio of round duration for processing (starts at 0.9)

	// Metrics
	totalRounds      int64
	totalCommitments int64
}

// RoundMetrics tracks performance metrics for a round
type RoundMetrics struct {
	CommitmentsProcessed int
	ProcessingTime       time.Duration
	RoundNumber          *api.BigInt
	Timestamp            time.Time
}

// NewRoundManager creates a new round manager
func NewRoundManager(cfg *config.Config, logger *logger.Logger, storage interfaces.Storage) (*RoundManager, error) {
	// Initialize SMT with empty tree - will be replaced with restored tree in Start()
	smtInstance := smt.NewSparseMerkleTree(api.SHA256)
	threadSafeSMT := NewThreadSafeSMT(smtInstance)

	rm := &RoundManager{
		config:              cfg,
		logger:              logger,
		storage:             storage,
		smt:                 threadSafeSMT,
		stopChan:            make(chan struct{}),
		roundDuration:       cfg.Processing.RoundDuration,        // Configurable round duration (default 1s)
		commitmentStream:    make(chan *models.Commitment, 3000), // Reasonable buffer for streaming
		avgProcessingRate:   1.0,                                 // Initial estimate: 1 commitment per ms
		processingRatio:     0.7,                                 // Start with 70% of round for processing (conservative but good throughput)
		avgFinalizationTime: 200 * time.Millisecond,              // Initial estimate (conservative)
		avgSMTUpdateTime:    5 * time.Millisecond,                // Initial estimate per batch
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

	// Reset cursor on startup
	rm.lastFetchedID = ""
	rm.logger.WithContext(ctx).Info("Reset commitment cursor")

	// Start the commitment stream prefetcher
	rm.wg.Add(1)
	go rm.commitmentPrefetcher(ctx)

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

// GetStreamingMetrics returns metrics about the streaming performance
func (rm *RoundManager) GetStreamingMetrics() map[string]interface{} {
	rm.streamMutex.RLock()
	defer rm.streamMutex.RUnlock()

	channelUtilization := float64(len(rm.commitmentStream)) / float64(cap(rm.commitmentStream)) * 100
	processingMs := time.Duration(float64(rm.roundDuration) * rm.processingRatio).Milliseconds()
	finalizationMs := time.Duration(float64(rm.roundDuration) * (1 - rm.processingRatio)).Milliseconds()

	return map[string]interface{}{
		"avgProcessingRate":       rm.avgProcessingRate,
		"targetCommitmentsPerSec": int(rm.avgProcessingRate * 1000),
		"channelSize":             len(rm.commitmentStream),
		"channelCapacity":         cap(rm.commitmentStream),
		"channelUtilization":      fmt.Sprintf("%.2f%%", channelUtilization),
		"adaptiveTiming": map[string]interface{}{
			"processingRatio":     fmt.Sprintf("%.2f", rm.processingRatio),
			"processingWindow":    fmt.Sprintf("%dms", processingMs),
			"finalizationWindow":  fmt.Sprintf("%dms", finalizationMs),
			"avgFinalizationTime": rm.avgFinalizationTime.String(),
			"avgSMTUpdateTime":    rm.avgSMTUpdateTime.String(),
		},
		"lastRound": map[string]interface{}{
			"number":               rm.lastRoundMetrics.RoundNumber,
			"commitmentsProcessed": rm.lastRoundMetrics.CommitmentsProcessed,
			"processingTime":       rm.lastRoundMetrics.ProcessingTime.String(),
			"timestamp":            rm.lastRoundMetrics.Timestamp,
		},
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

// processCurrentRound processes the current round using streaming approach with time bounds
func (rm *RoundManager) processCurrentRound(ctx context.Context) error {
	rm.logger.WithContext(ctx).Info("processCurrentRound called (streaming mode)")

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

	// Initialize commitments slice for this round
	// Note: Any commitments consumed from the channel MUST be processed in this round
	rm.currentRound.Commitments = make([]*models.Commitment, 0, 10000) // Larger pre-allocation for high throughput

	// Calculate adaptive processing deadline based on historical data
	processingDuration := time.Duration(float64(rm.roundDuration) * rm.processingRatio)
	rm.logger.WithContext(ctx).Debug("Using adaptive processing deadline",
		"roundDuration", rm.roundDuration,
		"processingRatio", rm.processingRatio,
		"processingDuration", processingDuration,
		"expectedFinalizationTime", rm.avgFinalizationTime)

	rm.roundMutex.Unlock()

	// Process commitments with streaming until adaptive deadline
	processingDeadline := time.Now().Add(processingDuration)
	commitmentsProcessed := 0
	processingStart := time.Now()
	smtUpdateTime := time.Duration(0)

	// Stream and process commitments until deadline
ProcessLoop:
	for time.Now().Before(processingDeadline) {
		select {
		case commitment := <-rm.commitmentStream:
			// Add commitment to current round
			rm.roundMutex.Lock()
			rm.currentRound.Commitments = append(rm.currentRound.Commitments, commitment)
			commitmentsProcessed++

			// Process in mini-batches for SMT efficiency
			if len(rm.currentRound.Commitments)%100 == 0 {
				// Process this mini-batch into SMT
				batchStart := time.Now()
				if err := rm.processMiniBatch(ctx, rm.currentRound.Commitments[len(rm.currentRound.Commitments)-100:]); err != nil {
					rm.logger.WithContext(ctx).Error("Failed to process mini-batch",
						"error", err.Error(),
						"roundNumber", roundNumber.String())
				}
				smtUpdateTime += time.Since(batchStart)
			}
			rm.roundMutex.Unlock()

		case <-ctx.Done():
			return ctx.Err()

		default:
			// No commitment immediately available
			remainingTime := time.Until(processingDeadline)
			if remainingTime < 50*time.Millisecond {
				// Not enough time to wait for more
				break ProcessLoop
			}
			// Wait a bit for more commitments
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Process any remaining commitments not in a full mini-batch
	rm.roundMutex.Lock()
	lastBatchStart := (commitmentsProcessed / 100) * 100
	if lastBatchStart < len(rm.currentRound.Commitments) {
		batchStart := time.Now()
		if err := rm.processMiniBatch(ctx, rm.currentRound.Commitments[lastBatchStart:]); err != nil {
			rm.logger.WithContext(ctx).Error("Failed to process final mini-batch",
				"error", err.Error(),
				"roundNumber", roundNumber.String())
		}
		smtUpdateTime += time.Since(batchStart)
	}

	// Calculate and track metrics
	processingTime := time.Since(processingStart)
	if processingTime.Milliseconds() > 0 {
		rm.avgProcessingRate = float64(commitmentsProcessed) / float64(processingTime.Milliseconds())
	}

	// Update average SMT update time (exponential moving average)
	if commitmentsProcessed > 0 {
		avgBatchTime := smtUpdateTime / time.Duration((commitmentsProcessed+99)/100) // Number of batches
		rm.avgSMTUpdateTime = (rm.avgSMTUpdateTime*4 + avgBatchTime) / 5             // Weight towards recent: 80/20
	}

	rm.lastRoundMetrics = RoundMetrics{
		CommitmentsProcessed: commitmentsProcessed,
		ProcessingTime:       processingTime,
		RoundNumber:          roundNumber,
		Timestamp:            time.Now(),
	}

	rm.logger.WithContext(ctx).Info("Streaming round processing complete",
		"roundNumber", roundNumber.String(),
		"commitmentsProcessed", commitmentsProcessed,
		"processingTime", processingTime,
		"smtUpdateTime", smtUpdateTime,
		"rate", fmt.Sprintf("%.2f commitments/ms", rm.avgProcessingRate),
		"targetRPS", int(rm.avgProcessingRate*1000))
	rm.roundMutex.Unlock()

	// Finalize SMT processing and get root hash
	var rootHash string
	if len(rm.currentRound.Commitments) > 0 {
		// Get the root hash from the snapshot (already processed via streaming)
		rm.roundMutex.RLock()
		if rm.currentRound.Snapshot != nil {
			rootHash = rm.currentRound.Snapshot.GetRootHash()
		}
		rm.roundMutex.RUnlock()

		// Store pending data in the round for later finalization
		rm.roundMutex.Lock()
		rm.currentRound.PendingRootHash = rootHash
		rm.roundMutex.Unlock()

		rm.logger.WithContext(ctx).Info("Round streaming complete, awaiting finalization",
			"roundNumber", roundNumber.String(),
			"commitmentCount", len(rm.currentRound.Commitments),
			"rootHash", rootHash)
	} else {
		// Empty round - use previous root hash or empty hash
		rootHash = rm.smt.GetRootHash()
		rm.logger.WithContext(ctx).Info("Empty round, using existing root hash",
			"roundNumber", roundNumber.String(),
			"rootHash", rootHash)
	}

	// Store processing time and proposal time for this round
	rm.roundMutex.Lock()
	if rm.currentRound != nil {
		rm.currentRound.ProcessingTime = processingTime
		rm.currentRound.ProposalTime = time.Now()
	}
	rm.roundMutex.Unlock()

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

	// Note: Actual finalization time will be tracked in FinalizeBlock when UC arrives
	// For now, we use the average finalization time for adaptive calculations
	rm.adjustProcessingRatio(ctx, processingTime, rm.avgFinalizationTime)

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

// commitmentPrefetcher continuously fetches commitments from storage and feeds them into the stream
func (rm *RoundManager) commitmentPrefetcher(ctx context.Context) {
	defer rm.wg.Done()

	rm.logger.WithContext(ctx).Info("Commitment prefetcher started")

	ticker := time.NewTicker(10 * time.Millisecond) // Check more frequently for high throughput
	defer ticker.Stop()

	for {
		select {
		case <-rm.stopChan:
			rm.logger.WithContext(ctx).Info("Commitment prefetcher stopping")
			return
		case <-ctx.Done():
			rm.logger.WithContext(ctx).Info("Commitment prefetcher context cancelled")
			return
		case <-ticker.C:
			// Check channel space - be conservative to avoid race conditions
			channelSpace := cap(rm.commitmentStream) - len(rm.commitmentStream)

			if channelSpace > 100 { // Only fetch if we have reasonable space
				// Get current cursor
				rm.streamMutex.RLock()
				cursor := rm.lastFetchedID
				rm.streamMutex.RUnlock()

				// Fetch only what we can fit, with some buffer for concurrent consumption
				// Fetch smaller batches to avoid overwhelming the channel
				fetchSize := min(channelSpace-50, 500) // Smaller batches, leave buffer
				if fetchSize <= 0 {
					continue
				}

				// Fetch batch using cursor
				commitments, _, err := rm.storage.CommitmentStorage().GetUnprocessedBatchWithCursor(ctx, cursor, fetchSize)
				if err != nil {
					rm.logger.WithContext(ctx).Error("Failed to fetch commitments", "error", err.Error())
					continue
				}

				// If we got no commitments and have a cursor, reset it to check for new ones
				if len(commitments) == 0 && cursor != "" {
					// Check if there are actually unprocessed commitments
					unprocessedCount, _ := rm.storage.CommitmentStorage().CountUnprocessed(ctx)
					if unprocessedCount > 0 {
						rm.logger.WithContext(ctx).Info("Resetting cursor to fetch new commitments",
							"unprocessedCount", unprocessedCount,
							"oldCursor", cursor)
						rm.streamMutex.Lock()
						rm.lastFetchedID = ""
						rm.streamMutex.Unlock()
						continue // Try again with reset cursor
					}
				}

				// Push to channel
				addedCount := 0
				lastAddedIdx := -1
				for i, commitment := range commitments {
					select {
					case rm.commitmentStream <- commitment:
						addedCount++
						lastAddedIdx = i
					case <-ctx.Done():
						return
					default:
						// Channel full, stop trying to add more
						goto DonePushing
					}
				}
				DonePushing:

				// Update cursor based on what we actually added
				if lastAddedIdx >= 0 {
					// Update cursor to the last successfully added commitment
					rm.streamMutex.Lock()
					rm.lastFetchedID = commitments[lastAddedIdx].ID.Hex()
					rm.streamMutex.Unlock()
				}
				// If we couldn't add ANY commitments (lastAddedIdx = -1),
				// we keep the cursor unchanged so we'll retry these same commitments

				if len(commitments) > 0 {
					rm.logger.WithContext(ctx).Debug("Prefetched commitments",
						"fetched", len(commitments),
						"added", addedCount,
						"skipped", len(commitments)-addedCount,
						"channelSize", len(rm.commitmentStream))
				}
			}
		}
	}
}

// adjustProcessingRatio dynamically adjusts the processing ratio based on actual performance
func (rm *RoundManager) adjustProcessingRatio(ctx context.Context, processingTime, expectedFinalizationTime time.Duration) {
	// Only adjust if we have meaningful data (processing took at least 100ms)
	if processingTime < 100*time.Millisecond {
		return
	}

	// Calculate what ratio we actually used
	actualRatio := float64(processingTime) / float64(rm.roundDuration)

	// Calculate ideal ratio based on actual finalization time
	// We want: processingTime + finalizationTime <= roundDuration
	// So: processingTime <= roundDuration - finalizationTime
	// Therefore: idealRatio = (roundDuration - expectedFinalizationTime) / roundDuration

	// Add a safety buffer based on the variability we've seen
	safetyBuffer := 100 * time.Millisecond
	if expectedFinalizationTime > 100*time.Millisecond {
		// For longer finalization times, use a proportional buffer
		safetyBuffer = expectedFinalizationTime / 5 // 20% buffer
	}

	safeFinalizationTime := expectedFinalizationTime + safetyBuffer
	idealRatio := 1.0 - (float64(safeFinalizationTime) / float64(rm.roundDuration))

	// Clamp ideal ratio to reasonable bounds
	// We want at least 50-70% for processing to maintain good throughput
	if idealRatio < 0.5 {
		idealRatio = 0.5 // At least 50% for processing (500ms in 1s round)
	} else if idealRatio > 0.85 {
		idealRatio = 0.85 // At most 85% for processing (leave 150ms for finalization)
	}

	// Adjust current ratio towards ideal with momentum
	// If we're consistently missing deadlines, adjust faster
	adjustmentWeight := 0.2 // Default: 20% weight on new measurement
	if actualRatio > rm.processingRatio && processingTime > time.Duration(float64(rm.roundDuration)*0.9) {
		// We're using more time than allocated and getting close to the limit
		adjustmentWeight = 0.4 // Adjust faster
	}

	rm.processingRatio = rm.processingRatio*(1-adjustmentWeight) + idealRatio*adjustmentWeight

	rm.logger.WithContext(ctx).Debug("Adjusted processing ratio",
		"actualRatio", fmt.Sprintf("%.2f", actualRatio),
		"idealRatio", fmt.Sprintf("%.2f", idealRatio),
		"newRatio", fmt.Sprintf("%.2f", rm.processingRatio),
		"avgFinalizationTime", rm.avgFinalizationTime,
		"processingTime", processingTime,
		"roundDuration", rm.roundDuration)
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
