package round

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/bft"
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
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

const miniBatchSize = 100 // Number of commitments to process per SMT mini-batch

// osExit is the function used to exit the process on fatal errors.
// It can be overridden in tests to prevent actual process termination.
var osExit = os.Exit

// SetExitFunc allows tests to override the exit function to prevent actual process termination.
// Returns a function to restore the original exit function.
func SetExitFunc(f func(int)) func() {
	original := osExit
	osExit = f
	return func() { osExit = original }
}

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
	Snapshot *smt.ThreadSafeSmtSnapshot
	// Store data for persistence during FinalizeBlock
	PendingLeaves []*smt.Leaf
	// Timing metrics for this round
	ProcessingTime     time.Duration
	ProposalTime       time.Time     // When block was proposed to BFT
	FinalizationTime   time.Time     // When block was actually finalized
	SubmissionDuration time.Duration // Child mode: time to submit root to parent
	ProofWaitDuration  time.Duration // Child mode: time spent waiting for parent proof
}

// RoundManager handles the creation of blocks and processing of commitments.
//
// The lifecycle of the RoundManager is managed by two pairs of methods:
//   - Start() and Stop(): These methods manage the overall lifecycle of the RoundManager instance.
//     Start() is called once during application initialization to set up core components
//     and restore state. Stop() is called once during application shutdown for graceful cleanup.
//   - Activate() and Deactivate(): these methods manage the RoundManager's active participation
//     in block creation based on High Availability (HA) leadership status. Activate() is called
//     when the node becomes the leader, enabling active block processing. Deactivate() is called
//     when the node loses leadership, putting it into a passive state.
//     A RoundManager can be Activated and Deactivated multiple times throughout its
//     overall Start-Stop lifecycle as leadership changes.
type RoundManager struct {
	config          *config.Config
	logger          *logger.Logger
	commitmentQueue interfaces.CommitmentQueue
	storage         interfaces.Storage
	smt             *smt.ThreadSafeSMT
	rootClient      RootAggregatorClient
	bftClient       bft.BFTClient
	stateTracker    *state.Tracker
	eventBus        *events.EventBus

	// Round management
	currentRound *Round
	roundMutex   sync.RWMutex
	wg           sync.WaitGroup

	// Round duration (configurable, default 1 second)
	roundDuration time.Duration

	// Streaming support
	commitmentStream chan *models.Commitment
	streamMutex      sync.RWMutex
	lastFetchedID    string             // Cursor for MongoDB pagination
	prefetchCancel   context.CancelFunc // Cancel function for running streamer/prefetcher

	// Adaptive throughput tracking
	avgProcessingRate float64 // commitments per millisecond
	lastRoundMetrics  RoundMetrics

	// Adaptive timing
	avgFinalizationTime time.Duration // Running average of finalization time
	avgSMTUpdateTime    time.Duration // Running average of SMT update time per batch

	// Pre-collection: collect commitments for next round while waiting for parent proof
	preCollectionSnapshot   *smt.ThreadSafeSmtSnapshot
	preCollectedCommitments []*models.Commitment
	preCollectedLeaves      []*smt.Leaf
	preCollectionMutex      sync.Mutex

	// Metrics
	totalRounds      int64
	totalCommitments int64
}

type RootAggregatorClient interface {
	SubmitShardRoot(ctx context.Context, req *api.SubmitShardRootRequest) error
	GetShardProof(ctx context.Context, req *api.GetShardProofRequest) (*api.RootShardInclusionProof, error)
	CheckHealth(ctx context.Context) error
}

// RoundMetrics tracks performance metrics for a round
type RoundMetrics struct {
	CommitmentsProcessed int
	ProcessingTime       time.Duration
	RoundNumber          *api.BigInt
	Timestamp            time.Time
}

// NewRoundManager creates a new round manager
func NewRoundManager(
	ctx context.Context,
	cfg *config.Config,
	logger *logger.Logger,
	commitmentQueue interfaces.CommitmentQueue,
	storage interfaces.Storage,
	rootAggregatorClient RootAggregatorClient,
	stateTracker *state.Tracker,
	luc *types.UnicityCertificate,
	eventBus *events.EventBus,
	threadSafeSmt *smt.ThreadSafeSMT,
	trustBaseProvider interfaces.TrustBaseProvider,
) (*RoundManager, error) {
	rm := &RoundManager{
		config:              cfg,
		logger:              logger,
		commitmentQueue:     commitmentQueue,
		storage:             storage,
		smt:                 threadSafeSmt,
		rootClient:          rootAggregatorClient,
		stateTracker:        stateTracker,
		eventBus:            eventBus,
		roundDuration:       cfg.Processing.RoundDuration,         // Configurable round duration (default 1s)
		commitmentStream:    make(chan *models.Commitment, 10000), // Reasonable buffer for streaming
		avgProcessingRate:   1.0,                                  // Initial estimate: 1 commitment per ms
		avgFinalizationTime: 200 * time.Millisecond,               // Initial estimate (conservative)
		avgSMTUpdateTime:    5 * time.Millisecond,                 // Initial estimate per batch
	}

	// create BFT client for standalone mode
	if cfg.Sharding.Mode == config.ShardingModeStandalone {
		if cfg.BFT.Enabled {
			var err error
			rm.bftClient, err = bft.NewBFTClient(ctx, &cfg.BFT, rm, trustBaseProvider, luc, logger, eventBus)
			if err != nil {
				return nil, fmt.Errorf("failed to create BFT client: %w", err)
			}
		} else {
			nextBlockNumber := api.NewBigInt(nil)
			lastBlockNumber := api.NewBigInt(big.NewInt(0))
			if rm.storage != nil && rm.storage.BlockStorage() != nil {
				var err error
				lastBlockNumber, err = rm.storage.BlockStorage().GetLatestNumber(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch latest block number: %w", err)
				}
				if lastBlockNumber == nil {
					lastBlockNumber = api.NewBigInt(big.NewInt(0))
				}
			}
			nextBlockNumber.Add(lastBlockNumber.Int, big.NewInt(1))
			rm.bftClient = bft.NewBFTClientStub(logger, rm, nextBlockNumber, cfg.BFT.StubDelay)
		}
	}
	return rm, nil
}

// Start begins the round manager operation
func (rm *RoundManager) Start(ctx context.Context) error {
	rm.logger.WithContext(ctx).Info("Starting Round Manager",
		"roundDuration", rm.roundDuration.String(),
		"batchLimit", rm.config.Processing.BatchLimit)

	recoveryResult, err := RecoverUnfinalizedBlock(ctx, rm.logger, rm.storage, rm.commitmentQueue)
	if err != nil {
		return fmt.Errorf("failed to recover unfinalized block: %w", err)
	}
	if recoveryResult.Recovered {
		rm.logger.Info("Recovered unfinalized block during startup",
			"blockNumber", recoveryResult.BlockNumber.String(),
			"requestCount", len(recoveryResult.RequestIDs))
	}

	_, err = rm.restoreSmtFromStorage(ctx)
	if err != nil {
		return fmt.Errorf("failed to restore SMT from storage: %w", err)
	}

	rm.logger.Info("Round Manager started successfully.")
	return nil
}

// Stop gracefully stops the round manager
func (rm *RoundManager) Stop(ctx context.Context) error {
	rm.logger.Info("Stopping Round Manager")

	if err := rm.Deactivate(ctx); err != nil {
		rm.logger.WithContext(ctx).Error("Failed to deactivate Round Manager", "error", err.Error())
	}

	// Wait for goroutines to finish
	rm.wg.Wait()

	rm.logger.Info("Round Manager stopped")
	return nil
}

// CheckParentHealth verifies the parent aggregator is reachable when running in child mode.
func (rm *RoundManager) CheckParentHealth(ctx context.Context) error {
	if rm.config.Sharding.Mode != config.ShardingModeChild || rm.rootClient == nil {
		return nil
	}
	return rm.rootClient.CheckHealth(ctx)
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

	return map[string]interface{}{
		"avgProcessingRate":       rm.avgProcessingRate,
		"targetCommitmentsPerSec": int(rm.avgProcessingRate * 1000),
		"channelSize":             len(rm.commitmentStream),
		"channelCapacity":         cap(rm.commitmentStream),
		"channelUtilization":      fmt.Sprintf("%.2f%%", channelUtilization),
		"adaptiveTiming": map[string]interface{}{
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
func (rm *RoundManager) GetSMT() *smt.ThreadSafeSMT {
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

// StartNewRound starts a new round for processing commitments (delegates to unified function)
func (rm *RoundManager) StartNewRound(ctx context.Context, roundNumber *api.BigInt) error {
	return rm.StartNewRoundWithSnapshot(ctx, roundNumber, nil, nil, nil)
}

// StartNewRoundWithSnapshot starts a new round, optionally with pre-collected data.
// If snapshot/commitments are nil (first round), an empty snapshot is created.
// All rounds are processed the same way - no separate collect phase.
func (rm *RoundManager) StartNewRoundWithSnapshot(
	ctx context.Context,
	roundNumber *api.BigInt,
	snapshot *smt.ThreadSafeSmtSnapshot,
	commitments []*models.Commitment,
	leaves []*smt.Leaf,
) error {
	rm.logger.WithContext(ctx).Info("StartNewRound called",
		"roundNumber", roundNumber.String(),
		"commitments", len(commitments))

	rm.roundMutex.Lock()

	if rm.currentRound != nil {
		rm.logger.WithContext(ctx).Info("Previous round state",
			"previousRoundNumber", rm.currentRound.Number.String(),
			"previousRoundState", rm.currentRound.State.String(),
			"previousRoundAge", time.Since(rm.currentRound.StartTime).String())
	}

	if snapshot == nil {
		snapshot = rm.smt.CreateSnapshot()
	}
	if commitments == nil {
		commitments = make([]*models.Commitment, 0)
	}
	if leaves == nil {
		leaves = make([]*smt.Leaf, 0)
	}

	rm.currentRound = &Round{
		Number:         roundNumber,
		StartTime:      time.Now(),
		State:          RoundStateProcessing,
		Commitments:    commitments,
		Snapshot:       snapshot,
		PendingLeaves:  leaves,
		PendingRecords: make([]*models.AggregatorRecord, 0, len(commitments)),
	}

	rm.roundMutex.Unlock()

	rm.logger.WithContext(ctx).Info("Started new round",
		"roundNumber", roundNumber.String(),
		"commitments", len(commitments))

	rm.wg.Go(func() {
		if err := rm.processRound(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				rm.logger.WithContext(ctx).Info("Round processing stopped due to shutdown",
					"roundNumber", roundNumber.String())
				return
			}
			rm.logger.WithContext(ctx).Error("FATAL: Round processing failed, exiting",
				"roundNumber", roundNumber.String(),
				"error", err.Error())
			osExit(1)
		}
	})

	return nil
}

func (rm *RoundManager) processRound(ctx context.Context) error {
	rm.roundMutex.Lock()
	if rm.currentRound == nil {
		rm.roundMutex.Unlock()
		return fmt.Errorf("no current round to process")
	}
	roundNumber := rm.currentRound.Number
	rm.roundMutex.Unlock()

	if !rm.config.Sharding.Mode.IsChild() {
		collectDuration := 200 * time.Millisecond
		deadline := time.Now().Add(collectDuration)

		for time.Now().Before(deadline) {
			select {
			case commitment := <-rm.commitmentStream:
				rm.roundMutex.Lock()
				rm.currentRound.Commitments = append(rm.currentRound.Commitments, commitment)
				if len(rm.currentRound.Commitments)%100 == 0 {
					batch := rm.currentRound.Commitments[len(rm.currentRound.Commitments)-100:]
					rm.processMiniBatch(ctx, batch)
				}
				rm.roundMutex.Unlock()
			case <-ctx.Done():
				return ctx.Err()
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}

		rm.roundMutex.Lock()
		remaining := len(rm.currentRound.Commitments) % 100
		if remaining > 0 {
			batch := rm.currentRound.Commitments[len(rm.currentRound.Commitments)-remaining:]
			rm.processMiniBatch(ctx, batch)
		}
		rm.roundMutex.Unlock()
	}
	rm.roundMutex.Lock()
	commitmentCount := len(rm.currentRound.Commitments)
	var rootHash string
	if rm.currentRound.Snapshot != nil {
		rootHash = rm.currentRound.Snapshot.GetRootHash()
	}
	rm.currentRound.PendingRootHash = rootHash
	rm.currentRound.ProposalTime = time.Now()
	rm.roundMutex.Unlock()

	rm.logger.WithContext(ctx).Info("processRound called",
		"roundNumber", roundNumber.String(),
		"commitments", commitmentCount,
		"rootHash", rootHash)

	if err := rm.proposeBlock(ctx, roundNumber, rootHash); err != nil {
		return fmt.Errorf("failed to propose block: %w", err)
	}

	rm.totalRounds++
	rm.totalCommitments += int64(commitmentCount)

	rm.logger.WithContext(ctx).Info("Round completed",
		"roundNumber", roundNumber.String(),
		"commitments", commitmentCount)

	return nil
}

// redisCommitmentStreamer uses StreamCommitments to continuously stream commitments.
// If connection fails, it retries every 5 seconds until context is cancelled.
func (rm *RoundManager) redisCommitmentStreamer(ctx context.Context) {
	rm.logger.WithContext(ctx).Info("Redis commitment streamer started")

	const retryInterval = 5 * time.Second

	for {
		err := rm.commitmentQueue.StreamCommitments(ctx, rm.commitmentStream)

		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			rm.logger.WithContext(ctx).Info("Redis commitment streamer stopped gracefully")
			return
		}

		rm.logger.WithContext(ctx).Error("Redis commitment streamer error, will retry",
			"error", err.Error(), "retryIn", retryInterval)

		select {
		case <-ctx.Done():
			return
		case <-time.After(retryInterval):
		}

		rm.logger.WithContext(ctx).Info("Attempting to reconnect Redis commitment streamer")
	}
}

// commitmentPrefetcher continuously fetches commitments from storage and feeds them into the stream
func (rm *RoundManager) commitmentPrefetcher(ctx context.Context) {
	rm.logger.WithContext(ctx).Info("Commitment prefetcher started")

	ticker := time.NewTicker(10 * time.Millisecond) // Check more frequently for high throughput
	defer ticker.Stop()

	for {
		select {
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
				commitments, _, err := rm.commitmentQueue.GetUnprocessedBatchWithCursor(ctx, cursor, fetchSize)
				if err != nil {
					rm.logger.WithContext(ctx).Error("Failed to fetch commitments", "error", err.Error())
					continue
				}

				// If we got no commitments and have a cursor, reset it to check for new ones
				if len(commitments) == 0 && cursor != "" {
					// Check if there are actually unprocessed commitments
					unprocessedCount, _ := rm.commitmentQueue.CountUnprocessed(ctx)
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

// restoreSmtFromStorage restores the SMT tree from persisted nodes in storage
func (rm *RoundManager) restoreSmtFromStorage(ctx context.Context) (*api.BigInt, error) {
	rm.logger.Info("Starting SMT restoration from storage")

	// Get total count for progress tracking
	totalCount, err := rm.storage.SmtStorage().Count(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get SMT node count: %w", err)
	}

	if totalCount == 0 {
		rm.logger.Info("No SMT nodes found in storage, starting with empty tree")
		return nil, nil
	}

	rm.logger.Info("Found SMT nodes in storage, starting restoration", "totalNodes", totalCount)

	const chunkSize = 1000
	offset := 0
	restoredCount := 0

	for {
		// Load chunk of nodes
		nodes, err := rm.storage.SmtStorage().GetChunked(ctx, offset, chunkSize)
		if err != nil {
			return nil, fmt.Errorf("failed to load SMT chunk at offset %d: %w", offset, err)
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
			return nil, fmt.Errorf("failed to restore SMT leaves at offset %d: %w", offset, err)
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
		return nil, fmt.Errorf("failed to get latest block for SMT verification: %w", err)
	} else if latestBlock == nil {
		rm.logger.Info("No latest block found, skipping SMT verification")
		return nil, nil
	} else {
		expectedRootHash := latestBlock.RootHash.String()
		if finalRootHash != expectedRootHash {
			rm.logger.Error("SMT restoration verification failed - root hash mismatch",
				"restoredRootHash", finalRootHash,
				"expectedRootHash", expectedRootHash,
				"latestBlockNumber", latestBlock.Index.String())
			return nil, fmt.Errorf("SMT restoration verification failed: restored root hash %s does not match latest block root hash %s",
				finalRootHash, expectedRootHash)
		}
		rm.logger.Info("SMT restoration verified successfully - root hash matches latest block",
			"rootHash", finalRootHash,
			"latestBlockNumber", latestBlock.Index.String())

		rm.stateTracker.SetLastSyncedBlock(latestBlock.Index.Int)
	}

	return latestBlock.Index, nil
}

func (rm *RoundManager) Activate(ctx context.Context) error {
	rm.logger.WithContext(ctx).Info("Activating round manager")
	if rm.config.HA.Enabled {
		recoveryResult, err := RecoverUnfinalizedBlock(ctx, rm.logger, rm.storage, rm.commitmentQueue)
		if err != nil {
			return fmt.Errorf("failed to recover unfinalized block on activation: %w", err)
		}
		if recoveryResult.Recovered {
			rm.logger.Info("Recovered unfinalized block on HA activation",
				"blockNumber", recoveryResult.BlockNumber.String(),
				"requestCount", len(recoveryResult.RequestIDs))

			if err := LoadRecoveredNodesIntoSMT(ctx, rm.logger, rm.storage, rm.smt, recoveryResult.RequestIDs); err != nil {
				return fmt.Errorf("failed to load recovered nodes into SMT: %w", err)
			}
		}
	}

	switch rm.config.Sharding.Mode {
	case config.ShardingModeStandalone:
		if err := rm.bftClient.Start(ctx); err != nil {
			return fmt.Errorf("failed to start BFT client: %w", err)
		}
	case config.ShardingModeChild:
		latestBlockNumber, err := rm.storage.BlockStorage().GetLatestNumber(ctx)
		if err != nil {
			return fmt.Errorf("failed to get latest block number: %w", err)
		}

		var roundNumber *big.Int
		if latestBlockNumber == nil {
			roundNumber = big.NewInt(1)
		} else {
			roundNumber = new(big.Int).Add(latestBlockNumber.Int, big.NewInt(1))
		}

		rm.logger.WithContext(ctx).Info("Starting child shard from database state",
			"latestBlock", latestBlockNumber,
			"nextRound", roundNumber.String())

		if err := rm.StartNewRound(ctx, api.NewBigInt(roundNumber)); err != nil {
			return fmt.Errorf("failed to start new round: %w", err)
		}
	default:
		return fmt.Errorf("invalid shard mode: %s", rm.config.Sharding.Mode)
	}

	rm.startCommitmentPrefetcher(ctx)
	return nil
}

func (rm *RoundManager) Deactivate(ctx context.Context) error {
	rm.logger.WithContext(ctx).Info("Deactivating round manager")
	rm.stopCommitmentPrefetcher()
	if rm.bftClient != nil {
		rm.bftClient.Stop()
	}

	return nil
}

func (rm *RoundManager) startCommitmentPrefetcher(ctx context.Context) {
	rm.streamMutex.Lock()
	defer rm.streamMutex.Unlock()

	if rm.prefetchCancel != nil {
		rm.logger.WithContext(ctx).Warn("Commitment prefetcher already running, ignoring start")
		return
	}

	prefetcherCtx, cancel := context.WithCancel(ctx)
	rm.prefetchCancel = cancel

	if rm.config.Storage.UseRedisForCommitments {
		rm.logger.WithContext(ctx).Info("Starting Redis commitment streamer")
		rm.wg.Go(func() {
			rm.redisCommitmentStreamer(prefetcherCtx)
		})
	} else {
		rm.logger.WithContext(ctx).Info("Starting MongoDB commitment prefetcher")
		rm.lastFetchedID = ""
		rm.wg.Go(func() {
			rm.commitmentPrefetcher(prefetcherCtx)
		})
	}
}

func (rm *RoundManager) stopCommitmentPrefetcher() {
	rm.streamMutex.Lock()
	defer rm.streamMutex.Unlock()

	if rm.prefetchCancel == nil {
		rm.logger.Warn("stopCommitmentPrefetcher called but no prefetcher running")
		return
	}
	rm.prefetchCancel()
	rm.prefetchCancel = nil
}
