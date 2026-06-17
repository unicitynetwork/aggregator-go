package round

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/bft"
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/storage/redis"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// RoundState represents the current state of a round
type RoundState int

const (
	RoundStateCollecting RoundState = iota // Collecting commitments
	RoundStateProcessing                   // Processing batch and computing SMT
	RoundStateFinalizing                   // Finalizing block
)

const defaultMiniBatchSize = 500 // Number of commitments to process per SMT mini-batch

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
	Commitments []*models.CertificationRequest
	Cancel      context.CancelFunc
	Block       *models.Block
	// Track commitments that have been added to SMT but not yet finalized in a block.
	// Raw 32-byte SMT root (no algorithm-id prefix), matching the V2 wire format.
	PendingRootHash api.HexBytes
	// SMT snapshot for this round - allows accumulating changes before committing
	Snapshot smtbackend.Snapshot
	// Store data for persistence during FinalizeBlock
	// PendingLeaves contains only leaves that were successfully added to the SMT
	PendingLeaves []smtbackend.LeafInput
	// PendingCommitments contains only commitments whose leaves were successfully added
	// This is used for creating aggregator records (must match PendingLeaves)
	PendingCommitments []*models.CertificationRequest
	// PreCollected is true when this round starts from a precollector snapshot.
	// These rounds skip the fixed collect window.
	PreCollected bool
	// ProposalRecordsStaged is true when the precollector already wrote the
	// round's proposed aggregator records before the certification request.
	ProposalRecordsStaged bool
	ProposalID            string
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
	smtBackend      smtbackend.Backend
	rootClient      RootAggregatorClient
	bftClient       bft.BFTClient
	stateTracker    *state.Tracker
	eventBus        *events.EventBus

	// Round management
	currentRound *Round
	roundMutex   sync.RWMutex
	// Guards the window where SMT root advances before block finalization is persisted.
	finalizationMu sync.RWMutex
	wg             sync.WaitGroup
	roundWG        sync.WaitGroup
	activeCtx      context.Context
	activeCancel   context.CancelFunc

	proofCacheMu          sync.RWMutex
	proofPending          map[string]struct{}
	latestProofReadyBlock *models.Block
	proofMetadataCache    *proofMetadataCache

	// Round duration (configurable, default 1 second)
	roundDuration time.Duration

	// Streaming support
	commitmentStream chan *models.CertificationRequest
	streamMutex      sync.RWMutex
	lastFetchedID    string             // Cursor for MongoDB pagination
	prefetchCancel   context.CancelFunc // Cancel function for running streamer/prefetcher
	prefetchDone     chan struct{}

	// Adaptive throughput tracking
	avgProcessingRate float64 // commitments per millisecond
	lastRoundMetrics  RoundMetrics

	// Adaptive timing
	avgFinalizationTime time.Duration // Running average of finalization time
	avgSMTUpdateTime    time.Duration // Running average of SMT update time per batch

	// Child-mode precollector: continuously collects commitments for next round.
	// Both fields are protected by roundMutex.
	precollector         *childPrecollector
	precollectorDisabled bool
	precollectorDone     chan struct{}

	// Child mode tracks the newest parent UC already accepted for finalization.
	// This prevents empty rounds from immediately reusing an older parent proof.
	lastAcceptedParentUCRound atomic.Uint64

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
	smtBackend, err := newConfiguredSMTBackend(cfg, threadSafeSmt)
	if err != nil {
		return nil, err
	}
	rm, err := NewRoundManagerWithBackend(ctx, cfg, logger, commitmentQueue, storage, rootAggregatorClient, stateTracker, luc, eventBus, threadSafeSmt, smtBackend, trustBaseProvider)
	if err != nil {
		_ = smtBackend.Close()
		return nil, err
	}
	return rm, nil
}

func NewRoundManagerWithBackend(
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
	smtBackend smtbackend.Backend,
	trustBaseProvider interfaces.TrustBaseProvider,
) (*RoundManager, error) {
	if smtBackend == nil {
		smtBackend = smtbackend.NewMemoryBackend(threadSafeSmt)
	}
	rm := &RoundManager{
		config:              cfg,
		logger:              logger,
		commitmentQueue:     commitmentQueue,
		storage:             storage,
		smt:                 threadSafeSmt,
		smtBackend:          smtBackend,
		rootClient:          rootAggregatorClient,
		stateTracker:        stateTracker,
		eventBus:            eventBus,
		roundDuration:       cfg.Processing.RoundDuration,                                                       // Configurable round duration (default 1s)
		commitmentStream:    make(chan *models.CertificationRequest, cfg.Processing.CommitmentStreamBufferSize), // Buffer for queue streamer
		proofPending:        make(map[string]struct{}),
		proofMetadataCache:  newProofMetadataCache(cfg.SMT.ProofMetadataCacheEntries),
		avgProcessingRate:   1.0,                    // Initial estimate: 1 commitment per ms
		avgFinalizationTime: 200 * time.Millisecond, // Initial estimate (conservative)
		avgSMTUpdateTime:    5 * time.Millisecond,   // Initial estimate per batch
	}

	if rm.storage != nil && rm.storage.SmtStorage() != nil {
		metrics.SetSMTNodesPersistedCountFunc(func(countCtx context.Context) (int64, error) {
			return rm.storage.SmtStorage().EstimatedCount(countCtx)
		})
	}
	if rm.commitmentQueue != nil {
		metrics.SetCommitmentQueueBacklogFunc(func(countCtx context.Context) (int64, error) {
			return rm.commitmentQueue.CountUnprocessed(countCtx)
		})
	}

	// create BFT client for modes that talk directly to BFT (standalone and bft-shard)
	if cfg.Sharding.Mode == config.ShardingModeStandalone || cfg.Sharding.Mode == config.ShardingModeBFTShard {
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

	if rm.usesDiskSMTBackend() {
		if _, err := rm.restoreOrVerifySMT(ctx); err != nil {
			return fmt.Errorf("failed to restore SMT from storage: %w", err)
		}
		// A stale disk follower (empty local RocksDB with existing finalized
		// history) defers all catch-up to the live BlockSyncer. It must not run
		// leader-only unfinalized-block recovery, which would try to apply a
		// single block onto an empty tree and fail.
		staleFollower, err := rm.diskAwaitingFollowerCatchup(ctx)
		if err != nil {
			return fmt.Errorf("failed to determine disk follower catch-up state: %w", err)
		}
		if staleFollower {
			// Nothing verified to publish yet; BlockSyncer refreshes the proof view per replayed block.
			rm.logger.Info("Stale disk follower; skipping unfinalized-block recovery, BlockSyncer will catch up")
		} else {
			recoveryResult, err := recoverUnfinalizedBlock(ctx, rm.logger, rm.storage, rm.commitmentQueue, false)
			if err != nil {
				return fmt.Errorf("failed to recover unfinalized block: %w", err)
			}
			if recoveryResult.Recovered {
				rm.logger.Info("Recovered unfinalized block during startup",
					"blockNumber", recoveryResult.BlockNumber.String(),
					"stateCount", len(recoveryResult.StateIDs))
				if err := rm.syncDiskSMTAfterRecoveredBlock(ctx, recoveryResult); err != nil {
					return fmt.Errorf("failed to sync disk SMT after recovered block: %w", err)
				}
			}
			if err := rm.refreshDiskProofView(ctx); err != nil {
				return fmt.Errorf("failed to initialize disk SMT proof view: %w", err)
			}
		}
	} else {
		recoveryResult, err := RecoverUnfinalizedBlock(ctx, rm.logger, rm.storage, rm.commitmentQueue)
		if err != nil {
			return fmt.Errorf("failed to recover unfinalized block: %w", err)
		}
		if recoveryResult.Recovered {
			rm.logger.Info("Recovered unfinalized block during startup",
				"blockNumber", recoveryResult.BlockNumber.String(),
				"stateCount", len(recoveryResult.StateIDs))
		}

		if _, err := rm.restoreOrVerifySMT(ctx); err != nil {
			return fmt.Errorf("failed to restore SMT from storage: %w", err)
		}
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

	if rm.smtBackend != nil {
		if err := rm.smtBackend.Close(); err != nil {
			return fmt.Errorf("failed to close SMT backend: %w", err)
		}
	}

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
		Commitments: append([]*models.CertificationRequest(nil), rm.currentRound.Commitments...),
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

func (rm *RoundManager) GetSMTBackend() smtbackend.Backend {
	return rm.smtBackend
}

func (rm *RoundManager) CommittedRoot(ctx context.Context) ([]byte, *api.BigInt, error) {
	if rm.smtBackend == nil {
		return nil, nil, fmt.Errorf("SMT backend is not initialized")
	}
	state, err := rm.smtBackend.CommittedState(ctx)
	if err != nil {
		return nil, nil, err
	}
	return state.RootHash, state.BlockNumber, nil
}

// FinalizationReadLock blocks only during the commit+finalize window to avoid root/block mismatches in proofs.
func (rm *RoundManager) FinalizationReadLock() func() {
	rm.finalizationMu.RLock()
	return func() { rm.finalizationMu.RUnlock() }
}

func (rm *RoundManager) GetKnownNotReadyBlock(stateID api.StateID) (*models.Block, bool) {
	rm.proofCacheMu.RLock()
	defer rm.proofCacheMu.RUnlock()

	if _, ok := rm.proofPending[stateID.String()]; !ok || rm.latestProofReadyBlock == nil {
		return nil, false
	}
	return rm.latestProofReadyBlock, true
}

func (rm *RoundManager) GetProofReadyBlockByRoot(rootHash api.HexBytes) (*models.Block, bool) {
	rm.proofCacheMu.RLock()
	defer rm.proofCacheMu.RUnlock()

	if rm.proofMetadataCache == nil {
		return nil, false
	}
	return rm.proofMetadataCache.getBlock(rootHash)
}

func (rm *RoundManager) GetCachedProofMetadata(stateID api.StateID, rootHash api.HexBytes) (*models.Block, *models.AggregatorRecord, bool) {
	rm.proofCacheMu.RLock()
	defer rm.proofCacheMu.RUnlock()

	if rm.proofMetadataCache == nil {
		return nil, nil, false
	}
	return rm.proofMetadataCache.get(stateID, rootHash)
}

func (rm *RoundManager) GetProofCacheStats() (pending int, records int, blocks int) {
	rm.proofCacheMu.RLock()
	defer rm.proofCacheMu.RUnlock()

	records, blocks = rm.proofMetadataCache.stats()
	return len(rm.proofPending), records, blocks
}

func (rm *RoundManager) markProofsPending(commitments []*models.CertificationRequest) {
	if len(commitments) == 0 {
		return
	}

	rm.proofCacheMu.Lock()
	defer rm.proofCacheMu.Unlock()
	for _, commitment := range commitments {
		if commitment != nil {
			rm.proofPending[commitment.StateID.String()] = struct{}{}
		}
	}
}

func (rm *RoundManager) markProofsReady(block *models.Block, stateIDs []api.StateID, records []*models.AggregatorRecord) {
	rm.proofCacheMu.Lock()
	defer rm.proofCacheMu.Unlock()

	rm.latestProofReadyBlock = block
	if rm.proofMetadataCache != nil {
		rm.proofMetadataCache.add(block, records)
	}
	for _, stateID := range stateIDs {
		delete(rm.proofPending, stateID.String())
	}
}

func (rm *RoundManager) clearProofPending() {
	rm.proofCacheMu.Lock()
	defer rm.proofCacheMu.Unlock()
	rm.proofPending = make(map[string]struct{})
}

// GetStats returns round manager statistics
func (rm *RoundManager) GetStats() map[string]interface{} {
	rm.roundMutex.RLock()
	defer rm.roundMutex.RUnlock()

	stats := map[string]interface{}{
		"totalRounds":      atomic.LoadInt64(&rm.totalRounds),
		"totalCommitments": atomic.LoadInt64(&rm.totalCommitments),
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
	abandonPendingRound := rm.hasPendingRoundToAbandon(roundNumber)
	if abandonPendingRound {
		rm.roundMutex.Lock()
		supersededSnapshot := rm.abandonSupersededRoundLocked(roundNumber)
		rm.roundMutex.Unlock()
		if supersededSnapshot != nil {
			supersededSnapshot.Discard(ctx)
		}
		resetDone := rm.discardActivePrecollector(ctx)
		if !resetDone {
			rm.clearProofPending()
			rm.resetRedisPendingSweep(ctx)
		}
	}
	return rm.StartNewRoundWithSnapshot(ctx, roundNumber, nil, nil, nil, false, "")
}

func (rm *RoundManager) hasPendingRoundToAbandon(roundNumber *api.BigInt) bool {
	if roundNumber == nil || roundNumber.Int == nil {
		return false
	}

	rm.roundMutex.RLock()
	defer rm.roundMutex.RUnlock()
	return rm.currentRound != nil &&
		rm.currentRound.Number != nil &&
		rm.currentRound.Number.Cmp(roundNumber.Int) < 0 &&
		rm.currentRound.Snapshot != nil
}

// StartNewRoundWithSnapshot starts a new round, optionally with pre-collected data.
// If snapshot/commitments are nil, an empty snapshot is created and the fixed
// collect path is used.
// ErrDeactivated is returned by StartNewRoundWithSnapshot when the round manager
// has been deactivated. The check and the decision not to start are atomic under roundMutex.
var ErrDeactivated = fmt.Errorf("round manager deactivated")

func (rm *RoundManager) StartNewRoundWithSnapshot(
	ctx context.Context,
	roundNumber *api.BigInt,
	snapshot smtbackend.Snapshot,
	commitments []*models.CertificationRequest,
	leaves []smtbackend.LeafInput,
	proposalRecordsStaged bool,
	proposalID string,
) error {
	rm.logger.WithContext(ctx).Info("StartNewRound called",
		"roundNumber", roundNumber.String(),
		"commitments", len(commitments))

	rm.roundMutex.Lock()

	if rm.precollectorDisabled {
		rm.roundMutex.Unlock()
		if snapshot != nil {
			snapshot.Discard(ctx)
		}
		rm.logger.WithContext(ctx).Info("Skipping round start - deactivated",
			"roundNumber", roundNumber.String())
		return ErrDeactivated
	}

	roundCtx := ctx
	if rm.activeCtx != nil {
		roundCtx = rm.activeCtx
	}

	if rm.currentRound != nil {
		currentRoundNumber := rm.currentRound.Number
		currentRoundState := rm.currentRound.State
		currentRoundAge := time.Since(rm.currentRound.StartTime)
		currentRoundNumberString := "<nil>"
		if currentRoundNumber != nil {
			currentRoundNumberString = currentRoundNumber.String()
		}
		rm.logger.WithContext(ctx).Info("Previous round state",
			"previousRoundNumber", currentRoundNumberString,
			"previousRoundState", currentRoundState.String(),
			"previousRoundAge", currentRoundAge.String())
		if currentRoundNumber != nil && currentRoundNumber.Cmp(roundNumber.Int) == 0 {
			retryRound := rm.currentRound
			shouldRetry := retryRound.State == RoundStateFinalizing && !retryRound.ProposalTime.IsZero()
			rm.roundMutex.Unlock()
			if snapshot != nil {
				snapshot.Discard(ctx)
			}
			if shouldRetry {
				rm.logger.WithContext(ctx).Info("Retrying existing round proposal",
					"roundNumber", roundNumber.String(),
					"currentRoundState", currentRoundState.String())
				rm.startRoundProposalRetry(roundCtx, retryRound)
			} else {
				rm.logger.WithContext(ctx).Info("Skipping duplicate round start",
					"roundNumber", roundNumber.String(),
					"currentRoundState", currentRoundState.String())
			}
			return nil
		}
		if currentRoundNumber != nil && currentRoundNumber.Cmp(roundNumber.Int) > 0 {
			rm.roundMutex.Unlock()
			if snapshot != nil {
				snapshot.Discard(ctx)
			}
			rm.logger.WithContext(ctx).Info("Skipping stale round start",
				"roundNumber", roundNumber.String(),
				"currentRoundNumber", currentRoundNumberString,
				"currentRoundState", currentRoundState.String())
			return nil
		}
	}

	preCollected := snapshot != nil
	if snapshot == nil {
		var err error
		snapshot, err = rm.smtBackend.CreateSnapshot(ctx)
		if err != nil {
			rm.roundMutex.Unlock()
			return fmt.Errorf("failed to create SMT snapshot: %w", err)
		}
	}
	if commitments == nil {
		commitments = make([]*models.CertificationRequest, 0)
	}
	if leaves == nil {
		leaves = make([]smtbackend.LeafInput, 0)
	}

	supersededSnapshot := rm.abandonSupersededRoundLocked(roundNumber)
	processCtx, processCancel := context.WithCancel(roundCtx)
	if proposalID == "" {
		proposalID = uuid.NewString()
	}

	round := &Round{
		Number:                roundNumber,
		StartTime:             time.Now(),
		State:                 RoundStateProcessing,
		Commitments:           commitments,
		Cancel:                processCancel,
		Snapshot:              snapshot,
		PendingLeaves:         leaves,
		PendingCommitments:    commitments, // In child mode, commitments are already filtered by pre-collection
		PreCollected:          preCollected,
		ProposalRecordsStaged: proposalRecordsStaged,
		ProposalID:            proposalID,
	}
	rm.currentRound = round

	// Start precollector on the first child-mode round so it begins collecting
	// commitments into a chained snapshot while the current round processes.
	if rm.config.Sharding.Mode.IsChild() && rm.precollector == nil && !rm.precollectorDisabled {
		rm.startPrecollectorLocked(roundCtx, snapshot, nil)
	}

	rm.roundWG.Go(func() {
		if err := rm.processRound(processCtx, round); err != nil {
			if errors.Is(err, context.Canceled) {
				rm.logger.WithContext(roundCtx).Info("Round processing stopped due to shutdown",
					"roundNumber", roundNumber.String())
				return
			}
			rm.logger.WithContext(roundCtx).Error("Round processing failed, requesting shutdown",
				"roundNumber", roundNumber.String(),
				"error", err.Error())
			if rm.eventBus != nil {
				rm.eventBus.Publish(events.TopicFatalError, events.FatalErrorEvent{
					Source: "round_manager",
					Error:  err.Error(),
				})
			}
		}
	})

	rm.roundMutex.Unlock()

	if supersededSnapshot != nil {
		supersededSnapshot.Discard(ctx)
	}

	rm.logger.WithContext(ctx).Info("Started new round",
		"roundNumber", roundNumber.String(),
		"commitments", len(commitments))

	return nil
}

func (rm *RoundManager) startRoundProposalRetry(ctx context.Context, round *Round) {
	rm.roundWG.Go(func() {
		if err := rm.retryRoundProposal(ctx, round); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, bft.ErrStaleCertificationRound) {
				rm.logger.WithContext(ctx).Info("Round proposal retry stopped",
					"error", err.Error())
				return
			}
			roundNumber := "<nil>"
			if round != nil && round.Number != nil {
				roundNumber = round.Number.String()
			}
			rm.logger.WithContext(ctx).Error("Round proposal retry failed, requesting shutdown",
				"roundNumber", roundNumber,
				"error", err.Error())
			if rm.eventBus != nil {
				rm.eventBus.Publish(events.TopicFatalError, events.FatalErrorEvent{
					Source: "round_manager",
					Error:  err.Error(),
				})
			}
		}
	})
}

func (rm *RoundManager) retryRoundProposal(ctx context.Context, round *Round) error {
	if round == nil || round.Number == nil {
		return fmt.Errorf("no round to retry")
	}

	rm.roundMutex.Lock()
	if rm.currentRound != round {
		rm.roundMutex.Unlock()
		return bft.ErrStaleCertificationRound
	}
	rootHash := append(api.HexBytes(nil), round.PendingRootHash...)
	if len(rootHash) == 0 && round.Snapshot != nil {
		rootHashRaw, err := round.Snapshot.RootHashRaw(ctx)
		if err != nil {
			rm.roundMutex.Unlock()
			return fmt.Errorf("failed to get SMT snapshot root: %w", err)
		}
		rootHash = append(api.HexBytes(nil), rootHashRaw...)
		round.PendingRootHash = rootHash
	}
	if len(rootHash) == 0 {
		rm.roundMutex.Unlock()
		return fmt.Errorf("round %s has no proposal root to retry", round.Number.String())
	}
	round.ProposalTime = time.Now()
	roundNumber := round.Number
	rm.roundMutex.Unlock()

	return rm.proposeBlock(ctx, round, roundNumber, rootHash)
}

// abandonSupersededRoundLocked cancels the current round when it is being
// superseded by a newer one and returns its unproposed snapshot (or nil) for the
// caller to discard after releasing roundMutex. Proof-pending markers are cleared
// separately via clearProofPending() at the abandon/deactivate/reset points.
func (rm *RoundManager) abandonSupersededRoundLocked(roundNumber *api.BigInt) smtbackend.Snapshot {
	if rm.currentRound == nil {
		return nil
	}
	if roundNumber == nil ||
		roundNumber.Int == nil ||
		rm.currentRound.Number == nil ||
		rm.currentRound.Number.Cmp(roundNumber.Int) >= 0 {
		return nil
	}
	if rm.currentRound.Cancel != nil {
		rm.currentRound.Cancel()
		rm.currentRound.Cancel = nil
	}

	superseded := rm.currentRound.Snapshot
	rm.currentRound.Snapshot = nil
	rm.currentRound.PendingRootHash = nil
	rm.currentRound.PendingLeaves = nil
	rm.currentRound.PendingCommitments = nil
	return superseded
}

func (rm *RoundManager) processRound(ctx context.Context, round *Round) error {
	if round == nil || round.Number == nil {
		return fmt.Errorf("no round to process")
	}
	roundNumber := round.Number
	preCollected := round.PreCollected
	proposed := false
	defer func() {
		if proposed {
			return
		}
		// Claim the snapshot under the lock so this and abandonSupersededRoundLocked
		// cannot both discard it; whichever nils it first owns the discard.
		rm.roundMutex.Lock()
		snapshot := round.Snapshot
		round.Snapshot = nil
		rm.roundMutex.Unlock()
		if snapshot != nil {
			snapshot.Discard(context.Background())
		}
	}()

	rm.roundMutex.Lock()
	if rm.currentRound != round {
		rm.roundMutex.Unlock()
		return nil
	}
	rm.roundMutex.Unlock()

	if !preCollected && !rm.config.Sharding.Mode.IsChild() {
		collectDuration := rm.config.Processing.CollectPhaseDuration
		if collectDuration <= 0 {
			collectDuration = 200 * time.Millisecond
		}
		miniBatchSize := rm.collectMiniBatchSize()
		deadline := time.Now().Add(collectDuration)
		flushCalls := 0
		flushAdded := 0
		var flushTotal time.Duration
		var flushMax time.Duration
		var finalFlush time.Duration
		finalFlushAdded := 0
		collectedCount := 0

		for time.Now().Before(deadline) {
			select {
			case commitment := <-rm.commitmentStream:
				rm.roundMutex.Lock()
				if rm.currentRound != round {
					rm.roundMutex.Unlock()
					return nil
				}
				round.Commitments = append(round.Commitments, commitment)
				var dropped []interfaces.CertificationRequestAck
				if len(round.Commitments)%miniBatchSize == 0 {
					batch := round.Commitments[len(round.Commitments)-miniBatchSize:]
					var err error
					pendingBefore := len(round.PendingCommitments)
					flushStart := time.Now()
					dropped, err = rm.processMiniBatchForRound(ctx, round, batch)
					flushElapsed := time.Since(flushStart)
					added := len(round.PendingCommitments) - pendingBefore
					flushCalls++
					flushAdded += added
					flushTotal += flushElapsed
					if flushElapsed > flushMax {
						flushMax = flushElapsed
					}
					if err != nil {
						rm.roundMutex.Unlock()
						return err
					}
				}
				rm.roundMutex.Unlock()
				ackDroppedCommitments(ctx, rm.logger, rm.commitmentQueue, dropped)
			case <-ctx.Done():
				return ctx.Err()
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}

		rm.roundMutex.Lock()
		if rm.currentRound != round {
			rm.roundMutex.Unlock()
			return nil
		}
		remaining := len(round.Commitments) % miniBatchSize
		var dropped []interfaces.CertificationRequestAck
		if remaining > 0 {
			batch := round.Commitments[len(round.Commitments)-remaining:]
			var err error
			pendingBefore := len(round.PendingCommitments)
			flushStart := time.Now()
			dropped, err = rm.processMiniBatchForRound(ctx, round, batch)
			finalFlush = time.Since(flushStart)
			finalFlushAdded = len(round.PendingCommitments) - pendingBefore
			flushCalls++
			flushAdded += finalFlushAdded
			flushTotal += finalFlush
			if finalFlush > flushMax {
				flushMax = finalFlush
			}
			if err != nil {
				rm.roundMutex.Unlock()
				return err
			}
		}
		collectedCount = len(round.Commitments)
		rm.roundMutex.Unlock()
		ackDroppedCommitments(ctx, rm.logger, rm.commitmentQueue, dropped)
		if flushCalls > 0 {
			rm.logger.WithContext(ctx).Debug("Round collection flush",
				"roundNumber", roundNumber.String(),
				"commitments", collectedCount,
				"miniBatchSize", miniBatchSize,
				"flushCalls", flushCalls,
				"flushAdded", flushAdded,
				"flushTotal", flushTotal.String(),
				"flushMax", flushMax.String(),
				"finalFlush", finalFlush.String(),
				"finalFlushAdded", finalFlushAdded,
				"collectDuration", collectDuration.String())
		}
	}

	rm.startActivePrecollectorIfNeeded(ctx, round)

	rm.roundMutex.Lock()
	if rm.currentRound != round {
		rm.roundMutex.Unlock()
		return nil
	}
	commitmentCount := len(round.Commitments)
	var rootHash api.HexBytes
	if round.Snapshot != nil {
		rootHashRaw, err := round.Snapshot.RootHashRaw(ctx)
		if err != nil {
			rm.roundMutex.Unlock()
			return fmt.Errorf("failed to get SMT snapshot root: %w", err)
		}
		rootHash = rootHashRaw
	}
	round.PendingRootHash = rootHash
	round.ProposalTime = time.Now()
	rm.roundMutex.Unlock()

	rm.logger.WithContext(ctx).Info("processRound called",
		"roundNumber", roundNumber.String(),
		"commitments", commitmentCount,
		"rootHash", rootHash.String())

	if err := rm.proposeBlock(ctx, round, roundNumber, rootHash); err != nil {
		if errors.Is(err, bft.ErrStaleCertificationRound) {
			rm.logger.WithContext(ctx).Info("Dropping stale round proposal",
				"roundNumber", roundNumber.String(),
				"error", err.Error())
			return nil
		}
		return fmt.Errorf("failed to propose block: %w", err)
	}
	proposed = true

	atomic.AddInt64(&rm.totalRounds, 1)
	atomic.AddInt64(&rm.totalCommitments, int64(commitmentCount))

	rm.logger.WithContext(ctx).Info("Round completed",
		"roundNumber", roundNumber.String(),
		"commitments", commitmentCount)

	return nil
}

// redisCommitmentStreamer uses StreamCertificationRequests to continuously stream commitments.
// If connection fails, it retries every 5 seconds until context is cancelled.
func (rm *RoundManager) redisCommitmentStreamer(ctx context.Context) {
	rm.logger.WithContext(ctx).Info("Redis commitment streamer started")

	const retryInterval = 5 * time.Second

	for {
		err := rm.commitmentQueue.StreamCertificationRequests(ctx, rm.commitmentStream)

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

	snapshot, err := rm.smtBackend.CreateSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create SMT restoration snapshot: %w", err)
	}
	defer snapshot.Discard(ctx)

	const chunkSize = 10000
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

		// Convert storage nodes to backend leaf inputs. Keys in storage are raw
		// fixed-width SMT keys, which is the format the disk backend will use too.
		leaves := make([]smtbackend.LeafInput, len(nodes))
		for i, node := range nodes {
			leaves[i] = smtbackend.LeafInput{
				Key:   node.Key,
				Value: node.Value,
			}
		}

		result, err := snapshot.AddLeavesClassified(ctx, leaves)
		if err != nil {
			return nil, fmt.Errorf("failed to restore SMT leaves at offset %d: %w", offset, err)
		}
		if err := result.ValidateAllAccepted(len(leaves)); err != nil {
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

	// Log final state (raw 32-byte root matching UC.IR.h / V2 wire format)
	finalRootHashRaw, err := snapshot.RootHashRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get restored SMT root hash: %w", err)
	}
	finalRootHash := api.HexBytes(finalRootHashRaw)
	rm.logger.Info("SMT restoration complete",
		"restoredNodes", restoredCount,
		"totalNodes", totalCount,
		"finalRootHash", finalRootHash.String())

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
		if err := snapshot.Commit(ctx, smtbackend.CommitMetadata{}); err != nil {
			return nil, fmt.Errorf("failed to commit restored SMT snapshot: %w", err)
		}
		return nil, nil
	} else {
		if !bytes.Equal(finalRootHash, latestBlock.RootHash) {
			rm.logger.Error("SMT restoration verification failed - root hash mismatch",
				"restoredRootHash", finalRootHash.String(),
				"expectedRootHash", latestBlock.RootHash.String(),
				"latestBlockNumber", latestBlock.Index.String())
			return nil, fmt.Errorf("SMT restoration verification failed: restored root hash %s does not match latest block root hash %s",
				finalRootHash.String(), latestBlock.RootHash.String())
		}
		rm.logger.Info("SMT restoration verified successfully - root hash matches latest block",
			"rootHash", finalRootHash.String(),
			"latestBlockNumber", latestBlock.Index.String())

		rm.stateTracker.SetLastSyncedBlock(latestBlock.Index.Int)
	}

	if err := snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: latestBlock.Index, RootHash: latestBlock.RootHash}); err != nil {
		return nil, fmt.Errorf("failed to commit restored SMT snapshot: %w", err)
	}

	return latestBlock.Index, nil
}

func (rm *RoundManager) Activate(ctx context.Context) error {
	rm.logger.WithContext(ctx).Info("Activating round manager")

	rm.roundMutex.RLock()
	alreadyActive := rm.activeCancel != nil
	rm.roundMutex.RUnlock()
	if alreadyActive {
		rm.logger.WithContext(ctx).Warn("Round manager already active, deactivating previous activation")
		if err := rm.Deactivate(ctx); err != nil {
			return fmt.Errorf("failed to deactivate previous activation: %w", err)
		}
	}

	activeCtx, activeCancel := context.WithCancel(ctx)
	activated := false
	defer func() {
		if activated {
			return
		}
		activeCancel()
		if err := rm.Deactivate(ctx); err != nil {
			rm.logger.WithContext(ctx).Error("Failed to deactivate after activation failure",
				"error", err.Error())
		}
	}()

	rm.roundMutex.Lock()
	rm.activeCtx = activeCtx
	rm.activeCancel = activeCancel
	rm.precollectorDisabled = false
	if rm.precollectorDone == nil {
		rm.precollectorDone = make(chan struct{})
	}
	rm.roundMutex.Unlock()

	if rm.config.HA.Enabled {
		recoveryResult, err := recoverUnfinalizedBlock(activeCtx, rm.logger, rm.storage, rm.commitmentQueue, !rm.usesDiskSMTBackend())
		if err != nil {
			return fmt.Errorf("failed to recover unfinalized block on activation: %w", err)
		}
		if recoveryResult.Recovered {
			rm.logger.Info("Recovered unfinalized block on HA activation",
				"blockNumber", recoveryResult.BlockNumber.String(),
				"stateCount", len(recoveryResult.StateIDs))

			if err := LoadRecoveredNodesIntoBackend(activeCtx, rm.logger, rm.storage, rm.smtBackend, recoveryResult.BlockNumber, recoveryResult.StateIDs); err != nil {
				return fmt.Errorf("failed to load recovered nodes into SMT: %w", err)
			}
		}
	}

	switch rm.config.Sharding.Mode {
	case config.ShardingModeStandalone, config.ShardingModeBFTShard:
		if err := rm.bftClient.Start(activeCtx); err != nil {
			return fmt.Errorf("failed to start BFT client: %w", err)
		}
	case config.ShardingModeChild:
		if err := rm.restoreLastAcceptedParentUC(activeCtx); err != nil {
			return fmt.Errorf("failed to restore latest parent UC from child blocks: %w", err)
		}

		latestBlockNumber, err := rm.storage.BlockStorage().GetLatestNumber(activeCtx)
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

		if err := rm.StartNewRound(activeCtx, api.NewBigInt(roundNumber)); err != nil {
			return fmt.Errorf("failed to start new round: %w", err)
		}
	default:
		return fmt.Errorf("invalid shard mode: %s", rm.config.Sharding.Mode)
	}

	rm.startCommitmentPrefetcher(activeCtx)
	activated = true
	return nil
}

func (rm *RoundManager) restoreLastAcceptedParentUC(ctx context.Context) error {
	rm.lastAcceptedParentUCRound.Store(0)

	if rm.storage == nil || rm.storage.BlockStorage() == nil {
		return nil
	}

	latestBlock, err := rm.storage.BlockStorage().GetLatest(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest child block: %w", err)
	}
	if latestBlock == nil || len(latestBlock.UnicityCertificate) == 0 {
		return nil
	}

	parentUC, err := decodeUnicityCertificate(latestBlock.UnicityCertificate)
	if err != nil {
		return fmt.Errorf("failed to decode parent UC from latest child block %s: %w", latestBlock.Index.String(), err)
	}

	rm.lastAcceptedParentUCRound.Store(parentUC.GetRoundNumber())
	rm.logger.WithContext(ctx).Info("Restored latest accepted parent UC from child storage",
		"childBlockNumber", latestBlock.Index.String(),
		"parentRound", parentUC.GetRoundNumber())

	return nil
}

func (rm *RoundManager) acceptParentUC(parentUC *types.UnicityCertificate) {
	if parentUC == nil {
		return
	}
	rm.lastAcceptedParentUCRound.Store(parentUC.GetRoundNumber())
}

func (rm *RoundManager) lastAcceptedParentUC() uint64 {
	return rm.lastAcceptedParentUCRound.Load()
}

func decodeUnicityCertificate(raw api.HexBytes) (*types.UnicityCertificate, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("unicity certificate is empty")
	}

	var uc types.UnicityCertificate
	if err := types.Cbor.Unmarshal(raw, &uc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal UC: %w", err)
	}

	return &uc, nil
}

func (rm *RoundManager) Deactivate(ctx context.Context) error {
	rm.logger.WithContext(ctx).Info("Deactivating round manager")

	var cp *childPrecollector
	rm.roundMutex.Lock()
	rm.precollectorDisabled = true
	if rm.precollectorDone != nil {
		close(rm.precollectorDone)
		rm.precollectorDone = nil
	}
	cp = rm.precollector
	rm.precollector = nil
	activeCancel := rm.activeCancel
	rm.activeCancel = nil
	rm.activeCtx = nil
	rm.roundMutex.Unlock()

	if activeCancel != nil {
		activeCancel()
	}
	if cp != nil {
		cp.Stop()
	}
	rm.stopCommitmentPrefetcher()
	if rm.bftClient != nil {
		rm.bftClient.Stop()
	}
	rm.roundWG.Wait()

	rm.roundMutex.Lock()
	rm.currentRound = nil
	rm.roundMutex.Unlock()
	rm.clearProofPending()

	return nil
}

func (rm *RoundManager) usesActivePrecollector() bool {
	if !rm.config.Storage.UseRedisForCommitments {
		return false
	}

	switch rm.config.Sharding.Mode {
	case config.ShardingModeStandalone, config.ShardingModeBFTShard:
		return true
	default:
		return false
	}
}

func (rm *RoundManager) discardActivePrecollector(ctx context.Context) bool {
	if !rm.usesActivePrecollector() {
		return false
	}

	var cp *childPrecollector
	rm.roundMutex.Lock()
	cp = rm.precollector
	rm.precollector = nil
	rm.roundMutex.Unlock()

	if cp != nil {
		cp.Stop()
		rm.clearProofPending()
		rm.resetRedisPendingSweep(ctx)
		return true
	}
	return false
}

func (rm *RoundManager) resetRedisPendingSweep(ctx context.Context) {
	cs, ok := rm.commitmentQueue.(*redis.CommitmentStorage)
	if !ok {
		return
	}

	restartPrefetcher := rm.stopCommitmentPrefetcherAndWait()
	rm.drainCommitmentStream()
	cs.ResetPendingSweep()
	if restartPrefetcher {
		rm.startCommitmentPrefetcher(ctx)
	}
}

func (rm *RoundManager) drainCommitmentStream() int {
	drained := 0
	for {
		select {
		case _, ok := <-rm.commitmentStream:
			if !ok {
				return drained
			}
			drained++
		default:
			return drained
		}
	}
}

// startActivePrecollectorIfNeeded starts the next-round precollector for
// standalone/bft-shard only after processRound has finished the fixed collect
// window, so it is the sole reader of commitmentStream while BFT is pending.
func (rm *RoundManager) startActivePrecollectorIfNeeded(ctx context.Context, round *Round) {
	if !rm.usesActivePrecollector() {
		return
	}

	rm.roundMutex.Lock()
	if rm.precollector != nil ||
		rm.precollectorDisabled ||
		rm.currentRound != round ||
		round == nil ||
		round.Snapshot == nil {
		rm.roundMutex.Unlock()
		return
	}

	precollectorCtx := ctx
	if rm.activeCtx != nil {
		precollectorCtx = rm.activeCtx
	}
	rm.startPrecollectorLocked(precollectorCtx, round.Snapshot, incrementRoundNumber(round.Number))
	rm.roundMutex.Unlock()
}

// startPrecollectorLocked forks the base snapshot and starts the precollector on
// the owned fork. The fork is taken under roundMutex so the base snapshot cannot
// be discarded while the precollector forks it. Caller must hold roundMutex; on
// fork failure the precollector is not started.
func (rm *RoundManager) startPrecollectorLocked(ctx context.Context, base smtbackend.Snapshot, blockNumber *api.BigInt) {
	forked, err := base.Fork(ctx)
	if err != nil {
		rm.logger.WithContext(ctx).Error("Failed to fork precollector snapshot", "error", err.Error())
		return
	}
	cp := rm.newActivePrecollector(blockNumber)
	rm.precollector = cp
	cp.Start(ctx, forked)
}

func (rm *RoundManager) newActivePrecollector(blockNumber *api.BigInt) *childPrecollector {
	cp := newChildPrecollector(
		rm.commitmentStream,
		rm.commitmentQueue,
		rm.logger,
		rm.config.Processing.MaxCommitmentsPerRound,
		rm.markProofsPending,
		blockNumber,
		"",
		rm.stageProposedCommitments,
	)
	return cp
}

func (rm *RoundManager) collectMiniBatchSize() int {
	if rm == nil || rm.config == nil || rm.config.Processing.CollectMiniBatchSize <= 0 {
		return defaultMiniBatchSize
	}
	return rm.config.Processing.CollectMiniBatchSize
}

func (rm *RoundManager) advancePrecollectorForHandoff(cp *childPrecollector) (*preCollectionResult, error) {
	return cp.AdvanceRound()
}

func validatePrecollectorBlockNumber(preResult *preCollectionResult, roundNumber *api.BigInt) error {
	if preResult == nil || !preResult.recordsStaged {
		return nil
	}
	if preResult.blockNumber == nil || preResult.blockNumber.Int == nil {
		return fmt.Errorf("precollector staged records without a block number")
	}
	if roundNumber == nil || roundNumber.Int == nil {
		return fmt.Errorf("precollector staged records for block %s cannot start a nil round", preResult.blockNumber.String())
	}
	if preResult.blockNumber.Cmp(roundNumber.Int) != 0 {
		return fmt.Errorf("precollector staged records for block %s cannot start round %s", preResult.blockNumber.String(), roundNumber.String())
	}
	return nil
}

// StartNextRoundFromPrecollector starts the next standalone/bft-shard round
// from the active precollector snapshot. If precollection is disabled or no
// precollector is available, it falls back to the fixed collect path.
func (rm *RoundManager) StartNextRoundFromPrecollector(ctx context.Context, roundNumber *api.BigInt) error {
	if !rm.usesActivePrecollector() {
		return rm.StartNewRound(ctx, roundNumber)
	}

	rm.roundMutex.RLock()
	cpExists := rm.precollector != nil
	precollectorDone := rm.precollectorDone
	precollectorDisabled := rm.precollectorDisabled
	rm.roundMutex.RUnlock()

	if precollectorDisabled {
		return nil
	}

	if cpExists {
		if err := rm.waitBeforePrecollectorHandoff(ctx, precollectorDone); err != nil {
			if errors.Is(err, ErrDeactivated) {
				return nil
			}
			return err
		}
	}

	rm.roundMutex.RLock()
	cp := rm.precollector
	precollectorDisabled = rm.precollectorDisabled
	rm.roundMutex.RUnlock()

	if precollectorDisabled {
		return nil
	}
	if cp == nil {
		return rm.StartNewRound(ctx, roundNumber)
	}

	advanceStart := time.Now()
	preResult, err := rm.advancePrecollectorForHandoff(cp)
	advanceDuration := time.Since(advanceStart)
	if err != nil {
		rm.roundMutex.RLock()
		precollectorDisabled = rm.precollectorDisabled
		rm.roundMutex.RUnlock()
		if precollectorDisabled {
			return nil
		}

		rm.logger.WithContext(ctx).Warn("Failed to advance precollector, falling back to fixed collect round",
			"error", err.Error(),
			"advanceDuration", advanceDuration.String())
		// The precollector failed to hand off; discard it here so a fresh one
		// can start. StartNewRound only discards when abandoning a pending
		// round, which is not the case on the post-finalization fallback path.
		rm.discardActivePrecollector(ctx)
		return rm.StartNewRound(ctx, roundNumber)
	}

	if err := validatePrecollectorBlockNumber(preResult, roundNumber); err != nil {
		preResult.snapshot.Discard(ctx)
		return err
	}
	if err := preResult.snapshot.SetCommitTarget(ctx, rm.smtBackend); err != nil {
		preResult.snapshot.Discard(ctx)
		return fmt.Errorf("failed to set precollector commit target: %w", err)
	}
	if err := rm.StartNewRoundWithSnapshot(ctx, roundNumber, preResult.snapshot, preResult.commitments, preResult.leaves, preResult.recordsStaged, preResult.proposalID); err != nil {
		if errors.Is(err, ErrDeactivated) {
			return nil
		}
		return err
	}
	return nil
}

func incrementRoundNumber(roundNumber *api.BigInt) *api.BigInt {
	if roundNumber == nil || roundNumber.Int == nil {
		return nil
	}
	return api.NewBigInt(new(big.Int).Add(roundNumber.Int, big.NewInt(1)))
}

func (rm *RoundManager) waitBeforePrecollectorHandoff(ctx context.Context, done <-chan struct{}) error {
	grace := rm.config.Processing.PrecollectorGracePeriod
	if grace <= 0 {
		return nil
	}

	timer := time.NewTimer(grace)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return ErrDeactivated
	}
}

func (rm *RoundManager) startCommitmentPrefetcher(ctx context.Context) {
	rm.streamMutex.Lock()
	defer rm.streamMutex.Unlock()

	if rm.prefetchCancel != nil {
		rm.logger.WithContext(ctx).Warn("Commitment prefetcher already running, ignoring start")
		return
	}

	prefetcherCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	rm.prefetchCancel = cancel
	rm.prefetchDone = done

	if rm.config.Storage.UseRedisForCommitments {
		if cs, ok := rm.commitmentQueue.(*redis.CommitmentStorage); ok {
			cs.ResetPendingSweep()
		}
		rm.logger.WithContext(ctx).Info("Starting Redis commitment streamer")
		rm.wg.Go(func() {
			defer close(done)
			rm.redisCommitmentStreamer(prefetcherCtx)
		})
	} else {
		rm.logger.WithContext(ctx).Info("Starting MongoDB commitment prefetcher")
		rm.lastFetchedID = ""
		rm.wg.Go(func() {
			defer close(done)
			rm.commitmentPrefetcher(prefetcherCtx)
		})
	}
}

func (rm *RoundManager) stopCommitmentPrefetcher() {
	rm.stopCommitmentPrefetcherAndWait()
}

func (rm *RoundManager) stopCommitmentPrefetcherAndWait() bool {
	rm.streamMutex.Lock()
	if rm.prefetchCancel == nil {
		rm.streamMutex.Unlock()
		rm.logger.Warn("stopCommitmentPrefetcher called but no prefetcher running")
		return false
	}
	cancel := rm.prefetchCancel
	done := rm.prefetchDone
	rm.prefetchCancel = nil
	rm.prefetchDone = nil
	rm.streamMutex.Unlock()

	cancel()
	if done != nil {
		<-done
	}
	return true
}
