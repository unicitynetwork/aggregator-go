package round

import (
	"context"
	"encoding/binary"
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

// ParentRound represents a single aggregation round for parent aggregator
type ParentRound struct {
	Number       *api.BigInt
	StartTime    time.Time
	State        RoundState
	ShardUpdates map[int]*models.ShardRootUpdate // Latest update per shard path (key is shard ID)
	Block        *models.Block

	// SMT snapshot for this round - allows accumulating shard changes before committing
	Snapshot *smt.ThreadSafeSmtSnapshot

	// Store processed data for persistence during FinalizeBlock
	ProcessedShardUpdates map[int]*models.ShardRootUpdate // Shard updates that were actually processed into the parent SMT

	// Timing metrics for this round
	ProcessingTime   time.Duration
	ProposalTime     time.Time // When block was proposed to BFT
	FinalizationTime time.Time // When block was actually finalized
}

// ParentRoundManager handles round processing for parent aggregator mode
type ParentRoundManager struct {
	config    *config.Config
	logger    *logger.Logger
	storage   interfaces.Storage
	parentSMT *smt.ThreadSafeSMT
	bftClient bft.BFTClient

	// Round management
	currentRound  *ParentRound
	roundMutex    sync.RWMutex
	stopChan      chan struct{}
	wg            sync.WaitGroup
	roundDuration time.Duration

	// Metrics
	totalRounds       int64
	totalShardUpdates int64
}

const parentRoundRetryDelay = 1 * time.Second

// NewParentRoundManager creates a new parent round manager
func NewParentRoundManager(ctx context.Context, cfg *config.Config, logger *logger.Logger, storage interfaces.Storage) (*ParentRoundManager, error) {
	// Initialize parent SMT in parent mode with support for mutable leaves
	smtInstance := smt.NewParentSparseMerkleTree(api.SHA256, cfg.Sharding.ShardIDLength)
	parentSMT := smt.NewThreadSafeSMT(smtInstance)

	prm := &ParentRoundManager{
		config:        cfg,
		logger:        logger,
		storage:       storage,
		parentSMT:     parentSMT,
		stopChan:      make(chan struct{}),
		roundDuration: cfg.Processing.RoundDuration,
	}

	// Create BFT client (same logic as regular RoundManager)
	if cfg.BFT.Enabled {
		var err error
		prm.bftClient, err = bft.NewBFTClient(ctx, &cfg.BFT, prm, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create BFT client: %w", err)
		}
	} else {
		// Calculate initial block number like regular round manager
		nextBlockNumber := api.NewBigInt(nil)
		lastBlockNumber := api.NewBigInt(big.NewInt(0))
		if storage != nil && storage.BlockStorage() != nil {
			var err error
			lastBlockNumber, err = storage.BlockStorage().GetLatestNumber(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch latest block number: %w", err)
			}
			if lastBlockNumber == nil {
				lastBlockNumber = api.NewBigInt(big.NewInt(0))
			}
		}
		nextBlockNumber.Add(lastBlockNumber.Int, big.NewInt(1))
		prm.bftClient = bft.NewBFTClientStub(logger, prm, nextBlockNumber)
	}

	return prm, nil
}

// Start performs initialization (called once at startup)
// Note: SMT reconstruction is done in Activate() when the node becomes leader
func (prm *ParentRoundManager) Start(ctx context.Context) error {
	prm.logger.WithContext(ctx).Info("Starting Parent Round Manager",
		"roundDuration", prm.roundDuration.String())

	prm.logger.WithContext(ctx).Info("Parent Round Manager started successfully")
	return nil
}

// Stop gracefully stops the parent round manager (called once at shutdown)
func (prm *ParentRoundManager) Stop(ctx context.Context) error {
	prm.logger.WithContext(ctx).Info("Stopping Parent Round Manager")

	// Deactivate first
	if err := prm.Deactivate(ctx); err != nil {
		prm.logger.WithContext(ctx).Error("Failed to deactivate Parent Round Manager", "error", err.Error())
	}

	// Wait for goroutines to finish
	prm.wg.Wait()

	prm.logger.WithContext(ctx).Info("Parent Round Manager stopped successfully")
	return nil
}

// CheckParentHealth implements the round.Manager interface for parent mode.
func (prm *ParentRoundManager) CheckParentHealth(ctx context.Context) error {
	return nil
}

// SubmitShardRoot accepts shard root updates during the current round
func (prm *ParentRoundManager) SubmitShardRoot(ctx context.Context, update *models.ShardRootUpdate) error {
	prm.roundMutex.Lock()
	defer prm.roundMutex.Unlock()

	if prm.currentRound == nil {
		return fmt.Errorf("no active round to accept shard root update")
	}

	shardKey := update.ShardID
	prm.currentRound.ShardUpdates[shardKey] = update
	prm.totalShardUpdates++

	prm.logger.WithContext(ctx).Debug("Shard root update accepted",
		"roundNumber", prm.currentRound.Number.String(),
		"shardID", update.ShardID,
		"rootHash", update.RootHash.String(),
		"totalShards", len(prm.currentRound.ShardUpdates))

	return nil
}

// StartNewRound begins a new aggregation round (public method for BFT interface)
func (prm *ParentRoundManager) StartNewRound(ctx context.Context, roundNumber *api.BigInt) error {
	return prm.startNewRound(ctx, roundNumber)
}

// startNewRound is the internal implementation
func (prm *ParentRoundManager) startNewRound(ctx context.Context, roundNumber *api.BigInt) error {
	prm.logger.WithContext(ctx).Info("Starting new parent round",
		"roundNumber", roundNumber.String())

	prm.roundMutex.Lock()

	var shardUpdates map[int]*models.ShardRootUpdate
	if prm.currentRound != nil {
		shardUpdates = prm.currentRound.ShardUpdates
	} else {
		shardUpdates = make(map[int]*models.ShardRootUpdate)
	}

	// Create new round
	prm.currentRound = &ParentRound{
		Number:       roundNumber,
		StartTime:    time.Now(),
		State:        RoundStateCollecting,
		ShardUpdates: shardUpdates,                   // Reuse the same map
		Snapshot:     prm.parentSMT.CreateSnapshot(), // Create SMT snapshot for this round
	}
	prm.roundMutex.Unlock()

	prm.logger.WithContext(ctx).Info("Parent round started",
		"roundNumber", roundNumber.String(),
		"duration", prm.roundDuration.String())

	if err := prm.processCurrentRound(ctx); err != nil {
		return err
	}

	return nil
}

// processCurrentRound processes the current round and creates a block
func (prm *ParentRoundManager) processCurrentRound(ctx context.Context) error {
	prm.roundMutex.Lock()

	if prm.currentRound == nil {
		prm.roundMutex.Unlock()
		return fmt.Errorf("no current round to process")
	}

	// Capture current round for processing
	round := prm.currentRound
	round.State = RoundStateProcessing

	round.ProcessedShardUpdates = make(map[int]*models.ShardRootUpdate, len(round.ShardUpdates))
	for shardKey, update := range round.ShardUpdates {
		round.ProcessedShardUpdates[shardKey] = update
	}
	clear(round.ShardUpdates)

	prm.roundMutex.Unlock()

	if err := prm.processRound(ctx, round); err != nil {
		return prm.retryRound(ctx, round)
	}
	return nil
}

// retryRound retries processing a round until it succeeds or context is cancelled
func (prm *ParentRoundManager) retryRound(ctx context.Context, round *ParentRound) error {
	for {
		select {
		case <-ctx.Done():
			prm.logger.WithContext(ctx).Warn("Stopping parent round retries due to context cancellation",
				"roundNumber", round.Number.String())
			return ctx.Err()
		case <-time.After(parentRoundRetryDelay):
		}

		prm.logger.WithContext(ctx).Warn("Retrying parent round processing",
			"roundNumber", round.Number.String())

		if err := prm.processRound(ctx, round); err != nil {
			prm.logger.WithContext(ctx).Error("Parent round retry failed",
				"roundNumber", round.Number.String(),
				"error", err.Error())
			continue
		}

		prm.logger.WithContext(ctx).Info("Parent round retry succeeded",
			"roundNumber", round.Number.String())
		return nil
	}
}

// processRound processes a specific round
func (prm *ParentRoundManager) processRound(ctx context.Context, round *ParentRound) error {
	startTime := time.Now()

	prm.logger.WithContext(ctx).Info("Processing parent round",
		"roundNumber", round.Number.String(),
		"shardCount", len(round.ProcessedShardUpdates))

	var parentRootHash api.HexBytes
	if len(round.ProcessedShardUpdates) == 0 {
		rootHashHex := round.Snapshot.GetRootHash()
		parsedRoot, err := api.NewHexBytesFromString(rootHashHex)
		if err != nil {
			return fmt.Errorf("failed to parse parent SMT root hash %q: %w", rootHashHex, err)
		}
		parentRootHash = parsedRoot
		prm.logger.WithContext(ctx).Info("Empty parent round, using current SMT root hash",
			"rootHash", parentRootHash.String())
	} else {
		leaves := make([]*smt.Leaf, 0, len(round.ProcessedShardUpdates))
		for _, update := range round.ProcessedShardUpdates {
			path := update.GetPath()

			leaf := &smt.Leaf{
				Path:  path,
				Value: update.RootHash,
			}
			leaves = append(leaves, leaf)
		}

		rootHashStr, err := round.Snapshot.AddLeaves(leaves)
		if err != nil {
			return fmt.Errorf("failed to add shard leaves to parent SMT snapshot: %w", err)
		}

		parsedRoot, err := api.NewHexBytesFromString(rootHashStr)
		if err != nil {
			return fmt.Errorf("failed to parse updated parent SMT root hash %q: %w", rootHashStr, err)
		}

		parentRootHash = parsedRoot
		prm.logger.WithContext(ctx).Info("Added shard updates to parent SMT snapshot",
			"shardCount", len(round.ProcessedShardUpdates),
			"newRootHash", parentRootHash.String())
	}

	var previousBlockHash api.HexBytes
	if round.Number.Cmp(big.NewInt(1)) > 0 {
		prevBlockNumber := api.NewBigInt(nil)
		prevBlockNumber.Set(round.Number.Int)
		prevBlockNumber.Sub(prevBlockNumber.Int, big.NewInt(1))

		prevBlock, err := prm.storage.BlockStorage().GetByNumber(ctx, prevBlockNumber)
		if err != nil {
			return fmt.Errorf("failed to get previous block %s: %w", prevBlockNumber.String(), err)
		}
		if prevBlock != nil {
			previousBlockHash = prevBlock.RootHash
		}
	}

	block := models.NewBlock(
		round.Number,
		prm.config.Chain.ID,
		0,
		prm.config.Chain.Version,
		prm.config.Chain.ForkID,
		parentRootHash,
		previousBlockHash,
		nil,
		nil,
	)

	round.Block = block
	round.State = RoundStateFinalizing
	round.ProposalTime = time.Now()

	if err := prm.bftClient.CertificationRequest(ctx, block); err != nil {
		prm.logger.WithContext(ctx).Error("Failed to send certification request",
			"roundNumber", round.Number.String(),
			"error", err.Error())
		return fmt.Errorf("failed to send certification request: %w", err)
	}

	prm.logger.WithContext(ctx).Info("Certification request sent successfully",
		"roundNumber", round.Number.String())

	round.ProcessingTime = time.Since(startTime)
	prm.totalRounds++

	prm.logger.WithContext(ctx).Info("Parent round processed successfully",
		"roundNumber", round.Number.String(),
		"processingTime", round.ProcessingTime.String(),
		"shardCount", len(round.ProcessedShardUpdates))

	return nil
}

// GetCurrentRound returns the current round (for BFT client callback compatibility)
func (prm *ParentRoundManager) GetCurrentRound() interface{} {
	prm.roundMutex.RLock()
	defer prm.roundMutex.RUnlock()
	return prm.currentRound
}

// GetSMT returns the parent SMT instance
func (prm *ParentRoundManager) GetSMT() *smt.ThreadSafeSMT {
	return prm.parentSMT
}

// Activate starts active round processing (called when node becomes leader in HA mode)
func (prm *ParentRoundManager) Activate(ctx context.Context) error {
	prm.logger.WithContext(ctx).Info("Activating parent round manager")

	// Reconstruct parent SMT from current shard states in storage
	// This ensures the follower-turned-leader has the latest state
	prm.logger.WithContext(ctx).Info("Reconstructing parent SMT from storage on leadership transition")
	if err := prm.reconstructParentSMT(ctx); err != nil {
		return fmt.Errorf("failed to reconstruct parent SMT on activation: %w", err)
	}

	// Start BFT client
	if err := prm.bftClient.Start(ctx); err != nil {
		return fmt.Errorf("failed to start BFT client: %w", err)
	}

	// Get latest block number to determine starting round
	latestBlockNumber, err := prm.storage.BlockStorage().GetLatestNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block number: %w", err)
	}

	// Calculate next round number
	var nextRoundNumber *api.BigInt
	if latestBlockNumber == nil {
		nextRoundNumber = api.NewBigInt(nil)
		nextRoundNumber.SetInt64(1)
	} else {
		nextRoundNumber = api.NewBigInt(nil)
		nextRoundNumber.Set(latestBlockNumber.Int)
		nextRoundNumber.Add(nextRoundNumber.Int, big.NewInt(1))
	}

	// Start first round
	if err := prm.startNewRound(ctx, nextRoundNumber); err != nil {
		return fmt.Errorf("failed to start initial round: %w", err)
	}

	prm.logger.WithContext(ctx).Info("Parent round manager activated successfully",
		"initialRound", nextRoundNumber.String())

	return nil
}

// Deactivate stops active round processing (called when node loses leadership in HA mode)
func (prm *ParentRoundManager) Deactivate(ctx context.Context) error {
	prm.logger.WithContext(ctx).Info("Deactivating parent round manager")

	// Stop BFT client
	prm.bftClient.Stop()

	prm.roundMutex.Lock()
	prm.currentRound = nil
	prm.roundMutex.Unlock()

	prm.logger.WithContext(ctx).Info("Parent round manager deactivated successfully")
	return nil
}

// FinalizeBlock is called by BFT client when block is finalized
func (prm *ParentRoundManager) FinalizeBlock(ctx context.Context, block *models.Block) error {
	prm.logger.WithContext(ctx).Info("Finalizing parent block",
		"blockNumber", block.Index.String())

	finalizationStart := time.Now()

	prm.roundMutex.RLock()
	var processedShardUpdates map[int]*models.ShardRootUpdate
	var snapshot *smt.ThreadSafeSmtSnapshot
	var proposalTime time.Time
	var processingTime time.Duration
	if prm.currentRound != nil && prm.currentRound.ProcessedShardUpdates != nil {
		processedShardUpdates = make(map[int]*models.ShardRootUpdate)
		for shardKey, update := range prm.currentRound.ProcessedShardUpdates {
			processedShardUpdates[shardKey] = update
		}
		snapshot = prm.currentRound.Snapshot
		proposalTime = prm.currentRound.ProposalTime
		processingTime = prm.currentRound.ProcessingTime
	}
	prm.roundMutex.RUnlock()

	if len(processedShardUpdates) > 0 {
		smtNodes := make([]*models.SmtNode, 0, len(processedShardUpdates))
		for _, update := range processedShardUpdates {
			bytes := make([]byte, 4)
			binary.BigEndian.PutUint32(bytes, uint32(update.ShardID))
			smtNode := models.NewSmtNode(api.NewHexBytes(bytes), update.RootHash)
			smtNodes = append(smtNodes, smtNode)
		}

		if err := prm.storage.SmtStorage().UpsertBatch(ctx, smtNodes); err != nil {
			return fmt.Errorf("failed to upsert shard states: %w", err)
		}

		prm.logger.WithContext(ctx).Info("Updated current shard states in storage",
			"blockNumber", block.Index.String(),
			"shardCount", len(processedShardUpdates),
			"shardIDs", getShardIDs(processedShardUpdates))
	}

	if err := prm.storage.BlockStorage().Store(ctx, block); err != nil {
		return fmt.Errorf("failed to store parent block: %w", err)
	}

	if snapshot != nil {
		prm.logger.WithContext(ctx).Info("Committing parent SMT snapshot to main tree after successful block storage",
			"blockNumber", block.Index.String())

		snapshot.Commit(prm.parentSMT)

		prm.logger.WithContext(ctx).Info("Successfully committed parent SMT snapshot to main tree",
			"blockNumber", block.Index.String())
	}

	finalizationTime := time.Since(finalizationStart)
	var bftWait time.Duration
	if !proposalTime.IsZero() {
		bftWait = finalizationStart.Sub(proposalTime)
	}
	totalRoundTime := processingTime + bftWait + finalizationTime
	shortDur := func(d time.Duration) string {
		if d <= 0 {
			return "0s"
		}
		return fmt.Sprintf("%.1fs", d.Seconds())
	}

	logFields := []interface{}{
		"block", block.Index.String(),
		"shards", len(processedShardUpdates),
		"roundTime", shortDur(totalRoundTime),
		"processing", shortDur(processingTime),
		"bftWait", shortDur(bftWait),
		"finalization", shortDur(finalizationTime),
	}
	prm.logger.WithContext(ctx).Info("PERF: Parent round completed", logFields...)

	prm.logger.WithContext(ctx).Info("Parent block finalized successfully",
		"blockNumber", block.Index.String())

	return nil
}

func (prm *ParentRoundManager) reconstructParentSMT(ctx context.Context) error {
	smtNodes, err := prm.storage.SmtStorage().GetAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to get SMT nodes for reconstruction: %w", err)
	}

	if len(smtNodes) == 0 {
		prm.logger.WithContext(ctx).Info("No existing SMT nodes found, starting with empty parent SMT")
		return nil
	}

	prm.logger.WithContext(ctx).Info("Reconstructing parent SMT from current shard states",
		"nodeCount", len(smtNodes))

	leaves := make([]*smt.Leaf, 0, len(smtNodes))
	for _, node := range smtNodes {
		path := new(big.Int).SetBytes(node.Key)

		leaf := &smt.Leaf{
			Path:  path,
			Value: node.Value,
		}
		leaves = append(leaves, leaf)
	}

	if len(leaves) > 0 {
		_, err := prm.parentSMT.AddLeaves(leaves)
		if err != nil {
			return fmt.Errorf("failed to add leaves to parent SMT during reconstruction: %w", err)
		}

		prm.logger.WithContext(ctx).Info("Successfully reconstructed parent SMT",
			"leafCount", len(leaves),
			"rootHash", prm.parentSMT.GetRootHash())
	}

	return nil
}

func getShardIDs(shardUpdates map[int]*models.ShardRootUpdate) []int {
	shardIDs := make([]int, 0, len(shardUpdates))
	for shardKey := range shardUpdates {
		shardIDs = append(shardIDs, shardKey)
	}
	return shardIDs
}
