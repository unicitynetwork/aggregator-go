package ha

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type (
	// Activatable defines a service that can be started and stopped
	// based on HA leadership status.
	Activatable interface {
		// Activate is called when the node becomes the leader.
		Activate(ctx context.Context) error

		// Deactivate is called when the node loses leadership.
		Deactivate(ctx context.Context) error
	}

	LeaderSelector interface {
		IsLeader(ctx context.Context) (bool, error)
	}

	// HAManager keeps track of node's leadership status,
	// calls the provided Activatable callback when leadership status changes,
	// and updates the follower node's SMT state using the BlockSyncer.
	// Needs to be started with the Start method and stopped with the Stop method.
	HAManager struct {
		logger         *logger.Logger
		leaderSelector LeaderSelector
		blockSyncer    *blockSyncer // Optional: nil when block syncing is disabled
		activatable    Activatable
		syncInterval   time.Duration

		wg     sync.WaitGroup
		cancel context.CancelFunc
	}
)

func NewHAManager(logger *logger.Logger,
	activatable Activatable,
	leaderSelector LeaderSelector,
	storage interfaces.Storage,
	smt *smt.ThreadSafeSMT,
	shardID api.ShardID,
	stateTracker *state.Tracker,
	syncInterval time.Duration,
	disableBlockSync bool, // Set true for parent mode where block syncing is not needed
) *HAManager {
	var syncer *blockSyncer
	if !disableBlockSync {
		syncer = newBlockSyncer(logger, storage, smt, shardID, stateTracker)
	}

	return &HAManager{
		logger:         logger,
		leaderSelector: leaderSelector,
		blockSyncer:    syncer,
		activatable:    activatable,
		syncInterval:   syncInterval,
	}
}

func (ham *HAManager) Start(ctx context.Context) {
	ctx, ham.cancel = context.WithCancel(ctx)
	ham.wg.Add(1)
	go func() {
		defer ham.wg.Done()
		ham.runLoop(ctx)
	}()
}

func (ham *HAManager) Stop() {
	if ham.cancel != nil {
		ham.cancel()
		ham.cancel = nil
	}
	ham.wg.Wait()
}

func (ham *HAManager) runLoop(ctx context.Context) {
	ticker := time.NewTicker(ham.syncInterval)
	defer ticker.Stop()

	var wasLeader bool
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ham.logger.WithContext(ctx).Debug("on block sync tick")
			isLeader, err := ham.onTick(ctx, wasLeader)
			if err != nil {
				ham.logger.WithContext(ctx).Warn("failed to sync block", "err", err.Error())
				continue
			}
			ham.logger.WithContext(ctx).Debug("block sync tick finished")
			wasLeader = isLeader
		}
	}
}

func (ham *HAManager) onTick(ctx context.Context, wasLeader bool) (bool, error) {
	isLeader, err := ham.leaderSelector.IsLeader(ctx)
	if err != nil {
		return wasLeader, fmt.Errorf("error on leader selection: %w", err)
	}
	// nothing to do if still leader
	if isLeader && wasLeader {
		ham.logger.WithContext(ctx).Debug("leader is already being synced")
		return isLeader, nil
	}

	// Only sync blocks if blockSyncer is enabled (regular aggregator mode)
	if ham.blockSyncer != nil {
		if err := ham.blockSyncer.syncToLatestBlock(ctx); err != nil {
			// Log the error but continue, as we might still need to handle a leadership change.
			ham.logger.Error("failed to sync smt to latest block", "error", err)
		}
	} else {
		ham.logger.WithContext(ctx).Debug("block syncing disabled (parent mode), skipping SMT sync")
	}

	if !wasLeader && isLeader {
		ham.logger.Info("Transitioning to LEADER")
		if err := ham.activatable.Activate(ctx); err != nil {
			return isLeader, fmt.Errorf("failed onActivate transition: %w", err)
		}
	} else if wasLeader && !isLeader {
		ham.logger.Info("Transitioning to FOLLOWER")
		if err := ham.activatable.Deactivate(ctx); err != nil {
			return isLeader, fmt.Errorf("failed onDeactivate transition: %w", err)
		}
	}
	return isLeader, nil
}
