package ha

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

// LeaderElection manages the HA leader election process.
// It polls the db for leadership lock and publishes leadership
// transition events on the provided EventBus.
type LeaderElection struct {
	log                     *logger.Logger
	storage                 interfaces.LeadershipStorage
	lockID                  string
	serverID                string
	heartbeatInterval       time.Duration
	electionPollingInterval time.Duration
	eventBus                *events.EventBus

	wg     sync.WaitGroup     // election polling thread wg
	cancel context.CancelFunc // election polling thread cancel signal

	leaderMu        sync.Mutex
	heartbeatCancel context.CancelFunc
	isLeader        atomic.Bool // cached leader flag
}

func NewLeaderElection(log *logger.Logger, cfg config.HAConfig, storage interfaces.LeadershipStorage, eventBus *events.EventBus) *LeaderElection {
	return &LeaderElection{
		log:                     log,
		storage:                 storage,
		lockID:                  cfg.LockID,
		serverID:                cfg.ServerID,
		heartbeatInterval:       cfg.LeaderHeartbeatInterval,
		electionPollingInterval: cfg.LeaderElectionPollingInterval,
		eventBus:                eventBus,
	}
}

func (le *LeaderElection) IsLeader(_ context.Context) (bool, error) {
	return le.isLeader.Load(), nil
}

func (le *LeaderElection) VerifyLeadership(ctx context.Context) (bool, error) {
	return le.storage.IsLeader(ctx, le.lockID, le.serverID)
}

func (le *LeaderElection) Resign(ctx context.Context) error {
	if le.isLeader.Load() {
		le.cancelHeartbeat()
	}
	released, err := le.storage.ReleaseLock(ctx, le.lockID, le.serverID)
	if err != nil {
		le.markFollower("Failed to release leadership lock during resign")
		return fmt.Errorf("failed to release leadership lock: %w", err)
	}
	if !released {
		le.log.WithComponent("leader-election").Warn("Leadership lock was not released during resign", "serverID", le.serverID)
	}
	le.markFollower("Resigned leadership")
	return nil
}

// Start starts the election polling.
func (le *LeaderElection) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	le.cancel = cancel

	// start election polling
	le.wg.Go(func() {
		le.startElectionPolling(ctx)
	})
}

// Stop stops the election polling and releases resources
func (le *LeaderElection) Stop(ctx context.Context) {
	// cancel election polling thread
	if le.cancel != nil {
		le.cancel()
	}
	le.cancelHeartbeat()
	// wait for election polling thread to exit
	le.wg.Wait()

	if _, err := le.storage.ReleaseLock(ctx, le.lockID, le.serverID); err != nil {
		le.log.WithComponent("leader-election").Error("Error releasing leadership lock", "error", err.Error())
	}
	le.markFollower("Stopped leadership election")

	le.log.WithComponent("leader-election").Info("Shutdown completed", "serverID", le.serverID)
}

// startHeartbeat long polling function that periodically updates the lock,
// returns when the heartbeat is cancelled or fails for any reason.
func (le *LeaderElection) startHeartbeat(ctx context.Context) error {
	ticker := time.NewTicker(le.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			ok, err := le.storage.UpdateHeartbeat(ctx, le.lockID, le.serverID)
			if err != nil {
				return fmt.Errorf("failed to update heartbeat: %w", err)
			}
			le.log.Debug("updated heartbeat", "success", ok)
			if !ok {
				// failed to update the lock (network too slow or someone else updated the leader? return to polling)
				return nil
			}
		}
	}
}

func (le *LeaderElection) startElectionPolling(ctx context.Context) {
	ticker := time.NewTicker(le.electionPollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			acquired, err := le.storage.TryAcquireLock(ctx, le.lockID, le.serverID)
			if err != nil {
				le.log.WithComponent("leader-election").Error("Error during leadership acquisition attempt", "error", err)
				continue // keep trying to acquire the lock
			}
			if acquired {
				le.log.WithComponent("leader-election").Info("Acquired leadership, publishing LeaderChangedEvent and starting heartbeat", "serverID", le.serverID)
				le.markLeader()

				heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
				le.setHeartbeatCancel(heartbeatCancel)
				if err := le.startHeartbeat(heartbeatCtx); err != nil {
					le.log.WithComponent("leader-election").Error("Error during heartbeat attempt", "error", err)
				}
				le.clearHeartbeatCancel()

				le.markFollower("Lost leadership, publishing LeaderChangedEvent and returning to polling")
			}
		}
	}
}

func (le *LeaderElection) setHeartbeatCancel(cancel context.CancelFunc) {
	le.leaderMu.Lock()
	defer le.leaderMu.Unlock()
	le.heartbeatCancel = cancel
}

func (le *LeaderElection) clearHeartbeatCancel() {
	le.leaderMu.Lock()
	defer le.leaderMu.Unlock()
	le.heartbeatCancel = nil
}

func (le *LeaderElection) cancelHeartbeat() {
	le.leaderMu.Lock()
	cancel := le.heartbeatCancel
	le.leaderMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (le *LeaderElection) markLeader() {
	if le.isLeader.Swap(true) {
		return
	}
	metrics.IsLeader.Set(1)
	metrics.LeaderTransitionsTotal.Inc()
	le.eventBus.Publish(events.TopicLeaderChanged, &events.LeaderChangedEvent{IsLeader: true})
}

func (le *LeaderElection) markFollower(message string) {
	if !le.isLeader.Swap(false) {
		return
	}
	le.log.WithComponent("leader-election").Info(message, "serverID", le.serverID)
	metrics.IsLeader.Set(0)
	metrics.LeaderTransitionsTotal.Inc()
	le.eventBus.Publish(events.TopicLeaderChanged, &events.LeaderChangedEvent{IsLeader: false})
}
