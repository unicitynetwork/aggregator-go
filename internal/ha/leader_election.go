package ha

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

// LeaderElection manages the HA leader election process
type LeaderElection struct {
	log                     *logger.Logger
	storage                 interfaces.LeadershipStorage
	lockID                  string
	serverID                string
	heartbeatInterval       time.Duration
	electionPollingInterval time.Duration

	wg     sync.WaitGroup     // election polling thread wg
	cancel context.CancelFunc // election polling thread cancel signal
}

func NewLeaderElection(log *logger.Logger, cfg config.HAConfig, storage interfaces.LeadershipStorage) *LeaderElection {
	return &LeaderElection{
		log:                     log,
		storage:                 storage,
		lockID:                  cfg.LockID,
		serverID:                cfg.ServerID,
		heartbeatInterval:       cfg.LeaderHeartbeatInterval,
		electionPollingInterval: cfg.LeaderElectionPollingInterval,
	}
}

func (le *LeaderElection) IsLeader(ctx context.Context) (bool, error) {
	return le.storage.IsLeader(ctx, le.lockID, le.serverID)
}

// Start stars the election polling
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
	// wait for election polling thread to exit
	le.wg.Wait()

	if _, err := le.storage.ReleaseLock(ctx, le.lockID, le.serverID); err != nil {
		le.log.WithComponent("leader-election").Error("Error releasing leadership lock", "error", err.Error())
	}

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
				le.log.WithComponent("leader-election").Info("Acquired leadership, starting heartbeat", "serverID", le.serverID)
				if err := le.startHeartbeat(ctx); err != nil {
					le.log.WithComponent("leader-election").Error("Error during heartbeat attempt", "error", err)
				}
				le.log.WithComponent("leader-election").Info("Lost leadership, returning to polling", "serverID", le.serverID)
			}
		}
	}
}
