package ha

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
)

var conf = config.Config{
	Database: config.DatabaseConfig{
		Database:               "aggregator_ha_leadership_test",
		ConnectTimeout:         20 * time.Second,
		ServerSelectionTimeout: 5 * time.Second,
		SocketTimeout:          30 * time.Second,
		MaxPoolSize:            100,
		MinPoolSize:            5,
		MaxConnIdleTime:        5 * time.Minute,
	},
	HA: config.HAConfig{
		Enabled:                       true,
		LockID:                        "test-lock",
		LockTTLSeconds:                1,
		LeaderHeartbeatInterval:       100 * time.Millisecond,
		LeaderElectionPollingInterval: 50 * time.Millisecond,
	},
}

func TestLeaderElection_LockContention(t *testing.T) {
	storage := testutil.SetupTestStorage(t, conf)

	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	// setup server 1
	le1Config := conf.HA
	le1Config.ServerID = "server-1"
	le1 := NewLeaderElection(log, le1Config, storage.LeadershipStorage())
	defer le1.Stop(context.Background())

	// setup server 2
	le2Config := conf.HA
	le2Config.ServerID = "server-2"
	le2 := NewLeaderElection(log, le2Config, storage.LeadershipStorage())
	defer le2.Stop(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start server 1 and wait for startup
	le1.Start(ctx)
	assert.Eventually(t, func() bool {
		isLeader, err := le1.IsLeader(ctx)
		require.NoError(t, err)
		return isLeader
	}, 2*time.Second, 50*time.Millisecond, "server 1 should become leader")

	// start server 2
	le2.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// verify server 1 is still the leader
	isLeader1, err := le1.IsLeader(ctx)
	assert.NoError(t, err)
	assert.True(t, isLeader1, "server 1 should remain the leader")

	// verify server 2 is NOT the leader
	isLeader2, err := le2.IsLeader(ctx)
	assert.NoError(t, err)
	assert.False(t, isLeader2, "server 2 should not become leader while server 1 is active")
}

func TestLeaderElection_Failover(t *testing.T) {
	storage := testutil.SetupTestStorage(t, conf)

	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	// setup server 1 with slower heartbeat than the TTL
	le1Config := conf.HA
	le1Config.ServerID = "server-1"
	le1Config.LeaderHeartbeatInterval = 2 * time.Second // slower heartbeat that TTL
	le1 := NewLeaderElection(log, le1Config, storage.LeadershipStorage())
	defer le1.Stop(context.Background())

	// setup server 2 with normal heartbeat
	le2Config := conf.HA
	le2Config.ServerID = "server-2"
	le2 := NewLeaderElection(log, le2Config, storage.LeadershipStorage())
	defer le2.Stop(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start server 1 and wait for it to become leader
	le1.Start(ctx)
	assert.Eventually(t, func() bool {
		isLeader, err := le1.IsLeader(ctx)
		require.NoError(t, err)
		return isLeader
	}, 2*time.Second, 50*time.Millisecond, "server 1 should become leader")

	// start server 2 (initially cannot get the lock)
	le2.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	isLeader2, err := le2.IsLeader(ctx)
	assert.NoError(t, err)
	assert.False(t, isLeader2, "server 2 should not become leader while server 1 is active")

	// wait long enough for server 1 lock to expire and server 2 to acquire it
	assert.Eventually(t, func() bool {
		isLeader2, err := le2.IsLeader(ctx)
		require.NoError(t, err)
		return isLeader2
	}, 5*time.Second, 100*time.Millisecond, "server 2 should take over leadership after server 1 misses heartbeat")

	// confirm server 1 is no longer leader
	isLeader1, err := le1.IsLeader(ctx)
	assert.NoError(t, err)
	assert.False(t, isLeader1, "server 1 should lose leadership after missing heartbeat")
}
