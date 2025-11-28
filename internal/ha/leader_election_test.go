package ha

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
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
	ctx := t.Context()
	storage := testutil.SetupTestStorage(t, conf)
	leadershipStorage := storage.LeadershipStorage()

	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	eventBus := events.NewEventBus(log)

	// setup server 1
	le1Config := conf.HA
	le1Config.ServerID = "server-1"

	le1 := NewLeaderElection(log, le1Config, leadershipStorage, eventBus)
	defer le1.Stop(ctx)

	// setup server 2
	le2Config := conf.HA
	le2Config.ServerID = "server-2"
	le2 := NewLeaderElection(log, le2Config, leadershipStorage, eventBus)
	defer le2.Stop(ctx)

	// start server 1 and wait for startup
	leaderChangedCh := eventBus.Subscribe(events.TopicLeaderChanged)
	le1.Start(ctx)
	select {
	case e := <-leaderChangedCh:
		evt := e.(*events.LeaderChangedEvent)
		require.True(t, evt.IsLeader)
	case <-time.After(time.Second):
		require.Fail(t, "LeaderChangedEvent not received")
	}
	isLeader, err := le1.IsLeader(ctx)
	require.NoError(t, err)
	require.True(t, isLeader)

	// start server 2
	le2.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// verify server 1 is still the leader
	isLeader1, err := le1.IsLeader(ctx)
	require.NoError(t, err)
	require.True(t, isLeader1, "server 1 should remain the leader")

	// verify server 2 is NOT the leader
	isLeader2, err := le2.IsLeader(ctx)
	require.NoError(t, err)
	require.False(t, isLeader2, "server 2 should not become leader while server 1 is active")
}

func TestLeaderElection_Failover(t *testing.T) {
	ctx := t.Context()
	storage := testutil.SetupTestStorage(t, conf)
	leadershipStorage := storage.LeadershipStorage()

	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	eventBus := events.NewEventBus(log)

	// setup server 1 with slower heartbeat than the TTL
	le1Config := conf.HA
	le1Config.ServerID = "server-1"
	le1Config.LeaderHeartbeatInterval = 2 * time.Second // slower heartbeat that TTL

	le1 := NewLeaderElection(log, le1Config, leadershipStorage, eventBus)
	defer le1.Stop(ctx)

	// setup server 2 with normal heartbeat
	le2Config := conf.HA
	le2Config.ServerID = "server-2"
	le2 := NewLeaderElection(log, le2Config, leadershipStorage, eventBus)
	defer le2.Stop(ctx)

	// start server 1
	leaderChangedCh := eventBus.Subscribe(events.TopicLeaderChanged)
	le1.Start(ctx)

	// wait for server 1 to become leader
	select {
	case e := <-leaderChangedCh:
		evt := e.(*events.LeaderChangedEvent)
		require.True(t, evt.IsLeader)
	case <-time.After(time.Second):
		require.Fail(t, "LeaderChangedEvent not received for server 1 becoming leader")
	}

	// verify server 1 cached flag is updated
	isLeader, err := le1.IsLeader(ctx)
	require.NoError(t, err)
	require.True(t, isLeader)

	// start server 2
	le2.Start(ctx)

	// wait long enough for server 1 lock to expire and server 2 to acquire it
	select {
	case e := <-leaderChangedCh:
		evt := e.(*events.LeaderChangedEvent)
		require.True(t, evt.IsLeader, "server 2 should have become leader")
	case <-time.After(2 * time.Second):
		require.Fail(t, "did not receive LeaderChangedEvent for server 2 taking over")
	}

	isLeader2, err := le2.IsLeader(ctx)
	require.NoError(t, err)
	require.True(t, isLeader2)

	// wait for server 1 heartbeat to fail and lose leadership (2s in config)
	select {
	case e := <-leaderChangedCh:
		evt := e.(*events.LeaderChangedEvent)
		require.False(t, evt.IsLeader, "server 1 should have lost leadership")
	case <-time.After(2 * time.Second):
		require.Fail(t, "did not receive LeaderChangedEvent for server 1 losing leadership")
	}

	// confirm server 1 is no longer leader
	isLeader1, err := le1.IsLeader(ctx)
	require.NoError(t, err)
	require.False(t, isLeader1, "server 1 should lose leadership after missing heartbeat")
}
