package ha

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type mockLeaderSelector struct {
	isLeader atomic.Bool
}

func (m *mockLeaderSelector) IsLeader(ctx context.Context) (bool, error) {
	return m.isLeader.Load(), nil
}

type mockActivatable struct {
	activateCalled   atomic.Int32
	deactivateCalled atomic.Int32
}

func newMockActivatable() *mockActivatable {
	return &mockActivatable{}
}

func (m *mockActivatable) Activate(_ context.Context) error {
	m.activateCalled.Add(1)
	return nil
}

func (m *mockActivatable) Deactivate(_ context.Context) error {
	m.deactivateCalled.Add(1)
	return nil
}

func TestHAManager(t *testing.T) {
	storage := testutil.SetupTestStorage(t, config.Config{
		Database: config.DatabaseConfig{
			Database:               "test_block_sync",
			ConnectTimeout:         30 * time.Second,
			ServerSelectionTimeout: 5 * time.Second,
			SocketTimeout:          30 * time.Second,
			MaxPoolSize:            100,
			MinPoolSize:            5,
			MaxConnIdleTime:        5 * time.Minute,
		},
	})

	ctx := context.Background()
	cfg := &config.Config{
		Processing: config.ProcessingConfig{RoundDuration: 100 * time.Millisecond},
		HA:         config.HAConfig{Enabled: true},
		BFT:        config.BFTConfig{Enabled: false},
	}
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	// initialize HA manger with isLeader=false
	mockLeader := &mockLeaderSelector{}
	mockLeader.isLeader.Store(false)
	callback := newMockActivatable()
	smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256))
	stateTracker := state.NewSyncStateTracker()
	disableBlockSync := false
	ham := NewHAManager(testLogger, callback, mockLeader, storage, smtInstance, 0, stateTracker, cfg.Processing.RoundDuration, disableBlockSync)

	// verify Activate/Deactivate has not been called initially
	require.Equal(t, int32(0), callback.activateCalled.Load(), "Activate should not be called initially")
	require.Equal(t, int32(0), callback.deactivateCalled.Load(), "Deactivate should not be called initially")

	// start the HA manager
	ham.Start(ctx)
	defer ham.Stop()

	// wait for HA manager to start
	time.Sleep(2 * cfg.Processing.RoundDuration)
	require.Equal(t, int32(0), callback.activateCalled.Load(), "Activate should not be called if not leader after initial ticks")
	require.Equal(t, int32(0), callback.deactivateCalled.Load(), "Deactivate should not be called if not leader after initial ticks")

	// set IsLeader to true and verify Activate is called
	mockLeader.isLeader.Store(true)
	require.Eventually(t, func() bool {
		return callback.activateCalled.Load() == 1
	}, 5*cfg.Processing.RoundDuration, 100*time.Millisecond, "Activate should be called once when becoming leader")
	require.Equal(t, int32(0), callback.deactivateCalled.Load(), "Deactivate count should not change if IsLeader goes false -> true")

	// set IsLeader to false and verify Deactivate is called
	mockLeader.isLeader.Store(false)
	require.Eventually(t, func() bool {
		return callback.deactivateCalled.Load() == 1
	}, 5*cfg.Processing.RoundDuration, 100*time.Millisecond, "Deactivate should be called once when losing leadership")
	require.Equal(t, int32(1), callback.activateCalled.Load(), "Activate count should not change if IsLeader goes true -> false")

	// ensure no further unexpected calls
	time.Sleep(2 * cfg.Processing.RoundDuration)
	require.Equal(t, int32(1), callback.activateCalled.Load(), "Activate should not be called again unexpectedly")
	require.Equal(t, int32(1), callback.deactivateCalled.Load(), "Deactivate should not be called again unexpectedly")
}
