package round

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	testsharding "github.com/unicitynetwork/aggregator-go/internal/sharding"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// test the good case where blocks are created and stored successfully
func TestParentShardIntegration_GoodCase(t *testing.T) {
	// setup dependencies
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := config.Config{
		Processing: config.ProcessingConfig{
			RoundDuration: 100 * time.Millisecond,
			BatchLimit:    1000,
		},
		Sharding: config.ShardingConfig{
			Mode: config.ShardingModeChild,
			Child: config.ChildConfig{
				ShardID:            0b11,
				ParentPollTimeout:  5 * time.Second,
				ParentPollInterval: 100 * time.Millisecond,
			},
		},
	}
	storage := testutil.SetupTestStorage(t, cfg)
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	rootAggregatorClient := testsharding.NewRootAggregatorClientStub()

	// create round manager
	rm, err := NewRoundManager(ctx, &cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), storage.CommitmentQueue(), storage, rootAggregatorClient, state.NewSyncStateTracker(), nil)
	require.NoError(t, err)

	// start round manager
	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	// verify first 3 blocks
	for i := 1; i <= 3; i++ {
		require.Eventually(t, func() bool {
			block, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(int64(i))))
			if err != nil {
				return false
			}
			return block != nil
		}, 3*time.Second, 100*time.Millisecond, "block %d should have been created", i)
	}

	// verify metrics
	require.Equal(t, 3, rootAggregatorClient.SubmissionCount())
	require.Equal(t, 3, rootAggregatorClient.ProofCount())
}

// test that errors on parent communication cause retries (not deadlock)
func TestParentShardIntegration_RoundProcessingError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := config.Config{
		Processing: config.ProcessingConfig{
			RoundDuration: 100 * time.Millisecond,
			BatchLimit:    1000,
		},
		Sharding: config.ShardingConfig{
			Mode: config.ShardingModeChild,
			Child: config.ChildConfig{
				ShardID:            0b11,
				ParentPollTimeout:  5 * time.Second,
				ParentPollInterval: 100 * time.Millisecond,
			},
		},
	}
	storage := testutil.SetupTestStorage(t, cfg)
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	// create root aggregator client where all submissions fail
	rootAggregatorClient := testsharding.NewRootAggregatorClientStub()
	rootAggregatorClient.SetSubmissionError(errors.New("some error"))

	rm, err := NewRoundManager(ctx, &cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), storage.CommitmentQueue(), storage, rootAggregatorClient, state.NewSyncStateTracker(), nil)
	require.NoError(t, err)

	require.NoError(t, rm.Start(ctx))
	require.NoError(t, rm.Activate(ctx))

	// wait for multiple retry attempts (retry delay is 1s)
	time.Sleep(2500 * time.Millisecond)

	// verify that no blocks were created (submissions all failed)
	latestBlock, err := storage.BlockStorage().GetLatest(ctx)
	require.NoError(t, err)
	require.Nil(t, latestBlock)

	// verify retries are happening (at least 2 attempts in 2.5s with 1s retry delay)
	require.GreaterOrEqual(t, rootAggregatorClient.SubmissionAttempts(), 2, "should have retried submissions")

	// verify no successful submissions
	require.Equal(t, 0, rootAggregatorClient.SubmissionCount())
}
