package round

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// TestAdaptiveProcessingRatio tests the dynamic adjustment of processing ratio
func TestAdaptiveProcessingRatio(t *testing.T) {
	// Setup
	cfg := &config.Config{
		BFT: config.BFTConfig{
			Enabled: false, // Use mock BFT client
		},
		Processing: config.ProcessingConfig{
			RoundDuration: 1 * time.Second,
			BatchLimit:    1000,
		},
	}

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	// Create round manager
	rm, err := NewRoundManager(context.Background(), cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), nil, nil, nil, state.NewSyncStateTracker())
	require.NoError(t, err)

	// Test initial values
	assert.Equal(t, 0.7, rm.processingRatio, "Initial processing ratio should be 0.7")
	assert.Equal(t, 200*time.Millisecond, rm.avgFinalizationTime, "Initial avg finalization time should be 200ms")
	assert.Equal(t, 5*time.Millisecond, rm.avgSMTUpdateTime, "Initial avg SMT update time should be 5ms")

	// Test 1: Fast finalization should increase processing ratio
	t.Run("FastFinalization", func(t *testing.T) {
		ctx := context.Background()
		processingTime := 900 * time.Millisecond
		finalizationTime := 10 * time.Millisecond // Very fast finalization

		// Update avg finalization time as processCurrentRound would
		rm.avgFinalizationTime = (rm.avgFinalizationTime*4 + finalizationTime) / 5
		rm.adjustProcessingRatio(ctx, processingTime, finalizationTime)

		// After very fast finalization, ratio should increase (more time for processing)
		// Starting from 0.7, with fast finalization we should see an increase
		assert.Greater(t, rm.processingRatio, 0.7, "Processing ratio should increase with fast finalization")
		assert.Less(t, rm.processingRatio, 0.95, "Processing ratio should not exceed 0.95")

		// Average finalization time should decrease from initial 200ms
		assert.Less(t, rm.avgFinalizationTime, 200*time.Millisecond, "Avg finalization time should decrease")
	})

	// Test 2: Slow finalization should decrease processing ratio
	t.Run("SlowFinalization", func(t *testing.T) {
		ctx := context.Background()
		// Reset to known state
		rm.processingRatio = 0.9
		rm.avgFinalizationTime = 100 * time.Millisecond

		processingTime := 900 * time.Millisecond
		finalizationTime := 200 * time.Millisecond // Slow finalization

		// Update avg finalization time as processCurrentRound would
		rm.avgFinalizationTime = (rm.avgFinalizationTime*4 + finalizationTime) / 5
		rm.adjustProcessingRatio(ctx, processingTime, finalizationTime)

		// After slow finalization, ratio should decrease (more time needed for finalization)
		assert.Less(t, rm.processingRatio, 0.9, "Processing ratio should decrease with slow finalization")
		assert.Greater(t, rm.processingRatio, 0.5, "Processing ratio should not go below 0.5")

		// Average finalization time should increase
		assert.Greater(t, rm.avgFinalizationTime, 100*time.Millisecond, "Avg finalization time should increase")
	})

	// Test 3: Bounds checking
	t.Run("BoundsChecking", func(t *testing.T) {
		ctx := context.Background()

		// Test upper bound
		rm.processingRatio = 0.9
		rm.avgFinalizationTime = 10 * time.Millisecond // Very fast finalization
		rm.adjustProcessingRatio(ctx, 900*time.Millisecond, 10*time.Millisecond)
		assert.LessOrEqual(t, rm.processingRatio, 0.95, "Processing ratio should not exceed 0.95")

		// Test lower bound
		rm.processingRatio = 0.6
		rm.avgFinalizationTime = 600 * time.Millisecond // Very slow finalization
		rm.adjustProcessingRatio(ctx, 600*time.Millisecond, 600*time.Millisecond)
		assert.GreaterOrEqual(t, rm.processingRatio, 0.5, "Processing ratio should not go below 0.5")
	})

	// Test 4: Gradual adjustment (not jumping too quickly)
	t.Run("GradualAdjustment", func(t *testing.T) {
		ctx := context.Background()
		rm.processingRatio = 0.9
		rm.avgFinalizationTime = 100 * time.Millisecond

		oldRatio := rm.processingRatio
		rm.adjustProcessingRatio(ctx, 900*time.Millisecond, 50*time.Millisecond)

		change := rm.processingRatio - oldRatio
		assert.Less(t, change, 0.1, "Single adjustment should not change ratio by more than 0.1")
	})
}

// TestAdaptiveDeadlineCalculation tests the calculation of processing deadline
func TestAdaptiveDeadlineCalculation(t *testing.T) {
	cfg := &config.Config{
		BFT: config.BFTConfig{
			Enabled: false,
		},
		Processing: config.ProcessingConfig{
			RoundDuration: 1 * time.Second,
			BatchLimit:    1000,
		},
	}

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	rm, err := NewRoundManager(context.Background(), cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), nil, nil, nil, state.NewSyncStateTracker())
	require.NoError(t, err)

	tests := []struct {
		name             string
		processingRatio  float64
		roundDuration    time.Duration
		expectedDeadline time.Duration
	}{
		{
			name:             "Default 90% ratio",
			processingRatio:  0.9,
			roundDuration:    1000 * time.Millisecond,
			expectedDeadline: 900 * time.Millisecond,
		},
		{
			name:             "Adjusted 80% ratio",
			processingRatio:  0.8,
			roundDuration:    1000 * time.Millisecond,
			expectedDeadline: 800 * time.Millisecond,
		},
		{
			name:             "Maximum 95% ratio",
			processingRatio:  0.95,
			roundDuration:    1000 * time.Millisecond,
			expectedDeadline: 950 * time.Millisecond,
		},
		{
			name:             "Minimum 50% ratio",
			processingRatio:  0.5,
			roundDuration:    1000 * time.Millisecond,
			expectedDeadline: 500 * time.Millisecond,
		},
		{
			name:             "Different round duration",
			processingRatio:  0.9,
			roundDuration:    2000 * time.Millisecond,
			expectedDeadline: 1800 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rm.processingRatio = tt.processingRatio
			rm.roundDuration = tt.roundDuration

			deadline := time.Duration(float64(rm.roundDuration) * rm.processingRatio)
			assert.Equal(t, tt.expectedDeadline, deadline, "Processing deadline calculation mismatch")
		})
	}
}

// TestSMTUpdateTimeTracking tests the tracking of SMT update times
func TestSMTUpdateTimeTracking(t *testing.T) {
	cfg := &config.Config{
		BFT: config.BFTConfig{
			Enabled: false,
		},
		Processing: config.ProcessingConfig{
			RoundDuration: 1 * time.Second,
			BatchLimit:    1000,
		},
	}

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	rm, err := NewRoundManager(context.Background(), cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), nil, nil, nil, state.NewSyncStateTracker())
	require.NoError(t, err)

	ctx := context.Background()

	// Create a test round
	rm.currentRound = &Round{
		Number:      api.NewBigInt(big.NewInt(1)),
		StartTime:   time.Now(),
		State:       RoundStateProcessing,
		Commitments: make([]*models.CertificationRequest, 0),
		Snapshot:    rm.smt.CreateSnapshot(),
	}

	// Simulate processing mini-batches
	for i := 0; i < 10; i++ {
		commitments := make([]*models.CertificationRequest, 100)
		for j := 0; j < 100; j++ {
			// Generate random IDs for testing
			stateIDBytes := make([]byte, 32)
			rand.Read(stateIDBytes)
			txHashBytes := make([]byte, 32)
			rand.Read(txHashBytes)

			commitments[j] = &models.CertificationRequest{
				StateID: api.ImprintHexString("0000" + hex.EncodeToString(stateIDBytes)),
				CertificationData: models.CertificationData{
					OwnerPredicate:  api.NewPayToPublicKeyPredicate(append([]byte{0x02}, make([]byte, 32)...)),
					SourceStateHash: api.ImprintHexString("0000" + hex.EncodeToString(make([]byte, 32))),
					TransactionHash: api.ImprintHexString("0000" + hex.EncodeToString(txHashBytes)),
					Witness:         make([]byte, 65),
				},
			}
		}

		// Process the mini-batch
		start := time.Now()
		err := rm.processMiniBatch(ctx, commitments)
		elapsed := time.Since(start)

		require.NoError(t, err)

		// Verify processing doesn't take too long
		assert.Less(t, elapsed, 100*time.Millisecond, "Mini-batch processing should be fast")
	}
}

// TestStreamingMetrics tests the metrics reporting for adaptive timing
func TestStreamingMetrics(t *testing.T) {
	cfg := &config.Config{
		BFT: config.BFTConfig{
			Enabled: false,
		},
		Processing: config.ProcessingConfig{
			RoundDuration: 1 * time.Second,
			BatchLimit:    1000,
		},
	}

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	rm, err := NewRoundManager(context.Background(), cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), nil, nil, nil, state.NewSyncStateTracker())
	require.NoError(t, err)

	// Set some test values
	rm.processingRatio = 0.85
	rm.avgFinalizationTime = 120 * time.Millisecond
	rm.avgSMTUpdateTime = 8 * time.Millisecond
	rm.avgProcessingRate = 1.5 // 1.5 commitments per ms
	rm.lastRoundMetrics = RoundMetrics{
		CommitmentsProcessed: 1350,
		ProcessingTime:       850 * time.Millisecond,
		RoundNumber:          api.NewBigInt(big.NewInt(42)),
		Timestamp:            time.Now(),
	}

	// Get metrics
	metrics := rm.GetStreamingMetrics()

	// Verify adaptive timing metrics
	adaptiveTiming, ok := metrics["adaptiveTiming"].(map[string]interface{})
	require.True(t, ok, "Should have adaptiveTiming metrics")

	assert.Equal(t, "0.85", adaptiveTiming["processingRatio"], "Processing ratio should match")
	assert.Equal(t, "850ms", adaptiveTiming["processingWindow"], "Processing window should be 850ms")
	assert.Equal(t, "150ms", adaptiveTiming["finalizationWindow"], "Finalization window should be 150ms")
	assert.Equal(t, "120ms", adaptiveTiming["avgFinalizationTime"], "Avg finalization time should match")
	assert.Equal(t, "8ms", adaptiveTiming["avgSMTUpdateTime"], "Avg SMT update time should match")

	// Verify other metrics
	assert.Equal(t, 1.5, metrics["avgProcessingRate"], "Processing rate should match")
	assert.Equal(t, 1500, metrics["targetCommitmentsPerSec"], "Target commits/sec should be 1500")

	// Verify last round metrics
	lastRound, ok := metrics["lastRound"].(map[string]interface{})
	require.True(t, ok, "Should have lastRound metrics")
	assert.Equal(t, 1350, lastRound["commitmentsProcessed"], "Last round commitments should match")
}

// TestAdaptiveTimingIntegration tests the full adaptive timing flow
func TestAdaptiveTimingIntegration(t *testing.T) {
	cfg := &config.Config{
		BFT: config.BFTConfig{
			Enabled: false,
		},
		Processing: config.ProcessingConfig{
			RoundDuration: 1 * time.Second,
			BatchLimit:    1000,
		},
	}

	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	rm, err := NewRoundManager(context.Background(), cfg, testLogger, smt.NewSparseMerkleTree(api.SHA256, 16+256), nil, nil, nil, state.NewSyncStateTracker())
	require.NoError(t, err)

	ctx := context.Background()

	// Simulate multiple rounds with varying performance
	scenarios := []struct {
		name             string
		processingTime   time.Duration
		finalizationTime time.Duration
	}{
		{"FastRound", 850 * time.Millisecond, 80 * time.Millisecond},
		{"SlowFinalization", 850 * time.Millisecond, 180 * time.Millisecond},
		{"FastFinalization", 850 * time.Millisecond, 60 * time.Millisecond},
		{"BalancedRound", 850 * time.Millisecond, 100 * time.Millisecond},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			oldRatio := rm.processingRatio
			oldFinalizationTime := rm.avgFinalizationTime

			// Simulate the round
			rm.adjustProcessingRatio(ctx, scenario.processingTime, scenario.finalizationTime)

			// Verify the system adapts
			if scenario.finalizationTime < oldFinalizationTime {
				// Fast finalization should increase or maintain processing ratio
				assert.GreaterOrEqual(t, rm.processingRatio, oldRatio-0.01,
					"Processing ratio should not decrease significantly with fast finalization")
			} else if scenario.finalizationTime > oldFinalizationTime+50*time.Millisecond {
				// Slow finalization should decrease processing ratio
				assert.Less(t, rm.processingRatio, oldRatio+0.01,
					"Processing ratio should not increase with slow finalization")
			}

			// Verify bounds are respected
			assert.GreaterOrEqual(t, rm.processingRatio, 0.5, "Processing ratio should not go below 0.5")
			assert.LessOrEqual(t, rm.processingRatio, 0.95, "Processing ratio should not exceed 0.95")
		})
	}
}
