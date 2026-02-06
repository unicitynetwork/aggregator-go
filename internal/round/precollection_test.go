package round

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	testsharding "github.com/unicitynetwork/aggregator-go/internal/sharding"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// helper to get leaf from commitment for tests
func getLeafFromCommitment(t *testing.T, commitment *models.CertificationRequest) *smt.Leaf {
	path, err := commitment.StateID.GetPath()
	require.NoError(t, err)
	leafValue, err := commitment.CertificationData.ToAPI().Hash()
	require.NoError(t, err)
	return smt.NewLeaf(path, leafValue)
}

func TestPreCollectionMechanism(t *testing.T) {
	t.Run("InitPreCollectionCreatesChainedSnapshot", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cfg := config.Config{
			Processing: config.ProcessingConfig{
				RoundDuration:          100 * time.Millisecond,
				MaxCommitmentsPerRound: 1000,
			},
			Sharding: config.ShardingConfig{
				Mode: config.ShardingModeChild,
				Child: config.ChildConfig{
					ShardID: 0b11,
				},
			},
		}

		testLogger, err := logger.New("info", "text", "stdout", false)
		require.NoError(t, err)

		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

		rm := &RoundManager{
			config:           &cfg,
			logger:           testLogger,
			smt:              smtInstance,
			commitmentStream: make(chan *models.CertificationRequest, 100),
		}

		// Create a round with a snapshot
		snapshot := smtInstance.CreateSnapshot()
		rm.currentRound = &Round{
			Number:   api.NewBigInt(big.NewInt(1)),
			Snapshot: snapshot,
		}

		// Initialize pre-collection
		rm.initPreCollection(ctx)

		// Verify pre-collection snapshot was created
		assert.NotNil(t, rm.preCollectionSnapshot)
		assert.NotNil(t, rm.preCollectedCommitments)
		assert.NotNil(t, rm.preCollectedLeaves)
		assert.Empty(t, rm.preCollectedCommitments)
		assert.Empty(t, rm.preCollectedLeaves)

		// Verify the pre-collection snapshot has the same root as current round
		assert.Equal(t, snapshot.GetRootHash(), rm.preCollectionSnapshot.GetRootHash())
	})

	t.Run("AddToPreCollectionAddsCommitment", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cfg := config.Config{
			Processing: config.ProcessingConfig{
				MaxCommitmentsPerRound: 1000,
			},
			Sharding: config.ShardingConfig{
				Mode: config.ShardingModeChild,
				Child: config.ChildConfig{
					ShardID: 0b11,
				},
			},
		}

		testLogger, err := logger.New("info", "text", "stdout", false)
		require.NoError(t, err)

		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

		rm := &RoundManager{
			config:           &cfg,
			logger:           testLogger,
			smt:              smtInstance,
			commitmentStream: make(chan *models.CertificationRequest, 100),
		}

		// Create current round and initialize pre-collection
		snapshot := smtInstance.CreateSnapshot()
		rm.currentRound = &Round{
			Number:   api.NewBigInt(big.NewInt(1)),
			Snapshot: snapshot,
		}
		rm.initPreCollection(ctx)

		initialRootHash := rm.preCollectionSnapshot.GetRootHash()

		// Create a test commitment using test utility
		commitment := testutil.CreateTestCertificationRequest(t, "precollect_test")

		// Add to pre-collection
		err = rm.addToPreCollection(ctx, commitment)
		require.NoError(t, err)

		// Verify commitment was added
		assert.Len(t, rm.preCollectedCommitments, 1)
		assert.Len(t, rm.preCollectedLeaves, 1)
		assert.Equal(t, commitment, rm.preCollectedCommitments[0])

		// Verify root hash changed (data was added to SMT)
		newRootHash := rm.preCollectionSnapshot.GetRootHash()
		assert.NotEqual(t, initialRootHash, newRootHash)
	})

	t.Run("ClearPreCollectionResetsState", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cfg := config.Config{
			Processing: config.ProcessingConfig{
				MaxCommitmentsPerRound: 1000,
			},
			Sharding: config.ShardingConfig{
				Mode: config.ShardingModeChild,
				Child: config.ChildConfig{
					ShardID: 0b11,
				},
			},
		}

		testLogger, err := logger.New("info", "text", "stdout", false)
		require.NoError(t, err)

		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

		rm := &RoundManager{
			config:           &cfg,
			logger:           testLogger,
			smt:              smtInstance,
			commitmentStream: make(chan *models.CertificationRequest, 100),
		}

		// Create current round and initialize pre-collection
		snapshot := smtInstance.CreateSnapshot()
		rm.currentRound = &Round{
			Number:   api.NewBigInt(big.NewInt(1)),
			Snapshot: snapshot,
		}
		rm.initPreCollection(ctx)

		// Add a commitment using test utility
		commitment := testutil.CreateTestCertificationRequest(t, "clear_test")
		err = rm.addToPreCollection(ctx, commitment)
		require.NoError(t, err)

		// Verify state exists
		assert.NotNil(t, rm.preCollectionSnapshot)
		assert.Len(t, rm.preCollectedCommitments, 1)

		// Clear pre-collection
		rm.clearPreCollection()

		// Verify state is cleared
		assert.Nil(t, rm.preCollectionSnapshot)
		assert.Nil(t, rm.preCollectedCommitments)
		assert.Nil(t, rm.preCollectedLeaves)
	})

	t.Run("AddToPreCollectionFailsWithoutSnapshot", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cfg := config.Config{}

		testLogger, err := logger.New("info", "text", "stdout", false)
		require.NoError(t, err)

		rm := &RoundManager{
			config: &cfg,
			logger: testLogger,
		}

		// Don't initialize pre-collection - snapshot is nil

		commitment := testutil.CreateTestCertificationRequest(t, "fail_test")

		// Should fail because pre-collection snapshot is nil
		err = rm.addToPreCollection(ctx, commitment)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pre-collection snapshot not initialized")
	})

	t.Run("AddBatchToPreCollectionAddsManyCommitments", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cfg := config.Config{
			Processing: config.ProcessingConfig{
				MaxCommitmentsPerRound: 1000,
			},
			Sharding: config.ShardingConfig{
				Mode: config.ShardingModeChild,
				Child: config.ChildConfig{
					ShardID: 0b11,
				},
			},
		}

		testLogger, err := logger.New("info", "text", "stdout", false)
		require.NoError(t, err)

		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

		rm := &RoundManager{
			config:           &cfg,
			logger:           testLogger,
			smt:              smtInstance,
			commitmentStream: make(chan *models.CertificationRequest, 100),
		}

		// Create current round and initialize pre-collection
		snapshot := smtInstance.CreateSnapshot()
		rm.currentRound = &Round{
			Number:   api.NewBigInt(big.NewInt(1)),
			Snapshot: snapshot,
		}
		rm.initPreCollection(ctx)

		initialRootHash := rm.preCollectionSnapshot.GetRootHash()

		// Create a batch of test commitments
		batchSize := 50
		commitments := make([]*models.CertificationRequest, batchSize)
		for i := 0; i < batchSize; i++ {
			commitments[i] = testutil.CreateTestCertificationRequest(t, "batch_test")
		}

		// Add batch to pre-collection
		err = rm.addBatchToPreCollection(ctx, commitments)
		require.NoError(t, err)

		// Verify all commitments were added
		assert.Len(t, rm.preCollectedCommitments, batchSize)
		assert.Len(t, rm.preCollectedLeaves, batchSize)

		// Verify root hash changed
		newRootHash := rm.preCollectionSnapshot.GetRootHash()
		assert.NotEqual(t, initialRootHash, newRootHash)
	})

	t.Run("AddBatchToPreCollectionEmptyBatchNoOp", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cfg := config.Config{}

		testLogger, err := logger.New("info", "text", "stdout", false)
		require.NoError(t, err)

		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

		rm := &RoundManager{
			config:           &cfg,
			logger:           testLogger,
			smt:              smtInstance,
			commitmentStream: make(chan *models.CertificationRequest, 100),
		}

		// Create current round and initialize pre-collection
		snapshot := smtInstance.CreateSnapshot()
		rm.currentRound = &Round{
			Number:   api.NewBigInt(big.NewInt(1)),
			Snapshot: snapshot,
		}
		rm.initPreCollection(ctx)

		// Empty batch should succeed without doing anything
		err = rm.addBatchToPreCollection(ctx, []*models.CertificationRequest{})
		require.NoError(t, err)

		assert.Empty(t, rm.preCollectedCommitments)
		assert.Empty(t, rm.preCollectedLeaves)
	})

	t.Run("FailedBatchCanBeRetriedAfterReinitialization", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cfg := config.Config{
			Processing: config.ProcessingConfig{
				MaxCommitmentsPerRound: 1000,
			},
			Sharding: config.ShardingConfig{
				Mode: config.ShardingModeChild,
				Child: config.ChildConfig{
					ShardID: 0b11,
				},
			},
		}

		testLogger, err := logger.New("info", "text", "stdout", false)
		require.NoError(t, err)

		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

		rm := &RoundManager{
			config:           &cfg,
			logger:           testLogger,
			smt:              smtInstance,
			commitmentStream: make(chan *models.CertificationRequest, 100),
		}

		// Create a batch of commitments (simulating pendingCommitments in flushBatch)
		batch := make([]*models.CertificationRequest, 5)
		for i := 0; i < 5; i++ {
			batch[i] = testutil.CreateTestCertificationRequest(t, "retry_test")
		}

		// First attempt: no snapshot initialized, should fail
		err = rm.addBatchToPreCollection(ctx, batch)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pre-collection snapshot not initialized")

		// Batch is still available for retry (caller retains it on error)
		assert.Len(t, batch, 5)

		// Now initialize pre-collection properly
		snapshot := smtInstance.CreateSnapshot()
		rm.currentRound = &Round{
			Number:   api.NewBigInt(big.NewInt(1)),
			Snapshot: snapshot,
		}
		rm.initPreCollection(ctx)

		// Retry the same batch - should succeed now
		err = rm.addBatchToPreCollection(ctx, batch)
		require.NoError(t, err)

		// Verify all commitments were added
		assert.Len(t, rm.preCollectedCommitments, 5)
		assert.Len(t, rm.preCollectedLeaves, 5)
	})

	t.Run("BatchWithBadLeafFallsBackToOneByOne", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cfg := config.Config{
			Processing: config.ProcessingConfig{
				MaxCommitmentsPerRound: 1000,
			},
			Sharding: config.ShardingConfig{
				Mode: config.ShardingModeChild,
				Child: config.ChildConfig{
					ShardID: 0b11, // Shard prefix is 11
				},
			},
		}

		testLogger, err := logger.New("info", "text", "stdout", false)
		require.NoError(t, err)

		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

		rm := &RoundManager{
			config:           &cfg,
			logger:           testLogger,
			smt:              smtInstance,
			commitmentStream: make(chan *models.CertificationRequest, 100),
		}

		// Create current round and initialize pre-collection
		snapshot := smtInstance.CreateSnapshot()
		rm.currentRound = &Round{
			Number:   api.NewBigInt(big.NewInt(1)),
			Snapshot: snapshot,
		}
		rm.initPreCollection(ctx)

		// Create 2 valid commitments
		commitment1 := testutil.CreateTestCertificationRequest(t, "fallback_test_1")
		commitment2 := testutil.CreateTestCertificationRequest(t, "fallback_test_2")

		// Add first commitment
		err = rm.addBatchToPreCollection(ctx, []*models.CertificationRequest{commitment1})
		require.NoError(t, err)
		assert.Len(t, rm.preCollectedCommitments, 1)

		// Create a modified version of commitment1 (same requestID, different value)
		// This will trigger ErrLeafModification when trying to add as batch
		modifiedCommitment := *commitment1
		modifiedCommitment.CertificationData.TransactionHash = commitment2.CertificationData.TransactionHash // Different hash
		originalLeafValue, err := commitment1.LeafValue()
		require.NoError(t, err)
		modifiedLeafValue, err := modifiedCommitment.LeafValue()
		require.NoError(t, err)
		require.NotEqual(t, originalLeafValue, modifiedLeafValue, "modified commitment must produce a different leaf value")

		// Now try to add a batch with: modifiedCommitment (will fail), commitment2 (valid)
		batch := []*models.CertificationRequest{&modifiedCommitment, commitment2}
		err = rm.addBatchToPreCollection(ctx, batch)
		require.NoError(t, err) // Should succeed overall (fallback handles errors)

		// Should have commitment1 and commitment2 (modifiedCommitment was skipped)
		// Total: 2 commitments and 2 leaves
		assert.Len(t, rm.preCollectedLeaves, 2)
		assert.Len(t, rm.preCollectedCommitments, 2)
		assert.Equal(t, len(rm.preCollectedLeaves), len(rm.preCollectedCommitments))
		assert.True(t, rm.preCollectedCommitments[0] == commitment1, "first commitment should remain the original one")
		assert.True(t, rm.preCollectedCommitments[1] == commitment2, "second commitment should be the new valid one")
	})
}

func TestPreCollectionReparenting(t *testing.T) {
	t.Run("ReparentedSnapshotCommitsToMainSMT", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cfg := config.Config{
			Processing: config.ProcessingConfig{
				MaxCommitmentsPerRound: 1000,
			},
			Sharding: config.ShardingConfig{
				Mode: config.ShardingModeChild,
				Child: config.ChildConfig{
					ShardID: 0b11,
				},
			},
		}

		testLogger, err := logger.New("info", "text", "stdout", false)
		require.NoError(t, err)

		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))
		initialMainRootHash := smtInstance.GetRootHash()

		rm := &RoundManager{
			config:           &cfg,
			logger:           testLogger,
			smt:              smtInstance,
			commitmentStream: make(chan *models.CertificationRequest, 100),
		}

		// Create commitment for Round N
		roundNCommitment := testutil.CreateTestCertificationRequest(t, "round_n")
		roundNLeaf := getLeafFromCommitment(t, roundNCommitment)

		// Round N: Create snapshot and add a leaf
		roundNSnapshot := smtInstance.CreateSnapshot()
		_, err = roundNSnapshot.AddLeaves([]*smt.Leaf{roundNLeaf})
		require.NoError(t, err)
		roundNRootHash := roundNSnapshot.GetRootHash()

		rm.currentRound = &Round{
			Number:   api.NewBigInt(big.NewInt(1)),
			Snapshot: roundNSnapshot,
		}

		// Main SMT still has initial hash
		assert.Equal(t, initialMainRootHash, smtInstance.GetRootHash())

		// Initialize pre-collection from Round N's snapshot
		rm.initPreCollection(ctx)

		// Add pre-collected commitment to chained snapshot
		preCollectedCommitment := testutil.CreateTestCertificationRequest(t, "precollected")
		err = rm.addToPreCollection(ctx, preCollectedCommitment)
		require.NoError(t, err)

		preCollectedRootHash := rm.preCollectionSnapshot.GetRootHash()

		// Pre-collected snapshot should differ from Round N's snapshot
		assert.NotEqual(t, roundNRootHash, preCollectedRootHash)

		// Commit Round N to main SMT (simulating FinalizeBlock)
		roundNSnapshot.Commit(smtInstance)
		assert.Equal(t, roundNRootHash, smtInstance.GetRootHash())

		// Reparent pre-collection snapshot to main SMT
		rm.preCollectionSnapshot.SetCommitTarget(smtInstance)

		// Commit pre-collection to main SMT
		rm.preCollectionSnapshot.Commit(smtInstance)

		// Main SMT should now have pre-collected data
		assert.Equal(t, preCollectedRootHash, smtInstance.GetRootHash())

		// Verify we can retrieve both leaves from main SMT
		roundNPath, err := roundNCommitment.StateID.GetPath()
		require.NoError(t, err)
		leaf1, err := smtInstance.GetLeaf(roundNPath)
		require.NoError(t, err)
		assert.NotNil(t, leaf1)

		preCollectedPath, err := preCollectedCommitment.StateID.GetPath()
		require.NoError(t, err)
		leaf2, err := smtInstance.GetLeaf(preCollectedPath)
		require.NoError(t, err)
		assert.NotNil(t, leaf2)
	})
}

func TestStartNewRoundWithSnapshot(t *testing.T) {
	t.Run("SkipsCollectPhaseWithPreCollectedData", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := config.Config{
			Database: config.DatabaseConfig{
				Database: "test_start_round_snapshot",
			},
			Processing: config.ProcessingConfig{
				RoundDuration:          100 * time.Millisecond,
				MaxCommitmentsPerRound: 1000,
			},
			Sharding: config.ShardingConfig{
				Mode: config.ShardingModeStandalone,
			},
		}
		storage := testutil.SetupTestStorage(t, cfg)

		testLogger, err := logger.New("info", "text", "stdout", false)
		require.NoError(t, err)

		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

		rm, err := NewRoundManager(
			ctx,
			&cfg,
			testLogger,
			storage.CommitmentQueue(),
			storage,
			nil, // No root client for standalone
			state.NewSyncStateTracker(),
			nil, // No BFT client for this test
			events.NewEventBus(testLogger),
			smtInstance,
		)
		require.NoError(t, err)

		// Create pre-collected snapshot with data
		preSnapshot := smtInstance.CreateSnapshot()
		commitment := testutil.CreateTestCertificationRequest(t, "precollected_round")
		leaf := getLeafFromCommitment(t, commitment)
		_, err = preSnapshot.AddLeaves([]*smt.Leaf{leaf})
		require.NoError(t, err)

		preCommitments := []*models.CertificationRequest{commitment}
		preLeaves := []*smt.Leaf{leaf}

		// Start round with pre-collected snapshot
		startTime := time.Now()
		err = rm.StartNewRoundWithSnapshot(ctx, api.NewBigInt(big.NewInt(1)), preSnapshot, preCommitments, preLeaves)
		require.NoError(t, err)

		// Verify round was created with Processing state (not Collecting)
		rm.roundMutex.RLock()
		roundState := rm.currentRound.State
		roundCommitments := len(rm.currentRound.Commitments)
		rm.roundMutex.RUnlock()

		// Round should start in Processing state, skipping Collecting
		assert.Equal(t, RoundStateProcessing, roundState)
		assert.Equal(t, 1, roundCommitments)

		// Verify it completed quickly (no 200ms collect phase wait)
		elapsed := time.Since(startTime)
		assert.Less(t, elapsed, 100*time.Millisecond, "Should not wait for collect phase")
	})
}

func TestPipelinedChildModeFlow(t *testing.T) {
	t.Run("SecondRoundUsesPreCollectedData", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cfg := config.Config{
			Database: config.DatabaseConfig{
				Database: "test_pipelined_child_flow",
			},
			Processing: config.ProcessingConfig{
				RoundDuration:          100 * time.Millisecond,
				MaxCommitmentsPerRound: 1000,
			},
			Sharding: config.ShardingConfig{
				Mode: config.ShardingModeChild,
				Child: config.ChildConfig{
					ShardID:            0b11,
					ParentPollTimeout:  5 * time.Second,
					ParentPollInterval: 50 * time.Millisecond,
				},
			},
		}
		storage := testutil.SetupTestStorage(t, cfg)

		testLogger, err := logger.New("info", "text", "stdout", false)
		require.NoError(t, err)

		rootAggregatorClient := testsharding.NewRootAggregatorClientStub()

		smtInstance := smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256))

		rm, err := NewRoundManager(
			ctx,
			&cfg,
			testLogger,
			storage.CommitmentQueue(),
			storage,
			rootAggregatorClient,
			state.NewSyncStateTracker(),
			nil, // No BFT client for child mode
			events.NewEventBus(testLogger),
			smtInstance,
		)
		require.NoError(t, err)

		require.NoError(t, rm.Start(ctx))
		require.NoError(t, rm.Activate(ctx))

		// Wait for first block to be created
		require.Eventually(t, func() bool {
			block, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(1)))
			return err == nil && block != nil
		}, 3*time.Second, 50*time.Millisecond, "first block should be created")

		// Wait for second block
		require.Eventually(t, func() bool {
			block, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(2)))
			return err == nil && block != nil
		}, 3*time.Second, 50*time.Millisecond, "second block should be created")

		// Verify both blocks exist
		block1, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(1)))
		require.NoError(t, err)
		assert.NotNil(t, block1)

		block2, err := storage.BlockStorage().GetByNumber(ctx, api.NewBigInt(big.NewInt(2)))
		require.NoError(t, err)
		assert.NotNil(t, block2)

		// Verify round manager used pre-collection (2 submissions, 2 proofs)
		assert.GreaterOrEqual(t, rootAggregatorClient.SubmissionCount(), 2)
		assert.GreaterOrEqual(t, rootAggregatorClient.ProofCount(), 2)
	})
}
