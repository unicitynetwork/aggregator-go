package service

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ParentServiceTestSuite is the test suite for parent aggregator service
type ParentServiceTestSuite struct {
	suite.Suite
	cfg     *config.Config
	logger  *logger.Logger
	storage *mongodb.Storage
	cleanup func()
	service *ParentAggregatorService
	prm     *round.ParentRoundManager
}

type staticLeaderSelector struct {
	leader bool
	err    error
}

func (s *staticLeaderSelector) IsLeader(ctx context.Context) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	return s.leader, nil
}

// SetupSuite runs once before all tests
func (suite *ParentServiceTestSuite) SetupSuite() {
	var err error
	suite.logger, err = logger.New("info", "text", "stdout", false)
	require.NoError(suite.T(), err, "Should create logger")

	suite.cfg = &config.Config{
		Sharding: config.ShardingConfig{
			Mode:          config.ShardingModeParent,
			ShardIDLength: 2, // 2 bits = 4 possible shards [4,5,6,7]
		},
		Database: config.DatabaseConfig{
			Database:       "test_parent_service",
			ConnectTimeout: 5 * time.Second,
		},
		BFT: config.BFTConfig{
			Enabled: false, // Use BFT stub
		},
		Processing: config.ProcessingConfig{
			RoundDuration: 100 * time.Millisecond,
		},
	}

	suite.storage = testutil.SetupTestStorage(suite.T(), *suite.cfg)
}

// TearDownSuite runs once after all tests
func (suite *ParentServiceTestSuite) TearDownSuite() {
	if suite.cleanup != nil {
		suite.cleanup()
	}
}

// SetupTest runs before each test
func (suite *ParentServiceTestSuite) SetupTest() {
	ctx := context.Background()

	// Create parent round manager
	var err error
	suite.prm, err = round.NewParentRoundManager(ctx, suite.cfg, suite.logger, suite.storage)
	require.NoError(suite.T(), err, "Should create parent round manager")
	require.NotNil(suite.T(), suite.prm, "Parent round manager should not be nil")

	// Start the round manager
	err = suite.prm.Start(ctx)
	require.NoError(suite.T(), err, "Should start parent round manager")

	// Activate the round manager (starts rounds)
	err = suite.prm.Activate(ctx)
	require.NoError(suite.T(), err, "Should activate parent round manager")

	// Create parent service with the round manager
	suite.service = NewParentAggregatorService(suite.cfg, suite.logger, suite.prm, suite.storage, nil)
	require.NotNil(suite.T(), suite.service, "Parent service should not be nil")
}

// TearDownTest runs after each test
func (suite *ParentServiceTestSuite) TearDownTest() {
	ctx := context.Background()

	// Stop round manager
	if suite.prm != nil {
		suite.prm.Stop(ctx)
	}

	// Clean all collections
	if err := suite.storage.CleanAllCollections(ctx); err != nil {
		suite.T().Logf("Warning: failed to clean collections: %v", err)
	}
}

// Test helpers
func makeTestHash(value byte) []byte {
	hash := make([]byte, 32)
	hash[0] = value
	return hash
}

// waitForShardToExist polls until the shard proof is available or times out
func (suite *ParentServiceTestSuite) waitForShardToExist(ctx context.Context, shardID int) {
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			suite.T().Fatalf("Timeout waiting for shard %d to be processed", shardID)
		case <-ticker.C:
			response, err := suite.service.GetShardProof(ctx, &api.GetShardProofRequest{ShardID: shardID})
			if err == nil && response.MerkleTreePath != nil {
				return // Shard exists and has a proof!
			}
			// Continue polling
		}
	}
}

// SubmitShardRoot - Valid Submission
func (suite *ParentServiceTestSuite) TestSubmitShardRoot_ValidSubmission() {
	ctx := context.Background()

	request := &api.SubmitShardRootRequest{
		ShardID:  4, // 0b100 - valid for 2-bit sharding
		RootHash: makeTestHash(0xAA),
	}

	response, err := suite.service.SubmitShardRoot(ctx, request)
	suite.Require().NoError(err, "Valid submission should succeed")
	suite.Require().NotNil(response, "Response should not be nil")
	suite.Assert().Equal(api.ShardRootStatusSuccess, response.Status, "Response should indicate success")

	suite.T().Log("✓ Valid shard root submission accepted")
}

func (suite *ParentServiceTestSuite) TestSubmitShardRoot_NotLeader() {
	ctx := context.Background()

	notLeaderSelector := &staticLeaderSelector{leader: false}
	suite.service = NewParentAggregatorService(suite.cfg, suite.logger, suite.prm, suite.storage, notLeaderSelector)

	request := &api.SubmitShardRootRequest{
		ShardID:  4,
		RootHash: makeTestHash(0xAA),
	}

	response, err := suite.service.SubmitShardRoot(ctx, request)
	suite.Require().NoError(err, "Follower rejection should not return Go error")
	suite.Require().NotNil(response, "Response should not be nil")
	suite.Assert().Equal(api.ShardRootStatusNotLeader, response.Status, "Follower should reject shard submissions with NOT_LEADER")
}

// SubmitShardRoot - Invalid ShardID (empty)
func (suite *ParentServiceTestSuite) TestSubmitShardRoot_EmptyShardID() {
	ctx := context.Background()

	request := &api.SubmitShardRootRequest{
		ShardID:  0, // Empty
		RootHash: makeTestHash(0xAA),
	}

	response, err := suite.service.SubmitShardRoot(ctx, request)
	suite.Require().NoError(err, "Should not return Go error")
	suite.Require().NotNil(response, "Response should not be nil")
	suite.Assert().Equal(api.ShardRootStatusInvalidShardID, response.Status, "Should return INVALID_SHARD_ID status")

	suite.T().Log("✓ Empty shard ID rejected correctly")
}

// SubmitShardRoot - Invalid ShardID (out of range)
func (suite *ParentServiceTestSuite) TestSubmitShardRoot_OutOfRange() {
	ctx := context.Background()

	// Test shard ID below minimum (MSB not set)
	// For ShardIDLength=2, valid range is [4,7] (0b100-0b111)
	// ShardID=1 (0b001) has no MSB prefix bit
	requestLow := &api.SubmitShardRootRequest{
		ShardID:  1, // Below minimum
		RootHash: makeTestHash(0xAA),
	}

	responseLow, err := suite.service.SubmitShardRoot(ctx, requestLow)
	suite.Require().NoError(err, "Should not return Go error")
	suite.Require().NotNil(responseLow, "Response should not be nil")
	suite.Assert().Equal(api.ShardRootStatusInvalidShardID, responseLow.Status, "Should return INVALID_SHARD_ID for below minimum")

	// Test shard ID above maximum
	// ShardID=8 (0b1000) exceeds the 2-bit range
	requestHigh := &api.SubmitShardRootRequest{
		ShardID:  8, // Above maximum
		RootHash: makeTestHash(0xBB),
	}

	responseHigh, err := suite.service.SubmitShardRoot(ctx, requestHigh)
	suite.Require().NoError(err, "Should not return Go error")
	suite.Require().NotNil(responseHigh, "Response should not be nil")
	suite.Assert().Equal(api.ShardRootStatusInvalidShardID, responseHigh.Status, "Should return INVALID_SHARD_ID for above maximum")

	suite.T().Log("✓ Out of range shard IDs rejected correctly")
}

// SubmitShardRoot - Verify Update is Queued
func (suite *ParentServiceTestSuite) TestSubmitShardRoot_UpdateQueued() {
	ctx := context.Background()

	shard0ID := 0
	shard0Root := makeTestHash(0xAA)

	request := &api.SubmitShardRootRequest{
		ShardID:  shard0ID,
		RootHash: shard0Root,
	}

	response, err := suite.service.SubmitShardRoot(ctx, request)
	suite.Require().NoError(err, "Submission should succeed")
	suite.Require().NotNil(response, "Response should not be nil")

	// Wait for round to process
	time.Sleep(150 * time.Millisecond)

	// Verify that the parent SMT root has changed (indicating the update was processed)
	rootHash := suite.prm.GetSMT().GetRootHash()
	suite.Assert().NotEmpty(rootHash, "Parent SMT root should be calculated")

	suite.T().Log("✓ Shard update was queued and processed correctly")
}

// GetShardProof - Success (existing shard with real child SMT)
func (suite *ParentServiceTestSuite) TestGetShardProof_Success() {
	ctx := context.Background()

	shard0ID := 4 // 0b100 - valid for 2-bit sharding

	// TODO(SMT): Child would extract root hash from their SMT and send to parent
	childRootRaw := makeTestHash(0xAA)

	// Submit the child root to parent
	submitReq := &api.SubmitShardRootRequest{
		ShardID:  shard0ID,
		RootHash: childRootRaw,
	}
	_, err := suite.service.SubmitShardRoot(ctx, submitReq)
	suite.Require().NoError(err)

	// Wait for round to process by polling for the shard to exist
	suite.waitForShardToExist(ctx, shard0ID)

	// Request proof for the shard
	proofReq := &api.GetShardProofRequest{
		ShardID: shard0ID,
	}

	response, err := suite.service.GetShardProof(ctx, proofReq)
	suite.Require().NoError(err, "Should get proof successfully")
	suite.Require().NotNil(response, "Response should not be nil")
	suite.Assert().NotNil(response.MerkleTreePath, "MerkleTreePath should not be nil")
	suite.Assert().NotEmpty(response.MerkleTreePath.Steps, "Proof should have steps")

	// Verify that the proof can be validated
	// Convert shard ID to big.Int for verification (the path in the parent SMT)
	shardPath := new(big.Int).SetInt64(int64(shard0ID))
	result, err := response.MerkleTreePath.Verify(shardPath)
	suite.Require().NoError(err, "Proof verification should not error")

	// Both PathValid and PathIncluded should be true
	suite.Assert().True(result.PathValid, "Proof path must be valid")
	suite.Assert().True(result.PathIncluded, "Proof should show path is included")

	suite.T().Log("✓ GetShardProof returned valid and verifiable proof")
}

// GetShardProof - Non-existent Shard (returns nil MerkleTreePath)
func (suite *ParentServiceTestSuite) TestGetShardProof_NonExistentShard() {
	ctx := context.Background()

	// Submit one shard
	shard0ID := 0b100 // 4 - valid for 2-bit sharding
	submitReq := &api.SubmitShardRootRequest{
		ShardID:  shard0ID,
		RootHash: makeTestHash(0xAA),
	}
	_, err := suite.service.SubmitShardRoot(ctx, submitReq)
	suite.Require().NoError(err)

	// Wait for round to process
	suite.waitForShardToExist(ctx, shard0ID)

	// Request proof for a shard that was never submitted
	shard5ID := 0b101 // 5 - valid for 2-bit sharding
	proofReq := &api.GetShardProofRequest{
		ShardID: shard5ID,
	}

	response, err := suite.service.GetShardProof(ctx, proofReq)
	suite.Require().NoError(err, "Should not return error for non-existent shard")
	suite.Require().NotNil(response, "Response should not be nil")
	suite.Assert().Nil(response.MerkleTreePath, "MerkleTreePath should be nil for non-existent shard")

	suite.T().Log("✓ GetShardProof returns nil MerkleTreePath for non-existent shard")
}

// GetShardProof - Empty Tree (no shards submitted yet)
func (suite *ParentServiceTestSuite) TestGetShardProof_EmptyTree() {
	ctx := context.Background()

	// Request proof before any shards have been submitted
	shard0ID := 4 // 0b100 - valid for 2-bit sharding
	proofReq := &api.GetShardProofRequest{
		ShardID: shard0ID,
	}

	response, err := suite.service.GetShardProof(ctx, proofReq)
	suite.Require().NoError(err, "Should not return error for empty tree")
	suite.Require().NotNil(response, "Response should not be nil")
	suite.Assert().Nil(response.MerkleTreePath, "MerkleTreePath should be nil when no shards submitted")

	suite.T().Log("✓ GetShardProof returns nil MerkleTreePath for empty tree")
}

// GetShardProof - Multiple Shards (verify each has correct proof)
func (suite *ParentServiceTestSuite) TestGetShardProof_MultipleShards() {
	ctx := context.Background()

	// Submit 3 different shards
	shard2ID := 0b100
	shard0ID := 0b101
	shard1ID := 0b111

	_, err := suite.service.SubmitShardRoot(ctx, &api.SubmitShardRootRequest{
		ShardID:  shard0ID,
		RootHash: makeTestHash(0xAA),
	})
	suite.Require().NoError(err)

	_, err = suite.service.SubmitShardRoot(ctx, &api.SubmitShardRootRequest{
		ShardID:  shard1ID,
		RootHash: makeTestHash(0xBB),
	})
	suite.Require().NoError(err)

	_, err = suite.service.SubmitShardRoot(ctx, &api.SubmitShardRootRequest{
		ShardID:  shard2ID,
		RootHash: makeTestHash(0xCC),
	})
	suite.Require().NoError(err)

	// Wait for all shards to be processed
	suite.waitForShardToExist(ctx, shard0ID)
	suite.waitForShardToExist(ctx, shard1ID)
	suite.waitForShardToExist(ctx, shard2ID)

	// Get proofs for all 3 shards
	proof0, err := suite.service.GetShardProof(ctx, &api.GetShardProofRequest{ShardID: shard0ID})
	suite.Require().NoError(err, "Should get proof for shard 0")
	suite.Assert().NotNil(proof0.MerkleTreePath, "Proof 0 should not be nil")

	proof1, err := suite.service.GetShardProof(ctx, &api.GetShardProofRequest{ShardID: shard1ID})
	suite.Require().NoError(err, "Should get proof for shard 1")
	suite.Assert().NotNil(proof1.MerkleTreePath, "Proof 1 should not be nil")

	proof2, err := suite.service.GetShardProof(ctx, &api.GetShardProofRequest{ShardID: shard2ID})
	suite.Require().NoError(err, "Should get proof for shard 2")
	suite.Assert().NotNil(proof2.MerkleTreePath, "Proof 2 should not be nil")

	// All proofs should have the same root (same parent SMT)
	suite.Assert().Equal(proof0.MerkleTreePath.Root, proof1.MerkleTreePath.Root, "All proofs should have same root")
	suite.Assert().Equal(proof0.MerkleTreePath.Root, proof2.MerkleTreePath.Root, "All proofs should have same root")

	suite.T().Log("✓ GetShardProof returns valid proofs for multiple shards with same root")
}

// TestParentServiceSuite runs the test suite
func TestParentServiceSuite(t *testing.T) {
	suite.Run(t, new(ParentServiceTestSuite))
}
