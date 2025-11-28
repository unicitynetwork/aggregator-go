package round

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ParentRoundManagerTestSuite is the test suite for parent round manager
type ParentRoundManagerTestSuite struct {
	suite.Suite
	cfg      *config.Config
	logger   *logger.Logger
	storage  *mongodb.Storage
	eventBus *events.EventBus
	cleanup  func()
}

// SetupSuite runs once before all tests - creates one MongoDB container for all tests
func (suite *ParentRoundManagerTestSuite) SetupSuite() {
	var err error
	suite.logger, err = logger.New("info", "text", "stdout", false)
	require.NoError(suite.T(), err, "Should create logger")

	suite.eventBus = events.NewEventBus(suite.logger)

	suite.cfg = &config.Config{
		Sharding: config.ShardingConfig{
			Mode:          config.ShardingModeParent,
			ShardIDLength: 4, // 4 bits = 16 possible shards (realistic for testing)
		},
		Database: config.DatabaseConfig{
			Database:       "test_parent_aggregator",
			ConnectTimeout: 5 * time.Second,
		},
		BFT: config.BFTConfig{
			Enabled: false, // Will use BFT stub
		},
		Processing: config.ProcessingConfig{
			RoundDuration: 100 * time.Millisecond, // Short duration for fast tests
		},
	}

	// Create storage once for all tests (reuses same MongoDB container)
	suite.storage = testutil.SetupTestStorage(suite.T(), *suite.cfg)
}

// TearDownSuite runs once after all tests
func (suite *ParentRoundManagerTestSuite) TearDownSuite() {
	if suite.cleanup != nil {
		suite.cleanup()
	}
}

// TearDownTest runs after each test to clean all collections
func (suite *ParentRoundManagerTestSuite) TearDownTest() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Clean all collections to ensure clean state for next test
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

// Test 1: Initialization
func (suite *ParentRoundManagerTestSuite) TestInitialization() {
	ctx := context.Background()

	// Create parent round manager (BFT stub will be created automatically when BFT.Enabled = false)
	prm, err := NewParentRoundManager(ctx, suite.cfg, suite.logger, suite.storage, nil, suite.eventBus, smt.NewThreadSafeSMT(smt.NewParentSparseMerkleTree(api.SHA256, suite.cfg.Sharding.ShardIDLength)))
	suite.Require().NoError(err, "Should create parent round manager successfully")
	suite.Require().NotNil(prm, "ParentRoundManager should not be nil")

	// Verify initial state
	suite.Assert().NotNil(prm.parentSMT, "Parent SMT should be initialized")
	suite.Assert().NotNil(prm.storage, "Storage should be set")
	suite.Assert().NotNil(prm.logger, "Logger should be set")

	// Since BFT is disabled in config, it should use BFT stub
	suite.Assert().NotNil(prm.bftClient, "BFT client stub should be initialized")

	suite.T().Log("✓ ParentRoundManager initialized successfully with BFT stub")
}

// Test 2: Basic Round Lifecycle
func (suite *ParentRoundManagerTestSuite) TestBasicRoundLifecycle() {
	ctx := context.Background()

	prm, err := NewParentRoundManager(ctx, suite.cfg, suite.logger, suite.storage, nil, suite.eventBus, smt.NewThreadSafeSMT(smt.NewParentSparseMerkleTree(api.SHA256, suite.cfg.Sharding.ShardIDLength)))
	suite.Require().NoError(err)
	defer prm.Stop(ctx) // Stop round manager before cleanup to avoid disconnection errors

	// Start the parent round manager (initialization and SMT restoration)
	err = prm.Start(ctx)
	suite.Require().NoError(err)

	// Activate the round manager to start round processing
	err = prm.Activate(ctx)
	suite.Require().NoError(err)

	// Create 2 shard updates (with ShardIDLength=4, valid IDs are 16-31)
	shard0ID := 16 // 0b10000
	shard1ID := 17 // 0b10001

	shard0Root := makeTestHash(0xAA)
	shard1Root := makeTestHash(0xBB)

	// Submit shard updates
	update0 := models.NewShardRootUpdate(shard0ID, shard0Root)
	update1 := models.NewShardRootUpdate(shard1ID, shard1Root)

	err = prm.SubmitShardRoot(ctx, update0)
	suite.Require().NoError(err, "Should submit shard 0 update")

	err = prm.SubmitShardRoot(ctx, update1)
	suite.Require().NoError(err, "Should submit shard 1 update")

	// Process the round
	// The BFT stub will automatically process the round after roundDuration (100ms)
	// and start the next round
	time.Sleep(150 * time.Millisecond) // Wait for round to process

	// Get the parent SMT root after processing
	parentRoot := prm.parentSMT.GetRootHash()
	suite.Require().NotNil(parentRoot, "Parent root should be calculated")

	suite.T().Logf("Parent root hash after 2 shard updates: %x", parentRoot[:16])
	suite.T().Log("✓ Basic round lifecycle completed successfully")
}

// Test 3: Multi-Round Updates
// TODO: This test will fail until SMT supports updating existing leaves
func (suite *ParentRoundManagerTestSuite) TestMultiRoundUpdates() {
	suite.T().Skip("TODO(SMT): enable once sparse Merkle tree supports updating existing leaves")
	ctx := context.Background()

	prm, err := NewParentRoundManager(ctx, suite.cfg, suite.logger, suite.storage, nil, suite.eventBus, smt.NewThreadSafeSMT(smt.NewParentSparseMerkleTree(api.SHA256, suite.cfg.Sharding.ShardIDLength)))
	suite.Require().NoError(err)
	defer prm.Stop(ctx) // Stop round manager before cleanup to avoid disconnection errors

	err = prm.Start(ctx)
	suite.Require().NoError(err)

	// Activate the round manager to start round processing
	err = prm.Activate(ctx)
	suite.Require().NoError(err)

	// With ShardIDLength=4, valid shard IDs are 16-31
	shard0ID := 16
	shard1ID := 17

	// Round 1: Both shards submit initial roots
	suite.T().Log("=== Round 1: Initial shard roots ===")
	update0_r1 := models.NewShardRootUpdate(shard0ID, makeTestHash(0x11))
	update1_r1 := models.NewShardRootUpdate(shard1ID, makeTestHash(0x22))

	err = prm.SubmitShardRoot(ctx, update0_r1)
	suite.Require().NoError(err)
	err = prm.SubmitShardRoot(ctx, update1_r1)
	suite.Require().NoError(err)

	time.Sleep(150 * time.Millisecond) // Wait for round 1 to process (100ms timer + buffer)
	root1 := prm.parentSMT.GetRootHash()
	suite.T().Logf("Parent root after round 1: %x", root1[:16])

	// Round 2: Shard 0 submits UPDATED root (Shard 1 unchanged)
	suite.T().Log("=== Round 2: Shard 0 updates ===")
	update0_r2 := models.NewShardRootUpdate(shard0ID, makeTestHash(0x33))

	err = prm.SubmitShardRoot(ctx, update0_r2)
	suite.Require().NoError(err)

	time.Sleep(150 * time.Millisecond) // Wait for round 2 to process
	root2 := prm.parentSMT.GetRootHash()
	suite.T().Logf("Parent root after round 2: %x", root2[:16])

	// Root should have changed because shard 0 updated
	suite.Assert().NotEqual(root1, root2, "Parent root should change when shard 0 updates")

	// Round 3: Shard 1 also updates
	suite.T().Log("=== Round 3: Shard 1 updates ===")
	update1_r3 := models.NewShardRootUpdate(shard1ID, makeTestHash(0x44))

	err = prm.SubmitShardRoot(ctx, update1_r3)
	suite.Require().NoError(err)

	time.Sleep(150 * time.Millisecond) // Wait for round 3 to process
	root3 := prm.parentSMT.GetRootHash()
	suite.T().Logf("Parent root after round 3: %x", root3[:16])

	// Root should have changed again
	suite.Assert().NotEqual(root2, root3, "Parent root should change when shard 1 updates")
	suite.Assert().NotEqual(root1, root3, "Parent root should be different from round 1")

	suite.T().Log("✓ Multi-round updates work correctly")
}

// Test 4: Multiple Shards in One Round
func (suite *ParentRoundManagerTestSuite) TestMultipleShards() {
	ctx := context.Background()

	prm, err := NewParentRoundManager(ctx, suite.cfg, suite.logger, suite.storage, nil, suite.eventBus, smt.NewThreadSafeSMT(smt.NewParentSparseMerkleTree(api.SHA256, suite.cfg.Sharding.ShardIDLength)))
	suite.Require().NoError(err)
	defer prm.Stop(ctx)

	err = prm.Start(ctx)
	suite.Require().NoError(err)

	// Activate the round manager to start round processing
	err = prm.Activate(ctx)
	suite.Require().NoError(err)

	// Submit 4 different shard updates (with ShardIDLength=4, valid IDs are 16-31)
	shard0ID := 16
	shard1ID := 17
	shard2ID := 18
	shard3ID := 19

	update0 := models.NewShardRootUpdate(shard0ID, makeTestHash(0x10))
	update1 := models.NewShardRootUpdate(shard1ID, makeTestHash(0x20))
	update2 := models.NewShardRootUpdate(shard2ID, makeTestHash(0x30))
	update3 := models.NewShardRootUpdate(shard3ID, makeTestHash(0x40))

	err = prm.SubmitShardRoot(ctx, update0)
	suite.Require().NoError(err, "Should submit shard 0")

	err = prm.SubmitShardRoot(ctx, update1)
	suite.Require().NoError(err, "Should submit shard 1")

	err = prm.SubmitShardRoot(ctx, update2)
	suite.Require().NoError(err, "Should submit shard 2")

	err = prm.SubmitShardRoot(ctx, update3)
	suite.Require().NoError(err, "Should submit shard 3")

	// Wait for round to process
	time.Sleep(150 * time.Millisecond)

	// Get the parent SMT root - if it's not empty, the round processed successfully
	parentRoot := prm.parentSMT.GetRootHash()
	suite.Require().NotNil(parentRoot, "Parent root should be calculated")
	suite.Assert().NotEqual([]byte{0}, parentRoot, "Parent root should not be empty")

	suite.T().Logf("Parent root with 4 shards: %x", parentRoot[:16])
	suite.T().Log("✓ Multiple shards processed correctly in one round")
}

// Test 5: Empty Round (no shard updates)
func (suite *ParentRoundManagerTestSuite) TestEmptyRound() {
	ctx := context.Background()

	prm, err := NewParentRoundManager(ctx, suite.cfg, suite.logger, suite.storage, nil, suite.eventBus, smt.NewThreadSafeSMT(smt.NewParentSparseMerkleTree(api.SHA256, suite.cfg.Sharding.ShardIDLength)))
	suite.Require().NoError(err)
	defer prm.Stop(ctx)

	err = prm.Start(ctx)
	suite.Require().NoError(err)

	// Activate the round manager to start round processing
	err = prm.Activate(ctx)
	suite.Require().NoError(err)

	// Get the initial root (should be empty tree root)
	initialRoot := prm.parentSMT.GetRootHash()
	suite.Require().NotNil(initialRoot, "Initial root should exist")

	// Don't submit any shard updates - just wait for the round to process
	time.Sleep(time.Second)

	// The root should remain the same (no changes)
	currentRoot := prm.parentSMT.GetRootHash()
	suite.Assert().Equal(initialRoot, currentRoot, "Root should not change in empty round")

	// Verify that a block was still created (empty rounds still create blocks)
	block, err := suite.storage.BlockStorage().GetLatest(ctx)
	suite.Require().NoError(err, "Should get latest block")
	suite.Require().NotNil(block, "Block should exist even for empty round")

	suite.T().Logf("Empty round processed with root: %x", currentRoot[:16])
	suite.T().Log("✓ Empty round processed correctly")
}

// Test 6: Duplicate Shard Update (same shard, same value)
func (suite *ParentRoundManagerTestSuite) TestDuplicateShardUpdate() {
	ctx := context.Background()

	prm, err := NewParentRoundManager(ctx, suite.cfg, suite.logger, suite.storage, nil, suite.eventBus, smt.NewThreadSafeSMT(smt.NewParentSparseMerkleTree(api.SHA256, suite.cfg.Sharding.ShardIDLength)))
	suite.Require().NoError(err)
	defer prm.Stop(ctx)

	err = prm.Start(ctx)
	suite.Require().NoError(err)

	// Activate the round manager to start round processing
	err = prm.Activate(ctx)
	suite.Require().NoError(err)

	// With ShardIDLength=4, valid shard IDs are 16-31
	shard0ID := 16
	shard0Root := makeTestHash(0xAA)

	// Submit the same shard update twice with the same value
	update1 := models.NewShardRootUpdate(shard0ID, shard0Root)
	update2 := models.NewShardRootUpdate(shard0ID, shard0Root)

	err = prm.SubmitShardRoot(ctx, update1)
	suite.Require().NoError(err, "First submission should succeed")

	err = prm.SubmitShardRoot(ctx, update2)
	suite.Require().NoError(err, "Second submission (duplicate) should succeed")

	// Wait for round to process
	time.Sleep(150 * time.Millisecond)

	// Get the parent SMT root - should be calculated correctly
	parentRoot := prm.parentSMT.GetRootHash()
	suite.Require().NotNil(parentRoot, "Parent root should be calculated")

	// The duplicate submission should not cause any issues
	// The SMT should have exactly one leaf for shard 0
	suite.T().Logf("Parent root with duplicate shard update: %x", parentRoot[:16])
	suite.T().Log("✓ Duplicate shard update handled correctly")
}

// Test 7: Multiple Updates from Same Shard (different values - latest should win)
// TODO: This test will fail until SMT supports updating existing leaves
func (suite *ParentRoundManagerTestSuite) TestSameShardMultipleValues() {
	suite.T().Skip("TODO(SMT): enable once sparse Merkle tree supports updating existing leaves")
	ctx := context.Background()

	prm, err := NewParentRoundManager(ctx, suite.cfg, suite.logger, suite.storage, nil, suite.eventBus, smt.NewThreadSafeSMT(smt.NewParentSparseMerkleTree(api.SHA256, suite.cfg.Sharding.ShardIDLength)))
	suite.Require().NoError(err)
	defer prm.Stop(ctx)

	err = prm.Start(ctx)
	suite.Require().NoError(err)

	// Activate the round manager to start round processing
	err = prm.Activate(ctx)
	suite.Require().NoError(err)

	// With ShardIDLength=4, valid shard IDs are 16-31
	shard0ID := 16
	latestValue := makeTestHash(0xCC)

	// Submit 3 different updates for the same shard
	update1 := models.NewShardRootUpdate(shard0ID, makeTestHash(0xAA))
	update2 := models.NewShardRootUpdate(shard0ID, makeTestHash(0xBB))
	update3 := models.NewShardRootUpdate(shard0ID, latestValue) // This should be the final value

	err = prm.SubmitShardRoot(ctx, update1)
	suite.Require().NoError(err, "First submission should succeed")

	err = prm.SubmitShardRoot(ctx, update2)
	suite.Require().NoError(err, "Second submission should succeed")

	err = prm.SubmitShardRoot(ctx, update3)
	suite.Require().NoError(err, "Third submission should succeed")

	// Wait for round to process
	time.Sleep(150 * time.Millisecond)

	// Get the parent SMT root
	parentRoot := prm.parentSMT.GetRootHash()
	suite.Require().NotNil(parentRoot, "Parent root should be calculated")

	// Create a reference SMT with only the latest value to verify
	smtInstance := smt.NewParentSparseMerkleTree(api.SHA256, suite.cfg.Sharding.ShardIDLength)
	referenceSMT := smt.NewThreadSafeSMT(smtInstance)

	// Add the latest value as a pre-hashed leaf (same way ParentRoundManager does it)
	err = referenceSMT.AddPreHashedLeaf(update3.GetPath(), latestValue)
	suite.Require().NoError(err, "Should add leaf to reference SMT")

	expectedRoot := referenceSMT.GetRootHash()
	suite.Assert().Equal(expectedRoot, parentRoot, "Parent root should match reference SMT with only latest value")

	suite.T().Logf("Parent root with latest value (0xCC): %s", parentRoot)
	suite.T().Logf("Expected root (reference SMT):       %s", expectedRoot)
	suite.T().Log("✓ Latest shard update value was used correctly")
}

// Test 8: Block root persisted in storage matches SMT root after round finalization
func (suite *ParentRoundManagerTestSuite) TestBlockRootMatchesSMTRoot() {
	ctx := context.Background()

	prm, err := NewParentRoundManager(ctx, suite.cfg, suite.logger, suite.storage, nil, suite.eventBus, smt.NewThreadSafeSMT(smt.NewParentSparseMerkleTree(api.SHA256, suite.cfg.Sharding.ShardIDLength)))
	suite.Require().NoError(err)
	defer prm.Stop(ctx)

	err = prm.Start(ctx)
	suite.Require().NoError(err)

	err = prm.Activate(ctx)
	suite.Require().NoError(err)

	// With ShardIDLength=4, valid shard IDs are 16-31 (binary 10000-11111)
	shardID := 16
	update := models.NewShardRootUpdate(shardID, makeTestHash(0xAB))
	err = prm.SubmitShardRoot(ctx, update)
	suite.Require().NoError(err)

	var latestBlock *models.Block
	suite.Require().Eventually(func() bool {
		block, err := suite.storage.BlockStorage().GetLatest(ctx)
		if err != nil || block == nil {
			return false
		}
		latestBlock = block
		return true
	}, 5*time.Second, 50*time.Millisecond, "expected finalized block to be available")

	currentRootHex := prm.GetSMT().GetRootHash()
	expectedRoot, err := api.NewHexBytesFromString(currentRootHex)
	suite.Require().NoError(err)
	suite.Require().NotNil(latestBlock, "latest block should be available")

	suite.Assert().Equal(expectedRoot.String(), latestBlock.RootHash.String(), "stored block root should match SMT root")
}

// TestParentRoundManagerSuite runs the test suite
func TestParentRoundManagerSuite(t *testing.T) {
	suite.Run(t, new(ParentRoundManagerTestSuite))
}
