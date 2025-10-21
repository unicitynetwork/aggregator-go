package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/ha"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/service"
	"github.com/unicitynetwork/aggregator-go/internal/storage"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type ShardingE2ETestSuite struct {
	suite.Suite
	mongoContainer testcontainers.Container
	mongoURI       string
	instances      []*aggregatorInstance
}

type aggregatorInstance struct {
	name            string
	cfg             *config.Config
	logger          *logger.Logger
	commitmentQueue interfaces.CommitmentQueue
	storage         interfaces.Storage
	manager         round.Manager
	service         gateway.Service
	server          *gateway.Server
	leaderSelector  *ha.LeaderElection
	cleanup         func()
}

type jsonRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      int         `json:"id"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    string `json:"data,omitempty"`
	} `json:"error,omitempty"`
	ID int `json:"id"`
}

func (suite *ShardingE2ETestSuite) SetupSuite() {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mongo:7.0",
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor:   wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
	}

	mongoContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	suite.Require().NoError(err)

	host, err := mongoContainer.Host(ctx)
	suite.Require().NoError(err)

	port, err := mongoContainer.MappedPort(ctx, "27017")
	suite.Require().NoError(err)

	suite.mongoContainer = mongoContainer
	suite.mongoURI = fmt.Sprintf("mongodb://%s:%s", host, port.Port())
	suite.instances = make([]*aggregatorInstance, 0)

	suite.T().Logf("MongoDB container started at %s", suite.mongoURI)
}

func (suite *ShardingE2ETestSuite) TearDownSuite() {
	ctx := context.Background()

	for _, inst := range suite.instances {
		if inst.cleanup != nil {
			inst.cleanup()
		}
	}

	if suite.mongoContainer != nil {
		suite.mongoContainer.Terminate(ctx)
	}
}

func (suite *ShardingE2ETestSuite) buildConfig(mode config.ShardingMode, port, dbName, serverID string, shardID int) *config.Config {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host:             "localhost",
			Port:             port,
			EnableCORS:       true,
			EnableDocs:       false,
			ReadTimeout:      30 * time.Second,
			WriteTimeout:     30 * time.Second,
			IdleTimeout:      60 * time.Second,
			ConcurrencyLimit: 100,
		},
		Database: config.DatabaseConfig{
			URI:                    suite.mongoURI,
			Database:               dbName,
			ConnectTimeout:         10 * time.Second,
			ServerSelectionTimeout: 10 * time.Second,
			SocketTimeout:          10 * time.Second,
			MaxPoolSize:            10,
			MinPoolSize:            2,
		},
		HA: config.HAConfig{
			Enabled: false,
		},
		Logging: config.LoggingConfig{
			Level:  "info",
			Format: "json",
		},
		BFT: config.BFTConfig{
			Enabled: false,
		},
		Processing: config.ProcessingConfig{
			RoundDuration:          100 * time.Millisecond,
			BatchLimit:             1000,
			MaxCommitmentsPerRound: 1000,
		},
		Storage: config.StorageConfig{
			UseRedisForCommitments: false,
		},
		Sharding: config.ShardingConfig{
			Mode:          mode,
			ShardIDLength: 2, // 4 shards: IDs 4-7
		},
	}

	// Parent-specific configuration
	if mode == config.ShardingModeParent {
		cfg.HA.Enabled = true
		cfg.HA.LockTTLSeconds = 30
		cfg.HA.LeaderHeartbeatInterval = 5 * time.Second
		cfg.HA.LeaderElectionPollingInterval = 1 * time.Second
		cfg.HA.LockID = "test-parent-lock"
		cfg.HA.ServerID = serverID
	}

	// Child-specific configuration
	if mode == config.ShardingModeChild {
		cfg.Sharding.Child = config.ChildConfig{
			ParentRpcAddr:      "http://localhost:9000",
			ShardID:            shardID,
			RoundDuration:      100 * time.Millisecond,
			ParentPollTimeout:  5 * time.Second,
			ParentPollInterval: 100 * time.Millisecond,
		}
	}

	return cfg
}

func (suite *ShardingE2ETestSuite) startAggregatorInstance(name string, cfg *config.Config) *aggregatorInstance {
	ctx := context.Background()

	log, err := logger.New(cfg.Logging.Level, cfg.Logging.Format, cfg.Logging.Output, cfg.Logging.EnableJSON)
	suite.Require().NoError(err)

	commitmentQueue, storageInstance, err := storage.NewStorage(cfg, log)
	suite.Require().NoError(err)

	err = commitmentQueue.Initialize(ctx)
	suite.Require().NoError(err)

	stateTracker := state.NewSyncStateTracker()

	manager, err := round.NewManager(ctx, cfg, log, commitmentQueue, storageInstance, stateTracker)
	suite.Require().NoError(err)

	err = manager.Start(ctx)
	suite.Require().NoError(err)

	var leaderSelector *ha.LeaderElection
	var haManager *ha.HAManager

	if cfg.HA.Enabled {
		leaderSelector = ha.NewLeaderElection(log, cfg.HA, storageInstance.LeadershipStorage())
		leaderSelector.Start(ctx)

		time.Sleep(100 * time.Millisecond)

		disableBlockSync := cfg.Sharding.Mode == config.ShardingModeParent
		haManager = ha.NewHAManager(log, manager, leaderSelector, storageInstance, manager.GetSMT(), cfg.Sharding.Child.ShardID, stateTracker, cfg.Processing.RoundDuration, disableBlockSync)
		haManager.Start(ctx)
	} else {
		err = manager.Activate(ctx)
		suite.Require().NoError(err)
	}

	svc, err := service.NewService(ctx, cfg, log, manager, commitmentQueue, storageInstance, leaderSelector)
	suite.Require().NoError(err)

	server := gateway.NewServer(cfg, log, svc)

	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Error("Server error", "error", err.Error())
		}
	}()

	time.Sleep(200 * time.Millisecond)

	inst := &aggregatorInstance{
		name:            name,
		cfg:             cfg,
		logger:          log,
		commitmentQueue: commitmentQueue,
		storage:         storageInstance,
		manager:         manager,
		service:         svc,
		server:          server,
		leaderSelector:  leaderSelector,
		cleanup: func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server.Stop(shutdownCtx)
			if haManager != nil {
				haManager.Stop()
			}
			if leaderSelector != nil {
				leaderSelector.Stop(context.Background())
			}
			manager.Stop(context.Background())
			storageInstance.Close(context.Background())
		},
	}

	suite.instances = append(suite.instances, inst)
	suite.T().Logf("✓ Started %s on :%s", name, cfg.Server.Port)

	return inst
}

func (suite *ShardingE2ETestSuite) rpcCall(url string, method string, params interface{}) (json.RawMessage, error) {
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      1,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	var rpcResp jsonRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	return rpcResp.Result, nil
}

func (suite *ShardingE2ETestSuite) submitCommitment(url string, commitment *api.SubmitCommitmentRequest) (*api.SubmitCommitmentResponse, error) {
	result, err := suite.rpcCall(url, "submit_commitment", commitment)
	if err != nil {
		return nil, err
	}

	var response api.SubmitCommitmentResponse
	if err := json.Unmarshal(result, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &response, nil
}

func (suite *ShardingE2ETestSuite) getInclusionProof(url string, requestID string) (*api.GetInclusionProofResponse, error) {
	params := map[string]string{"requestId": requestID}
	result, err := suite.rpcCall(url, "get_inclusion_proof", params)
	if err != nil {
		return nil, err
	}

	var response api.GetInclusionProofResponse
	if err := json.Unmarshal(result, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &response, nil
}

func (suite *ShardingE2ETestSuite) getBlockHeight(url string) (*api.GetBlockHeightResponse, error) {
	result, err := suite.rpcCall(url, "get_block_height", nil)
	if err != nil {
		return nil, err
	}

	var response api.GetBlockHeightResponse
	if err := json.Unmarshal(result, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &response, nil
}

func (suite *ShardingE2ETestSuite) createCommitmentForShard(shardID int, shardIDLength int) (*api.SubmitCommitmentRequest, string) {
	// Shard ID is encoded in the LSBs of the requestID (see commitment_validator.go verifyShardID)
	msbPos := shardIDLength
	compareMask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), uint(msbPos)), big.NewInt(1))
	expectedLSBs := new(big.Int).And(big.NewInt(int64(shardID)), compareMask)

	for attempts := 0; attempts < 1000; attempts++ {
		baseData := fmt.Sprintf("shard_%d_attempt_%d", shardID, attempts)
		commitment := testutil.CreateTestCommitment(suite.T(), baseData)

		requestIDBytes, err := commitment.RequestID.Bytes()
		suite.Require().NoError(err)
		requestIDBigInt := new(big.Int).SetBytes(requestIDBytes)
		requestIDLSBs := new(big.Int).And(requestIDBigInt, compareMask)

		if requestIDLSBs.Cmp(expectedLSBs) == 0 {
			receipt := true
			apiCommitment := &api.SubmitCommitmentRequest{
				RequestID:       commitment.RequestID,
				TransactionHash: api.TransactionHash(commitment.TransactionHash),
				Authenticator: api.Authenticator{
					Algorithm: commitment.Authenticator.Algorithm,
					PublicKey: api.HexBytes(commitment.Authenticator.PublicKey),
					Signature: api.HexBytes(commitment.Authenticator.Signature),
					StateHash: api.StateHash(commitment.Authenticator.StateHash),
				},
				Receipt: &receipt,
			}

			return apiCommitment, commitment.RequestID.String()
		}
	}

	suite.FailNow("Failed to generate commitment for shard after 1000 attempts")
	return nil, ""
}

func (suite *ShardingE2ETestSuite) waitForBlock(url string, blockNumber int64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := suite.getBlockHeight(url)
		if err == nil && resp.BlockNumber != nil && resp.BlockNumber.Cmp(big.NewInt(blockNumber)) >= 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	suite.FailNow(fmt.Sprintf("Timeout waiting for block %d at %s", blockNumber, url))
}

// waitForProofAvailable waits for a VALID inclusion proof to become available
// This includes waiting for the parent proof to be received and joined
func (suite *ShardingE2ETestSuite) waitForProofAvailable(url, requestID string, timeout time.Duration) *api.GetInclusionProofResponse {
	deadline := time.Now().Add(timeout)
	reqID := api.RequestID(requestID)
	reqIDPath, err := reqID.GetPath()
	suite.Require().NoError(err)

	for time.Now().Before(deadline) {
		resp, err := suite.getInclusionProof(url, requestID)
		if err == nil && resp.InclusionProof != nil && resp.InclusionProof.MerkleTreePath != nil {
			// Also verify that the proof is valid (includes parent proof)
			result, verifyErr := resp.InclusionProof.MerkleTreePath.Verify(reqIDPath)
			if verifyErr == nil && result != nil && result.Result {
				return resp
			}
			// Proof exists but not valid yet (probably waiting for parent proof), keep retrying
		}
		time.Sleep(50 * time.Millisecond)
	}
	suite.FailNow(fmt.Sprintf("Timeout waiting for valid proof for requestID %s at %s", requestID, url))
	return nil
}

// TestShardingE2E verifies the complete hierarchical sharding flow:
// 1. Starts a parent aggregator and 2 child aggregators (shards 4 and 5)
// 2. Submits commitments to children → children create blocks and submit roots to parent
// 3. Parent creates aggregated block → children poll and receive parent proofs
// 4. Verifies that clients can retrieve and validate fully joined proofs
// 5. Tests that proofs remain valid after additional blocks are created
func (suite *ShardingE2ETestSuite) TestShardingE2E() {
	ctx := context.Background()
	_ = ctx

	parentCfg := suite.buildConfig(config.ShardingModeParent, "9000", "aggregator_test_parent", "test-parent-server", 0)
	suite.startAggregatorInstance("parent aggregator", parentCfg)
	parentURL := "http://localhost:9000"

	child0Cfg := suite.buildConfig(config.ShardingModeChild, "9001", "aggregator_test_child_0", "", 4)
	suite.startAggregatorInstance("child aggregator 0 (shard 4)", child0Cfg)
	child0URL := "http://localhost:9001"

	child1Cfg := suite.buildConfig(config.ShardingModeChild, "9002", "aggregator_test_child_1", "", 5)
	suite.startAggregatorInstance("child aggregator 1 (shard 5)", child1Cfg)
	child1URL := "http://localhost:9002"

	time.Sleep(500 * time.Millisecond)

	suite.T().Log("Phase 1: Submitting commitments...")
	commitment1, reqID1 := suite.createCommitmentForShard(4, 2)
	resp1, err := suite.submitCommitment(child0URL, commitment1)
	suite.Require().NoError(err)
	suite.Require().Equal("SUCCESS", resp1.Status)
	suite.T().Logf("  Submitted commitment 1 to child 0: %s", reqID1)

	commitment2, reqID2 := suite.createCommitmentForShard(4, 2)
	resp2, err := suite.submitCommitment(child0URL, commitment2)
	suite.Require().NoError(err)
	suite.Require().Equal("SUCCESS", resp2.Status)
	suite.T().Logf("  Submitted commitment 2 to child 0: %s", reqID2)

	commitment3, reqID3 := suite.createCommitmentForShard(5, 2)
	resp3, err := suite.submitCommitment(child1URL, commitment3)
	suite.Require().NoError(err)
	suite.Require().Equal("SUCCESS", resp3.Status)
	suite.T().Logf("  Submitted commitment 3 to child 1: %s", reqID3)

	commitment4, reqID4 := suite.createCommitmentForShard(5, 2)
	resp4, err := suite.submitCommitment(child1URL, commitment4)
	suite.Require().NoError(err)
	suite.Require().Equal("SUCCESS", resp4.Status)
	suite.T().Logf("  Submitted commitment 4 to child 1: %s", reqID4)

	suite.T().Log("✓ Submitted 2 commitments to child 0")
	suite.T().Log("✓ Submitted 2 commitments to child 1")

	suite.T().Log("Phase 2: Waiting for parent block...")
	suite.waitForBlock(parentURL, 1, 5*time.Second)
	suite.T().Log("✓ Parent created block 1 (children submitted roots)")

	// Give extra time for children to receive parent proofs and finalize blocks
	time.Sleep(500 * time.Millisecond)

	child0Height, err := suite.getBlockHeight(child0URL)
	suite.Require().NoError(err)
	suite.T().Logf("Child 0 block height: %s", child0Height.BlockNumber.String())

	child1Height, err := suite.getBlockHeight(child1URL)
	suite.Require().NoError(err)
	suite.T().Logf("Child 1 block height: %s", child1Height.BlockNumber.String())

	suite.T().Log("Phase 3: Verifying joined proofs...")

	testCases := []struct {
		requestID string
		childURL  string
		shardID   int
		name      string
	}{
		{reqID1, child0URL, 4, "commitment 1 (child 0)"},
		{reqID2, child0URL, 4, "commitment 2 (child 0)"},
		{reqID3, child1URL, 5, "commitment 3 (child 1)"},
		{reqID4, child1URL, 5, "commitment 4 (child 1)"},
	}

	for _, tc := range testCases {
		childProofResp, err := suite.getInclusionProof(tc.childURL, tc.requestID)
		suite.Require().NoError(err, "Failed to get inclusion proof for %s", tc.name)
		suite.Require().NotNil(childProofResp.InclusionProof, "Inclusion proof is nil for %s", tc.name)
		suite.Require().NotNil(childProofResp.InclusionProof.MerkleTreePath, "Merkle path is nil for %s", tc.name)
		joinedProof := childProofResp.InclusionProof.MerkleTreePath

		reqID := api.RequestID(tc.requestID)
		reqIDPath, err := reqID.GetPath()
		suite.Require().NoError(err, "Failed to get path from requestID for %s", tc.name)

		result, err := joinedProof.Verify(reqIDPath)
		suite.Require().NoError(err, "Proof verification failed for %s", tc.name)
		suite.Require().NotNil(result, "Verification result is nil for %s", tc.name)

		suite.Require().True(result.PathValid, "Path not valid for %s", tc.name)
		suite.Require().True(result.PathIncluded, "Path not included for %s", tc.name)
		suite.Require().True(result.Result, "Overall verification failed for %s", tc.name)

		suite.T().Logf("✓ Verified joined proof for %s", tc.name)
	}

	suite.T().Log("✓ All initial proofs verified successfully!")

	suite.T().Log("Phase 4: Testing with additional blocks...")

	child0HeightBefore, err := suite.getBlockHeight(child0URL)
	suite.Require().NoError(err)
	child1HeightBefore, err := suite.getBlockHeight(child1URL)
	suite.Require().NoError(err)

	expectedChild0Height := child0HeightBefore.BlockNumber.Int.Int64() + 1
	expectedChild1Height := child1HeightBefore.BlockNumber.Int.Int64() + 1

	suite.T().Logf("Current heights before Phase 4: Child 0=%s, Child 1=%s",
		child0HeightBefore.BlockNumber.String(), child1HeightBefore.BlockNumber.String())

	commitment5, reqID5 := suite.createCommitmentForShard(4, 2)
	suite.submitCommitment(child0URL, commitment5)

	commitment6, reqID6 := suite.createCommitmentForShard(5, 2)
	suite.submitCommitment(child1URL, commitment6)

	suite.T().Log("✓ Submitted additional commitments")

	suite.T().Logf("Waiting for child 0 to reach block %d...", expectedChild0Height)
	suite.waitForBlock(child0URL, expectedChild0Height, 5*time.Second)
	suite.T().Logf("✓ Child 0 reached block %d", expectedChild0Height)

	suite.T().Logf("Waiting for child 1 to reach block %d...", expectedChild1Height)
	suite.waitForBlock(child1URL, expectedChild1Height, 5*time.Second)
	suite.T().Logf("✓ Child 1 reached block %d", expectedChild1Height)

	child0HeightAfter, err := suite.getBlockHeight(child0URL)
	suite.Require().NoError(err)
	suite.T().Logf("Child 0 block height after: %s", child0HeightAfter.BlockNumber.String())

	child1HeightAfter, err := suite.getBlockHeight(child1URL)
	suite.Require().NoError(err)
	suite.T().Logf("Child 1 block height after: %s", child1HeightAfter.BlockNumber.String())

	suite.T().Log("Verifying old commitments still work...")
	for _, tc := range testCases {
		childProofResp, err := suite.getInclusionProof(tc.childURL, tc.requestID)
		suite.Require().NoError(err, "Failed to get proof for old %s", tc.name)
		suite.Require().NotNil(childProofResp.InclusionProof)

		reqID := api.RequestID(tc.requestID)
		reqIDPath, err := reqID.GetPath()
		suite.Require().NoError(err)

		result, err := childProofResp.InclusionProof.MerkleTreePath.Verify(reqIDPath)
		suite.Require().NoError(err, "Verification failed for old %s", tc.name)
		suite.Require().True(result.Result, "Old commitment proof invalid for %s", tc.name)
	}
	suite.T().Log("✓ All old commitments still verify correctly")

	suite.T().Log("Verifying new commitments...")
	newTestCases := []struct {
		requestID string
		childURL  string
		name      string
	}{
		{reqID5, child0URL, "new commitment (child 0)"},
		{reqID6, child1URL, "new commitment (child 1)"},
	}

	for _, tc := range newTestCases {
		suite.T().Logf("Waiting for proof availability for %s...", tc.name)
		childProofResp := suite.waitForProofAvailable(tc.childURL, tc.requestID, 10*time.Second)
		suite.Require().NotNil(childProofResp.InclusionProof)

		reqID := api.RequestID(tc.requestID)
		reqIDPath, err := reqID.GetPath()
		suite.Require().NoError(err)

		result, err := childProofResp.InclusionProof.MerkleTreePath.Verify(reqIDPath)
		suite.Require().NoError(err, "Verification failed for %s", tc.name)
		suite.Require().True(result.Result, "New commitment proof invalid for %s", tc.name)

		suite.T().Logf("✓ Verified %s", tc.name)
	}

	suite.T().Log("✓ All new commitments verify correctly!")
	suite.T().Log("✓ E2E sharding test completed successfully - old and new proofs work!")
}

func TestShardingE2E(t *testing.T) {
	suite.Run(t, new(ShardingE2ETestSuite))
}
