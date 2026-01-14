package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	redisContainer "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/sharding"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// AggregatorTestSuite defines the test suite
type AggregatorTestSuite struct {
	suite.Suite
	serverAddr string
	ctx        context.Context
	cleanup    func()
}

// SetupTest runs before each test - ensures clean state
func (suite *AggregatorTestSuite) SetupTest() {
	suite.ctx = context.Background()
	serverAddr, cleanup := setupMongoDBAndAggregator(suite.T(), suite.ctx)
	suite.serverAddr = serverAddr
	suite.cleanup = cleanup
}

// TearDownTest runs after each test - ensures cleanup
func (suite *AggregatorTestSuite) TearDownTest() {
	if suite.cleanup != nil {
		suite.cleanup()
	}
}

func setupMongoDBAndAggregator(t *testing.T, ctx context.Context) (string, func()) {
	var mongoContainer *mongodb.MongoDBContainer

	// Override osExit to prevent test process termination during teardown
	restoreExit := round.SetExitFunc(func(code int) {
		t.Logf("os.Exit(%d) called but suppressed in test", code)
	})

	// Ensure cleanup happens even if setup fails
	defer func() {
		if r := recover(); r != nil {
			restoreExit()
			if mongoContainer != nil {
				mongoContainer.Terminate(context.Background())
			}
			panic(r)
		}
	}()

	// Start MongoDB container with replica set (required for transactions)
	mongoContainer, err := mongodb.Run(ctx, "mongo:7.0", mongodb.WithReplicaSet("rs0"))
	if err != nil {
		t.Fatalf("Failed to start MongoDB container: %v", err)
	}
	mongoURI, err := mongoContainer.ConnectionString(ctx)
	require.NoError(t, err)
	mongoURI += "&directConnection=true"

	// Start Redis container
	redis, err := redisContainer.Run(ctx, "redis:7")
	require.NoError(t, err)
	redisURI, err := redis.ConnectionString(ctx)
	require.NoError(t, err)
	redisURL, _ := url.Parse(redisURI)
	redisHost := redisURL.Hostname()
	redisPort, _ := strconv.Atoi(redisURL.Port())

	// Get a free port to avoid conflicts
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// Set environment variables for the aggregator
	os.Setenv("MONGODB_URI", mongoURI)
	os.Setenv("MONGODB_DATABASE", "aggregator_test")
	os.Setenv("PORT", strconv.Itoa(port))
	os.Setenv("HOST", "127.0.0.1")
	os.Setenv("LOG_LEVEL", "warn")
	os.Setenv("DISABLE_HIGH_AVAILABILITY", "true")
	os.Setenv("BFT_ENABLED", "false")

	// Load configuration
	cfg, err := config.Load()
	require.NoError(t, err)

	// Configure Redis for commitments
	cfg.Redis = config.RedisConfig{Host: redisHost, Port: redisPort}
	cfg.Storage.UseRedisForCommitments = true
	cfg.Storage.RedisStreamName = "commitments:test"
	cfg.Storage.RedisFlushInterval = 100 * time.Millisecond
	cfg.Storage.RedisMaxBatchSize = 1000
	cfg.Storage.RedisCleanupInterval = 1 * time.Minute
	cfg.Storage.RedisMaxStreamLength = 100000

	// Initialize logger
	log, err := logger.New("warn", "text", "stdout", false)
	require.NoError(t, err)

	// Initialize storage with Redis commitment queue
	commitmentQueue, mongoStorage, err := storage.NewStorage(ctx, cfg, log)
	require.NoError(t, err)
	commitmentQueue.Initialize(ctx)

	// Initialize round manager
	rootAggregatorClient := sharding.NewRootAggregatorClientStub()
	roundManager, err := round.NewRoundManager(ctx, cfg, log, commitmentQueue, mongoStorage, rootAggregatorClient, state.NewSyncStateTracker(), nil, events.NewEventBus(log), smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, 16+256)))
	require.NoError(t, err)

	// Start the round manager (restores SMT)
	err = roundManager.Start(ctx)
	require.NoError(t, err)

	// In non-HA test mode, we are the leader, so activate the service directly
	err = roundManager.Activate(ctx)
	require.NoError(t, err)

	// Initialize aggregator service
	aggregatorService := NewAggregatorService(cfg, log, roundManager, commitmentQueue, mongoStorage, nil)

	// Initialize gateway server
	server := gateway.NewServer(cfg, log, aggregatorService)

	// Start server in a goroutine
	go func() {
		if err := server.Start(); err != nil && err.Error() != "http: Server closed" {
			t.Logf("Server start error: %v", err)
		}
	}()

	// Wait for the server to be ready
	serverAddr := fmt.Sprintf("http://%s:%s", cfg.Server.Host, cfg.Server.Port)
	t.Logf("Waiting for server at %s", serverAddr)

	require.Eventually(t, func() bool {
		resp, err := http.Get(serverAddr + "/health")
		if err != nil {
			t.Logf("Health check failed: %v", err)
			return false
		}
		defer resp.Body.Close()
		t.Logf("Health check response: %d", resp.StatusCode)
		return resp.StatusCode == http.StatusOK
	}, 10*time.Second, 100*time.Millisecond, "Server failed to start")

	// Return the server address and cleanup function
	return serverAddr, func() {
		// Restore the original exit function at the end
		defer restoreExit()

		// Create shutdown context
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Stop the server gracefully
		if server != nil {
			t.Logf("Stopping HTTP server...")
			server.Stop(shutdownCtx)
		}

		// Stop round manager gracefully
		if roundManager != nil {
			t.Logf("Stopping round manager...")
			roundManager.Stop(shutdownCtx)
		}

		// Stop containers
		termCtx, termCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer termCancel()
		if redis != nil {
			redis.Terminate(termCtx)
		}
		if mongoContainer != nil {
			mongoContainer.Terminate(termCtx)
		}
	}
}

// Helper function to make JSON-RPC requests and unmarshal responses
func makeJSONRPCRequest[T any](t *testing.T, serverAddr, method, requestID string, params interface{}) T {
	request, err := jsonrpc.NewRequest(method, params, requestID)
	require.NoError(t, err)

	bodyBytes, err := json.Marshal(request)
	require.NoError(t, err)

	httpResponse, err := http.Post(serverAddr, "application/json", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	defer httpResponse.Body.Close()

	var response jsonrpc.Response
	err = json.NewDecoder(httpResponse.Body).Decode(&response)
	require.NoError(t, err)

	assert.Nil(t, response.Error, fmt.Sprintf("%s should succeed", method))

	resultBytes, err := json.Marshal(response.Result)
	require.NoError(t, err)

	var result T
	err = json.Unmarshal(resultBytes, &result)
	require.NoError(t, err)

	return result
}

// Helper function to validate inclusion proof structure and encoding
func validateInclusionProof(t *testing.T, proof *api.InclusionProofV2, stateID api.StateID) {
	assert.NotNil(t, proof.CertificationData, "Should have certification data")
	assert.NotNil(t, proof.MerkleTreePath, "Should have merkle tree path")

	// Validate unicity certificate field
	if len(proof.UnicityCertificate) > 0 {
		assert.NotEmpty(t, proof.UnicityCertificate, "Unicity certificate should not be empty")
	}

	// Validate certification data encoding
	if proof.CertificationData != nil {
		assert.NotEmpty(t, proof.CertificationData.OwnerPredicate, "CertificationData should have owner predicate")
		assert.NotEmpty(t, proof.CertificationData.Witness, "CertificationData should have signature")
		assert.NotEmpty(t, proof.CertificationData.SourceStateHash, "CertificationData should have source state hash")
		assert.NotEmpty(t, proof.CertificationData.TransactionHash, "CertificationData should have transaction hash")

		// Verify CBOR encoding of certification data
		certDataBytes, err := cbor.Marshal(proof.CertificationData)
		require.NoError(t, err, "CertificationData should be CBOR encodable")
		assert.NotEmpty(t, certDataBytes, "CBOR encoded certification data should not be empty")

		// Verify we can decode it back
		var decodedAuth api.CertificationData
		err = cbor.Unmarshal(certDataBytes, &decodedAuth)
		require.NoError(t, err, "Should be able to decode CBOR certification data")
	}

	// Validate merkle tree path encoding
	if proof.MerkleTreePath != nil {
		assert.NotEmpty(t, proof.MerkleTreePath.Root, "Merkle path should have root")
		assert.NotNil(t, proof.MerkleTreePath.Steps, "Merkle path should have steps")

		// Verify Merkle tree path with state ID
		stateIDBigInt, err := stateID.GetPath()
		require.NoError(t, err, "Should be able to get path from stateID")

		verificationResult, err := proof.MerkleTreePath.Verify(stateIDBigInt)
		require.NoError(t, err, "Merkle tree path verification should not error")
		assert.True(t, verificationResult.PathValid, "Merkle tree path should be valid")
		assert.True(t, verificationResult.PathIncluded, "Request should be included in the Merkle tree")
		assert.True(t, verificationResult.Result, "Overall verification result should be true")

		// Verify CBOR encoding of merkle tree path
		pathBytes, err := cbor.Marshal(proof.MerkleTreePath)
		require.NoError(t, err, "Merkle tree path should be CBOR encodable")
		assert.NotEmpty(t, pathBytes, "CBOR encoded merkle path should not be empty")

		// Verify we can decode it back
		var decodedPath api.MerkleTreePath
		err = cbor.Unmarshal(pathBytes, &decodedPath)
		require.NoError(t, err, "Should be able to decode CBOR merkle path")
		assert.Equal(t, proof.MerkleTreePath.Root, decodedPath.Root, "Decoded merkle path should match original")
	}
}

// TestInclusionProofMissingRecord tests getting inclusion proof for non-existent record
func (suite *AggregatorTestSuite) TestInclusionProofMissingRecord() {
	// First, submit some commitments and wait for block processing to ensure we have blocks
	testCommitments := createTestCertificationRequests(suite.T(), 1)
	submitResponse := makeJSONRPCRequest[api.CertificationResponse](suite.T(),
		suite.serverAddr, "certification_request", "submit-setup", testCommitments[0],
	)
	suite.Equal("SUCCESS", submitResponse.Status)

	// Wait for block processing
	time.Sleep(3 * time.Second)

	// Now test non-inclusion proof for a different state ID
	stateId := ""
	for i := 0; i < 2+32; i++ {
		stateId = stateId + "00"
	}
	inclusionProof := makeJSONRPCRequest[api.GetInclusionProofResponseV2](suite.T(),
		suite.serverAddr,
		"get_inclusion_proof.v2",
		"test-request-id",
		&api.GetInclusionProofRequestV2{StateID: api.StateID(stateId)},
	)

	// Validate non-inclusion proof structure
	suite.Nil(inclusionProof.InclusionProof.CertificationData)
	suite.NotNil(inclusionProof.InclusionProof.MerkleTreePath)

	// Verify that UnicityCertificate is included in non-inclusion proof
	suite.NotNil(inclusionProof.InclusionProof.UnicityCertificate, "Non-inclusion proof should include UnicityCertificate")
	suite.NotEmpty(inclusionProof.InclusionProof.UnicityCertificate, "UnicityCertificate should not be empty")
}

func TestGetInclusionProofShardMismatch(t *testing.T) {
	shardingCfg := config.ShardingConfig{
		Mode: config.ShardingModeChild,
		Child: config.ChildConfig{
			ShardID: 4,
		},
	}
	tree := smt.NewChildSparseMerkleTree(api.SHA256, 16+256, shardingCfg.Child.ShardID)
	service := newAggregatorServiceForTest(t, shardingCfg, tree)

	invalidShardID := api.StateID(strings.Repeat("00", 33) + "01")
	_, err := service.GetInclusionProofV2(context.Background(), &api.GetInclusionProofRequestV2{StateID: invalidShardID})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "state ID validation failed")
}

func TestGetInclusionProofInvalidRequestFormat(t *testing.T) {
	shardingCfg := config.ShardingConfig{
		Mode: config.ShardingModeChild,
		Child: config.ChildConfig{
			ShardID: 4,
		},
	}
	tree := smt.NewChildSparseMerkleTree(api.SHA256, 16+256, shardingCfg.Child.ShardID)
	service := newAggregatorServiceForTest(t, shardingCfg, tree)

	_, err := service.GetInclusionProofV2(context.Background(), &api.GetInclusionProofRequestV2{StateID: api.RequestID("zz")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "state ID validation failed")
}

func TestGetInclusionProofSMTUnavailable(t *testing.T) {
	shardingCfg := config.ShardingConfig{
		Mode: config.ShardingModeChild,
		Child: config.ChildConfig{
			ShardID: 4,
		},
	}
	service := newAggregatorServiceForTest(t, shardingCfg, nil)

	validID := api.RequestID(strings.Repeat("00", 34))
	_, err := service.GetInclusionProofV2(t.Context(), &api.GetInclusionProofRequestV2{StateID: validID})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "merkle tree not initialized")
}

func TestInclusionProofInvalidPathLength(t *testing.T) {
	shardingCfg := config.ShardingConfig{
		Mode: config.ShardingModeStandalone,
	}
	tree := smt.NewSparseMerkleTree(api.SHA256, 16+256)
	service := newAggregatorServiceForTest(t, shardingCfg, tree)

	validID := createTestCertificationRequests(t, 1)[0].StateID.String()
	require.Greater(t, len(validID), 2)
	badID := api.RequestID(validID[2:])

	_, err := service.GetInclusionProofV2(context.Background(), &api.GetInclusionProofRequestV2{StateID: badID})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "path length")
}

// TestInclusionProof tests the complete inclusion proof workflow
func (suite *AggregatorTestSuite) TestInclusionProof() {
	// 1) Send commitments
	testRequests := createTestCertificationRequests(suite.T(), 3)
	var submittedStateIDs []string

	for i, req := range testRequests {
		submitResponse := makeJSONRPCRequest[api.CertificationResponse](
			suite.T(), suite.serverAddr, "certification_request", fmt.Sprintf("submit-%d", i), req)

		suite.Equal("SUCCESS", submitResponse.Status, "Should return SUCCESS status")
		submittedStateIDs = append(submittedStateIDs, req.StateID.String())
	}

	// Wait for block processing
	time.Sleep(3 * time.Second)

	// 2) Verify inclusion proofs and store root hashes
	firstBatchRootHashes := make(map[string]string)

	for _, stateID := range submittedStateIDs {
		proofRequest := &api.GetInclusionProofRequestV2{StateID: api.StateID(stateID)}
		proofResponse := makeJSONRPCRequest[api.GetInclusionProofResponseV2](
			suite.T(), suite.serverAddr, "get_inclusion_proof.v2", "get-proof", proofRequest)

		// Validate inclusion proof structure and encoding
		validateInclusionProof(suite.T(), proofResponse.InclusionProof, api.StateID(stateID))

		// Store root hash for later stability check
		suite.Require().NotNil(proofResponse.InclusionProof.MerkleTreePath)
		rootHash := proofResponse.InclusionProof.MerkleTreePath.Root
		firstBatchRootHashes[stateID] = rootHash
	}

	// 3) Send more requests
	additionalRequests := createTestCertificationRequests(suite.T(), 2)

	for i, req := range additionalRequests {
		makeJSONRPCRequest[api.CertificationResponse](
			suite.T(), suite.serverAddr, "certification_request", fmt.Sprintf("additional-submit-%d", i), req)
	}

	// Wait for new block processing
	time.Sleep(3 * time.Second)

	// 4) Verify original commitments now reference current root hash
	for _, stateID := range submittedStateIDs {
		proofRequest := &api.GetInclusionProofRequestV2{StateID: api.StateID(stateID)}
		proofResponse := makeJSONRPCRequest[api.GetInclusionProofResponseV2](
			suite.T(), suite.serverAddr, "get_inclusion_proof.v2", "stability-check", proofRequest)

		suite.Require().NotNil(proofResponse.InclusionProof.MerkleTreePath)
		currentRootHash := proofResponse.InclusionProof.MerkleTreePath.Root
		originalRootHash, exists := firstBatchRootHashes[stateID]

		suite.True(exists, "Should have stored original root hash for %s", stateID)
		// With on-demand proof generation, the root hash should now be different (current SMT state)
		suite.NotEqual(originalRootHash, currentRootHash,
			"Root hash in inclusion proof should be current (not original) for StateID %s. Original: %s, Current: %s",
			stateID, originalRootHash, currentRootHash)

		// Validate that the proof is still valid for the commitment
		validateInclusionProof(suite.T(), proofResponse.InclusionProof, api.StateID(stateID))
	}
}

// TestAggregatorTestSuite runs the test suite
func TestAggregatorTestSuite(t *testing.T) {
	suite.Run(t, new(AggregatorTestSuite))
}

func createTestCertificationRequests(t *testing.T, count int) []*api.CertificationRequest {
	requests := make([]*api.CertificationRequest, count)

	for i := 0; i < count; i++ {
		privateKey, err := btcec.NewPrivateKey()
		require.NoError(t, err, "Failed to generate private key")
		publicKeyBytes := privateKey.PubKey().SerializeCompressed()
		ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)

		stateData := make([]byte, 32)
		for j := range stateData {
			stateData[j] = byte(i*32 + j + 1)
		}
		sourceStateHashImprint := signing.CreateDataHashImprint(stateData)

		stateID, err := api.CreateStateID(ownerPredicate, sourceStateHashImprint)
		require.NoError(t, err, "Failed to create state ID")

		transactionData := make([]byte, 32)
		for j := range transactionData {
			transactionData[j] = byte(i*64 + j + 1)
		}
		transactionHashImprint := signing.CreateDataHashImprint(transactionData)

		// Sign the transaction
		signingService := signing.NewSigningService()
		certData := &api.CertificationData{
			OwnerPredicate:  ownerPredicate,
			SourceStateHash: sourceStateHashImprint,
			TransactionHash: transactionHashImprint,
		}
		require.NoError(t, signingService.SignCertData(certData, privateKey.Serialize()))

		requests[i] = &api.CertificationRequest{
			StateID:           stateID,
			CertificationData: *certData,
			Receipt:           false,
		}
	}

	return requests
}

type stubRoundManager struct {
	smt *smt.ThreadSafeSMT
}

func (s *stubRoundManager) Start(context.Context) error    { return nil }
func (s *stubRoundManager) Stop(context.Context) error     { return nil }
func (s *stubRoundManager) Activate(context.Context) error { return nil }
func (s *stubRoundManager) Deactivate(context.Context) error {
	return nil
}
func (s *stubRoundManager) GetSMT() *smt.ThreadSafeSMT              { return s.smt }
func (s *stubRoundManager) CheckParentHealth(context.Context) error { return nil }

func newAggregatorServiceForTest(t *testing.T, shardingCfg config.ShardingConfig, baseTree *smt.SparseMerkleTree) *AggregatorService {
	t.Helper()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)

	var tsmt *smt.ThreadSafeSMT
	if baseTree != nil {
		tsmt = smt.NewThreadSafeSMT(baseTree)
	}

	return &AggregatorService{
		config:                        &config.Config{Sharding: shardingCfg},
		logger:                        log,
		roundManager:                  &stubRoundManager{smt: tsmt},
		certificationRequestValidator: signing.NewCertificationRequestValidator(shardingCfg),
	}
}
