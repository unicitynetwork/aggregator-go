package service

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
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
	bfttypes "github.com/unicitynetwork/bft-go-base/types"
	bfthex "github.com/unicitynetwork/bft-go-base/types/hex"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/sharding"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
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

	// Ensure cleanup happens even if setup fails
	defer func() {
		if r := recover(); r != nil {
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
	roundManager, err := round.NewRoundManager(ctx, cfg, log, commitmentQueue, mongoStorage, rootAggregatorClient, state.NewSyncStateTracker(), nil, events.NewEventBus(log), smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)), nil)
	require.NoError(t, err)

	// Start the round manager (restores SMT)
	err = roundManager.Start(ctx)
	require.NoError(t, err)

	// In non-HA test mode, we are the leader, so activate the service directly
	err = roundManager.Activate(ctx)
	require.NoError(t, err)

	// Initialize aggregator service
	aggregatorService := NewAggregatorService(cfg, log, roundManager, commitmentQueue, mongoStorage, nil, nil)

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

// validateInclusionProof validates the v2 inclusion proof wire structure
// and performs end-to-end verification against the originating
// CertificationRequest via the new InclusionCert + UC.IR.h binding.
func validateInclusionProof(t *testing.T, proof *api.InclusionProofV2, req *api.CertificationRequest) {
	assert.NotNil(t, proof.CertificationData, "Should have certification data")
	assert.NotEmpty(t, proof.CertificateBytes, "Should have inclusion cert bytes")
	assert.NotEmpty(t, proof.UnicityCertificate, "Unicity certificate should not be empty")

	assert.NotEmpty(t, proof.CertificationData.OwnerPredicate, "CertificationData should have owner predicate")
	assert.NotEmpty(t, proof.CertificationData.Witness, "CertificationData should have signature")
	assert.NotEmpty(t, proof.CertificationData.SourceStateHash, "CertificationData should have source state hash")
	assert.NotEmpty(t, proof.CertificationData.TransactionHash, "CertificationData should have transaction hash")

	// Verify CBOR round-trip of certification data.
	certDataBytes, err := cbor.Marshal(proof.CertificationData)
	require.NoError(t, err, "CertificationData should be CBOR encodable")
	assert.NotEmpty(t, certDataBytes, "CBOR encoded certification data should not be empty")
	var decodedAuth api.CertificationData
	require.NoError(t, cbor.Unmarshal(certDataBytes, &decodedAuth))

	// Wire-decode the inclusion certificate and perform full v2 verification.
	var cert api.InclusionCert
	require.NoError(t, cert.UnmarshalBinary(proof.CertificateBytes), "InclusionCert must decode")

	require.NoError(t, proof.Verify(req), "v2 inclusion proof must verify")
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

	// Now test non-inclusion proof for a raw 32-byte v2 state ID that has
	// never been submitted.
	stateId := strings.Repeat("00", api.StateTreeKeyLengthBytes)
	inclusionProof := makeJSONRPCRequest[api.GetInclusionProofResponseV2](suite.T(),
		suite.serverAddr,
		"get_inclusion_proof.v2",
		"test-request-id",
		&api.GetInclusionProofRequestV2{StateID: api.RequireNewImprintV2(stateId)},
	)

	// v2 non-inclusion: ExclusionCert wire path is not yet implemented. The
	// service returns an empty-cert payload with CertificationData == nil
	// plus a bound UnicityCertificate for round-trip.
	suite.Nil(inclusionProof.InclusionProof.CertificationData)
	suite.Empty(inclusionProof.InclusionProof.CertificateBytes)
	suite.NotEmpty(inclusionProof.InclusionProof.UnicityCertificate, "Non-inclusion proof should include UnicityCertificate")
}

func TestGetInclusionProofShardMismatch(t *testing.T) {
	shardingCfg := config.ShardingConfig{
		Mode: config.ShardingModeChild,
		Child: config.ChildConfig{
			ShardID: 4,
		},
	}
	tree := smt.NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, shardingCfg.Child.ShardID)
	service := newAggregatorServiceForTest(t, shardingCfg, tree)

	// Raw 32-byte v2 stateId whose shard-prefix bits don't match shard 4 (=0b100).
	invalidShardID := api.RequireNewImprintV2("01" + strings.Repeat("00", api.StateTreeKeyLengthBytes-1))
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
	tree := smt.NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, shardingCfg.Child.ShardID)
	service := newAggregatorServiceForTest(t, shardingCfg, tree)

	_, err := service.GetInclusionProofV2(context.Background(), &api.GetInclusionProofRequestV2{StateID: api.ImprintV2([]byte("zz"))})
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

	// Raw 32-byte v2 stateId; all-zero shard-prefix bits match shard 4 (expected = 0b00).
	validID := api.RequireNewImprintV2(strings.Repeat("00", api.StateTreeKeyLengthBytes))
	_, err := service.GetInclusionProofV2(t.Context(), &api.GetInclusionProofRequestV2{StateID: validID})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "merkle tree not initialized")
}

func TestInclusionProofInvalidPathLength(t *testing.T) {
	shardingCfg := config.ShardingConfig{
		Mode: config.ShardingModeStandalone,
	}
	tree := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	service := newAggregatorServiceForTest(t, shardingCfg, tree)

	// Drop one byte from the canonical 32-byte stateId — v2 strict enforcement
	// must reject it at length validation, before any SMT traversal.
	validID := createTestCertificationRequests(t, 1)[0].StateID
	require.Len(t, validID, api.StateTreeKeyLengthBytes)
	badID := api.ImprintV2(append([]byte(nil), validID[:len(validID)-1]...))

	_, err := service.GetInclusionProofV2(context.Background(), &api.GetInclusionProofRequestV2{StateID: badID})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid state ID length")
}

func TestGetInclusionProofV2Child_ComposesParentFragment(t *testing.T) {
	shardingCfg := config.ShardingConfig{
		Mode:          config.ShardingModeChild,
		ShardIDLength: 2,
		Child: config.ChildConfig{
			ShardID: 4, // shard prefix bits 00
		},
	}

	stateID := api.RequireNewImprintV2(strings.Repeat("00", api.StateTreeKeyLengthBytes))
	sourceStateHash := api.RequireNewImprintV2("10" + strings.Repeat("00", api.StateTreeKeyLengthBytes-1))
	transactionHash := api.RequireNewImprintV2("20" + strings.Repeat("00", api.StateTreeKeyLengthBytes-1))

	childTree := smt.NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, shardingCfg.Child.ShardID)
	path, err := stateID.GetPath()
	require.NoError(t, err)
	require.NoError(t, childTree.AddLeaf(path, transactionHash.DataBytes()))
	childRoot := childTree.GetRootHashRaw()

	parentTree := smt.NewParentSparseMerkleTree(api.SHA256, shardingCfg.ShardIDLength)
	require.NoError(t, smt.NewThreadSafeSMT(parentTree).AddPreHashedLeaf(big.NewInt(int64(shardingCfg.Child.ShardID)), childRoot))
	parentRoot := parentTree.GetRootHashRaw()
	parentFragment, err := parentTree.GetShardInclusionFragment(shardingCfg.Child.ShardID)
	require.NoError(t, err)
	require.NotNil(t, parentFragment)

	parentUC := testChildProofUC(t, 9, parentRoot)
	block := models.NewBlock(
		api.NewBigIntFromUint64(3),
		"test-chain",
		shardingCfg.Child.ShardID,
		"v",
		"f",
		api.HexBytes(childRoot),
		nil,
		parentUC,
	)
	block.ParentFragment = parentFragment
	block.ParentBlockNumber = 9
	block.Finalized = true

	record := &models.AggregatorRecord{
		Version: 2,
		StateID: stateID,
		CertificationData: models.CertificationData{
			OwnerPredicate:  api.Predicate{Engine: 1, Code: []byte{0x01}, Params: []byte{0x02}},
			SourceStateHash: sourceStateHash,
			TransactionHash: transactionHash,
			Witness:         []byte{0x01, 0x02},
		},
		BlockNumber: api.NewBigIntFromUint64(1),
		LeafIndex:   api.NewBigIntFromUint64(0),
		CreatedAt:   api.Now(),
		FinalizedAt: api.Now(),
	}

	service := newAggregatorServiceForTest(t, shardingCfg, childTree)
	service.storage = &testStorage{
		blockStorage: &testBlockStorage{latestByRoot: map[string]*models.Block{block.RootHash.String(): block}},
		recordStorage: &testAggregatorRecordStorage{
			byStateID: map[string]*models.AggregatorRecord{stateID.String(): record},
		},
	}

	resp, err := service.GetInclusionProofV2(context.Background(), &api.GetInclusionProofRequestV2{StateID: stateID})
	require.NoError(t, err)
	require.Equal(t, uint64(9), resp.BlockNumber)
	require.NotNil(t, resp.InclusionProof)
	require.Equal(t, parentUC, api.HexBytes(resp.InclusionProof.UnicityCertificate))

	req := &api.CertificationRequest{
		StateID: stateID,
		CertificationData: api.CertificationData{
			OwnerPredicate:  record.CertificationData.OwnerPredicate,
			SourceStateHash: record.CertificationData.SourceStateHash,
			TransactionHash: record.CertificationData.TransactionHash,
			Witness:         record.CertificationData.Witness,
		},
	}
	validateInclusionProof(t, resp.InclusionProof, req)
}

func TestGetInclusionProofV2Child_NonInclusionUsesParentBundleMetadata(t *testing.T) {
	shardingCfg := config.ShardingConfig{
		Mode:          config.ShardingModeChild,
		ShardIDLength: 2,
		Child: config.ChildConfig{
			ShardID: 4,
		},
	}

	childTree := smt.NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, shardingCfg.Child.ShardID)
	parentUC := testChildProofUC(t, 12, childTree.GetRootHashRaw())
	block := models.NewBlock(
		api.NewBigIntFromUint64(4),
		"test-chain",
		shardingCfg.Child.ShardID,
		"v",
		"f",
		api.HexBytes(childTree.GetRootHashRaw()),
		nil,
		parentUC,
	)
	block.ParentBlockNumber = 12
	block.Finalized = true

	service := newAggregatorServiceForTest(t, shardingCfg, childTree)
	service.storage = &testStorage{
		blockStorage:  &testBlockStorage{latestByRoot: map[string]*models.Block{block.RootHash.String(): block}},
		recordStorage: &testAggregatorRecordStorage{byStateID: map[string]*models.AggregatorRecord{}},
	}

	stateID := api.RequireNewImprintV2(strings.Repeat("00", api.StateTreeKeyLengthBytes))
	resp, err := service.GetInclusionProofV2(context.Background(), &api.GetInclusionProofRequestV2{StateID: stateID})
	require.NoError(t, err)
	require.Equal(t, uint64(12), resp.BlockNumber)
	require.Nil(t, resp.InclusionProof.CertificationData)
	require.Empty(t, resp.InclusionProof.CertificateBytes)
	require.Equal(t, parentUC, api.HexBytes(resp.InclusionProof.UnicityCertificate))
}

// TestInclusionProof tests the complete inclusion proof workflow
func (suite *AggregatorTestSuite) TestInclusionProof() {
	// 1) Send commitments
	testRequests := createTestCertificationRequests(suite.T(), 3)

	for i, req := range testRequests {
		submitResponse := makeJSONRPCRequest[api.CertificationResponse](
			suite.T(), suite.serverAddr, "certification_request", fmt.Sprintf("submit-%d", i), req)

		suite.Equal("SUCCESS", submitResponse.Status, "Should return SUCCESS status")
	}

	// Wait for block processing
	time.Sleep(3 * time.Second)

	// 2) Verify inclusion proofs and store bound UC roots for the stability
	//    check below. With the v2 wire, the authoritative root identity is
	//    UC.IR.h, sourced from the proof's UnicityCertificate.
	firstBatchRoots := make(map[string]string) // stateID => hex(UC.IR.h)

	for _, req := range testRequests {
		proofRequest := &api.GetInclusionProofRequestV2{StateID: req.StateID}
		proofResponse := makeJSONRPCRequest[api.GetInclusionProofResponseV2](
			suite.T(), suite.serverAddr, "get_inclusion_proof.v2", "get-proof", proofRequest)

		// Validate inclusion proof structure and encoding + end-to-end verify.
		validateInclusionProof(suite.T(), proofResponse.InclusionProof, req)

		rootRaw, err := proofResponse.InclusionProof.UCInputRecordHashRaw()
		suite.Require().NoError(err)
		firstBatchRoots[req.StateID.String()] = hex.EncodeToString(rootRaw)
	}

	// 3) Send more requests
	additionalRequests := createTestCertificationRequests(suite.T(), 2)

	for i, req := range additionalRequests {
		makeJSONRPCRequest[api.CertificationResponse](
			suite.T(), suite.serverAddr, "certification_request", fmt.Sprintf("additional-submit-%d", i), req)
	}

	// Wait for new block processing
	time.Sleep(3 * time.Second)

	// 4) Verify original commitments now reference current root hash (UC.IR.h).
	for _, req := range testRequests {
		proofRequest := &api.GetInclusionProofRequestV2{StateID: req.StateID}
		proofResponse := makeJSONRPCRequest[api.GetInclusionProofResponseV2](
			suite.T(), suite.serverAddr, "get_inclusion_proof.v2", "stability-check", proofRequest)

		currentRootRaw, err := proofResponse.InclusionProof.UCInputRecordHashRaw()
		suite.Require().NoError(err)
		currentRootHash := hex.EncodeToString(currentRootRaw)

		originalRootHash, exists := firstBatchRoots[req.StateID.String()]
		suite.True(exists, "Should have stored original root hash for %s", req.StateID)
		// With on-demand proof generation, the root hash should now be different (current SMT state)
		suite.NotEqual(originalRootHash, currentRootHash,
			"Root hash in inclusion proof should be current (not original) for StateID %s. Original: %s, Current: %s",
			req.StateID, originalRootHash, currentRootHash)

		// Validate that the proof is still valid for the commitment
		validateInclusionProof(suite.T(), proofResponse.InclusionProof, req)
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
		sourceStateHash := signing.CreateDataHash(stateData)

		stateID, err := api.CreateStateID(ownerPredicate, sourceStateHash)
		require.NoError(t, err, "Failed to create state ID")

		transactionData := make([]byte, 32)
		for j := range transactionData {
			transactionData[j] = byte(i*64 + j + 1)
		}
		transactionHash := signing.CreateDataHash(transactionData)

		// Sign the transaction
		signingService := signing.NewSigningService()
		certData := &api.CertificationData{
			OwnerPredicate:  ownerPredicate,
			SourceStateHash: sourceStateHash,
			TransactionHash: transactionHash,
		}
		require.NoError(t, signingService.SignCertData(certData, privateKey.Serialize()))

		requests[i] = &api.CertificationRequest{
			StateID:           stateID,
			CertificationData: *certData,
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
func (s *stubRoundManager) FinalizationReadLock() func()            { return func() {} }

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

type testStorage struct {
	blockStorage  interfaces.BlockStorage
	recordStorage interfaces.AggregatorRecordStorage
}

func (s *testStorage) AggregatorRecordStorage() interfaces.AggregatorRecordStorage {
	return s.recordStorage
}
func (s *testStorage) BlockStorage() interfaces.BlockStorage               { return s.blockStorage }
func (s *testStorage) SmtStorage() interfaces.SmtStorage                   { return nil }
func (s *testStorage) BlockRecordsStorage() interfaces.BlockRecordsStorage { return nil }
func (s *testStorage) LeadershipStorage() interfaces.LeadershipStorage     { return nil }
func (s *testStorage) TrustBaseStorage() interfaces.TrustBaseStorage       { return nil }
func (s *testStorage) Initialize(context.Context) error                    { return nil }
func (s *testStorage) Ping(context.Context) error                          { return nil }
func (s *testStorage) Close(context.Context) error                         { return nil }
func (s *testStorage) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

type testBlockStorage struct {
	latestByRoot map[string]*models.Block
}

func (s *testBlockStorage) Store(context.Context, *models.Block) error { return nil }
func (s *testBlockStorage) GetByNumber(context.Context, *api.BigInt) (*models.Block, error) {
	return nil, nil
}
func (s *testBlockStorage) GetLatest(context.Context) (*models.Block, error)     { return nil, nil }
func (s *testBlockStorage) GetLatestNumber(context.Context) (*api.BigInt, error) { return nil, nil }
func (s *testBlockStorage) Count(context.Context) (int64, error)                 { return 0, nil }
func (s *testBlockStorage) GetRange(context.Context, *api.BigInt, *api.BigInt) ([]*models.Block, error) {
	return nil, nil
}
func (s *testBlockStorage) SetFinalized(context.Context, *api.BigInt, bool) error { return nil }
func (s *testBlockStorage) GetUnfinalized(context.Context) ([]*models.Block, error) {
	return nil, nil
}
func (s *testBlockStorage) GetLatestByRootHash(ctx context.Context, rootHash api.HexBytes) (*models.Block, error) {
	if s == nil {
		return nil, nil
	}
	return s.latestByRoot[rootHash.String()], nil
}

type testAggregatorRecordStorage struct {
	byStateID map[string]*models.AggregatorRecord
}

func (s *testAggregatorRecordStorage) Store(context.Context, *models.AggregatorRecord) error {
	return nil
}
func (s *testAggregatorRecordStorage) StoreBatch(context.Context, []*models.AggregatorRecord) error {
	return nil
}
func (s *testAggregatorRecordStorage) GetByBlockNumber(context.Context, *api.BigInt) ([]*models.AggregatorRecord, error) {
	return nil, nil
}
func (s *testAggregatorRecordStorage) Count(context.Context) (int64, error) { return 0, nil }
func (s *testAggregatorRecordStorage) GetExistingRequestIDs(context.Context, []string) (map[string]bool, error) {
	return nil, nil
}
func (s *testAggregatorRecordStorage) GetByStateID(ctx context.Context, stateID api.StateID) (*models.AggregatorRecord, error) {
	if s == nil {
		return nil, nil
	}
	return s.byStateID[stateID.String()], nil
}

func testChildProofUC(t *testing.T, roundNumber uint64, rootHash []byte) api.HexBytes {
	t.Helper()
	uc := bfttypes.UnicityCertificate{
		InputRecord: &bfttypes.InputRecord{
			RoundNumber: roundNumber,
			Hash:        bfthex.Bytes(rootHash),
		},
		UnicitySeal: &bfttypes.UnicitySeal{
			RootChainRoundNumber: roundNumber,
		},
	}
	ucBytes, err := bfttypes.Cbor.Marshal(uc)
	require.NoError(t, err)
	return api.NewHexBytes(ucBytes)
}
