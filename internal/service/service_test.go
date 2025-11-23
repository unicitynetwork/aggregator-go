package service

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	"github.com/unicitynetwork/aggregator-go/internal/sharding"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	mongodbStorage "github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
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

	// Start MongoDB container
	mongoContainer, err := mongodb.Run(ctx,
		"mongo:7.0",
		mongodb.WithUsername("admin"),
		mongodb.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Waiting for connections").WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("Failed to start MongoDB container: %v", err)
	}

	// Get MongoDB connection string
	mongoURI, err := mongoContainer.ConnectionString(ctx)
	require.NoError(t, err)

	// Get a free port to avoid conflicts
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// Set environment variables for the aggregator
	os.Setenv("MONGODB_URI", mongoURI)
	os.Setenv("MONGODB_DATABASE", "aggregator_test")
	os.Setenv("MONGODB_CONNECT_TIMEOUT", "30s")
	os.Setenv("MONGODB_SERVER_SELECTION_TIMEOUT", "30s")
	os.Setenv("MONGODB_SOCKET_TIMEOUT", "60s")
	os.Setenv("PORT", strconv.Itoa(port))
	os.Setenv("HOST", "127.0.0.1")
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("DISABLE_HIGH_AVAILABILITY", "true")
	os.Setenv("BFT_ENABLED", "false")

	// Load configuration
	cfg, err := config.Load()
	require.NoError(t, err)

	// Initialize logger
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	// Initialize storage
	mongoStorage, err := mongodbStorage.NewStorage(*cfg)
	require.NoError(t, err)

	// Use MongoDB for both commitment queue and storage
	commitmentQueue := mongoStorage.CommitmentQueue()

	// Initialize round manager
	rootAggregatorClient := sharding.NewRootAggregatorClientStub()
	roundManager, err := round.NewRoundManager(ctx, cfg, log, smt.NewSparseMerkleTree(api.SHA256, 16+256), commitmentQueue, mongoStorage, rootAggregatorClient, state.NewSyncStateTracker())
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

		// Stop MongoDB container with timeout
		if mongoContainer != nil {
			t.Logf("Stopping MongoDB container...")
			termCtx, termCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer termCancel()

			if err := mongoContainer.Terminate(termCtx); err != nil {
				t.Logf("Failed to terminate MongoDB container: %v", err)
				// Force kill if graceful termination fails
				t.Logf("Attempting to force remove container...")
			}
		}
	}
}

// Helper function to make JSON-RPC requests and unmarshal responses
func makeJSONRPCRequest[T any](t *testing.T, serverAddr, method, stateID string, params any) T {
	request, err := jsonrpc.NewRequest(method, params, stateID)
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
func validateInclusionProof(t *testing.T, proof *api.InclusionProof, stateID api.StateID) {
	assert.NotNil(t, proof.CertificationData, "Should have certification data")
	assert.NotNil(t, proof.MerkleTreePath, "Should have merkle tree path")

	// Validate unicity certificate field
	if proof.UnicityCertificate != nil && len(proof.UnicityCertificate) > 0 {
		assert.NotEmpty(t, proof.UnicityCertificate, "Unicity certificate should not be empty")
		// Verify it's valid hex-encoded data
		_, err := hex.DecodeString(string(proof.UnicityCertificate))
		assert.NoError(t, err, "Unicity certificate should be valid hex")
	}

	// Validate certification data encoding
	if proof.CertificationData != nil {
		assert.NotEmpty(t, proof.CertificationData.PublicKey, "CertificationData should have public key")
		assert.NotEmpty(t, proof.CertificationData.Signature, "CertificationData should have signature")
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
	submitResponse := makeJSONRPCRequest[api.CertificationResponse](
		suite.T(), suite.serverAddr, "certification_request", "submit-setup", testCommitments[0])
	suite.Equal("SUCCESS", submitResponse.Status)

	// Wait for block processing
	time.Sleep(3 * time.Second)

	// Now test non-inclusion proof for a different state ID
	stateId := ""
	for i := 0; i < 2+32; i++ {
		stateId = stateId + "00"
	}
	inclusionProof := makeJSONRPCRequest[api.GetInclusionProofResponse](
		suite.T(), suite.serverAddr, "get_inclusion_proof", "test-request-id",
		&api.GetInclusionProofRequest{StateID: api.StateID(stateId)})

	// Validate non-inclusion proof structure
	suite.Nil(inclusionProof.InclusionProof.CertificationData)
	suite.NotNil(inclusionProof.InclusionProof.MerkleTreePath)

	// Verify that UnicityCertificate is included in non-inclusion proof
	suite.NotNil(inclusionProof.InclusionProof.UnicityCertificate, "Non-inclusion proof should include UnicityCertificate")
	suite.NotEmpty(inclusionProof.InclusionProof.UnicityCertificate, "UnicityCertificate should not be empty")
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
		proofRequest := &api.GetInclusionProofRequest{StateID: api.StateID(stateID)}
		proofResponse := makeJSONRPCRequest[api.GetInclusionProofResponse](
			suite.T(), suite.serverAddr, "get_inclusion_proof", "get-proof", proofRequest)

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
		proofRequest := &api.GetInclusionProofRequest{StateID: api.StateID(stateID)}
		proofResponse := makeJSONRPCRequest[api.GetInclusionProofResponse](
			suite.T(), suite.serverAddr, "get_inclusion_proof", "stability-check", proofRequest)

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

		stateData := make([]byte, 32)
		for j := range stateData {
			stateData[j] = byte(i*32 + j + 1)
		}
		sourceStateHashImprint := signing.CreateDataHashImprint(stateData)

		stateID, err := api.CreateStateID(sourceStateHashImprint, publicKeyBytes)
		require.NoError(t, err, "Failed to create state ID")

		transactionData := make([]byte, 32)
		for j := range transactionData {
			transactionData[j] = byte(i*64 + j + 1)
		}
		transactionHashImprint := signing.CreateDataHashImprint(transactionData)

		// Sign the transaction
		signingService := signing.NewSigningService()
		certData := &api.CertificationData{
			PublicKey:       publicKeyBytes,
			SourceStateHash: sourceStateHashImprint,
			TransactionHash: transactionHashImprint,
		}
		require.NoError(t, signingService.SignCertData(certData, privateKey.Serialize()))

		receipt := false

		requests[i] = &api.CertificationRequest{
			StateID:           stateID,
			CertificationData: *certData,
			Receipt:           &receipt,
		}
	}

	return requests
}
