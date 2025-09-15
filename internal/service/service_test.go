package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/round"
	mongodbStorage "github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

func setupMongoDBAndAggregator(t *testing.T, ctx context.Context) (string, func()) {
	// Start MongoDB container
	mongoContainer, err := mongodb.Run(ctx,
		"mongo:7.0",
		mongodb.WithUsername("admin"),
		mongodb.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Waiting for connections").WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)

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

	// Initialize round manager
	roundManager, err := round.NewRoundManager(ctx, cfg, log, mongoStorage, nil)
	require.NoError(t, err)

	// Initialize service
	aggregatorService := NewAggregatorService(cfg, log, roundManager, mongoStorage, nil)

	// Start the aggregator service
	err = aggregatorService.Start(ctx)
	require.NoError(t, err)

	// Initialize gateway server
	server := gateway.NewServer(cfg, log, mongoStorage, aggregatorService)

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
		// Stop the server
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Stop(shutdownCtx)

		// Stop aggregator service
		aggregatorService.Stop(shutdownCtx)

		// Stop MongoDB container
		if err := mongoContainer.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate MongoDB container: %v", err)
		}
	}
}

func TestInclusionProofMissingRecord(t *testing.T) {
	ctx := context.Background()
	serverAddr, cleanup := setupMongoDBAndAggregator(t, ctx)
	defer cleanup()

	request, err := jsonrpc.NewRequest(
		"get_inclusion_proof",
		&api.GetInclusionProofRequest{RequestID: "00000000"},
		"test-request-id")
	require.NoError(t, err)

	bodyBytes, err := json.Marshal(request)
	require.NoError(t, err)

	httpResponse, err := http.Post(serverAddr, "application/json", bytes.NewReader(bodyBytes))
	require.NoError(t, err)
	defer httpResponse.Body.Close()

	var response jsonrpc.Response
	err = json.NewDecoder(httpResponse.Body).Decode(&response)
	require.NoError(t, err)

	resultBytes, err := json.Marshal(response.Result)
	require.NoError(t, err)

	var inclusionProof api.GetInclusionProofResponse
	err = json.Unmarshal(resultBytes, &inclusionProof)
	require.NoError(t, err)

	assert.Nil(t, inclusionProof.TransactionHash)
	assert.Nil(t, inclusionProof.Authenticator)
	assert.NotNil(t, inclusionProof.MerkleTreePath)
}
