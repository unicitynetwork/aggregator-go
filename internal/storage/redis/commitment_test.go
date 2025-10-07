package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func setupTestRedis(t *testing.T) (*CommitmentStorage, func()) {
	ctx := context.Background()

	// Start Redis container with disk persistence enabled
	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
		Cmd: []string{
			"redis-server",
			"--save", "1", "1", // Save to disk every 1 second if at least 1 key changed
			"--appendonly", "yes", // Enable AOF for additional durability
			"--appendfsync", "everysec", // Fsync AOF every second
		},
	}

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("Could not start Redis container: %v", err)
	}

	// Get container host and port
	host, err := redisContainer.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := redisContainer.MappedPort(ctx, "6379")
	require.NoError(t, err)

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", host, mappedPort.Port()),
		Password:     "",
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
		MaxRetries:   3,
	})

	// Test connection
	err = client.Ping(ctx).Err()
	require.NoError(t, err, "Failed to connect to Redis container")

	// Clean database
	err = client.FlushDB(ctx).Err()
	require.NoError(t, err)

	storage := NewCommitmentStorage(client, "test-server")
	require.NoError(t, storage.Initialize(ctx))

	cleanup := func() {
		storage.Close(ctx)
		client.Close()
		redisContainer.Terminate(ctx)
	}

	return storage, cleanup
}

func TestCommitmentStorage_BasicOperations(t *testing.T) {
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Test Store
	commitment := createTestCommitment()
	err := storage.Store(ctx, commitment)
	require.NoError(t, err)

	// Test GetByRequestID
	retrieved, err := storage.GetByRequestID(ctx, commitment.RequestID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, commitment.RequestID, retrieved.RequestID)
	assert.Equal(t, commitment.Authenticator.Algorithm, retrieved.Authenticator.Algorithm)

	// Test Count
	count, err := storage.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestCommitmentStorage_GetUnprocessedBatch(t *testing.T) {
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Store multiple commitments
	commitments := []*models.Commitment{
		createTestCommitment(),
		createTestCommitment(),
		createTestCommitment(),
	}

	for _, c := range commitments {
		err := storage.Store(ctx, c)
		require.NoError(t, err)
	}

	// Test GetUnprocessedBatch
	retrieved, err := storage.GetUnprocessedBatch(ctx, 2)
	require.NoError(t, err)
	assert.Len(t, retrieved, 2)

	// Test GetUnprocessedBatchWithCursor
	retrievedWithCursor, cursor, err := storage.GetUnprocessedBatchWithCursor(ctx, "", 2)
	require.NoError(t, err)
	assert.Len(t, retrievedWithCursor, 2)
	assert.NotEmpty(t, cursor)

	// Get next batch with cursor
	nextBatch, newCursor, err := storage.GetUnprocessedBatchWithCursor(ctx, cursor, 2)
	require.NoError(t, err)
	assert.Len(t, nextBatch, 1) // Should get the remaining commitment
	assert.NotEmpty(t, newCursor)
}

func TestCommitmentStorage_MarkProcessed(t *testing.T) {
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Store commitments
	commitments := []*models.Commitment{
		createTestCommitment(),
		createTestCommitment(),
		createTestCommitment(),
	}

	for _, c := range commitments {
		err := storage.Store(ctx, c)
		require.NoError(t, err)
	}

	// Get unprocessed batch (this moves them to pending state)
	retrieved, err := storage.GetUnprocessedBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, retrieved, 3)

	// Mark first two as processed
	requestIDs := []api.RequestID{
		retrieved[0].RequestID,
		retrieved[1].RequestID,
	}

	err = storage.MarkProcessed(ctx, requestIDs)
	require.NoError(t, err)

	// Count unprocessed should now be 1 (the third one is still pending)
	unprocessedCount, err := storage.CountUnprocessed(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), unprocessedCount)
}

func TestCommitmentStorage_CursorConsistency(t *testing.T) {
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Store a bunch of commitments
	numCommitments := 10
	for i := 0; i < numCommitments; i++ {
		commitment := createTestCommitment()
		err := storage.Store(ctx, commitment)
		require.NoError(t, err)
	}

	// Fetch in batches using cursor
	var allRetrieved []*models.Commitment
	cursor := ""
	batchSize := 3

	for {
		batch, newCursor, err := storage.GetUnprocessedBatchWithCursor(ctx, cursor, batchSize)
		require.NoError(t, err)

		if len(batch) == 0 {
			break
		}

		allRetrieved = append(allRetrieved, batch...)
		cursor = newCursor
	}

	// Should have retrieved all commitments without duplicates
	assert.Len(t, allRetrieved, numCommitments)

	// Check for duplicates
	seen := make(map[string]bool)
	for _, c := range allRetrieved {
		requestID := string(c.RequestID)
		assert.False(t, seen[requestID], "Duplicate commitment found: %s", requestID)
		seen[requestID] = true
	}
}

func TestCommitmentStorage_EmptyResults(t *testing.T) {
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Test empty database
	commitments, err := storage.GetUnprocessedBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, commitments, 0)

	// Test cursor-based on empty database
	commitmentsWithCursor, cursor, err := storage.GetUnprocessedBatchWithCursor(ctx, "", 10)
	require.NoError(t, err)
	assert.Len(t, commitmentsWithCursor, 0)
	assert.Equal(t, "", cursor)

	// Test count on empty database
	count, err := storage.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	unprocessedCount, err := storage.CountUnprocessed(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), unprocessedCount)
}

func TestCommitmentStorage_GetByRequestID_NotFound(t *testing.T) {
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Test non-existent request ID
	commitment, err := storage.GetByRequestID(ctx, api.RequestID("non-existent"))
	require.NoError(t, err)
	assert.Nil(t, commitment)
}

func TestCommitmentStorage_GetUnprocessedBatch_NoDuplicates(t *testing.T) {
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Store multiple commitments
	commitments := []*models.Commitment{
		createTestCommitment(),
		createTestCommitment(),
		createTestCommitment(),
		createTestCommitment(),
		createTestCommitment(),
	}

	for _, c := range commitments {
		err := storage.Store(ctx, c)
		require.NoError(t, err)
	}

	// First call to GetUnprocessedBatch - should get some commitments and move them to pending
	firstBatch, err := storage.GetUnprocessedBatch(ctx, 3)
	require.NoError(t, err)
	assert.Len(t, firstBatch, 3)

	// Second call to GetUnprocessedBatch - should get remaining commitments, not duplicates
	secondBatch, err := storage.GetUnprocessedBatch(ctx, 3)
	require.NoError(t, err)
	assert.Len(t, secondBatch, 2) // Should get remaining 2 commitments

	// Third call to GetUnprocessedBatch - should get nothing (all are now pending)
	thirdBatch, err := storage.GetUnprocessedBatch(ctx, 3)
	require.NoError(t, err)
	assert.Len(t, thirdBatch, 0)

	// Verify no duplicates across batches
	seenRequestIDs := make(map[string]bool)

	// Check first batch
	for _, c := range firstBatch {
		requestID := string(c.RequestID)
		assert.False(t, seenRequestIDs[requestID], "Duplicate request ID found: %s", requestID)
		seenRequestIDs[requestID] = true
	}

	// Check second batch
	for _, c := range secondBatch {
		requestID := string(c.RequestID)
		assert.False(t, seenRequestIDs[requestID], "Duplicate request ID found: %s", requestID)
		seenRequestIDs[requestID] = true
	}

	// Verify we got all 5 commitments total
	assert.Len(t, seenRequestIDs, 5)

	// Verify that unprocessed count is 0 (all moved to pending)
	unprocessedCount, err := storage.CountUnprocessed(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(5), unprocessedCount) // All 5 are now pending (unprocessed by consumer group definition)
}

func TestCommitmentStorage_GetUnprocessedBatchWithCursor_NoDuplicates(t *testing.T) {
	storage, cleanup := setupTestRedis(t)
	defer cleanup()

	ctx := context.Background()

	// Store multiple commitments
	commitments := []*models.Commitment{
		createTestCommitment(),
		createTestCommitment(),
		createTestCommitment(),
		createTestCommitment(),
		createTestCommitment(),
	}

	for _, c := range commitments {
		err := storage.Store(ctx, c)
		require.NoError(t, err)
	}

	// Use cursor-based reading (this should NOT consume messages)
	var allRetrieved []*models.Commitment
	cursor := ""
	batchSize := 2

	// Read in batches using cursor
	for i := 0; i < 10; i++ { // Safety limit to avoid infinite loop
		batch, newCursor, err := storage.GetUnprocessedBatchWithCursor(ctx, cursor, batchSize)
		require.NoError(t, err)

		if len(batch) == 0 {
			break
		}

		allRetrieved = append(allRetrieved, batch...)
		cursor = newCursor
	}

	// Should have retrieved all 5 commitments
	assert.Len(t, allRetrieved, 5)

	// Verify no duplicates
	seenRequestIDs := make(map[string]bool)
	for _, c := range allRetrieved {
		requestID := string(c.RequestID)
		assert.False(t, seenRequestIDs[requestID], "Duplicate request ID found: %s", requestID)
		seenRequestIDs[requestID] = true
	}

	// Since GetUnprocessedBatchWithCursor uses XREAD (not consumer groups),
	// the messages should still be available for GetUnprocessedBatch
	regularBatch, err := storage.GetUnprocessedBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, regularBatch, 5) // All 5 should still be available since cursor-based didn't consume them
}
