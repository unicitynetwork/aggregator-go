package mongodb

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// setupAggregatorRecordTestDB creates a test database connection using Testcontainers
func setupAggregatorRecordTestDB(t *testing.T) *mongo.Database {
	ctx := context.Background()

	// Create MongoDB container
	mongoContainer, err := mongodb.Run(ctx, "mongo:7.0")
	if err != nil {
		t.Fatalf("Failed to start MongoDB container: %v", err)
	}

	// Get connection string
	connStr, err := mongoContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get MongoDB connection string: %v", err)
	}

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	// Test connection
	err = client.Ping(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to ping MongoDB: %v", err)
	}

	db := client.Database("test_aggregator_records")

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := client.Disconnect(ctx); err != nil {
			t.Logf("Failed to disconnect from MongoDB: %v", err)
		}

		if err := mongoContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate MongoDB container: %v", err)
		}
	})

	return db
}

// createTestAggregatorRecord creates a test aggregator record
func createTestAggregatorRecord(requestID string, blockNumber int64, leafIndex int64) *models.AggregatorRecord {
	// Create a complete commitment with all required fields
	transactionHash, _ := api.NewImprintHexString("0x00001234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	stateHash, _ := api.NewImprintHexString("0x0000abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")

	authenticator := models.Authenticator{
		Algorithm: "ed25519",
		PublicKey: api.HexBytes("test_public_key_1234567890abcdef"),
		Signature: api.HexBytes("test_signature_1234567890abcdef"),
		StateHash: stateHash,
	}

	commitment := models.NewCommitment(api.RequestID(requestID), transactionHash, authenticator)

	blockIndex := api.NewBigInt(big.NewInt(blockNumber))
	leafIdx := api.NewBigInt(big.NewInt(leafIndex))

	return models.NewAggregatorRecord(commitment, blockIndex, leafIdx)
}

func TestAggregatorRecordStorage_StoreBatch_DuplicateHandling(t *testing.T) {
	db := setupAggregatorRecordTestDB(t)

	storage := NewAggregatorRecordStorage(db)
	ctx := context.Background()

	// Create indexes first
	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	// Create test records
	records1 := []*models.AggregatorRecord{
		createTestAggregatorRecord("request1", 1, 0),
		createTestAggregatorRecord("request2", 1, 1),
		createTestAggregatorRecord("request3", 1, 2),
	}

	records2 := []*models.AggregatorRecord{
		createTestAggregatorRecord("request1", 1, 0), // Duplicate of first record
		createTestAggregatorRecord("request2", 1, 1), // Duplicate of second record
		createTestAggregatorRecord("request4", 1, 3), // New record
	}

	// Store first batch
	err = storage.StoreBatch(ctx, records1)
	require.NoError(t, err, "First StoreBatch should not return an error")

	// Store second batch with duplicates - duplicates are ignored
	// With SetOrdered(false), non-duplicate inserts still happen
	err = storage.StoreBatch(ctx, records2)
	require.NoError(t, err, "StoreBatch should ignore duplicate key errors")

	// With SetOrdered(false), request4 was still inserted despite duplicates
	count, err := storage.Count(ctx)
	require.NoError(t, err, "Count should not return an error")
	assert.Equal(t, int64(4), count, "Should have 4 records (3 original + 1 new, duplicates failed)")

	// Test GetExistingRequestIDs to filter duplicates before inserting
	requestIDs := []string{"request1", "request2", "request4", "request5"}
	existing, err := storage.GetExistingRequestIDs(ctx, requestIDs)
	require.NoError(t, err, "GetExistingRequestIDs should not return an error")
	assert.True(t, existing["request1"], "request1 should exist")
	assert.True(t, existing["request2"], "request2 should exist")
	assert.True(t, existing["request4"], "request4 should exist now")
	assert.False(t, existing["request5"], "request5 should not exist")

	// Insert only new records (after filtering with GetExistingRequestIDs)
	newRecords := []*models.AggregatorRecord{
		createTestAggregatorRecord("request5", 1, 4),
	}
	err = storage.StoreBatch(ctx, newRecords)
	require.NoError(t, err, "StoreBatch with only new records should succeed")

	// Verify all 5 records now exist
	count, err = storage.Count(ctx)
	require.NoError(t, err, "Count should not return an error")
	assert.Equal(t, int64(5), count, "Should have exactly 5 records now")
}

func TestAggregatorRecordStorage_GetByBlockNumber(t *testing.T) {
	db := setupAggregatorRecordTestDB(t)
	storage := NewAggregatorRecordStorage(db)
	ctx := context.Background()

	// Create indexes first
	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	t.Run("should return empty slice when no records exist", func(t *testing.T) {
		blockNum := api.NewBigInt(big.NewInt(100))
		retrieved, err := storage.GetByBlockNumber(ctx, blockNum)
		require.NoError(t, err)
		require.Len(t, retrieved, 0)
	})

	// Store some test records
	records := []*models.AggregatorRecord{
		createTestAggregatorRecord("req1-b100", 100, 0),
		createTestAggregatorRecord("req2-b100", 100, 1),
		createTestAggregatorRecord("req3-b100", 100, 2),
		createTestAggregatorRecord("req1-b101", 101, 0),
		createTestAggregatorRecord("req2-b101", 101, 1),
		createTestAggregatorRecord("req1-b0", 0, 0),
	}
	err = storage.StoreBatch(ctx, records)
	require.NoError(t, err, "StoreBatch should not return an error")

	largeBlockNumberRecord := createTestAggregatorRecord("req1-large", 99999999999999999, 0)
	err = storage.Store(ctx, largeBlockNumberRecord)
	require.NoError(t, err, "Store should not return an error for large block number")

	t.Run("should return records for a specific block number", func(t *testing.T) {
		blockNum := api.NewBigInt(big.NewInt(100))
		retrieved, err := storage.GetByBlockNumber(ctx, blockNum)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Len(t, retrieved, 3)

		// Check request IDs to be sure
		requestIDs := make(map[api.RequestID]bool)
		for _, r := range retrieved {
			requestIDs[r.RequestID] = true
		}
		require.True(t, requestIDs["req1-b100"])
		require.True(t, requestIDs["req2-b100"])
		require.True(t, requestIDs["req3-b100"])
	})

	t.Run("should return empty slice for non-existent block number", func(t *testing.T) {
		blockNum := api.NewBigInt(big.NewInt(999))
		retrieved, err := storage.GetByBlockNumber(ctx, blockNum)
		require.NoError(t, err)
		require.Len(t, retrieved, 0)
	})

	t.Run("should handle zero block number", func(t *testing.T) {
		blockNum := api.NewBigInt(big.NewInt(0))
		retrieved, err := storage.GetByBlockNumber(ctx, blockNum)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Len(t, retrieved, 1)
		require.Equal(t, api.RequestID("req1-b0"), retrieved[0].RequestID)
	})
}
