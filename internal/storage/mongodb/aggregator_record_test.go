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
func createTestAggregatorRecord(stateID string, blockNumber int64, leafIndex int64) *models.AggregatorRecord {
	// Create a complete certification request with all required fields
	transactionHash, _ := api.NewImprintHexString("0x00001234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	sourceStateHash, _ := api.NewImprintHexString("0x0000abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")

	certData := models.CertificationData{
		OwnerPredicate:  api.HexBytes("test_public_key_1234567890abcdef"),
		SourceStateHash: sourceStateHash,
		TransactionHash: transactionHash,
		Witness:         api.HexBytes("test_signature_1234567890abcdef"),
	}

	commitment := models.NewCertificationRequest(api.StateID(stateID), certData)

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

	// Create test records with some duplicates
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

	// Store second batch with duplicates - should not error
	err = storage.StoreBatch(ctx, records2)
	assert.NoError(t, err, "StoreBatch with duplicates should not return an error")

	// Verify that we only have 4 unique records (3 from first batch + 1 new from second)
	count, err := storage.Count(ctx)
	require.NoError(t, err, "Count should not return an error")
	assert.Equal(t, int64(4), count, "Should have exactly 4 records (duplicates ignored)")

	// Verify the new record was stored
	newRecord, err := storage.GetByStateID(ctx, "request4")
	require.NoError(t, err, "GetByStateID should not return an error")
	assert.NotNil(t, newRecord, "New record should be found")
	assert.Equal(t, api.StateID("request4"), newRecord.StateID, "New record should have correct state ID")

	// Verify duplicates were ignored (original records still exist)
	record1, err := storage.GetByStateID(ctx, "request1")
	require.NoError(t, err, "GetByStateID should not return an error")
	assert.NotNil(t, record1, "Original record should still exist")
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

		// Check state IDs to be sure
		stateIDs := make(map[api.StateID]bool)
		for _, r := range retrieved {
			stateIDs[r.StateID] = true
		}
		require.True(t, stateIDs["req1-b100"])
		require.True(t, stateIDs["req2-b100"])
		require.True(t, stateIDs["req3-b100"])
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
		require.Equal(t, api.StateID("req1-b0"), retrieved[0].StateID)
	})
}
