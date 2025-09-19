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
func setupAggregatorRecordTestDB(t *testing.T) (*mongo.Database, func()) {
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

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := client.Disconnect(ctx); err != nil {
			t.Logf("Failed to disconnect from MongoDB: %v", err)
		}

		if err := mongoContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate MongoDB container: %v", err)
		}
	}

	return db, cleanup
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
	db, cleanup := setupAggregatorRecordTestDB(t)
	defer cleanup()

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
	newRecord, err := storage.GetByRequestID(ctx, "request4")
	require.NoError(t, err, "GetByRequestID should not return an error")
	assert.NotNil(t, newRecord, "New record should be found")
	assert.Equal(t, api.RequestID("request4"), newRecord.RequestID, "New record should have correct request ID")

	// Verify duplicates were ignored (original records still exist)
	record1, err := storage.GetByRequestID(ctx, "request1")
	require.NoError(t, err, "GetByRequestID should not return an error")
	assert.NotNil(t, record1, "Original record should still exist")
}
