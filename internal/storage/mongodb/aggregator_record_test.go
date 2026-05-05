package mongodb

import (
	"context"
	"math/big"
	"strings"
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

func testStateIDHex(suffix string) string {
	if len(suffix) >= api.StateTreeKeyLengthBytes*2 {
		return suffix
	}
	return strings.Repeat("0", api.StateTreeKeyLengthBytes*2-len(suffix)) + suffix
}

// createTestAggregatorRecord creates a test aggregator record
func createTestAggregatorRecord(stateID string, blockNumber int64, leafIndex int64) *models.AggregatorRecord {
	// Create a complete certification request with all required fields
	transactionHash := api.RequireNewImprintV2("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	sourceStateHash := api.RequireNewImprintV2("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")

	certData := models.CertificationData{
		OwnerPredicate:  api.NewPayToPublicKeyPredicate([]byte("test_public_key_1234567890abcdef")),
		SourceStateHash: sourceStateHash,
		TransactionHash: transactionHash,
		Witness:         api.HexBytes("test_signature_1234567890abcdef"),
	}

	commitment := models.NewCertificationRequest(api.RequireNewImprintV2(testStateIDHex(stateID)), certData)

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
	state1 := testStateIDHex("01")
	state2 := testStateIDHex("02")
	state3 := testStateIDHex("03")
	state4 := testStateIDHex("04")
	state5 := testStateIDHex("05")
	records1 := []*models.AggregatorRecord{
		createTestAggregatorRecord(state1, 1, 0),
		createTestAggregatorRecord(state2, 1, 1),
		createTestAggregatorRecord(state3, 1, 2),
	}

	records2 := []*models.AggregatorRecord{
		createTestAggregatorRecord(state1, 1, 0), // Duplicate of first record
		createTestAggregatorRecord(state2, 1, 1), // Duplicate of second record
		createTestAggregatorRecord(state4, 1, 3), // New record
	}

	// Store first batch
	err = storage.StoreBatch(ctx, records1)
	require.NoError(t, err, "First StoreBatch should not return an error")

	// Store second batch with duplicates - duplicates are ignored
	// With SetOrdered(false), non-duplicate inserts still happen
	err = storage.StoreBatch(ctx, records2)
	require.NoError(t, err, "StoreBatch should ignore duplicate key errors")

	// With SetOrdered(false), state4 was still inserted despite duplicates
	count, err := storage.Count(ctx)
	require.NoError(t, err, "Count should not return an error")
	assert.Equal(t, int64(4), count, "Should have 4 records (3 original + 1 new, duplicates failed)")

	// Test GetExistingStateIDs to filter duplicates before inserting
	stateIDs := []string{state1, state2, state4, state5}
	existing, err := storage.GetExistingStateIDs(ctx, stateIDs)
	require.NoError(t, err, "GetExistingStateIDs should not return an error")
	assert.True(t, existing[state1], "state1 should exist")
	assert.True(t, existing[state2], "state2 should exist")
	assert.True(t, existing[state4], "state4 should exist now")
	assert.False(t, existing[state5], "state5 should not exist")

	// Insert only new records (after filtering with GetExistingStateIDs)
	newRecords := []*models.AggregatorRecord{
		createTestAggregatorRecord(state5, 1, 4),
	}
	err = storage.StoreBatch(ctx, newRecords)
	require.NoError(t, err, "StoreBatch with only new records should succeed")

	// Verify all 5 records now exist
	count, err = storage.Count(ctx)
	require.NoError(t, err, "Count should not return an error")
	assert.Equal(t, int64(5), count, "Should have exactly 5 records now")
}

func TestAggregatorRecordStorage_StoreBatch_ChunkedDuplicateHandling(t *testing.T) {
	db := setupAggregatorRecordTestDB(t)

	storage := NewAggregatorRecordStorage(db, finalizationInsertOptions{
		chunkSize: 2,
		workers:   2,
	})
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	state1 := testStateIDHex("01")
	state2 := testStateIDHex("02")
	state3 := testStateIDHex("03")
	state4 := testStateIDHex("04")
	state5 := testStateIDHex("05")

	initialRecords := []*models.AggregatorRecord{
		createTestAggregatorRecord(state1, 1, 0),
		createTestAggregatorRecord(state2, 1, 1),
		createTestAggregatorRecord(state3, 1, 2),
	}
	mixedRecords := []*models.AggregatorRecord{
		createTestAggregatorRecord(state1, 1, 0),
		createTestAggregatorRecord(state4, 1, 3),
		createTestAggregatorRecord(state2, 1, 1),
		createTestAggregatorRecord(state5, 1, 4),
	}

	err = storage.StoreBatch(ctx, initialRecords)
	require.NoError(t, err, "Initial StoreBatch should not return an error")

	err = storage.StoreBatch(ctx, mixedRecords)
	require.NoError(t, err, "Chunked StoreBatch should ignore duplicate key errors")

	count, err := storage.Count(ctx)
	require.NoError(t, err, "Count should not return an error")
	assert.Equal(t, int64(5), count, "Should have exactly 5 unique records")

	record4, err := storage.GetByStateID(ctx, api.RequireNewImprintV2(state4))
	require.NoError(t, err, "GetByStateID should not return an error for state4")
	require.NotNil(t, record4, "state4 should be inserted from a chunk with a duplicate")

	record5, err := storage.GetByStateID(ctx, api.RequireNewImprintV2(state5))
	require.NoError(t, err, "GetByStateID should not return an error for state5")
	require.NotNil(t, record5, "state5 should be inserted from a chunk with a duplicate")
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
	state0101 := testStateIDHex("0101")
	state0102 := testStateIDHex("0102")
	state0103 := testStateIDHex("0103")
	records := []*models.AggregatorRecord{
		createTestAggregatorRecord(state0101, 100, 0),
		createTestAggregatorRecord(state0102, 100, 1),
		createTestAggregatorRecord(state0103, 100, 2),
		createTestAggregatorRecord("0104", 101, 0),
		createTestAggregatorRecord("0105", 101, 1),
		createTestAggregatorRecord("0006", 0, 0),
	}
	err = storage.StoreBatch(ctx, records)
	require.NoError(t, err, "StoreBatch should not return an error")

	largeBlockNumberRecord := createTestAggregatorRecord("1000", 99999999999999999, 0)
	err = storage.Store(ctx, largeBlockNumberRecord)
	require.NoError(t, err, "Store should not return an error for large block number")

	t.Run("should return records for a specific block number", func(t *testing.T) {
		blockNum := api.NewBigInt(big.NewInt(100))
		retrieved, err := storage.GetByBlockNumber(ctx, blockNum)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.Len(t, retrieved, 3)

		// Check state IDs to be sure
		stateIDs := make(map[string]bool)
		for _, r := range retrieved {
			stateIDs[r.StateID.String()] = true
		}
		require.True(t, stateIDs[state0101])
		require.True(t, stateIDs[state0102])
		require.True(t, stateIDs[state0103])
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
		require.Equal(t, api.RequireNewImprintV2(testStateIDHex("0006")), retrieved[0].StateID)
	})
}

func TestAggregatorRecordStorage_RoundTrip(t *testing.T) {
	db := setupAggregatorRecordTestDB(t)
	storage := NewAggregatorRecordStorage(db)
	ctx := t.Context()

	stateIDHex := "00004d1b938134c52340952357dd89c4c270b9b0b523bd69c03c1774fed907f1"
	record := createTestAggregatorRecord(stateIDHex, 500, 5)
	require.NoError(t, storage.Store(ctx, record))

	retrieved, err := storage.GetByStateID(ctx, api.RequireNewImprintV2(stateIDHex))
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, record.StateID, retrieved.StateID)
	require.Equal(t, record.CertificationData.SourceStateHash, retrieved.CertificationData.SourceStateHash)
}
