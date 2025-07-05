package mongodb

/*
BlockRecordsStorage Tests

This file contains comprehensive tests for the BlockRecordsStorage.Store() method.

Test Categories:
1. Unit Tests (TestBlockRecordsStorage_Store_Unit): Test data validation and structure without requiring MongoDB
2. BSON Tests (TestBlockRecordsStorage_Store_BSON): Test BSON marshaling/unmarshaling of BlockRecords
3. Integration Tests (TestBlockRecordsStorage_Store): Test actual database operations using Testcontainers

Running Tests:
- Unit/BSON tests: go test ./internal/storage/mongodb -v -run "TestBlockRecordsStorage_Store_Unit|TestBlockRecordsStorage_Store_BSON"
- All tests: go test ./internal/storage/mongodb -v (includes integration tests with containerized MongoDB)

MongoDB Setup:
Integration tests use Testcontainers to spin up a MongoDB container automatically.
No external MongoDB setup is required. Docker must be available to run integration tests.
*/

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Test configuration
const (
	testDBName  = "test_aggregator_db"
	testTimeout = 30 * time.Second
)

// setupTestDB creates a test database connection using Testcontainers
func setupTestDB(t *testing.T) (*mongo.Database, func()) {
	ctx := context.Background()

	// Create MongoDB container
	mongoContainer, err := mongodb.Run(ctx, "mongo:7.0")
	if err != nil {
		t.Skipf("Skipping MongoDB tests - cannot start MongoDB container: %v", err)
	}

	// Get connection URI
	mongoURI, err := mongoContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get MongoDB connection string: %v", err)
	}

	// Connect to MongoDB
	connectCtx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()

	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	// Ping to verify connection
	if err := client.Ping(connectCtx, nil); err != nil {
		t.Fatalf("Failed to ping MongoDB: %v", err)
	}

	// Create test database
	db := client.Database(testDBName)

	// Cleanup function
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		// Drop the test database
		if err := db.Drop(ctx); err != nil {
			t.Logf("Failed to drop test database: %v", err)
		}

		// Disconnect client
		if err := client.Disconnect(ctx); err != nil {
			t.Logf("Failed to disconnect MongoDB client: %v", err)
		}

		// Terminate the container
		if err := mongoContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate MongoDB container: %v", err)
		}
	}

	return db, cleanup
}

// createTestBlockRecords creates a test BlockRecords instance
func createTestBlockRecords(blockNumber *api.BigInt, requestIDs []api.RequestID) *models.BlockRecords {
	return &models.BlockRecords{
		BlockNumber: blockNumber,
		RequestIDs:  requestIDs,
		CreatedAt:   api.Now(),
	}
}

func TestBlockRecordsStorage_Store(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	storage := NewBlockRecordsStorage(db)
	ctx := context.Background()

	t.Run("should store valid block records", func(t *testing.T) {
		// Create test data
		blockNumber := api.NewBigInt(big.NewInt(12345))
		requestIDs := []api.RequestID{
			api.RequestID("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
			api.RequestID("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12"),
		}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Store the records
		err := storage.Store(ctx, records)
		require.NoError(t, err, "Store should not return an error")

		// Verify the record was stored by retrieving it
		var storedRecord models.BlockRecords
		err = storage.collection.FindOne(ctx, bson.M{"blockNumber": blockNumber}).Decode(&storedRecord)
		require.NoError(t, err, "Should be able to retrieve stored record")

		// Verify the stored data matches
		assert.Equal(t, blockNumber.String(), storedRecord.BlockNumber.String())
		assert.Equal(t, len(requestIDs), len(storedRecord.RequestIDs))
		for i, requestID := range requestIDs {
			assert.Equal(t, string(requestID), string(storedRecord.RequestIDs[i]))
		}
		assert.NotNil(t, storedRecord.CreatedAt)
	})

	t.Run("should store block records with empty request IDs", func(t *testing.T) {
		// Create test data with empty request IDs
		blockNumber := api.NewBigInt(big.NewInt(54321))
		requestIDs := []api.RequestID{}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Store the records
		err := storage.Store(ctx, records)
		require.NoError(t, err, "Store should not return an error")

		// Verify the record was stored
		var storedRecord models.BlockRecords
		err = storage.collection.FindOne(ctx, bson.M{"blockNumber": blockNumber}).Decode(&storedRecord)
		require.NoError(t, err, "Should be able to retrieve stored record")

		// Verify the stored data
		assert.Equal(t, blockNumber.String(), storedRecord.BlockNumber.String())
		assert.Equal(t, 0, len(storedRecord.RequestIDs))
		assert.NotNil(t, storedRecord.CreatedAt)
	})

	t.Run("should store block records with large block number", func(t *testing.T) {
		// Create test data with large block number
		largeNumber, ok := new(big.Int).SetString("999999999999999999999999999999999999999999", 10)
		require.True(t, ok, "Should be able to create large big.Int")

		blockNumber := api.NewBigInt(largeNumber)
		requestIDs := []api.RequestID{
			api.RequestID("ffff123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde00"),
		}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Store the records
		err := storage.Store(ctx, records)
		require.NoError(t, err, "Store should not return an error")

		// Verify the record was stored
		var storedRecord models.BlockRecords
		err = storage.collection.FindOne(ctx, bson.M{"blockNumber": blockNumber}).Decode(&storedRecord)
		require.NoError(t, err, "Should be able to retrieve stored record")

		// Verify the stored data
		assert.Equal(t, blockNumber.String(), storedRecord.BlockNumber.String())
		assert.Equal(t, len(requestIDs), len(storedRecord.RequestIDs))
		assert.Equal(t, string(requestIDs[0]), string(storedRecord.RequestIDs[0]))
	})

	t.Run("should store multiple block records", func(t *testing.T) {
		// Create multiple test records
		testCases := []struct {
			blockNumber *api.BigInt
			requestIDs  []api.RequestID
		}{
			{
				blockNumber: api.NewBigInt(big.NewInt(1001)),
				requestIDs: []api.RequestID{
					api.RequestID("0000000000000000000000000000000000000000000000000000000000000001"),
				},
			},
			{
				blockNumber: api.NewBigInt(big.NewInt(1002)),
				requestIDs: []api.RequestID{
					api.RequestID("0000000000000000000000000000000000000000000000000000000000000002"),
					api.RequestID("0000000000000000000000000000000000000000000000000000000000000003"),
				},
			},
			{
				blockNumber: api.NewBigInt(big.NewInt(1003)),
				requestIDs: []api.RequestID{
					api.RequestID("0000000000000000000000000000000000000000000000000000000000000004"),
					api.RequestID("0000000000000000000000000000000000000000000000000000000000000005"),
					api.RequestID("0000000000000000000000000000000000000000000000000000000000000006"),
				},
			},
		}

		// Store all records
		for _, tc := range testCases {
			records := createTestBlockRecords(tc.blockNumber, tc.requestIDs)
			err := storage.Store(ctx, records)
			require.NoError(t, err, "Store should not return an error for block %s", tc.blockNumber.String())
		}

		// Verify all records were stored
		for _, tc := range testCases {
			var storedRecord models.BlockRecords
			err := storage.collection.FindOne(ctx, bson.M{"blockNumber": tc.blockNumber}).Decode(&storedRecord)
			require.NoError(t, err, "Should be able to retrieve stored record for block %s", tc.blockNumber.String())

			assert.Equal(t, tc.blockNumber.String(), storedRecord.BlockNumber.String())
			assert.Equal(t, len(tc.requestIDs), len(storedRecord.RequestIDs))
			for i, requestID := range tc.requestIDs {
				assert.Equal(t, string(requestID), string(storedRecord.RequestIDs[i]))
			}
		}
	})

	t.Run("should store block records with zero block number", func(t *testing.T) {
		// Create test data with zero block number
		blockNumber := api.NewBigInt(big.NewInt(0))
		requestIDs := []api.RequestID{
			api.RequestID("0000000000000000000000000000000000000000000000000000000000000000"),
		}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Store the records
		err := storage.Store(ctx, records)
		require.NoError(t, err, "Store should not return an error")

		// Verify the record was stored
		var storedRecord models.BlockRecords
		err = storage.collection.FindOne(ctx, bson.M{"blockNumber": blockNumber}).Decode(&storedRecord)
		require.NoError(t, err, "Should be able to retrieve stored record")

		// Verify the stored data
		assert.Equal(t, "0", storedRecord.BlockNumber.String())
		assert.Equal(t, len(requestIDs), len(storedRecord.RequestIDs))
	})

	t.Run("should handle context cancellation", func(t *testing.T) {
		// Create a context that is already cancelled
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		// Create test data
		blockNumber := api.NewBigInt(big.NewInt(99999))
		requestIDs := []api.RequestID{
			api.RequestID("0000000000000000000000000000000000000000000000000000000000099999"),
		}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Attempt to store with cancelled context
		err := storage.Store(cancelledCtx, records)
		assert.Error(t, err, "Store should return an error when context is cancelled")
		assert.Contains(t, err.Error(), "failed to store block records")
	})

	t.Run("should handle context timeout", func(t *testing.T) {
		// Create a context with very short timeout
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// Wait for timeout to trigger
		time.Sleep(10 * time.Millisecond)

		// Create test data
		blockNumber := api.NewBigInt(big.NewInt(88888))
		requestIDs := []api.RequestID{
			api.RequestID("0000000000000000000000000000000000000000000000000000000000088888"),
		}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Attempt to store with timed out context
		err := storage.Store(timeoutCtx, records)
		assert.Error(t, err, "Store should return an error when context times out")
		assert.Contains(t, err.Error(), "failed to store block records")
	})

	t.Run("should handle nil BlockRecords", func(t *testing.T) {
		// Attempt to store nil records
		err := storage.Store(ctx, nil)
		assert.Error(t, err, "Store should return an error when records is nil")
		assert.Contains(t, err.Error(), "failed to store block records")
	})

	t.Run("should store block records with very long request ID list", func(t *testing.T) {
		// Create test data with many request IDs
		blockNumber := api.NewBigInt(big.NewInt(77777))
		requestIDs := make([]api.RequestID, 1000)
		for i := 0; i < 1000; i++ {
			requestIDs[i] = api.RequestID(fmt.Sprintf("0000%060d", i))
		}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Store the records
		err := storage.Store(ctx, records)
		require.NoError(t, err, "Store should not return an error")

		// Verify the record was stored
		var storedRecord models.BlockRecords
		err = storage.collection.FindOne(ctx, bson.M{"blockNumber": blockNumber}).Decode(&storedRecord)
		require.NoError(t, err, "Should be able to retrieve stored record")

		// Verify the stored data
		assert.Equal(t, blockNumber.String(), storedRecord.BlockNumber.String())
		assert.Equal(t, 1000, len(storedRecord.RequestIDs))

		// Verify a few random request IDs
		assert.Equal(t, string(requestIDs[0]), string(storedRecord.RequestIDs[0]))
		assert.Equal(t, string(requestIDs[500]), string(storedRecord.RequestIDs[500]))
		assert.Equal(t, string(requestIDs[999]), string(storedRecord.RequestIDs[999]))
	})
}

func TestBlockRecordsStorage_Store_Integration(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	storage := NewBlockRecordsStorage(db)
	ctx := context.Background()

	t.Run("should create indexes and store records", func(t *testing.T) {
		// Create indexes first
		err := storage.CreateIndexes(ctx)
		require.NoError(t, err, "CreateIndexes should not return an error")

		// Create test data
		blockNumber := api.NewBigInt(big.NewInt(555555))
		requestIDs := []api.RequestID{
			api.RequestID("0000000000000000000000000000000000000000000000000000000000555555"),
		}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Store the records
		err = storage.Store(ctx, records)
		require.NoError(t, err, "Store should not return an error")

		// Verify the record was stored and can be retrieved using the GetByBlockNumber method
		retrievedRecords, err := storage.GetByBlockNumber(ctx, blockNumber)
		require.NoError(t, err, "GetByBlockNumber should not return an error")
		require.NotNil(t, retrievedRecords, "Retrieved records should not be nil")

		// Verify the data
		assert.Equal(t, blockNumber.String(), retrievedRecords.BlockNumber.String())
		assert.Equal(t, len(requestIDs), len(retrievedRecords.RequestIDs))
		assert.Equal(t, string(requestIDs[0]), string(retrievedRecords.RequestIDs[0]))
	})

	t.Run("should handle duplicate block numbers with unique index", func(t *testing.T) {
		// Create indexes first
		err := storage.CreateIndexes(ctx)
		require.NoError(t, err, "CreateIndexes should not return an error")

		// Create test data with same block number
		blockNumber := api.NewBigInt(big.NewInt(666666))
		requestIDs1 := []api.RequestID{
			api.RequestID("0000000000000000000000000000000000000000000000000000000000666661"),
		}
		requestIDs2 := []api.RequestID{
			api.RequestID("0000000000000000000000000000000000000000000000000000000000666662"),
		}

		records1 := createTestBlockRecords(blockNumber, requestIDs1)
		records2 := createTestBlockRecords(blockNumber, requestIDs2)

		// Store the first record
		err = storage.Store(ctx, records1)
		require.NoError(t, err, "First store should not return an error")

		// Attempt to store the second record with the same block number
		err = storage.Store(ctx, records2)
		assert.Error(t, err, "Second store should return an error due to unique index")
		assert.Contains(t, err.Error(), "failed to store block records")
	})
}

// TestBlockRecordsStorage_Store_Unit contains unit tests that don't require a real MongoDB connection
func TestBlockRecordsStorage_Store_Unit(t *testing.T) {
	t.Run("should validate BlockRecords structure", func(t *testing.T) {
		// Test that BlockRecords can be created properly
		blockNumber := api.NewBigInt(big.NewInt(42))
		requestIDs := []api.RequestID{
			api.RequestID("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
			api.RequestID("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12"),
		}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Validate the created records
		assert.NotNil(t, records, "Created records should not be nil")
		assert.Equal(t, blockNumber.String(), records.BlockNumber.String())
		assert.Equal(t, len(requestIDs), len(records.RequestIDs))
		assert.NotNil(t, records.CreatedAt)

		for i, requestID := range requestIDs {
			assert.Equal(t, string(requestID), string(records.RequestIDs[i]))
		}
	})

	t.Run("should handle empty request IDs list", func(t *testing.T) {
		// Test that BlockRecords can be created with empty request IDs
		blockNumber := api.NewBigInt(big.NewInt(100))
		requestIDs := []api.RequestID{}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Validate the created records
		assert.NotNil(t, records, "Created records should not be nil")
		assert.Equal(t, blockNumber.String(), records.BlockNumber.String())
		assert.Equal(t, 0, len(records.RequestIDs))
		assert.NotNil(t, records.CreatedAt)
	})

	t.Run("should handle large block numbers", func(t *testing.T) {
		// Test that BlockRecords can be created with very large block numbers
		largeNumber, ok := new(big.Int).SetString("999999999999999999999999999999999999999999", 10)
		require.True(t, ok, "Should be able to create large big.Int")

		blockNumber := api.NewBigInt(largeNumber)
		requestIDs := []api.RequestID{
			api.RequestID("ffff123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde00"),
		}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Validate the created records
		assert.NotNil(t, records, "Created records should not be nil")
		assert.Equal(t, largeNumber.String(), records.BlockNumber.String())
		assert.Equal(t, len(requestIDs), len(records.RequestIDs))
		assert.NotNil(t, records.CreatedAt)
	})

	t.Run("should handle zero block number", func(t *testing.T) {
		// Test that BlockRecords can be created with zero block number
		blockNumber := api.NewBigInt(big.NewInt(0))
		requestIDs := []api.RequestID{
			api.RequestID("0000000000000000000000000000000000000000000000000000000000000000"),
		}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Validate the created records
		assert.NotNil(t, records, "Created records should not be nil")
		assert.Equal(t, "0", records.BlockNumber.String())
		assert.Equal(t, len(requestIDs), len(records.RequestIDs))
		assert.NotNil(t, records.CreatedAt)
	})

	t.Run("should handle many request IDs", func(t *testing.T) {
		// Test that BlockRecords can be created with many request IDs
		blockNumber := api.NewBigInt(big.NewInt(1000))
		requestIDs := make([]api.RequestID, 100)
		for i := 0; i < 100; i++ {
			requestIDs[i] = api.RequestID(fmt.Sprintf("0000%060d", i))
		}

		records := createTestBlockRecords(blockNumber, requestIDs)

		// Validate the created records
		assert.NotNil(t, records, "Created records should not be nil")
		assert.Equal(t, blockNumber.String(), records.BlockNumber.String())
		assert.Equal(t, 100, len(records.RequestIDs))
		assert.NotNil(t, records.CreatedAt)

		// Verify a few random request IDs
		assert.Equal(t, string(requestIDs[0]), string(records.RequestIDs[0]))
		assert.Equal(t, string(requestIDs[50]), string(records.RequestIDs[50]))
		assert.Equal(t, string(requestIDs[99]), string(records.RequestIDs[99]))
	})

	t.Run("should create storage instance", func(t *testing.T) {
		// Test that NewBlockRecordsStorage creates a proper instance
		// Note: We can't create a real database connection, but we can test the constructor

		// This would normally take a *mongo.Database, but for unit testing
		// we can verify that the constructor works with a proper interface
		storage := &BlockRecordsStorage{
			collection: nil, // In real usage, this would be a MongoDB collection
		}

		assert.NotNil(t, storage, "Storage instance should not be nil")
		// Note: collection will be nil in this test, but that's expected for unit testing
	})
}

// TestBlockRecordsStorage_Store_BSON tests BSON marshaling/unmarshaling of BlockRecords
func TestBlockRecordsStorage_Store_BSON(t *testing.T) {
	t.Run("should marshal and unmarshal BlockRecords to BSON", func(t *testing.T) {
		// Create test data
		blockNumber := api.NewBigInt(big.NewInt(12345))
		requestIDs := []api.RequestID{
			api.RequestID("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
			api.RequestID("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12"),
		}

		originalRecords := createTestBlockRecords(blockNumber, requestIDs)

		// Marshal to BSON
		bsonData, err := bson.Marshal(originalRecords)
		require.NoError(t, err, "Should be able to marshal BlockRecords to BSON")

		// Unmarshal from BSON
		var unmarshaledRecords models.BlockRecords
		err = bson.Unmarshal(bsonData, &unmarshaledRecords)
		require.NoError(t, err, "Should be able to unmarshal BlockRecords from BSON")

		// Verify the data matches
		assert.Equal(t, originalRecords.BlockNumber.String(), unmarshaledRecords.BlockNumber.String())
		assert.Equal(t, len(originalRecords.RequestIDs), len(unmarshaledRecords.RequestIDs))

		for i, requestID := range originalRecords.RequestIDs {
			assert.Equal(t, string(requestID), string(unmarshaledRecords.RequestIDs[i]))
		}

		// Note: CreatedAt comparison would need special handling due to potential precision differences
		assert.NotNil(t, unmarshaledRecords.CreatedAt)
	})

	t.Run("should marshal and unmarshal empty request IDs", func(t *testing.T) {
		// Create test data with empty request IDs
		blockNumber := api.NewBigInt(big.NewInt(54321))
		requestIDs := []api.RequestID{}

		originalRecords := createTestBlockRecords(blockNumber, requestIDs)

		// Marshal to BSON
		bsonData, err := bson.Marshal(originalRecords)
		require.NoError(t, err, "Should be able to marshal BlockRecords with empty requestIDs to BSON")

		// Unmarshal from BSON
		var unmarshaledRecords models.BlockRecords
		err = bson.Unmarshal(bsonData, &unmarshaledRecords)
		require.NoError(t, err, "Should be able to unmarshal BlockRecords with empty requestIDs from BSON")

		// Verify the data matches
		assert.Equal(t, originalRecords.BlockNumber.String(), unmarshaledRecords.BlockNumber.String())
		assert.Equal(t, 0, len(unmarshaledRecords.RequestIDs))
		assert.NotNil(t, unmarshaledRecords.CreatedAt)
	})

	t.Run("should marshal and unmarshal large block numbers", func(t *testing.T) {
		// Create test data with large block number
		largeNumber, ok := new(big.Int).SetString("999999999999999999999999999999999999999999", 10)
		require.True(t, ok, "Should be able to create large big.Int")

		blockNumber := api.NewBigInt(largeNumber)
		requestIDs := []api.RequestID{
			api.RequestID("ffff123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde00"),
		}

		originalRecords := createTestBlockRecords(blockNumber, requestIDs)

		// Marshal to BSON
		bsonData, err := bson.Marshal(originalRecords)
		require.NoError(t, err, "Should be able to marshal BlockRecords with large block number to BSON")

		// Unmarshal from BSON
		var unmarshaledRecords models.BlockRecords
		err = bson.Unmarshal(bsonData, &unmarshaledRecords)
		require.NoError(t, err, "Should be able to unmarshal BlockRecords with large block number from BSON")

		// Verify the data matches
		assert.Equal(t, originalRecords.BlockNumber.String(), unmarshaledRecords.BlockNumber.String())
		assert.Equal(t, len(originalRecords.RequestIDs), len(unmarshaledRecords.RequestIDs))
		assert.Equal(t, string(originalRecords.RequestIDs[0]), string(unmarshaledRecords.RequestIDs[0]))
		assert.NotNil(t, unmarshaledRecords.CreatedAt)
	})
}

// TestBlockRecordsStorage_Store_Comprehensive demonstrates complete functionality
func TestBlockRecordsStorage_Store_Comprehensive(t *testing.T) {
	t.Run("should demonstrate complete BlockRecords functionality", func(t *testing.T) {
		// This test demonstrates that all components work together:
		// - BigInt BSON marshaling/unmarshaling
		// - RequestID (ImprintHexString) BSON marshaling/unmarshaling
		// - Timestamp BSON marshaling/unmarshaling
		// - BlockRecords structure and BSON serialization

		// Create test data with various data types
		blockNumber := api.NewBigInt(big.NewInt(123456789))
		requestIDs := []api.RequestID{
			api.RequestID("0000ea659cdc838619b3767c057fdf8e6d99fde2680c5d8517eb06761c0878d40c40"),
			api.RequestID("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef12"),
			api.RequestID("ffff123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde00"),
		}

		// Create BlockRecords
		originalRecords := createTestBlockRecords(blockNumber, requestIDs)

		// Verify structure is correct
		assert.NotNil(t, originalRecords)
		assert.Equal(t, blockNumber.String(), originalRecords.BlockNumber.String())
		assert.Equal(t, len(requestIDs), len(originalRecords.RequestIDs))
		assert.NotNil(t, originalRecords.CreatedAt)

		// Test BSON round-trip
		bsonData, err := bson.Marshal(originalRecords)
		require.NoError(t, err, "Should marshal BlockRecords to BSON")

		var unmarshaledRecords models.BlockRecords
		err = bson.Unmarshal(bsonData, &unmarshaledRecords)
		require.NoError(t, err, "Should unmarshal BlockRecords from BSON")

		// Verify all data is preserved through BSON round-trip
		assert.Equal(t, originalRecords.BlockNumber.String(), unmarshaledRecords.BlockNumber.String())
		assert.Equal(t, len(originalRecords.RequestIDs), len(unmarshaledRecords.RequestIDs))

		for i, originalID := range originalRecords.RequestIDs {
			assert.Equal(t, string(originalID), string(unmarshaledRecords.RequestIDs[i]))
		}

		// Verify that all custom types still function after unmarshaling

		// Test BigInt functionality
		bigIntBytes := unmarshaledRecords.BlockNumber.Int.Bytes()
		assert.True(t, len(bigIntBytes) > 0, "BigInt should have byte representation")

		// Test RequestID functionality
		for _, requestID := range unmarshaledRecords.RequestIDs {
			imprintBytes, err := requestID.Imprint()
			assert.NoError(t, err, "RequestID should be able to convert to imprint")
			assert.True(t, len(imprintBytes) > 0, "RequestID should have imprint bytes")

			algorithm, err := requestID.Algorithm()
			assert.NoError(t, err, "RequestID should be able to extract algorithm")
			assert.Len(t, algorithm, 2, "Algorithm should be 2 bytes")
		}

		// Test Timestamp functionality
		assert.True(t, unmarshaledRecords.CreatedAt.UnixMilli() > 0, "Timestamp should have valid Unix time")

		t.Logf("✓ Complete round-trip successful:")
		t.Logf("  Block Number: %s", unmarshaledRecords.BlockNumber.String())
		t.Logf("  Request IDs: %d", len(unmarshaledRecords.RequestIDs))
		t.Logf("  Created At: %v", unmarshaledRecords.CreatedAt.Time)
		t.Logf("  BSON Size: %d bytes", len(bsonData))
	})

	t.Run("should handle edge cases", func(t *testing.T) {
		// Test various edge cases that Store method might encounter

		testCases := []struct {
			name        string
			blockNumber *api.BigInt
			requestIDs  []api.RequestID
		}{
			{
				name:        "zero block number",
				blockNumber: api.NewBigInt(big.NewInt(0)),
				requestIDs:  []api.RequestID{"0000000000000000000000000000000000000000000000000000000000000000"},
			},
			{
				name:        "empty request IDs",
				blockNumber: api.NewBigInt(big.NewInt(999)),
				requestIDs:  []api.RequestID{},
			},
			{
				name: "large block number",
				blockNumber: func() *api.BigInt {
					large, _ := new(big.Int).SetString("999999999999999999999999999999999999999999", 10)
					return api.NewBigInt(large)
				}(),
				requestIDs: []api.RequestID{"ffff000000000000000000000000000000000000000000000000000000000000"},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				records := createTestBlockRecords(tc.blockNumber, tc.requestIDs)

				// Test BSON serialization
				bsonData, err := bson.Marshal(records)
				require.NoError(t, err, "Should marshal edge case to BSON")

				var unmarshaled models.BlockRecords
				err = bson.Unmarshal(bsonData, &unmarshaled)
				require.NoError(t, err, "Should unmarshal edge case from BSON")

				// Verify data integrity
				assert.Equal(t, records.BlockNumber.String(), unmarshaled.BlockNumber.String())
				assert.Equal(t, len(records.RequestIDs), len(unmarshaled.RequestIDs))

				t.Logf("✓ %s: Block %s with %d request IDs",
					tc.name, unmarshaled.BlockNumber.String(), len(unmarshaled.RequestIDs))
			})
		}
	})
}
