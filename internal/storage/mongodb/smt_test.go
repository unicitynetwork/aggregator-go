package mongodb

import (
	"context"
	"fmt"
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

// Test configuration
const (
	smtTestTimeout = 30 * time.Second
)

// setupSmtTestDB creates a test database connection using Testcontainers
func setupSmtTestDB(t *testing.T) (*mongo.Database, func()) {
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
	connectCtx, cancel := context.WithTimeout(ctx, smtTestTimeout)
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
	db := client.Database("test_smt_db")

	// Cleanup function
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), smtTestTimeout)
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

// createTestSmtNode creates a test SMT node
func createTestSmtNode(key api.HexBytes, value []byte) *models.SmtNode {
	return &models.SmtNode{
		Key:       key,
		Value:     value,
		CreatedAt: api.Now(),
	}
}

// createTestSmtNodes creates multiple test SMT nodes with truly unique keys
func createTestSmtNodes(count int) []*models.SmtNode {
	nodes := make([]*models.SmtNode, count)
	for i := 0; i < count; i++ {
		// Create unique keys and values using all bytes to avoid collisions
		key := make([]byte, 32)
		value := make([]byte, 32)

		// Use multiple bytes to ensure uniqueness for large counts
		key[28] = byte(i >> 24)
		key[29] = byte(i >> 16)
		key[30] = byte(i >> 8)
		key[31] = byte(i)

		value[28] = byte((i + 100) >> 24)
		value[29] = byte((i + 100) >> 16)
		value[30] = byte((i + 100) >> 8)
		value[31] = byte(i + 100)

		nodes[i] = createTestSmtNode(api.HexBytes(key), value)
	}
	return nodes
}

func TestSmtStorage_Store(t *testing.T) {
	db, cleanup := setupSmtTestDB(t)
	defer cleanup()

	storage := NewSmtStorage(db)
	ctx := context.Background()

	// Create indexes first
	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	t.Run("should store valid SMT node", func(t *testing.T) {
		// Create test data
		key := api.HexBytes([]byte("test_key_1234567890abcdef1234567890ab"))
		value := []byte("test_value_1234567890abcdef")

		node := createTestSmtNode(key, value)

		// Store the node
		err := storage.Store(ctx, node)
		require.NoError(t, err, "Store should not return an error")

		// Verify the node was stored by retrieving it
		storedNode, err := storage.GetByKey(ctx, key)
		require.NoError(t, err, "Should be able to retrieve stored node")
		require.NotNil(t, storedNode, "Retrieved node should not be nil")

		// Verify the stored data matches
		assert.Equal(t, key, storedNode.Key)
		assert.Equal(t, api.HexBytes(value), storedNode.Value)
		assert.NotNil(t, storedNode.CreatedAt)
	})

	t.Run("should handle duplicate key on single insert", func(t *testing.T) {
		// Create test data
		key := api.HexBytes([]byte("duplicate_key_1234567890abcdef12345678"))
		value1 := []byte("first_value_123456789")
		value2 := []byte("second_value_987654321")

		node1 := createTestSmtNode(key, value1)
		node2 := createTestSmtNode(key, value2)

		// Store the first node
		err := storage.Store(ctx, node1)
		require.NoError(t, err, "First store should not return an error")

		// Attempt to store the second node with the same key - should succeed (ignore duplicate)
		err = storage.Store(ctx, node2)
		assert.NoError(t, err, "Second store should not return an error (duplicate should be ignored)")

		// Verify only the first node remains in storage
		storedNode, err := storage.GetByKey(ctx, key)
		require.NoError(t, err, "Should be able to retrieve stored node")
		require.NotNil(t, storedNode, "Retrieved node should not be nil")

		// Should have the first node's data (not overwritten)
		assert.Equal(t, key, storedNode.Key)
		assert.Equal(t, api.HexBytes(value1), storedNode.Value)
	})

	t.Run("should store multiple different nodes", func(t *testing.T) {
		// Create multiple test nodes with different keys
		nodes := createTestSmtNodes(5)

		// Store all nodes
		for i, node := range nodes {
			err := storage.Store(ctx, node)
			require.NoError(t, err, "Store should not return an error for node %d", i)
		}

		// Verify all nodes were stored
		for i, node := range nodes {
			storedNode, err := storage.GetByKey(ctx, node.Key)
			require.NoError(t, err, "Should be able to retrieve stored node %d", i)
			require.NotNil(t, storedNode, "Retrieved node %d should not be nil", i)

			assert.Equal(t, []byte(node.Key), []byte(storedNode.Key))
			assert.Equal(t, node.Value, storedNode.Value)
		}
	})
}

func TestSmtStorage_StoreBatch(t *testing.T) {
	db, cleanup := setupSmtTestDB(t)
	defer cleanup()

	storage := NewSmtStorage(db)
	ctx := context.Background()

	// Create indexes first
	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	t.Run("should store batch of valid SMT nodes", func(t *testing.T) {
		// Create test data
		nodes := createTestSmtNodes(10)

		// Store the batch
		err := storage.StoreBatch(ctx, nodes)
		require.NoError(t, err, "StoreBatch should not return an error")

		// Verify all nodes were stored
		for i, node := range nodes {
			storedNode, err := storage.GetByKey(ctx, node.Key)
			require.NoError(t, err, "Should be able to retrieve stored node %d", i)
			require.NotNil(t, storedNode, "Retrieved node %d should not be nil", i)

			assert.Equal(t, []byte(node.Key), []byte(storedNode.Key))
			assert.Equal(t, node.Value, storedNode.Value)
		}
	})

	t.Run("should handle batch with duplicate keys", func(t *testing.T) {
		// Create first batch
		batch1 := createTestSmtNodes(5)

		// Store first batch
		err := storage.StoreBatch(ctx, batch1)
		require.NoError(t, err, "First StoreBatch should not return an error")

		// Create second batch with some duplicate keys
		batch2 := make([]*models.SmtNode, 5)

		// First 2 nodes have duplicate keys from batch1
		batch2[0] = createTestSmtNode(batch1[0].Key, []byte("new_value_1"))
		batch2[1] = createTestSmtNode(batch1[1].Key, []byte("new_value_2"))

		// Last 3 nodes have new unique keys
		for i := 2; i < 5; i++ {
			key := make([]byte, 32)
			value := make([]byte, 32)

			// Create unique keys different from batch1
			key[30] = byte(i + 100)
			key[31] = byte(i + 200)
			value[31] = byte(i + 50)

			batch2[i] = createTestSmtNode(api.HexBytes(key), value)
		}

		// Store second batch - should succeed despite duplicates
		err = storage.StoreBatch(ctx, batch2)
		assert.NoError(t, err, "Second StoreBatch should not return an error (duplicates should be ignored)")

		// Verify original nodes are unchanged (not overwritten)
		for i := 0; i < 2; i++ {
			storedNode, err := storage.GetByKey(ctx, batch1[i].Key)
			require.NoError(t, err, "Should be able to retrieve original node %d", i)
			require.NotNil(t, storedNode, "Retrieved node %d should not be nil", i)

			// Should have original values, not the new ones
			assert.Equal(t, batch1[i].Key, storedNode.Key)
			assert.Equal(t, batch1[i].Value, storedNode.Value)
		}

		// Verify new unique nodes were stored
		for i := 2; i < 5; i++ {
			storedNode, err := storage.GetByKey(ctx, batch2[i].Key)
			require.NoError(t, err, "Should be able to retrieve new node %d", i)
			require.NotNil(t, storedNode, "Retrieved new node %d should not be nil", i)

			assert.Equal(t, batch2[i].Key, storedNode.Key)
			assert.Equal(t, batch2[i].Value, storedNode.Value)
		}
	})

	t.Run("should handle batch with all duplicate keys", func(t *testing.T) {
		// Create and store first batch
		originalBatch := createTestSmtNodes(3)
		err := storage.StoreBatch(ctx, originalBatch)
		require.NoError(t, err, "Original StoreBatch should not return an error")

		// Create second batch with all duplicate keys but different values
		duplicateBatch := make([]*models.SmtNode, 3)
		for i := 0; i < 3; i++ {
			newValue := []byte(fmt.Sprintf("duplicate_value_%d", i))

			duplicateBatch[i] = createTestSmtNode(originalBatch[i].Key, newValue)
		}

		// Store duplicate batch - should succeed and ignore all duplicates
		err = storage.StoreBatch(ctx, duplicateBatch)
		assert.NoError(t, err, "Duplicate StoreBatch should not return an error")

		// Verify original data is unchanged
		for i := 0; i < 3; i++ {
			storedNode, err := storage.GetByKey(ctx, originalBatch[i].Key)
			require.NoError(t, err, "Should be able to retrieve original node %d", i)
			require.NotNil(t, storedNode, "Retrieved node %d should not be nil", i)

			// Should have original values, not the duplicate values
			assert.Equal(t, originalBatch[i].Key, storedNode.Key)
			assert.Equal(t, originalBatch[i].Value, storedNode.Value)
		}
	})

	t.Run("should handle empty batch", func(t *testing.T) {
		// Store empty batch
		err := storage.StoreBatch(ctx, []*models.SmtNode{})
		assert.NoError(t, err, "Empty StoreBatch should not return an error")

		// Store nil batch
		err = storage.StoreBatch(ctx, nil)
		assert.NoError(t, err, "Nil StoreBatch should not return an error")
	})

	t.Run("should handle large batch with duplicates", func(t *testing.T) {
		// Create large batch
		largeBatch := createTestSmtNodes(100)

		// Store first batch
		err := storage.StoreBatch(ctx, largeBatch)
		require.NoError(t, err, "Large StoreBatch should not return an error")

		// Create another batch with 50% duplicates
		mixedBatch := make([]*models.SmtNode, 100)

		// First 50 are duplicates
		for i := 0; i < 50; i++ {
			newValue := []byte(fmt.Sprintf("duplicate_large_value_%d", i))

			mixedBatch[i] = createTestSmtNode(largeBatch[i].Key, newValue)
		}

		// Last 50 are new
		for i := 50; i < 100; i++ {
			key := make([]byte, 32)
			value := make([]byte, 32)

			// Create unique keys
			key[29] = byte(i)
			key[30] = byte(i + 100)
			key[31] = byte(i + 200)
			value[31] = byte(i % 256)

			mixedBatch[i] = createTestSmtNode(api.HexBytes(key), value)
		}

		// Store mixed batch - should succeed
		err = storage.StoreBatch(ctx, mixedBatch)
		assert.NoError(t, err, "Mixed batch with duplicates should not return an error")

		// Verify original data is preserved for duplicates
		for i := 0; i < 50; i++ {
			storedNode, err := storage.GetByKey(ctx, largeBatch[i].Key)
			require.NoError(t, err, "Should be able to retrieve original node %d", i)

			// Should have original values
			assert.Equal(t, largeBatch[i].Value, storedNode.Value)
		}

		// Verify new data is stored
		for i := 50; i < 100; i++ {
			storedNode, err := storage.GetByKey(ctx, mixedBatch[i].Key)
			require.NoError(t, err, "Should be able to retrieve new node %d", i)

			// Should have new values
			assert.Equal(t, mixedBatch[i].Value, storedNode.Value)
		}
	})
}

func TestSmtStorage_Count(t *testing.T) {
	db, cleanup := setupSmtTestDB(t)
	defer cleanup()

	storage := NewSmtStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	t.Run("should count stored nodes correctly", func(t *testing.T) {
		// Initially should be 0
		count, err := storage.Count(ctx)
		require.NoError(t, err, "Count should not return an error")
		assert.Equal(t, int64(0), count, "Initial count should be 0")

		// Store some nodes
		nodes := createTestSmtNodes(5)
		err = storage.StoreBatch(ctx, nodes)
		require.NoError(t, err, "StoreBatch should not return an error")

		// Count should be 5
		count, err = storage.Count(ctx)
		require.NoError(t, err, "Count should not return an error")
		assert.Equal(t, int64(5), count, "Count should be 5 after storing 5 nodes")

		// Store duplicates
		err = storage.StoreBatch(ctx, nodes)
		require.NoError(t, err, "StoreBatch with duplicates should not return an error")

		// Count should still be 5 (duplicates ignored)
		count, err = storage.Count(ctx)
		require.NoError(t, err, "Count should not return an error")
		assert.Equal(t, int64(5), count, "Count should still be 5 after storing duplicates")
	})
}

func TestSmtStorage_GetChunked(t *testing.T) {
	db, cleanup := setupSmtTestDB(t)
	defer cleanup()

	storage := NewSmtStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	t.Run("should retrieve chunked nodes after duplicate handling", func(t *testing.T) {
		// Store initial batch
		nodes := createTestSmtNodes(10)
		err := storage.StoreBatch(ctx, nodes)
		require.NoError(t, err, "StoreBatch should not return an error")

		// Store duplicates (should be ignored)
		err = storage.StoreBatch(ctx, nodes)
		require.NoError(t, err, "StoreBatch with duplicates should not return an error")

		// Retrieve all nodes in chunks
		chunk1, err := storage.GetChunked(ctx, 0, 5)
		require.NoError(t, err, "GetChunked should not return an error")
		assert.Len(t, chunk1, 5, "First chunk should have 5 nodes")

		chunk2, err := storage.GetChunked(ctx, 5, 5)
		require.NoError(t, err, "GetChunked should not return an error")
		assert.Len(t, chunk2, 5, "Second chunk should have 5 nodes")

		// Third chunk should be empty
		chunk3, err := storage.GetChunked(ctx, 10, 5)
		require.NoError(t, err, "GetChunked should not return an error")
		assert.Len(t, chunk3, 0, "Third chunk should be empty")

		// Verify we got all unique nodes (no duplicates)
		allChunkedKeys := make(map[string]bool)
		for _, node := range append(chunk1, chunk2...) {
			keyStr := string(node.Key)
			assert.False(t, allChunkedKeys[keyStr], "Should not have duplicate keys in chunked results")
			allChunkedKeys[keyStr] = true
		}

		assert.Len(t, allChunkedKeys, 10, "Should have exactly 10 unique keys")
	})
}

func TestSmtStorage_StoreBatch_DuplicateHandling(t *testing.T) {
	db, cleanup := setupSmtTestDB(t)
	defer cleanup()

	storage := NewSmtStorage(db)
	ctx := context.Background()

	// Create indexes first
	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	// Create test nodes with some duplicates
	nodes1 := createTestSmtNodes(3)
	nodes2 := []*models.SmtNode{
		nodes1[0], // Duplicate of first node
		nodes1[1], // Duplicate of second node
		createTestSmtNode(api.HexBytes("newkey"), []byte("newvalue")), // New node
	}

	// Store first batch
	err = storage.StoreBatch(ctx, nodes1)
	require.NoError(t, err, "First StoreBatch should not return an error")

	// Store second batch with duplicates - should not error
	err = storage.StoreBatch(ctx, nodes2)
	assert.NoError(t, err, "StoreBatch with duplicates should not return an error")

	// Verify that we only have 4 unique nodes (3 from first batch + 1 new from second)
	count, err := storage.Count(ctx)
	require.NoError(t, err, "Count should not return an error")
	assert.Equal(t, int64(4), count, "Should have exactly 4 nodes (duplicates ignored)")

	// Verify the new node was stored
	newNode, err := storage.GetByKey(ctx, api.HexBytes("newkey"))
	require.NoError(t, err, "GetByKey should not return an error")
	assert.NotNil(t, newNode, "New node should be found")
	assert.Equal(t, []byte("newvalue"), []byte(newNode.Value), "New node should have correct value")
}
