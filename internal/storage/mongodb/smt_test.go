package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

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
func setupSmtTestDB(t *testing.T) *mongo.Database {
	ctx := context.Background()

	// Create MongoDB container
	mongoContainer, err := mongodb.Run(ctx, "mongo:7.0")
	if err != nil {
		t.Skipf("Skipping MongoDB tests - cannot start MongoDB container: %v", err)
	}

	// Get connection URI
	mongoURI, err := mongoContainer.ConnectionString(ctx)
	require.NoError(t, err)

	// Connect to MongoDB
	connectCtx, cancel := context.WithTimeout(ctx, smtTestTimeout)
	defer cancel()

	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(mongoURI))
	require.NoError(t, err)

	// Ping to verify connection
	err = client.Ping(connectCtx, nil)
	require.NoError(t, err)

	// Create test database
	db := client.Database("test_smt_db")

	// Cleanup function
	t.Cleanup(func() {
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
	})

	return db
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

		nodes[i] = models.NewSmtNode(key, value)
	}
	return nodes
}

func TestSmtStorage_Store(t *testing.T) {
	db := setupSmtTestDB(t)

	storage := NewSmtStorage(db)
	ctx := context.Background()

	// Create indexes first
	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	t.Run("should store valid SMT node", func(t *testing.T) {
		// Create test data
		key := api.HexBytes("test_key_1234567890abcdef1234567890ab")
		value := api.HexBytes("test_value_1234567890abcdef")
		node := models.NewSmtNode(key, value)

		// Store the node
		err := storage.Store(ctx, node)
		require.NoError(t, err, "Store should not return an error")

		// Verify the node was stored by retrieving it
		storedNode, err := storage.GetByKey(ctx, key)
		require.NoError(t, err, "Should be able to retrieve stored node")
		require.NotNil(t, storedNode, "Retrieved node should not be nil")

		// Verify the stored data matches
		require.Equal(t, key, storedNode.Key)
		require.Equal(t, value, storedNode.Value)
		require.NotNil(t, storedNode.CreatedAt)
	})

	t.Run("should handle duplicate key on single insert", func(t *testing.T) {
		// Create test data
		key := api.HexBytes([]byte("duplicate_key_1234567890abcdef12345678"))
		value1 := []byte("first_value_123456789")
		value2 := []byte("second_value_987654321")

		node1 := models.NewSmtNode(key, value1)
		node2 := models.NewSmtNode(key, value2)

		// Store the first node
		err := storage.Store(ctx, node1)
		require.NoError(t, err, "First store should not return an error")

		// Attempt to store the second node with the same key - should succeed (ignore duplicate)
		err = storage.Store(ctx, node2)
		require.NoError(t, err, "Second store should not return an error (duplicate should be ignored)")

		// Verify only the first node remains in storage
		storedNode, err := storage.GetByKey(ctx, key)
		require.NoError(t, err, "Should be able to retrieve stored node")
		require.NotNil(t, storedNode, "Retrieved node should not be nil")

		// Should have the first node's data (not overwritten)
		require.Equal(t, key, storedNode.Key)
		require.Equal(t, api.HexBytes(value1), storedNode.Value)
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

			require.Equal(t, []byte(node.Key), []byte(storedNode.Key))
			require.Equal(t, node.Value, storedNode.Value)
		}
	})
}

func TestSmtStorage_StoreBatch(t *testing.T) {
	db := setupSmtTestDB(t)
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

			require.Equal(t, []byte(node.Key), []byte(storedNode.Key))
			require.Equal(t, node.Value, storedNode.Value)
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
		batch2[0] = models.NewSmtNode(batch1[0].Key, []byte("new_value_1"))
		batch2[1] = models.NewSmtNode(batch1[1].Key, []byte("new_value_2"))

		// Last 3 nodes have new unique keys
		for i := 2; i < 5; i++ {
			key := make([]byte, 32)
			value := make([]byte, 32)

			// Create unique keys different from batch1
			key[30] = byte(i + 100)
			key[31] = byte(i + 200)
			value[31] = byte(i + 50)

			batch2[i] = models.NewSmtNode(api.HexBytes(key), value)
		}

		// Store second batch - should succeed despite duplicates
		err = storage.StoreBatch(ctx, batch2)
		require.NoError(t, err, "Second StoreBatch should not return an error (duplicates should be ignored)")

		// Verify original nodes are unchanged (not overwritten)
		for i := 0; i < 2; i++ {
			storedNode, err := storage.GetByKey(ctx, batch1[i].Key)
			require.NoError(t, err, "Should be able to retrieve original node %d", i)
			require.NotNil(t, storedNode, "Retrieved node %d should not be nil", i)

			// Should have original values, not the new ones
			require.Equal(t, batch1[i].Key, storedNode.Key)
			require.Equal(t, batch1[i].Value, storedNode.Value)
		}

		// Verify new unique nodes were stored
		for i := 2; i < 5; i++ {
			storedNode, err := storage.GetByKey(ctx, batch2[i].Key)
			require.NoError(t, err, "Should be able to retrieve new node %d", i)
			require.NotNil(t, storedNode, "Retrieved new node %d should not be nil", i)

			require.Equal(t, batch2[i].Key, storedNode.Key)
			require.Equal(t, batch2[i].Value, storedNode.Value)
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

			duplicateBatch[i] = models.NewSmtNode(originalBatch[i].Key, newValue)
		}

		// Store duplicate batch - should succeed and ignore all duplicates
		err = storage.StoreBatch(ctx, duplicateBatch)
		require.NoError(t, err, "Duplicate StoreBatch should not return an error")

		// Verify original data is unchanged
		for i := 0; i < 3; i++ {
			storedNode, err := storage.GetByKey(ctx, originalBatch[i].Key)
			require.NoError(t, err, "Should be able to retrieve original node %d", i)
			require.NotNil(t, storedNode, "Retrieved node %d should not be nil", i)

			// Should have original values, not the duplicate values
			require.Equal(t, originalBatch[i].Key, storedNode.Key)
			require.Equal(t, originalBatch[i].Value, storedNode.Value)
		}
	})

	t.Run("should handle empty batch", func(t *testing.T) {
		// Store empty batch
		err := storage.StoreBatch(ctx, []*models.SmtNode{})
		require.NoError(t, err, "Empty StoreBatch should not return an error")

		// Store nil batch
		err = storage.StoreBatch(ctx, nil)
		require.NoError(t, err, "Nil StoreBatch should not return an error")
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

			mixedBatch[i] = models.NewSmtNode(largeBatch[i].Key, newValue)
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

			mixedBatch[i] = models.NewSmtNode(api.HexBytes(key), value)
		}

		// Store mixed batch - should succeed
		err = storage.StoreBatch(ctx, mixedBatch)
		require.NoError(t, err, "Mixed batch with duplicates should not return an error")

		// Verify original data is preserved for duplicates
		for i := 0; i < 50; i++ {
			storedNode, err := storage.GetByKey(ctx, largeBatch[i].Key)
			require.NoError(t, err, "Should be able to retrieve original node %d", i)

			// Should have original values
			require.Equal(t, largeBatch[i].Value, storedNode.Value)
		}

		// Verify new data is stored
		for i := 50; i < 100; i++ {
			storedNode, err := storage.GetByKey(ctx, mixedBatch[i].Key)
			require.NoError(t, err, "Should be able to retrieve new node %d", i)

			// Should have new values
			require.Equal(t, mixedBatch[i].Value, storedNode.Value)
		}
	})
}

func TestSmtStorage_Count(t *testing.T) {
	db := setupSmtTestDB(t)
	storage := NewSmtStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	t.Run("should count stored nodes correctly", func(t *testing.T) {
		// Initially should be 0
		count, err := storage.Count(ctx)
		require.NoError(t, err, "Count should not return an error")
		require.Equal(t, int64(0), count, "Initial count should be 0")

		// Store some nodes
		nodes := createTestSmtNodes(5)
		err = storage.StoreBatch(ctx, nodes)
		require.NoError(t, err, "StoreBatch should not return an error")

		// Count should be 5
		count, err = storage.Count(ctx)
		require.NoError(t, err, "Count should not return an error")
		require.Equal(t, int64(5), count, "Count should be 5 after storing 5 nodes")

		// Store duplicates
		err = storage.StoreBatch(ctx, nodes)
		require.NoError(t, err, "StoreBatch with duplicates should not return an error")

		// Count should still be 5 (duplicates ignored)
		count, err = storage.Count(ctx)
		require.NoError(t, err, "Count should not return an error")
		require.Equal(t, int64(5), count, "Count should still be 5 after storing duplicates")
	})
}

func TestSmtStorage_GetChunked(t *testing.T) {
	db := setupSmtTestDB(t)
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
		require.Len(t, chunk1, 5, "First chunk should have 5 nodes")

		chunk2, err := storage.GetChunked(ctx, 5, 5)
		require.NoError(t, err, "GetChunked should not return an error")
		require.Len(t, chunk2, 5, "Second chunk should have 5 nodes")

		// Third chunk should be empty
		chunk3, err := storage.GetChunked(ctx, 10, 5)
		require.NoError(t, err, "GetChunked should not return an error")
		require.Len(t, chunk3, 0, "Third chunk should be empty")

		// Verify we got all unique nodes (no duplicates)
		allChunkedKeys := make(map[string]bool)
		for _, node := range append(chunk1, chunk2...) {
			keyStr := string(node.Key)
			require.False(t, allChunkedKeys[keyStr], "Should not have duplicate keys in chunked results")
			allChunkedKeys[keyStr] = true
		}

		require.Len(t, allChunkedKeys, 10, "Should have exactly 10 unique keys")
	})
}

func TestSmtStorage_StoreBatch_DuplicateHandling(t *testing.T) {
	db := setupSmtTestDB(t)
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
		models.NewSmtNode(api.HexBytes("newkey"), api.HexBytes("newvalue")),
	}

	// Store first batch
	err = storage.StoreBatch(ctx, nodes1)
	require.NoError(t, err, "First StoreBatch should not return an error")

	// Store second batch with duplicates - should not error
	err = storage.StoreBatch(ctx, nodes2)
	require.NoError(t, err, "StoreBatch with duplicates should not return an error")

	// Verify that we only have 4 unique nodes (3 from first batch + 1 new from second)
	count, err := storage.Count(ctx)
	require.NoError(t, err, "Count should not return an error")
	require.Equal(t, int64(4), count, "Should have exactly 4 nodes (duplicates ignored)")

	// Verify the new node was stored
	newNode, err := storage.GetByKey(ctx, api.HexBytes("newkey"))
	require.NoError(t, err, "GetByKey should not return an error")
	require.NotNil(t, newNode, "New node should be found")
	require.Equal(t, []byte("newvalue"), []byte(newNode.Value), "New node should have correct value")
}

func TestSmtStorage_GetByKeys(t *testing.T) {
	db := setupSmtTestDB(t)
	storage := NewSmtStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	nodesToStore := createTestSmtNodes(10)
	err = storage.StoreBatch(ctx, nodesToStore)
	require.NoError(t, err, "StoreBatch should not return an error")

	t.Run("should retrieve multiple existing keys", func(t *testing.T) {
		keysToGet := []api.HexBytes{
			nodesToStore[1].Key,
			nodesToStore[3].Key,
			nodesToStore[5].Key,
		}

		retrievedNodes, err := storage.GetByKeys(ctx, keysToGet)
		require.NoError(t, err, "GetByKeys should not return an error")
		require.NotNil(t, retrievedNodes)
		require.Len(t, retrievedNodes, 3, "Should retrieve exactly 3 nodes")

		// Verify the correct nodes were returned
		expectedNodes := make(map[string]*models.SmtNode)
		expectedNodes[string(nodesToStore[1].Key)] = nodesToStore[1]
		expectedNodes[string(nodesToStore[3].Key)] = nodesToStore[3]
		expectedNodes[string(nodesToStore[5].Key)] = nodesToStore[5]

		for _, retrieved := range retrievedNodes {
			expected, ok := expectedNodes[string(retrieved.Key)]
			require.True(t, ok, "Retrieved a key that was not requested")
			require.Equal(t, expected.Value, retrieved.Value, "Value for key %s does not match", retrieved.Key)
		}
	})

	t.Run("should handle a mix of existing and non-existing keys", func(t *testing.T) {
		nonExistentKey := api.HexBytes("deadbeefdeadbeefdeadbeefdeadbeef")
		keysToGet := []api.HexBytes{
			nodesToStore[0].Key,
			nonExistentKey,
			nodesToStore[2].Key,
		}

		retrievedNodes, err := storage.GetByKeys(ctx, keysToGet)
		require.NoError(t, err, "GetByKeys should not return an error")
		require.NotNil(t, retrievedNodes)

		// Verify the results - should only get the 2 existing nodes
		expectedNodes := make(map[string]*models.SmtNode)
		expectedNodes[string(nodesToStore[0].Key)] = nodesToStore[0]
		expectedNodes[string(nodesToStore[2].Key)] = nodesToStore[2]

		require.Len(t, retrievedNodes, len(expectedNodes), "Should retrieve only the existing nodes")

		for _, retrieved := range retrievedNodes {
			keyStr := string(retrieved.Key)
			expected, ok := expectedNodes[keyStr]
			require.True(t, ok, "Retrieved a key that was not requested: %s", keyStr)
			require.Equal(t, expected.Value, retrieved.Value, "Value for key %s does not match", keyStr)
			// Remove from map to ensure no duplicates are retrieved and all were found
			delete(expectedNodes, keyStr)
		}

		require.Empty(t, expectedNodes, "Not all expected nodes were retrieved")
	})

	t.Run("should return an empty slice for all non-existing keys", func(t *testing.T) {
		keysToGet := []api.HexBytes{
			api.HexBytes("facefeedfacefeedfacefeedfacefeed"),
			api.HexBytes("badcoffeebadcoffeebadcoffeebadcoffee"),
		}

		retrievedNodes, err := storage.GetByKeys(ctx, keysToGet)
		require.NoError(t, err, "GetByKeys should not return an error")
		require.Len(t, retrievedNodes, 0, "Should return an empty slice for non-existing keys")
	})

	t.Run("should return an empty slice for an empty key list", func(t *testing.T) {
		retrievedNodes, err := storage.GetByKeys(ctx, []api.HexBytes{})
		require.NoError(t, err, "GetByKeys should not return an error")
		require.NotNil(t, retrievedNodes)
		require.Len(t, retrievedNodes, 0, "Should return an empty slice for an empty key list")
	})
}

func TestSmtStorage_DeleteBatch(t *testing.T) {
	db := setupSmtTestDB(t)
	storage := NewSmtStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	t.Run("should delete a batch of existing nodes", func(t *testing.T) {
		// 1. Store a known set of nodes for this specific test.
		nodesToStore := createTestSmtNodes(10)
		err := storage.StoreBatch(ctx, nodesToStore)
		require.NoError(t, err, "StoreBatch should not return an error")

		// 2. Define keys to delete and a key to keep.
		keysToDelete := []api.HexBytes{
			nodesToStore[2].Key,
			nodesToStore[4].Key,
			nodesToStore[6].Key,
		}
		keyToKeep := nodesToStore[0].Key

		// 3. Call DeleteBatch.
		err = storage.DeleteBatch(ctx, keysToDelete)
		require.NoError(t, err, "DeleteBatch should not return an error")

		// 4. Verify that the deleted keys are gone.
		for _, deletedKey := range keysToDelete {
			node, err := storage.GetByKey(ctx, deletedKey)
			require.NoError(t, err)
			require.Nil(t, node, "Deleted node with key %s should not be found", deletedKey)
		}

		// 5. Verify a non-deleted node still exists.
		stillExistsNode, err := storage.GetByKey(ctx, keyToKeep)
		require.NoError(t, err)
		require.NotNil(t, stillExistsNode, "Node that was not deleted should still exist")
	})

	t.Run("should handle a mix of existing and non-existing keys", func(t *testing.T) {
		// 1. Store a known set of nodes.
		nodesToStore := createTestSmtNodes(5)
		err := storage.StoreBatch(ctx, nodesToStore)
		require.NoError(t, err, "StoreBatch should not return an error")

		// 2. Define keys to delete, including some that don't exist.
		keyToDelete1 := nodesToStore[1].Key
		keyToDelete2 := nodesToStore[3].Key
		nonExistentKey := api.HexBytes("deadbeefdeadbeefdeadbeefdeadbeef")
		keysToDelete := []api.HexBytes{
			keyToDelete1,
			nonExistentKey,
			keyToDelete2,
		}

		// 3. Call DeleteBatch - should not error.
		err = storage.DeleteBatch(ctx, keysToDelete)
		require.NoError(t, err, "DeleteBatch should not error on non-existent keys")

		// 4. Verify the nodes that should have been deleted are gone.
		deletedNode1, err := storage.GetByKey(ctx, keyToDelete1)
		require.NoError(t, err)
		require.Nil(t, deletedNode1, "Node 1 should be deleted")

		deletedNode3, err := storage.GetByKey(ctx, keyToDelete2)
		require.NoError(t, err)
		require.Nil(t, deletedNode3, "Node 3 should be deleted")
	})

	t.Run("should handle an empty key list", func(t *testing.T) {
		// 1. Store a known set of nodes.
		nodesToStore := createTestSmtNodes(5)
		err := storage.StoreBatch(ctx, nodesToStore)
		require.NoError(t, err, "StoreBatch should not return an error")

		// 2. Get the initial state of one node.
		nodeBefore, err := storage.GetByKey(ctx, nodesToStore[0].Key)
		require.NoError(t, err)
		require.NotNil(t, nodeBefore)

		// 3. Call DeleteBatch with an empty slice.
		err = storage.DeleteBatch(ctx, []api.HexBytes{})
		require.NoError(t, err, "DeleteBatch with empty slice should not return an error")

		// 4. Verify the node is unchanged by fetching it again.
		nodeAfter, err := storage.GetByKey(ctx, nodesToStore[0].Key)
		require.NoError(t, err)
		require.NotNil(t, nodeAfter, "Node should still exist after empty delete batch")
		require.Equal(t, nodeBefore.Value, nodeAfter.Value)
	})
}

func TestSmtStorage_Delete(t *testing.T) {
	db := setupSmtTestDB(t)
	storage := NewSmtStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	t.Run("should delete an existing node", func(t *testing.T) {
		// 1. Store a known set of nodes.
		nodesToStore := createTestSmtNodes(3)
		err := storage.StoreBatch(ctx, nodesToStore)
		require.NoError(t, err, "StoreBatch should not return an error")

		keyToDelete := nodesToStore[1].Key
		keyToKeep := nodesToStore[0].Key

		// 2. Call Delete.
		err = storage.Delete(ctx, keyToDelete)
		require.NoError(t, err, "Delete should not return an error")

		// 3. Verify the node is gone.
		deletedNode, err := storage.GetByKey(ctx, keyToDelete)
		require.NoError(t, err)
		require.Nil(t, deletedNode, "Deleted node should not be found")

		// 4. Verify other nodes are unaffected.
		keptNode, err := storage.GetByKey(ctx, keyToKeep)
		require.NoError(t, err)
		require.NotNil(t, keptNode, "Other nodes should not be deleted")
	})

	t.Run("should not error when deleting a non-existing node", func(t *testing.T) {
		// 1. Store a node to ensure the collection is not empty.
		nodeToStore := createTestSmtNodes(1)[0]
		err := storage.Store(ctx, nodeToStore)
		require.NoError(t, err, "Store should not return an error")

		// 2. Attempt to delete a key that does not exist.
		nonExistentKey := api.HexBytes("deadbeefdeadbeefdeadbeefdeadbeef")
		err = storage.Delete(ctx, nonExistentKey)
		require.NoError(t, err, "Delete should not return an error for a non-existing key")

		// 3. Verify the original node is unaffected.
		originalNode, err := storage.GetByKey(ctx, nodeToStore.Key)
		require.NoError(t, err)
		require.NotNil(t, originalNode, "Existing node should not be affected")
	})
}

func TestSmtStorage_UpsertBatch(t *testing.T) {
	db := setupSmtTestDB(t)
	storage := NewSmtStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	t.Run("should insert new nodes correctly", func(t *testing.T) {
		nodesToInsert := createTestSmtNodes(5)
		err := storage.UpsertBatch(ctx, nodesToInsert)
		require.NoError(t, err, "UpsertBatch should not return an error for new nodes")

		// Verify all nodes were inserted
		var keys []api.HexBytes
		for _, node := range nodesToInsert {
			keys = append(keys, node.Key)
		}
		retrievedNodes, err := storage.GetByKeys(ctx, keys)
		require.NoError(t, err)
		require.Len(t, retrievedNodes, 5)
	})

	t.Run("should update existing nodes", func(t *testing.T) {
		// 1. Store an initial batch
		initialNodes := createTestSmtNodes(3)
		err := storage.StoreBatch(ctx, initialNodes)
		require.NoError(t, err)

		// 2. Create a new batch with the same keys but different values
		nodesToUpdate := []*models.SmtNode{
			models.NewSmtNode(initialNodes[0].Key, []byte("new-value-0")),
			models.NewSmtNode(initialNodes[2].Key, []byte("new-value-2")),
		}

		// 3. Call UpsertBatch
		err = storage.UpsertBatch(ctx, nodesToUpdate)
		require.NoError(t, err, "UpsertBatch should not return an error when updating nodes")

		// 4. Verify the nodes were updated
		updatedNode0, err := storage.GetByKey(ctx, initialNodes[0].Key)
		require.NoError(t, err)
		require.NotNil(t, updatedNode0)
		require.Equal(t, api.HexBytes("new-value-0"), updatedNode0.Value)

		updatedNode2, err := storage.GetByKey(ctx, initialNodes[2].Key)
		require.NoError(t, err)
		require.NotNil(t, updatedNode2)
		require.Equal(t, api.HexBytes("new-value-2"), updatedNode2.Value)

		// 5. Verify the non-updated node is unchanged
		unaffectedNode1, err := storage.GetByKey(ctx, initialNodes[1].Key)
		require.NoError(t, err)
		require.NotNil(t, unaffectedNode1)
		require.Equal(t, initialNodes[1].Value, unaffectedNode1.Value)
	})

	t.Run("should handle a mix of new and existing nodes", func(t *testing.T) {
		// 1. Store an initial batch
		initialNodes := createTestSmtNodes(5)
		err := storage.StoreBatch(ctx, initialNodes)
		require.NoError(t, err)
		countBefore, err := storage.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(5), countBefore)

		// 2. Create a mixed batch with truly unique new keys
		newNode1 := models.NewSmtNode(api.HexBytes("new_key_1"), []byte("new-value-1"))
		newNode2 := models.NewSmtNode(api.HexBytes("new_key_2"), []byte("new-value-2"))

		mixedBatch := []*models.SmtNode{
			// Update existing nodes
			models.NewSmtNode(initialNodes[0].Key, []byte("updated-value-0")),
			models.NewSmtNode(initialNodes[4].Key, []byte("updated-value-4")),
			// Insert new nodes
			newNode1,
			newNode2,
		}

		// 3. Call UpsertBatch
		err = storage.UpsertBatch(ctx, mixedBatch)
		require.NoError(t, err, "UpsertBatch should handle mixed operations")

		// 4. Verify counts
		countAfter, err := storage.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(7), countAfter, "Count should be 7 (5 initial + 2 new)")

		// 5. Verify updated nodes
		updatedNode, err := storage.GetByKey(ctx, initialNodes[0].Key)
		require.NoError(t, err)
		require.Equal(t, api.HexBytes("updated-value-0"), updatedNode.Value)

		// 6. Verify inserted nodes
		insertedNode, err := storage.GetByKey(ctx, newNode1.Key)
		require.NoError(t, err)
		require.NotNil(t, insertedNode)
		require.Equal(t, newNode1.Value, insertedNode.Value)
	})
}

func TestSmtStorage_GetAll(t *testing.T) {
	t.Run("should return all nodes from a populated collection", func(t *testing.T) {
		db := setupSmtTestDB(t)
		storage := NewSmtStorage(db)
		ctx := context.Background()
		err := storage.CreateIndexes(ctx)
		require.NoError(t, err)

		nodesToStore := createTestSmtNodes(15)
		err = storage.StoreBatch(ctx, nodesToStore)
		require.NoError(t, err)

		allNodes, err := storage.GetAll(ctx)
		require.NoError(t, err)
		require.Len(t, allNodes, 15)
	})

	t.Run("should return an empty slice for an empty collection", func(t *testing.T) {
		db := setupSmtTestDB(t)
		storage := NewSmtStorage(db)
		ctx := context.Background()
		err := storage.CreateIndexes(ctx)
		require.NoError(t, err)

		allNodes, err := storage.GetAll(ctx)
		require.NoError(t, err)
		require.Len(t, allNodes, 0)
	})
}
