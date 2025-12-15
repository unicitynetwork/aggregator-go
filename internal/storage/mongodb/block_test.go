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

// Test configuration
const (
	blockTestTimeout = 30 * time.Second
)

// setupBlockTestDB creates a test database connection using Testcontainers
func setupBlockTestDB(t *testing.T) (*mongo.Database, func()) {
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
	connectCtx, cancel := context.WithTimeout(ctx, blockTestTimeout)
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
	db := client.Database("test_block_db")

	// Cleanup function
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), blockTestTimeout)
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

// createTestBlock creates a test block with the given index
func createTestBlock(index *api.BigInt) *models.Block {
	rootHash, _ := api.NewHexBytesFromString("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	previousBlockHash, _ := api.NewHexBytesFromString("0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321")
	unicityCertificate, _ := api.NewHexBytesFromString("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
	noDeletionProofHash, _ := api.NewHexBytesFromString("0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba")

	block := &models.Block{
		Index:               index,
		ChainID:             "test-chain-id",
		Version:             "1.0.0",
		ForkID:              "test-fork",
		RootHash:            rootHash,
		PreviousBlockHash:   previousBlockHash,
		NoDeletionProofHash: noDeletionProofHash,
		CreatedAt:           api.Now(),
		UnicityCertificate:  unicityCertificate,
		Finalized:           true, // Blocks are finalized by default in tests
	}

	return block
}

// createTestBlockWithRootHash creates a test block with specified index, chainID, and root hash
func createTestBlockWithRootHash(index int64, chainID string, rootHash api.HexBytes) *models.Block {
	indexBigInt := api.NewBigInt(big.NewInt(index))
	previousBlockHash, _ := api.NewHexBytesFromString("0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321")
	unicityCertificate, _ := api.NewHexBytesFromString("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
	noDeletionProofHash, _ := api.NewHexBytesFromString("0x9876543210fedcba9876543210fedcba9876543210fedcba9876543210fedcba")

	block := &models.Block{
		Index:               indexBigInt,
		ChainID:             chainID,
		Version:             "1.0.0",
		ForkID:              "test-fork",
		RootHash:            rootHash,
		PreviousBlockHash:   previousBlockHash,
		NoDeletionProofHash: noDeletionProofHash,
		CreatedAt:           api.Now(),
		UnicityCertificate:  unicityCertificate,
		Finalized:           true, // Blocks are finalized by default in tests
	}

	return block
}

// createTestBlocksRange creates a range of test blocks from start to end (inclusive)
func createTestBlocksRange(start, count int64) []*models.Block {
	blocks := make([]*models.Block, count)
	for i := int64(0); i < count; i++ {
		index := api.NewBigInt(big.NewInt(start + i))
		blocks[i] = createTestBlock(index)
		// Vary some fields to make blocks distinguishable
		blocks[i].ChainID = "chain-" + index.String()
		blocks[i].Version = "1.0." + index.String()
	}
	return blocks
}

func TestBlockStorage_Store(t *testing.T) {
	db, cleanup := setupBlockTestDB(t)
	defer cleanup()

	storage := NewBlockStorage(db)
	ctx := context.Background()

	// Create indexes first
	err := storage.CreateIndexes(ctx)
	require.NoError(t, err, "CreateIndexes should not return an error")

	t.Run("should store valid block", func(t *testing.T) {
		// Create test block
		index := api.NewBigInt(big.NewInt(1))
		block := createTestBlock(index)

		// Store the block
		err := storage.Store(ctx, block)
		require.NoError(t, err, "Store should not return an error")

		// Verify the block was stored by retrieving it
		storedBlock, err := storage.GetByNumber(ctx, index)
		require.NoError(t, err, "Should be able to retrieve stored block")
		require.NotNil(t, storedBlock, "Retrieved block should not be nil")

		// Verify the stored data matches
		assert.Equal(t, 0, block.Index.Cmp(storedBlock.Index.Int), "Index should match")
		assert.Equal(t, block.ChainID, storedBlock.ChainID)
		assert.Equal(t, block.Version, storedBlock.Version)
		assert.Equal(t, block.ForkID, storedBlock.ForkID)
		assert.Equal(t, block.RootHash, storedBlock.RootHash)
		assert.Equal(t, block.PreviousBlockHash, storedBlock.PreviousBlockHash)
		assert.Equal(t, block.UnicityCertificate, storedBlock.UnicityCertificate)
		assert.NotNil(t, storedBlock.CreatedAt)
	})

	t.Run("should handle large block numbers", func(t *testing.T) {
		// Create block with large number
		largeNumber := new(big.Int)
		largeNumber.SetString("999999999999999999999999999999", 10)
		index := api.NewBigInt(largeNumber)

		block := createTestBlock(index)

		// Store the block
		err := storage.Store(ctx, block)
		require.NoError(t, err, "Store should not return an error for large number")

		// Retrieve and verify
		storedBlock, err := storage.GetByNumber(ctx, index)
		require.NoError(t, err, "Should be able to retrieve block with large number")
		require.NotNil(t, storedBlock, "Retrieved block should not be nil")
		assert.Equal(t, 0, index.Cmp(storedBlock.Index.Int), "Index should match for large number")
	})

	t.Run("should fail to store duplicate block index", func(t *testing.T) {
		// Create first block
		index := api.NewBigInt(big.NewInt(100))
		block1 := createTestBlock(index)

		// Store first block
		err := storage.Store(ctx, block1)
		require.NoError(t, err, "First store should not return an error")

		// Create second block with same index
		block2 := createTestBlock(index)
		block2.ChainID = "different-chain"

		// Attempt to store second block - should fail due to unique index constraint
		err = storage.Store(ctx, block2)
		assert.Error(t, err, "Second store with duplicate index should return an error")
	})
}

func TestBlockStorage_GetByNumber(t *testing.T) {
	db, cleanup := setupBlockTestDB(t)
	defer cleanup()

	storage := NewBlockStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err)

	t.Run("should return nil for non-existent block", func(t *testing.T) {
		nonExistentIndex := api.NewBigInt(big.NewInt(9999))

		block, err := storage.GetByNumber(ctx, nonExistentIndex)
		require.NoError(t, err, "GetByNumber should not return an error for non-existent block")
		assert.Nil(t, block, "Non-existent block should return nil")
	})

	t.Run("should retrieve existing block", func(t *testing.T) {
		// Store a block
		index := api.NewBigInt(big.NewInt(42))
		originalBlock := createTestBlock(index)
		originalBlock.ChainID = "test-retrieval-chain"

		err := storage.Store(ctx, originalBlock)
		require.NoError(t, err)

		// Retrieve the block
		retrievedBlock, err := storage.GetByNumber(ctx, index)
		require.NoError(t, err, "GetByNumber should not return an error")
		require.NotNil(t, retrievedBlock, "Retrieved block should not be nil")

		// Verify data matches
		assert.Equal(t, 0, originalBlock.Index.Cmp(retrievedBlock.Index.Int), "Index should match")
		assert.Equal(t, originalBlock.ChainID, retrievedBlock.ChainID)
		assert.Equal(t, originalBlock.Version, retrievedBlock.Version)
	})

	t.Run("should handle decimal128 conversion correctly", func(t *testing.T) {
		// Test various number formats
		testCases := []int64{0, 1, 100, 999, 1000, 99999, 1000000}

		for _, num := range testCases {
			index := api.NewBigInt(big.NewInt(num))
			block := createTestBlock(index)
			block.ChainID = "decimal-test-" + index.String()

			// Store
			err := storage.Store(ctx, block)
			require.NoError(t, err, "Should store block with index %d", num)

			// Retrieve
			retrieved, err := storage.GetByNumber(ctx, index)
			require.NoError(t, err, "Should retrieve block with index %d", num)
			require.NotNil(t, retrieved, "Retrieved block should not be nil for index %d", num)

			assert.Equal(t, 0, index.Cmp(retrieved.Index.Int), "Index should match for %d", num)
			assert.Equal(t, block.ChainID, retrieved.ChainID)
		}
	})
}

func TestBlockStorage_GetLatest(t *testing.T) {
	db, cleanup := setupBlockTestDB(t)
	defer cleanup()

	storage := NewBlockStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err)

	t.Run("should return nil when no blocks exist", func(t *testing.T) {
		block, err := storage.GetLatest(ctx)
		require.NoError(t, err, "GetLatest should not return an error when empty")
		assert.Nil(t, block, "GetLatest should return nil when no blocks exist")
	})

	t.Run("should return latest block with single block", func(t *testing.T) {
		// Store single block
		index := api.NewBigInt(big.NewInt(5))
		originalBlock := createTestBlock(index)
		originalBlock.ChainID = "single-latest-test"

		err := storage.Store(ctx, originalBlock)
		require.NoError(t, err)

		// Get latest
		latestBlock, err := storage.GetLatest(ctx)
		require.NoError(t, err, "GetLatest should not return an error")
		require.NotNil(t, latestBlock, "Latest block should not be nil")

		assert.Equal(t, 0, index.Cmp(latestBlock.Index.Int), "Index should match")
		assert.Equal(t, originalBlock.ChainID, latestBlock.ChainID)
	})

	t.Run("should return latest block with multiple blocks", func(t *testing.T) {
		// Store multiple blocks in non-sequential order (using different numbers to avoid conflicts)
		blocks := []*models.Block{
			createTestBlock(api.NewBigInt(big.NewInt(110))),
			createTestBlock(api.NewBigInt(big.NewInt(125))),
			createTestBlock(api.NewBigInt(big.NewInt(115))),
			createTestBlock(api.NewBigInt(big.NewInt(130))),
			createTestBlock(api.NewBigInt(big.NewInt(105))),
		}

		// Store all blocks
		for i, block := range blocks {
			block.ChainID = "multi-latest-test-" + block.Index.String()
			err := storage.Store(ctx, block)
			require.NoError(t, err, "Should store block %d", i)
		}

		// Get latest - should be block with index 130
		latestBlock, err := storage.GetLatest(ctx)
		require.NoError(t, err, "GetLatest should not return an error")
		require.NotNil(t, latestBlock, "Latest block should not be nil")

		expectedLatestIndex := api.NewBigInt(big.NewInt(130))
		assert.Equal(t, 0, expectedLatestIndex.Cmp(latestBlock.Index.Int), "Should get latest block")
		assert.Equal(t, "multi-latest-test-130", latestBlock.ChainID)
	})

	t.Run("should handle decimal128 sorting correctly for large numbers", func(t *testing.T) {
		// Test with large numbers to ensure decimal128 sorts correctly
		largeNumbers := []string{
			"1000000000000000000000",
			"999999999999999999999",
			"1000000000000000000001",
			"2000000000000000000000",
		}

		var expectedLatestIndex *api.BigInt
		for _, numStr := range largeNumbers {
			bigInt := new(big.Int)
			bigInt.SetString(numStr, 10)
			index := api.NewBigInt(bigInt)

			block := createTestBlock(index)
			block.ChainID = "large-num-test-" + numStr

			err := storage.Store(ctx, block)
			require.NoError(t, err, "Should store large number block")

			// Track expected latest (largest number)
			if expectedLatestIndex == nil || index.Cmp(expectedLatestIndex.Int) > 0 {
				expectedLatestIndex = index
			}
		}

		// Get latest
		latestBlock, err := storage.GetLatest(ctx)
		require.NoError(t, err, "GetLatest should not return an error")
		require.NotNil(t, latestBlock, "Latest block should not be nil")

		assert.Equal(t, 0, expectedLatestIndex.Cmp(latestBlock.Index.Int), "Should get latest block")
		assert.Equal(t, "large-num-test-2000000000000000000000", latestBlock.ChainID)
	})
}

func TestBlockStorage_GetLatestByRootHash(t *testing.T) {
	db, cleanup := setupBlockTestDB(t)
	defer cleanup()

	storage := NewBlockStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err)

	t.Run("GetLatestByRootHash_NonExistentRootHash", func(t *testing.T) {
		rootHash := api.HexBytes("nonexistent")
		block, err := storage.GetLatestByRootHash(ctx, rootHash)
		require.NoError(t, err, "GetLatestByRootHash should not return an error for non-existent root hash")
		assert.Nil(t, block, "Should return nil for non-existent root hash")
	})

	t.Run("GetLatestByRootHash_WithBlocks", func(t *testing.T) {
		// Create test blocks with different root hashes
		rootHash1 := api.HexBytes("0000111122223333444455556666777788889999aaaabbbbccccddddeeeeffff")
		rootHash2 := api.HexBytes("1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff")

		blocks := []*models.Block{
			createTestBlockWithRootHash(1, "test-chain", rootHash1),
			createTestBlockWithRootHash(2, "test-chain", rootHash2),
			createTestBlockWithRootHash(3, "test-chain", rootHash1), // Same root hash as block 1
			createTestBlockWithRootHash(4, "test-chain", rootHash2), // Same root hash as block 2
		}

		// Store all blocks
		for _, block := range blocks {
			err := storage.Store(ctx, block)
			require.NoError(t, err, "Should store block successfully")
		}

		// Test getting latest block with rootHash1 (should return block 3)
		latestBlock1, err := storage.GetLatestByRootHash(ctx, rootHash1)
		require.NoError(t, err, "GetLatestByRootHash should not return an error")
		require.NotNil(t, latestBlock1, "Should find block with rootHash1")
		assert.Equal(t, big.NewInt(3), latestBlock1.Index.Int, "Should return the latest block (block 3) with rootHash1")
		assert.Equal(t, rootHash1.String(), latestBlock1.RootHash.String(), "Root hash should match")

		// Test getting latest block with rootHash2 (should return block 4)
		latestBlock2, err := storage.GetLatestByRootHash(ctx, rootHash2)
		require.NoError(t, err, "GetLatestByRootHash should not return an error")
		require.NotNil(t, latestBlock2, "Should find block with rootHash2")
		assert.Equal(t, big.NewInt(4), latestBlock2.Index.Int, "Should return the latest block (block 4) with rootHash2")
		assert.Equal(t, rootHash2.String(), latestBlock2.RootHash.String(), "Root hash should match")

		// Test with non-existent root hash
		nonExistentHash := api.HexBytes("9999888877776666555544443333222211110000aaaabbbbccccddddeeeeffff")
		nonExistentBlock, err := storage.GetLatestByRootHash(ctx, nonExistentHash)
		require.NoError(t, err, "GetLatestByRootHash should not return an error for non-existent hash")
		assert.Nil(t, nonExistentBlock, "Should return nil for non-existent root hash")
	})
}

func TestBlockStorage_GetLatestNumber(t *testing.T) {
	db, cleanup := setupBlockTestDB(t)
	defer cleanup()

	storage := NewBlockStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err)

	t.Run("should return nil when no blocks exist", func(t *testing.T) {
		number, err := storage.GetLatestNumber(ctx)
		require.NoError(t, err, "GetLatestNumber should not return an error when empty")
		assert.Nil(t, number, "GetLatestNumber should return nil when no blocks exist")
	})

	t.Run("should return latest number with single block", func(t *testing.T) {
		// Store single block
		index := api.NewBigInt(big.NewInt(123))
		block := createTestBlock(index)

		err := storage.Store(ctx, block)
		require.NoError(t, err)

		// Get latest number
		latestNumber, err := storage.GetLatestNumber(ctx)
		require.NoError(t, err, "GetLatestNumber should not return an error")
		require.NotNil(t, latestNumber, "Latest number should not be nil")

		assert.Equal(t, 0, index.Cmp(latestNumber.Int), "Index should match")
	})

	t.Run("should return latest number with multiple blocks", func(t *testing.T) {
		// Store multiple blocks
		indices := []int64{50, 100, 75, 200, 25}
		expectedLatest := int64(200)

		for _, idx := range indices {
			block := createTestBlock(api.NewBigInt(big.NewInt(idx)))
			err := storage.Store(ctx, block)
			require.NoError(t, err, "Should store block with index %d", idx)
		}

		// Get latest number
		latestNumber, err := storage.GetLatestNumber(ctx)
		require.NoError(t, err, "GetLatestNumber should not return an error")
		require.NotNil(t, latestNumber, "Latest number should not be nil")

		expectedLatestBigInt := api.NewBigInt(big.NewInt(expectedLatest))
		assert.Equal(t, 0, expectedLatestBigInt.Cmp(latestNumber.Int), "Latest number should match")
	})

	t.Run("should handle decimal128 to BigInt conversion", func(t *testing.T) {
		// Test various edge cases for decimal128 conversion
		testNumbers := []string{
			"0",
			"1",
			"999999999999999999999",
			"1000000000000000000000",
		}

		var expectedLatest *api.BigInt

		for _, numStr := range testNumbers {
			bigInt := new(big.Int)
			bigInt.SetString(numStr, 10)
			index := api.NewBigInt(bigInt)

			block := createTestBlock(index)
			err := storage.Store(ctx, block)
			require.NoError(t, err, "Should store block with number %s", numStr)

			if expectedLatest == nil || index.Cmp(expectedLatest.Int) > 0 {
				expectedLatest = index
			}
		}

		// Get latest number
		latestNumber, err := storage.GetLatestNumber(ctx)
		require.NoError(t, err, "GetLatestNumber should not return an error")
		require.NotNil(t, latestNumber, "Latest number should not be nil")

		assert.Equal(t, 0, expectedLatest.Cmp(latestNumber.Int), "Latest should match expected")
	})
}

func TestBlockStorage_Count(t *testing.T) {
	db, cleanup := setupBlockTestDB(t)
	defer cleanup()

	storage := NewBlockStorage(db)
	ctx := context.Background()

	t.Run("should return 0 for empty collection", func(t *testing.T) {
		count, err := storage.Count(ctx)
		require.NoError(t, err, "Count should not return an error")
		assert.Equal(t, int64(0), count, "Count should be 0 for empty collection")
	})

	t.Run("should return correct count with single block", func(t *testing.T) {
		// Store single block
		block := createTestBlock(api.NewBigInt(big.NewInt(1)))
		err := storage.Store(ctx, block)
		require.NoError(t, err)

		count, err := storage.Count(ctx)
		require.NoError(t, err, "Count should not return an error")
		assert.Equal(t, int64(1), count, "Count should be 1 after storing one block")
	})

	t.Run("should return correct count with multiple blocks", func(t *testing.T) {
		// Store multiple blocks
		blocks := createTestBlocksRange(10, 5) // blocks 10-14
		for _, block := range blocks {
			err := storage.Store(ctx, block)
			require.NoError(t, err, "Should store block")
		}

		count, err := storage.Count(ctx)
		require.NoError(t, err, "Count should not return an error")
		assert.Equal(t, int64(6), count, "Count should be 6 (1 existing + 5 new)")
	})
}

func TestBlockStorage_GetRange(t *testing.T) {
	db, cleanup := setupBlockTestDB(t)
	defer cleanup()

	storage := NewBlockStorage(db)
	ctx := context.Background()

	err := storage.CreateIndexes(ctx)
	require.NoError(t, err)

	// Store test blocks for range testing
	blocks := createTestBlocksRange(1, 10) // blocks 1-10
	for _, block := range blocks {
		err := storage.Store(ctx, block)
		require.NoError(t, err, "Should store setup block")
	}

	t.Run("should return empty range for non-existent blocks", func(t *testing.T) {
		fromBlock := api.NewBigInt(big.NewInt(100))
		toBlock := api.NewBigInt(big.NewInt(110))

		blocks, err := storage.GetRange(ctx, fromBlock, toBlock)
		require.NoError(t, err, "GetRange should not return an error for non-existent range")
		assert.Empty(t, blocks, "Should return empty slice for non-existent range")
	})

	t.Run("should return single block in range", func(t *testing.T) {
		fromBlock := api.NewBigInt(big.NewInt(5))
		toBlock := api.NewBigInt(big.NewInt(5))

		blocks, err := storage.GetRange(ctx, fromBlock, toBlock)
		require.NoError(t, err, "GetRange should not return an error")
		require.Len(t, blocks, 1, "Should return exactly one block")

		assert.Equal(t, 0, fromBlock.Cmp(blocks[0].Index.Int), "Block index should match")
	})

	t.Run("should return multiple blocks in range", func(t *testing.T) {
		fromBlock := api.NewBigInt(big.NewInt(3))
		toBlock := api.NewBigInt(big.NewInt(7))

		blocks, err := storage.GetRange(ctx, fromBlock, toBlock)
		require.NoError(t, err, "GetRange should not return an error")
		require.Len(t, blocks, 5, "Should return 5 blocks (3,4,5,6,7)")

		// Verify blocks are returned in ascending order
		for i, block := range blocks {
			expectedIndex := api.NewBigInt(big.NewInt(int64(3 + i)))
			assert.Equal(t, 0, expectedIndex.Cmp(block.Index.Int), "Block %d should have index %d", i, 3+i)
		}
	})

	t.Run("should return all blocks when range covers all", func(t *testing.T) {
		fromBlock := api.NewBigInt(big.NewInt(1))
		toBlock := api.NewBigInt(big.NewInt(10))

		blocks, err := storage.GetRange(ctx, fromBlock, toBlock)
		require.NoError(t, err, "GetRange should not return an error")
		require.Len(t, blocks, 10, "Should return all 10 blocks")

		// Verify ascending order
		for i, block := range blocks {
			expectedIndex := api.NewBigInt(big.NewInt(int64(1 + i)))
			assert.Equal(t, 0, expectedIndex.Cmp(block.Index.Int), "Block should be in ascending order")
		}
	})

	t.Run("should handle partial overlap range", func(t *testing.T) {
		fromBlock := api.NewBigInt(big.NewInt(8))
		toBlock := api.NewBigInt(big.NewInt(15)) // extends beyond stored blocks

		blocks, err := storage.GetRange(ctx, fromBlock, toBlock)
		require.NoError(t, err, "GetRange should not return an error")
		require.Len(t, blocks, 3, "Should return 3 blocks (8,9,10)")

		expectedIndices := []int64{8, 9, 10}
		for i, block := range blocks {
			expectedIndex := api.NewBigInt(big.NewInt(expectedIndices[i]))
			assert.Equal(t, 0, expectedIndex.Cmp(block.Index.Int), "Index should match")
		}
	})

	t.Run("should handle inverted range (from > to)", func(t *testing.T) {
		fromBlock := api.NewBigInt(big.NewInt(10))
		toBlock := api.NewBigInt(big.NewInt(5))

		blocks, err := storage.GetRange(ctx, fromBlock, toBlock)
		require.NoError(t, err, "GetRange should not return an error for inverted range")
		assert.Empty(t, blocks, "Should return empty slice for inverted range")
	})

	t.Run("should handle large number ranges", func(t *testing.T) {
		// Store a block with a very large number
		largeIndex := new(big.Int)
		largeIndex.SetString("999999999999999999999", 10)
		largeIndexBigInt := api.NewBigInt(largeIndex)

		largeBlock := createTestBlock(largeIndexBigInt)
		err := storage.Store(ctx, largeBlock)
		require.NoError(t, err, "Should store large number block")

		// Query range around the large number
		fromBlock := largeIndexBigInt
		toBlock := largeIndexBigInt

		blocks, err := storage.GetRange(ctx, fromBlock, toBlock)
		require.NoError(t, err, "GetRange should not return an error for large numbers")
		require.Len(t, blocks, 1, "Should return the large number block")

		assert.Equal(t, 0, largeIndexBigInt.Cmp(blocks[0].Index.Int), "Large index should match")
	})
}
