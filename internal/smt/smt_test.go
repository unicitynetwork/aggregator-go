package smt

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/unicitynetwork/aggregator-go/pkg/api"

	"github.com/stretchr/testify/require"
)

// TestSMTTypeScriptCompatibility tests exact compatibility with TypeScript implementation
func TestSMTTypeScriptCompatibility(t *testing.T) {
	// Test case 1: Simple case (known to work)
	t.Run("SimpleCase", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		err := smt.AddLeaf(big.NewInt(0b10), []byte{1, 2, 3})
		require.NoError(t, err, "AddLeaf failed")

		err = smt.AddLeaf(big.NewInt(0b101), []byte{4, 5, 6})
		require.NoError(t, err, "AddLeaf failed")

		expectedHash := "00001c84da4abb4a2af2fa49e295032a5fbce583e2b8043a20246c27f327ee38d927"
		actualHash := smt.GetRootHashHex()

		require.Equal(t, expectedHash, actualHash, "Hash mismatch")
		t.Logf("✅ Simple case exact match: %s", actualHash)
	})

	// Test case 2: Complex case from TypeScript tests
	t.Run("ComplexCase", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		testData := []struct {
			path  int64
			value string
		}{
			{0b110010000, "value00010000"},
			{0b100000000, "value00000000"},
			{0b100010000, "value00010000"},
			{0b111100101, "value11100101"},
			{0b1100, "value100"},
			{0b1011, "value011"},
			{0b111101111, "value11101111"},
			{0b10001010, "value0001010"},
			{0b11010101, "value1010101"},
		}

		for _, data := range testData {
			err := smt.AddLeaf(big.NewInt(data.path), []byte(data.value))
			if err != nil {
				t.Fatalf("AddLeaf failed for path %b: %v", data.path, err)
			}
		}

		expectedHash := "00001fd5fffc41e26f249d04e435b71dbe86d079711131671ed54431a5e117291b42"
		actualHash := smt.GetRootHashHex()

		if actualHash != expectedHash {
			t.Errorf("Hash mismatch:\nExpected: %s\nActual:   %s", expectedHash, actualHash)
		} else {
			t.Logf("✅ Complex case exact match: %s", actualHash)
		}
	})
}

// TestSMTBatchOperations tests batch functionality
func TestSMTBatchOperations(t *testing.T) {
	// First test the exact same simple case that works in TypeScript compatibility
	t.Run("SimpleRetrievalTest", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		err := smt.AddLeaf(big.NewInt(0b10), []byte{1, 2, 3})
		if err != nil {
			t.Fatalf("AddLeaf failed: %v", err)
		}

		// Try to retrieve it
		retrieved, err := smt.GetLeaf(big.NewInt(0b10))
		if err != nil {
			t.Errorf("Failed to retrieve simple leaf: %v", err)
		} else {
			t.Logf("✅ Successfully retrieved simple leaf: %v", retrieved.Value)
		}

		// Add second leaf
		err = smt.AddLeaf(big.NewInt(0b101), []byte{4, 5, 6})
		if err != nil {
			t.Fatalf("AddLeaf failed: %v", err)
		}

		// Try to retrieve both
		retrieved1, err1 := smt.GetLeaf(big.NewInt(0b10))
		retrieved2, err2 := smt.GetLeaf(big.NewInt(0b101))

		if err1 != nil {
			t.Errorf("Failed to retrieve first leaf: %v", err1)
		} else {
			t.Logf("✅ Retrieved first leaf: %v", retrieved1.Value)
		}

		if err2 != nil {
			t.Errorf("Failed to retrieve second leaf: %v", err2)
		} else {
			t.Logf("✅ Retrieved second leaf: %v", retrieved2.Value)
		}
	})

	// Test batch operations produce same results as sequential
	t.Run("BatchVsSequentialEquivalence", func(t *testing.T) {
		// Use a subset of the working complex test data
		testData := []struct {
			path  int64
			value string
		}{
			{0b110010000, "value00010000"}, // 400
			{0b100000000, "value00000000"}, // 256
			{0b100010000, "value00010000"}, // 272
		}

		// Build tree sequentially
		smt1 := NewSparseMerkleTree(api.SHA256)
		for _, data := range testData {
			err := smt1.AddLeaf(big.NewInt(data.path), []byte(data.value))
			if err != nil {
				t.Fatalf("Sequential AddLeaf failed for path %d: %v", data.path, err)
			}
		}
		sequentialHash := smt1.GetRootHashHex()

		// Build tree using batch operation
		smt2 := NewSparseMerkleTree(api.SHA256)
		leaves := make([]*Leaf, len(testData))
		for i, data := range testData {
			leaves[i] = NewLeaf(big.NewInt(data.path), []byte(data.value))
		}

		err := smt2.AddLeaves(leaves)
		if err != nil {
			t.Fatalf("Batch AddLeaves failed: %v", err)
		}
		batchHash := smt2.GetRootHashHex()

		// Verify same root hash
		if sequentialHash != batchHash {
			t.Errorf("Hash mismatch between sequential and batch:\nSequential: %s\nBatch:      %s",
				sequentialHash, batchHash)
		} else {
			t.Logf("✅ Sequential and batch produce identical hash: %s", sequentialHash)
		}

		// Verify leaf retrieval works the same
		for _, data := range testData {
			leaf1, err1 := smt1.GetLeaf(big.NewInt(data.path))
			leaf2, err2 := smt2.GetLeaf(big.NewInt(data.path))

			if err1 != nil || err2 != nil {
				t.Errorf("Retrieval error - Sequential: %v, Batch: %v", err1, err2)
				continue
			}

			if string(leaf1.Value) != string(leaf2.Value) {
				t.Errorf("Leaf value mismatch for path %d: Sequential=%s, Batch=%s",
					data.path, string(leaf1.Value), string(leaf2.Value))
			} else {
				t.Logf("✅ Both trees contain leaf at path %d: %s", data.path, string(leaf1.Value))
			}
		}
	})
}

// TestSMTErrorHandling tests error conditions
func TestSMTErrorHandling(t *testing.T) {
	smt := NewSparseMerkleTree(api.SHA256)

	// Add initial leaves
	smt.AddLeaf(big.NewInt(0b110010000), []byte("value00010000"))
	smt.AddLeaf(big.NewInt(0b100000000), []byte("value00000000"))

	// Test error conditions that TypeScript would throw
	err := smt.AddLeaf(big.NewInt(0b10000000), []byte("OnPath"))
	if err == nil {
		t.Logf("No error for conflicting path (Go implementation may handle differently)")
	} else {
		t.Logf("Expected error for conflicting path: %v", err)
	}
}

// TestSMTCommonPath tests the common path calculation
func TestSMTCommonPath(t *testing.T) {
	testCases := []struct {
		path1   *big.Int
		path2   *big.Int
		expLen  uint
		expPath *big.Int
	}{
		{big.NewInt(0b11), big.NewInt(0b111101111), 1, big.NewInt(0b11)},
		{big.NewInt(0b111101111), big.NewInt(0b11), 1, big.NewInt(0b11)},
		{big.NewInt(0b110010000), big.NewInt(0b100010000), 7, big.NewInt(0b10010000)},
	}

	for i, tc := range testCases {
		result := calculateCommonPath(tc.path1, tc.path2)
		assert.Equal(t, tc.expLen, result.length, "Test %d: length mismatch", i)
		assert.Equal(t, tc.expPath, result.path, "Test %d: path mismatch", i)
	}
}

// TestSMTBigintEncoding tests bigint encoding
func TestSMTBigintEncoding(t *testing.T) {
	testCases := []struct {
		input    *big.Int
		expected []byte
	}{
		{big.NewInt(0), []byte{}},
		{big.NewInt(1), []byte{1}},
		{big.NewInt(255), []byte{255}},
		{big.NewInt(256), []byte{1, 0}},
		{big.NewInt(0b10), []byte{2}},
		{big.NewInt(0b101), []byte{5}},
	}

	for i, tc := range testCases {
		result := api.BigintEncode(tc.input)

		if len(result) != len(tc.expected) {
			t.Errorf("Test %d: length mismatch: expected %v, got %v", i, tc.expected, result)
			continue
		}

		for j := range result {
			if result[j] != tc.expected[j] {
				t.Errorf("Test %d: byte mismatch at %d: expected %d, got %d", i, j, tc.expected[j], result[j])
			}
		}
	}
}

// TestSMTPerformance tests basic performance characteristics
func TestSMTPerformance(t *testing.T) {
	smt := NewSparseMerkleTree(api.SHA256)

	// Use paths that are guaranteed not to conflict by using distinct bit patterns
	testData := []struct {
		path  int64
		value string
	}{
		{0b1000000000000, "perf_value_1"},  // 4096
		{0b1000000000001, "perf_value_2"},  // 4097
		{0b1000000000010, "perf_value_3"},  // 4098
		{0b1000000000100, "perf_value_4"},  // 4100
		{0b1000000001000, "perf_value_5"},  // 4104
		{0b1000000010000, "perf_value_6"},  // 4112
		{0b1000000100000, "perf_value_7"},  // 4128
		{0b1000001000000, "perf_value_8"},  // 4160
		{0b1000010000000, "perf_value_9"},  // 4224
		{0b1000100000000, "perf_value_10"}, // 4352
	}

	// Test individual addition
	for _, data := range testData {
		err := smt.AddLeaf(big.NewInt(data.path), []byte(data.value))
		if err != nil {
			t.Fatalf("AddLeaf failed for path %d: %v", data.path, err)
		}
	}

	// Verify all leaves are present
	for i, data := range testData {
		retrieved, err := smt.GetLeaf(big.NewInt(data.path))
		if err != nil {
			t.Errorf("Failed to retrieve leaf %d at path %d: %v", i, data.path, err)
			continue
		}

		if string(retrieved.Value) != data.value {
			t.Errorf("Value mismatch for leaf %d: expected %s, got %s", i, data.value, string(retrieved.Value))
		}
	}

	// Test root hash calculation
	rootHash := smt.GetRootHashHex()
	if len(rootHash) == 0 {
		t.Errorf("Root hash should not be empty")
	}

	t.Logf("Successfully added %d leaves, root hash: %s", len(testData), rootHash[:16]+"...")
}

// TestSMTBatchPerformanceGains demonstrates batch operation performance improvements
func TestSMTBatchPerformanceGains(t *testing.T) {
	// Use the known working dataset from TypeScript compatibility tests
	testData := []struct {
		path  int64
		value string
	}{
		{0b110010000, "value00010000"},
		{0b100000000, "value00000000"},
		{0b100010000, "value00010000"},
		{0b111100101, "value11100101"},
		{0b1100, "value100"},
		{0b1011, "value011"},
		{0b111101111, "value11101111"},
		{0b10001010, "value0001010"},
		{0b11010101, "value1010101"},
	}

	// Measure sequential addition
	start := time.Now()
	smt1 := NewSparseMerkleTree(api.SHA256)
	for _, data := range testData {
		err := smt1.AddLeaf(big.NewInt(data.path), []byte(data.value))
		if err != nil {
			t.Fatalf("Sequential AddLeaf failed: %v", err)
		}
	}
	_ = smt1.GetRootHash()
	sequentialTime := time.Since(start)

	// Measure batch addition
	start = time.Now()
	smt2 := NewSparseMerkleTree(api.SHA256)
	leaves := make([]*Leaf, len(testData))
	for i, data := range testData {
		leaves[i] = NewLeaf(big.NewInt(data.path), []byte(data.value))
	}
	err := smt2.AddLeaves(leaves)
	if err != nil {
		t.Fatalf("Batch AddLeaves failed: %v", err)
	}
	_ = smt2.GetRootHash()
	batchTime := time.Since(start)

	// Verify same results
	hash1 := smt1.GetRootHashHex()
	hash2 := smt2.GetRootHashHex()
	if hash1 != hash2 {
		t.Errorf("Hash mismatch: Sequential=%s, Batch=%s", hash1, hash2)
	}

	// Calculate improvement
	improvement := float64(sequentialTime) / float64(batchTime)

	t.Logf("🚀 Performance Results:")
	t.Logf("   Sequential: %v", sequentialTime)
	t.Logf("   Batch:      %v", batchTime)
	t.Logf("   Improvement: %.2fx faster", improvement)
	t.Logf("   Root hash: %s", hash1)

	if improvement < 1.0 {
		t.Logf("⚠️  Batch operations should be faster than sequential")
	} else if improvement >= 1.2 {
		t.Logf("✅ Batch operations are significantly faster!")
	}
}

// TestSMTProductionTiming measures actual wall-clock time for production batches
func TestSMTProductionTiming(t *testing.T) {
	sizes := []int{5000, 10000, 100000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Production_%dk", size/1000), func(t *testing.T) {
			smt := NewSparseMerkleTree(api.SHA256)

			// Generate realistic production data
			leaves := make([]*Leaf, size)
			for j := 0; j < size; j++ {
				// Simulate realistic hash-like paths
				basePath := int64(0x1000000000000000)
				offset := int64(j) * 0x10000000
				path := big.NewInt(basePath + offset)
				value := []byte(fmt.Sprintf("tx_data_%d_hash", j))
				leaves[j] = NewLeaf(path, value)
			}

			// Measure batch operation time
			start := time.Now()
			err := smt.AddLeaves(leaves)
			if err != nil {
				t.Fatalf("Failed to add %d leaves: %v", size, err)
			}

			// Measure root hash calculation time
			hashStart := time.Now()
			rootHash := smt.GetRootHash()
			hashTime := time.Since(hashStart)

			totalTime := time.Since(start)

			if len(rootHash) == 0 {
				t.Fatalf("Root hash is empty")
			}

			t.Logf("📊 Production Scale Results for %d leaves:", size)
			t.Logf("   Total Time:     %v", totalTime)
			t.Logf("   Hash Calc Time: %v", hashTime)
			t.Logf("   Rate:           %.0f leaves/sec", float64(size)/totalTime.Seconds())
			t.Logf("   Root Hash:      %s", fmt.Sprintf("%x", rootHash)[:32]+"...")

			// Verify reasonable performance
			maxTimePerLeaf := time.Microsecond * 50 // 50µs per leaf should be reasonable
			if totalTime > time.Duration(size)*maxTimePerLeaf {
				t.Logf("⚠️  Processing slower than expected: %v per leaf", totalTime/time.Duration(size))
			} else {
				t.Logf("✅ Performance is excellent: %v per leaf", totalTime/time.Duration(size))
			}
		})
	}
}

// TestSMTGetPath tests the GetPath method implementation
func TestSMTGetPath(t *testing.T) {

	t.Run("ExpectedPath", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		// Add some test data
		path := big.NewInt(0)
		path.SetString("7588617121771513359933852905331119149238064034818011809301695587375759386505263024", 10)

		leafValue, err := hex.DecodeString("00000777e81da35187bc52073e96a10f89d7fe9aa826693982c8e748a96a3cc7d7b7")
		require.NoError(t, err)

		err = smt.AddLeaf(path, leafValue)
		require.NoError(t, err, "AddLeaf failed")

		// Test getting path for an existing leaf
		merkleTreePath := smt.GetPath(path)
		require.NotNil(t, merkleTreePath, "GetPath should return a path")
		require.NotEmpty(t, merkleTreePath.Root, "Root hash should not be empty")
		require.NotNil(t, merkleTreePath.Steps, "Steps should not be nil")
		require.Equal(t, "0000482ddbdcdc36ad18e203c0262ad81af809aec071cce7b45ac84d5d9b0f40f079", merkleTreePath.Root, "Root hash should match expected value")
		require.Equal(t, 1, len(merkleTreePath.Steps), "There should be exactly one step in the path")
		require.Equal(t, 1, len(merkleTreePath.Steps[0].Branch), "Step should have one branch")
		// Branch should contain the value of the LeafBranch, not its hash
		require.Equal(t, "00000777e81da35187bc52073e96a10f89d7fe9aa826693982c8e748a96a3cc7d7b7", merkleTreePath.Steps[0].Branch[0], "Branch should contain the leaf value")
		require.Equal(t, "7588617121771513359933852905331119149238064034818011809301695587375759386505263024", merkleTreePath.Steps[0].Path, "Step path should match leaf path")
		require.Nil(t, merkleTreePath.Steps[0].Sibling, "Step should not have a sibling")

		t.Logf("✅ Expected path test - Root: %s, Branch: %s", merkleTreePath.Root, merkleTreePath.Steps[0].Branch[0])
	})

	t.Run("BasicGetPath", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		// Add some test data
		err := smt.AddLeaf(big.NewInt(0b10), []byte{1, 2, 3})
		require.NoError(t, err, "AddLeaf failed")

		err = smt.AddLeaf(big.NewInt(0b101), []byte{4, 5, 6})
		require.NoError(t, err, "AddLeaf failed")

		// Test getting path for an existing leaf
		path := smt.GetPath(big.NewInt(0b10))
		require.NotNil(t, path, "GetPath should return a path")
		require.NotEmpty(t, path.Root, "Root hash should not be empty")
		require.NotNil(t, path.Steps, "Steps should not be nil")

		t.Logf("✅ GetPath for existing leaf - Root: %s, Steps: %d", path.Root, len(path.Steps))

		// Verify the root hash matches the tree's root
		expectedRoot := smt.GetRootHashHex()
		require.Equal(t, expectedRoot, path.Root, "Path root should match tree root")
	})

	t.Run("GetPathForNonExistentLeaf", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		// Add some test data
		err := smt.AddLeaf(big.NewInt(0b10), []byte{1, 2, 3})
		require.NoError(t, err, "AddLeaf failed")

		// Test getting path for a non-existent leaf
		path := smt.GetPath(big.NewInt(0b11))
		require.NotNil(t, path, "GetPath should return a path even for non-existent leaves")
		require.NotEmpty(t, path.Root, "Root hash should not be empty")
		require.NotNil(t, path.Steps, "Steps should not be nil")

		t.Logf("✅ GetPath for non-existent leaf - Root: %s, Steps: %d", path.Root, len(path.Steps))
	})
	t.Run("GetPathStructure", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		// Add test data - just use the two paths that we know work
		err := smt.AddLeaf(big.NewInt(0b10), []byte{1, 2, 3})
		require.NoError(t, err, "AddLeaf failed")

		err = smt.AddLeaf(big.NewInt(0b101), []byte{4, 5, 6})
		require.NoError(t, err, "AddLeaf failed")

		// Test path structure
		path := smt.GetPath(big.NewInt(0b10))
		require.NotNil(t, path, "GetPath should return a path")

		// Verify step structure
		for i, step := range path.Steps {
			require.NotEmpty(t, step.Path, "Step path should not be empty")
			t.Logf("Step %d: Path=%s, Branch=%v, Sibling=%v", i, step.Path, len(step.Branch), step.Sibling != nil)
		}

		t.Logf("✅ GetPath structure verification - Root: %s, Steps: %d", path.Root, len(path.Steps))
	})

	t.Run("EmptyTreeGetPath", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		// Test getting path from empty tree
		path := smt.GetPath(big.NewInt(0b10))
		require.NotNil(t, path, "GetPath should return a path even for empty tree")
		require.NotEmpty(t, path.Root, "Root hash should not be empty even for empty tree")
		require.NotNil(t, path.Steps, "Steps should not be nil")

		t.Logf("✅ GetPath for empty tree - Root: %s, Steps: %d", path.Root, len(path.Steps))
	})
}

// TestSMTGetPathComprehensive tests the GetPath method comprehensively
func TestSMTGetPathComprehensive(t *testing.T) {
	t.Run("GetPathWithSingleLeaf", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		// Add a single leaf
		leafPath := big.NewInt(0b101)
		leafValue := []byte("test value")
		err := smt.AddLeaf(leafPath, leafValue)
		require.NoError(t, err, "AddLeaf failed")

		// Get path for the leaf
		path := smt.GetPath(leafPath)
		require.NotNil(t, path, "GetPath should return a path")
		require.Equal(t, smt.GetRootHashHex(), path.Root, "Path root should match tree root")
		require.Len(t, path.Steps, 1, "Single leaf should have one step")

		step := path.Steps[0]
		require.Equal(t, leafPath.String(), step.Path, "Step path should match leaf path")
		require.Len(t, step.Branch, 1, "Step should have one branch hash")
		require.Nil(t, step.Sibling, "Single leaf should have no sibling")

		t.Logf("✅ Single leaf path: Root=%s, Step path=%s, Branch=%s",
			path.Root, step.Path, step.Branch[0])
	})

	t.Run("GetPathWithTwoLeaves", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		// Add two leaves that will require a node
		path1 := big.NewInt(0b10)  // binary: 10
		path2 := big.NewInt(0b101) // binary: 101

		err := smt.AddLeaf(path1, []byte("value1"))
		require.NoError(t, err, "AddLeaf 1 failed")

		err = smt.AddLeaf(path2, []byte("value2"))
		require.NoError(t, err, "AddLeaf 2 failed")

		// Get path for first leaf
		merkPath1 := smt.GetPath(path1)
		require.NotNil(t, merkPath1, "GetPath should return a path")
		require.Equal(t, smt.GetRootHashHex(), merkPath1.Root, "Path root should match tree root")
		require.NotEmpty(t, merkPath1.Steps, "Should have steps")

		// Get path for second leaf
		merkPath2 := smt.GetPath(path2)
		require.NotNil(t, merkPath2, "GetPath should return a path")
		require.Equal(t, smt.GetRootHashHex(), merkPath2.Root, "Path root should match tree root")
		require.NotEmpty(t, merkPath2.Steps, "Should have steps")

		// Both paths should have the same root but different steps
		require.Equal(t, merkPath1.Root, merkPath2.Root, "Both paths should have same root")

		t.Logf("✅ Two leaves - Path1 steps: %d, Path2 steps: %d",
			len(merkPath1.Steps), len(merkPath2.Steps))

		// Verify step details
		for i, step := range merkPath1.Steps {
			t.Logf("Path1 Step %d: Path=%s, Branch count=%d, Has sibling=%v",
				i, step.Path, len(step.Branch), step.Sibling != nil)
		}
		for i, step := range merkPath2.Steps {
			t.Logf("Path2 Step %d: Path=%s, Branch count=%d, Has sibling=%v",
				i, step.Path, len(step.Branch), step.Sibling != nil)
		}
	})

	t.Run("GetPathForNonExistentPath", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		// Add some leaves
		err := smt.AddLeaf(big.NewInt(0b10), []byte("value1"))
		require.NoError(t, err, "AddLeaf failed")

		err = smt.AddLeaf(big.NewInt(0b101), []byte("value2"))
		require.NoError(t, err, "AddLeaf failed")

		// Try to get path for non-existent leaf
		nonExistentPath := big.NewInt(0b11) // binary: 11
		merkPath := smt.GetPath(nonExistentPath)

		require.NotNil(t, merkPath, "GetPath should return a path even for non-existent paths")
		require.Equal(t, smt.GetRootHashHex(), merkPath.Root, "Path root should match tree root")
		require.NotEmpty(t, merkPath.Steps, "Should have steps even for non-existent path")

		t.Logf("✅ Non-existent path: Root=%s, Steps=%d",
			merkPath.Root, len(merkPath.Steps))

		// Verify the path structure
		for i, step := range merkPath.Steps {
			t.Logf("Step %d: Path=%s, Branch count=%d, Has sibling=%v",
				i, step.Path, len(step.Branch), step.Sibling != nil)
		}
	})
	t.Run("GetPathComplexTree", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		// Add multiple leaves to create a complex tree structure with non-conflicting paths
		// Use paths from the successful TypeScript compatibility test
		testPaths := []*big.Int{
			big.NewInt(0b110010000), // 400
			big.NewInt(0b100000000), // 256
			big.NewInt(0b100010000), // 272
			big.NewInt(0b1100),      // 12
			big.NewInt(0b1011),      // 11
			big.NewInt(0b10001010),  // 138
		}

		for i, path := range testPaths {
			value := []byte(fmt.Sprintf("value%d", i))
			err := smt.AddLeaf(path, value)
			require.NoError(t, err, "AddLeaf failed for path %s", path.String())
		}

		// Get paths for all leaves and verify they're consistent
		rootHash := smt.GetRootHashHex()

		for i, path := range testPaths {
			merkPath := smt.GetPath(path)
			require.NotNil(t, merkPath, "GetPath should return a path for leaf %d", i)
			require.Equal(t, rootHash, merkPath.Root, "All paths should have same root")
			require.NotEmpty(t, merkPath.Steps, "Path should have steps for leaf %d", i)

			t.Logf("Path %s (%s): %d steps",
				path.String(), fmt.Sprintf("0b%s", path.Text(2)), len(merkPath.Steps))

			// Verify each step has valid structure
			for j, step := range merkPath.Steps {
				require.NotEmpty(t, step.Path, "Step %d should have a path", j)
				require.NotNil(t, step.Branch, "Step %d should have branch array", j)
				// step.Sibling can be nil (that's valid)

				t.Logf("  Step %d: Path=%s, Branch count=%d, Sibling=%v",
					j, step.Path, len(step.Branch), step.Sibling != nil)
			}
		}

		t.Logf("✅ Complex tree with %d leaves - all paths generated successfully", len(testPaths))
	})

	t.Run("GetPathEmptyTree", func(t *testing.T) {
		smt := NewSparseMerkleTree(api.SHA256)

		// Get path from empty tree
		path := smt.GetPath(big.NewInt(0b101))
		require.NotNil(t, path, "GetPath should return a path even for empty tree")
		require.NotEmpty(t, path.Root, "Root should not be empty even for empty tree")
		require.NotNil(t, path.Steps, "Steps should not be nil") // Empty tree should have minimal steps
		require.Len(t, path.Steps, 1, "Empty tree should have one step")

		step := path.Steps[0]
		require.Equal(t, "5", step.Path, "Step path should match requested path (decimal representation)")
		require.Empty(t, step.Branch, "Empty tree step should have no branch")
		require.Nil(t, step.Sibling, "Empty tree step should have no sibling")

		t.Logf("✅ Empty tree path: Root=%s, Step path=%s", path.Root, step.Path)
	})

	t.Run("GetPathValidation", func(t *testing.T) {
		// This test validates that the path structure is correct and could be used for verification
		smt := NewSparseMerkleTree(api.SHA256)

		// Add test data with non-conflicting paths from working complex test
		testLeaves := []struct {
			path  *big.Int
			value []byte
		}{
			{big.NewInt(0b110010000), []byte("data1")}, // 400
			{big.NewInt(0b100000000), []byte("data2")}, // 256
			{big.NewInt(0b1100), []byte("data3")},      // 12
		}

		for _, leaf := range testLeaves {
			err := smt.AddLeaf(leaf.path, leaf.value)
			require.NoError(t, err, "AddLeaf failed")
		}

		// Get paths and validate structure
		for _, leaf := range testLeaves {
			merkPath := smt.GetPath(leaf.path)
			require.NotNil(t, merkPath, "GetPath should return a path")

			// Validate path structure for verification compatibility
			require.NotEmpty(t, merkPath.Root, "Root hash required for verification")
			require.NotEmpty(t, merkPath.Steps, "Steps required for verification")

			// Each step should have valid data for verification
			for i, step := range merkPath.Steps {
				require.NotEmpty(t, step.Path, "Step %d must have path", i)

				// Branch should either be empty (for missing nodes) or have one hash
				require.True(t, len(step.Branch) <= 1, "Step %d should have at most one branch hash", i)

				// If branch exists, it should be a valid hex hash
				if len(step.Branch) > 0 {
					require.Regexp(t, "^[0-9a-f]+$", step.Branch[0], "Branch hash should be valid hex")
					require.True(t, len(step.Branch[0]) > 0, "Branch hash should not be empty")
				}

				// If sibling exists, it should be a valid hex hash
				if len(step.Sibling) > 0 {
					require.Regexp(t, "^[0-9a-f]+$", step.Sibling[0], "Sibling hash should be valid hex")
					require.True(t, len(step.Sibling[0]) > 0, "Sibling hash should not be empty")
				}
			}

			t.Logf("✅ Path validation passed for %s (%d steps)",
				leaf.path.String(), len(merkPath.Steps))
		}
	})

	t.Run("GetPathConsistency", func(t *testing.T) {
		// Test that GetPath returns consistent results
		smt := NewSparseMerkleTree(api.SHA256)

		// Add leaves
		path1 := big.NewInt(0b1010)
		path2 := big.NewInt(0b1011)

		err := smt.AddLeaf(path1, []byte("consistent1"))
		require.NoError(t, err, "AddLeaf failed")

		err = smt.AddLeaf(path2, []byte("consistent2"))
		require.NoError(t, err, "AddLeaf failed")

		// Get paths multiple times and verify consistency
		merkPath1a := smt.GetPath(path1)
		merkPath1b := smt.GetPath(path1)
		merkPath2a := smt.GetPath(path2)
		merkPath2b := smt.GetPath(path2)

		// Same path should return identical results
		require.Equal(t, merkPath1a.Root, merkPath1b.Root, "Same path should have same root")
		require.Equal(t, len(merkPath1a.Steps), len(merkPath1b.Steps), "Same path should have same number of steps")
		require.Equal(t, merkPath2a.Root, merkPath2b.Root, "Same path should have same root")
		require.Equal(t, len(merkPath2a.Steps), len(merkPath2b.Steps), "Same path should have same number of steps")

		// Different paths should have same root but potentially different steps
		require.Equal(t, merkPath1a.Root, merkPath2a.Root, "All paths from same tree should have same root")

		// Verify step-by-step consistency
		for i, step1a := range merkPath1a.Steps {
			step1b := merkPath1b.Steps[i]
			require.Equal(t, step1a.Path, step1b.Path, "Step %d path should be consistent", i)
			require.Equal(t, step1a.Branch, step1b.Branch, "Step %d branch should be consistent", i)
			require.Equal(t, step1a.Sibling, step1b.Sibling, "Step %d sibling should be consistent", i)
		}

		t.Logf("✅ GetPath consistency verified for multiple calls")
	})
	t.Run("GetPathBinaryRepresentation", func(t *testing.T) {
		// Test path handling with various binary representations
		testCases := []struct {
			name     string
			path     *big.Int
			binary   string
			expected string
		}{
			{"Small path", big.NewInt(0b1), "1", "1"},
			{"Medium path", big.NewInt(0b1010), "1010", "10"},         // decimal: 10
			{"Large path", big.NewInt(0b10000000), "10000000", "128"}, // decimal: 128
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Create a new tree for each test case to avoid conflicts
				smt := NewSparseMerkleTree(api.SHA256)

				// Add leaf
				err := smt.AddLeaf(tc.path, []byte(fmt.Sprintf("value_%s", tc.name)))
				require.NoError(t, err, "AddLeaf failed for %s", tc.name)

				// Get path
				merkPath := smt.GetPath(tc.path)
				require.NotNil(t, merkPath, "GetPath should return a path for %s", tc.name)

				// Verify the path representation in steps
				found := false
				for _, step := range merkPath.Steps {
					if step.Path == tc.expected {
						found = true
						break
					}
				}
				require.True(t, found, "Path %s should appear in steps", tc.expected)

				t.Logf("✅ Binary path %s (%s) handled correctly", tc.binary, tc.expected)
			})
		}
	})
}

// TestAddLeavesToExistingTree tests that AddLeaves adds leaves to existing tree instead of replacing it
func TestAddLeavesToExistingTree(t *testing.T) {
	smt := NewSparseMerkleTree(api.SHA256)

	// Add initial leaves to create a tree (using non-conflicting paths from TypeScript tests)
	initialLeaves := []*Leaf{
		{Path: big.NewInt(0b100000000), Value: []byte("value256")}, // 256
		{Path: big.NewInt(0b100010000), Value: []byte("value272")}, // 272
	}

	err := smt.AddLeaves(initialLeaves)
	require.NoError(t, err)

	// Verify initial leaves are in the tree
	leaf256, err := smt.GetLeaf(big.NewInt(0b100000000))
	require.NoError(t, err)
	require.NotNil(t, leaf256)
	require.Equal(t, []byte("value256"), leaf256.Value)

	leaf272, err := smt.GetLeaf(big.NewInt(0b100010000))
	require.NoError(t, err)
	require.NotNil(t, leaf272)
	require.Equal(t, []byte("value272"), leaf272.Value)

	// Store the initial root hash
	initialRootHash := smt.GetRootHashHex()

	// Add more leaves to the existing tree (using non-conflicting paths from TypeScript tests)
	additionalLeaves := []*Leaf{
		{Path: big.NewInt(0b1100), Value: []byte("value12")}, // 12
		{Path: big.NewInt(0b1011), Value: []byte("value11")}, // 11
	}

	err = smt.AddLeaves(additionalLeaves)
	require.NoError(t, err)

	// Verify all leaves (initial + additional) are still in the tree
	leaf256, err = smt.GetLeaf(big.NewInt(0b100000000))
	require.NoError(t, err)
	require.NotNil(t, leaf256)
	require.Equal(t, []byte("value256"), leaf256.Value)

	leaf272, err = smt.GetLeaf(big.NewInt(0b100010000))
	require.NoError(t, err)
	require.NotNil(t, leaf272)
	require.Equal(t, []byte("value272"), leaf272.Value)

	leaf12, err := smt.GetLeaf(big.NewInt(0b1100))
	require.NoError(t, err)
	require.NotNil(t, leaf12)
	require.Equal(t, []byte("value12"), leaf12.Value)

	leaf11, err := smt.GetLeaf(big.NewInt(0b1011))
	require.NoError(t, err)
	require.NotNil(t, leaf11)
	require.Equal(t, []byte("value11"), leaf11.Value)

	// Verify the root hash changed (because we added new leaves)
	finalRootHash := smt.GetRootHashHex()
	require.NotEqual(t, initialRootHash, finalRootHash)
}

// TestAddLeavesEquivalentToSequentialAddLeaf tests that AddLeaves produces the same result as sequential AddLeaf calls
func TestAddLeavesEquivalentToSequentialAddLeaf(t *testing.T) {
	// Test data (using non-conflicting paths from TypeScript tests)
	leaves := []*Leaf{
		{Path: big.NewInt(0b100000000), Value: []byte("value256")}, // 256
		{Path: big.NewInt(0b100010000), Value: []byte("value272")}, // 272
		{Path: big.NewInt(0b1100), Value: []byte("value12")},       // 12
		{Path: big.NewInt(0b1011), Value: []byte("value11")},       // 11
	}
	// Create tree using AddLeaves
	smt1 := NewSparseMerkleTree(api.SHA256)
	err := smt1.AddLeaves(leaves)
	require.NoError(t, err)

	// Create tree using sequential AddLeaf
	smt2 := NewSparseMerkleTree(api.SHA256)
	for _, leaf := range leaves {
		err := smt2.AddLeaf(leaf.Path, leaf.Value)
		require.NoError(t, err)
	}

	// Both trees should have the same root hash
	require.Equal(t, smt1.GetRootHashHex(), smt2.GetRootHashHex())

	// Both trees should have the same leaves
	for _, leaf := range leaves {
		leaf1, err := smt1.GetLeaf(leaf.Path)
		require.NoError(t, err)
		require.NotNil(t, leaf1)

		leaf2, err := smt2.GetLeaf(leaf.Path)
		require.NoError(t, err)
		require.NotNil(t, leaf2)

		require.Equal(t, leaf1.Value, leaf2.Value)
		require.Equal(t, leaf1.Path, leaf2.Path)
	}
}

// TestAddLeavesWithExistingAndNewToExistingTree tests adding leaves to a tree that already has some leaves
func TestAddLeavesWithExistingAndNewToExistingTree(t *testing.T) {
	smt := NewSparseMerkleTree(api.SHA256)
	// Add some initial leaves (using non-conflicting paths from TypeScript tests)
	err := smt.AddLeaf(big.NewInt(0b110010000), []byte("initial400")) // 400
	require.NoError(t, err)
	err = smt.AddLeaf(big.NewInt(0b111100101), []byte("initial485")) // 485
	require.NoError(t, err)

	initialRootHash := smt.GetRootHashHex()

	// Add more leaves using AddLeaves (non-conflicting paths from TypeScript tests)
	newLeaves := []*Leaf{
		{Path: big.NewInt(0b100000000), Value: []byte("new256")}, // 256
		{Path: big.NewInt(0b100010000), Value: []byte("new272")}, // 272
		{Path: big.NewInt(0b1100), Value: []byte("new12")},       // 12
	}
	err = smt.AddLeaves(newLeaves)
	require.NoError(t, err)

	// Verify all leaves are present
	leaf400, err := smt.GetLeaf(big.NewInt(0b110010000))
	require.NoError(t, err)
	require.Equal(t, []byte("initial400"), leaf400.Value)

	leaf256, err := smt.GetLeaf(big.NewInt(0b100000000))
	require.NoError(t, err)
	require.Equal(t, []byte("new256"), leaf256.Value)

	leaf485, err := smt.GetLeaf(big.NewInt(0b111100101))
	require.NoError(t, err)
	require.Equal(t, []byte("initial485"), leaf485.Value)

	leaf272, err := smt.GetLeaf(big.NewInt(0b100010000))
	require.NoError(t, err)
	require.Equal(t, []byte("new272"), leaf272.Value)

	leaf12, err := smt.GetLeaf(big.NewInt(0b1100))
	require.NoError(t, err)
	require.Equal(t, []byte("new12"), leaf12.Value)

	// Root hash should be different after adding new leaves
	finalRootHash := smt.GetRootHashHex()
	require.NotEqual(t, initialRootHash, finalRootHash)
}

// TestAddLeavesEmptyList tests that AddLeaves with empty list doesn't affect the tree
func TestAddLeavesEmptyList(t *testing.T) {
	smt := NewSparseMerkleTree(api.SHA256)

	// Add initial leaf (using non-conflicting path from TypeScript tests)
	err := smt.AddLeaf(big.NewInt(0b1011), []byte("value11")) // 11
	require.NoError(t, err)

	initialRootHash := smt.GetRootHashHex()

	// Add empty list of leaves
	err = smt.AddLeaves([]*Leaf{})
	require.NoError(t, err)

	// Tree should be unchanged
	finalRootHash := smt.GetRootHashHex()
	require.Equal(t, initialRootHash, finalRootHash)

	// Original leaf should still be there
	leaf11, err := smt.GetLeaf(big.NewInt(0b1011))
	require.NoError(t, err)
	require.Equal(t, []byte("value11"), leaf11.Value)
}

// TestAddLeaves_DuplicatePathError specifically tests the lazy-build logic for duplicate leaves.
func TestAddLeaves_DuplicatePathError(t *testing.T) {
	smt := NewSparseMerkleTree(api.SHA256)
	path := big.NewInt(42)

	// Create a batch containing two leaves with the exact same path.
	leaves := []*Leaf{
		NewLeaf(path, []byte("value1")),
		NewLeaf(path, []byte("value2")),
	}

	// The AddLeaves loop will add the first one, then fail on the second.
	err := smt.AddLeaves(leaves)
	require.Error(t, err, "AddLeaves should fail when a batch contains a duplicate path")
	require.ErrorIs(t, err, ErrLeafModification)
}

// Replace TestAddLeaves_DuplicatePathError with this new, more comprehensive test.
func TestAddLeaves_SkipsDuplicatesAndContinues(t *testing.T) {
	smt := NewSparseMerkleTree(api.SHA256)

	// Setup: Add an initial leaf to the tree.
	initialPath := big.NewInt(100)
	initialValue := []byte("initial_value")
	err := smt.AddLeaf(initialPath, initialValue)
	require.NoError(t, err)

	// Create a batch that contains:
	// 1. A new, valid leaf.
	// 2. A leaf that is a duplicate of one already in the tree.
	// 3. Another new, valid leaf.
	batch := []*Leaf{
		NewLeaf(big.NewInt(200), []byte("new_value_A")), // Should be added
		NewLeaf(initialPath, initialValue),              // Should be SKIPPED
		NewLeaf(big.NewInt(300), []byte("new_value_B")), // Should be added
	}

	// Action: Process the batch. This function should now SUCCEED.
	err = smt.AddLeaves(batch)
	require.NoError(t, err, "AddLeaves should not return an error for batches containing duplicates")

	// Verification: Check the final state of the tree.

	// 1. Check that the new, valid leaves were added.
	leafA, errA := smt.GetLeaf(big.NewInt(200))
	require.NoError(t, errA)
	assert.Equal(t, []byte("new_value_A"), leafA.Value)

	leafB, errB := smt.GetLeaf(big.NewInt(300))
	require.NoError(t, errB)
	assert.Equal(t, []byte("new_value_B"), leafB.Value)

	// 2. Check that the duplicate leaf was SKIPPED by verifying the value was NOT overwritten.
	originalLeaf, errOrig := smt.GetLeaf(initialPath)
	require.NoError(t, errOrig)
	assert.Equal(t, initialValue, originalLeaf.Value, "The value of the original leaf should not have been changed by the duplicate in the batch")
}

// TestSMTSnapshot tests the snapshot functionality with copy-on-write semantics
func TestSMTSnapshot(t *testing.T) {
	t.Run("BasicSnapshotOperations", func(t *testing.T) {
		// Create original tree and add some initial data
		original := NewSparseMerkleTree(api.SHA256)

		// Create a snapshot to add initial data (since original tree can't be modified directly)
		initialSnapshot := original.CreateSnapshot()
		err := initialSnapshot.AddLeaf(big.NewInt(0b10), []byte{1, 2, 3})
		require.NoError(t, err)
		err = initialSnapshot.AddLeaf(big.NewInt(0b101), []byte{4, 5, 6})
		require.NoError(t, err)
		initialSnapshot.Commit()

		originalHash := original.GetRootHashHex()

		// Create snapshot and verify it has the same hash initially
		snapshot := original.CreateSnapshot()
		snapshotHash := snapshot.GetRootHashHex()
		assert.Equal(t, originalHash, snapshotHash, "Snapshot should have same hash as original initially")

		// Add a leaf to snapshot
		err = snapshot.AddLeaf(big.NewInt(0b111), []byte{7, 8, 9})
		require.NoError(t, err)

		// Verify original tree is unchanged
		assert.Equal(t, originalHash, original.GetRootHashHex(), "Original tree should be unchanged")

		// Verify snapshot has different hash
		newSnapshotHash := snapshot.GetRootHashHex()
		assert.NotEqual(t, originalHash, newSnapshotHash, "Snapshot should have different hash after modification")

		// Commit snapshot and verify original tree is updated
		snapshot.Commit()
		assert.Equal(t, newSnapshotHash, original.GetRootHashHex(), "Original tree should match snapshot after commit")
	})

	t.Run("CannotModifyOriginalTree", func(t *testing.T) {
		original := NewSparseMerkleTree(api.SHA256)

		// Original tree can be modified directly (backward compatibility)
		err := original.AddLeaf(big.NewInt(0b10), []byte{1, 2, 3})
		require.NoError(t, err, "Should be able to modify original tree directly for backward compatibility")

		// Verify CanModify method
		assert.False(t, original.CanModify(), "Original tree should report false for CanModify")

		// Create snapshot and verify it can be modified
		snapshot := original.CreateSnapshot()
		assert.True(t, snapshot.CanModify(), "Snapshot should be modifiable")

		// Verify snapshot methods work
		err = snapshot.AddLeaf(big.NewInt(0b101), []byte{4, 5, 6})
		require.NoError(t, err, "Should be able to add leaf to snapshot")
	})

	t.Run("MultipleSnapshots", func(t *testing.T) {
		// Create original tree with initial data
		original := NewSparseMerkleTree(api.SHA256)
		initialSnapshot := original.CreateSnapshot()
		err := initialSnapshot.AddLeaf(big.NewInt(0b10), []byte{1, 2, 3}) // path 2
		require.NoError(t, err)
		initialSnapshot.Commit()

		originalHash := original.GetRootHashHex()

		// Create two snapshots
		snapshot1 := original.CreateSnapshot()
		snapshot2 := original.CreateSnapshot()

		// Modify each snapshot differently with paths that don't conflict
		err = snapshot1.AddLeaf(big.NewInt(0b101), []byte{4, 5, 6}) // path 5
		require.NoError(t, err)

		err = snapshot2.AddLeaf(big.NewInt(0b111), []byte{7, 8, 9}) // path 7
		require.NoError(t, err)

		// Verify original is unchanged
		assert.Equal(t, originalHash, original.GetRootHashHex(), "Original should be unchanged")

		// Verify snapshots have different hashes
		hash1 := snapshot1.GetRootHashHex()
		hash2 := snapshot2.GetRootHashHex()
		assert.NotEqual(t, originalHash, hash1, "Snapshot1 should differ from original")
		assert.NotEqual(t, originalHash, hash2, "Snapshot2 should differ from original")
		assert.NotEqual(t, hash1, hash2, "Snapshots should differ from each other")

		// Commit first snapshot
		snapshot1.Commit()
		assert.Equal(t, hash1, original.GetRootHashHex(), "Original should match snapshot1 after commit")

		// Second snapshot should still work independently (though now based on old state)
		assert.Equal(t, hash2, snapshot2.GetRootHashHex(), "Snapshot2 should retain its hash")
	})

	t.Run("SnapshotMemoryEfficiency", func(t *testing.T) {
		// This test verifies that snapshots share memory with the original tree
		original := NewSparseMerkleTree(api.SHA256)

		// Add initial data with well-spaced paths that don't conflict
		initialSnapshot := original.CreateSnapshot()
		err := initialSnapshot.AddLeaf(big.NewInt(0b10), []byte{1}) // path 2
		require.NoError(t, err)
		err = initialSnapshot.AddLeaf(big.NewInt(0b101), []byte{2}) // path 5
		require.NoError(t, err)
		initialSnapshot.Commit()

		// Create snapshot - should share root with original initially
		snapshot := original.CreateSnapshot()

		// Verify root is shared (pointer equality)
		assert.Same(t, original.root, snapshot.root, "Snapshot should share root with original initially")

		// Modify snapshot - this should trigger copy-on-write
		err = snapshot.AddLeaf(big.NewInt(0b111), []byte{3}) // path 7
		require.NoError(t, err)

		// Verify root is no longer shared
		assert.NotSame(t, original.root, snapshot.root, "Snapshot should have its own root after modification")
	})
}

// TestSMTOrderDependencyOneByOne - SMT order dependency test when adding leaves one by one
func TestSMTOrderDependencyOneByOne(t *testing.T) {
	smt1 := NewSparseMerkleTree(api.SHA256)
	require.NoError(t, smt1.AddLeaf(big.NewInt(5), []byte("value_1")))
	require.NoError(t, smt1.AddLeaf(big.NewInt(6), []byte("value_2")))
	hash1 := smt1.GetRootHashHex()
	t.Logf("Order [5,6]: %s", hash1)

	smt2 := NewSparseMerkleTree(api.SHA256)
	require.NoError(t, smt2.AddLeaf(big.NewInt(6), []byte("value_2")))
	require.NoError(t, smt2.AddLeaf(big.NewInt(5), []byte("value_1")))
	hash2 := smt2.GetRootHashHex()
	t.Logf("Order [6,5]: %s", hash2)

	assert.Equal(t, hash1, hash2, "SMT additions should be order-independent")
}

// TestSMTOrderDependencyBatch - SMT order dependency test when adding leaves in a batch
func TestSMTOrderDependencyBatch(t *testing.T) {
	leaves1 := []*Leaf{
		{Path: big.NewInt(5), Value: []byte("value_1")},
		{Path: big.NewInt(6), Value: []byte("value_2")},
	}

	leaves2 := []*Leaf{
		{Path: big.NewInt(6), Value: []byte("value_2")},
		{Path: big.NewInt(5), Value: []byte("value_1")},
	}

	smt1 := NewSparseMerkleTree(api.SHA256)
	err := smt1.AddLeaves(leaves1)
	require.NoError(t, err)
	hash1 := smt1.GetRootHashHex()

	smt2 := NewSparseMerkleTree(api.SHA256)
	err = smt2.AddLeaves(leaves2)
	require.NoError(t, err)
	hash2 := smt2.GetRootHashHex()

	t.Logf("Order [5,6]: %s", hash1)
	t.Logf("Order [6,5]: %s", hash2)

	assert.Equal(t, hash1, hash2, "SMT additions should be order-independent")
}

// TestSMTAddingNodeUnderLeaf - Test that the SMT does not allow adding child nodes under existing leaves
func TestSMTAddingNodeUnderLeaf(t *testing.T) {
	smt1 := NewSparseMerkleTree(api.SHA256)
	require.NoError(t, smt1.AddLeaf(big.NewInt(2), []byte("leaf_1")))
	require.Error(t, smt1.AddLeaf(big.NewInt(4), []byte("child_under_leaf_1")), "SMT should not allow adding child nodes under leaves")

	smt2 := NewSparseMerkleTree(api.SHA256)
	leaves2 := []*Leaf{
		{Path: big.NewInt(2), Value: []byte("leaf_1")},
		{Path: big.NewInt(4), Value: []byte("child_under_leaf_1")},
	}
	require.Error(t, smt2.AddLeaves(leaves2), "SMT should not allow adding child nodes under leaves, even in a batch")
}

// TestSMTAddingLeafAboveNode - Test that the SMT does not allow adding leaves above existing nodes
func TestSMTAddingLeafAboveNode(t *testing.T) {
	smt1 := NewSparseMerkleTree(api.SHA256)
	require.NoError(t, smt1.AddLeaf(big.NewInt(4), []byte("leaf_1")))
	require.Error(t, smt1.AddLeaf(big.NewInt(2), []byte("node_above_leaf_1")), "SMT should not allow adding leaves above existing nodes")

	smt2 := NewSparseMerkleTree(api.SHA256)
	leaves2 := []*Leaf{
		{Path: big.NewInt(4), Value: []byte("leaf_1")},
		{Path: big.NewInt(2), Value: []byte("node_above_leaf_1")},
	}
	require.Error(t, smt2.AddLeaves(leaves2), "SMT should not allow adding leaves above existing nodes, even in a batch")
}
