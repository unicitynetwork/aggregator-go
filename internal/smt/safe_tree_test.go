package smt

import (
	"math/big"
	"sync"
	"testing"
	"time"
)

func TestSafeSparseMerkleTreeBasicOperations(t *testing.T) {
	tree := NewSafeSparseMerkleTree(SHA256)
	
	path := big.NewInt(42)
	value := []byte("test value")
	
	// Test AddLeaf
	err := tree.AddLeaf(path, value)
	if err != nil {
		t.Fatalf("AddLeaf failed: %v", err)
	}
	
	// Test GetLeaf
	leaf, err := tree.GetLeaf(path)
	if err != nil {
		t.Fatalf("GetLeaf failed: %v", err)
	}
	
	if leaf.Path.Cmp(path) != 0 {
		t.Errorf("Expected path %v, got %v", path, leaf.Path)
	}
	
	if string(leaf.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", value, leaf.Value)
	}
	
	// Test GetRootHash
	rootHash := tree.GetRootHash()
	if len(rootHash) != 32 {
		t.Errorf("Expected root hash length 32, got %d", len(rootHash))
	}
}

func TestSafeSparseMerkleTreeConcurrentReads(t *testing.T) {
	tree := NewSafeSparseMerkleTree(SHA256)
	
	// Add some initial data
	leaves := []*Leaf{
		NewLeaf(big.NewInt(1), []byte("value1")),
		NewLeaf(big.NewInt(2), []byte("value2")),
		NewLeaf(big.NewInt(3), []byte("value3")),
	}
	
	err := tree.AddLeaves(leaves)
	if err != nil {
		t.Fatalf("AddLeaves failed: %v", err)
	}
	
	// Test concurrent reads
	const numGoroutines = 10
	const numOperations = 100
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	errChan := make(chan error, numGoroutines*numOperations)
	
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			for j := 0; j < numOperations; j++ {
				// Perform various read operations
				switch j % 4 {
				case 0:
					_, err := tree.GetLeaf(big.NewInt(1))
					if err != nil {
						errChan <- err
						return
					}
				case 1:
					_ = tree.GetRootHash()
				case 2:
					_, err := tree.GenerateInclusionProof(big.NewInt(2))
					if err != nil {
						errChan <- err
						return
					}
				case 3:
					_ = tree.Stats()
				}
			}
		}(i)
	}
	
	wg.Wait()
	close(errChan)
	
	// Check for any errors
	for err := range errChan {
		t.Errorf("Concurrent read operation failed: %v", err)
	}
}

func TestSafeSparseMerkleTreeConcurrentWrites(t *testing.T) {
	tree := NewSafeSparseMerkleTree(SHA256)
	
	const numGoroutines = 5
	const numOperations = 20
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	errChan := make(chan error, numGoroutines*numOperations)
	
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			for j := 0; j < numOperations; j++ {
				// Each goroutine writes to different paths to avoid conflicts
				path := big.NewInt(int64(goroutineID*numOperations + j))
				value := []byte("value-" + path.String())
				
				err := tree.AddLeaf(path, value)
				if err != nil {
					errChan <- err
					return
				}
			}
		}(i)
	}
	
	wg.Wait()
	close(errChan)
	
	// Check for any errors
	for err := range errChan {
		t.Errorf("Concurrent write operation failed: %v", err)
	}
	
	// Verify all data was written correctly
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOperations; j++ {
			path := big.NewInt(int64(i*numOperations + j))
			expectedValue := "value-" + path.String()
			
			leaf, err := tree.GetLeaf(path)
			if err != nil {
				t.Errorf("Failed to retrieve leaf at path %v: %v", path, err)
				continue
			}
			
			if string(leaf.Value) != expectedValue {
				t.Errorf("Expected value %s, got %s", expectedValue, leaf.Value)
			}
		}
	}
}

func TestSafeSparseMerkleTreeMixedOperations(t *testing.T) {
	tree := NewSafeSparseMerkleTree(SHA256)
	
	// Add initial data
	err := tree.AddLeaf(big.NewInt(100), []byte("initial"))
	if err != nil {
		t.Fatalf("AddLeaf failed: %v", err)
	}
	
	const numGoroutines = 6
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	errChan := make(chan error, numGoroutines*10)
	
	// Readers
	for i := 0; i < 3; i++ {
		go func(readerID int) {
			defer wg.Done()
			
			for j := 0; j < 50; j++ {
				switch j % 3 {
				case 0:
					tree.GetRootHash()
				case 1:
					tree.GetLeaf(big.NewInt(100))
				case 2:
					tree.Stats()
				}
				time.Sleep(time.Microsecond)
			}
		}(i)
	}
	
	// Writers
	for i := 0; i < 3; i++ {
		go func(writerID int) {
			defer wg.Done()
			
			for j := 0; j < 10; j++ {
				path := big.NewInt(int64(writerID*100 + j))
				value := []byte("writer-" + path.String())
				
				err := tree.AddLeaf(path, value)
				if err != nil {
					errChan <- err
					return
				}
				time.Sleep(time.Millisecond)
			}
		}(i)
	}
	
	wg.Wait()
	close(errChan)
	
	// Check for any errors
	for err := range errChan {
		t.Errorf("Mixed operation failed: %v", err)
	}
}

func TestSafeSparseMerkleTreeClone(t *testing.T) {
	tree := NewSafeSparseMerkleTree(SHA256)
	
	// Add some data
	leaves := []*Leaf{
		NewLeaf(big.NewInt(1), []byte("value1")),
		NewLeaf(big.NewInt(2), []byte("value2")),
		NewLeaf(big.NewInt(3), []byte("value3")),
	}
	
	err := tree.AddLeaves(leaves)
	if err != nil {
		t.Fatalf("AddLeaves failed: %v", err)
	}
	
	// Clone the tree
	clonedTree := tree.Clone()
	
	// Verify both trees have the same root hash
	originalHash := tree.GetRootHash()
	clonedHash := clonedTree.GetRootHash()
	
	if !bytesEqual(originalHash, clonedHash) {
		t.Errorf("Cloned tree has different root hash")
	}
	
	// Verify cloned tree has the same data
	for _, leaf := range leaves {
		originalLeaf, err := tree.GetLeaf(leaf.Path)
		if err != nil {
			t.Fatalf("Failed to get leaf from original tree: %v", err)
		}
		
		clonedLeaf, err := clonedTree.GetLeaf(leaf.Path)
		if err != nil {
			t.Fatalf("Failed to get leaf from cloned tree: %v", err)
		}
		
		if originalLeaf.Path.Cmp(clonedLeaf.Path) != 0 {
			t.Errorf("Paths differ in cloned tree")
		}
		
		if string(originalLeaf.Value) != string(clonedLeaf.Value) {
			t.Errorf("Values differ in cloned tree")
		}
	}
	
	// Modify original tree and verify clone is unaffected
	err = tree.AddLeaf(big.NewInt(999), []byte("new value"))
	if err != nil {
		t.Fatalf("AddLeaf to original tree failed: %v", err)
	}
	
	// Clone should not have the new leaf
	_, err = clonedTree.GetLeaf(big.NewInt(999))
	if err == nil {
		t.Errorf("Cloned tree should not have new leaf added to original")
	}
}

func TestSafeSparseMerkleTreeWithLocks(t *testing.T) {
	tree := NewSafeSparseMerkleTree(SHA256)
	
	// Test WithReadLock
	var rootHash []byte
	tree.WithReadLock(func(unsafeTree *SparseMerkleTree) {
		rootHash = unsafeTree.GetRootHash()
	})
	
	if len(rootHash) != 32 {
		t.Errorf("Expected root hash length 32, got %d", len(rootHash))
	}
	
	// Test WithWriteLock
	tree.WithWriteLock(func(unsafeTree *SparseMerkleTree) {
		err := unsafeTree.AddLeaf(big.NewInt(42), []byte("test"))
		if err != nil {
			t.Errorf("AddLeaf in WithWriteLock failed: %v", err)
		}
	})
	
	// Verify the leaf was added
	leaf, err := tree.GetLeaf(big.NewInt(42))
	if err != nil {
		t.Fatalf("GetLeaf failed: %v", err)
	}
	
	if string(leaf.Value) != "test" {
		t.Errorf("Expected value 'test', got %s", leaf.Value)
	}
}

func TestSafeSparseMerkleTreeStats(t *testing.T) {
	tree := NewSafeSparseMerkleTree(SHA256)
	
	// Check stats for empty tree
	stats := tree.Stats()
	if stats.TotalLeaves != 0 {
		t.Errorf("Expected 0 leaves in empty tree, got %d", stats.TotalLeaves)
	}
	
	// Add some leaves
	leaves := []*Leaf{
		NewLeaf(big.NewInt(1), []byte("value1")),
		NewLeaf(big.NewInt(2), []byte("value2")),
		NewLeaf(big.NewInt(4), []byte("value4")),
		NewLeaf(big.NewInt(8), []byte("value8")),
	}
	
	err := tree.AddLeaves(leaves)
	if err != nil {
		t.Fatalf("AddLeaves failed: %v", err)
	}
	
	// Check stats after adding leaves
	stats = tree.Stats()
	if stats.TotalLeaves != len(leaves) {
		t.Errorf("Expected %d leaves, got %d", len(leaves), stats.TotalLeaves)
	}
	
	if stats.TotalNodes == 0 {
		t.Errorf("Expected non-zero total nodes")
	}
	
	if stats.MaxDepth == 0 {
		t.Errorf("Expected non-zero max depth")
	}
}

func BenchmarkSafeSparseMerkleTreeAddLeaf(b *testing.B) {
	tree := NewSafeSparseMerkleTree(SHA256)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := big.NewInt(int64(i))
		value := []byte("value")
		tree.AddLeaf(path, value)
	}
}

func BenchmarkSafeSparseMerkleTreeGetLeaf(b *testing.B) {
	tree := NewSafeSparseMerkleTree(SHA256)
	
	// Pre-populate tree
	for i := 0; i < 1000; i++ {
		path := big.NewInt(int64(i))
		value := []byte("value")
		tree.AddLeaf(path, value)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := big.NewInt(int64(i % 1000))
		tree.GetLeaf(path)
	}
}

func BenchmarkSafeSparseMerkleTreeGetRootHash(b *testing.B) {
	tree := NewSafeSparseMerkleTree(SHA256)
	
	// Pre-populate tree
	for i := 0; i < 100; i++ {
		path := big.NewInt(int64(i))
		value := []byte("value")
		tree.AddLeaf(path, value)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.GetRootHash()
	}
}