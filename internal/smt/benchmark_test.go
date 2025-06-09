package smt

import (
	"math/big"
	"testing"
)

// BenchmarkSMTBatchAdd benchmarks adding 1000 leaves at once in a batch operation
func BenchmarkSMTBatchAdd(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := NewSparseMerkleTree(SHA256)
		
		// Prepare 1000 leaves
		leaves := make([]*Leaf, 1000)
		for j := 0; j < 1000; j++ {
			path := big.NewInt(int64(j))
			value := []byte("batch_value_" + path.String())
			leaves[j] = NewLeaf(path, value)
		}
		
		b.StartTimer()
		
		// Add all leaves in a batch and calculate root hash
		err := tree.AddLeaves(leaves)
		if err != nil {
			b.Fatalf("AddLeaves failed: %v", err)
		}
		
		// Calculate root hash (this is included in the timing)
		_ = tree.GetRootHash()
	}
}

// BenchmarkSMTIndividualAdd benchmarks adding 1000 leaves one by one
func BenchmarkSMTIndividualAdd(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := NewSparseMerkleTree(SHA256)
		
		b.StartTimer()
		
		// Add 1000 leaves one by one
		for j := 0; j < 1000; j++ {
			path := big.NewInt(int64(j))
			value := []byte("individual_value_" + path.String())
			
			err := tree.AddLeaf(path, value)
			if err != nil {
				b.Fatalf("AddLeaf failed: %v", err)
			}
		}
		
		// Calculate root hash (this is included in the timing)
		_ = tree.GetRootHash()
	}
}

// BenchmarkSMTBatchAddThreadSafe benchmarks batch addition with thread-safe wrapper
func BenchmarkSMTBatchAddThreadSafe(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := NewSafeSparseMerkleTree(SHA256)
		
		// Prepare 1000 leaves
		leaves := make([]*Leaf, 1000)
		for j := 0; j < 1000; j++ {
			path := big.NewInt(int64(j))
			value := []byte("safe_batch_value_" + path.String())
			leaves[j] = NewLeaf(path, value)
		}
		
		b.StartTimer()
		
		// Add all leaves in a batch and calculate root hash
		err := tree.AddLeaves(leaves)
		if err != nil {
			b.Fatalf("AddLeaves failed: %v", err)
		}
		
		// Calculate root hash (this is included in the timing)
		_ = tree.GetRootHash()
	}
}

// BenchmarkSMTIndividualAddThreadSafe benchmarks individual addition with thread-safe wrapper
func BenchmarkSMTIndividualAddThreadSafe(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := NewSafeSparseMerkleTree(SHA256)
		
		b.StartTimer()
		
		// Add 1000 leaves one by one
		for j := 0; j < 1000; j++ {
			path := big.NewInt(int64(j))
			value := []byte("safe_individual_value_" + path.String())
			
			err := tree.AddLeaf(path, value)
			if err != nil {
				b.Fatalf("AddLeaf failed: %v", err)
			}
		}
		
		// Calculate root hash (this is included in the timing)
		_ = tree.GetRootHash()
	}
}

// BenchmarkSMTRootHashCalculation benchmarks just the root hash calculation on various tree sizes
func BenchmarkSMTRootHashCalculation(b *testing.B) {
	sizes := []int{10, 100, 1000, 5000}
	
	for _, size := range sizes {
		b.Run("size_"+string(rune(size/10+48)), func(b *testing.B) {
			// Pre-populate tree
			tree := NewSparseMerkleTree(SHA256)
			leaves := make([]*Leaf, size)
			for j := 0; j < size; j++ {
				path := big.NewInt(int64(j))
				value := []byte("hash_bench_value_" + path.String())
				leaves[j] = NewLeaf(path, value)
			}
			
			err := tree.AddLeaves(leaves)
			if err != nil {
				b.Fatalf("AddLeaves failed: %v", err)
			}
			
			b.ResetTimer()
			
			// Benchmark just the root hash calculation
			for i := 0; i < b.N; i++ {
				_ = tree.GetRootHash()
			}
		})
	}
}

// BenchmarkSMTInclusionProofGeneration benchmarks inclusion proof generation
func BenchmarkSMTInclusionProofGeneration(b *testing.B) {
	// Pre-populate tree with 1000 leaves
	tree := NewSparseMerkleTree(SHA256)
	leaves := make([]*Leaf, 1000)
	for j := 0; j < 1000; j++ {
		path := big.NewInt(int64(j))
		value := []byte("proof_bench_value_" + path.String())
		leaves[j] = NewLeaf(path, value)
	}
	
	err := tree.AddLeaves(leaves)
	if err != nil {
		b.Fatalf("AddLeaves failed: %v", err)
	}
	
	b.ResetTimer()
	
	// Benchmark proof generation for different paths
	for i := 0; i < b.N; i++ {
		path := big.NewInt(int64(i % 1000))
		_, err := tree.GenerateInclusionProof(path)
		if err != nil {
			b.Fatalf("GenerateInclusionProof failed: %v", err)
		}
	}
}

// BenchmarkSMTInclusionProofVerification benchmarks inclusion proof verification
func BenchmarkSMTInclusionProofVerification(b *testing.B) {
	// Use simple 2-leaf tree that we know works
	tree := NewSparseMerkleTree(SHA256)
	
	err := tree.AddLeaf(big.NewInt(0), []byte("value0"))
	if err != nil {
		b.Fatalf("AddLeaf failed: %v", err)
	}
	
	err = tree.AddLeaf(big.NewInt(1), []byte("value1"))
	if err != nil {
		b.Fatalf("AddLeaf failed: %v", err)
	}
	
	// Pre-generate proofs for both leaves
	proof0, err := tree.GenerateInclusionProof(big.NewInt(0))
	if err != nil {
		b.Fatalf("GenerateInclusionProof failed: %v", err)
	}
	
	proof1, err := tree.GenerateInclusionProof(big.NewInt(1))
	if err != nil {
		b.Fatalf("GenerateInclusionProof failed: %v", err)
	}
	
	b.ResetTimer()
	
	// Benchmark proof verification
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			isValid := proof0.Verify(SHA256, big.NewInt(0), []byte("value0"))
			if !isValid {
				b.Fatalf("Proof verification failed for path 0")
			}
		} else {
			isValid := proof1.Verify(SHA256, big.NewInt(1), []byte("value1"))
			if !isValid {
				b.Fatalf("Proof verification failed for path 1")
			}
		}
	}
}

// BenchmarkSMTMemoryUsage provides a simple memory usage test
func BenchmarkSMTMemoryUsage(b *testing.B) {
	sizes := []int{1000, 5000, 10000}
	
	for _, size := range sizes {
		b.Run("leaves_"+string(rune(size/1000+48))+"k", func(b *testing.B) {
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				tree := NewSparseMerkleTree(SHA256)
				leaves := make([]*Leaf, size)
				
				for j := 0; j < size; j++ {
					path := big.NewInt(int64(j))
					value := []byte("memory_test_value_" + path.String())
					leaves[j] = NewLeaf(path, value)
				}
				
				err := tree.AddLeaves(leaves)
				if err != nil {
					b.Fatalf("AddLeaves failed: %v", err)
				}
				
				_ = tree.GetRootHash()
			}
		})
	}
}

// BenchmarkSMTLargePathNumbers benchmarks performance with large path numbers
func BenchmarkSMTLargePathNumbers(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := NewSparseMerkleTree(SHA256)
		
		// Prepare 1000 leaves with large path numbers
		leaves := make([]*Leaf, 1000)
		for j := 0; j < 1000; j++ {
			// Use large numbers for paths
			path := new(big.Int).Mul(big.NewInt(int64(j)), big.NewInt(1000000000000))
			value := []byte("large_path_value_" + path.String())
			leaves[j] = NewLeaf(path, value)
		}
		
		b.StartTimer()
		
		// Add all leaves in a batch and calculate root hash
		err := tree.AddLeaves(leaves)
		if err != nil {
			b.Fatalf("AddLeaves failed: %v", err)
		}
		
		// Calculate root hash
		_ = tree.GetRootHash()
	}
}