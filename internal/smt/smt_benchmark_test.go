package smt

import (
	"fmt"
	"math/big"
	"testing"
)

// BenchmarkSMTBatchAdd benchmarks adding 1000 leaves at once (batch approach)
func BenchmarkSMTBatchAdd(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		smt := NewSparseMerkleTree(SHA256)

		// Prepare 1000 leaves with non-conflicting paths
		leaves := make([]*Leaf, 1000)
		for j := 0; j < 1000; j++ {
			// Use large spacing to avoid path conflicts: j * 100000
			path := big.NewInt(int64(j * 100000))
			value := []byte("batch_value_" + path.String())
			leaves[j] = NewLeaf(path, value)
		}

		b.StartTimer()

		// Add all leaves in TRUE BATCH and calculate root hash ONCE
		err := smt.AddLeaves(leaves)
		if err != nil {
			b.Fatalf("AddLeaves failed: %v", err)
		}

		// Calculate root hash (included in timing) - happens only once
		_ = smt.GetRootHash()
	}
}

// BenchmarkSMTIndividualAdd benchmarks adding 1000 leaves one by one
func BenchmarkSMTIndividualAdd(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		smt := NewSparseMerkleTree(SHA256)

		b.StartTimer()

		// Add 1000 leaves one by one (tree reconstruction each time)
		for j := 0; j < 1000; j++ {
			// Use large spacing to avoid path conflicts: j * 100000
			path := big.NewInt(int64(j * 100000))
			value := []byte("individual_value_" + path.String())

			err := smt.AddLeaf(path, value)
			if err != nil {
				b.Fatalf("AddLeaf failed: %v", err)
			}
		}

		// Calculate root hash (included in timing)
		_ = smt.GetRootHash()
	}
}

// BenchmarkSMTLargeBatch benchmarks adding 2000 leaves to show larger performance gains
func BenchmarkSMTLargeBatch(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		smt := NewSparseMerkleTree(SHA256)

		// Prepare 2000 leaves with very large spacing to avoid path conflicts
		leaves := make([]*Leaf, 2000)
		for j := 0; j < 2000; j++ {
			// Use massive spacing to avoid path conflicts: j * 1000000
			path := big.NewInt(int64(j * 1000000))
			value := []byte("large_batch_" + path.String())
			leaves[j] = NewLeaf(path, value)
		}

		b.StartTimer()

		// Add all leaves in batch - optimized with lazy hash calculation
		err := smt.AddLeaves(leaves)
		if err != nil {
			b.Fatalf("AddLeaves failed: %v", err)
		}

		// Calculate root hash only once at the end
		_ = smt.GetRootHash()
	}
}

// BenchmarkSMTLargeIndividual benchmarks adding 2000 leaves individually
func BenchmarkSMTLargeIndividual(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		smt := NewSparseMerkleTree(SHA256)

		b.StartTimer()

		// Add 2000 leaves one by one - many hash calculations
		for j := 0; j < 2000; j++ {
			path := big.NewInt(int64(j * 1000000))
			value := []byte("large_individual_" + path.String())

			err := smt.AddLeaf(path, value)
			if err != nil {
				b.Fatalf("AddLeaf failed: %v", err)
			}
		}

		// Calculate root hash
		_ = smt.GetRootHash()
	}
}

// BenchmarkSMTRealWorld benchmarks with realistic crypto hash-like paths
func BenchmarkSMTRealWorldBatch(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		smt := NewSparseMerkleTree(SHA256)

		// Generate 500 realistic paths using bit shifts to avoid conflicts
		leaves := make([]*Leaf, 500)
		for j := 0; j < 500; j++ {
			// Create paths that look like real hash prefixes
			path := big.NewInt(0x1000000000000000 + int64(j)*0x1000000)
			value := []byte("real_world_value_" + path.String())
			leaves[j] = NewLeaf(path, value)
		}

		b.StartTimer()

		err := smt.AddLeaves(leaves)
		if err != nil {
			b.Fatalf("RealWorld AddLeaves failed: %v", err)
		}

		_ = smt.GetRootHash()
	}
}

// BenchmarkSMTRealWorldIndividual benchmarks individual addition with realistic paths
func BenchmarkSMTRealWorldIndividual(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		smt := NewSparseMerkleTree(SHA256)

		b.StartTimer()

		// Add 500 realistic paths one by one
		for j := 0; j < 500; j++ {
			path := big.NewInt(0x1000000000000000 + int64(j)*0x1000000)
			value := []byte("real_world_value_" + path.String())

			err := smt.AddLeaf(path, value)
			if err != nil {
				b.Fatalf("RealWorld AddLeaf failed: %v", err)
			}
		}

		_ = smt.GetRootHash()
	}
}

// BenchmarkSMTRootHashCalculation benchmarks just the root hash calculation
func BenchmarkSMTRootHashCalculation(b *testing.B) {
	// Pre-populate tree
	smt := NewSparseMerkleTree(SHA256)
	for j := 0; j < 100; j++ {
		path := big.NewInt(int64(j * 1000))
		value := []byte("hash_bench_value_" + path.String())
		smt.AddLeaf(path, value)
	}

	b.ResetTimer()

	// Benchmark just the root hash calculation
	for i := 0; i < b.N; i++ {
		_ = smt.GetRootHash()
	}
}

// BenchmarkSMTKnownDataset benchmarks with the known TypeScript test dataset
func BenchmarkSMTKnownDataset(b *testing.B) {
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

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		smt := NewSparseMerkleTree(SHA256)

		// Add all leaves and calculate hash
		for _, data := range testData {
			err := smt.AddLeaf(big.NewInt(data.path), []byte(data.value))
			if err != nil {
				b.Fatalf("AddLeaf failed: %v", err)
			}
		}

		_ = smt.GetRootHash()
	}
}

// BenchmarkSMTMemoryUsage benchmarks memory usage with different tree sizes
func BenchmarkSMTMemoryUsage(b *testing.B) {
	sizes := []int{100, 500, 1000}

	for _, size := range sizes {
		b.Run("leaves_"+string(rune(size/100+48))+"00", func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				smt := NewSparseMerkleTree(SHA256)

				for j := 0; j < size; j++ {
					path := big.NewInt(int64(j * 10000))
					value := []byte("memory_test_value_" + path.String())
					smt.AddLeaf(path, value)
				}

				_ = smt.GetRootHash()
			}
		})
	}
}

// BenchmarkSMTLeafRetrieval benchmarks leaf retrieval performance
func BenchmarkSMTLeafRetrieval(b *testing.B) {
	// Pre-populate tree with non-conflicting paths
	smt := NewSparseMerkleTree(SHA256)

	// Use paths with distinct bit patterns to avoid conflicts
	basePaths := []int64{
		0b1000000000000, // 4096
		0b1000000000001, // 4097
		0b1000000000010, // 4098
		0b1000000000100, // 4100
		0b1000000001000, // 4104
		0b1000000010000, // 4112
		0b1000000100000, // 4128
		0b1000001000000, // 4160
		0b1000010000000, // 4224
		0b1000100000000, // 4352
	}

	paths := make([]*big.Int, len(basePaths))

	for j, basePath := range basePaths {
		path := big.NewInt(basePath)
		value := []byte("retrieval_test_" + path.String())
		paths[j] = path
		err := smt.AddLeaf(path, value)
		if err != nil {
			b.Fatalf("AddLeaf failed during setup: %v", err)
		}
	}

	b.ResetTimer()

	// Benchmark leaf retrieval
	for i := 0; i < b.N; i++ {
		path := paths[i%len(paths)]
		_, err := smt.GetLeaf(path)
		if err != nil {
			b.Fatalf("GetLeaf failed: %v", err)
		}
	}
}

// BenchmarkSMTMassiveScale tests 100k+ batch operations only (no individual comparison)
func BenchmarkSMTMassiveScale(b *testing.B) {
	size := 100000

	b.Run("Batch_100k", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			smt := NewSparseMerkleTree(SHA256)

			// Generate 100k non-conflicting paths efficiently
			leaves := make([]*Leaf, size)
			for j := 0; j < size; j++ {
				// Use very large spacing and different base to avoid conflicts
				basePath := int64(0x2000000000000000)
				offset := int64(j) * 0x1000000 // 16MB spacing in path space
				path := big.NewInt(basePath + offset)

				value := []byte(fmt.Sprintf("massive_scale_%d", j))
				leaves[j] = NewLeaf(path, value)
			}

			b.StartTimer()

			// Measure MASSIVE batch operation
			err := smt.AddLeaves(leaves)
			if err != nil {
				b.Fatalf("Massive batch failed: %v", err)
			}

			// Calculate final root hash
			rootHash := smt.GetRootHash()
			if len(rootHash) == 0 {
				b.Fatalf("Root hash is empty")
			}
		}
	})
}

// BenchmarkSMTProductionScale benchmarks with production-scale datasets (5k, 10k, 100k leaves)
func BenchmarkSMTProductionScale(b *testing.B) {
	sizes := []int{5000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Batch_%dk", size/1000), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				smt := NewSparseMerkleTree(SHA256)

				// Generate paths that won't conflict by using crypto-hash-like distribution
				leaves := make([]*Leaf, size)
				for j := 0; j < size; j++ {
					// Use bit patterns that simulate real cryptographic hashes
					// Start from a large base and add structured offsets
					basePath := int64(0x1000000000000000) // Large starting point
					offset := int64(j) * 0x10000000       // Large step size
					path := big.NewInt(basePath + offset)

					value := []byte(fmt.Sprintf("production_value_%d", j))
					leaves[j] = NewLeaf(path, value)
				}

				b.StartTimer()

				// TRUE BATCH: Add all leaves and calculate root hash once
				err := smt.AddLeaves(leaves)
				if err != nil {
					b.Fatalf("Batch AddLeaves failed for %d leaves: %v", size, err)
				}

				// Calculate root hash - this is what we're measuring
				rootHash := smt.GetRootHash()
				if len(rootHash) == 0 {
					b.Fatalf("Root hash is empty")
				}
			}
		})

		b.Run(fmt.Sprintf("Individual_%dk", size/1000), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				smt := NewSparseMerkleTree(SHA256)

				b.StartTimer()

				// Add leaves one by one (for comparison)
				for j := 0; j < size; j++ {
					basePath := int64(0x1000000000000000)
					offset := int64(j) * 0x10000000
					path := big.NewInt(basePath + offset)

					value := []byte(fmt.Sprintf("production_value_%d", j))

					err := smt.AddLeaf(path, value)
					if err != nil {
						b.Fatalf("Individual AddLeaf failed at %d: %v", j, err)
					}
				}

				// Calculate final root hash
				rootHash := smt.GetRootHash()
				if len(rootHash) == 0 {
					b.Fatalf("Root hash is empty")
				}
			}
		})
	}
}

// BenchmarkSMTMemoryScaling tests memory usage with large datasets
func BenchmarkSMTMemoryScaling(b *testing.B) {
	sizes := []int{1000, 5000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Memory_%dk", size/1000), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				smt := NewSparseMerkleTree(SHA256)

				// Generate leaves
				leaves := make([]*Leaf, size)
				for j := 0; j < size; j++ {
					basePath := int64(0x2000000000000000)
					offset := int64(j) * 0x10000000
					path := big.NewInt(basePath + offset)
					value := []byte(fmt.Sprintf("memory_test_%d", j))
					leaves[j] = NewLeaf(path, value)
				}

				// Add all leaves and calculate hash
				smt.AddLeaves(leaves)
				_ = smt.GetRootHash()
			}
		})
	}
}

// BenchmarkSMTRootHashOnly benchmarks just the root hash calculation on pre-built trees
func BenchmarkSMTRootHashOnly(b *testing.B) {
	sizes := []int{1000, 5000, 10000, 100000}

	for _, size := range sizes {
		// Pre-build the tree
		smt := NewSparseMerkleTree(SHA256)
		leaves := make([]*Leaf, size)
		for j := 0; j < size; j++ {
			basePath := int64(0x3000000000000000)
			offset := int64(j) * 0x10000000
			path := big.NewInt(basePath + offset)
			value := []byte(fmt.Sprintf("hash_test_%d", j))
			leaves[j] = NewLeaf(path, value)
		}

		err := smt.AddLeaves(leaves)
		if err != nil {
			b.Fatalf("Failed to build tree for %d leaves: %v", size, err)
		}

		b.Run(fmt.Sprintf("HashOnly_%dk", size/1000), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Just measure hash calculation time
				_ = smt.GetRootHash()
			}
		})
	}
}

// BenchmarkSMTBatchVsIndividual compares batch vs individual addition directly
func BenchmarkSMTBatchVsIndividual(b *testing.B) {
	// Prepare test data (known working Unicity SDK-compatible set)
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

	b.Run("Batch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			smt := NewSparseMerkleTree(SHA256)

			// Prepare batch
			leaves := make([]*Leaf, len(testData))
			for j, data := range testData {
				leaves[j] = NewLeaf(big.NewInt(data.path), []byte(data.value))
			}

			// Add batch and get hash
			smt.AddLeaves(leaves)
			_ = smt.GetRootHash()
		}
	})

	b.Run("Individual", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			smt := NewSparseMerkleTree(SHA256)

			// Add one by one
			for _, data := range testData {
				smt.AddLeaf(big.NewInt(data.path), []byte(data.value))
			}
			_ = smt.GetRootHash()
		}
	})

	b.Run("IndividualWithHashEach", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			smt := NewSparseMerkleTree(SHA256)

			// Add one by one with hash calculation after each
			for _, data := range testData {
				smt.AddLeaf(big.NewInt(data.path), []byte(data.value))
				_ = smt.GetRootHash() // Calculate hash after each addition
			}
		}
	})
}

// BenchmarkSMTPerformanceComparison provides a clear comparison of batch vs individual
func BenchmarkSMTPerformanceComparison(b *testing.B) {
	// Test with increasing dataset sizes to show scaling benefits
	sizes := []int{10, 50, 100}

	for _, size := range sizes {
		// Generate non-conflicting paths
		testData := make([]struct {
			path  int64
			value string
		}, size)

		for i := 0; i < size; i++ {
			testData[i] = struct {
				path  int64
				value string
			}{
				path:  int64(i * 1000000), // Large spacing to avoid conflicts
				value: "perf_test_" + string(rune(i+48)),
			}
		}

		b.Run(fmt.Sprintf("Batch_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				smt := NewSparseMerkleTree(SHA256)

				leaves := make([]*Leaf, len(testData))
				for j, data := range testData {
					leaves[j] = NewLeaf(big.NewInt(data.path), []byte(data.value))
				}

				// TRUE BATCH: Build tree once, hash once
				smt.AddLeaves(leaves)
				_ = smt.GetRootHash()
			}
		})

		b.Run(fmt.Sprintf("Individual_%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				smt := NewSparseMerkleTree(SHA256)

				// Sequential: Build tree incrementally, multiple hash calculations
				for _, data := range testData {
					smt.AddLeaf(big.NewInt(data.path), []byte(data.value))
				}
				_ = smt.GetRootHash()
			}
		})
	}
}
