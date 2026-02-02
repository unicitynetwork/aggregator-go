package smt

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"runtime"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// generateCommitment creates a valid commitment for benchmarking without testing.T dependency
func generateCommitment(index int) (*models.Commitment, error) {
	privateKey, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()

	stateData := make([]byte, 32)
	baseData := fmt.Sprintf("bench_%d", index)
	copy(stateData, baseData)
	if len(baseData) < 32 {
		_, err = rand.Read(stateData[len(baseData):])
		if err != nil {
			return nil, fmt.Errorf("failed to generate random state data: %w", err)
		}
	}
	stateHashImprint := signing.CreateDataHashImprint(stateData)

	requestID, err := api.CreateRequestID(publicKeyBytes, stateHashImprint)
	if err != nil {
		return nil, fmt.Errorf("failed to create request ID: %w", err)
	}

	transactionData := make([]byte, 32)
	txPrefix := fmt.Sprintf("tx_bench_%d", index)
	copy(transactionData, txPrefix)
	if len(txPrefix) < 32 {
		_, err = rand.Read(transactionData[len(txPrefix):])
		if err != nil {
			return nil, fmt.Errorf("failed to generate random transaction data: %w", err)
		}
	}
	transactionHashImprint := signing.CreateDataHashImprint(transactionData)

	transactionHashBytes, err := transactionHashImprint.DataBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to extract transaction hash: %w", err)
	}

	signingService := signing.NewSigningService()
	signatureBytes, err := signingService.SignHash(transactionHashBytes, privateKey.Serialize())
	if err != nil {
		return nil, fmt.Errorf("failed to sign transaction: %w", err)
	}

	authenticator := models.Authenticator{
		Algorithm: "secp256k1",
		PublicKey: publicKeyBytes,
		Signature: signatureBytes,
		StateHash: stateHashImprint,
	}

	return models.NewCommitment(requestID, transactionHashImprint, authenticator), nil
}

// BenchmarkSMTMemoryUsageRealistic measures actual memory usage with realistic commitments
func BenchmarkSMTMemoryUsageRealistic(b *testing.B) {
	sizes := []int{10_000, 100_000, 500_000, 1_000_000, 2_000_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("leaves_%d", size), func(b *testing.B) {
			// Force GC and get baseline memory
			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			// Create SMT with realistic key length (16 bits shard prefix + 256 bits hash)
			smtTree := NewSparseMerkleTree(api.SHA256, 16+256)

			// Generate realistic commitments and add as leaves
			leaves := make([]*Leaf, size)
			for i := 0; i < size; i++ {
				commitment, err := generateCommitment(i)
				if err != nil {
					b.Fatalf("Failed to generate commitment: %v", err)
				}

				path, err := commitment.RequestID.GetPath()
				if err != nil {
					b.Fatalf("Failed to get path: %v", err)
				}

				leafValue, err := commitment.CreateLeafValue()
				if err != nil {
					b.Fatalf("Failed to create leaf value: %v", err)
				}

				leaves[i] = &Leaf{Path: path, Value: leafValue}

				// Progress logging for large runs
				if (i+1)%100_000 == 0 {
					b.Logf("Generated %d/%d leaves", i+1, size)
				}
			}

			b.ResetTimer()

			// Add all leaves to SMT
			err := smtTree.AddLeaves(leaves)
			if err != nil {
				b.Fatalf("Failed to add leaves: %v", err)
			}

			// Calculate root hash (triggers hash computation)
			_ = smtTree.GetRootHash()

			b.StopTimer()

			// Force GC and measure memory after
			runtime.GC()
			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)

			// Calculate memory used by SMT
			memUsedBytes := memAfter.Alloc - memBefore.Alloc
			memUsedMB := float64(memUsedBytes) / 1024 / 1024

			b.ReportMetric(float64(memUsedBytes), "bytes")
			b.ReportMetric(memUsedMB, "MB")
			b.ReportMetric(float64(memUsedBytes)/float64(size), "bytes/leaf")

			b.Logf("SMT with %d leaves: %.2f MB total, %.2f bytes/leaf",
				size, memUsedMB, float64(memUsedBytes)/float64(size))
		})
	}
}

// TestSMTMemoryMeasurement is a test (not benchmark) for precise memory measurement
// Run with: go test -v -run TestSMTMemoryMeasurement -timeout 30m
func TestSMTMemoryMeasurement(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory measurement in short mode")
	}

	sizes := []int{10_000, 100_000, 500_000, 1_000_000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("leaves_%d", size), func(t *testing.T) {
			// Step 1: Generate leaves first (this allocates memory for commitments)
			t.Logf("Generating %d commitments...", size)
			leaves := make([]*Leaf, size)
			for i := 0; i < size; i++ {
				commitment := testutil.CreateTestCommitment(t, fmt.Sprintf("mem_test_%d", i))

				path, err := commitment.RequestID.GetPath()
				if err != nil {
					t.Fatalf("Failed to get path: %v", err)
				}

				leafValue, err := commitment.CreateLeafValue()
				if err != nil {
					t.Fatalf("Failed to create leaf value: %v", err)
				}

				leaves[i] = &Leaf{Path: path, Value: leafValue}

				if (i+1)%50_000 == 0 {
					t.Logf("Generated %d/%d leaves", i+1, size)
				}
			}

			// Step 2: Force GC to get accurate baseline AFTER commitment generation
			runtime.GC()
			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			// Step 3: Create SMT and add leaves
			smtTree := NewSparseMerkleTree(api.SHA256, 16+256)
			t.Logf("Adding %d leaves to SMT...", size)
			err := smtTree.AddLeaves(leaves)
			if err != nil {
				t.Fatalf("Failed to add leaves: %v", err)
			}

			// Calculate root hash (materializes cached hashes)
			rootHash := smtTree.GetRootHashHex()
			t.Logf("Root hash: %s", rootHash[:16]+"...")

			// Step 4: Measure memory BEFORE clearing leaves (SMT still holds refs)
			var memAfterSMT runtime.MemStats
			runtime.ReadMemStats(&memAfterSMT)

			smtAllocBytes := memAfterSMT.HeapAlloc - memBefore.HeapAlloc
			smtAllocMB := float64(smtAllocBytes) / 1024 / 1024

			// Step 5: Now clear leaves and force GC to see SMT-only memory
			leaves = nil
			runtime.GC()
			runtime.GC()

			var memFinal runtime.MemStats
			runtime.ReadMemStats(&memFinal)

			// This is the memory held by SMT alone (after leaves slice is GC'd)
			finalHeapMB := float64(memFinal.HeapAlloc) / 1024 / 1024

			t.Logf("\n=== SMT Memory Usage for %d leaves ===", size)
			t.Logf("SMT allocation:  %.2f MB (%.2f bytes/leaf)", smtAllocMB, float64(smtAllocBytes)/float64(size))
			t.Logf("Final heap:      %.2f MB", finalHeapMB)
			t.Logf("TotalAlloc:      %.2f MB (cumulative)", float64(memFinal.TotalAlloc)/1024/1024)
			t.Logf("Sys (from OS):   %.2f MB", float64(memFinal.Sys)/1024/1024)
			t.Logf("==========================================\n")

			// Keep SMT alive to prevent optimization
			runtime.KeepAlive(smtTree)
		})
	}
}

// BenchmarkSMTOperationsWithLoad benchmarks operations with a pre-populated SMT
func BenchmarkSMTOperationsWithLoad(b *testing.B) {
	// Pre-populate with 100k leaves
	const preloadSize = 100_000

	smtTree := NewSparseMerkleTree(api.SHA256, 16+256)
	leaves := make([]*Leaf, preloadSize)
	paths := make([]*big.Int, preloadSize)

	b.Logf("Pre-populating SMT with %d leaves...", preloadSize)
	for i := 0; i < preloadSize; i++ {
		commitment, err := generateCommitment(i)
		if err != nil {
			b.Fatalf("Failed to generate commitment: %v", err)
		}

		path, _ := commitment.RequestID.GetPath()
		leafValue, _ := commitment.CreateLeafValue()

		leaves[i] = &Leaf{Path: path, Value: leafValue}
		paths[i] = path
	}

	err := smtTree.AddLeaves(leaves)
	if err != nil {
		b.Fatalf("Failed to preload SMT: %v", err)
	}

	// Force hash computation
	_ = smtTree.GetRootHash()
	b.Logf("SMT preloaded, running benchmarks...")

	b.Run("GetPath_100k_leaves", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			path := paths[i%preloadSize]
			_, err := smtTree.GetPath(path)
			if err != nil {
				b.Fatalf("GetPath failed: %v", err)
			}
		}
	})

	b.Run("AddLeaf_to_100k_tree", func(b *testing.B) {
		// Create a snapshot for each add to avoid modifying the base tree
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			snapshot := smtTree.CreateSnapshot()
			commitment, err := generateCommitment(preloadSize + i)
			if err != nil {
				b.Fatalf("Failed to generate commitment: %v", err)
			}
			path, _ := commitment.RequestID.GetPath()
			leafValue, _ := commitment.CreateLeafValue()
			b.StartTimer()

			err = snapshot.AddLeaf(path, leafValue)
			if err != nil && err != ErrDuplicateLeaf {
				b.Fatalf("AddLeaf failed: %v", err)
			}
		}
	})

	b.Run("GetRootHash_100k_leaves", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = smtTree.GetRootHash()
		}
	})
}
