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
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// generateCommitment creates a valid commitment for benchmarking without testing.T dependency
func generateCommitment(index int) (*models.CertificationRequest, error) {
	privateKey, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}
	publicKeyBytes := privateKey.PubKey().SerializeCompressed()
	ownerPredicate := api.NewPayToPublicKeyPredicate(publicKeyBytes)

	stateData := make([]byte, 32)
	baseData := fmt.Sprintf("bench_%d", index)
	copy(stateData, baseData)
	if len(baseData) < 32 {
		_, err = rand.Read(stateData[len(baseData):])
		if err != nil {
			return nil, fmt.Errorf("failed to generate random state data: %w", err)
		}
	}
	sourceStateHash := signing.CreateDataHash(stateData)
	stateID, err := api.CreateStateID(ownerPredicate, sourceStateHash)
	if err != nil {
		return nil, fmt.Errorf("failed to create state ID: %w", err)
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
	transactionHash := signing.CreateDataHash(transactionData)
	signingService := signing.NewSigningService()
	sigDataHash := api.SigDataHash(sourceStateHash, transactionHash)
	signatureBytes, err := signingService.SignDataHash(sigDataHash, privateKey.Serialize())
	if err != nil {
		return nil, fmt.Errorf("failed to sign transaction: %w", err)
	}

	certData := models.CertificationData{
		OwnerPredicate:  ownerPredicate,
		SourceStateHash: sourceStateHash,
		TransactionHash: transactionHash,
		Witness:         signatureBytes,
	}
	return models.NewCertificationRequest(stateID, certData), nil
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

				path, err := commitment.StateID.GetPath()
				if err != nil {
					b.Fatalf("Failed to get path: %v", err)
				}

				leafValue, err := commitment.LeafValue()
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

		path, _ := commitment.StateID.GetPath()
		leafValue, _ := commitment.LeafValue()

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
			path, _ := commitment.StateID.GetPath()
			leafValue, _ := commitment.LeafValue()
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
