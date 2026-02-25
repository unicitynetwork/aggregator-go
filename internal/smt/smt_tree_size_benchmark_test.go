package smt

import (
	"math/big"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const (
	treeSizeKeyLen    = 16 + 256
	treeSizeValueLen  = 34
	treeSizeBatchSize = 50_000
)

func makeDeterministicLeaves(rng *rand.Rand, n int) []*Leaf {
	leaves := make([]*Leaf, n)
	for i := 0; i < n; i++ {
		path := big.NewInt(1)
		path.Lsh(path, uint(treeSizeKeyLen))

		randomPathPart := make([]byte, 32)
		_, _ = rng.Read(randomPathPart)
		path.Or(path, new(big.Int).SetBytes(randomPathPart))

		value := make([]byte, treeSizeValueLen)
		_, _ = rng.Read(value)
		leaves[i] = &Leaf{Path: path, Value: value}
	}
	return leaves
}

// BenchmarkSMTTreeSize reports retained heap size of the built SMT.
//
// Example runs:
//
//	go test ./internal/smt -run '^$' -bench BenchmarkSMTTreeSize/tree_1m -benchtime=1x
//	go test ./internal/smt -run '^$' -bench BenchmarkSMTTreeSize/tree_5m -benchtime=1x
func BenchmarkSMTTreeSize(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{
		{name: "tree_1m", n: 1_000_000},
		{name: "tree_5m", n: 5_000_000},
	}

	for _, tc := range sizes {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				runtime.GC()
				runtime.GC()
				time.Sleep(200 * time.Millisecond)

				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				baseline := before.HeapAlloc

				tree := NewSparseMerkleTree(api.SHA256, treeSizeKeyLen)
				rng := rand.New(rand.NewSource(42))

				buildStart := time.Now()
				for generated := 0; generated < tc.n; generated += treeSizeBatchSize {
					n := treeSizeBatchSize
					if tc.n-generated < n {
						n = tc.n - generated
					}
					if err := tree.AddLeaves(makeDeterministicLeaves(rng, n)); err != nil {
						b.Fatalf("AddLeaves failed at %d/%d: %v", generated, tc.n, err)
					}
				}
				buildDur := time.Since(buildStart)

				rootStart := time.Now()
				root := tree.GetRootHashHex()
				rootDur := time.Since(rootStart)

				runtime.GC()
				runtime.GC()
				time.Sleep(200 * time.Millisecond)

				var after runtime.MemStats
				runtime.ReadMemStats(&after)

				treeBytes := after.HeapAlloc - baseline
				b.ReportMetric(float64(treeBytes)/1024/1024, "tree_heap_mb")
				b.ReportMetric(float64(treeBytes)/float64(tc.n), "tree_bytes_per_leaf")
				b.ReportMetric(buildDur.Seconds(), "build_sec")
				b.ReportMetric(rootDur.Seconds(), "root_hash_sec")
				b.ReportMetric(float64(after.Sys)/1024/1024, "sys_mb")

				// Keep values alive through measurement point.
				runtime.KeepAlive(tree)
				runtime.KeepAlive(root)
			}
		})
	}
}
