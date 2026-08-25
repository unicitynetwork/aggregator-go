package smt

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Same authenticated leaf set (one live commitment in shard 0b10000, nothing
// anywhere else), two different IR.h values depending only on whether an
// empty shard happened to report in this round.
func TestAudit_RootDependsOnLiveShardSet(t *testing.T) {
	const shardIDLen = 4

	emptyChild := NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, api.ShardID(0b11111))
	fmt.Printf("empty CHILD shard root = %x\n", emptyChild.GetRootHashRaw())

	realRoot := make([]byte, 32)
	for i := range realRoot {
		realRoot[i] = 0xAA
	}

	p1 := NewParentSparseMerkleTree(api.SHA256, shardIDLen)
	if err := p1.AddLeaf(big.NewInt(0b10000), realRoot); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("IR.h with shard 1111 silent      = %x\n", p1.GetRootHashRaw())

	p2 := NewParentSparseMerkleTree(api.SHA256, shardIDLen)
	if err := p2.AddLeaf(big.NewInt(0b10000), realRoot); err != nil {
		t.Fatal(err)
	}
	if err := p2.AddLeaf(big.NewInt(0b11111), emptyChild.GetRootHashRaw()); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("IR.h with shard 1111 reporting 0 = %x\n", p2.GetRootHashRaw())

	// dependence on SHARD_ID_LENGTH for the *empty* tree, which is certified
	// verbatim by processRound when no shard reported this round.
	for _, kl := range []int{1, 2, 3, 4, 8, 16} {
		tr := NewParentSparseMerkleTree(api.SHA256, kl)
		fmt.Printf("empty parent IR.h @ SHARD_ID_LENGTH=%2d -> %x\n", kl, tr.GetRootHashRaw())
	}
}

// Cleanest canonicity counterexample: exactly one live shard. The
// whole-partition RSMT root then equals that shard's own root (all keys share
// the shard prefix, so the child tree IS the partition tree).
func TestAudit_SingleLiveShardRootIsNotChildRoot(t *testing.T) {
	realRoot := make([]byte, 32)
	for i := range realRoot {
		realRoot[i] = 0xAA
	}
	for _, kl := range []int{1, 2, 4} {
		p := NewParentSparseMerkleTree(api.SHA256, kl)
		sid := big.NewInt(int64(1<<kl) | 0) // sentinel + all-zero shard bits
		if err := p.AddLeaf(sid, realRoot); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("SHARD_ID_LENGTH=%d single live shard: canonical=%x got=%x\n",
			kl, realRoot, p.GetRootHashRaw())
	}
}

// Sharpest case: it hits even SHARD_ID_LENGTH=1 (the reference compose value).
// A live-but-empty child reports root 0^32 (= bottom). The parent then folds
// bottom in as a genuine child digest instead of compressing the edge away.
func TestAudit_EmptyShardReportsBottom(t *testing.T) {
	realRoot := make([]byte, 32)
	for i := range realRoot {
		realRoot[i] = 0xAA
	}
	zero := make([]byte, 32)
	for _, kl := range []int{1, 2, 4} {
		p := NewParentSparseMerkleTree(api.SHA256, kl)
		if err := p.AddLeaf(big.NewInt(int64(1<<kl)), realRoot); err != nil { // shard 0..0
			t.Fatal(err)
		}
		if err := p.AddLeaf(big.NewInt(int64(1<<(kl+1))-1), zero); err != nil { // shard 1..1, empty
			t.Fatal(err)
		}
		fmt.Printf("SHARD_ID_LENGTH=%d one real + one live-empty shard: canonical=%x got=%x\n",
			kl, realRoot, p.GetRootHashRaw())
	}
}
