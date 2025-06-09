package smt

import (
	"math/big"
	"sync"
)

// SafeSparseMerkleTree provides a thread-safe wrapper around SparseMerkleTree
type SafeSparseMerkleTree struct {
	tree *SparseMerkleTree
	mu   sync.RWMutex
}

// NewSafeSparseMerkleTree creates a new thread-safe sparse merkle tree
func NewSafeSparseMerkleTree(algorithm HashAlgorithm) *SafeSparseMerkleTree {
	return &SafeSparseMerkleTree{
		tree: NewSparseMerkleTree(algorithm),
	}
}

// GetRootHash returns the root hash of the tree (thread-safe read)
func (smt *SafeSparseMerkleTree) GetRootHash() []byte {
	smt.mu.RLock()
	defer smt.mu.RUnlock()
	return smt.tree.GetRootHash()
}

// GetRoot returns a copy of the root node (thread-safe read)
func (smt *SafeSparseMerkleTree) GetRoot() *RootNode {
	smt.mu.RLock()
	defer smt.mu.RUnlock()
	return smt.tree.GetRoot()
}

// AddLeaf adds a single leaf to the tree (thread-safe write)
func (smt *SafeSparseMerkleTree) AddLeaf(path *big.Int, value []byte) error {
	smt.mu.Lock()
	defer smt.mu.Unlock()
	return smt.tree.AddLeaf(path, value)
}

// AddLeaves adds multiple leaves to the tree in a batch operation (thread-safe write)
// This is optimized to only calculate the root hash once after all leaves are added
func (smt *SafeSparseMerkleTree) AddLeaves(leaves []*Leaf) error {
	smt.mu.Lock()
	defer smt.mu.Unlock()
	return smt.tree.AddLeaves(leaves)
}

// GetLeaf retrieves a leaf by its path (thread-safe read)
func (smt *SafeSparseMerkleTree) GetLeaf(path *big.Int) (*LeafNode, error) {
	smt.mu.RLock()
	defer smt.mu.RUnlock()
	return smt.tree.GetLeaf(path)
}

// GenerateInclusionProof generates an inclusion proof for the given path (thread-safe read)
func (smt *SafeSparseMerkleTree) GenerateInclusionProof(path *big.Int) (*InclusionProof, error) {
	smt.mu.RLock()
	defer smt.mu.RUnlock()
	return smt.tree.GenerateInclusionProof(path)
}

// VerifyInclusionProof verifies an inclusion proof against the current root hash (thread-safe read)
func (smt *SafeSparseMerkleTree) VerifyInclusionProof(proof *InclusionProof, leafPath *big.Int, leafValue []byte) bool {
	smt.mu.RLock()
	defer smt.mu.RUnlock()
	return smt.tree.VerifyInclusionProof(proof, leafPath, leafValue)
}

// Clone creates a deep copy of the tree (thread-safe read)
// This can be useful for creating snapshots or for read-only operations
func (smt *SafeSparseMerkleTree) Clone() *SafeSparseMerkleTree {
	smt.mu.RLock()
	defer smt.mu.RUnlock()
	
	// Create a new tree with the same algorithm
	newTree := NewSafeSparseMerkleTree(smt.tree.algorithm)
	
	// Collect all leaves from the current tree
	leaves := smt.tree.collectAllLeaves(smt.tree.root)
	
	// Add all leaves to the new tree
	if len(leaves) > 0 {
		newTree.tree.AddLeaves(leaves)
	}
	
	return newTree
}

// WithReadLock executes a function with a read lock held
// This is useful for performing multiple read operations atomically
func (smt *SafeSparseMerkleTree) WithReadLock(fn func(*SparseMerkleTree)) {
	smt.mu.RLock()
	defer smt.mu.RUnlock()
	fn(smt.tree)
}

// WithWriteLock executes a function with a write lock held
// This is useful for performing multiple write operations atomically
func (smt *SafeSparseMerkleTree) WithWriteLock(fn func(*SparseMerkleTree)) {
	smt.mu.Lock()
	defer smt.mu.Unlock()
	fn(smt.tree)
}

// Stats returns statistics about the tree (thread-safe read)
func (smt *SafeSparseMerkleTree) Stats() TreeStats {
	smt.mu.RLock()
	defer smt.mu.RUnlock()
	
	stats := TreeStats{}
	smt.calculateStats(smt.tree.root, &stats, 0)
	return stats
}

// TreeStats holds statistics about the tree structure
type TreeStats struct {
	TotalNodes    int
	LeafNodes     int
	InternalNodes int
	RootNodes     int
	MaxDepth      int
	TotalLeaves   int
}

// calculateStats recursively calculates tree statistics
func (smt *SafeSparseMerkleTree) calculateStats(node Node, stats *TreeStats, depth int) {
	if node == nil {
		return
	}
	
	stats.TotalNodes++
	if depth > stats.MaxDepth {
		stats.MaxDepth = depth
	}
	
	switch n := node.(type) {
	case *LeafNode:
		stats.LeafNodes++
		stats.TotalLeaves++
	case *InternalNode:
		stats.InternalNodes++
		smt.calculateStats(n.Left, stats, depth+1)
		smt.calculateStats(n.Right, stats, depth+1)
	case *RootNode:
		stats.RootNodes++
		smt.calculateStats(n.Left, stats, depth+1)
		smt.calculateStats(n.Right, stats, depth+1)
	}
}