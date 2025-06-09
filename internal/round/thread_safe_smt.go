package round

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
)

// ThreadSafeSMT provides thread-safe access to SMT operations
// It prevents concurrent access during batch operations and proof generation
type ThreadSafeSMT struct {
	smt    *smt.SparseMerkleTree
	rwMux  sync.RWMutex // RWMutex allows multiple readers but exclusive writers
}

// NewThreadSafeSMT creates a new thread-safe SMT wrapper
func NewThreadSafeSMT(smtInstance *smt.SparseMerkleTree) *ThreadSafeSMT {
	return &ThreadSafeSMT{
		smt: smtInstance,
	}
}

// AddLeaves adds multiple leaves to the SMT in a batch operation
// This operation is exclusive and blocks all other operations
func (ts *ThreadSafeSMT) AddLeaves(leaves []*smt.Leaf) (string, error) {
	ts.rwMux.Lock()
	defer ts.rwMux.Unlock()

	err := ts.smt.AddLeaves(leaves)
	if err != nil {
		return "", fmt.Errorf("failed to add leaves to SMT: %w", err)
	}

	return ts.smt.GetRootHashHex(), nil
}

// AddLeaf adds a single leaf to the SMT
// This operation is exclusive and blocks all other operations
func (ts *ThreadSafeSMT) AddLeaf(path *big.Int, value []byte) error {
	ts.rwMux.Lock()
	defer ts.rwMux.Unlock()

	return ts.smt.AddLeaf(path, value)
}

// GetRootHash returns the current root hash
// This is a read operation that can be performed concurrently
func (ts *ThreadSafeSMT) GetRootHash() string {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()

	return ts.smt.GetRootHashHex()
}

// GetLeaf retrieves a leaf by path
// This is a read operation that can be performed concurrently  
func (ts *ThreadSafeSMT) GetLeaf(path *big.Int) (*smt.LeafBranch, error) {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()

	return ts.smt.GetLeaf(path)
}

// TODO: Add inclusion proof methods when SMT supports them
// GenerateInclusionProof generates an inclusion proof for a given path
// This is a read operation that can be performed concurrently
// However, it's blocked during batch operations to ensure consistency
// func (ts *ThreadSafeSMT) GenerateInclusionProof(path uint64) (*smt.InclusionProof, error) {
//     ts.rwMux.RLock()
//     defer ts.rwMux.RUnlock()
//     return ts.smt.GenerateInclusionProof(path)
// }

// GetStats returns statistics about the SMT
// This is a read operation that can be performed concurrently
func (ts *ThreadSafeSMT) GetStats() map[string]interface{} {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()

	// Get basic stats from the underlying SMT
	return map[string]interface{}{
		"rootHash":   ts.smt.GetRootHashHex(),
		"leafCount":  ts.getLeafCount(),
		"isLocked":   false, // Could be enhanced to show lock status
	}
}

// getLeafCount returns the number of leaves in the tree
// Note: This is an internal method that requires the caller to hold a lock
func (ts *ThreadSafeSMT) getLeafCount() int {
	// This would need to be implemented in the underlying SMT
	// For now, return 0 as a placeholder
	return 0
}

// WithReadLock executes a function while holding a read lock
// This is useful for complex read operations that need consistency
func (ts *ThreadSafeSMT) WithReadLock(fn func() error) error {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()
	return fn()
}

// WithWriteLock executes a function while holding a write lock
// This is useful for complex write operations that need atomicity
func (ts *ThreadSafeSMT) WithWriteLock(fn func() error) error {
	ts.rwMux.Lock()
	defer ts.rwMux.Unlock()
	return fn()
}