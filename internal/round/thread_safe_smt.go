package round

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/unicitynetwork/aggregator-go/pkg/api"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
)

// ThreadSafeSMT provides thread-safe access to SMT operations
// It prevents concurrent access during batch operations and proof generation
type ThreadSafeSMT struct {
	smt   *smt.SparseMerkleTree
	rwMux sync.RWMutex // RWMutex allows multiple readers but exclusive writers
}

// ThreadSafeSmtSnapshot provides thread-safe access to SMT snapshot operations for writing only
// It wraps an SmtSnapshot and provides thread-safety for write operations (AddLeaf, AddLeaves)
// Read operations should be performed on the original ThreadSafeSMT after committing the snapshot
type ThreadSafeSmtSnapshot struct {
	snapshot *smt.SmtSnapshot
	rwMux    sync.RWMutex // RWMutex allows multiple readers but exclusive writers
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

// GetPath generates a Merkle tree path for the given path
// This is a read operation and allows concurrent access
func (ts *ThreadSafeSMT) GetPath(path *big.Int) *api.MerkleTreePath {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()
	return ts.smt.GetPath(path)
}

// GetStats returns statistics about the SMT
// This is a read operation that can be performed concurrently
func (ts *ThreadSafeSMT) GetStats() map[string]interface{} {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()

	// Get basic stats from the underlying SMT
	return map[string]interface{}{
		"rootHash":  ts.smt.GetRootHashHex(),
		"leafCount": ts.getLeafCount(),
		"isLocked":  false, // Could be enhanced to show lock status
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

// CreateSnapshot creates a thread-safe snapshot of the current SMT state
// This operation requires a read lock to ensure consistency during snapshot creation
func (ts *ThreadSafeSMT) CreateSnapshot() *ThreadSafeSmtSnapshot {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()

	snapshot := ts.smt.CreateSnapshot()
	return &ThreadSafeSmtSnapshot{
		snapshot: snapshot,
	}
}

// ThreadSafeSmtSnapshot Methods

// AddLeaves adds multiple leaves to the snapshot in a batch operation
// This operation is exclusive and blocks all other operations on this snapshot
func (tss *ThreadSafeSmtSnapshot) AddLeaves(leaves []*smt.Leaf) (string, error) {
	tss.rwMux.Lock()
	defer tss.rwMux.Unlock()

	return tss.addLeavesUnsafe(leaves)
}

// addLeavesUnsafe adds multiple leaves without acquiring locks (internal use)
func (tss *ThreadSafeSmtSnapshot) addLeavesUnsafe(leaves []*smt.Leaf) (string, error) {
	err := tss.snapshot.AddLeaves(leaves)
	if err != nil {
		return "", fmt.Errorf("failed to add leaves to SMT snapshot: %w", err)
	}

	return tss.snapshot.GetRootHashHex(), nil
}

// AddLeaf adds a single leaf to the snapshot
// This operation is exclusive and blocks all other operations on this snapshot
func (tss *ThreadSafeSmtSnapshot) AddLeaf(path *big.Int, value []byte) error {
	tss.rwMux.Lock()
	defer tss.rwMux.Unlock()

	return tss.addLeafUnsafe(path, value)
}

// addLeafUnsafe adds a single leaf without acquiring locks (internal use)
func (tss *ThreadSafeSmtSnapshot) addLeafUnsafe(path *big.Int, value []byte) error {
	return tss.snapshot.AddLeaf(path, value)
}

// GetRootHash returns the current root hash of the snapshot
// This is a read operation that can be performed concurrently
func (tss *ThreadSafeSmtSnapshot) GetRootHash() string {
	tss.rwMux.RLock()
	defer tss.rwMux.RUnlock()

	return tss.snapshot.GetRootHashHex()
}

// GetStats returns statistics about the snapshot
// This is a read operation that can be performed concurrently
func (tss *ThreadSafeSmtSnapshot) GetStats() map[string]interface{} {
	tss.rwMux.RLock()
	defer tss.rwMux.RUnlock()

	// Get basic stats from the underlying snapshot
	return map[string]interface{}{
		"rootHash":   tss.snapshot.GetRootHashHex(),
		"leafCount":  0, // Could be enhanced to show actual count
		"isSnapshot": true,
		"isLocked":   false, // Could be enhanced to show lock status
	}
}

// Commit commits the snapshot changes back to the original ThreadSafeSMT
// This operation requires write locks on both the snapshot and the original SMT
// to ensure atomicity of the commit operation
func (tss *ThreadSafeSmtSnapshot) Commit(originalSMT *ThreadSafeSMT) {
	// Acquire locks in a consistent order to prevent deadlocks
	// Lock the snapshot first, then the original SMT
	tss.rwMux.Lock()
	defer tss.rwMux.Unlock()

	originalSMT.rwMux.Lock()
	defer originalSMT.rwMux.Unlock()

	tss.snapshot.Commit()
	return
}

// WithWriteLock executes a function while holding a write lock on the snapshot
// The function receives the underlying snapshot for direct access to avoid deadlocks
func (tss *ThreadSafeSmtSnapshot) WithWriteLock(fn func(*smt.SmtSnapshot) error) error {
	tss.rwMux.Lock()
	defer tss.rwMux.Unlock()
	return fn(tss.snapshot)
}
