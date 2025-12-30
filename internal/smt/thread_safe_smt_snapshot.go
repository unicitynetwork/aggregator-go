package smt

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ThreadSafeSmtSnapshot provides thread-safe access to SMT snapshot operations for writing only
// It wraps an SmtSnapshot and provides thread-safety for write operations (AddLeaf, AddLeaves)
// Read operations should be performed on the original ThreadSafeSMT after committing the snapshot
type ThreadSafeSmtSnapshot struct {
	snapshot *SmtSnapshot
	rwMux    sync.RWMutex // RWMutex allows multiple readers but exclusive writers
}

// NewThreadSafeSmtSnapshot creates a new thread-safe SMT snapshot wrapper
func NewThreadSafeSmtSnapshot(snapshot *SmtSnapshot) *ThreadSafeSmtSnapshot {
	return &ThreadSafeSmtSnapshot{
		snapshot: snapshot,
	}
}

// AddLeaves adds multiple leaves to the snapshot in a batch operation
// This operation is exclusive and blocks all other operations on this snapshot
func (tss *ThreadSafeSmtSnapshot) AddLeaves(leaves []*Leaf) (string, error) {
	tss.rwMux.Lock()
	defer tss.rwMux.Unlock()

	return tss.addLeavesUnsafe(leaves)
}

// addLeavesUnsafe adds multiple leaves without acquiring locks (internal use)
func (tss *ThreadSafeSmtSnapshot) addLeavesUnsafe(leaves []*Leaf) (string, error) {
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

func (tss *ThreadSafeSmtSnapshot) GetPath(path *big.Int) (*api.MerkleTreePath, error) {
	tss.rwMux.RLock()
	defer tss.rwMux.RUnlock()

	return tss.snapshot.GetPath(path)
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
}

// SetCommitTarget changes the target tree for snapshot chaining.
func (tss *ThreadSafeSmtSnapshot) SetCommitTarget(target *ThreadSafeSMT) {
	tss.rwMux.Lock()
	defer tss.rwMux.Unlock()

	target.rwMux.RLock()
	defer target.rwMux.RUnlock()

	tss.snapshot.SetCommitTarget(target.smt)
}

// CreateSnapshot creates a child snapshot for chained/pipelined processing.
func (tss *ThreadSafeSmtSnapshot) CreateSnapshot() *ThreadSafeSmtSnapshot {
	tss.rwMux.RLock()
	defer tss.rwMux.RUnlock()

	childSnapshot := tss.snapshot.CreateSnapshot()
	return NewThreadSafeSmtSnapshot(childSnapshot)
}

// WithWriteLock executes a function while holding a write lock on the snapshot
// The function receives the underlying snapshot for direct access to avoid deadlocks
func (tss *ThreadSafeSmtSnapshot) WithWriteLock(fn func(*SmtSnapshot) error) error {
	tss.rwMux.Lock()
	defer tss.rwMux.Unlock()
	return fn(tss.snapshot)
}
