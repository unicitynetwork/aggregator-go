package smt

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ThreadSafeSMT provides thread-safe access to SMT operations
// It prevents concurrent access during batch operations and proof generation
type ThreadSafeSMT struct {
	smt   *SparseMerkleTree
	rwMux sync.RWMutex // RWMutex allows multiple readers but exclusive writers
}

// NewThreadSafeSMT creates a new thread-safe SMT wrapper
func NewThreadSafeSMT(smtInstance *SparseMerkleTree) *ThreadSafeSMT {
	// Prime hash caches before publishing the wrapper so no later read path
	// under RLock needs to mutate node state on a freshly constructed tree.
	smtInstance.ensureHashes()
	return &ThreadSafeSMT{
		smt: smtInstance,
	}
}

// AddLeaves adds multiple leaves to the SMT in a batch operation
// This operation is exclusive and blocks all other operations
func (ts *ThreadSafeSMT) AddLeaves(leaves []*Leaf) (string, error) {
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

	if err := ts.smt.AddLeaf(path, value); err != nil {
		return err
	}
	ts.smt.ensureHashes()
	return nil
}

// AddPreHashedLeaf adds a leaf where the value is already a hash calculated externally
// This operation is exclusive and blocks all other operations
func (ts *ThreadSafeSMT) AddPreHashedLeaf(path *big.Int, hash []byte) error {
	ts.rwMux.Lock()
	defer ts.rwMux.Unlock()

	if err := ts.smt.AddLeaf(path, hash); err != nil {
		return err
	}
	ts.smt.ensureHashes()
	return nil
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
func (ts *ThreadSafeSMT) GetLeaf(path *big.Int) (*LeafBranch, error) {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()

	return ts.smt.GetLeaf(path)
}

// GetPath generates a Merkle tree path for the given path
// This is a read operation and allows concurrent access
func (ts *ThreadSafeSMT) GetPath(path *big.Int) (*api.MerkleTreePath, error) {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()
	return ts.smt.GetPath(path)
}

// GetRootHashRaw returns the raw 32-byte root hash without algorithm prefix.
// This is a read operation and allows concurrent access.
func (ts *ThreadSafeSMT) GetRootHashRaw() []byte {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()
	return ts.smt.GetRootHashRaw()
}

// GetInclusionCert builds a v2 inclusion certificate for the leaf
// at the given raw 32-byte key. This is a read operation and allows
// concurrent access.
func (ts *ThreadSafeSMT) GetInclusionCert(key []byte) (*api.InclusionCert, error) {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()
	return ts.smt.GetInclusionCert(key)
}

// GetShardInclusionFragment builds the native parent proof fragment for the
// given shard ID. This is only valid on parent-mode SMT instances.
func (ts *ThreadSafeSMT) GetShardInclusionFragment(shardID api.ShardID) (*api.ParentInclusionFragment, error) {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()
	return ts.smt.GetShardInclusionFragment(shardID)
}

// GetShardInclusionFragmentWithRoot atomically reads the parent fragment and
// the raw SMT root from the same in-memory snapshot. This avoids serving a
// fragment from one root and looking up a block for a newer root in a later
// read section.
func (ts *ThreadSafeSMT) GetShardInclusionFragmentWithRoot(shardID api.ShardID) (*api.ParentInclusionFragment, []byte, error) {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()

	parentFragment, err := ts.smt.GetShardInclusionFragment(shardID)
	if err != nil {
		return nil, nil, err
	}
	if parentFragment == nil {
		return nil, nil, nil
	}
	return parentFragment, ts.smt.GetRootHashRaw(), nil
}

// GetKeyLength exposes the configured SMT key length.
func (ts *ThreadSafeSMT) GetKeyLength() int {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()
	return ts.smt.keyLength
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
	if err := fn(); err != nil {
		return err
	}
	ts.smt.ensureHashes()
	return nil
}

// CreateSnapshot creates a thread-safe snapshot of the current SMT state
// This operation requires a read lock to ensure consistency during snapshot creation
func (ts *ThreadSafeSMT) CreateSnapshot() *ThreadSafeSmtSnapshot {
	ts.rwMux.RLock()
	defer ts.rwMux.RUnlock()

	snapshot := ts.smt.CreateSnapshot()
	return NewThreadSafeSmtSnapshot(snapshot)
}
