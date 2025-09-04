package smt

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

var (
	ErrLeafExists = errors.New("smt: leaf already exists")
)

// goroutineLimiter is a semaphore to limit the number of concurrent goroutines
// during hash calculations to prevent resource exhaustion with large trees
// Default is sequential processing (no goroutines)
var goroutineLimiter chan struct{}

// init initializes the goroutine limiter to default (sequential processing)
func init() {
	// Default to sequential processing (no concurrent goroutines)
	goroutineLimiter = make(chan struct{})
}

// SetMaxConcurrentGoroutines allows configuring the maximum number of goroutines
// used during hash calculations. This should be called before creating trees or
// calculating hashes. Setting to 0 will use sequential processing.
// Note: The -1 value (CPU-based default) is handled in config loading.
func SetMaxConcurrentGoroutines(maxGoroutines int) {
	if maxGoroutines == 0 {
		// Disable concurrent processing (sequential)
		goroutineLimiter = make(chan struct{})
	} else {
		// Set specific limit
		goroutineLimiter = make(chan struct{}, maxGoroutines)
	}
}

// GetMaxConcurrentGoroutines returns the current maximum number of goroutines
// that can be used concurrently for hash calculations. Returns 0 if sequential
// processing is enabled.
func GetMaxConcurrentGoroutines() int {
	return cap(goroutineLimiter)
}

type (
	// SparseMerkleTree implements a sparse merkle tree compatible with Unicity SDK
	SparseMerkleTree struct {
		algorithm  api.HashAlgorithm
		root       *RootNode
		isSnapshot bool              // true if this is a snapshot, false if original tree
		original   *SparseMerkleTree // reference to original tree (nil for original)
	}

	// SmtSnapshot represents a snapshot of the SMT with copy-on-write semantics
	SmtSnapshot struct {
		*SparseMerkleTree
	}
)

// NewSparseMerkleTree creates a new sparse merkle tree
func NewSparseMerkleTree(algorithm api.HashAlgorithm) *SparseMerkleTree {
	return &SparseMerkleTree{
		algorithm:  algorithm,
		root:       NewRootNode(algorithm, nil, nil),
		isSnapshot: false,
		original:   nil,
	}
}

// CreateSnapshot creates a snapshot of the current SMT state
// The snapshot shares nodes with the original tree (copy-on-write)
func (smt *SparseMerkleTree) CreateSnapshot() *SmtSnapshot {
	snapshot := &SparseMerkleTree{
		algorithm:  smt.algorithm,
		root:       smt.root, // Share the root initially
		isSnapshot: true,
		original:   smt,
	}
	return &SmtSnapshot{SparseMerkleTree: snapshot}
}

// Commit commits the snapshot changes back to the original tree
func (snapshot *SmtSnapshot) Commit() {
	if snapshot.original != nil {
		snapshot.original.root = snapshot.root
	}
}

// AddLeaf adds a single leaf to the snapshot
func (snapshot *SmtSnapshot) AddLeaf(path *big.Int, value []byte) error {
	return snapshot.SparseMerkleTree.AddLeaf(path, value)
}

// AddLeaves adds multiple leaves to the snapshot
func (snapshot *SmtSnapshot) AddLeaves(leaves []*Leaf) error {
	return snapshot.SparseMerkleTree.AddLeaves(leaves)
}

// GetRootHash returns the current root hash of the snapshot
func (snapshot *SmtSnapshot) GetRootHash() []byte {
	return snapshot.SparseMerkleTree.GetRootHash()
}

// GetRootHashHex returns the current root hash of the snapshot as hex string
func (snapshot *SmtSnapshot) GetRootHashHex() string {
	return snapshot.SparseMerkleTree.GetRootHashHex()
}

// CanModify returns true if the tree can be modified (i.e., it's a snapshot)
func (smt *SparseMerkleTree) CanModify() bool {
	return smt.isSnapshot
}

// copyOnWriteRoot creates a new root if this snapshot is sharing it with the original
func (smt *SparseMerkleTree) copyOnWriteRoot() *RootNode {
	if smt.original != nil && smt.root == smt.original.root {
		// Root is shared with original, create a copy
		return &RootNode{
			Left:  smt.root.Left,
			Right: smt.root.Right,
			Path:  new(big.Int).Set(smt.root.Path),
		}
	}
	return smt.root
}

// cloneBranch creates a deep copy of a branch for copy-on-write
func (smt *SparseMerkleTree) cloneBranch(branch Branch) Branch {
	if branch == nil {
		return nil
	}

	if branch.IsLeaf() {
		leafBranch := branch.(*LeafBranch)
		return NewLeafBranch(smt.algorithm, leafBranch.Path, leafBranch.Value)
	} else {
		nodeBranch := branch.(*NodeBranch)
		return NewNodeBranch(smt.algorithm, nodeBranch.Path, nodeBranch.Left, nodeBranch.Right)
	}
}

// RootNode represents the root of the tree
type RootNode struct {
	Left  Branch
	Right Branch
	Path  *big.Int
}

// Branch interface for tree nodes (matches TypeScript Branch interface)
type Branch interface {
	CalculateHash(algo api.HashAlgorithm) *api.DataHash
	GetPath() *big.Int
	IsLeaf() bool
}

// LeafBranch represents a leaf node (matches TypeScript LeafBranch)
type LeafBranch struct {
	Path  *big.Int
	Value []byte
	hash  *api.DataHash
}

// NodeBranch represents an internal node (matches TypeScript NodeBranch)
type NodeBranch struct {
	Algorithm    api.HashAlgorithm
	Path         *big.Int
	Left         Branch
	Right        Branch
	childrenHash *api.DataHash
	hash         *api.DataHash
}

// NewRootNode creates a new root node
func NewRootNode(algorithm api.HashAlgorithm, left, right Branch) *RootNode {
	return &RootNode{
		Left:  left,
		Right: right,
		Path:  big.NewInt(1),
	}
}

// CalculateHash calculates root hash with adaptive parallel processing
func (r *RootNode) CalculateHash(algo api.HashAlgorithm) *api.DataHash {
	return api.NewDataHash(algo, calculateHashData(r.Left, r.Right, algo))
}

// NewLeafBranch creates a leaf branch
func NewLeafBranch(algorithm api.HashAlgorithm, path *big.Int, value []byte) *LeafBranch {
	leaf := &LeafBranch{
		Path:  new(big.Int).Set(path),
		Value: append([]byte(nil), value...),
	}

	// Calculate hash: BigintConverter.encode(path) + value
	pathBytes := api.BigintEncode(path)
	data := append(pathBytes, value...)
	hashData := api.Sha256Hash(data)

	leaf.hash = api.NewDataHash(algorithm, hashData)
	return leaf
}

// NewLeafBranchLazy creates a leaf branch without calculating hash (for batch operations)
func NewLeafBranchLazy(algorithm api.HashAlgorithm, path *big.Int, value []byte) *LeafBranch {
	return &LeafBranch{
		Path:  new(big.Int).Set(path),
		Value: append([]byte(nil), value...),
		hash:  nil, // Hash will be calculated on demand
	}
}

func (l *LeafBranch) CalculateHash(algo api.HashAlgorithm) *api.DataHash {
	if l.hash != nil {
		return l.hash
	}

	pathBytes := api.BigintEncode(l.Path)
	data := append(pathBytes, l.Value...)
	hashData := api.Sha256Hash(data)

	l.hash = api.NewDataHash(algo, hashData)
	return l.hash
}

func (l *LeafBranch) GetPath() *big.Int {
	return l.Path
}

func (l *LeafBranch) IsLeaf() bool {
	return true
}

// NewNodeBranch creates a node branch
func NewNodeBranch(algorithm api.HashAlgorithm, path *big.Int, left, right Branch) *NodeBranch {
	node := &NodeBranch{
		Algorithm: algorithm,
		Path:      new(big.Int).Set(path),
		Left:      left,
		Right:     right,
	}

	// Calculate children hash first
	leftHash := left.CalculateHash(algorithm).Data
	rightHash := right.CalculateHash(algorithm).Data
	combined := append(leftHash, rightHash...)
	childrenHashData := api.Sha256Hash(combined)
	node.childrenHash = api.NewDataHash(algorithm, childrenHashData)

	// Calculate node hash: BigintConverter.encode(path) + childrenHash
	pathBytes := api.BigintEncode(path)
	data := append(pathBytes, childrenHashData...)
	hashData := api.Sha256Hash(data)
	node.hash = api.NewDataHash(algorithm, hashData)

	return node
}

// NewNodeBranchLazy creates a node branch without calculating hashes (for batch operations)
func NewNodeBranchLazy(algorithm api.HashAlgorithm, path *big.Int, left, right Branch) *NodeBranch {
	return &NodeBranch{
		Algorithm:    algorithm,
		Path:         new(big.Int).Set(path),
		Left:         left,
		Right:        right,
		childrenHash: nil, // Hashes will be calculated on demand
		hash:         nil,
	}
}

func (n *NodeBranch) childrenHashData() []byte {
	return calculateHashData(n.Left, n.Right, n.Algorithm)
}

func calculateHashData(left, right Branch, algorithm api.HashAlgorithm) []byte {
	var leftData, rightData []byte

	// Only use a goroutine if both children exist (real parallel work)
	if left != nil && right != nil {
		var wg sync.WaitGroup

		// Try to acquire a slot in the goroutine limiter
		select {
		case goroutineLimiter <- struct{}{}:
			// Successfully acquired a slot, use goroutine for left child
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-goroutineLimiter }() // Release the slot when done
				leftData = left.CalculateHash(algorithm).Data
			}()

			// Calculate right child hash in current goroutine (no extra goroutine needed)
			rightData = right.CalculateHash(algorithm).Data

			// Wait for the left goroutine to complete
			wg.Wait()
		default:
			// No available slots, fall back to sequential processing
			leftData = left.CalculateHash(algorithm).Data
			rightData = right.CalculateHash(algorithm).Data
		}
	} else {
		// Sequential processing when only one or no children exist
		if left != nil {
			leftData = left.CalculateHash(algorithm).Data
		} else {
			leftData = []byte{0}
		}

		if right != nil {
			rightData = right.CalculateHash(algorithm).Data
		} else {
			rightData = []byte{0}
		}
	}

	return api.Sha256Hash(append(leftData, rightData...))
}

func (n *NodeBranch) CalculateHash(algo api.HashAlgorithm) *api.DataHash {
	if n.hash != nil {
		return n.hash
	}

	// Recalculate if needed
	childrenHashData := n.childrenHashData()
	n.childrenHash = api.NewDataHash(algo, childrenHashData)

	pathBytes := api.BigintEncode(n.Path)
	data := append(pathBytes, childrenHashData...)
	hashData := api.Sha256Hash(data)
	n.hash = api.NewDataHash(algo, hashData)

	return n.hash
}

func (n *NodeBranch) GetPath() *big.Int {
	return n.Path
}

func (n *NodeBranch) IsLeaf() bool {
	return false
}

// AddLeaf adds a single leaf to the tree
func (smt *SparseMerkleTree) AddLeaf(path *big.Int, value []byte) error {
	// Implement copy-on-write for snapshots only
	if smt.isSnapshot {
		smt.root = smt.copyOnWriteRoot()
	}

	// TypeScript: const isRight = path & 1n;
	isRight := new(big.Int).And(path, big.NewInt(1)).Cmp(big.NewInt(0)) != 0

	var left, right Branch

	if isRight {
		left = smt.root.Left
		if smt.root.Right != nil {
			// Clone the branch before modifying it if this is a snapshot
			var rightBranch Branch
			if smt.isSnapshot {
				rightBranch = smt.cloneBranch(smt.root.Right)
			} else {
				rightBranch = smt.root.Right
			}
			newRight, err := smt.buildTree(rightBranch, path, value)
			if err != nil {
				return err
			}
			right = newRight
		} else {
			right = NewLeafBranch(smt.algorithm, path, value)
		}
	} else {
		if smt.root.Left != nil {
			// Clone the branch before modifying it if this is a snapshot
			var leftBranch Branch
			if smt.isSnapshot {
				leftBranch = smt.cloneBranch(smt.root.Left)
			} else {
				leftBranch = smt.root.Left
			}
			newLeft, err := smt.buildTree(leftBranch, path, value)
			if err != nil {
				return err
			}
			left = newLeft
		} else {
			left = NewLeafBranch(smt.algorithm, path, value)
		}
		right = smt.root.Right
	}

	smt.root = NewRootNode(smt.algorithm, left, right)
	return nil
}

// AddLeaves adds multiple leaves efficiently (batch operation for performance) to the existing tree
// This produces the EXACT same tree structure as sequential AddLeaf calls,
// and maintains the existing tree structure when adding new leaves
func (smt *SparseMerkleTree) AddLeaves(leaves []*Leaf) error {
	if len(leaves) == 0 {
		return nil
	}

	// Implement copy-on-write for snapshots only
	if smt.isSnapshot {
		smt.root = smt.copyOnWriteRoot()
	}

	// Add leaves one by one to the existing tree using AddLeaf
	// This ensures that new leaves are added to the existing tree structure
	for _, leaf := range leaves {
		err := smt.addLeafBatch(leaf.Path, leaf.Value)
		if err != nil {
			if errors.Is(err, ErrLeafExists) {
				// Skip duplicate leaves silently
				continue
			}
			return err
		}
	}

	return nil
}

// addLeafBatch is optimized for batch operations using lazy hash calculation
func (smt *SparseMerkleTree) addLeafBatch(path *big.Int, value []byte) error {
	// Copy-on-write for snapshots only
	if smt.isSnapshot {
		smt.root = smt.copyOnWriteRoot()
	}

	isRight := new(big.Int).And(path, big.NewInt(1)).Cmp(big.NewInt(0)) != 0

	var left, right Branch

	if isRight {
		left = smt.root.Left
		if smt.root.Right != nil {
			// Clone the branch before modifying it if this is a snapshot
			var rightBranch Branch
			if smt.isSnapshot {
				rightBranch = smt.cloneBranch(smt.root.Right)
			} else {
				rightBranch = smt.root.Right
			}
			newRight, err := smt.buildTreeLazy(rightBranch, path, value)
			if err != nil {
				return err
			}
			right = newRight
		} else {
			right = NewLeafBranchLazy(smt.algorithm, path, value)
		}
	} else {
		if smt.root.Left != nil {
			// Clone the branch before modifying it if this is a snapshot
			var leftBranch Branch
			if smt.isSnapshot {
				leftBranch = smt.cloneBranch(smt.root.Left)
			} else {
				leftBranch = smt.root.Left
			}
			newLeft, err := smt.buildTreeLazy(leftBranch, path, value)
			if err != nil {
				return err
			}
			left = newLeft
		} else {
			left = NewLeafBranchLazy(smt.algorithm, path, value)
		}
		right = smt.root.Right
	}

	smt.root = NewRootNode(smt.algorithm, left, right)
	return nil
}

// buildTreeLazy correctly handles all insertion scenarios for a "no-overwrite" SMT.
func (smt *SparseMerkleTree) buildTreeLazy(branch Branch, remainingPath *big.Int, value []byte) (Branch, error) {
	// --- Case 1: The existing branch is a LEAF ---
	if branch.IsLeaf() {
		leafBranch := branch.(*LeafBranch)

		// Case 1a: DUPLICATE path. This is an error in a no-overwrite tree.
		// TODO: if values don't match, return a different error
		if remainingPath.Cmp(leafBranch.Path) == 0 {
			return nil, fmt.Errorf("path '%s': %w", remainingPath, ErrLeafExists)
		}

		// Case 1b: Paths are different, a SPLIT is required.
		commonPath := calculateCommonPath(remainingPath, leafBranch.Path)

		newBranchPath := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
		newBranch := NewLeafBranchLazy(smt.algorithm, newBranchPath, value)

		oldBranchPath := new(big.Int).Rsh(leafBranch.Path, uint(commonPath.length.Uint64()))
		oldBranch := NewLeafBranchLazy(smt.algorithm, oldBranchPath, leafBranch.Value)

		shiftedRemaining := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
		isNewBranchRight := new(big.Int).And(shiftedRemaining, big.NewInt(1)).Cmp(big.NewInt(0)) != 0

		if isNewBranchRight {
			return NewNodeBranchLazy(smt.algorithm, commonPath.path, oldBranch, newBranch), nil
		}
		return NewNodeBranchLazy(smt.algorithm, commonPath.path, newBranch, oldBranch), nil
	}

	// --- Case 2: The existing branch is an internal NODE ---
	nodeBranch := branch.(*NodeBranch)
	commonPath := calculateCommonPath(remainingPath, nodeBranch.Path)

	// Case 2a: The Node's path is a prefix of the new leaf's path. Go deeper.
	if commonPath.path.Cmp(nodeBranch.Path) == 0 {
		shiftedRemaining := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
		isRight := new(big.Int).And(shiftedRemaining, big.NewInt(1)).Cmp(big.NewInt(0)) != 0

		if isRight {
			if nodeBranch.Right == nil {
				return NewNodeBranchLazy(smt.algorithm, nodeBranch.Path, nodeBranch.Left, NewLeafBranchLazy(smt.algorithm, shiftedRemaining, value)), nil
			}
			newRight, err := smt.buildTreeLazy(nodeBranch.Right, shiftedRemaining, value)
			if err != nil {
				return nil, err
			}
			return NewNodeBranchLazy(smt.algorithm, nodeBranch.Path, nodeBranch.Left, newRight), nil
		}
		// Go left
		if nodeBranch.Left == nil {
			return NewNodeBranchLazy(smt.algorithm, nodeBranch.Path, NewLeafBranchLazy(smt.algorithm, shiftedRemaining, value), nodeBranch.Right), nil
		}
		newLeft, err := smt.buildTreeLazy(nodeBranch.Left, shiftedRemaining, value)
		if err != nil {
			return nil, err
		}
		return NewNodeBranchLazy(smt.algorithm, nodeBranch.Path, newLeft, nodeBranch.Right), nil
	}

	// Case 2b: Paths diverge before the end of the Node's path. Split the node.
	newLeafBranchPath := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
	newLeafBranch := NewLeafBranchLazy(smt.algorithm, newLeafBranchPath, value)

	oldNodeBranchPath := new(big.Int).Rsh(nodeBranch.Path, uint(commonPath.length.Uint64()))
	oldNodeBranch := NewNodeBranchLazy(smt.algorithm, oldNodeBranchPath, nodeBranch.Left, nodeBranch.Right)

	shiftedRemaining := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
	isNewBranchRight := new(big.Int).And(shiftedRemaining, big.NewInt(1)).Cmp(big.NewInt(0)) != 0

	if isNewBranchRight {
		return NewNodeBranchLazy(smt.algorithm, commonPath.path, oldNodeBranch, newLeafBranch), nil
	}
	return NewNodeBranchLazy(smt.algorithm, commonPath.path, newLeafBranch, oldNodeBranch), nil
}

// GetRootHash returns the root hash as hex string with imprint
func (smt *SparseMerkleTree) GetRootHash() []byte {
	return smt.root.CalculateHash(smt.algorithm).Imprint
}

// GetRootHashHex returns the root hash as hex string
func (smt *SparseMerkleTree) GetRootHashHex() string {
	return smt.root.CalculateHash(smt.algorithm).ToHex()
}

// GetLeaf retrieves a leaf by path (for compatibility)
func (smt *SparseMerkleTree) GetLeaf(path *big.Int) (*LeafBranch, error) {
	return smt.findLeaf(smt.root, path)
}

// findLeaf searches for a leaf in the tree following the same logic as addLeaf
func (smt *SparseMerkleTree) findLeaf(node interface{}, targetPath *big.Int) (*LeafBranch, error) {
	switch n := node.(type) {
	case *RootNode:
		// At root, use bit 0 to navigate (same as AddLeaf)
		isRight := new(big.Int).And(targetPath, big.NewInt(1)).Cmp(big.NewInt(0)) != 0
		if isRight && n.Right != nil {
			return smt.findLeafInBranch(n.Right, targetPath)
		} else if !isRight && n.Left != nil {
			return smt.findLeafInBranch(n.Left, targetPath)
		}
		return nil, fmt.Errorf("leaf not found")

	default:
		return nil, fmt.Errorf("invalid node type")
	}
}

// findLeafInBranch searches within a branch, handling path shifting correctly
func (smt *SparseMerkleTree) findLeafInBranch(branch Branch, targetPath *big.Int) (*LeafBranch, error) {
	switch b := branch.(type) {
	case *LeafBranch:
		if b.Path.Cmp(targetPath) == 0 {
			return b, nil
		}
		return nil, fmt.Errorf("leaf not found")

	case *NodeBranch:
		// Mirror the buildTree logic exactly
		commonPath := calculateCommonPath(targetPath, b.Path)

		// Check if targetPath can be in this subtree
		if commonPath.path.Cmp(targetPath) == 0 {
			return nil, fmt.Errorf("leaf not found")
		}

		// Navigate using the same logic as buildTree
		shifted := new(big.Int).Rsh(targetPath, uint(commonPath.length.Uint64()))
		isRight := new(big.Int).And(shifted, big.NewInt(1)).Cmp(big.NewInt(0)) != 0

		// KEY FIX: Pass the shifted path to match tree construction
		if isRight && b.Right != nil {
			return smt.findLeafInBranch(b.Right, shifted)
		} else if !isRight && b.Left != nil {
			return smt.findLeafInBranch(b.Left, shifted)
		}

		return nil, fmt.Errorf("leaf not found")

	default:
		return nil, fmt.Errorf("invalid branch type")
	}
}

// buildTree matches TypeScript buildTree logic exactly
func (smt *SparseMerkleTree) buildTree(branch Branch, remainingPath *big.Int, value []byte) (Branch, error) {
	commonPath := calculateCommonPath(remainingPath, branch.GetPath())

	// TypeScript: const isRight = (remainingPath >> commonPath.length) & 1n;
	shifted := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
	isRight := new(big.Int).And(shifted, big.NewInt(1)).Cmp(big.NewInt(0)) != 0

	if commonPath.path.Cmp(remainingPath) == 0 {
		return nil, fmt.Errorf("cannot add leaf inside branch, commonPath: '%s', remainingPath: '%s'", commonPath.path, remainingPath)
	}

	// If a leaf must be split from the middle
	if branch.IsLeaf() {
		leafBranch := branch.(*LeafBranch)
		if commonPath.path.Cmp(leafBranch.Path) == 0 {
			return nil, fmt.Errorf("cannot extend tree through leaf")
		}

		// TypeScript: branch.path >> commonPath.length
		oldBranchPath := new(big.Int).Rsh(leafBranch.Path, uint(commonPath.length.Uint64()))
		oldBranch := NewLeafBranch(smt.algorithm, oldBranchPath, leafBranch.Value)

		// TypeScript: remainingPath >> commonPath.length
		newBranchPath := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
		newBranch := NewLeafBranch(smt.algorithm, newBranchPath, value)

		if isRight {
			return NewNodeBranch(smt.algorithm, commonPath.path, oldBranch, newBranch), nil
		} else {
			return NewNodeBranch(smt.algorithm, commonPath.path, newBranch, oldBranch), nil
		}
	}

	// If node branch is split in the middle
	nodeBranch := branch.(*NodeBranch)
	if commonPath.path.Cmp(nodeBranch.Path) < 0 {
		newBranchPath := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
		newBranch := NewLeafBranch(smt.algorithm, newBranchPath, value)

		oldBranchPath := new(big.Int).Rsh(nodeBranch.Path, uint(commonPath.length.Uint64()))
		oldBranch := NewNodeBranch(smt.algorithm, oldBranchPath, nodeBranch.Left, nodeBranch.Right)

		if isRight {
			return NewNodeBranch(smt.algorithm, commonPath.path, oldBranch, newBranch), nil
		} else {
			return NewNodeBranch(smt.algorithm, commonPath.path, newBranch, oldBranch), nil
		}
	}

	if isRight {
		newRight, err := smt.buildTree(nodeBranch.Right, new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64())), value)
		if err != nil {
			return nil, err
		}
		return NewNodeBranch(smt.algorithm, nodeBranch.Path, nodeBranch.Left, newRight), nil
	} else {
		newLeft, err := smt.buildTree(nodeBranch.Left, new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64())), value)
		if err != nil {
			return nil, err
		}
		return NewNodeBranch(smt.algorithm, nodeBranch.Path, newLeft, nodeBranch.Right), nil
	}
}

func (smt *SparseMerkleTree) GetPath(path *big.Int) *api.MerkleTreePath {
	rootHash := smt.root.CalculateHash(smt.algorithm)
	steps := smt.generatePath(path, smt.root.Left, smt.root.Right)

	return &api.MerkleTreePath{
		Root:  rootHash.ToHex(),
		Steps: steps,
	}
}

// generatePath recursively generates the Merkle tree path steps
func (smt *SparseMerkleTree) generatePath(remainingPath *big.Int, left, right Branch) []api.MerkleTreeStep {
	// Determine if we should go right (remainingPath & 1n)
	isRight := new(big.Int).And(remainingPath, big.NewInt(1)).Cmp(big.NewInt(0)) != 0

	var branch, siblingBranch Branch
	if isRight {
		branch = right
		siblingBranch = left
	} else {
		branch = left
		siblingBranch = right
	}

	if branch == nil {
		// No branch exists at this position - create step without branch
		step := api.MerkleTreeStep{
			Path:    remainingPath.String(),
			Branch:  nil, // nil indicates no branch exists
			Sibling: nil,
		}
		if siblingBranch != nil {
			siblingHash := siblingBranch.CalculateHash(smt.algorithm)
			siblingHex := siblingHash.ToHex()
			step.Sibling = &siblingHex
		}
		return []api.MerkleTreeStep{step}
	}

	commonPath := calculateCommonPath(remainingPath, branch.GetPath())

	if branch.GetPath().Cmp(commonPath.path) == 0 {
		if branch.IsLeaf() {
			return []api.MerkleTreeStep{smt.createMerkleTreeStep(branch.GetPath(), branch, siblingBranch)}
		}

		// If path has ended, return the current non-leaf branch data
		shifted := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
		if shifted.Cmp(big.NewInt(1)) == 0 {
			return []api.MerkleTreeStep{smt.createMerkleTreeStep(branch.GetPath(), branch, siblingBranch)}
		}

		// Continue recursively into the branch
		nodeBranch, ok := branch.(*NodeBranch)
		if !ok {
			// Should not happen if IsLeaf() returned false
			return []api.MerkleTreeStep{smt.createMerkleTreeStep(branch.GetPath(), branch, siblingBranch)}
		}

		// Recursively generate path for the shifted remaining path
		shiftedRemaining := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
		recursiveSteps := smt.generatePath(shiftedRemaining, nodeBranch.Left, nodeBranch.Right)

		// Create the current step without branch (since we went into it)
		currentStep := smt.createMerkleTreeStep(branch.GetPath(), nil, siblingBranch)

		// Prepend recursive steps to current step (TypeScript: [...recursiveSteps, currentStep])
		steps := make([]api.MerkleTreeStep, 0, len(recursiveSteps)+1)
		steps = append(steps, recursiveSteps...)
		steps = append(steps, currentStep)
		return steps
	}

	return []api.MerkleTreeStep{smt.createMerkleTreeStep(branch.GetPath(), branch, siblingBranch)}
}

// createMerkleTreeStep creates a api.MerkleTreeStep with proper branch and sibling handling
func (smt *SparseMerkleTree) createMerkleTreeStep(path *big.Int, branch, siblingBranch Branch) api.MerkleTreeStep {
	step := api.MerkleTreeStep{
		Path:    path.String(),
		Branch:  nil, // Initialize as nil
		Sibling: nil,
	}

	// Add branch data
	if branch != nil {
		// Branch exists, add its data
		step.Branch = []string{}
		if leafBranch, ok := branch.(*LeafBranch); ok {
			step.Branch = []string{hex.EncodeToString(leafBranch.Value)}
		} else {
			// Otherwise use branch children hash data
			step.Branch = []string{hex.EncodeToString(branch.(*NodeBranch).childrenHashData())}
		}
	} else {
		// No branch, but we need to distinguish between:
		// - TypeScript's createWithoutBranch (branch = null)
		// - TypeScript's create with null value (branch = empty)
		// Based on the TypeScript code, when we pass null to create, it creates empty branch
		step.Branch = []string{}
	}

	// Add sibling hash if sibling exists
	if siblingBranch != nil {
		siblingHash := siblingBranch.CalculateHash(smt.algorithm)
		siblingHex := siblingHash.ToHex()
		step.Sibling = &siblingHex
	}

	return step
}

// calculateCommonPath matches TypeScript calculateCommonPath exactly
func calculateCommonPath(path1, path2 *big.Int) struct {
	length *big.Int
	path   *big.Int
} {
	path := big.NewInt(1)
	mask := big.NewInt(1)
	length := big.NewInt(0)

	for {
		// Check (path1 & mask) === (path2 & mask)
		mask1 := new(big.Int).And(path1, mask)
		mask2 := new(big.Int).And(path2, mask)

		if mask1.Cmp(mask2) != 0 {
			break
		}

		// Check path < path1 && path < path2
		if path.Cmp(path1) >= 0 || path.Cmp(path2) >= 0 {
			break
		}

		// mask <<= 1n
		mask.Lsh(mask, 1)

		// length += 1n
		length.Add(length, big.NewInt(1))

		// path = mask | ((mask - 1n) & path1)
		maskMinus1 := new(big.Int).Sub(mask, big.NewInt(1))
		temp := new(big.Int).And(maskMinus1, path1)
		path.Or(mask, temp)
	}

	return struct {
		length *big.Int
		path   *big.Int
	}{length, path}
}

// Leaf represents a leaf to be inserted (for batch operations)
type Leaf struct {
	Path  *big.Int
	Value []byte
}

// NewLeaf creates a new leaf
func NewLeaf(path *big.Int, value []byte) *Leaf {
	return &Leaf{
		Path:  new(big.Int).Set(path),
		Value: append([]byte(nil), value...),
	}
}
