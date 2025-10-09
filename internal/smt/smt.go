package smt

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

var (
	ErrDuplicateLeaf    = errors.New("smt: duplicate leaf")
	ErrLeafModification = errors.New("smt: attempt to modify an existing leaf")
)

type (
	// SparseMerkleTree implements a sparse merkle tree compatible with Unicity SDK
	SparseMerkleTree struct {
		hasher     *api.DataHasher
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
		hasher:     api.NewDataHasher(algorithm),
		root:       newRootNode(nil, nil),
		isSnapshot: false,
		original:   nil,
	}
}

// CreateSnapshot creates a snapshot of the current SMT state
// The snapshot shares nodes with the original tree (copy-on-write)
func (smt *SparseMerkleTree) CreateSnapshot() *SmtSnapshot {
	snapshot := &SparseMerkleTree{
		hasher:     api.NewDataHasher(smt.hasher.GetAlgorithm()),
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
func (smt *SparseMerkleTree) cloneBranch(branch branch) branch {
	if branch == nil {
		return nil
	}

	if branch.isLeaf() {
		leafBranch := branch.(*LeafBranch)
		return newLeafBranch(leafBranch.Path, leafBranch.Value)
	} else {
		nodeBranch := branch.(*NodeBranch)
		return newNodeBranch(nodeBranch.Path, nodeBranch.Left, nodeBranch.Right)
	}
}

// RootNode represents the root of the tree
type RootNode struct {
	Left  branch
	Right branch
	Path  *big.Int
}

// Branch interface for tree nodes (matches TypeScript Branch interface)
type branch interface {
	calculateHash(hasher *api.DataHasher) *api.DataHash
	getPath() *big.Int
	isLeaf() bool
}

// LeafBranch represents a leaf node (matches TypeScript LeafBranch)
type LeafBranch struct {
	Path  *big.Int
	Value []byte
	hash  *api.DataHash
}

// NodeBranch represents an internal node (matches TypeScript NodeBranch)
type NodeBranch struct {
	Path  *big.Int
	Left  branch
	Right branch
	hash  *api.DataHash
}

// NewRootNode creates a new root node
func newRootNode(left, right branch) *RootNode {
	return &RootNode{
		Left:  left,
		Right: right,
		Path:  big.NewInt(1),
	}
}

// calculateHash calculates root hash
func (r *RootNode) calculateHash(hasher *api.DataHasher) *api.DataHash {
	// Separate hasher object that the calculateHash() calls below do not spoil
	rootHasher := api.NewDataHasher(hasher.GetAlgorithm())
	rootHasher.AddData(api.CborArray(3))

	pathBytes := api.BigintEncode(r.Path)
	rootHasher.AddCborBytes(pathBytes)

	if r.Left == nil {
		rootHasher.AddCborNull()
	} else {
		rootHasher.AddCborBytes(r.Left.calculateHash(hasher).RawHash)
	}

	if r.Right == nil {
		rootHasher.AddCborNull()
	} else {
		rootHasher.AddCborBytes(r.Right.calculateHash(hasher).RawHash)
	}

	return rootHasher.GetHash()
}

// NewLeafBranch creates a leaf branch
func newLeafBranch(path *big.Int, value []byte) *LeafBranch {
	return &LeafBranch{
		Path:  new(big.Int).Set(path),
		Value: append([]byte(nil), value...),
		// Hash will be computed on demand
	}
}

func (l *LeafBranch) calculateHash(hasher *api.DataHasher) *api.DataHash {
	if l.hash != nil {
		return l.hash
	}

	pathBytes := api.BigintEncode(l.Path)
	l.hash = hasher.Reset().AddData(api.CborArray(2)).
		AddCborBytes(pathBytes).AddCborBytes(l.Value).GetHash()
	return l.hash
}

func (l *LeafBranch) getPath() *big.Int {
	return l.Path
}

func (l *LeafBranch) isLeaf() bool {
	return true
}

// NewNodeBranch creates a node branch
func newNodeBranch(path *big.Int, left, right branch) *NodeBranch {
	return &NodeBranch{
		Path:  new(big.Int).Set(path),
		Left:  left,
		Right: right,
		// Hash will be computed on demand
	}
}

func (n *NodeBranch) calculateHash(hasher *api.DataHasher) *api.DataHash {
	if n.hash != nil {
		return n.hash
	}

	pathBytes := api.BigintEncode(n.Path)
	leftHash := n.Left.calculateHash(hasher).RawHash
	rightHash := n.Right.calculateHash(hasher).RawHash
	n.hash = hasher.Reset().AddData(api.CborArray(3)).
		AddCborBytes(pathBytes).AddCborBytes(leftHash).AddCborBytes(rightHash).GetHash()
	return n.hash
}

func (n *NodeBranch) getPath() *big.Int {
	return n.Path
}

func (n *NodeBranch) isLeaf() bool {
	return false
}

// AddLeaf adds a single leaf to the tree
func (smt *SparseMerkleTree) AddLeaf(path *big.Int, value []byte) error {
	// Implement copy-on-write for snapshots only
	if smt.isSnapshot {
		smt.root = smt.copyOnWriteRoot()
	}

	// TypeScript: const isRight = path & 1n;
	isRight := path.Bit(0) == 1

	var left, right branch

	if isRight {
		left = smt.root.Left
		if smt.root.Right != nil {
			// Clone the branch before modifying it if this is a snapshot
			var rightBranch branch
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
			right = newLeafBranch(path, value)
		}
	} else {
		if smt.root.Left != nil {
			// Clone the branch before modifying it if this is a snapshot
			var leftBranch branch
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
			left = newLeafBranch(path, value)
		}
		right = smt.root.Right
	}

	smt.root = newRootNode(left, right)
	return nil
}

// AddLeaves adds multiple leaves to the tree
func (smt *SparseMerkleTree) AddLeaves(leaves []*Leaf) error {
	if len(leaves) == 0 {
		return nil
	}

	for _, leaf := range leaves {
		err := smt.AddLeaf(leaf.Path, leaf.Value)
		if err != nil {
			if errors.Is(err, ErrDuplicateLeaf) {
				// Skip duplicate leaves silently
				continue
			}
			return err
		}
	}

	return nil
}

// GetRootHash returns the root hash as imprint
func (smt *SparseMerkleTree) GetRootHash() []byte {
	return smt.root.calculateHash(smt.hasher).GetImprint()
}

// GetRootHashHex returns the root hash as hex string
func (smt *SparseMerkleTree) GetRootHashHex() string {
	return smt.root.calculateHash(smt.hasher).ToHex()
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
		isRight := targetPath.Bit(0) == 1
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
func (smt *SparseMerkleTree) findLeafInBranch(branch branch, targetPath *big.Int) (*LeafBranch, error) {
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
		shifted := new(big.Int).Rsh(targetPath, commonPath.length)
		isRight := shifted.Bit(0) == 1

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
func (smt *SparseMerkleTree) buildTree(branch branch, remainingPath *big.Int, value []byte) (branch, error) {
	// Special checks for adding a leaf that already exists in the tree
	if branch.isLeaf() && branch.getPath().Cmp(remainingPath) == 0 {
		leafBranch := branch.(*LeafBranch)
		if bytes.Equal(leafBranch.Value, value) {
			return nil, ErrDuplicateLeaf
		} else {
			return nil, ErrLeafModification
		}
	}

	commonPath := calculateCommonPath(remainingPath, branch.getPath())
	shifted := new(big.Int).Rsh(remainingPath, commonPath.length)
	isRight := shifted.Bit(0) == 1

	if commonPath.path.Cmp(remainingPath) == 0 {
		return nil, fmt.Errorf("cannot add leaf inside branch, commonPath: '%s', remainingPath: '%s'", commonPath.path, remainingPath)
	}

	// If a leaf must be split from the middle
	if branch.isLeaf() {
		leafBranch := branch.(*LeafBranch)
		if commonPath.path.Cmp(leafBranch.Path) == 0 {
			return nil, fmt.Errorf("cannot extend tree through leaf")
		}

		// TypeScript: branch.path >> commonPath.length
		oldBranchPath := new(big.Int).Rsh(leafBranch.Path, commonPath.length)
		oldBranch := newLeafBranch(oldBranchPath, leafBranch.Value)

		// TypeScript: remainingPath >> commonPath.length
		newBranchPath := new(big.Int).Rsh(remainingPath, commonPath.length)
		newBranch := newLeafBranch(newBranchPath, value)

		if isRight {
			return newNodeBranch(commonPath.path, oldBranch, newBranch), nil
		} else {
			return newNodeBranch(commonPath.path, newBranch, oldBranch), nil
		}
	}

	// If node branch is split in the middle
	nodeBranch := branch.(*NodeBranch)
	if commonPath.path.Cmp(nodeBranch.Path) < 0 {
		newBranchPath := new(big.Int).Rsh(remainingPath, commonPath.length)
		newBranch := newLeafBranch(newBranchPath, value)

		oldBranchPath := new(big.Int).Rsh(nodeBranch.Path, commonPath.length)
		oldBranch := newNodeBranch(oldBranchPath, nodeBranch.Left, nodeBranch.Right)

		if isRight {
			return newNodeBranch(commonPath.path, oldBranch, newBranch), nil
		} else {
			return newNodeBranch(commonPath.path, newBranch, oldBranch), nil
		}
	}

	if isRight {
		newRight, err := smt.buildTree(nodeBranch.Right, new(big.Int).Rsh(remainingPath, commonPath.length), value)
		if err != nil {
			return nil, err
		}
		return newNodeBranch(nodeBranch.Path, nodeBranch.Left, newRight), nil
	} else {
		newLeft, err := smt.buildTree(nodeBranch.Left, new(big.Int).Rsh(remainingPath, commonPath.length), value)
		if err != nil {
			return nil, err
		}
		return newNodeBranch(nodeBranch.Path, newLeft, nodeBranch.Right), nil
	}
}

func (smt *SparseMerkleTree) GetPath(path *big.Int) *api.MerkleTreePath {
	rootHash := smt.root.calculateHash(smt.hasher)
	steps := smt.generatePath(path, smt.root.Left, smt.root.Right)

	return &api.MerkleTreePath{
		Root:  rootHash.ToHex(),
		Steps: steps,
	}
}

// generatePath recursively generates the Merkle tree path steps
func (smt *SparseMerkleTree) generatePath(remainingPath *big.Int, left, right branch) []api.MerkleTreeStep {
	// Determine if we should go right (remainingPath & 1n)
	isRight := remainingPath.Bit(0) == 1

	var branch, siblingBranch branch
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
			Path:   remainingPath.String(),
			Branch: nil, // nil indicates no branch exists
		}
		if siblingBranch != nil {
			siblingHash := siblingBranch.calculateHash(smt.hasher)
			step.Sibling = []string{hex.EncodeToString(siblingHash.RawHash)}
		}
		// If siblingBranch is nil, leave Sibling as nil (omitempty will exclude it from JSON)
		return []api.MerkleTreeStep{step}
	}

	commonPath := calculateCommonPath(remainingPath, branch.getPath())

	if branch.getPath().Cmp(commonPath.path) == 0 {
		if branch.isLeaf() {
			return []api.MerkleTreeStep{smt.createMerkleTreeStep(branch.getPath(), branch, siblingBranch)}
		}

		// If path has ended, return the current non-leaf branch data
		shifted := new(big.Int).Rsh(remainingPath, commonPath.length)
		if shifted.Cmp(big.NewInt(1)) == 0 {
			return []api.MerkleTreeStep{smt.createMerkleTreeStep(branch.getPath(), branch, siblingBranch)}
		}

		// Continue recursively into the branch
		nodeBranch, ok := branch.(*NodeBranch)
		if !ok {
			// Should not happen if IsLeaf() returned false
			return []api.MerkleTreeStep{smt.createMerkleTreeStep(branch.getPath(), branch, siblingBranch)}
		}

		// Recursively generate path for the shifted remaining path
		shiftedRemaining := new(big.Int).Rsh(remainingPath, commonPath.length)
		recursiveSteps := smt.generatePath(shiftedRemaining, nodeBranch.Left, nodeBranch.Right)

		// Create the current step without branch (since we went into it)
		currentStep := smt.createMerkleTreeStep(branch.getPath(), nil, siblingBranch)

		// Prepend recursive steps to current step (TypeScript: [...recursiveSteps, currentStep])
		steps := make([]api.MerkleTreeStep, 0, len(recursiveSteps)+1)
		steps = append(steps, recursiveSteps...)
		steps = append(steps, currentStep)
		return steps
	}

	return []api.MerkleTreeStep{smt.createMerkleTreeStep(branch.getPath(), branch, siblingBranch)}
}

// createMerkleTreeStep creates a api.MerkleTreeStep with proper branch and sibling handling
func (smt *SparseMerkleTree) createMerkleTreeStep(path *big.Int, branch, siblingBranch branch) api.MerkleTreeStep {
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
			// Otherwise use branch hash
			branchHash := branch.calculateHash(smt.hasher)
			step.Branch = []string{hex.EncodeToString(branchHash.RawHash)}
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
		siblingHash := siblingBranch.calculateHash(smt.hasher)
		step.Sibling = []string{hex.EncodeToString(siblingHash.RawHash)}
	}
	// If siblingBranch is nil, leave Sibling as nil (omitempty will exclude it from JSON)

	return step
}

// calculateCommonPath computes the longest common prefix of path1 and path2
func calculateCommonPath(path1, path2 *big.Int) struct {
	length uint
	path   *big.Int
} {
	if path1.Sign() != 1 || path2.Sign() != 1 {
		panic("Non-positive path value")
	}

	maxPos := min(path1.BitLen(), path2.BitLen()) - 1
	pos := 0
	for pos < maxPos && path1.Bit(pos) == path2.Bit(pos) {
		pos++
	}

	var mask, res big.Int
	mask.SetBit(big.NewInt(0), pos, 1)
	res.Sub(&mask, big.NewInt(1))
	res.And(&res, path1)
	res.Or(&res, &mask)

	return struct {
		length uint
		path   *big.Int
	}{uint(pos), &res}
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
