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
		keyLength  int // bit length of the keys in the tree
		hasher     *api.DataHasher
		root       *NodeBranch
		isSnapshot bool              // true if this is a snapshot, false if original tree
		original   *SparseMerkleTree // reference to original tree (nil for original)
	}

	// SmtSnapshot represents a snapshot of the SMT with copy-on-write semantics
	SmtSnapshot struct {
		*SparseMerkleTree
	}
)

// NewSparseMerkleTree creates a new sparse merkle tree
func NewSparseMerkleTree(algorithm api.HashAlgorithm, keyLength int) *SparseMerkleTree {
	if keyLength <= 0 {
		panic("SMT key length must be positive")
	}
	return &SparseMerkleTree{
		keyLength:  keyLength,
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
		keyLength:  smt.keyLength,
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
func (smt *SparseMerkleTree) copyOnWriteRoot() *NodeBranch {
	if smt.original != nil && smt.root == smt.original.root {
		return newRootNode(smt.root.Left, smt.root.Right)
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

// Branch interface for tree nodes
type branch interface {
	calculateHash(hasher *api.DataHasher) *api.DataHash
	getPath() *big.Int
	isLeaf() bool
}

// LeafBranch represents a leaf node
type LeafBranch struct {
	Path  *big.Int
	Value []byte
	hash  *api.DataHash
}

// NodeBranch represents an internal node
type NodeBranch struct {
	Path  *big.Int
	Left  branch
	Right branch
	hash  *api.DataHash
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

// NewRootNode creates a new root node
func newRootNode(left, right branch) *NodeBranch {
	return newNodeBranch(big.NewInt(1), left, right)
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

	// Get the child hashes first so we can reset and reuse the hasher object
	var leftHash *api.DataHash
	if n.Left != nil {
		leftHash = n.Left.calculateHash(hasher)
	}

	var rightHash *api.DataHash
	if n.Right != nil {
		rightHash = n.Right.calculateHash(hasher)
	}

	hasher.Reset().AddData(api.CborArray(3))

	pathBytes := api.BigintEncode(n.Path)
	hasher.AddCborBytes(pathBytes)

	if leftHash == nil {
		hasher.AddCborNull()
	} else {
		hasher.AddCborBytes(leftHash.RawHash)
	}

	if rightHash == nil {
		hasher.AddCborNull()
	} else {
		hasher.AddCborBytes(rightHash.RawHash)
	}

	n.hash = hasher.GetHash()
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
	if path.BitLen()-1 != smt.keyLength {
		return fmt.Errorf("invalid key length %d, should be %d", path.BitLen()-1, smt.keyLength)
	}

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
	return smt.findLeafInBranch(smt.root, path)
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
	if path.BitLen()-1 != smt.keyLength {
		// TODO: better error handling
		fmt.Printf("SparseMerkleTree.GetPath(): invalid key length %d, should be %d", path.BitLen()-1, smt.keyLength)
		return nil
	}

	rootHash := smt.root.calculateHash(smt.hasher)
	steps := smt.generatePath(path, smt.root)

	return &api.MerkleTreePath{
		Root:  rootHash.ToHex(),
		Steps: steps,
	}
}

// generatePath recursively generates the Merkle tree path steps
func (smt *SparseMerkleTree) generatePath(remainingPath *big.Int, currentNode branch) []api.MerkleTreeStep {
	if remainingPath.BitLen() < 2 {
		panic("Invalid remaining path, must be internal logic error")
	}

	if currentNode.isLeaf() {
		// We are in a leaf branch, no further navigation
		// Create the corresponding leaf hash step
		currentLeaf, _ := currentNode.(*LeafBranch)
		path := currentLeaf.Path.String()
		data := hex.EncodeToString(currentLeaf.Value)
		return []api.MerkleTreeStep{
			{Path: path, Data: &data},
		}
	}

	currentBranch, ok := currentNode.(*NodeBranch)
	if !ok {
		panic("Unknown target branch type")
	}

	commonPath := calculateCommonPath(remainingPath, currentBranch.Path)
	if commonPath.length < uint(currentBranch.Path.BitLen()-1) {
		// Remaining path diverges or ends here
		// Root node is a special case, because of its empty path
		// Create the corresponding 2-step proof
		// No nil children in non-root nodes
		leftHash := hex.EncodeToString(currentBranch.Left.calculateHash(smt.hasher).RawHash)
		rightHash := hex.EncodeToString(currentBranch.Right.calculateHash(smt.hasher).RawHash)
		// This looks weird, but see the effect in api.MerkleTreePath.Verify()
		return []api.MerkleTreeStep{
			{Path: "0", Data: &rightHash},
			{Path: currentBranch.Path.String(), Data: &leftHash},
		}
	}

	// Trim remaining path for descending into subtree
	remainingPath = new(big.Int).Rsh(remainingPath, commonPath.length)

	var target, sibling branch
	if remainingPath.Bit(0) == 0 {
		// Target in the left child
		target = currentBranch.Left
		sibling = currentBranch.Right
	} else {
		// Target in the right child
		target = currentBranch.Right
		sibling = currentBranch.Left
	}

	if target == nil {
		// Target branch empty
		// This can happen only at the root node
		// Create the 2-step exclusion proof
		// There may be nil children here
		var leftHash, rightHash *string
		if currentBranch.Left != nil {
			tmp := hex.EncodeToString(currentBranch.Left.calculateHash(smt.hasher).RawHash)
			leftHash = &tmp
		}
		if currentBranch.Right != nil {
			tmp := hex.EncodeToString(currentBranch.Right.calculateHash(smt.hasher).RawHash)
			rightHash = &tmp
		}
		// This looks weird, but see the effect in api.MerkleTreePath.Verify()
		return []api.MerkleTreeStep{
			{Path: "0", Data: rightHash},
			{Path: "1", Data: leftHash},
		}
	}

	steps := smt.generatePath(remainingPath, target)

	// Add the step for the current branch
	step := api.MerkleTreeStep{
		Path: currentBranch.Path.String(),
	}
	if sibling != nil {
		tmp := hex.EncodeToString(sibling.calculateHash(smt.hasher).RawHash)
		step.Data = &tmp
	}
	return append(steps, step)
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

	mask := new(big.Int).SetBit(big.NewInt(0), pos, 1) // mask = 2^pos
	res := new(big.Int).Sub(mask, big.NewInt(1))       // res = mask - 1
	res.And(res, path1)                                // res &= path
	res.Or(res, mask)                                  // res |= mask

	return struct {
		length uint
		path   *big.Int
	}{uint(pos), res}
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
