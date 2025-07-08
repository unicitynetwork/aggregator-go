package smt

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
)

// HashAlgorithm represents the hashing algorithm to use
type HashAlgorithm int

const (
	SHA256 HashAlgorithm = iota
)

type (
	// SparseMerkleTree implements a sparse merkle tree compatible with Unicity SDK
	SparseMerkleTree struct {
		algorithm HashAlgorithm
		root      *RootNode
	}

	// MerkleTreeStep represents a single step in a Merkle tree path
	MerkleTreeStep struct {
		Branch  []string `json:"branch"`
		Path    string   `json:"path"`
		Sibling *string  `json:"sibling"`
	}

	// MerkleTreePath represents the path to verify inclusion in a Merkle tree
	MerkleTreePath struct {
		Root  string           `json:"root"`
		Steps []MerkleTreeStep `json:"steps"`
	}
)

// NewSparseMerkleTree creates a new sparse merkle tree
func NewSparseMerkleTree(algorithm HashAlgorithm) *SparseMerkleTree {
	return &SparseMerkleTree{
		algorithm: algorithm,
		root:      NewRootNode(algorithm, nil, nil),
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
	CalculateHash(algo HashAlgorithm) *DataHash
	GetPath() *big.Int
	IsLeaf() bool
}

// LeafBranch represents a leaf node (matches TypeScript LeafBranch)
type LeafBranch struct {
	Path  *big.Int
	Value []byte
	hash  *DataHash
}

// NodeBranch represents an internal node (matches TypeScript NodeBranch)
type NodeBranch struct {
	Algorithm    HashAlgorithm
	Path         *big.Int
	Left         Branch
	Right        Branch
	childrenHash *DataHash
	hash         *DataHash
}

// DataHash represents a hash with algorithm imprint (matches TypeScript DataHash)
type DataHash struct {
	Algorithm HashAlgorithm
	Data      []byte
	Imprint   []byte
}

// NewRootNode creates a new root node
func NewRootNode(algorithm HashAlgorithm, left, right Branch) *RootNode {
	return &RootNode{
		Left:  left,
		Right: right,
		Path:  big.NewInt(1),
	}
}

// CalculateHash calculates root hash (matches TypeScript logic)
func (r *RootNode) CalculateHash(algo HashAlgorithm) *DataHash {
	var leftHash, rightHash []byte

	if r.Left != nil {
		leftHash = r.Left.CalculateHash(algo).Data
	} else {
		leftHash = []byte{0} // TypeScript: new Uint8Array(1)
	}

	if r.Right != nil {
		rightHash = r.Right.CalculateHash(algo).Data
	} else {
		rightHash = []byte{0} // TypeScript: new Uint8Array(1)
	}

	// Combine and hash: leftHash + rightHash
	combined := append(leftHash, rightHash...)
	hashData := sha256Hash(combined)

	return NewDataHash(algo, hashData)
}

// NewLeafBranch creates a leaf branch
func NewLeafBranch(algorithm HashAlgorithm, path *big.Int, value []byte) *LeafBranch {
	leaf := &LeafBranch{
		Path:  new(big.Int).Set(path),
		Value: append([]byte(nil), value...),
	}

	// Calculate hash: BigintConverter.encode(path) + value
	pathBytes := bigintEncode(path)
	data := append(pathBytes, value...)
	hashData := sha256Hash(data)

	leaf.hash = NewDataHash(algorithm, hashData)
	return leaf
}

// NewLeafBranchLazy creates a leaf branch without calculating hash (for batch operations)
func NewLeafBranchLazy(algorithm HashAlgorithm, path *big.Int, value []byte) *LeafBranch {
	return &LeafBranch{
		Path:  new(big.Int).Set(path),
		Value: append([]byte(nil), value...),
		hash:  nil, // Hash will be calculated on demand
	}
}

func (l *LeafBranch) CalculateHash(algo HashAlgorithm) *DataHash {
	if l.hash != nil {
		return l.hash
	}

	pathBytes := bigintEncode(l.Path)
	data := append(pathBytes, l.Value...)
	hashData := sha256Hash(data)

	l.hash = NewDataHash(algo, hashData)
	return l.hash
}

func (l *LeafBranch) GetPath() *big.Int {
	return l.Path
}

func (l *LeafBranch) IsLeaf() bool {
	return true
}

// NewNodeBranch creates a node branch
func NewNodeBranch(algorithm HashAlgorithm, path *big.Int, left, right Branch) *NodeBranch {
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
	childrenHashData := sha256Hash(combined)
	node.childrenHash = NewDataHash(algorithm, childrenHashData)

	// Calculate node hash: BigintConverter.encode(path) + childrenHash
	pathBytes := bigintEncode(path)
	data := append(pathBytes, childrenHashData...)
	hashData := sha256Hash(data)
	node.hash = NewDataHash(algorithm, hashData)

	return node
}

// NewNodeBranchLazy creates a node branch without calculating hashes (for batch operations)
func NewNodeBranchLazy(algorithm HashAlgorithm, path *big.Int, left, right Branch) *NodeBranch {
	return &NodeBranch{
		Algorithm:    algorithm,
		Path:         new(big.Int).Set(path),
		Left:         left,
		Right:        right,
		childrenHash: nil, // Hashes will be calculated on demand
		hash:         nil,
	}
}

func (n *NodeBranch) CalculateHash(algo HashAlgorithm) *DataHash {
	if n.hash != nil {
		return n.hash
	}

	// Recalculate if needed
	leftHash := n.Left.CalculateHash(algo).Data
	rightHash := n.Right.CalculateHash(algo).Data
	combined := append(leftHash, rightHash...)
	childrenHashData := sha256Hash(combined)
	n.childrenHash = NewDataHash(algo, childrenHashData)

	pathBytes := bigintEncode(n.Path)
	data := append(pathBytes, childrenHashData...)
	hashData := sha256Hash(data)
	n.hash = NewDataHash(algo, hashData)

	return n.hash
}

func (n *NodeBranch) GetPath() *big.Int {
	return n.Path
}

func (n *NodeBranch) IsLeaf() bool {
	return false
}

// NewDataHash creates a DataHash with algorithm imprint
func NewDataHash(algorithm HashAlgorithm, data []byte) *DataHash {
	imprint := make([]byte, len(data)+2)
	// Set algorithm bytes (SHA256 = 0, so 0x0000)
	imprint[0] = byte((int(algorithm) & 0xff00) >> 8)
	imprint[1] = byte(int(algorithm) & 0xff)
	copy(imprint[2:], data)

	return &DataHash{
		Algorithm: algorithm,
		Data:      append([]byte(nil), data...),
		Imprint:   imprint,
	}
}

// ToHex returns hex string of imprint (for compatibility)
func (h *DataHash) ToHex() string {
	return fmt.Sprintf("%x", h.Imprint)
}

// AddLeaf adds a single leaf to the tree (matches TypeScript addLeaf)
func (smt *SparseMerkleTree) AddLeaf(path *big.Int, value []byte) error {
	// TypeScript: const isRight = path & 1n;
	isRight := new(big.Int).And(path, big.NewInt(1)).Cmp(big.NewInt(0)) != 0

	var left, right Branch

	if isRight {
		left = smt.root.Left
		if smt.root.Right != nil {
			newRight, err := smt.buildTree(smt.root.Right, path, value)
			if err != nil {
				return err
			}
			right = newRight
		} else {
			right = NewLeafBranch(smt.algorithm, path, value)
		}
	} else {
		if smt.root.Left != nil {
			newLeft, err := smt.buildTree(smt.root.Left, path, value)
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

// AddLeaves adds multiple leaves to the existing tree
// This produces the EXACT same tree structure as sequential AddLeaf calls,
// and maintains the existing tree structure when adding new leaves
func (smt *SparseMerkleTree) AddLeaves(leaves []*Leaf) error {
	if len(leaves) == 0 {
		return nil
	}

	// Add leaves one by one to the existing tree using AddLeaf
	// This ensures that new leaves are added to the existing tree structure
	for _, leaf := range leaves {
		err := smt.AddLeaf(leaf.Path, leaf.Value)
		if err != nil {
			return err
		}
	}

	return nil
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
		return nil, fmt.Errorf("cannot add leaf inside branch")
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

func (smt *SparseMerkleTree) GetPath(path *big.Int) *MerkleTreePath {
	rootHash := smt.root.CalculateHash(smt.algorithm)
	steps := smt.generatePath(path, smt.root.Left, smt.root.Right)

	return &MerkleTreePath{
		Root:  rootHash.ToHex(),
		Steps: steps,
	}
}

// generatePath recursively generates the Merkle tree path steps
func (smt *SparseMerkleTree) generatePath(remainingPath *big.Int, left, right Branch) []MerkleTreeStep {
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
		return []MerkleTreeStep{smt.createMerkleTreeStep(remainingPath, nil, siblingBranch)}
	}

	commonPath := calculateCommonPath(remainingPath, branch.GetPath())

	if branch.GetPath().Cmp(commonPath.path) == 0 {
		if branch.IsLeaf() {
			return []MerkleTreeStep{smt.createMerkleTreeStep(branch.GetPath(), branch, siblingBranch)}
		}

		// If path has ended, return the current non-leaf branch data
		shifted := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
		if shifted.Cmp(big.NewInt(1)) == 0 {
			return []MerkleTreeStep{smt.createMerkleTreeStep(branch.GetPath(), branch, siblingBranch)}
		}

		// Continue recursively into the branch
		nodeBranch, ok := branch.(*NodeBranch)
		if !ok {
			// Should not happen if IsLeaf() returned false
			return []MerkleTreeStep{smt.createMerkleTreeStep(branch.GetPath(), branch, siblingBranch)}
		}

		// Recursively generate path for the shifted remaining path
		shiftedRemaining := new(big.Int).Rsh(remainingPath, uint(commonPath.length.Uint64()))
		recursiveSteps := smt.generatePath(shiftedRemaining, nodeBranch.Left, nodeBranch.Right)

		// Append the current step without branch (since we went into it)
		currentStep := smt.createMerkleTreeStep(branch.GetPath(), nil, siblingBranch)

		return append(recursiveSteps, currentStep)
	}

	return []MerkleTreeStep{smt.createMerkleTreeStep(branch.GetPath(), branch, siblingBranch)}
}

// createMerkleTreeStep creates a MerkleTreeStep with proper branch and sibling handling
func (smt *SparseMerkleTree) createMerkleTreeStep(path *big.Int, branch, siblingBranch Branch) MerkleTreeStep {
	step := MerkleTreeStep{
		Path:    path.String(),
		Branch:  []string{},
		Sibling: nil,
	}

	// Add branch hash if branch exists
	if branch != nil {
		// If it's a LeafBranch, use the value instead of the hash
		if leafBranch, ok := branch.(*LeafBranch); ok {
			step.Branch = []string{hex.EncodeToString(leafBranch.Value)}
		} else {
			branchHash := branch.CalculateHash(smt.algorithm)
			step.Branch = []string{branchHash.ToHex()}
		}
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

// bigintEncode matches TypeScript BigintConverter.encode
func bigintEncode(value *big.Int) []byte {
	if value.Sign() == 0 {
		return []byte{}
	}

	// Convert to bytes in big-endian format (matches Unicity SDK)
	return value.Bytes()
}

// sha256Hash performs SHA256 hashing
func sha256Hash(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
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
