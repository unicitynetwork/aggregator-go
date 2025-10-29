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
	ErrKeyLength        = errors.New("smt: invalid key length")
	ErrWrongShard       = errors.New("smt: key does not belong in this shard")
)

type (
	// SparseMerkleTree implements a sparse Merkle tree compatible with Unicity SDK
	SparseMerkleTree struct {
		parentMode bool // true if this tree operates in "parent mode"
		keyLength  int  // bit length of the keys in the tree
		algorithm  api.HashAlgorithm
		root       *NodeBranch
		isSnapshot bool              // true if this is a snapshot, false if original tree
		original   *SparseMerkleTree // reference to original tree (nil for original)
	}

	// SmtSnapshot represents a snapshot of the SMT with copy-on-write semantics
	SmtSnapshot struct {
		*SparseMerkleTree
	}
)

// NewSparseMerkleTree creates a new sparse Merkle tree for a monolithic aggregator
func NewSparseMerkleTree(algorithm api.HashAlgorithm, keyLength int) *SparseMerkleTree {
	if keyLength <= 0 {
		panic("SMT key length must be positive")
	}
	return &SparseMerkleTree{
		parentMode: false,
		keyLength:  keyLength,
		algorithm:  algorithm,
		root:       newRootBranch(big.NewInt(1), nil, nil),
		isSnapshot: false,
		original:   nil,
	}
}

// NewChildSparseMerkleTree creates a new sparse Merkle tree for a child aggregator in sharded setup
func NewChildSparseMerkleTree(algorithm api.HashAlgorithm, keyLength int, shardID api.ShardID) *SparseMerkleTree {
	if keyLength <= 0 {
		panic("SMT key length must be positive")
	}
	if shardID <= 1 {
		panic("Shard ID must be positive and have at least 2 bits")
	}
	path := big.NewInt(int64(shardID))
	if path.BitLen() > keyLength {
		panic("Shard ID must be shorter than SMT key length")
	}
	return &SparseMerkleTree{
		parentMode: false,
		keyLength:  keyLength,
		algorithm:  algorithm,
		root:       newRootBranch(path, nil, nil),
		isSnapshot: false,
		original:   nil,
	}
}

// NewParentSparseMerkleTree creates a new sparse Merkle tree for the parent aggregator in sharded setup
func NewParentSparseMerkleTree(algorithm api.HashAlgorithm, keyLength int) *SparseMerkleTree {
	tree := NewSparseMerkleTree(algorithm, keyLength)
	tree.parentMode = true

	// Populate all leaves with null hashes
	// To allow the child aggregators to compute the correct root hashes
	// for their respective leaves in the parent aggregator's tree, the
	// parent tree is fully populated (thus not a sparse tree at all)
	// It is expected that these nulls will be replaced from the stored
	// state at once after the tree is initially constructed, but still
	// better to ensure all the leaves exist; otherwise the hash values
	// of siblings of the missing nodes would not match the structure of
	// the tree and the corresponding inclusion proofs would fail to verify
	tree.root.Left = populate(2, keyLength)
	tree.root.Right = populate(3, keyLength)

	return tree
}

func populate(path, levels int) branch {
	if levels == 1 {
		return newChildLeafBranch(big.NewInt(int64(path)), nil)
	}
	left := populate(2, levels-1)
	right := populate(3, levels-1)
	return newNodeBranch(big.NewInt(int64(path)), left, right)
}

// CreateSnapshot creates a snapshot of the current SMT state
// The snapshot shares nodes with the original tree (copy-on-write)
func (smt *SparseMerkleTree) CreateSnapshot() *SmtSnapshot {
	snapshot := &SparseMerkleTree{
		parentMode: smt.parentMode,
		keyLength:  smt.keyLength,
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
// In regular and child mode, only new leaves can be added and any attempt
// to overwrite an existing leaf is an error; in parent mode, updates are allowed
func (snapshot *SmtSnapshot) AddLeaf(path *big.Int, value []byte) error {
	return snapshot.SparseMerkleTree.AddLeaf(path, value)
}

// AddLeaves adds multiple leaves to the snapshot
// In regular and child mode, only new leaves can be added and any attempt
// to overwrite an existing leaf is an error; in parent mode, updates are allowed
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
		return newRootBranch(smt.root.Path, smt.root.Left, smt.root.Right)
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
		cloned := newLeafBranch(leafBranch.Path, leafBranch.Value)
		// Preserve the isChild flag for parent mode trees
		cloned.isChild = leafBranch.isChild
		return cloned
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
	Path    *big.Int
	Value   []byte
	hash    *api.DataHash
	isChild bool // true if this is the root hash form a child aggregator
}

// NodeBranch represents an internal node
type NodeBranch struct {
	Path   *big.Int
	Left   branch
	Right  branch
	hash   *api.DataHash
	isRoot bool // true if this is the root node
}

// NewLeafBranch creates a regular leaf branch
func newLeafBranch(path *big.Int, value []byte) *LeafBranch {
	return &LeafBranch{
		Path:    new(big.Int).Set(path),
		Value:   append([]byte(nil), value...),
		isChild: false,
		// Hash will be computed on demand
	}
}

// NewChildLeafBranch creates a parent tree leaf containing the root hash of a child tree
func newChildLeafBranch(path *big.Int, value []byte) *LeafBranch {
	if value != nil {
		value = append([]byte(nil), value...)
	}
	return &LeafBranch{
		Path:    new(big.Int).Set(path),
		Value:   value,
		isChild: true,
		// Hash will be set on demand
	}
}

func (l *LeafBranch) calculateHash(hasher *api.DataHasher) *api.DataHash {
	if l.hash != nil {
		return l.hash
	}

	if l.isChild {
		if l.Value != nil {
			l.hash = api.NewDataHash(hasher.GetAlgorithm(), l.Value)
		}
	} else {
		pathBytes := api.BigintEncode(l.Path)
		l.hash = hasher.Reset().AddData(api.CborArray(2)).
			AddCborBytes(pathBytes).AddCborBytes(l.Value).GetHash()
	}
	return l.hash
}

func (l *LeafBranch) getPath() *big.Int {
	return l.Path
}

func (l *LeafBranch) isLeaf() bool {
	return true
}

// NewNodeBranch creates a regular node branch
func newNodeBranch(path *big.Int, left, right branch) *NodeBranch {
	return &NodeBranch{
		Path:   new(big.Int).Set(path),
		Left:   left,
		Right:  right,
		isRoot: false,
		// Hash will be computed on demand
	}
}

// NewRootBranch creates a root node branch
func newRootBranch(path *big.Int, left, right branch) *NodeBranch {
	return &NodeBranch{
		Path:   new(big.Int).Set(path),
		Left:   left,
		Right:  right,
		isRoot: true,
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

	if n.isRoot && n.Path.BitLen() > 1 {
		// This is root of a child tree in sharded setup
		// The path to add is the last bit of the shard ID
		pos := n.Path.BitLen() - 2
		path := big.NewInt(int64(2 + n.Path.Bit(pos)))
		hasher.AddCborBytes(api.BigintEncode(path))
	} else {
		// In all other cases we just add the actual path
		hasher.AddCborBytes(api.BigintEncode(n.Path))
	}

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
		return ErrKeyLength
	}
	if calculateCommonPath(path, smt.root.Path).BitLen() != smt.root.Path.BitLen() {
		return ErrWrongShard
	}

	// Implement copy-on-write for snapshots only
	if smt.isSnapshot {
		smt.root = smt.copyOnWriteRoot()
	}

	shifted := new(big.Int).Rsh(path, uint(smt.root.Path.BitLen()-1))
	isRight := shifted.Bit(0) == 1

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
			newRight, err := smt.buildTree(rightBranch, shifted, value)
			if err != nil {
				return err
			}
			right = newRight
		} else {
			right = newLeafBranch(shifted, value)
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
			newLeft, err := smt.buildTree(leftBranch, shifted, value)
			if err != nil {
				return err
			}
			left = newLeft
		} else {
			left = newLeafBranch(shifted, value)
		}
		right = smt.root.Right
	}

	smt.root = newRootBranch(smt.root.Path, left, right)
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
	// Create a new hasher to ensure thread safety
	hasher := api.NewDataHasher(smt.algorithm)
	return smt.root.calculateHash(hasher).GetImprint()
}

// GetRootHashHex returns the root hash as hex string
func (smt *SparseMerkleTree) GetRootHashHex() string {
	// Create a new hasher to ensure thread safety
	hasher := api.NewDataHasher(smt.algorithm)
	return smt.root.calculateHash(hasher).ToHex()
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
		if commonPath.Cmp(targetPath) == 0 {
			return nil, fmt.Errorf("leaf not found")
		}

		// Navigate using the same logic as buildTree
		shifted := new(big.Int).Rsh(targetPath, uint(commonPath.BitLen()-1))
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
		if leafBranch.isChild {
			return newChildLeafBranch(leafBranch.Path, value), nil
		} else if bytes.Equal(leafBranch.Value, value) {
			return nil, ErrDuplicateLeaf
		} else {
			return nil, ErrLeafModification
		}
	}

	commonPath := calculateCommonPath(remainingPath, branch.getPath())
	shifted := new(big.Int).Rsh(remainingPath, uint(commonPath.BitLen()-1))
	isRight := shifted.Bit(0) == 1

	if commonPath.Cmp(remainingPath) == 0 {
		return nil, fmt.Errorf("cannot add leaf inside branch, commonPath: '%s', remainingPath: '%s'", commonPath, remainingPath)
	}

	// If a leaf must be split from the middle
	if branch.isLeaf() {
		leafBranch := branch.(*LeafBranch)
		if commonPath.Cmp(leafBranch.Path) == 0 {
			return nil, fmt.Errorf("cannot extend tree through leaf")
		}

		// TypeScript: branch.path >> commonPath.length
		oldBranchPath := new(big.Int).Rsh(leafBranch.Path, uint(commonPath.BitLen()-1))
		oldBranch := newLeafBranch(oldBranchPath, leafBranch.Value)

		// TypeScript: remainingPath >> commonPath.length
		newBranchPath := new(big.Int).Rsh(remainingPath, uint(commonPath.BitLen()-1))
		newBranch := newLeafBranch(newBranchPath, value)

		if isRight {
			return newNodeBranch(commonPath, oldBranch, newBranch), nil
		} else {
			return newNodeBranch(commonPath, newBranch, oldBranch), nil
		}
	}

	// If node branch is split in the middle
	nodeBranch := branch.(*NodeBranch)
	if commonPath.Cmp(nodeBranch.Path) < 0 {
		newBranchPath := new(big.Int).Rsh(remainingPath, uint(commonPath.BitLen()-1))
		newBranch := newLeafBranch(newBranchPath, value)

		oldBranchPath := new(big.Int).Rsh(nodeBranch.Path, uint(commonPath.BitLen()-1))
		oldBranch := newNodeBranch(oldBranchPath, nodeBranch.Left, nodeBranch.Right)

		if isRight {
			return newNodeBranch(commonPath, oldBranch, newBranch), nil
		} else {
			return newNodeBranch(commonPath, newBranch, oldBranch), nil
		}
	}

	if isRight {
		newRight, err := smt.buildTree(nodeBranch.Right, new(big.Int).Rsh(remainingPath, uint(commonPath.BitLen()-1)), value)
		if err != nil {
			return nil, err
		}
		return newNodeBranch(nodeBranch.Path, nodeBranch.Left, newRight), nil
	} else {
		newLeft, err := smt.buildTree(nodeBranch.Left, new(big.Int).Rsh(remainingPath, uint(commonPath.BitLen()-1)), value)
		if err != nil {
			return nil, err
		}
		return newNodeBranch(nodeBranch.Path, newLeft, nodeBranch.Right), nil
	}
}

func (smt *SparseMerkleTree) GetPath(path *big.Int) (*api.MerkleTreePath, error) {
	if path.BitLen()-1 != smt.keyLength {
		return nil, ErrKeyLength
	}
	if calculateCommonPath(path, smt.root.Path).BitLen() != smt.root.Path.BitLen() {
		return nil, ErrWrongShard
	}

	// Create a new hasher to ensure thread safety
	hasher := api.NewDataHasher(smt.algorithm)

	rootHash := smt.root.calculateHash(hasher)
	steps := smt.generatePath(hasher, path, smt.root)

	return &api.MerkleTreePath{
		Root:  rootHash.ToHex(),
		Steps: steps,
	}, nil
}

// generatePath recursively generates the Merkle tree path steps
func (smt *SparseMerkleTree) generatePath(hasher *api.DataHasher, remainingPath *big.Int, currentNode branch) []api.MerkleTreeStep {
	if remainingPath.BitLen() < 2 {
		panic("Invalid remaining path, must be internal logic error")
	}

	if currentNode.isLeaf() {
		// We are in a leaf branch, no further navigation
		// Create the corresponding leaf hash step
		currentLeaf, _ := currentNode.(*LeafBranch)
		path := currentLeaf.Path.String()
		var data *string
		if currentLeaf.Value != nil {
			tmp := hex.EncodeToString(currentLeaf.Value)
			data = &tmp
		}
		return []api.MerkleTreeStep{
			{Path: path, Data: data},
		}
	}

	currentBranch, ok := currentNode.(*NodeBranch)
	if !ok {
		panic("Unknown target branch type")
	}

	var path *big.Int
	if currentBranch.isRoot && currentBranch.Path.BitLen() > 1 {
		// This is root of a child tree in sharded setup
		// The path to add is the last bit of the shard ID
		pos := currentBranch.Path.BitLen() - 2
		path = big.NewInt(int64(0b10 | currentBranch.Path.Bit(pos)))
	} else {
		// In all other cases we just add the actual path
		path = currentBranch.Path
	}

	var leftHash, rightHash *string
	if currentBranch.Left != nil {
		hash := currentBranch.Left.calculateHash(hasher)
		if hash != nil {
			tmp := hex.EncodeToString(hash.RawHash)
			leftHash = &tmp
		}
	}
	if currentBranch.Right != nil {
		hash := currentBranch.Right.calculateHash(hasher)
		if hash != nil {
			tmp := hex.EncodeToString(hash.RawHash)
			rightHash = &tmp
		}
	}

	commonPath := calculateCommonPath(remainingPath, currentBranch.Path)
	if currentBranch != smt.root && commonPath.BitLen() < currentBranch.Path.BitLen() {
		// Remaining path diverges or ends here
		// Create the corresponding 2-step proof
		return []api.MerkleTreeStep{
			{Path: "0", Data: leftHash},
			{Path: path.String(), Data: rightHash},
		}
	}

	// Trim remaining path for descending into subtree
	remainingPath = new(big.Int).Rsh(remainingPath, uint(commonPath.BitLen()-1))

	var step api.MerkleTreeStep
	var steps []api.MerkleTreeStep
	if remainingPath.Bit(0) == 0 {
		// Target in the left child, right child is sibling
		step = api.MerkleTreeStep{Path: path.String(), Data: rightHash}
		if leftHash == nil {
			steps = []api.MerkleTreeStep{{Path: "0", Data: nil}}
		} else {
			steps = smt.generatePath(hasher, remainingPath, currentBranch.Left)
		}
	} else {
		step = api.MerkleTreeStep{Path: path.String(), Data: leftHash}
		// Target in the right child, left child is sibling
		if rightHash == nil {
			steps = []api.MerkleTreeStep{{Path: "1", Data: nil}}
		} else {
			steps = smt.generatePath(hasher, remainingPath, currentBranch.Right)
		}
	}
	return append(steps, step)
}

// calculateCommonPath computes the longest common prefix of path1 and path2
func calculateCommonPath(path1, path2 *big.Int) *big.Int {
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

	return res
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

// JoinPaths joins the hash proofs from a child and parent in sharded setting
func JoinPaths(child, parent *api.MerkleTreePath) (*api.MerkleTreePath, error) {
	if child == nil {
		return nil, errors.New("nil child path")
	}
	if parent == nil {
		return nil, errors.New("nil parent path")
	}

	// Root hashes are hex-encoded imprints, the first 4 characters are hash function identifiers
	if len(child.Root) < 4 {
		return nil, errors.New("invalid child root hash format")
	}
	if len(parent.Root) < 4 {
		return nil, errors.New("invalid parent root hash format")
	}
	if child.Root[:4] != parent.Root[:4] {
		return nil, errors.New("can't join paths: child hash algorithm does not match parent")
	}

	if len(parent.Steps) == 0 {
		return nil, errors.New("empty parent hash steps")
	}
	if parent.Steps[0].Data == nil || *parent.Steps[0].Data != child.Root[4:] {
		return nil, errors.New("can't join paths: child root hash does not match parent input hash")
	}

	steps := make([]api.MerkleTreeStep, len(child.Steps)+len(parent.Steps)-1)
	copy(steps, child.Steps)
	copy(steps[len(child.Steps):], parent.Steps[1:])

	return &api.MerkleTreePath{
		Root:  parent.Root,
		Steps: steps,
	}, nil
}
