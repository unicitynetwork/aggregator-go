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
	ErrDuplicateLeaf          = errors.New("smt: duplicate leaf")
	ErrLeafModification       = errors.New("smt: attempt to modify an existing leaf")
	ErrKeyLength              = errors.New("smt: invalid key length")
	ErrWrongShard             = errors.New("smt: key does not belong in this shard")
	ErrInvalidChildHashLength = errors.New("smt: child hash value length does not match hash algorithm")
)

// smtCachedHashBytes is the fixed size of the inline hash cache arrays in
// LeafBranch and NodeBranch. All currently supported algorithms (SHA256,
// SHA3-256) produce 32-byte digests. If a longer-digest algorithm is added,
// increase this constant and update the struct field types.
const smtCachedHashBytes = 32

// hashLen returns the raw digest length in bytes for the given algorithm.
// Panics for unsupported algorithms so callers get an early failure if a new
// algorithm is added without updating the inline cache arrays.
func hashLen(algo api.HashAlgorithm) int {
	switch algo {
	case api.SHA256, api.SHA3_256:
		return 32
	default:
		panic(fmt.Sprintf("smt: unsupported hash algorithm %d", algo))
	}
}

// buildImprint constructs the algorithm-prefixed hash imprint from raw bytes.
// Used at API boundaries (GetRootHash, GetPath) where a full DataHash object
// is not needed.
func buildImprint(algo api.HashAlgorithm, raw []byte) []byte {
	alg := uint(algo)
	out := make([]byte, len(raw)+2)
	out[0] = byte(alg >> 8)
	out[1] = byte(alg)
	copy(out[2:], raw)
	return out
}

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
	if hashLen(algorithm) != smtCachedHashBytes {
		panic(fmt.Sprintf("smt: hash algorithm output (%d bytes) exceeds inline cache size (%d) — update smtCachedHashBytes",
			hashLen(algorithm), smtCachedHashBytes))
	}
	tree := &SparseMerkleTree{
		parentMode: false,
		keyLength:  keyLength,
		algorithm:  algorithm,
		root:       newRootBranch(big.NewInt(1), nil, nil, 0),
		isSnapshot: false,
		original:   nil,
	}
	// Prime the root hash to eliminate races on the first concurrent reads
	tree.root.calculateHash(api.NewDataHasher(algorithm))
	return tree
}

// NewChildSparseMerkleTree creates a new sparse Merkle tree for a child aggregator in sharded setup
func NewChildSparseMerkleTree(algorithm api.HashAlgorithm, keyLength int, shardID api.ShardID) *SparseMerkleTree {
	if keyLength <= 0 {
		panic("SMT key length must be positive")
	}
	if hashLen(algorithm) != smtCachedHashBytes {
		panic(fmt.Sprintf("smt: hash algorithm output (%d bytes) exceeds inline cache size (%d) — update smtCachedHashBytes",
			hashLen(algorithm), smtCachedHashBytes))
	}
	if shardID <= 1 {
		panic("Shard ID must be positive and have at least 2 bits")
	}
	path := big.NewInt(int64(shardID))
	if path.BitLen() > keyLength {
		panic("Shard ID must be shorter than SMT key length")
	}
	tree := &SparseMerkleTree{
		parentMode: false,
		keyLength:  keyLength,
		algorithm:  algorithm,
		root:       newRootBranch(path, nil, nil, path.BitLen()-1),
		isSnapshot: false,
		original:   nil,
	}
	// Prime the root hash to eliminate races on the first concurrent reads
	tree.root.calculateHash(api.NewDataHasher(algorithm))
	return tree
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
	tree.root.Left = populate(0b10, keyLength, 1)
	tree.root.Right = populate(0b11, keyLength, 1)

	// Mutation above invalidated the root hash primed by NewSparseMerkleTree.
	// We reset and re-prime.
	tree.root.hashSet = false
	tree.root.calculateHash(api.NewDataHasher(algorithm))

	return tree
}

func populate(path, levels, depth int) branch {
	if levels == 1 {
		return newChildLeafBranch(big.NewInt(int64(path)), nil)
	}
	left := populate(0b10, levels-1, depth+1)
	right := populate(0b11, levels-1, depth+1)
	return newNodeBranchWithDepth(big.NewInt(int64(path)), left, right, depth)
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

// SetCommitTarget changes the target tree that this snapshot will commit to.
// This is used for snapshot chaining where a child snapshot needs to commit
// to the main tree after its parent snapshot has been committed.
func (snapshot *SmtSnapshot) SetCommitTarget(target *SparseMerkleTree) {
	snapshot.original = target
}

// CreateSnapshot creates a child snapshot from this snapshot.
// The child snapshot shares the current state and can accumulate its own changes.
// This enables snapshot chaining for pipelined processing.
func (snapshot *SmtSnapshot) CreateSnapshot() *SmtSnapshot {
	return snapshot.SparseMerkleTree.CreateSnapshot()
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
		return newRootBranch(smt.root.Path, smt.root.Left, smt.root.Right, int(smt.root.Depth))
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
		cloned := newLeafBranchWithKey(leafBranch.Path, leafBranch.Key, leafBranch.Value)
		// Preserve the isChild flag for parent mode trees
		cloned.isChild = leafBranch.isChild
		return cloned
	} else {
		nodeBranch := branch.(*NodeBranch)
		return newNodeBranchWithDepth(nodeBranch.Path, nodeBranch.Left, nodeBranch.Right, int(nodeBranch.Depth))
	}
}

// Branch interface for tree nodes
type branch interface {
	calculateHash(hasher *api.DataHasher) []byte
	getPath() *big.Int
	isLeaf() bool
}

// LeafBranch represents a leaf node
type LeafBranch struct {
	Path    *big.Int
	Key     []byte
	Value   []byte
	rawHash [smtCachedHashBytes]byte // inline hash cache; valid when hashSet == true
	hashSet bool
	isChild bool // true if this is the root hash from a child aggregator
}

// NodeBranch represents an internal node
type NodeBranch struct {
	Path    *big.Int
	Depth   uint8
	Left    branch
	Right   branch
	rawHash [smtCachedHashBytes]byte // inline hash cache; valid when hashSet == true
	hashSet bool
	isRoot  bool // true if this is the root node
}

// NewLeafBranch creates a regular leaf branch
func newLeafBranch(path *big.Int, value []byte) *LeafBranch {
	key, err := api.PathToFixedBytes(path, path.BitLen()-1)
	if err != nil {
		panic(fmt.Sprintf("smt: failed to derive key bytes for leaf path: %v", err))
	}
	return newLeafBranchWithKey(path, key, value)
}

func newLeafBranchWithKey(path *big.Int, key, value []byte) *LeafBranch {
	return &LeafBranch{
		Path:    new(big.Int).Set(path),
		Key:     append([]byte(nil), key...),
		Value:   append([]byte(nil), value...),
		isChild: false,
		// Hash will be computed on demand
	}
}

// NewChildLeafBranch creates a parent tree leaf containing the root hash of a child tree
func newChildLeafBranch(path *big.Int, value []byte) *LeafBranch {
	return newChildLeafBranchWithKey(path, nil, value)
}

func newChildLeafBranchWithKey(path *big.Int, key, value []byte) *LeafBranch {
	if value != nil {
		value = append([]byte(nil), value...)
	}
	if key != nil {
		key = append([]byte(nil), key...)
	}
	return &LeafBranch{
		Path:    new(big.Int).Set(path),
		Key:     key,
		Value:   value,
		isChild: true,
		// Hash will be set on demand
	}
}

func (l *LeafBranch) calculateHash(hasher *api.DataHasher) []byte {
	if l.hashSet {
		return l.rawHash[:]
	}
	if l.isChild {
		if l.Value == nil {
			return nil // nil child: parent tree slot not yet filled
		}
		copy(l.rawHash[:], l.Value) // safe: AddLeaf enforced len(l.Value) == hashLen
		l.hashSet = true
		return l.rawHash[:]
	}
	// v2 leaf hashing with domain separation:
	// H(0x00 || key || value)
	hasher.Reset().
		AddData([]byte{0x00}).
		AddData(l.Key).
		AddData(l.Value)
	hasher.SumRaw(l.rawHash[:0])
	l.hashSet = true
	return l.rawHash[:]
}

func (l *LeafBranch) getPath() *big.Int {
	return l.Path
}

func (l *LeafBranch) isLeaf() bool {
	return true
}

// NewNodeBranch creates a regular node branch
func newNodeBranch(path *big.Int, left, right branch) *NodeBranch {
	return newNodeBranchWithDepth(path, left, right, path.BitLen()-1)
}

func newNodeBranchWithDepth(path *big.Int, left, right branch, depth int) *NodeBranch {
	if depth < 0 || depth > 255 {
		panic(fmt.Sprintf("smt: node depth %d out of uint8 range [0, 255]", depth))
	}
	return &NodeBranch{
		Path:   new(big.Int).Set(path),
		Depth:  uint8(depth),
		Left:   left,
		Right:  right,
		isRoot: false,
	}
}

// newRootBranch creates a root node branch
func newRootBranch(path *big.Int, left, right branch, depth int) *NodeBranch {
	if depth < 0 || depth > 255 {
		panic(fmt.Sprintf("smt: root depth %d out of uint8 range [0, 255]", depth))
	}
	return &NodeBranch{
		Path:   new(big.Int).Set(path),
		Depth:  uint8(depth),
		Left:   left,
		Right:  right,
		isRoot: true,
	}
}

func (n *NodeBranch) calculateHash(hasher *api.DataHasher) []byte {
	if n.hashSet {
		return n.rawHash[:]
	}

	// Get the child hashes first so we can reset and reuse the hasher object
	var leftHash, rightHash []byte
	if n.Left != nil {
		leftHash = n.Left.calculateHash(hasher)
	}
	if n.Right != nil {
		rightHash = n.Right.calculateHash(hasher)
	}

	// v2 SMT semantics for unary nodes: hash is child hash.
	if leftHash == nil && rightHash != nil {
		copy(n.rawHash[:], rightHash)
		n.hashSet = true
		return n.rawHash[:]
	}
	if rightHash == nil && leftHash != nil {
		copy(n.rawHash[:], leftHash)
		n.hashSet = true
		return n.rawHash[:]
	}

	// Keep root hash stable for empty trees by hashing domain+level when both
	// children are empty.
	hasher.Reset().
		AddData([]byte{0x01}).
		AddData([]byte{n.Depth})
	if leftHash != nil {
		hasher.AddData(leftHash)
	}
	if rightHash != nil {
		hasher.AddData(rightHash)
	}

	hasher.SumRaw(n.rawHash[:0])
	n.hashSet = true
	return n.rawHash[:]
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
	leafKey, err := api.PathToFixedBytes(path, smt.keyLength)
	if err != nil {
		return fmt.Errorf("failed to derive leaf key bytes from path: %w", err)
	}
	if smt.parentMode && value != nil && len(value) != hashLen(smt.algorithm) {
		return ErrInvalidChildHashLength
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
			newRight, err := smt.buildTree(rightBranch, shifted, leafKey, value, int(smt.root.Depth))
			if err != nil {
				return err
			}
			right = newRight
		} else {
			right = newLeafBranchWithKey(shifted, leafKey, value)
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
			newLeft, err := smt.buildTree(leftBranch, shifted, leafKey, value, int(smt.root.Depth))
			if err != nil {
				return err
			}
			left = newLeft
		} else {
			left = newLeafBranchWithKey(shifted, leafKey, value)
		}
		right = smt.root.Right
	}

	smt.root = newRootBranch(smt.root.Path, left, right, int(smt.root.Depth))
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

// ensureHashes eagerly computes all node hashes in the tree. Must be called
// under a write lock so that subsequent readers under RLock find every
// hashSet flag already true and never mutate node state.
func (smt *SparseMerkleTree) ensureHashes() {
	hasher := api.NewDataHasher(smt.algorithm)
	smt.root.calculateHash(hasher)
}

// GetRootHash returns the root hash as imprint
func (smt *SparseMerkleTree) GetRootHash() []byte {
	// Create a new hasher to ensure thread safety
	hasher := api.NewDataHasher(smt.algorithm)
	return buildImprint(smt.algorithm, smt.root.calculateHash(hasher))
}

// GetRootHashHex returns the root hash as hex string
func (smt *SparseMerkleTree) GetRootHashHex() string {
	// Create a new hasher to ensure thread safety
	hasher := api.NewDataHasher(smt.algorithm)
	return fmt.Sprintf("%x", buildImprint(smt.algorithm, smt.root.calculateHash(hasher)))
}

// GetRootHashRaw returns the raw 32-byte root hash without the algorithm
// prefix. This is the canonical v2 root hash consumed by
// api.InclusionCert verification and by UC.IR.h binding.
func (smt *SparseMerkleTree) GetRootHashRaw() []byte {
	hasher := api.NewDataHasher(smt.algorithm)
	raw := smt.root.calculateHash(hasher)
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

// GetInclusionCert builds a v2 inclusion certificate for the leaf at the
// given raw 32-byte key. Verifier consumes bitmap + siblings in root-to-leaf
// wire order.
//
// Returns an error if no leaf exists at the key. Non-inclusion certificates
// are produced by a separate path (not yet implemented).
func (smt *SparseMerkleTree) GetInclusionCert(key []byte) (*api.InclusionCert, error) {
	if len(key) != api.StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("%w: got %d, want %d", api.ErrCertKeyLength, len(key), api.StateTreeKeyLengthBytes)
	}

	// Prime the hash cache by hashing the root. This cascades through every
	// node reachable from the root, so sibling reads below are cache hits.
	hasher := api.NewDataHasher(smt.algorithm)
	_ = smt.root.calculateHash(hasher)

	var cert api.InclusionCert
	if _, err := smt.generateInclusionCertWithLeafValue(hasher, key, smt.root, &cert); err != nil {
		return nil, err
	}
	return &cert, nil
}

// GetShardInclusionFragment builds the native parent proof fragment used by a
// child aggregator to later compose a full v2 proof locally. The fragment uses
// the same bitmap+sibling wire shape as InclusionCert but only contains the
// shallow parent-tree depths. The returned shard leaf value must equal the
// child tree root for composition to proceed.
func (smt *SparseMerkleTree) GetShardInclusionFragment(shardID api.ShardID) (*api.ParentInclusionFragment, error) {
	if !smt.parentMode {
		return nil, fmt.Errorf("smt: shard inclusion fragment only valid for parent trees")
	}

	path := big.NewInt(int64(shardID))
	if path.BitLen()-1 != smt.keyLength {
		return nil, ErrKeyLength
	}

	key, err := api.PathToFixedBytes(path, smt.keyLength)
	if err != nil {
		return nil, fmt.Errorf("failed to derive shard key bytes: %w", err)
	}

	hasher := api.NewDataHasher(smt.algorithm)
	_ = smt.root.calculateHash(hasher)

	var cert api.InclusionCert
	leafValue, err := smt.generateInclusionCertWithLeafValue(hasher, key, smt.root, &cert)
	if err != nil {
		return nil, err
	}
	if len(leafValue) == 0 {
		return nil, nil
	}

	certBytes, err := cert.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to encode parent fragment cert: %w", err)
	}
	return &api.ParentInclusionFragment{
		CertificateBytes: api.NewHexBytes(certBytes),
		ShardLeafValue:   api.NewHexBytes(leafValue),
	}, nil
}

// generateInclusionCert recursively walks from the current node toward the
// leaf matching key, appending siblings and setting bitmap bits at every
// 2-child branching node along the path. Unary passthrough nodes contribute
// nothing to either the bitmap or the sibling list.
func (smt *SparseMerkleTree) generateInclusionCert(hasher *api.DataHasher, key []byte, current branch, cert *api.InclusionCert) error {
	_, err := smt.generateInclusionCertWithLeafValue(hasher, key, current, cert)
	return err
}

func (smt *SparseMerkleTree) generateInclusionCertWithLeafValue(hasher *api.DataHasher, key []byte, current branch, cert *api.InclusionCert) ([]byte, error) {
	if current == nil {
		return nil, fmt.Errorf("smt: inclusion cert traversal reached nil subtree")
	}
	if current.isLeaf() {
		leaf := current.(*LeafBranch)
		// Parent-mode populate() creates placeholder leaves with nil Key and nil
		// Value. For those placeholders, key equality is validated only after the
		// first real AddLeaf replaces them with a keyed leaf.
		if leaf.Key != nil && !bytes.Equal(leaf.Key, key) {
			return nil, fmt.Errorf("smt: leaf not found for key %x", key)
		}
		if leaf.Value == nil {
			return nil, nil
		}
		return append([]byte(nil), leaf.Value...), nil
	}

	node := current.(*NodeBranch)
	if node.Left != nil && node.Right != nil {
		depth := int(node.Depth)
		if depth < 0 || depth >= api.StateTreeKeyLengthBits {
			return nil, fmt.Errorf("smt: invalid branch depth %d", depth)
		}

		var sibling, child branch
		if keyBit(key, depth) == 0 {
			sibling = node.Right
			child = node.Left
		} else {
			sibling = node.Left
			child = node.Right
		}

		childHash := child.calculateHash(hasher)
		if childHash == nil {
			return nil, nil
		}

		sibHash := sibling.calculateHash(hasher)
		if sibHash == nil {
			// The sibling subtree is empty (all-placeholder / no submitted leaf),
			// so this depth is a unary passthrough: no bitmap bit and no sibling.
			// Any already recorded shallower siblings remain valid.
			return smt.generateInclusionCertWithLeafValue(hasher, key, child, cert)
		}

		cert.Bitmap[depth/8] |= 1 << (uint(depth) % 8)
		if len(sibHash) != api.SiblingSize {
			return nil, fmt.Errorf("smt: sibling hash unexpected length: got %d, want %d", len(sibHash), api.SiblingSize)
		}
		var sib [api.SiblingSize]byte
		copy(sib[:], sibHash)
		cert.Siblings = append(cert.Siblings, sib)

		return smt.generateInclusionCertWithLeafValue(hasher, key, child, cert)
	}

	// Unary passthrough: typically only at the root when the tree holds
	// ≤1 leaves. The v2 rule says a single-child node's hash equals the
	// child's hash, so no bitmap bit and no sibling are added.
	if node.Left != nil {
		return smt.generateInclusionCertWithLeafValue(hasher, key, node.Left, cert)
	}
	if node.Right != nil {
		return smt.generateInclusionCertWithLeafValue(hasher, key, node.Right, cert)
	}
	return nil, fmt.Errorf("smt: reached empty subtree in inclusion cert traversal")
}

// keyBit returns bit d of the raw key under LSB-first byte layout.
// Matches api.keyBitAt.
func keyBit(key []byte, d int) byte {
	return (key[d/8] >> (uint(d) % 8)) & 1
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

// buildTree updates a subtree while preserving the leaf full key bytes and
// tracking absolute node depth (0..255) for v2 node hashing.
func (smt *SparseMerkleTree) buildTree(branch branch, remainingPath *big.Int, leafKey, value []byte, depthOffset int) (branch, error) {
	// Special checks for adding a leaf that already exists in the tree
	if branch.isLeaf() && branch.getPath().Cmp(remainingPath) == 0 {
		leafBranch := branch.(*LeafBranch)
		if leafBranch.isChild {
			return newChildLeafBranchWithKey(leafBranch.Path, leafKey, value), nil
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
		oldBranch := newLeafBranchWithKey(oldBranchPath, leafBranch.Key, leafBranch.Value)

		// TypeScript: remainingPath >> commonPath.length
		newBranchPath := new(big.Int).Rsh(remainingPath, uint(commonPath.BitLen()-1))
		newBranch := newLeafBranchWithKey(newBranchPath, leafKey, value)

		nodeDepth := depthOffset + (commonPath.BitLen() - 1)

		if isRight {
			return newNodeBranchWithDepth(commonPath, oldBranch, newBranch, nodeDepth), nil
		} else {
			return newNodeBranchWithDepth(commonPath, newBranch, oldBranch, nodeDepth), nil
		}
	}

	// If node branch is split in the middle
	nodeBranch := branch.(*NodeBranch)
	if commonPath.Cmp(nodeBranch.Path) < 0 {
		newBranchPath := new(big.Int).Rsh(remainingPath, uint(commonPath.BitLen()-1))
		newBranch := newLeafBranchWithKey(newBranchPath, leafKey, value)

		oldBranchPath := new(big.Int).Rsh(nodeBranch.Path, uint(commonPath.BitLen()-1))
		oldBranch := newNodeBranchWithDepth(oldBranchPath, nodeBranch.Left, nodeBranch.Right, int(nodeBranch.Depth))

		nodeDepth := depthOffset + (commonPath.BitLen() - 1)

		if isRight {
			return newNodeBranchWithDepth(commonPath, oldBranch, newBranch, nodeDepth), nil
		} else {
			return newNodeBranchWithDepth(commonPath, newBranch, oldBranch, nodeDepth), nil
		}
	}

	nextDepthOffset := depthOffset + (commonPath.BitLen() - 1)
	if isRight {
		newRight, err := smt.buildTree(nodeBranch.Right, new(big.Int).Rsh(remainingPath, uint(commonPath.BitLen()-1)), leafKey, value, nextDepthOffset)
		if err != nil {
			return nil, err
		}
		return newNodeBranchWithDepth(nodeBranch.Path, nodeBranch.Left, newRight, int(nodeBranch.Depth)), nil
	} else {
		newLeft, err := smt.buildTree(nodeBranch.Left, new(big.Int).Rsh(remainingPath, uint(commonPath.BitLen()-1)), leafKey, value, nextDepthOffset)
		if err != nil {
			return nil, err
		}
		return newNodeBranchWithDepth(nodeBranch.Path, newLeft, nodeBranch.Right, int(nodeBranch.Depth)), nil
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

	rawRoot := smt.root.calculateHash(hasher)
	steps := smt.generatePath(hasher, path, smt.root)

	return &api.MerkleTreePath{
		Root:  fmt.Sprintf("%x", buildImprint(smt.algorithm, rawRoot)),
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
		if raw := currentBranch.Left.calculateHash(hasher); raw != nil {
			tmp := hex.EncodeToString(raw)
			leftHash = &tmp
		}
	}
	if currentBranch.Right != nil {
		if raw := currentBranch.Right.calculateHash(hasher); raw != nil {
			tmp := hex.EncodeToString(raw)
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
