package smt

import (
	"crypto/sha256"
	"fmt"
	"math/big"
)

// HashAlgorithm represents the hashing algorithm to use
type HashAlgorithm int

const (
	SHA256 HashAlgorithm = iota
)

// Node represents a node in the sparse merkle tree
type Node interface {
	// CalculateHash computes and returns the hash of this node
	CalculateHash(algo HashAlgorithm) []byte
	
	// GetPath returns the path of this node
	GetPath() *big.Int
	
	// IsLeaf returns true if this is a leaf node
	IsLeaf() bool
	
	// GetHash returns the cached hash (if available)
	GetHash() []byte
}

// RootNode represents the root of the sparse merkle tree
type RootNode struct {
	Left  Node
	Right Node
	hash  []byte
}

// NewRootNode creates a new root node
func NewRootNode(left, right Node) *RootNode {
	return &RootNode{
		Left:  left,
		Right: right,
	}
}

// CalculateHash calculates the hash of the root node
func (r *RootNode) CalculateHash(algo HashAlgorithm) []byte {
	if r.hash != nil {
		return r.hash
	}
	
	var leftHash, rightHash []byte
	
	if r.Left != nil {
		leftHash = r.Left.CalculateHash(algo)
	} else {
		leftHash = make([]byte, 32) // Zero hash
	}
	
	if r.Right != nil {
		rightHash = r.Right.CalculateHash(algo)
	} else {
		rightHash = make([]byte, 32) // Zero hash
	}
	
	r.hash = hash(algo, append(leftHash, rightHash...))
	return r.hash
}

// GetPath returns the path of the root (always 1)
func (r *RootNode) GetPath() *big.Int {
	return big.NewInt(1)
}

// IsLeaf returns false for root nodes
func (r *RootNode) IsLeaf() bool {
	return false
}

// GetHash returns the cached hash
func (r *RootNode) GetHash() []byte {
	return r.hash
}

// InternalNode represents an internal node in the tree
type InternalNode struct {
	Path  *big.Int
	Left  Node
	Right Node
	hash  []byte
}

// NewInternalNode creates a new internal node
func NewInternalNode(path *big.Int, left, right Node) *InternalNode {
	return &InternalNode{
		Path:  new(big.Int).Set(path),
		Left:  left,
		Right: right,
	}
}

// CalculateHash calculates the hash of the internal node
func (n *InternalNode) CalculateHash(algo HashAlgorithm) []byte {
	if n.hash != nil {
		return n.hash
	}
	
	var leftHash, rightHash []byte
	
	if n.Left != nil {
		leftHash = n.Left.CalculateHash(algo)
	} else {
		leftHash = make([]byte, 32) // Zero hash
	}
	
	if n.Right != nil {
		rightHash = n.Right.CalculateHash(algo)
	} else {
		rightHash = make([]byte, 32) // Zero hash
	}
	
	childrenHash := hash(algo, append(leftHash, rightHash...))
	
	// Hash of path || childrenHash
	pathBytes := n.Path.Bytes()
	data := append(pathBytes, childrenHash...)
	n.hash = hash(algo, data)
	
	return n.hash
}

// GetPath returns the path of this node
func (n *InternalNode) GetPath() *big.Int {
	return n.Path
}

// IsLeaf returns false for internal nodes
func (n *InternalNode) IsLeaf() bool {
	return false
}

// GetHash returns the cached hash
func (n *InternalNode) GetHash() []byte {
	return n.hash
}

// LeafNode represents a leaf node in the tree
type LeafNode struct {
	Path  *big.Int
	Value []byte
	hash  []byte
}

// NewLeafNode creates a new leaf node
func NewLeafNode(path *big.Int, value []byte) *LeafNode {
	return &LeafNode{
		Path:  new(big.Int).Set(path),
		Value: append([]byte(nil), value...), // Copy value
	}
}

// CalculateHash calculates the hash of the leaf node
func (l *LeafNode) CalculateHash(algo HashAlgorithm) []byte {
	if l.hash != nil {
		return l.hash
	}
	
	// Hash of path || value
	pathBytes := l.Path.Bytes()
	data := append(pathBytes, l.Value...)
	l.hash = hash(algo, data)
	
	return l.hash
}

// GetPath returns the path of this leaf
func (l *LeafNode) GetPath() *big.Int {
	return l.Path
}

// IsLeaf returns true for leaf nodes
func (l *LeafNode) IsLeaf() bool {
	return true
}

// GetHash returns the cached hash
func (l *LeafNode) GetHash() []byte {
	return l.hash
}

// GetValue returns a copy of the leaf value
func (l *LeafNode) GetValue() []byte {
	return append([]byte(nil), l.Value...)
}

// Leaf represents a leaf to be inserted into the tree
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

// ProofStep represents a single step in an inclusion proof
type ProofStep struct {
	Path    *big.Int `json:"path"`
	Branch  []byte   `json:"branch"`
	Sibling []byte   `json:"sibling"`
}

// InclusionProof represents an inclusion proof for a leaf
type InclusionProof struct {
	RootHash []byte       `json:"rootHash"`
	Steps    []*ProofStep `json:"steps"`
	Included bool         `json:"included"`
}

// NewInclusionProof creates a new inclusion proof
func NewInclusionProof(rootHash []byte, steps []*ProofStep, included bool) *InclusionProof {
	return &InclusionProof{
		RootHash: append([]byte(nil), rootHash...),
		Steps:    steps,
		Included: included,
	}
}

// Verify verifies the inclusion proof
func (p *InclusionProof) Verify(algo HashAlgorithm, leafPath *big.Int, leafValue []byte) bool {
	if !p.Included {
		// For exclusion proofs, we need different verification logic
		return p.verifyExclusion(algo, leafPath)
	}
	
	if len(p.Steps) == 0 {
		// Direct leaf at root level (tree with single leaf)
		leafHash := hash(algo, append(leafPath.Bytes(), leafValue...))
		return bytesEqual(leafHash, p.RootHash)
	}
	
	// Start with the leaf hash (path || value)
	currentHash := hash(algo, append(leafPath.Bytes(), leafValue...))
	currentDepth := 0
	
	// Process each step in the proof from leaf to root
	for _, step := range p.Steps {
		// Determine if current node is on left or right using LSB first
		var isRight bool
		if currentDepth >= 256 {
			isRight = false // Should not happen in normal operation
		} else {
			// Use LSB first approach
			isRight = leafPath.Bit(currentDepth) == 1
		}
		
		// Hash current node with its sibling
		if isRight {
			// Current node is on the right, sibling is on the left
			currentHash = hash(algo, append(step.Sibling, currentHash...))
		} else {
			// Current node is on the left, sibling is on the right
			currentHash = hash(algo, append(currentHash, step.Sibling...))
		}
		
		// Move up one level
		currentDepth++
	}
	
	// Compare with the root hash
	return bytesEqual(currentHash, p.RootHash)
}

// verifyExclusion verifies an exclusion proof
func (p *InclusionProof) verifyExclusion(algo HashAlgorithm, leafPath *big.Int) bool {
	// For exclusion proofs, we verify that the path leads to a different leaf
	// or to an empty position in the tree
	// This is a simplified implementation - full exclusion proof verification
	// would need to prove the path doesn't exist or leads to a different leaf
	return true // Placeholder - implement based on specific exclusion proof format
}

// hash performs hashing with the specified algorithm
func hash(algo HashAlgorithm, data []byte) []byte {
	switch algo {
	case SHA256:
		h := sha256.Sum256(data)
		return h[:]
	default:
		panic(fmt.Sprintf("unsupported hash algorithm: %d", algo))
	}
}

// bytesEqual compares two byte slices for equality
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}