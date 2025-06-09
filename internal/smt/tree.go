package smt

import (
	"fmt"
	"math/big"
	"sort"
)

// SparseMerkleTree represents a sparse merkle tree
type SparseMerkleTree struct {
	algorithm HashAlgorithm
	root      *RootNode
}

// NewSparseMerkleTree creates a new sparse merkle tree
func NewSparseMerkleTree(algorithm HashAlgorithm) *SparseMerkleTree {
	return &SparseMerkleTree{
		algorithm: algorithm,
		root:      NewRootNode(nil, nil),
	}
}

// GetRootHash returns the root hash of the tree
func (smt *SparseMerkleTree) GetRootHash() []byte {
	return smt.root.CalculateHash(smt.algorithm)
}

// GetRoot returns the root node
func (smt *SparseMerkleTree) GetRoot() *RootNode {
	return smt.root
}

// AddLeaf adds a single leaf to the tree
func (smt *SparseMerkleTree) AddLeaf(path *big.Int, value []byte) error {
	leaf := NewLeaf(path, value)
	return smt.AddLeaves([]*Leaf{leaf})
}

// AddLeaves adds multiple leaves to the tree in a batch operation
// This is optimized to only calculate the root hash once after all leaves are added
func (smt *SparseMerkleTree) AddLeaves(leaves []*Leaf) error {
	if len(leaves) == 0 {
		return nil
	}
	
	// Sort leaves by path for optimal tree construction
	sortedLeaves := make([]*Leaf, len(leaves))
	copy(sortedLeaves, leaves)
	sort.Slice(sortedLeaves, func(i, j int) bool {
		return sortedLeaves[i].Path.Cmp(sortedLeaves[j].Path) < 0
	})
	
	// Remove duplicates (keep the last one for each path)
	uniqueLeaves := make([]*Leaf, 0, len(sortedLeaves))
	seen := make(map[string]bool)
	
	for _, leaf := range sortedLeaves {
		pathKey := leaf.Path.String()
		if !seen[pathKey] {
			uniqueLeaves = append(uniqueLeaves, leaf)
			seen[pathKey] = true
		}
	}
	
	// Build new tree with all leaves
	newRoot, err := smt.buildTreeWithLeaves(uniqueLeaves)
	if err != nil {
		return fmt.Errorf("failed to build tree with leaves: %w", err)
	}
	
	smt.root = newRoot
	return nil
}

// buildTreeWithLeaves builds a new tree containing existing leaves plus new leaves
func (smt *SparseMerkleTree) buildTreeWithLeaves(newLeaves []*Leaf) (*RootNode, error) {
	// Collect all existing leaves
	existingLeaves := smt.collectAllLeaves(smt.root)
	
	// Merge existing and new leaves
	allLeaves := make([]*Leaf, 0, len(existingLeaves)+len(newLeaves))
	allLeaves = append(allLeaves, existingLeaves...)
	allLeaves = append(allLeaves, newLeaves...)
	
	// Sort by path
	sort.Slice(allLeaves, func(i, j int) bool {
		return allLeaves[i].Path.Cmp(allLeaves[j].Path) < 0
	})
	
	// Remove duplicates (new leaves override existing ones)
	uniqueLeaves := make([]*Leaf, 0, len(allLeaves))
	pathMap := make(map[string]*Leaf)
	
	for _, leaf := range allLeaves {
		pathKey := leaf.Path.String()
		pathMap[pathKey] = leaf
	}
	
	for _, leaf := range pathMap {
		uniqueLeaves = append(uniqueLeaves, leaf)
	}
	
	// Sort again after deduplication
	sort.Slice(uniqueLeaves, func(i, j int) bool {
		return uniqueLeaves[i].Path.Cmp(uniqueLeaves[j].Path) < 0
	})
	
	// Build new tree from scratch
	return smt.buildTreeFromLeaves(uniqueLeaves), nil
}

// collectAllLeaves recursively collects all leaves from the tree
func (smt *SparseMerkleTree) collectAllLeaves(node Node) []*Leaf {
	if node == nil {
		return nil
	}
	
	if node.IsLeaf() {
		leaf := node.(*LeafNode)
		return []*Leaf{NewLeaf(leaf.Path, leaf.Value)}
	}
	
	var leaves []*Leaf
	
	switch n := node.(type) {
	case *RootNode:
		leaves = append(leaves, smt.collectAllLeaves(n.Left)...)
		leaves = append(leaves, smt.collectAllLeaves(n.Right)...)
	case *InternalNode:
		leaves = append(leaves, smt.collectAllLeaves(n.Left)...)
		leaves = append(leaves, smt.collectAllLeaves(n.Right)...)
	}
	
	return leaves
}

// buildTreeFromLeaves builds a complete tree from a sorted list of leaves
func (smt *SparseMerkleTree) buildTreeFromLeaves(leaves []*Leaf) *RootNode {
	if len(leaves) == 0 {
		return NewRootNode(nil, nil)
	}
	
	if len(leaves) == 1 {
		leaf := NewLeafNode(leaves[0].Path, leaves[0].Value)
		// Determine placement based on bit 0 (LSB)
		if leaves[0].Path.Bit(0) == 0 {
			return NewRootNode(leaf, nil)
		} else {
			return NewRootNode(nil, leaf)
		}
	}
	
	// Group leaves by their bit 0 (LSB) for root level
	var leftLeaves, rightLeaves []*Leaf
	
	for _, leaf := range leaves {
		// Use bit 0 (LSB) for navigation
		if leaf.Path.Bit(0) == 0 {
			leftLeaves = append(leftLeaves, leaf)
		} else {
			rightLeaves = append(rightLeaves, leaf)
		}
	}
	
	// Build left and right subtrees
	var leftChild, rightChild Node
	
	if len(leftLeaves) > 0 {
		leftChild = smt.buildSubtree(leftLeaves, 1)
	}
	
	if len(rightLeaves) > 0 {
		rightChild = smt.buildSubtree(rightLeaves, 1)
	}
	
	return NewRootNode(leftChild, rightChild)
}

// buildSubtree recursively builds a subtree for leaves at a given depth
func (smt *SparseMerkleTree) buildSubtree(leaves []*Leaf, depth int) Node {
	if len(leaves) == 0 {
		return nil
	}
	
	if len(leaves) == 1 {
		return NewLeafNode(leaves[0].Path, leaves[0].Value)
	}
	
	// If we've gone too deep, we have leaves with identical paths
	// This shouldn't happen in normal cases, but handle it gracefully
	if depth >= 256 {
		// Return the last leaf (effectively an update operation)
		return NewLeafNode(leaves[len(leaves)-1].Path, leaves[len(leaves)-1].Value)
	}
	
	// Split leaves based on the bit at current depth
	var leftLeaves, rightLeaves []*Leaf
	
	for _, leaf := range leaves {
		// Determine direction based on bit at current depth (LSB first)
		goLeft := leaf.Path.Bit(depth) == 0
		
		if goLeft {
			leftLeaves = append(leftLeaves, leaf)
		} else {
			rightLeaves = append(rightLeaves, leaf)
		}
	}
	
	// Build child nodes recursively
	var leftChild, rightChild Node
	
	if len(leftLeaves) > 0 {
		leftChild = smt.buildSubtree(leftLeaves, depth+1)
	}
	
	if len(rightLeaves) > 0 {
		rightChild = smt.buildSubtree(rightLeaves, depth+1)
	}
	
	// For simplicity, use root nodes at every level instead of internal nodes
	// This avoids complications with internal node path handling
	return NewRootNode(leftChild, rightChild)
}


// GetLeaf retrieves a leaf by its path
func (smt *SparseMerkleTree) GetLeaf(path *big.Int) (*LeafNode, error) {
	return smt.findLeaf(smt.root, path, 0)
}

// findLeaf recursively searches for a leaf with the given path
func (smt *SparseMerkleTree) findLeaf(node Node, path *big.Int, depth int) (*LeafNode, error) {
	if node == nil {
		return nil, fmt.Errorf("leaf not found")
	}
	
	if node.IsLeaf() {
		leaf := node.(*LeafNode)
		if leaf.Path.Cmp(path) == 0 {
			return leaf, nil
		}
		return nil, fmt.Errorf("leaf not found")
	}
	
	// Determine which child to follow based on the bit at current depth
	// Use LSB-first approach which is more natural for sparse merkle trees
	var goLeft bool
	
	if depth >= 256 { // Prevent infinite recursion with reasonable depth limit
		// We've gone too deep, this shouldn't happen in normal operation
		goLeft = true
	} else {
		// Use depth as bit position directly (LSB first)
		goLeft = path.Bit(depth) == 0
	}
	
	var nextNode Node
	nextDepth := depth + 1
	
	// Only handle Root nodes in the simplified approach
	if rootNode, ok := node.(*RootNode); ok {
		if goLeft {
			nextNode = rootNode.Left
		} else {
			nextNode = rootNode.Right
		}
	} else {
		return nil, fmt.Errorf("unexpected node type")
	}
	
	return smt.findLeaf(nextNode, path, nextDepth)
}


// GenerateInclusionProof generates an inclusion proof for the given path
func (smt *SparseMerkleTree) GenerateInclusionProof(path *big.Int) (*InclusionProof, error) {
	rootHash := smt.GetRootHash()
	steps, included, err := smt.generateProofSteps(smt.root, path, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to generate proof steps: %w", err)
	}
	
	return NewInclusionProof(rootHash, steps, included), nil
}

// generateProofSteps recursively generates proof steps from root to target leaf
func (smt *SparseMerkleTree) generateProofSteps(node Node, targetPath *big.Int, depth int) ([]*ProofStep, bool, error) {
	if node == nil {
		// Path leads to empty position - generate exclusion proof
		return []*ProofStep{}, false, nil
	}
	
	if node.IsLeaf() {
		leaf := node.(*LeafNode)
		if leaf.Path.Cmp(targetPath) == 0 {
			// Found the target leaf - inclusion proof, no more steps needed
			return []*ProofStep{}, true, nil
		}
		// Found different leaf - exclusion proof
		return []*ProofStep{}, false, nil
	}
	
	// Determine which child to follow using consistent bit navigation
	var goLeft bool
	
	if depth >= 256 {
		goLeft = true
	} else {
		// Use LSB first approach
		goLeft = targetPath.Bit(depth) == 0
	}
	
	var nextNode Node
	var siblingNode Node
	
	// Only handle Root nodes in the simplified approach
	if rootNode, ok := node.(*RootNode); ok {
		if goLeft {
			nextNode = rootNode.Left
			siblingNode = rootNode.Right
		} else {
			nextNode = rootNode.Right
			siblingNode = rootNode.Left
		}
	} else {
		return nil, false, fmt.Errorf("unexpected node type")
	}
	
	// Recursively get steps from child
	childSteps, childIncluded, childErr := smt.generateProofSteps(nextNode, targetPath, depth+1)
	if childErr != nil {
		return nil, false, childErr
	}
	
	var steps []*ProofStep
	
	// Only add step if we found the leaf (inclusion proof)
	if childIncluded {
		var siblingHash []byte
		if siblingNode != nil {
			siblingHash = siblingNode.CalculateHash(smt.algorithm)
		} else {
			siblingHash = make([]byte, 32) // Zero hash for empty sibling
		}
		
		// Create proof step for this level
		step := &ProofStep{
			Path:    new(big.Int).Set(targetPath),
			Branch:  []byte{}, // Empty for simplicity
			Sibling: siblingHash,
		}
		
		steps = append(childSteps, step)
	}
	
	return steps, childIncluded, nil
}

// VerifyInclusionProof verifies an inclusion proof against a root hash
func (smt *SparseMerkleTree) VerifyInclusionProof(proof *InclusionProof, leafPath *big.Int, leafValue []byte) bool {
	return proof.Verify(smt.algorithm, leafPath, leafValue)
}