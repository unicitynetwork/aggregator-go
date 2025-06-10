package api

import (
	"fmt"
)

// InclusionProof represents a merkle inclusion proof
type InclusionProof struct {
	RequestID   RequestID   `json:"requestId"`
	BlockNumber *BigInt     `json:"blockNumber,omitempty"`
	LeafIndex   *BigInt     `json:"leafIndex,omitempty"`
	Proof       []ProofNode `json:"proof"`
	RootHash    HexBytes    `json:"rootHash"`
	Included    bool        `json:"included"`
}

// ProofNode represents a single node in an inclusion proof
type ProofNode struct {
	Hash      HexBytes `json:"hash"`
	Direction string   `json:"direction"` // "left" or "right"
}

// NewInclusionProof creates a new inclusion proof
func NewInclusionProof(requestID RequestID, blockNumber, leafIndex *BigInt, proof []ProofNode, rootHash HexBytes, included bool) *InclusionProof {
	return &InclusionProof{
		RequestID:   requestID,
		BlockNumber: blockNumber,
		LeafIndex:   leafIndex,
		Proof:       proof,
		RootHash:    rootHash,
		Included:    included,
	}
}

// TODO remove?

// APIInclusionProof represents a proof for external API (TypeScript compatible)
type APIInclusionProof struct {
	MerkleTreePath  *MerkleTreePath `json:"merkleTreePath"`
	Authenticator   *Authenticator  `json:"authenticator"`
	TransactionHash *HexBytes       `json:"transactionHash"`
}

// MerkleTreeStep represents a single step in a Merkle tree path
type MerkleTreeStep struct {
	Branch  []string `json:"branch"`
	Path    string   `json:"path"`
	Sibling *string  `json:"sibling"`
}

// MerkleTreePath represents the path to verify inclusion in a Merkle tree
type MerkleTreePath struct {
	Root  string           `json:"root"`
	Steps []MerkleTreeStep `json:"steps"`
}

// ValidateInclusionProof validates the consistency of an APIInclusionProof
// Both authenticator and transaction hash must be set, or both must be null
func ValidateInclusionProof(proof *APIInclusionProof) error {
	if proof == nil {
		return fmt.Errorf("inclusion proof cannot be nil")
	}

	// Check consistency: both must be set or both must be null
	authenticatorSet := proof.Authenticator != nil
	transactionHashSet := proof.TransactionHash != nil

	if authenticatorSet != transactionHashSet {
		return fmt.Errorf("Authenticator and transaction hash must be both set or both null")
	}

	return nil
}
