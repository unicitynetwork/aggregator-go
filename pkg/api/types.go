// Package api provides public JSON-RPC request and response types for the Unicity Aggregator.
// These types can be imported and used by clients to interact with the aggregator service.
package api

import "github.com/unicitynetwork/aggregator-go/internal/models"

// SubmitCommitmentRequest represents the submit_commitment JSON-RPC request
type SubmitCommitmentRequest struct {
	RequestID       models.RequestID       `json:"requestId"`
	TransactionHash models.TransactionHash `json:"transactionHash"`
	Authenticator   models.Authenticator   `json:"authenticator"`
	Receipt         *bool                  `json:"receipt,omitempty"`
}

// SubmitCommitmentResponse represents the submit_commitment JSON-RPC response
type SubmitCommitmentResponse struct {
	Status  string          `json:"status"`
	Receipt *models.Receipt `json:"receipt,omitempty"`
}

// GetInclusionProofRequest represents the get_inclusion_proof JSON-RPC request
type GetInclusionProofRequest struct {
	RequestID models.RequestID `json:"requestId"`
}

// GetInclusionProofResponse represents the get_inclusion_proof JSON-RPC response
type GetInclusionProofResponse struct {
	InclusionProof *models.InclusionProof `json:"inclusionProof"`
}

// GetNoDeletionProofResponse represents the get_no_deletion_proof JSON-RPC response
type GetNoDeletionProofResponse struct {
	NoDeletionProof *models.NoDeletionProof `json:"noDeletionProof"`
}

// GetBlockHeightResponse represents the get_block_height JSON-RPC response
type GetBlockHeightResponse struct {
	BlockNumber *models.BigInt `json:"blockNumber"`
}

// GetBlockRequest represents the get_block JSON-RPC request
type GetBlockRequest struct {
	BlockNumber interface{} `json:"blockNumber"` // Can be number, string, or "latest"
}

// GetBlockResponse represents the get_block JSON-RPC response
type GetBlockResponse struct {
	Block *models.Block `json:"block"`
}

// GetBlockCommitmentsRequest represents the get_block_commitments JSON-RPC request
type GetBlockCommitmentsRequest struct {
	BlockNumber *models.BigInt `json:"blockNumber"`
}

// GetBlockCommitmentsResponse represents the get_block_commitments JSON-RPC response
type GetBlockCommitmentsResponse struct {
	Commitments []*models.AggregatorRecord `json:"commitments"`
}