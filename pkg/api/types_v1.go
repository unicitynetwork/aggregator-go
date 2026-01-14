// Package api provides public JSON-RPC request and response types for the Unicity Aggregator.
// These types can be imported and used by clients to interact with the aggregator service.
package api

import (
	"crypto/sha256"
	"fmt"
)

// Authenticator represents the authentication data for a commitment
type Authenticator struct {
	Algorithm string          `json:"algorithm"` // Algorithm used for signing
	PublicKey HexBytes        `json:"publicKey"`
	Signature HexBytes        `json:"signature"`
	StateHash SourceStateHash `json:"stateHash"`
}

// Commitment represents a state transition request
type Commitment struct {
	RequestID             RequestID       `json:"requestId"`
	TransactionHash       TransactionHash `json:"transactionHash"`
	Authenticator         Authenticator   `json:"authenticator"`
	AggregateRequestCount uint64          `json:"aggregateRequestCount,omitempty,string"`
	CreatedAt             *Timestamp      `json:"createdAt"`
	ProcessedAt           *Timestamp      `json:"processedAt,omitempty"`
}

// NewCommitment creates a new commitment
func NewCommitment(requestID RequestID, transactionHash TransactionHash, authenticator Authenticator) *Commitment {
	return &Commitment{
		RequestID:             requestID,
		TransactionHash:       transactionHash,
		Authenticator:         authenticator,
		AggregateRequestCount: 1, // Default to 1 for direct requests
		CreatedAt:             Now(),
	}
}

// NewCommitmentWithAggregate creates a new commitment with aggregate count
func NewCommitmentWithAggregate(requestID RequestID, transactionHash TransactionHash, authenticator Authenticator, aggregateCount uint64) *Commitment {
	return &Commitment{
		RequestID:             requestID,
		TransactionHash:       transactionHash,
		Authenticator:         authenticator,
		AggregateRequestCount: aggregateCount,
		CreatedAt:             Now(),
	}
}

// ReceiptV1 represents a signed receipt for a commitment submission
type ReceiptV1 struct {
	Algorithm string           `json:"algorithm"`
	PublicKey HexBytes         `json:"publicKey"`
	Signature HexBytes         `json:"signature"`
	Request   ReceiptRequestV1 `json:"request"`
}

// ReceiptRequestV1 represents the request data in a receipt
type ReceiptRequestV1 struct {
	Service         string          `json:"service"`
	Method          string          `json:"method"`
	RequestID       RequestID       `json:"requestId"`
	TransactionHash TransactionHash `json:"transactionHash"`
	StateHash       SourceStateHash `json:"stateHash"`
}

// NewReceiptV1 creates a new receipt for a commitment
func NewReceiptV1(commitment *Commitment, algorithm string, publicKey, signature HexBytes) *ReceiptV1 {
	return &ReceiptV1{
		Algorithm: algorithm,
		PublicKey: publicKey,
		Signature: signature,
		Request: ReceiptRequestV1{
			Service:         "aggregator",
			Method:          "submit_commitment",
			RequestID:       commitment.RequestID,
			TransactionHash: commitment.TransactionHash,
			StateHash:       commitment.Authenticator.StateHash,
		},
	}
}

// JSON-RPC Request and Response Types

// SubmitCommitmentRequest represents the submit_commitment JSON-RPC request
type SubmitCommitmentRequest struct {
	RequestID             RequestID       `json:"requestId"`
	TransactionHash       TransactionHash `json:"transactionHash"`
	Authenticator         Authenticator   `json:"authenticator"`
	Receipt               *bool           `json:"receipt,omitempty"`
	AggregateRequestCount uint64          `json:"aggregateRequestCount,omitempty,string"`
}

// SubmitCommitmentResponse represents the submit_commitment JSON-RPC response
type SubmitCommitmentResponse struct {
	Status  string     `json:"status"`
	Receipt *ReceiptV1 `json:"receipt,omitempty"`
}

// GetInclusionProofRequestV1 represents the get_inclusion_proof JSON-RPC request
type GetInclusionProofRequestV1 struct {
	RequestID RequestID `json:"requestId"`
}

// GetInclusionProofResponseV1 represents the get_inclusion_proof JSON-RPC response
type GetInclusionProofResponseV1 struct {
	InclusionProof *InclusionProofV1 `json:"inclusionProof"`
}

type InclusionProofV1 struct {
	Authenticator      *Authenticator   `json:"authenticator"`
	MerkleTreePath     *MerkleTreePath  `json:"merkleTreePath"`
	TransactionHash    *TransactionHash `json:"transactionHash"`
	UnicityCertificate HexBytes         `json:"unicityCertificate"`
}

// GetBlockCommitmentsRequest represents the get_block_commitments JSON-RPC request
type GetBlockCommitmentsRequest struct {
	BlockNumber *BigInt `json:"blockNumber"`
}

// GetBlockCommitmentsResponse represents the get_block_commitments JSON-RPC response
type GetBlockCommitmentsResponse struct {
	Commitments []*AggregatorRecordV1 `json:"commitments"`
}

type RequestID = ImprintHexString

// CreateRequestID creates a RequestID from public key and state hash
func CreateRequestID(publicKey []byte, stateHash ImprintHexString) (RequestID, error) {
	stateHashBytes, err := stateHash.Imprint()
	if err != nil {
		return "", fmt.Errorf("failed to convert state hash to bytes: %w", err)
	}
	return CreateRequestIDFromBytes(publicKey, stateHashBytes)
}

func CreateRequestIDFromBytes(publicKey []byte, stateHashBytes []byte) (RequestID, error) {
	// Create the data to hash: publicKey + stateHash
	data := make([]byte, 0, len(publicKey)+len(stateHashBytes))
	data = append(data, publicKey...)
	data = append(data, stateHashBytes...)

	return NewImprintHexString(fmt.Sprintf("0000%x", sha256.Sum256(data)))
}

func ValidateRequestID(requestID RequestID, publicKey []byte, stateHashBytes []byte) (bool, error) {
	expectedRequestID, err := CreateRequestIDFromBytes(publicKey, stateHashBytes)
	if err != nil {
		return false, err
	}

	return requestID == expectedRequestID, nil
}

// AggregatorRecordV1 represents a finalized commitment with proof data
type AggregatorRecordV1 struct {
	RequestID             RequestID       `json:"requestId"`
	TransactionHash       TransactionHash `json:"transactionHash"`
	Authenticator         Authenticator   `json:"authenticator"`
	AggregateRequestCount uint64          `json:"aggregateRequestCount,omitempty,string"`
	BlockNumber           *BigInt         `json:"blockNumber"`
	LeafIndex             *BigInt         `json:"leafIndex"`
	CreatedAt             *Timestamp      `json:"createdAt"`
	FinalizedAt           *Timestamp      `json:"finalizedAt"`
}

// NewAggregatorRecordV1 creates a new aggregator record from a commitment
func NewAggregatorRecordV1(commitment *Commitment, blockNumber, leafIndex *BigInt) *AggregatorRecordV1 {
	return &AggregatorRecordV1{
		RequestID:             commitment.RequestID,
		TransactionHash:       commitment.TransactionHash,
		Authenticator:         commitment.Authenticator,
		AggregateRequestCount: commitment.AggregateRequestCount,
		BlockNumber:           blockNumber,
		LeafIndex:             leafIndex,
		CreatedAt:             commitment.CreatedAt,
		FinalizedAt:           Now(),
	}
}
