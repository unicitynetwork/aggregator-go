// Package api provides public JSON-RPC request and response types for the Unicity Aggregator.
// These types can be imported and used by clients to interact with the aggregator service.
package api

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
)

// Basic types for API
type StateHash = ImprintHexString
type TransactionHash = ImprintHexString

// Authenticator represents the authentication data for a commitment
type Authenticator struct {
	Algorithm string    `json:"algorithm"` // Algorithm used for signing
	PublicKey HexBytes  `json:"publicKey"`
	Signature HexBytes  `json:"signature"`
	StateHash StateHash `json:"stateHash"`
}

// Commitment represents a state transition request
type Commitment struct {
	RequestID       RequestID       `json:"requestId"`
	TransactionHash TransactionHash `json:"transactionHash"`
	Authenticator   Authenticator   `json:"authenticator"`
	CreatedAt       *Timestamp      `json:"createdAt"`
	ProcessedAt     *Timestamp      `json:"processedAt,omitempty"`
}

// NewCommitment creates a new commitment
func NewCommitment(requestID RequestID, transactionHash TransactionHash, authenticator Authenticator) *Commitment {
	return &Commitment{
		RequestID:       requestID,
		TransactionHash: transactionHash,
		Authenticator:   authenticator,
		CreatedAt:       Now(),
	}
}

// Timestamp wraps time.Time for consistent JSON serialization
type Timestamp struct {
	time.Time
}

// NewTimestamp creates a new Timestamp
func NewTimestamp(t time.Time) *Timestamp {
	return &Timestamp{Time: t}
}

// Now creates a Timestamp for current time
func Now() *Timestamp {
	return &Timestamp{Time: time.Now()}
}

// MarshalJSON implements json.Marshaler
func (t *Timestamp) MarshalJSON() ([]byte, error) {
	// Use Unix timestamp in milliseconds as string
	millis := t.Time.UnixMilli()
	return json.Marshal(strconv.FormatInt(millis, 10))
}

// UnmarshalJSON implements json.Unmarshaler
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	millis, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	t.Time = time.UnixMilli(millis)
	return nil
}

// MarshalBSONValue implements bson.ValueMarshaler
func (t *Timestamp) MarshalBSONValue() (bsontype.Type, []byte, error) {
	if t == nil {
		return bson.TypeNull, nil, nil
	}
	// Use Unix timestamp in milliseconds as int64 for BSON
	millis := t.Time.UnixMilli()
	return bson.MarshalValue(millis)
}

// UnmarshalBSONValue implements bson.ValueUnmarshaler
func (t *Timestamp) UnmarshalBSONValue(bsonType bsontype.Type, data []byte) error {
	if bsonType == bson.TypeNull {
		t.Time = time.Time{}
		return nil
	}

	var millis int64
	err := bson.UnmarshalValue(bsonType, data, &millis)
	if err != nil {
		return err
	}

	t.Time = time.UnixMilli(millis)
	return nil
}

// AggregatorRecord represents a finalized commitment with proof data
type AggregatorRecord struct {
	RequestID       RequestID       `json:"requestId"`
	TransactionHash TransactionHash `json:"transactionHash"`
	Authenticator   Authenticator   `json:"authenticator"`
	BlockNumber     *BigInt         `json:"blockNumber"`
	LeafIndex       *BigInt         `json:"leafIndex"`
	CreatedAt       *Timestamp      `json:"createdAt"`
	FinalizedAt     *Timestamp      `json:"finalizedAt"`
}

// NewAggregatorRecord creates a new aggregator record from a commitment
func NewAggregatorRecord(commitment *Commitment, blockNumber, leafIndex *BigInt) *AggregatorRecord {
	return &AggregatorRecord{
		RequestID:       commitment.RequestID,
		TransactionHash: commitment.TransactionHash,
		Authenticator:   commitment.Authenticator,
		BlockNumber:     blockNumber,
		LeafIndex:       leafIndex,
		CreatedAt:       commitment.CreatedAt,
		FinalizedAt:     Now(),
	}
}

// Block represents a blockchain block
type Block struct {
	Index               *BigInt    `json:"index"`
	ChainID             string     `json:"chainId"`
	Version             string     `json:"version"`
	ForkID              string     `json:"forkId"`
	RootHash            HexBytes   `json:"rootHash"`
	PreviousBlockHash   HexBytes   `json:"previousBlockHash"`
	NoDeletionProofHash HexBytes   `json:"noDeletionProofHash"`
	CreatedAt           *Timestamp `json:"createdAt"`
	UnicityCertificate  HexBytes   `json:"unicityCertificate"`
}

// NewBlock creates a new block
func NewBlock(index *BigInt, chainID, version, forkID string, rootHash, previousBlockHash, unicityCertificate HexBytes) *Block {
	return &Block{
		Index:              index,
		ChainID:            chainID,
		Version:            version,
		ForkID:             forkID,
		RootHash:           rootHash,
		PreviousBlockHash:  previousBlockHash,
		CreatedAt:          Now(),
		UnicityCertificate: unicityCertificate,
	}
}

// NoDeletionProof represents a no-deletion proof
type NoDeletionProof struct {
	Proof     HexBytes   `json:"proof"`
	CreatedAt *Timestamp `json:"createdAt"`
}

// NewNoDeletionProof creates a new no-deletion proof
func NewNoDeletionProof(proof HexBytes) *NoDeletionProof {
	return &NoDeletionProof{
		Proof:     proof,
		CreatedAt: Now(),
	}
}

// Receipt represents a signed receipt for a commitment submission
type Receipt struct {
	Algorithm string         `json:"algorithm"`
	PublicKey HexBytes       `json:"publicKey"`
	Signature HexBytes       `json:"signature"`
	Request   ReceiptRequest `json:"request"`
}

// ReceiptRequest represents the request data in a receipt
type ReceiptRequest struct {
	//Service         string          `json:"service"`
	//Method          string          `json:"method"`
	RequestID       RequestID       `json:"requestId"`
	TransactionHash TransactionHash `json:"transactionHash"`
	StateHash       StateHash       `json:"stateHash"`
}

// NewReceipt creates a new receipt for a commitment
func NewReceipt(commitment *Commitment, algorithm string, publicKey, signature HexBytes) *Receipt {
	return &Receipt{
		Algorithm: algorithm,
		PublicKey: publicKey,
		Signature: signature,
		Request: ReceiptRequest{
			//Service:         "aggregator",
			//Method:          "submit_commitment",
			RequestID:       commitment.RequestID,
			TransactionHash: commitment.TransactionHash,
			StateHash:       commitment.Authenticator.StateHash,
		},
	}
}

// JSON-RPC Request and Response Types

// SubmitCommitmentRequest represents the submit_commitment JSON-RPC request
type SubmitCommitmentRequest struct {
	RequestID       RequestID       `json:"requestId"`
	TransactionHash TransactionHash `json:"transactionHash"`
	Authenticator   Authenticator   `json:"authenticator"`
	Receipt         *bool           `json:"receipt,omitempty"`
}

// SubmitCommitmentResponse represents the submit_commitment JSON-RPC response
type SubmitCommitmentResponse struct {
	Status  string   `json:"status"`
	Receipt *Receipt `json:"receipt,omitempty"`
}

// GetInclusionProofRequest represents the get_inclusion_proof JSON-RPC request
type GetInclusionProofRequest struct {
	RequestID RequestID `json:"requestId"`
}

// GetInclusionProofResponse represents the get_inclusion_proof JSON-RPC response
type GetInclusionProofResponse struct {
	Authenticator   *Authenticator      `json:"authenticator"`
	MerkleTreePath  *smt.MerkleTreePath `json:"merkleTreePath"`
	TransactionHash *TransactionHash    `json:"transactionHash"`
}

// GetNoDeletionProofResponse represents the get_no_deletion_proof JSON-RPC response
type GetNoDeletionProofResponse struct {
	NoDeletionProof *NoDeletionProof `json:"noDeletionProof"`
}

// GetBlockHeightResponse represents the get_block_height JSON-RPC response
type GetBlockHeightResponse struct {
	BlockNumber *BigInt `json:"blockNumber"`
}

// GetBlockRequest represents the get_block JSON-RPC request
type GetBlockRequest struct {
	BlockNumber interface{} `json:"blockNumber"` // Can be number, string, or "latest"
}

// GetBlockResponse represents the get_block JSON-RPC response
type GetBlockResponse struct {
	Block *Block `json:"block"`
}

// GetBlockCommitmentsRequest represents the get_block_commitments JSON-RPC request
type GetBlockCommitmentsRequest struct {
	BlockNumber *BigInt `json:"blockNumber"`
}

// GetBlockCommitmentsResponse represents the get_block_commitments JSON-RPC response
type GetBlockCommitmentsResponse struct {
	Commitments []*AggregatorRecord `json:"commitments"`
}

// HealthStatus represents the health status of the service
type HealthStatus struct {
	Status   string            `json:"status"`
	Role     string            `json:"role"`
	ServerID string            `json:"serverId"`
	Details  map[string]string `json:"details,omitempty"`
}

// NewHealthStatus creates a new health status
func NewHealthStatus(role, serverID string) *HealthStatus {
	return &HealthStatus{
		Status:   "ok",
		Role:     role,
		ServerID: serverID,
		Details:  make(map[string]string),
	}
}

// AddDetail adds a detail to the health status
func (h *HealthStatus) AddDetail(key, value string) {
	if h.Details == nil {
		h.Details = make(map[string]string)
	}
	h.Details[key] = value
}
