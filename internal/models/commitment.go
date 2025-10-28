package models

import (
	"crypto/sha256"
	"fmt"

	"github.com/fxamacker/cbor/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Commitment represents a state transition request
type Commitment struct {
	ID                    primitive.ObjectID  `json:"-" bson:"_id,omitempty"`
	RequestID             api.RequestID       `json:"requestId" bson:"requestId"`
	TransactionHash       api.TransactionHash `json:"transactionHash" bson:"transactionHash"`
	Authenticator         Authenticator       `json:"authenticator" bson:"authenticator"`
	AggregateRequestCount uint64              `json:"aggregateRequestCount" bson:"aggregateRequestCount"`
	CreatedAt             *api.Timestamp      `json:"createdAt" bson:"createdAt"`
	ProcessedAt           *api.Timestamp      `json:"processedAt,omitempty" bson:"processedAt,omitempty"`
	StreamID              string              `json:"-" bson:"-"` // Redis stream ID used for stream acknowledgements
}

// Authenticator represents the authentication data for a commitment
type Authenticator struct {
	Algorithm string        `json:"algorithm" bson:"algorithm"`
	PublicKey api.HexBytes  `json:"publicKey" bson:"publicKey"`
	Signature api.HexBytes  `json:"signature" bson:"signature"`
	StateHash api.StateHash `json:"stateHash" bson:"stateHash"`
}

// NewCommitment creates a new commitment
func NewCommitment(requestID api.RequestID, transactionHash api.TransactionHash, authenticator Authenticator) *Commitment {
	return &Commitment{
		RequestID:             requestID,
		TransactionHash:       transactionHash,
		Authenticator:         authenticator,
		AggregateRequestCount: 1, // Default to 1 for direct requests
		CreatedAt:             api.Now(),
	}
}

// NewCommitmentWithAggregate creates a new commitment with aggregate count
func NewCommitmentWithAggregate(requestID api.RequestID, transactionHash api.TransactionHash, authenticator Authenticator, aggregateCount uint64) *Commitment {
	return &Commitment{
		RequestID:             requestID,
		TransactionHash:       transactionHash,
		Authenticator:         authenticator,
		AggregateRequestCount: aggregateCount,
		CreatedAt:             api.Now(),
	}
}

// CreateLeafValue creates the value to store in the SMT leaf for a commitment
// This matches the TypeScript LeafValue.create() method exactly:
// - CBOR encode the authenticator as an array [algorithm, publicKey, signature, stateHashImprint]
// - Hash the CBOR-encoded authenticator and transaction hash imprint using SHA256
// - Return as DataHash imprint format (2-byte algorithm prefix + hash)
func (c *Commitment) CreateLeafValue() ([]byte, error) {
	// Get the state hash imprint for CBOR encoding
	stateHashImprint, err := c.Authenticator.StateHash.Imprint()
	if err != nil {
		return nil, fmt.Errorf("failed to get state hash imprint: %w", err)
	}

	// CBOR encode the authenticator as an array (matching TypeScript authenticator.toCBOR())
	// TypeScript: [algorithm, publicKey, signature.encode(), stateHash.imprint]
	authenticatorArray := []interface{}{
		c.Authenticator.Algorithm,         // algorithm as text string
		[]byte(c.Authenticator.PublicKey), // publicKey as byte string
		[]byte(c.Authenticator.Signature), // signature as byte string
		stateHashImprint,                  // stateHash.imprint as byte string
	}

	authenticatorCBOR, err := cbor.Marshal(authenticatorArray)
	if err != nil {
		return nil, fmt.Errorf("failed to CBOR encode authenticator: %w", err)
	}

	// Get the transaction hash imprint
	transactionHashImprint, err := c.TransactionHash.Imprint()
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction hash imprint: %w", err)
	}

	// Create SHA256 hasher and update with CBOR-encoded authenticator and transaction hash imprint
	// This matches the TypeScript DataHasher(SHA256).update(authenticator.toCBOR()).update(transactionHash.imprint).digest()
	hasher := sha256.New()
	hasher.Write(authenticatorCBOR)
	hasher.Write(transactionHashImprint)

	// Get the final hash
	hash := hasher.Sum(nil)

	// Return as DataHash imprint with SHA256 algorithm prefix (0x00, 0x00)
	imprint := make([]byte, 2+len(hash))
	imprint[0] = 0x00 // SHA256 algorithm high byte
	imprint[1] = 0x00 // SHA256 algorithm low byte
	copy(imprint[2:], hash[:])

	return imprint, nil
}
