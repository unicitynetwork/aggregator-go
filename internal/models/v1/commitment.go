package v1

import (
	"fmt"
	"time"

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

// CommitmentBSON represents the BSON version of Commitment for MongoDB storage
type CommitmentBSON struct {
	ID                    primitive.ObjectID `bson:"_id,omitempty"`
	RequestID             string             `bson:"requestId"`
	TransactionHash       string             `bson:"transactionHash"`
	Authenticator         AuthenticatorBSON  `bson:"authenticator"`
	AggregateRequestCount uint64             `bson:"aggregateRequestCount"`
	CreatedAt             time.Time          `bson:"createdAt"`
	ProcessedAt           *time.Time         `bson:"processedAt,omitempty"`
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

// ToBSON converts Commitment to CommitmentBSON for MongoDB storage
func (c *Commitment) ToBSON() *CommitmentBSON {
	var processedAt *time.Time
	if c.ProcessedAt != nil {
		processedAt = &c.ProcessedAt.Time
	}
	return &CommitmentBSON{
		ID:                    c.ID,
		RequestID:             c.RequestID.String(),
		TransactionHash:       c.TransactionHash.String(),
		Authenticator:         c.Authenticator.ToBSON(),
		AggregateRequestCount: c.AggregateRequestCount,
		CreatedAt:             c.CreatedAt.Time,
		ProcessedAt:           processedAt,
	}
}

// FromBSON converts CommitmentBSON back to Commitment
func (cb *CommitmentBSON) FromBSON() (*Commitment, error) {
	var processedAt *api.Timestamp
	if cb.ProcessedAt != nil {
		processedAt = api.NewTimestamp(*cb.ProcessedAt)
	}
	authenticator, err := cb.Authenticator.FromBSON()
	if err != nil {
		return nil, err
	}

	requestID, err := api.NewImprintV2(cb.RequestID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode requestId: %w", err)
	}

	transactionHash, err := api.NewImprintV2(cb.TransactionHash)
	if err != nil {
		return nil, fmt.Errorf("failed to decode transactionHash: %w", err)
	}

	return &Commitment{
		ID:                    cb.ID,
		RequestID:             requestID,
		TransactionHash:       transactionHash,
		Authenticator:         *authenticator,
		AggregateRequestCount: cb.AggregateRequestCount,
		CreatedAt:             api.NewTimestamp(cb.CreatedAt),
		ProcessedAt:           processedAt,
	}, nil
}

func (c *Commitment) ToAPI() *api.Commitment {
	return &api.Commitment{
		RequestID:             c.RequestID,
		TransactionHash:       c.TransactionHash,
		Authenticator:         *c.Authenticator.ToAPI(),
		AggregateRequestCount: c.AggregateRequestCount,
		CreatedAt:             c.CreatedAt,
		ProcessedAt:           c.ProcessedAt,
	}
}
