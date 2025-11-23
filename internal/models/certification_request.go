package models

import (
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// CertificationRequest represents a state transition certification request
type CertificationRequest struct {
	ID                    primitive.ObjectID `json:"-"`
	StateID               api.StateID        `json:"stateId"`
	CertificationData     CertificationData  `json:"certificationData"`
	AggregateRequestCount uint64             `json:"aggregateRequestCount"`
	CreatedAt             *api.Timestamp     `json:"createdAt"`
	ProcessedAt           *api.Timestamp     `json:"processedAt,omitempty"`
	StreamID              string             `json:"-"` // Redis stream ID used for stream acknowledgements
}

// CertificationRequestBSON represents the BSON version of CertificationRequest for MongoDB storage
type CertificationRequestBSON struct {
	ID                    primitive.ObjectID    `bson:"_id,omitempty"`
	StateID               string                `bson:"stateId"`
	TransactionHash       string                `bson:"transactionHash"`
	CertificationData     CertificationDataBSON `bson:"certificationData"`
	AggregateRequestCount uint64                `bson:"aggregateRequestCount"`
	CreatedAt             time.Time             `bson:"createdAt"`
	ProcessedAt           *time.Time            `bson:"processedAt,omitempty"`
}

// NewCertificationRequest creates a new certification request
func NewCertificationRequest(stateID api.StateID, certData CertificationData) *CertificationRequest {
	return &CertificationRequest{
		StateID:               stateID,
		CertificationData:     certData,
		AggregateRequestCount: 1, // Default to 1 for direct requests
		CreatedAt:             api.Now(),
	}
}

// NewCertificationRequestWithAggregate creates a new certification request with aggregate count
func NewCertificationRequestWithAggregate(stateID api.StateID, certData CertificationData, aggregateCount uint64) *CertificationRequest {
	return &CertificationRequest{
		StateID:               stateID,
		CertificationData:     certData,
		AggregateRequestCount: aggregateCount,
		CreatedAt:             api.Now(),
	}
}

// ToBSON converts CertificationRequest to CertificationRequestBSON for MongoDB storage
func (c *CertificationRequest) ToBSON() *CertificationRequestBSON {
	var processedAt *time.Time
	if c.ProcessedAt != nil {
		processedAt = &c.ProcessedAt.Time
	}
	return &CertificationRequestBSON{
		ID:                    c.ID,
		StateID:               c.StateID.String(),
		CertificationData:     c.CertificationData.ToBSON(),
		AggregateRequestCount: c.AggregateRequestCount,
		CreatedAt:             c.CreatedAt.Time,
		ProcessedAt:           processedAt,
	}
}

// FromBSON converts CertificationRequestBSON back to CertificationRequest
func (cb *CertificationRequestBSON) FromBSON() (*CertificationRequest, error) {
	var processedAt *api.Timestamp
	if cb.ProcessedAt != nil {
		processedAt = api.NewTimestamp(*cb.ProcessedAt)
	}
	certData, err := cb.CertificationData.FromBSON()
	if err != nil {
		return nil, err
	}
	return &CertificationRequest{
		ID:                    cb.ID,
		StateID:               api.StateID(cb.StateID),
		CertificationData:     *certData,
		AggregateRequestCount: cb.AggregateRequestCount,
		CreatedAt:             api.NewTimestamp(cb.CreatedAt),
		ProcessedAt:           processedAt,
	}, nil
}

// CreateLeafValue creates the value to store in the SMT leaf for a certification request
//  1. CBOR encode the CertificationData as an array [publicKey, signature, sourceStateHash, transactionHash]
//  2. Hash the CBOR-encoded certification data using SHA256
//  3. Return as DataHash imprint format (2-byte algorithm prefix + hash of cbor array)
func (c *CertificationRequest) CreateLeafValue() ([]byte, error) {
	cborData, err := types.Cbor.Marshal(c.CertificationData)
	if err != nil {
		return nil, fmt.Errorf("failed to CBOR encode certification data: %w", err)
	}
	hash := sha256.Sum256(cborData)
	out := make([]byte, 34)
	copy(out[2:], hash[:])
	return out, nil
}
