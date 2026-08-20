package models

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// CertificationRequest represents a state transition certification request
type CertificationRequest struct {
	ID                    primitive.ObjectID `json:"-"`
	Version               uint32             `json:"version"`
	StateID               api.StateID        `json:"stateId"`
	CertificationData     CertificationData  `json:"certificationData"`
	AggregateRequestCount uint64             `json:"aggregateRequestCount"`
	// ReferenceTime is the reference time of the round this request's leaf was
	// created in. Zero until the round that materialises the leaf pins it.
	ReferenceTime uint64 `json:"referenceTime"`
	// EffectiveTimeout is the absolute consensus-time deadline used for queue
	// admission. It is assigned by the service when CertificationData.ExpiresAt
	// is absent, and otherwise repeats that explicit deadline.
	EffectiveTimeout uint64         `json:"effectiveTimeout"`
	CreatedAt        *api.Timestamp `json:"createdAt"`
	ProcessedAt      *api.Timestamp `json:"processedAt,omitempty"`
	StreamID         string         `json:"-"` // Redis stream ID used for stream acknowledgements
}

// CertificationRequestBSON represents the BSON version of CertificationRequest for MongoDB storage
type CertificationRequestBSON struct {
	ID                    primitive.ObjectID    `bson:"_id,omitempty"`
	Version               uint32                `bson:"version"`
	StateID               string                `bson:"stateId"`
	TransactionHash       string                `bson:"transactionHash"`
	CertificationData     CertificationDataBSON `bson:"certificationData"`
	AggregateRequestCount uint64                `bson:"aggregateRequestCount"`
	ReferenceTime         uint64                `bson:"referenceTime"`
	EffectiveTimeout      uint64                `bson:"effectiveTimeout,omitempty"`
	CreatedAt             time.Time             `bson:"createdAt"`
	ProcessedAt           *time.Time            `bson:"processedAt,omitempty"`
}

// NewCertificationRequest creates a new certification request
func NewCertificationRequest(stateID api.StateID, certData CertificationData) *CertificationRequest {
	return &CertificationRequest{
		Version:               2,
		StateID:               stateID,
		CertificationData:     certData,
		AggregateRequestCount: 1, // Default to 1 for direct requests
		CreatedAt:             api.Now(),
	}
}

// NewCertificationRequestWithAggregate creates a new certification request with aggregate count
func NewCertificationRequestWithAggregate(stateID api.StateID, certData CertificationData, aggregateCount uint64) *CertificationRequest {
	return &CertificationRequest{
		Version:               2,
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
		Version:               c.Version,
		StateID:               c.StateID.String(),
		CertificationData:     c.CertificationData.ToBSON(),
		AggregateRequestCount: c.AggregateRequestCount,
		ReferenceTime:         c.ReferenceTime,
		EffectiveTimeout:      c.EffectiveTimeout,
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
	stateID, err := api.NewImprintV2(cb.StateID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode stateID: %w", err)
	}

	return &CertificationRequest{
		ID:                    cb.ID,
		Version:               cb.Version,
		StateID:               stateID,
		CertificationData:     *certData,
		AggregateRequestCount: cb.AggregateRequestCount,
		ReferenceTime:         cb.ReferenceTime,
		EffectiveTimeout:      cb.EffectiveTimeout,
		CreatedAt:             api.NewTimestamp(cb.CreatedAt),
		ProcessedAt:           processedAt,
	}, nil
}

func (c *CertificationRequest) ToAPI() *api.CertificationRequest {
	return &api.CertificationRequest{
		StateID:               c.StateID,
		CertificationData:     *c.CertificationData.ToAPI(),
		AggregateRequestCount: c.AggregateRequestCount,
	}
}

// LeafValue returns the SMT leaf value for this request under the given round
// reference time: H(txhash, referenceTime). The reference time is a property of
// the leaf, not of whichever inclusion proof later establishes it.
func (c *CertificationRequest) LeafValue(referenceTime uint64) ([]byte, error) {
	if c.Version != 2 {
		return nil, fmt.Errorf("invalid version: %d", c.Version)
	}
	return api.LeafValue(c.CertificationData.TransactionHash.DataBytes(), referenceTime), nil
}
