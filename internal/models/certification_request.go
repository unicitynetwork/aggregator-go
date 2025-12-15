package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// CertificationRequest represents a state transition certification request
type CertificationRequest struct {
	ID                    primitive.ObjectID `json:"-"`
	Version               int                `json:"version"`
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
	Version               int                   `json:"version"`
	StateID               string                `bson:"requestId"` // keep stateID stored as "requestId"
	TransactionHash       string                `bson:"transactionHash"`
	CertificationData     CertificationDataBSON `bson:"certificationData"`
	AggregateRequestCount uint64                `bson:"aggregateRequestCount"`
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
		Version:               cb.Version,
		StateID:               api.StateID(cb.StateID),
		CertificationData:     *certData,
		AggregateRequestCount: cb.AggregateRequestCount,
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
