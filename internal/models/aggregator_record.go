package models

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// AggregatorRecord represents a finalized state transition certification request with proof data
type AggregatorRecord struct {
	Version               uint32            `json:"version"`
	StateID               api.StateID       `json:"stateId"`
	CertificationData     CertificationData `json:"certificationData"`
	AggregateRequestCount uint64            `json:"aggregateRequestCount"`
	BlockNumber           *api.BigInt       `json:"blockNumber"`
	LeafIndex             *api.BigInt       `json:"leafIndex"`
	ProposalID            string            `json:"proposalId,omitempty"`
	CreatedAt             *api.Timestamp    `json:"createdAt"`
}

// AggregatorRecordBSON represents the BSON version of AggregatorRecord for MongoDB storage
type AggregatorRecordBSON struct {
	Version               uint32                `bson:"version"`
	StateID               string                `bson:"stateId"`
	CertificationData     CertificationDataBSON `bson:"certificationData"`
	AggregateRequestCount uint64                `bson:"aggregateRequestCount"`
	BlockNumber           primitive.Decimal128  `bson:"blockNumber"`
	LeafIndex             primitive.Decimal128  `bson:"leafIndex"`
	ProposalID            string                `bson:"proposalId,omitempty"`
	CreatedAt             time.Time             `bson:"createdAt"`
}

// NewAggregatorRecord creates a new aggregator record from a certification request
func NewAggregatorRecord(certRequest *CertificationRequest, blockNumber, leafIndex *api.BigInt) *AggregatorRecord {
	return &AggregatorRecord{
		Version:               certRequest.Version,
		StateID:               certRequest.StateID,
		CertificationData:     certRequest.CertificationData,
		AggregateRequestCount: certRequest.AggregateRequestCount,
		BlockNumber:           blockNumber,
		LeafIndex:             leafIndex,
		CreatedAt:             certRequest.CreatedAt,
	}
}

// ToBSON converts AggregatorRecord to AggregatorRecordBSON for MongoDB storage
func (ar *AggregatorRecord) ToBSON() (*AggregatorRecordBSON, error) {
	blockNumber, err := primitive.ParseDecimal128(ar.BlockNumber.String())
	if err != nil {
		return nil, fmt.Errorf("error converting block number to decimal-128: %w", err)
	}
	leafIndex, err := primitive.ParseDecimal128(ar.LeafIndex.String())
	if err != nil {
		return nil, fmt.Errorf("error converting leaf index to decimal-128: %w", err)
	}
	var createdAt time.Time
	if ar.CreatedAt != nil {
		createdAt = ar.CreatedAt.Time
	}
	return &AggregatorRecordBSON{
		Version:               ar.Version,
		StateID:               ar.StateID.String(),
		CertificationData:     ar.CertificationData.ToBSON(),
		AggregateRequestCount: ar.AggregateRequestCount,
		BlockNumber:           blockNumber,
		LeafIndex:             leafIndex,
		ProposalID:            ar.ProposalID,
		CreatedAt:             createdAt,
	}, nil
}

// FromBSON converts AggregatorRecordBSON back to AggregatorRecord
func (arb *AggregatorRecordBSON) FromBSON() (*AggregatorRecord, error) {
	blockNumber, err := api.NewBigIntFromString(arb.BlockNumber.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse blockNumber: %w", err)
	}

	leafIndex, err := api.NewBigIntFromString(arb.LeafIndex.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse leafIndex: %w", err)
	}

	certDataBSON, err := arb.CertificationData.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CertificationData: %w", err)
	}

	stateID, err := api.NewImprintV2(arb.StateID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode stateID: %w", err)
	}

	record := &AggregatorRecord{
		Version:               arb.Version,
		StateID:               stateID,
		CertificationData:     *certDataBSON,
		AggregateRequestCount: arb.AggregateRequestCount,
		BlockNumber:           blockNumber,
		LeafIndex:             leafIndex,
		ProposalID:            arb.ProposalID,
		CreatedAt:             api.NewTimestamp(arb.CreatedAt),
	}
	return record, nil
}
