package models

import (
	"fmt"
	"time"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// AggregatorRecord represents a finalized commitment with proof data
type AggregatorRecord struct {
	RequestID             api.RequestID       `json:"requestId" bson:"requestId"`
	TransactionHash       api.TransactionHash `json:"transactionHash" bson:"transactionHash"`
	Authenticator         Authenticator       `json:"authenticator" bson:"authenticator"`
	AggregateRequestCount uint64              `json:"aggregateRequestCount" bson:"aggregateRequestCount"`
	BlockNumber           *api.BigInt         `json:"blockNumber" bson:"blockNumber"`
	LeafIndex             *api.BigInt         `json:"leafIndex" bson:"leafIndex"`
	CreatedAt             *api.Timestamp      `json:"createdAt" bson:"createdAt"`
	FinalizedAt           *api.Timestamp      `json:"finalizedAt" bson:"finalizedAt"`
}

// AggregatorRecordBSON represents the BSON version of AggregatorRecord for MongoDB storage
type AggregatorRecordBSON struct {
	RequestID             string            `bson:"requestId"`
	TransactionHash       string            `bson:"transactionHash"`
	Authenticator         AuthenticatorBSON `bson:"authenticator"`
	AggregateRequestCount uint64            `bson:"aggregateRequestCount"`
	BlockNumber           string            `bson:"blockNumber"`
	LeafIndex             string            `bson:"leafIndex"`
	CreatedAt             time.Time         `bson:"createdAt"`
	FinalizedAt           time.Time         `bson:"finalizedAt"`
}

// NewAggregatorRecord creates a new aggregator record from a commitment
func NewAggregatorRecord(commitment *Commitment, blockNumber, leafIndex *api.BigInt) *AggregatorRecord {
	return &AggregatorRecord{
		RequestID:             commitment.RequestID,
		TransactionHash:       commitment.TransactionHash,
		Authenticator:         commitment.Authenticator,
		AggregateRequestCount: commitment.AggregateRequestCount,
		BlockNumber:           blockNumber,
		LeafIndex:             leafIndex,
		CreatedAt:             commitment.CreatedAt,
		FinalizedAt:           api.Now(),
	}
}

// ToBSON converts AggregatorRecord to AggregatorRecordBSON for MongoDB storage
func (ar *AggregatorRecord) ToBSON() *AggregatorRecordBSON {
	return &AggregatorRecordBSON{
		RequestID:             string(ar.RequestID),
		TransactionHash:       string(ar.TransactionHash),
		Authenticator:         ar.Authenticator.ToBSON(),
		AggregateRequestCount: ar.AggregateRequestCount,
		BlockNumber:           ar.BlockNumber.String(),
		LeafIndex:             ar.LeafIndex.String(),
		CreatedAt:             ar.CreatedAt.Time,
		FinalizedAt:           ar.FinalizedAt.Time,
	}
}

// FromBSON converts AggregatorRecordBSON back to AggregatorRecord
func (arb *AggregatorRecordBSON) FromBSON() (*AggregatorRecord, error) {
	blockNumber, err := api.NewBigIntFromString(arb.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to parse blockNumber: %w", err)
	}

	leafIndex, err := api.NewBigIntFromString(arb.LeafIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse leafIndex: %w", err)
	}

	// Default AggregateRequestCount to 1 if not present (backward compatibility)
	aggregateRequestCount := arb.AggregateRequestCount
	if aggregateRequestCount == 0 {
		aggregateRequestCount = 1
	}

	authenticatorBSON, err := arb.Authenticator.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to parse authenticator: %w", err)
	}

	return &AggregatorRecord{
		RequestID:             api.RequestID(arb.RequestID),
		TransactionHash:       api.TransactionHash(arb.TransactionHash),
		Authenticator:         *authenticatorBSON,
		AggregateRequestCount: aggregateRequestCount,
		BlockNumber:           blockNumber,
		LeafIndex:             leafIndex,
		CreatedAt:             api.NewTimestamp(arb.CreatedAt),
		FinalizedAt:           api.NewTimestamp(arb.FinalizedAt),
	}, nil
}
