package models

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// AggregatorRecord represents a finalized commitment with proof data
type AggregatorRecord struct {
	RequestID             api.RequestID       `json:"requestId"`
	TransactionHash       api.TransactionHash `json:"transactionHash"`
	Authenticator         Authenticator       `json:"authenticator"`
	AggregateRequestCount uint64              `json:"aggregateRequestCount"`
	BlockNumber           *api.BigInt         `json:"blockNumber"`
	LeafIndex             *api.BigInt         `json:"leafIndex"`
	CreatedAt             *api.Timestamp      `json:"createdAt"`
	FinalizedAt           *api.Timestamp      `json:"finalizedAt"`
}

// AggregatorRecordBSON represents the BSON version of AggregatorRecord for MongoDB storage
type AggregatorRecordBSON struct {
	RequestID             string               `bson:"requestId"`
	TransactionHash       string               `bson:"transactionHash"`
	Authenticator         AuthenticatorBSON    `bson:"authenticator"`
	AggregateRequestCount uint64               `bson:"aggregateRequestCount"`
	BlockNumber           primitive.Decimal128 `bson:"blockNumber"`
	LeafIndex             primitive.Decimal128 `bson:"leafIndex"`
	CreatedAt             time.Time            `bson:"createdAt"`
	FinalizedAt           time.Time            `bson:"finalizedAt"`
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
func (ar *AggregatorRecord) ToBSON() (*AggregatorRecordBSON, error) {
	blockNumber, err := primitive.ParseDecimal128(ar.BlockNumber.String())
	if err != nil {
		return nil, fmt.Errorf("error converting block number to decimal-128: %w", err)
	}
	leafIndex, err := primitive.ParseDecimal128(ar.LeafIndex.String())
	if err != nil {
		return nil, fmt.Errorf("error converting leaf index to decimal-128: %w", err)
	}
	return &AggregatorRecordBSON{
		RequestID:             string(ar.RequestID),
		TransactionHash:       string(ar.TransactionHash),
		Authenticator:         ar.Authenticator.ToBSON(),
		AggregateRequestCount: ar.AggregateRequestCount,
		BlockNumber:           blockNumber,
		LeafIndex:             leafIndex,
		CreatedAt:             ar.CreatedAt.Time,
		FinalizedAt:           ar.FinalizedAt.Time,
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
