package models

import (
	"fmt"
	"strconv"
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
	CreatedAt             string            `bson:"createdAt"`
	FinalizedAt           string            `bson:"finalizedAt"`
}

type AuthenticatorBSON struct {
	Algorithm string `bson:"algorithm"`
	PublicKey string `bson:"publicKey"`
	Signature string `bson:"signature"`
	StateHash string `bson:"stateHash"`
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
		RequestID:       string(ar.RequestID),
		TransactionHash: string(ar.TransactionHash),
		Authenticator: AuthenticatorBSON{
			Algorithm: ar.Authenticator.Algorithm,
			PublicKey: ar.Authenticator.PublicKey.String(),
			Signature: ar.Authenticator.Signature.String(),
			StateHash: ar.Authenticator.StateHash.String(),
		},
		AggregateRequestCount: ar.AggregateRequestCount,
		BlockNumber:           ar.BlockNumber.String(),
		LeafIndex:             ar.LeafIndex.String(),
		CreatedAt:             strconv.FormatInt(ar.CreatedAt.UnixMilli(), 10),
		FinalizedAt:           strconv.FormatInt(ar.FinalizedAt.UnixMilli(), 10),
	}
}

// FromBSON converts AggregatorRecordBSON back to AggregatorRecord
func (arb *AggregatorRecordBSON) FromBSON() (*AggregatorRecord, error) {
	//requestID, err := NewRequestID(arb.RequestID)
	//if err != nil {
	//	return nil, fmt.Errorf("failed to parse requestID: %w", err)
	//}
	//
	//transactionHash, err := NewTransactionHash(arb.TransactionHash)
	//if err != nil {
	//	return nil, fmt.Errorf("failed to parse transactionHash: %w", err)
	//}
	//
	blockNumber, err := api.NewBigIntFromString(arb.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to parse blockNumber: %w", err)
	}

	leafIndex, err := api.NewBigIntFromString(arb.LeafIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse leafIndex: %w", err)
	}

	publicKey, err := api.NewHexBytesFromString(arb.Authenticator.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse publicKey: %w", err)
	}

	signature, err := api.NewHexBytesFromString(arb.Authenticator.Signature)
	if err != nil {
		return nil, fmt.Errorf("failed to parse signature: %w", err)
	}

	//stateHash, err := NewHexBytesFromString(arb.Authenticator.StateHash)
	//if err != nil {
	//	return nil, fmt.Errorf("failed to parse stateHash: %w", err)
	//}

	createdAtMillis, err := strconv.ParseInt(arb.CreatedAt, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse createdAt: %w", err)
	}
	createdAt := &api.Timestamp{Time: time.UnixMilli(createdAtMillis)}

	finalizedAtMillis, err := strconv.ParseInt(arb.FinalizedAt, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse finalizedAt: %w", err)
	}
	finalizedAt := &api.Timestamp{Time: time.UnixMilli(finalizedAtMillis)}

	// Default AggregateRequestCount to 1 if not present (backward compatibility)
	aggregateRequestCount := arb.AggregateRequestCount
	if aggregateRequestCount == 0 {
		aggregateRequestCount = 1
	}

	return &AggregatorRecord{
		RequestID:       api.RequestID(arb.RequestID),
		TransactionHash: api.TransactionHash(arb.TransactionHash),
		Authenticator: Authenticator{
			Algorithm: arb.Authenticator.Algorithm,
			PublicKey: publicKey,
			Signature: signature,
			StateHash: api.StateHash(arb.Authenticator.StateHash),
		},
		AggregateRequestCount: aggregateRequestCount,
		BlockNumber:           blockNumber,
		LeafIndex:             leafIndex,
		CreatedAt:             createdAt,
		FinalizedAt:           finalizedAt,
	}, nil
}
