package v1

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// AggregatorRecordV1 represents a finalized commitment with proof data
type AggregatorRecordV1 struct {
	RequestID             api.RequestID       `json:"requestId"`
	TransactionHash       api.TransactionHash `json:"transactionHash"`
	Authenticator         Authenticator       `json:"authenticator"`
	AggregateRequestCount uint64              `json:"aggregateRequestCount"`
	BlockNumber           *api.BigInt         `json:"blockNumber"`
	LeafIndex             *api.BigInt         `json:"leafIndex"`
	CreatedAt             *api.Timestamp      `json:"createdAt"`
	FinalizedAt           *api.Timestamp      `json:"finalizedAt"`
}

// AggregatorRecordV1BSON represents the BSON version of AggregatorRecord for MongoDB storage
type AggregatorRecordV1BSON struct {
	RequestID             string               `bson:"requestId"`
	TransactionHash       string               `bson:"transactionHash"`
	Authenticator         AuthenticatorBSON    `bson:"authenticator"`
	AggregateRequestCount uint64               `bson:"aggregateRequestCount"`
	BlockNumber           primitive.Decimal128 `bson:"blockNumber"`
	LeafIndex             primitive.Decimal128 `bson:"leafIndex"`
	CreatedAt             time.Time            `bson:"createdAt"`
	FinalizedAt           time.Time            `bson:"finalizedAt"`
}

// NewAggregatorRecordV1 creates a new aggregator record from a commitment
func NewAggregatorRecordV1(commitment *Commitment, blockNumber, leafIndex *api.BigInt) *AggregatorRecordV1 {
	return &AggregatorRecordV1{
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
func (ar *AggregatorRecordV1) ToBSON() (*AggregatorRecordV1BSON, error) {
	blockNumber, err := primitive.ParseDecimal128(ar.BlockNumber.String())
	if err != nil {
		return nil, fmt.Errorf("error converting block number to decimal-128: %w", err)
	}
	leafIndex, err := primitive.ParseDecimal128(ar.LeafIndex.String())
	if err != nil {
		return nil, fmt.Errorf("error converting leaf index to decimal-128: %w", err)
	}
	return &AggregatorRecordV1BSON{
		RequestID:             ar.RequestID.String(),
		TransactionHash:       ar.TransactionHash.String(),
		Authenticator:         ar.Authenticator.ToBSON(),
		AggregateRequestCount: ar.AggregateRequestCount,
		BlockNumber:           blockNumber,
		LeafIndex:             leafIndex,
		CreatedAt:             ar.CreatedAt.Time,
		FinalizedAt:           ar.FinalizedAt.Time,
	}, nil
}

// FromBSON converts AggregatorRecordBSON back to AggregatorRecord
func (arb *AggregatorRecordV1BSON) FromBSON() (*AggregatorRecordV1, error) {
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

	return &AggregatorRecordV1{
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

func AggregatorRecordFromV1(v1 *AggregatorRecordV1) (*models.AggregatorRecord, error) {
	return &models.AggregatorRecord{
		Version: 1,
		StateID: v1.RequestID,
		CertificationData: models.CertificationData{
			OwnerPredicate:  api.NewPayToPublicKeyPredicate(v1.Authenticator.PublicKey),
			SourceStateHash: v1.Authenticator.StateHash,
			TransactionHash: v1.TransactionHash,
			Witness:         v1.Authenticator.Signature,
		},
		AggregateRequestCount: v1.AggregateRequestCount,
		BlockNumber:           v1.BlockNumber,
		LeafIndex:             v1.LeafIndex,
		CreatedAt:             v1.CreatedAt,
		FinalizedAt:           v1.FinalizedAt,
	}, nil
}
