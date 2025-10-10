package models

import (
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// BlockRecords represents the mapping of block numbers to request IDs
type BlockRecords struct {
	BlockNumber *api.BigInt     `json:"blockNumber" bson:"blockNumber"`
	RequestIDs  []api.RequestID `json:"requestIds" bson:"requestIds"`
	CreatedAt   *api.Timestamp  `json:"createdAt" bson:"createdAt"`
}

// BlockRecordsBSON is the MongoDB representation of BlockRecords
type BlockRecordsBSON struct {
	BlockNumber primitive.Decimal128 `bson:"blockNumber"`
	RequestIDs  []api.RequestID      `bson:"requestIds"`
	CreatedAt   string               `bson:"createdAt"`
}

// NewBlockRecords creates a new block records entry
func NewBlockRecords(blockNumber *api.BigInt, requestIDs []api.RequestID) *BlockRecords {
	return &BlockRecords{
		BlockNumber: blockNumber,
		RequestIDs:  requestIDs,
		CreatedAt:   api.Now(),
	}
}

// ToBSON converts BlockRecords to BlockRecordsBSON
func (br *BlockRecords) ToBSON() *BlockRecordsBSON {
	blockNumberDecimal, err := primitive.ParseDecimal128(br.BlockNumber.String())
	if err != nil {
		// This should never happen with valid BigInt, but fallback to zero
		blockNumberDecimal = primitive.NewDecimal128(0, 0)
	}

	return &BlockRecordsBSON{
		BlockNumber: blockNumberDecimal,
		RequestIDs:  br.RequestIDs,
		CreatedAt:   strconv.FormatInt(br.CreatedAt.UnixMilli(), 10),
	}
}

// FromBSON converts BlockRecordsBSON to BlockRecords
func (brb *BlockRecordsBSON) FromBSON() (*BlockRecords, error) {
	blockNumber, err := api.NewBigIntFromString(brb.BlockNumber.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse blockNumber: %w", err)
	}

	createdAtMillis, err := strconv.ParseInt(brb.CreatedAt, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse createdAt: %w", err)
	}
	createdAt := &api.Timestamp{Time: time.UnixMilli(createdAtMillis)}

	return &BlockRecords{
		BlockNumber: blockNumber,
		RequestIDs:  brb.RequestIDs,
		CreatedAt:   createdAt,
	}, nil
}
