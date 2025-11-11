package models

import (
	"fmt"
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
	RequestIDs  []string             `bson:"requestIds"`
	CreatedAt   time.Time            `bson:"createdAt"`
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
func (br *BlockRecords) ToBSON() (*BlockRecordsBSON, error) {
	blockNumberDecimal, err := primitive.ParseDecimal128(br.BlockNumber.String())
	if err != nil {
		return nil, fmt.Errorf("error converting block number to decimal: %w", err)
	}

	requestIDs := make([]string, len(br.RequestIDs))
	for i, r := range br.RequestIDs {
		requestIDs[i] = r.String()
	}

	return &BlockRecordsBSON{
		BlockNumber: blockNumberDecimal,
		RequestIDs:  requestIDs,
		CreatedAt:   br.CreatedAt.Time,
	}, nil
}

// FromBSON converts BlockRecordsBSON to BlockRecords
func (brb *BlockRecordsBSON) FromBSON() (*BlockRecords, error) {
	blockNumber, _, err := brb.BlockNumber.BigInt()
	if err != nil {
		return nil, fmt.Errorf("failed to parse blockNumber: %w", err)
	}

	requestIDs := make([]api.RequestID, len(brb.RequestIDs))
	for i, r := range brb.RequestIDs {
		requestIDs[i] = api.RequestID(r)
	}

	return &BlockRecords{
		BlockNumber: api.NewBigInt(blockNumber),
		RequestIDs:  requestIDs,
		CreatedAt:   api.NewTimestamp(brb.CreatedAt),
	}, nil
}
