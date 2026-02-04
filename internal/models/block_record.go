package models

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// BlockRecords represents the mapping of block numbers to state IDs
type BlockRecords struct {
	BlockNumber *api.BigInt    `json:"blockNumber"`
	StateIDs    []api.StateID  `json:"stateIds"`
	CreatedAt   *api.Timestamp `json:"createdAt"`
}

// BlockRecordsBSON is the MongoDB representation of BlockRecords
type BlockRecordsBSON struct {
	BlockNumber primitive.Decimal128 `bson:"blockNumber"`
	StateIDs    []string             `bson:"requestIds"` // keep requestId in BSON for backwards compatibility
	CreatedAt   time.Time            `bson:"createdAt"`
}

// NewBlockRecords creates a new block records entry
func NewBlockRecords(blockNumber *api.BigInt, stateIDs []api.StateID) *BlockRecords {
	return &BlockRecords{
		BlockNumber: blockNumber,
		StateIDs:    stateIDs,
		CreatedAt:   api.Now(),
	}
}

// ToBSON converts BlockRecords to BlockRecordsBSON
func (br *BlockRecords) ToBSON() (*BlockRecordsBSON, error) {
	blockNumberDecimal, err := primitive.ParseDecimal128(br.BlockNumber.String())
	if err != nil {
		return nil, fmt.Errorf("error converting block number to decimal: %w", err)
	}

	stateIDs := make([]string, len(br.StateIDs))
	for i, r := range br.StateIDs {
		stateIDs[i] = r.String()
	}

	return &BlockRecordsBSON{
		BlockNumber: blockNumberDecimal,
		StateIDs:    stateIDs,
		CreatedAt:   br.CreatedAt.Time,
	}, nil
}

// FromBSON converts BlockRecordsBSON to BlockRecords
func (brb *BlockRecordsBSON) FromBSON() (*BlockRecords, error) {
	blockNumber, _, err := brb.BlockNumber.BigInt()
	if err != nil {
		return nil, fmt.Errorf("failed to parse blockNumber: %w", err)
	}

	stateIDs := make([]api.StateID, len(brb.StateIDs))
	for i, r := range brb.StateIDs {
		stateIDs[i] = api.StateID(r)
	}

	return &BlockRecords{
		BlockNumber: api.NewBigInt(blockNumber),
		StateIDs:    stateIDs,
		CreatedAt:   api.NewTimestamp(brb.CreatedAt),
	}, nil
}
