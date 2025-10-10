package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const blockRecordsCollection = "block_records"

// BlockRecordsStorage implements block records storage for MongoDB
type BlockRecordsStorage struct {
	collection *mongo.Collection
}

// NewBlockRecordsStorage creates a new block records storage instance
func NewBlockRecordsStorage(db *mongo.Database) *BlockRecordsStorage {
	return &BlockRecordsStorage{
		collection: db.Collection(blockRecordsCollection),
	}
}

// Store stores a new block records entry
func (brs *BlockRecordsStorage) Store(ctx context.Context, records *models.BlockRecords) error {
	if records == nil {
		return errors.New("block records is nil")
	}
	_, err := brs.collection.InsertOne(ctx, records.ToBSON())
	if err != nil {
		return fmt.Errorf("failed to store block records: %w", err)
	}
	return nil
}

// GetByBlockNumber retrieves block records by block number
func (brs *BlockRecordsStorage) GetByBlockNumber(ctx context.Context, blockNumber *api.BigInt) (*models.BlockRecords, error) {
	var result models.BlockRecordsBSON
	filter := bson.M{"blockNumber": bigIntToDecimal128(blockNumber)}
	err := brs.collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get block records by block number: %w", err)
	}

	blockRecords, err := result.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to convert from BSON: %w", err)
	}
	return blockRecords, nil
}

// GetByRequestID retrieves the block number for a request ID
func (brs *BlockRecordsStorage) GetByRequestID(ctx context.Context, requestID api.RequestID) (*api.BigInt, error) {
	filter := bson.M{"requestIds": requestID}
	opts := options.FindOne().SetProjection(bson.M{"blockNumber": 1})

	var result struct {
		BlockNumber primitive.Decimal128 `bson:"blockNumber"`
	}

	err := brs.collection.FindOne(ctx, filter, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get block number by request ID: %w", err)
	}

	blockNumber, err := api.NewBigIntFromString(result.BlockNumber.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse block number: %w", err)
	}
	return blockNumber, nil
}

// Count returns the total number of block records
func (brs *BlockRecordsStorage) Count(ctx context.Context) (int64, error) {
	count, err := brs.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("failed to count block records: %w", err)
	}
	return count, nil
}

// GetNextBlock retrieves the next block record after the given block number.
// If blockNumber is nil then returns the very first block.
func (brs *BlockRecordsStorage) GetNextBlock(ctx context.Context, blockNumber *api.BigInt) (*models.BlockRecords, error) {
	var filter bson.M
	if blockNumber != nil {
		filter = bson.M{"blockNumber": bson.M{"$gt": bigIntToDecimal128(blockNumber)}}
	} else {
		filter = bson.M{}
	}
	opts := options.FindOne().SetSort(bson.D{{Key: "blockNumber", Value: 1}})

	var result models.BlockRecordsBSON
	err := brs.collection.FindOne(ctx, filter, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get next block record: %w", err)
	}

	blockRecord, err := result.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to convert from BSON: %w", err)
	}
	return blockRecord, nil
}

// GetLatestBlock retrieves the latest block
func (brs *BlockRecordsStorage) GetLatestBlock(ctx context.Context) (*models.BlockRecords, error) {
	opts := options.FindOne().SetSort(bson.D{{Key: "blockNumber", Value: -1}})

	var result models.BlockRecordsBSON
	err := brs.collection.FindOne(ctx, bson.M{}, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get latest block record: %w", err)
	}

	blockRecord, err := result.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to convert from BSON: %w", err)
	}
	return blockRecord, nil
}

// CreateIndexes creates necessary indexes for the block records collection
func (brs *BlockRecordsStorage) CreateIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "blockNumber", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "requestIds", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "createdAt", Value: -1}},
		},
	}

	_, err := brs.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create block records indexes: %w", err)
	}
	return nil
}
