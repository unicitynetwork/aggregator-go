package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
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
	_, err := brs.collection.InsertOne(ctx, records)
	if err != nil {
		return fmt.Errorf("failed to store block records: %w", err)
	}
	return nil
}

// GetByBlockNumber retrieves block records by block number
func (brs *BlockRecordsStorage) GetByBlockNumber(ctx context.Context, blockNumber *models.BigInt) (*models.BlockRecords, error) {
	var records models.BlockRecords
	err := brs.collection.FindOne(ctx, bson.M{"blockNumber": blockNumber.String()}).Decode(&records)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get block records by block number: %w", err)
	}
	return &records, nil
}

// GetByRequestID retrieves the block number for a request ID
func (brs *BlockRecordsStorage) GetByRequestID(ctx context.Context, requestID models.RequestID) (*models.BigInt, error) {
	filter := bson.M{"requestIds": requestID}
	opts := options.FindOne().SetProjection(bson.M{"blockNumber": 1})
	
	var result struct {
		BlockNumber *models.BigInt `bson:"blockNumber"`
	}
	
	err := brs.collection.FindOne(ctx, filter, opts).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get block number by request ID: %w", err)
	}
	
	return result.BlockNumber, nil
}

// Count returns the total number of block records
func (brs *BlockRecordsStorage) Count(ctx context.Context) (int64, error) {
	count, err := brs.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("failed to count block records: %w", err)
	}
	return count, nil
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