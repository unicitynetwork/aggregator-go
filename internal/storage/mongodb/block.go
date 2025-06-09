package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
)

const blockCollection = "blocks"

// BlockStorage implements block storage for MongoDB
type BlockStorage struct {
	collection *mongo.Collection
}

// NewBlockStorage creates a new block storage instance
func NewBlockStorage(db *mongo.Database) *BlockStorage {
	return &BlockStorage{
		collection: db.Collection(blockCollection),
	}
}

// Store stores a new block
func (bs *BlockStorage) Store(ctx context.Context, block *models.Block) error {
	_, err := bs.collection.InsertOne(ctx, block)
	if err != nil {
		return fmt.Errorf("failed to store block: %w", err)
	}
	return nil
}

// GetByNumber retrieves a block by number
func (bs *BlockStorage) GetByNumber(ctx context.Context, blockNumber *models.BigInt) (*models.Block, error) {
	var block models.Block
	err := bs.collection.FindOne(ctx, bson.M{"index": blockNumber.String()}).Decode(&block)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get block by number: %w", err)
	}
	return &block, nil
}

// GetLatest retrieves the latest block
func (bs *BlockStorage) GetLatest(ctx context.Context) (*models.Block, error) {
	opts := options.FindOne().SetSort(bson.M{"index": -1})
	
	var block models.Block
	err := bs.collection.FindOne(ctx, bson.M{}, opts).Decode(&block)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}
	return &block, nil
}

// GetLatestNumber retrieves the latest block number
func (bs *BlockStorage) GetLatestNumber(ctx context.Context) (*models.BigInt, error) {
	opts := options.FindOne().
		SetSort(bson.M{"index": -1}).
		SetProjection(bson.M{"index": 1})
	
	var result struct {
		Index *models.BigInt `bson:"index"`
	}
	
	err := bs.collection.FindOne(ctx, bson.M{}, opts).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return models.NewBigInt(nil), nil // Return 0 if no blocks exist
		}
		return nil, fmt.Errorf("failed to get latest block number: %w", err)
	}
	return result.Index, nil
}

// Count returns the total number of blocks
func (bs *BlockStorage) Count(ctx context.Context) (int64, error) {
	count, err := bs.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("failed to count blocks: %w", err)
	}
	return count, nil
}

// GetRange retrieves blocks in a range
func (bs *BlockStorage) GetRange(ctx context.Context, fromBlock, toBlock *models.BigInt) ([]*models.Block, error) {
	filter := bson.M{
		"index": bson.M{
			"$gte": fromBlock.String(),
			"$lte": toBlock.String(),
		},
	}
	
	opts := options.Find().SetSort(bson.M{"index": 1})
	cursor, err := bs.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find blocks in range: %w", err)
	}
	defer cursor.Close(ctx)

	var blocks []*models.Block
	for cursor.Next(ctx) {
		var block models.Block
		if err := cursor.Decode(&block); err != nil {
			return nil, fmt.Errorf("failed to decode block: %w", err)
		}
		blocks = append(blocks, &block)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return blocks, nil
}

// CreateIndexes creates necessary indexes for the block collection
func (bs *BlockStorage) CreateIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "index", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "timestamp", Value: -1}},
		},
		{
			Keys: bson.D{{Key: "createdAt", Value: -1}},
		},
		{
			Keys: bson.D{{Key: "chainId", Value: 1}},
		},
	}

	_, err := bs.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create block indexes: %w", err)
	}

	return nil
}