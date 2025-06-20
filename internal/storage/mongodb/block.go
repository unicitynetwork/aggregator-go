package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
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
	// Convert to BSON format for storage
	blockBSON := block.ToBSON()

	_, err := bs.collection.InsertOne(ctx, blockBSON)
	if err != nil {
		return fmt.Errorf("failed to store block: %w", err)
	}
	return nil
}

// GetByNumber retrieves a block by number
func (bs *BlockStorage) GetByNumber(ctx context.Context, blockNumber *api.BigInt) (*models.Block, error) {
	var blockBSON models.BlockBSON
	err := bs.collection.FindOne(ctx, bson.M{"index": blockNumber.String()}).Decode(&blockBSON)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get block by number: %w", err)
	}

	block, err := blockBSON.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to convert from BSON: %w", err)
	}
	return block, nil
}

// GetLatest retrieves the latest block
func (bs *BlockStorage) GetLatest(ctx context.Context) (*models.Block, error) {
	opts := options.FindOne().SetSort(bson.M{"index": -1})

	var blockBSON models.BlockBSON
	err := bs.collection.FindOne(ctx, bson.M{}, opts).Decode(&blockBSON)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	block, err := blockBSON.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to convert from BSON: %w", err)
	}
	return block, nil
}

// GetLatestNumber retrieves the latest block number
func (bs *BlockStorage) GetLatestNumber(ctx context.Context) (*api.BigInt, error) {
	// Get all block indices and find the maximum numerically
	opts := options.Find().SetProjection(bson.M{"index": 1})
	cursor, err := bs.collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find blocks: %w", err)
	}
	defer cursor.Close(ctx)

	var maxBlockNumber *api.BigInt

	for cursor.Next(ctx) {
		var result struct {
			Index string `bson:"index"`
		}

		if err := cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode block: %w", err)
		}

		blockNumber, err := api.NewBigIntFromString(result.Index)
		if err != nil {
			continue // Skip invalid block numbers
		}

		if maxBlockNumber == nil || blockNumber.Cmp(maxBlockNumber.Int) > 0 {
			maxBlockNumber = blockNumber
		}
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return maxBlockNumber, nil
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
func (bs *BlockStorage) GetRange(ctx context.Context, fromBlock, toBlock *api.BigInt) ([]*models.Block, error) {
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
		var blockBSON models.BlockBSON
		if err := cursor.Decode(&blockBSON); err != nil {
			return nil, fmt.Errorf("failed to decode block: %w", err)
		}

		block, err := blockBSON.FromBSON()
		if err != nil {
			return nil, fmt.Errorf("failed to convert from BSON: %w", err)
		}
		blocks = append(blocks, block)
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
