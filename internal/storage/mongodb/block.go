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

// bigIntToDecimal128 converts a BigInt to Decimal128 for MongoDB queries
func bigIntToDecimal128(bigInt *api.BigInt) primitive.Decimal128 {
	decimal, err := primitive.ParseDecimal128(bigInt.String())
	if err != nil {
		// Fallback to zero if parsing fails (should never happen with valid BigInt)
		return primitive.NewDecimal128(0, 0)
	}
	return decimal
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
	indexDecimal := bigIntToDecimal128(blockNumber)
	err := bs.collection.FindOne(ctx, bson.M{"index": indexDecimal}).Decode(&blockBSON)
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
	opts := options.FindOne().
		SetProjection(bson.M{"index": 1}).
		SetSort(bson.M{"index": -1})

	var result struct {
		Index primitive.Decimal128 `bson:"index"`
	}

	err := bs.collection.FindOne(ctx, bson.M{}, opts).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get latest block number: %w", err)
	}

	blockNumber, err := api.NewBigIntFromString(result.Index.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse latest block number: %w", err)
	}

	return blockNumber, nil
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
	fromDecimal := bigIntToDecimal128(fromBlock)
	toDecimal := bigIntToDecimal128(toBlock)

	filter := bson.M{
		"index": bson.M{
			"$gte": fromDecimal,
			"$lte": toDecimal,
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
