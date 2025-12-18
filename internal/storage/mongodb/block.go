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
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
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
	blockBSON, err := block.ToBSON()
	if err != nil {
		return fmt.Errorf("failed to convert block to bson: %w", err)
	}
	_, err = bs.collection.InsertOne(ctx, blockBSON)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("block already exists: %w", interfaces.ErrDuplicateKey)
		}
		return fmt.Errorf("failed to store block: %w", err)
	}
	return nil
}

// GetByNumber retrieves a block by number
func (bs *BlockStorage) GetByNumber(ctx context.Context, blockNumber *api.BigInt) (*models.Block, error) {
	var blockBSON models.BlockBSON
	indexDecimal := bigIntToDecimal128(blockNumber)
	filter := bson.M{"index": indexDecimal, "finalized": true}
	err := bs.collection.FindOne(ctx, filter).Decode(&blockBSON)
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

// GetLatest retrieves the latest finalized block
func (bs *BlockStorage) GetLatest(ctx context.Context) (*models.Block, error) {
	opts := options.FindOne().SetSort(bson.M{"index": -1})
	filter := bson.M{"finalized": true}

	var blockBSON models.BlockBSON
	err := bs.collection.FindOne(ctx, filter, opts).Decode(&blockBSON)
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

// GetLatestNumber retrieves the latest finalized block number
func (bs *BlockStorage) GetLatestNumber(ctx context.Context) (*api.BigInt, error) {
	opts := options.FindOne().
		SetProjection(bson.M{"index": 1}).
		SetSort(bson.M{"index": -1})
	filter := bson.M{"finalized": true}

	var result struct {
		Index primitive.Decimal128 `bson:"index"`
	}

	err := bs.collection.FindOne(ctx, filter, opts).Decode(&result)
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

// GetLatestByRootHash retrieves the latest finalized block with the given root hash
func (bs *BlockStorage) GetLatestByRootHash(ctx context.Context, rootHash api.HexBytes) (*models.Block, error) {
	filter := bson.M{"rootHash": rootHash.String(), "finalized": true}
	opts := options.FindOne().SetSort(bson.M{"index": -1})

	var blockBSON models.BlockBSON
	err := bs.collection.FindOne(ctx, filter, opts).Decode(&blockBSON)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get latest block by root hash: %w", err)
	}

	block, err := blockBSON.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to convert from BSON: %w", err)
	}
	return block, nil
}

// Count returns the total number of blocks
func (bs *BlockStorage) Count(ctx context.Context) (int64, error) {
	count, err := bs.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("failed to count blocks: %w", err)
	}
	return count, nil
}

// GetRange retrieves finalized blocks in a range
func (bs *BlockStorage) GetRange(ctx context.Context, fromBlock, toBlock *api.BigInt) ([]*models.Block, error) {
	fromDecimal := bigIntToDecimal128(fromBlock)
	toDecimal := bigIntToDecimal128(toBlock)

	filter := bson.M{
		"index": bson.M{
			"$gte": fromDecimal,
			"$lte": toDecimal,
		},
		"finalized": true,
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
		{
			Keys: bson.D{{Key: "rootHash", Value: 1}, {Key: "index", Value: -1}},
		},
		{
			Keys: bson.D{{Key: "finalized", Value: 1}, {Key: "index", Value: -1}},
		},
	}

	_, err := bs.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create block indexes: %w", err)
	}

	// Migration: Set finalized=true for old blocks without the field. Remove after deployment.
	_, err = bs.collection.UpdateMany(ctx,
		bson.M{"finalized": bson.M{"$exists": false}},
		bson.M{"$set": bson.M{"finalized": true}},
	)
	if err != nil {
		return fmt.Errorf("failed to migrate blocks finalized field: %w", err)
	}

	return nil
}

// SetFinalized marks a block as finalized or unfinalized
func (bs *BlockStorage) SetFinalized(ctx context.Context, blockNumber *api.BigInt, finalized bool) error {
	indexDecimal := bigIntToDecimal128(blockNumber)
	filter := bson.M{"index": indexDecimal}
	update := bson.M{"$set": bson.M{"finalized": finalized}}

	result, err := bs.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to set block finalized status: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("block %s not found", blockNumber.String())
	}
	return nil
}

// GetUnfinalized returns all unfinalized blocks (should be at most 1 in normal operation)
func (bs *BlockStorage) GetUnfinalized(ctx context.Context) ([]*models.Block, error) {
	filter := bson.M{"finalized": false}
	opts := options.Find().SetSort(bson.M{"index": 1})

	cursor, err := bs.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find unfinalized blocks: %w", err)
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
