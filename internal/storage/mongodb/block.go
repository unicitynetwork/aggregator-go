package mongodb

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const blockCollection = "blocks"

func finalizingBlockFilter() bson.M {
	return bson.M{"$or": []bson.M{
		{"status": bson.M{"$exists": false}},
		{"status": ""},
		{"status": models.FinalityStatusFinalizing},
	}}
}

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
	return bs.getByNumber(ctx, blockNumber, true)
}

// GetByNumberAnyFinalization retrieves a block regardless of its finality status.
func (bs *BlockStorage) GetByNumberAnyFinalization(ctx context.Context, blockNumber *api.BigInt) (*models.Block, error) {
	return bs.getByNumber(ctx, blockNumber, false)
}

func (bs *BlockStorage) getByNumber(ctx context.Context, blockNumber *api.BigInt, finalizedOnly bool) (*models.Block, error) {
	var blockBSON models.BlockBSON
	indexDecimal := bigIntToDecimal128(blockNumber)
	filter := bson.M{"index": indexDecimal}
	if finalizedOnly {
		filter["finalized"] = true
	}
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

// GetFinalizedPage retrieves a bounded page of finalized blocks after afterBlock, up to toBlock.
func (bs *BlockStorage) GetFinalizedPage(ctx context.Context, afterBlock, toBlock *api.BigInt, limit int) ([]*models.Block, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("finalized block page limit must be positive")
	}

	afterDecimal := bigIntToDecimal128(afterBlock)
	toDecimal := bigIntToDecimal128(toBlock)
	filter := bson.M{
		"index": bson.M{
			"$gt":  afterDecimal,
			"$lte": toDecimal,
		},
		"finalized": true,
	}
	opts := options.Find().
		SetSort(bson.M{"index": 1}).
		SetLimit(int64(limit))
	cursor, err := bs.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find finalized block page after %s: %w", afterBlock.String(), err)
	}
	defer cursor.Close(ctx)

	blocks := make([]*models.Block, 0, limit)
	for cursor.Next(ctx) {
		var blockBSON models.BlockBSON
		if err := cursor.Decode(&blockBSON); err != nil {
			return nil, fmt.Errorf("failed to decode finalized block page: %w", err)
		}
		block, err := blockBSON.FromBSON()
		if err != nil {
			return nil, fmt.Errorf("failed to convert finalized block page from BSON: %w", err)
		}
		blocks = append(blocks, block)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("finalized block page cursor error: %w", err)
	}
	return blocks, nil
}

// GetNextFinalizedAfter retrieves the first finalized block after afterBlock, up to toBlock.
func (bs *BlockStorage) GetNextFinalizedAfter(ctx context.Context, afterBlock, toBlock *api.BigInt) (*models.Block, error) {
	afterDecimal := bigIntToDecimal128(afterBlock)
	toDecimal := bigIntToDecimal128(toBlock)

	filter := bson.M{
		"index": bson.M{
			"$gt":  afterDecimal,
			"$lte": toDecimal,
		},
		"finalized": true,
	}
	opts := options.FindOne().SetSort(bson.M{"index": 1})

	var blockBSON models.BlockBSON
	err := bs.collection.FindOne(ctx, filter, opts).Decode(&blockBSON)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to find next finalized block after %s: %w", afterBlock.String(), err)
	}

	block, err := blockBSON.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to convert from BSON: %w", err)
	}
	return block, nil
}

// partitionViewIndexName names the partial index over the blocks which carry a partition view.
const partitionViewIndexName = "index_with_partition_view"

// PrunePartitionViews unsets the partition view of up to limit blocks whose number is below the
// given one, oldest first, and returns how many blocks it changed. Only the latest finalized block
// (and an in-flight finalizing candidate above it) needs its view, so the caller passes the number
// of a block which is durably finalized: the cutoff is a height, never "everything except my
// block", and a stale caller can only clear blocks which are older than the newest one anyway.
// The UC of a block is left as it is; a view is only ever cleared together with nothing else.
func (bs *BlockStorage) PrunePartitionViews(ctx context.Context, below *api.BigInt, limit int) (int, error) {
	if below == nil || limit <= 0 {
		return 0, errors.New("prune partition views: block number and a positive limit are required")
	}
	filter := bson.M{
		"index":         bson.M{"$lt": bigIntToDecimal128(below)},
		"partitionView": bson.M{"$exists": true},
	}
	cursor, err := bs.collection.Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "index", Value: 1}}).SetLimit(int64(limit)).
			SetProjection(bson.M{"index": 1}))
	if err != nil {
		return 0, fmt.Errorf("failed to find blocks with partition views: %w", err)
	}
	var found []struct {
		Index bson.RawValue `bson:"index"`
	}
	if err := cursor.All(ctx, &found); err != nil {
		return 0, fmt.Errorf("failed to read blocks with partition views: %w", err)
	}
	if len(found) == 0 {
		return 0, nil
	}
	ids := make(bson.A, 0, len(found))
	for _, f := range found {
		ids = append(ids, f.Index)
	}
	// the cutoff is part of the update as well: nothing at or above it is ever touched
	filter["index"] = bson.M{"$lt": bigIntToDecimal128(below), "$in": ids}
	res, err := bs.collection.UpdateMany(ctx, filter, bson.M{"$unset": bson.M{"partitionView": ""}})
	if err != nil {
		return 0, fmt.Errorf("failed to unset partition views: %w", err)
	}
	return int(res.ModifiedCount), nil
}

// PruneRecentPartitionViews unsets, in one indexed update, the partition view of the blocks in
// [below-window, below): the heights right behind a durably finalized block, where the views left
// by earlier finalizations live. The cutoff is a height as in PrunePartitionViews. What lies
// further back (crash leftovers, history of earlier versions) is the startup sweep's job.
func (bs *BlockStorage) PruneRecentPartitionViews(ctx context.Context, below *api.BigInt, window int) (int, error) {
	if below == nil || window <= 0 {
		return 0, errors.New("prune partition views: block number and a positive window are required")
	}
	index := bson.M{"$lt": bigIntToDecimal128(below)}
	if low := new(big.Int).Sub(below.Int, big.NewInt(int64(window))); low.Sign() > 0 {
		index["$gte"] = bigIntToDecimal128(api.NewBigInt(low))
	}
	res, err := bs.collection.UpdateMany(ctx,
		bson.M{"index": index, "partitionView": bson.M{"$exists": true}},
		bson.M{"$unset": bson.M{"partitionView": ""}})
	if err != nil {
		return 0, fmt.Errorf("failed to unset partition views: %w", err)
	}
	return int(res.ModifiedCount), nil
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
		{
			// holds only the few blocks which carry a partition view: finds the views to prune without
			// scanning the history
			Keys: bson.D{{Key: "index", Value: 1}},
			Options: options.Index().SetName(partitionViewIndexName).
				SetPartialFilterExpression(bson.M{"partitionView": bson.M{"$exists": true}}),
		},
	}

	_, err := bs.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create block indexes: %w", err)
	}

	return nil
}

// SetFinalized marks a block as finalized or unfinalized
func (bs *BlockStorage) SetFinalized(ctx context.Context, blockNumber *api.BigInt, finalized bool) error {
	indexDecimal := bigIntToDecimal128(blockNumber)
	filter := bson.M{"index": indexDecimal}
	status := models.FinalityStatusFinalizing
	if finalized {
		status = models.FinalityStatusFinalized
	}
	update := bson.M{"$set": bson.M{"finalized": finalized, "status": status}}

	result, err := bs.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to set block finalized status: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("block %s not found", blockNumber.String())
	}
	return nil
}

func (bs *BlockStorage) SetStatus(ctx context.Context, blockNumber *api.BigInt, status string) error {
	filter := bson.M{"index": bigIntToDecimal128(blockNumber)}
	update := bson.M{"status": status, "finalized": status == models.FinalityStatusFinalized}
	result, err := bs.collection.UpdateOne(ctx, filter, bson.M{"$set": update})
	if err != nil {
		return fmt.Errorf("failed to set block status: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("block %s not found", blockNumber.String())
	}
	return nil
}

// SetFinalizingWithCertificate marks a certified block as finalizing and
// persists the UC before non-disk SMT node writes begin.
func (bs *BlockStorage) SetFinalizingWithCertificate(ctx context.Context, block *models.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}
	filter := bson.M{"index": bigIntToDecimal128(block.Index)}
	update := certificateUpdate(block, bson.M{
		"finalized": false,
		"status":    models.FinalityStatusFinalizing,
	})

	result, err := bs.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to set block as finalizing with certificate: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("block %s not found", block.Index.String())
	}
	return nil
}

// SetFinalizedWithCertificate marks a block finalized and persists the UC that
// may have arrived after the proposed block was first stored.
func (bs *BlockStorage) SetFinalizedWithCertificate(ctx context.Context, block *models.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}
	indexDecimal := bigIntToDecimal128(block.Index)
	filter := bson.M{"index": indexDecimal}
	update := certificateUpdate(block, bson.M{
		"finalized": true,
		"status":    models.FinalityStatusFinalized,
	})

	result, err := bs.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to set block as finalized with certificate: %w", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("block %s not found", block.Index.String())
	}
	return nil
}

// GetUnfinalized returns all unfinalized blocks (should be at most 1 in normal operation)
func (bs *BlockStorage) GetUnfinalized(ctx context.Context) ([]*models.Block, error) {
	filter := finalizingBlockFilter()
	filter["finalized"] = false
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

// certificateUpdate builds the update which persists the block's UC together with its
// optional partition view. The view is always written or cleared in the same update as
// the UC, so a stored view can only belong to the stored UC bytes.
func certificateUpdate(block *models.Block, set bson.M) bson.M {
	set["unicityCertificate"] = block.UnicityCertificate.String()
	update := bson.M{}
	if len(block.PartitionView) > 0 {
		set["partitionView"] = block.PartitionView.String()
	} else {
		update["$unset"] = bson.M{"partitionView": ""}
	}
	update["$set"] = set
	return update
}
