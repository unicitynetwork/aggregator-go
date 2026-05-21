package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const aggregatorRecordCollection = "aggregator_records"

// AggregatorRecordStorage implements aggregator record storage for MongoDB
type AggregatorRecordStorage struct {
	collection *mongo.Collection
	insertOpts finalizationInsertOptions
}

// NewAggregatorRecordStorage creates a new aggregator record storage instance
func NewAggregatorRecordStorage(db *mongo.Database, insertOpts ...finalizationInsertOptions) *AggregatorRecordStorage {
	opts := finalizationInsertOptions{workers: 1}
	if len(insertOpts) > 0 {
		opts = insertOpts[0]
	}
	return &AggregatorRecordStorage{
		collection: db.Collection(aggregatorRecordCollection),
		insertOpts: opts,
	}
}

// Store stores a new aggregator record
func (ars *AggregatorRecordStorage) Store(ctx context.Context, record *models.AggregatorRecord) error {
	recordBSON, err := record.ToBSON()
	if err != nil {
		return fmt.Errorf("failed to marshal aggregator record to BSON: %w", err)
	}
	if _, err := ars.collection.InsertOne(ctx, recordBSON); err != nil {
		return fmt.Errorf("failed to store aggregator record: %w", err)
	}
	return nil
}

// StoreBatch stores multiple aggregator records using InsertMany.
// Duplicate key errors are ignored (duplicates are simply skipped).
func (ars *AggregatorRecordStorage) StoreBatch(ctx context.Context, records []*models.AggregatorRecord) error {
	if len(records) == 0 {
		return nil
	}

	docs := make([]interface{}, len(records))
	for i, record := range records {
		recordBSON, err := record.ToBSON()
		if err != nil {
			return fmt.Errorf("failed to marshal aggregator record to BSON: %w", err)
		}
		docs[i] = recordBSON
	}

	err := insertManyFinalizationBatch(ctx, ars.collection, docs, ars.insertOpts)
	if err != nil {
		return fmt.Errorf("failed to store aggregator records batch: %w", err)
	}
	return nil
}

// GetExistingStateIDs returns which of the given state IDs already exist in the database.
// Used to filter duplicates before inserting.
func (ars *AggregatorRecordStorage) GetExistingStateIDs(ctx context.Context, stateIDs []string) (map[string]bool, error) {
	if len(stateIDs) == 0 {
		return make(map[string]bool), nil
	}

	cursor, err := ars.collection.Find(ctx, bson.M{"stateId": bson.M{"$in": stateIDs}},
		options.Find().SetProjection(bson.M{"stateId": 1}))
	if err != nil {
		return nil, fmt.Errorf("failed to query existing state IDs: %w", err)
	}
	defer cursor.Close(ctx)

	existing := make(map[string]bool)
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err == nil {
			if stateID, ok := doc["stateId"].(string); ok {
				existing[stateID] = true
			}
		}
	}
	return existing, nil
}

// GetByStateID retrieves an aggregator record by state ID
func (ars *AggregatorRecordStorage) GetByStateID(ctx context.Context, stateID api.StateID) (*models.AggregatorRecord, error) {
	var raw bson.Raw
	filter := bson.M{"stateId": stateID.String()}
	if err := ars.collection.FindOne(ctx, filter).Decode(&raw); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get aggregator record by state ID: %w", err)
	}
	return ars.decodeRecord(raw)
}

// GetByBlockNumber retrieves all records for a specific block
func (ars *AggregatorRecordStorage) GetByBlockNumber(ctx context.Context, blockNumber *api.BigInt) ([]*models.AggregatorRecord, error) {
	filter := bson.M{"blockNumber": bigIntToDecimal128(blockNumber)}
	cursor, err := ars.collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to find records by block number: %w", err)
	}
	defer cursor.Close(ctx)

	var records []*models.AggregatorRecord
	for cursor.Next(ctx) {
		var raw bson.Raw
		if err := cursor.Decode(&raw); err != nil {
			return nil, fmt.Errorf("failed to decode aggregator record: %w", err)
		}
		record, err := ars.decodeRecord(raw)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	// Preserve get_block_records ordering after dropping the write-heavy
	// {blockNumber, leafIndex} Mongo index.
	sort.SliceStable(records, func(i, j int) bool {
		left := records[i].LeafIndex
		right := records[j].LeafIndex
		if left == nil || left.Int == nil {
			return right != nil && right.Int != nil
		}
		if right == nil || right.Int == nil {
			return false
		}
		return left.Int.Cmp(right.Int) < 0
	})

	return records, nil
}

type versionProbe struct {
	Version uint32 `bson:"version"`
}

func (ars *AggregatorRecordStorage) decodeRecord(raw bson.Raw) (*models.AggregatorRecord, error) {
	var probe versionProbe

	if err := bson.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("failed to probe record version: %w", err)
	}

	switch probe.Version {
	case 2:
		var v2 models.AggregatorRecordBSON
		if err := bson.Unmarshal(raw, &v2); err != nil {
			return nil, fmt.Errorf("failed to unmarshal aggregator record: %w", err)
		}
		return v2.FromBSON()
	default:
		return nil, fmt.Errorf("unsupported aggregator record version: %d", probe.Version)
	}
}

// Count returns the total number of records
func (ars *AggregatorRecordStorage) Count(ctx context.Context) (int64, error) {
	count, err := ars.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("failed to count aggregator records: %w", err)
	}
	return count, nil
}

// CreateIndexes creates the necessary indexes needed by the submit, proof, and
// block-record lookup paths.
func (ars *AggregatorRecordStorage) CreateIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "stateId", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "blockNumber", Value: 1}},
		},
	}

	_, err := ars.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create aggregator record indexes: %w", err)
	}

	return nil
}
