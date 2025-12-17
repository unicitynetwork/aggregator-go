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

const aggregatorRecordCollection = "aggregator_records"

// AggregatorRecordStorage implements aggregator record storage for MongoDB
type AggregatorRecordStorage struct {
	collection *mongo.Collection
}

// NewAggregatorRecordStorage creates a new aggregator record storage instance
func NewAggregatorRecordStorage(db *mongo.Database) *AggregatorRecordStorage {
	return &AggregatorRecordStorage{
		collection: db.Collection(aggregatorRecordCollection),
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

	_, err := ars.collection.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil {
		// Ignore duplicate key errors - with SetOrdered(false), non-duplicates are still inserted
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return fmt.Errorf("failed to store aggregator records batch: %w", err)
	}
	return nil
}

// GetExistingRequestIDs returns which of the given request IDs already exist in the database.
// Used to filter duplicates before inserting.
func (ars *AggregatorRecordStorage) GetExistingRequestIDs(ctx context.Context, requestIDs []string) (map[string]bool, error) {
	if len(requestIDs) == 0 {
		return make(map[string]bool), nil
	}

	cursor, err := ars.collection.Find(ctx, bson.M{"requestId": bson.M{"$in": requestIDs}},
		options.Find().SetProjection(bson.M{"requestId": 1}))
	if err != nil {
		return nil, fmt.Errorf("failed to query existing request IDs: %w", err)
	}
	defer cursor.Close(ctx)

	existing := make(map[string]bool)
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err == nil {
			if reqID, ok := doc["requestId"].(string); ok {
				existing[reqID] = true
			}
		}
	}
	return existing, nil
}

// GetByRequestID retrieves an aggregator record by request ID
func (ars *AggregatorRecordStorage) GetByRequestID(ctx context.Context, requestID api.RequestID) (*models.AggregatorRecord, error) {
	var recordBSON models.AggregatorRecordBSON
	err := ars.collection.FindOne(ctx, bson.M{"requestId": string(requestID)}).Decode(&recordBSON)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get aggregator record by request ID: %w", err)
	}

	record, err := recordBSON.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to convert from BSON: %w", err)
	}
	return record, nil
}

// GetByBlockNumber retrieves all records for a specific block
func (ars *AggregatorRecordStorage) GetByBlockNumber(ctx context.Context, blockNumber *api.BigInt) ([]*models.AggregatorRecord, error) {
	filter := bson.M{"blockNumber": bigIntToDecimal128(blockNumber)}
	cursor, err := ars.collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to find records by block number: %w", err)
	}
	defer cursor.Close(ctx)

	records := make([]*models.AggregatorRecord, 0)
	for cursor.Next(ctx) {
		var recordBSON models.AggregatorRecordBSON
		if err := cursor.Decode(&recordBSON); err != nil {
			return nil, fmt.Errorf("failed to decode aggregator record: %w", err)
		}
		record, err := recordBSON.FromBSON()
		if err != nil {
			return nil, fmt.Errorf("failed to convert from BSON: %w", err)
		}
		records = append(records, record)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return records, nil
}

// Count returns the total number of records
func (ars *AggregatorRecordStorage) Count(ctx context.Context) (int64, error) {
	count, err := ars.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("failed to count aggregator records: %w", err)
	}
	return count, nil
}

// GetLatest retrieves the most recent records
func (ars *AggregatorRecordStorage) GetLatest(ctx context.Context, limit int) ([]*models.AggregatorRecord, error) {
	opts := options.Find().
		SetLimit(int64(limit)).
		SetSort(bson.M{"finalizedAt": -1}) // Most recent first

	cursor, err := ars.collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find latest records: %w", err)
	}
	defer cursor.Close(ctx)

	var records []*models.AggregatorRecord
	for cursor.Next(ctx) {
		var recordBSON models.AggregatorRecordBSON
		if err := cursor.Decode(&recordBSON); err != nil {
			return nil, fmt.Errorf("failed to decode aggregator record: %w", err)
		}

		record, err := recordBSON.FromBSON()
		if err != nil {
			return nil, fmt.Errorf("failed to convert from BSON: %w", err)
		}
		records = append(records, record)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return records, nil
}

// CreateIndexes creates necessary indexes for the aggregator record collection
func (ars *AggregatorRecordStorage) CreateIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "requestId", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "blockNumber", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "leafIndex", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "finalizedAt", Value: -1}},
		},
		{
			Keys: bson.D{
				{Key: "blockNumber", Value: 1},
				{Key: "leafIndex", Value: 1},
			},
		},
	}

	_, err := ars.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create aggregator record indexes: %w", err)
	}

	return nil
}
