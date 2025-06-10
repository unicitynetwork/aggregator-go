package mongodb

import (
	"context"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
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
	recordBSON := record.ToBSON()
	_, err := ars.collection.InsertOne(ctx, recordBSON)
	if err != nil {
		return fmt.Errorf("failed to store aggregator record: %w", err)
	}
	return nil
}

// StoreBatch stores multiple aggregator records
func (ars *AggregatorRecordStorage) StoreBatch(ctx context.Context, records []*models.AggregatorRecord) error {
	if len(records) == 0 {
		return nil
	}

	docs := make([]interface{}, len(records))
	for i, record := range records {
		docs[i] = record.ToBSON()
	}

	_, err := ars.collection.InsertMany(ctx, docs)
	if err != nil {
		return fmt.Errorf("failed to store aggregator records batch: %w", err)
	}
	return nil
}

// GetByRequestID retrieves an aggregator record by request ID
func (ars *AggregatorRecordStorage) GetByRequestID(ctx context.Context, requestID api.RequestID) (*models.AggregatorRecord, error) {
	var recordBSON models.AggregatorRecordBSON
	err := ars.collection.FindOne(ctx, bson.M{"requestId": string(requestID)}).Decode(&recordBSON)
	if err != nil {
		if err == mongo.ErrNoDocuments {
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
	filter := bson.M{"blockNumber": blockNumber.String()}
	cursor, err := ars.collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to find records by block number: %w", err)
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
