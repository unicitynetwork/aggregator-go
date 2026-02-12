package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	modelsV1 "github.com/unicitynetwork/aggregator-go/internal/models/v1"
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

// GetByStateID retrieves an aggregator record by state ID
func (ars *AggregatorRecordStorage) GetByStateID(ctx context.Context, stateID api.StateID) (*models.AggregatorRecord, error) {
	var raw bson.Raw
	filter := bson.M{"requestId": stateID.String()}
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
	case 0: // data stored before commitment-v2
		var bsonV1 modelsV1.AggregatorRecordV1BSON
		if err := bson.Unmarshal(raw, &bsonV1); err != nil {
			return nil, fmt.Errorf("failed to unmarshal v1 aggregator record: %w", err)
		}
		aggregatorRecordV1, err := bsonV1.FromBSON()
		if err != nil {
			return nil, fmt.Errorf("failed to convert aggregator record v1 bson to domain: %w", err)
		}
		return aggregatorRecordFromV1(aggregatorRecordV1)
	case 1, 2:
		// version 1 - data stored after commitment-v2 through v1 api
		// version 2 - data stored after commitment-v2 through v2 api
		var v2 models.AggregatorRecordBSON
		if err := bson.Unmarshal(raw, &v2); err != nil {
			return nil, fmt.Errorf("failed to unmarshal v2 aggregator record: %w", err)
		}
		return v2.FromBSON()
	default:
		return nil, fmt.Errorf("unsupported aggregator record version: %d", probe.Version)
	}
}

func aggregatorRecordFromV1(v1 *modelsV1.AggregatorRecordV1) (*models.AggregatorRecord, error) {
	return &models.AggregatorRecord{
		Version: 1,
		StateID: v1.RequestID,
		CertificationData: models.CertificationData{
			OwnerPredicate:  api.NewPayToPublicKeyPredicate(v1.Authenticator.PublicKey),
			SourceStateHash: v1.Authenticator.StateHash,
			TransactionHash: v1.TransactionHash,
			Witness:         v1.Authenticator.Signature,
		},
		AggregateRequestCount: v1.AggregateRequestCount,
		BlockNumber:           v1.BlockNumber,
		LeafIndex:             v1.LeafIndex,
		CreatedAt:             v1.CreatedAt,
		FinalizedAt:           v1.FinalizedAt,
	}, nil
}

// Count returns the total number of records
func (ars *AggregatorRecordStorage) Count(ctx context.Context) (int64, error) {
	count, err := ars.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("failed to count aggregator records: %w", err)
	}
	return count, nil
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
