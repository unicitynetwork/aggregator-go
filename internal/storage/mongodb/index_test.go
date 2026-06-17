package mongodb

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoIndexSpec struct {
	Name   string `bson:"name"`
	Key    bson.D `bson:"key"`
	Unique bool   `bson:"unique,omitempty"`
}

func listIndexSpecsByName(t *testing.T, ctx context.Context, collection *mongo.Collection) map[string]mongoIndexSpec {
	t.Helper()

	cursor, err := collection.Indexes().List(ctx)
	require.NoError(t, err)
	defer cursor.Close(ctx)

	indexes := make(map[string]mongoIndexSpec)
	for cursor.Next(ctx) {
		var index mongoIndexSpec
		require.NoError(t, cursor.Decode(&index))
		indexes[index.Name] = index
	}
	require.NoError(t, cursor.Err())

	return indexes
}

func requireIndexNames(t *testing.T, indexes map[string]mongoIndexSpec, expected ...string) {
	t.Helper()

	actual := make([]string, 0, len(indexes))
	for name := range indexes {
		actual = append(actual, name)
	}
	assert.ElementsMatch(t, expected, actual)
}

func requireIndexKey(t *testing.T, index mongoIndexSpec, expected bson.D) {
	t.Helper()

	require.Len(t, index.Key, len(expected))
	for i := range expected {
		assert.Equal(t, expected[i].Key, index.Key[i].Key)
		assert.Equal(t, fmt.Sprint(expected[i].Value), fmt.Sprint(index.Key[i].Value))
	}
}

func TestAggregatorRecordStorage_CreateIndexes_ProductionIndexSet(t *testing.T) {
	db := setupTestDB(t)
	storage := NewAggregatorRecordStorage(db)
	ctx := context.Background()

	require.NoError(t, storage.CreateIndexes(ctx))

	indexes := listIndexSpecsByName(t, ctx, storage.collection)
	requireIndexNames(t, indexes, "_id_", "stateId_hashed", "blockNumber_1_proposalId_1")
	requireIndexKey(t, indexes["stateId_hashed"], bson.D{{Key: "stateId", Value: "hashed"}})
	requireIndexKey(t, indexes["blockNumber_1_proposalId_1"], bson.D{{Key: "blockNumber", Value: 1}, {Key: "proposalId", Value: 1}})
	assert.False(t, indexes["stateId_hashed"].Unique, "duplicate filtering is handled by the SMT apply path")
	assert.False(t, indexes["blockNumber_1_proposalId_1"].Unique, "multiple records belong to the same proposal")
}

func TestAggregatorRecordStorage_CreateIndexes_RejectsLegacyUniqueStateIDIndex(t *testing.T) {
	db := setupTestDB(t)
	storage := NewAggregatorRecordStorage(db)
	ctx := context.Background()

	// Simulate an upgraded-in-place database that still carries the pre-proposalId
	// unique stateId index. The new model re-stages the same stateId across
	// proposal attempts, so this index would silently drop records.
	_, err := storage.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "stateId", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	require.NoError(t, err)

	err = storage.CreateIndexes(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "legacy unique index")

	// The check must bail before creating the new indexes, so a legacy DB is not
	// left half-migrated.
	indexes := listIndexSpecsByName(t, ctx, storage.collection)
	require.NotContains(t, indexes, "stateId_hashed")
}

func TestSmtStorage_CreateIndexes_ProductionIndexSet(t *testing.T) {
	db := setupTestDB(t)
	storage := NewSmtStorage(db)
	ctx := context.Background()

	require.NoError(t, storage.CreateIndexes(ctx))

	indexes := listIndexSpecsByName(t, ctx, storage.collection)
	requireIndexNames(t, indexes, "_id_", "key_1")
	requireIndexKey(t, indexes["key_1"], bson.D{{Key: "key", Value: 1}})
	assert.True(t, indexes["key_1"].Unique, "SMT node keys must stay unique")
}
