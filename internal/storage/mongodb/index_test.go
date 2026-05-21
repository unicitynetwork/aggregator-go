package mongodb

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
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
	requireIndexNames(t, indexes, "_id_", "stateId_1", "blockNumber_1")
	requireIndexKey(t, indexes["stateId_1"], bson.D{{Key: "stateId", Value: 1}})
	requireIndexKey(t, indexes["blockNumber_1"], bson.D{{Key: "blockNumber", Value: 1}})
	assert.True(t, indexes["stateId_1"].Unique, "stateId must stay unique for proof lookup and duplicate protection")
	assert.False(t, indexes["blockNumber_1"].Unique, "multiple records belong to the same block")
}

func TestBlockRecordsStorage_CreateIndexes_ProductionIndexSet(t *testing.T) {
	db := setupTestDB(t)
	storage := NewBlockRecordsStorage(db)
	ctx := context.Background()

	require.NoError(t, storage.CreateIndexes(ctx))

	indexes := listIndexSpecsByName(t, ctx, storage.collection)
	requireIndexNames(t, indexes, "_id_", "blockNumber_1")
	requireIndexKey(t, indexes["blockNumber_1"], bson.D{{Key: "blockNumber", Value: 1}})
	assert.True(t, indexes["blockNumber_1"].Unique, "one block_records document is stored per block")
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
