package mongodb

import (
	"context"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// The documented minimum is MongoDB 5.0; mainnet and testnet2 run 7.0.
var indexTestImages = []string{"mongo:7.0"}

func startMongo(t *testing.T, image string) (uri string, db *mongo.Database) {
	t.Helper()
	return startMongoWith(t, image, false)
}

// startMongoWith optionally starts a replica set (NewStorage requires one).
func startMongoWith(t *testing.T, image string, replicaSet bool) (uri string, db *mongo.Database) {
	t.Helper()
	ctx := context.Background()
	var opts []testcontainers.ContainerCustomizer
	if replicaSet {
		opts = append(opts, mongodb.WithReplicaSet("rs0"))
	}
	c, err := mongodb.Run(ctx, image, opts...)
	if err != nil {
		t.Skipf("cannot start %s: %v", image, err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(cctx)
	})
	uri, err = c.ConnectionString(ctx)
	require.NoError(t, err)
	if replicaSet {
		uri += "&directConnection=true"
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	require.NoError(t, client.Ping(ctx, nil))
	return uri, client.Database("test_pv_index")
}

// explainPrune explains the query shape of the startup sweep and returns the index names of the winning plan and
// the work done.
func explainPrune(t *testing.T, db *mongo.Database) (indexes []string, keys, docs int64) {
	t.Helper()
	var out bson.M
	err := db.RunCommand(context.Background(), bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: blockCollection},
			{Key: "filter", Value: bson.D{{Key: "partitionView", Value: bson.D{{Key: "$exists", Value: true}}}}},
			{Key: "sort", Value: bson.D{{Key: "index", Value: 1}}},
			{Key: "limit", Value: 64},
		}},
		{Key: "verbosity", Value: "executionStats"},
	}).Decode(&out)
	require.NoError(t, err)
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case bson.M:
			if n, ok := x["indexName"].(string); ok {
				indexes = append(indexes, n)
			}
			for _, c := range x {
				walk(c)
			}
		case bson.A:
			for _, c := range x {
				walk(c)
			}
		}
	}
	walk(out["queryPlanner"].(bson.M)["winningPlan"])
	stats := out["executionStats"].(bson.M)
	num := func(v any) int64 {
		switch n := v.(type) {
		case int32:
			return int64(n)
		case int64:
			return n
		}
		return -1
	}
	return indexes, num(stats["totalKeysExamined"]), num(stats["totalDocsExamined"])
}

func fillHistory(t *testing.T, bs *BlockStorage, n int, withViewUpTo int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		view := ""
		if i <= withViewUpTo {
			view = "v"
		}
		finalizeWithView(t, bs, viewBlock(int64(i), fmt.Sprintf("uc%d", i), view))
	}
}

// The startup indexes are created, the partition view index is built separately and the planner really uses it in
// the normal state (one block carries a view).
func TestPartitionViewIndex_FreshAndUpgradedDatabases(t *testing.T) {
	for _, image := range indexTestImages {
		t.Run(image, func(t *testing.T) {
			ctx := context.Background()
			_, db := startMongo(t, image)
			bs := NewBlockStorage(db)

			t.Run("fresh database", func(t *testing.T) {
				require.NoError(t, bs.CreateIndexes(ctx), "the fatal startup indexes on %s", image)
				require.NotContains(t, listIndexSpecsByName(t, ctx, bs.collection), partitionViewIndexName)
				require.NoError(t, bs.EnsurePartitionViewIndex(ctx))
				require.NoError(t, bs.EnsurePartitionViewIndex(ctx), "idempotent")
				specs := listIndexSpecsByName(t, ctx, bs.collection)
				require.Contains(t, specs, partitionViewIndexName)
				require.Equal(t, bson.D{{Key: "index", Value: int32(1)}}, specs[partitionViewIndexName].Key)
				require.True(t, specs["index_1"].Unique, "the unique index is untouched")

				// the planner picks the partial index in the normal state and touches nothing
				fillHistory(t, bs, 300, 0)
				finalizeWithView(t, bs, viewBlock(301, "uc301", "latest-view"))
				idx, keys, docs := explainPrune(t, db)
				require.Contains(t, idx, partitionViewIndexName, "winning plan: %v", idx)
				require.LessOrEqual(t, keys, int64(1))
				require.LessOrEqual(t, docs, int64(1))
			})

			t.Run("upgraded database written by the released version", func(t *testing.T) {
				require.NoError(t, bs.collection.Drop(ctx))
				require.NoError(t, bs.CreateIndexes(ctx))
				// every block of the old release carries a view; there is no partial index yet
				fillHistory(t, bs, 100, 100)
				require.NoError(t, bs.EnsurePartitionViewIndex(ctx))
				got, err := bs.PrunePartitionViews(ctx, api.NewBigInt(big.NewInt(100)), 1000)
				require.NoError(t, err)
				require.Equal(t, 99, got)
				require.Equal(t, api.HexBytes("v"), viewOf(t, bs, 100))
				require.Empty(t, viewOf(t, bs, 99))
			})
		})
	}
}

func storageConfig(uri string) config.Config {
	return config.Config{Database: config.DatabaseConfig{
		URI: uri, Database: "test_pv_storage",
		ConnectTimeout: 30 * time.Second, ServerSelectionTimeout: 10 * time.Second, SocketTimeout: 30 * time.Second,
		MaxPoolSize: 20, MinPoolSize: 1, MaxConnIdleTime: time.Minute,
	}}
}

// The index is built outside the fatal startup path: startup returns while the build has not even finished (here:
// it is held), the storage works, and a failing build is logged and retried until it succeeds.
func TestPartitionViewIndex_BuiltInTheBackgroundAndRetried(t *testing.T) {
	origAttempt, origDelays := ensurePartitionViewIndex, partitionViewIndexRetryDelays
	t.Cleanup(func() { ensurePartitionViewIndex, partitionViewIndexRetryDelays = origAttempt, origDelays })
	partitionViewIndexRetryDelays = []time.Duration{20 * time.Millisecond}

	for _, image := range indexTestImages {
		t.Run(image, func(t *testing.T) {
			ctx := context.Background()
			uri, _ := startMongoWith(t, image, true)
			var attempts atomic.Int32
			release := make(chan struct{})
			ensurePartitionViewIndex = func(ctx context.Context, bs *BlockStorage) error {
				n := attempts.Add(1)
				if n <= 2 {
					return fmt.Errorf("simulated build failure %d", n) // e.g. a deadline: never fatal
				}
				<-release // the (slow) build of a large collection
				return bs.EnsurePartitionViewIndex(ctx)
			}

			started := time.Now()
			st, err := NewStorage(ctx, storageConfig(uri))
			require.NoError(t, err, "a missing or failing performance index must not abort the startup")
			defer st.Close(ctx)
			require.Less(t, time.Since(started), 20*time.Second)

			// usable and correct without the index
			bs := st.blockStorage
			require.NotContains(t, listIndexSpecsByName(t, ctx, bs.collection), partitionViewIndexName)
			fillHistory(t, bs, 5, 5)
			got, err := bs.PruneRecentPartitionViews(ctx, api.NewBigInt(big.NewInt(5)), 100)
			require.NoError(t, err)
			require.Equal(t, 4, got)

			select {
			case <-st.indexBuildReady:
				t.Fatal("the build was held")
			case <-time.After(200 * time.Millisecond):
			}
			close(release)
			select {
			case <-st.indexBuildReady:
			case <-time.After(60 * time.Second):
				t.Fatal("the index was not built")
			}
			require.GreaterOrEqual(t, attempts.Load(), int32(3), "two failures were retried")
			require.Contains(t, listIndexSpecsByName(t, ctx, bs.collection), partitionViewIndexName)
		})
	}
}

// Closing the storage stops the retries.
func TestPartitionViewIndex_CloseStopsTheBuild(t *testing.T) {
	origAttempt, origDelays := ensurePartitionViewIndex, partitionViewIndexRetryDelays
	t.Cleanup(func() { ensurePartitionViewIndex, partitionViewIndexRetryDelays = origAttempt, origDelays })
	partitionViewIndexRetryDelays = []time.Duration{10 * time.Millisecond}
	var attempts atomic.Int32
	ensurePartitionViewIndex = func(context.Context, *BlockStorage) error {
		attempts.Add(1)
		return fmt.Errorf("always failing")
	}
	ctx := context.Background()
	uri, _ := startMongoWith(t, "mongo:7.0", true)
	st, err := NewStorage(ctx, storageConfig(uri))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return attempts.Load() >= 3 }, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, st.Close(ctx))
	time.Sleep(100 * time.Millisecond)
	n := attempts.Load()
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, n, attempts.Load(), "no attempts after Close")
}
