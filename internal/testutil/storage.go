package testutil

import (
	"context"
	"testing"
	"time"

	mongoContainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
)

// SetupTestStorage creates a complete storage instance with MongoDB using testcontainers
func SetupTestStorage(t *testing.T, conf config.Config) *mongodb.Storage {
	ctx := context.Background()

	container, err := mongoContainer.Run(ctx, "mongo:7.0", mongoContainer.WithReplicaSet("rs0"))
	if err != nil {
		t.Skipf("Skipping MongoDB tests - cannot start MongoDB container (Docker not available?): %v", err)
		return nil
	}

	mongoURI, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get MongoDB connection string: %v", err)
	}
	mongoURI += "&directConnection=true"

	conf.Database.URI = mongoURI
	if conf.Database.ConnectTimeout == 0 {
		conf.Database.ConnectTimeout = 5 * time.Second
	}
	if conf.Database.Database == "" {
		conf.Database.Database = t.Name()
	}

	connectCtx, cancel := context.WithTimeout(ctx, conf.Database.ConnectTimeout)
	defer cancel()

	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	if err := client.Ping(connectCtx, nil); err != nil {
		t.Fatalf("Failed to ping MongoDB: %v", err)
	}
	storage, err := mongodb.NewStorage(ctx, conf)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), conf.Database.ConnectTimeout)
		defer cancel()

		if err := storage.Close(ctx); err != nil {
			t.Logf("Failed to close storage: %v", err)
		}
		if err := client.Disconnect(ctx); err != nil {
			t.Logf("Failed to disconnect MongoDB client: %v", err)
		}
		if err := container.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate MongoDB container: %v", err)
		}
	}
	t.Cleanup(cleanup)

	return storage
}
