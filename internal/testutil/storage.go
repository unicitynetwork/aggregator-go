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
func SetupTestStorage(t *testing.T, conf config.Config) (*mongodb.Storage, func()) {
	ctx := context.Background()

	container, err := mongoContainer.Run(ctx, "mongo:7.0")
	if err != nil {
		t.Skipf("Skipping MongoDB tests - cannot start MongoDB container (Docker not available?): %v", err)
		return nil, func() {}
	}

	mongoURI, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get MongoDB connection string: %v", err)
	}

	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	if err := client.Ping(connectCtx, nil); err != nil {
		t.Fatalf("Failed to ping MongoDB: %v", err)
	}
	conf.Database.URI = mongoURI
	storage, err := mongodb.NewStorage(conf)
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

	return storage, cleanup
}
