package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

// Storage implements the complete storage interface for MongoDB
type Storage struct {
	client     *mongo.Client
	database   *mongo.Database
	config     *config.DatabaseConfig
	
	// Storage implementations
	commitmentStorage       *CommitmentStorage
	aggregatorRecordStorage *AggregatorRecordStorage
	blockStorage           *BlockStorage
	smtStorage             *SmtStorage
	blockRecordsStorage    *BlockRecordsStorage
	leadershipStorage      *LeadershipStorage
}

// NewStorage creates a new MongoDB storage instance
func NewStorage(cfg *config.DatabaseConfig) (*Storage, error) {
	// Create client options
	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetServerSelectionTimeout(cfg.ServerSelectionTimeout).
		SetSocketTimeout(cfg.SocketTimeout).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetMinPoolSize(cfg.MinPoolSize).
		SetMaxConnIdleTime(cfg.MaxConnIdleTime)

	// Create client
	client, err := mongo.NewClient(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create MongoDB client: %w", err)
	}

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping to verify connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	database := client.Database(cfg.Database)

	storage := &Storage{
		client:   client,
		database: database,
		config:   cfg,
	}

	// Initialize storage implementations
	storage.commitmentStorage = NewCommitmentStorage(database)
	storage.aggregatorRecordStorage = NewAggregatorRecordStorage(database)
	storage.blockStorage = NewBlockStorage(database)
	storage.smtStorage = NewSmtStorage(database)
	storage.blockRecordsStorage = NewBlockRecordsStorage(database)
	storage.leadershipStorage = NewLeadershipStorage(database)

	// Create indexes
	if err := storage.createIndexes(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to create indexes: %w", err)
	}

	return storage, nil
}

// CommitmentStorage returns the commitment storage implementation
func (s *Storage) CommitmentStorage() interfaces.CommitmentStorage {
	return s.commitmentStorage
}

// AggregatorRecordStorage returns the aggregator record storage implementation
func (s *Storage) AggregatorRecordStorage() interfaces.AggregatorRecordStorage {
	return s.aggregatorRecordStorage
}

// BlockStorage returns the block storage implementation
func (s *Storage) BlockStorage() interfaces.BlockStorage {
	return s.blockStorage
}

// SmtStorage returns the SMT storage implementation
func (s *Storage) SmtStorage() interfaces.SmtStorage {
	return s.smtStorage
}

// BlockRecordsStorage returns the block records storage implementation
func (s *Storage) BlockRecordsStorage() interfaces.BlockRecordsStorage {
	return s.blockRecordsStorage
}

// LeadershipStorage returns the leadership storage implementation
func (s *Storage) LeadershipStorage() interfaces.LeadershipStorage {
	return s.leadershipStorage
}

// Ping verifies the database connection
func (s *Storage) Ping(ctx context.Context) error {
	return s.client.Ping(ctx, readpref.Primary())
}

// Close closes the database connection
func (s *Storage) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

// WithTransaction executes a function within a MongoDB transaction
func (s *Storage) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	session, err := s.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		return nil, fn(sessCtx)
	}

	_, err = session.WithTransaction(ctx, callback)
	return err
}

// createIndexes creates all necessary database indexes
func (s *Storage) createIndexes(ctx context.Context) error {
	// Create indexes for each collection
	if err := s.commitmentStorage.CreateIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create commitment indexes: %w", err)
	}

	if err := s.aggregatorRecordStorage.CreateIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create aggregator record indexes: %w", err)
	}

	if err := s.blockStorage.CreateIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create block indexes: %w", err)
	}

	if err := s.smtStorage.CreateIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create SMT indexes: %w", err)
	}

	if err := s.blockRecordsStorage.CreateIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create block records indexes: %w", err)
	}

	if err := s.leadershipStorage.CreateIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create leadership indexes: %w", err)
	}

	return nil
}