package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

// Storage implements the complete storage interface for MongoDB
type Storage struct {
	client   *mongo.Client
	database *mongo.Database
	config   *config.DatabaseConfig

	// Storage implementations
	commitmentStorage       *CommitmentStorage
	aggregatorRecordStorage *AggregatorRecordStorage
	blockStorage            *BlockStorage
	smtStorage              *SmtStorage
	blockRecordsStorage     *BlockRecordsStorage
	leadershipStorage       *LeadershipStorage
	cachedTrustBaseStorage  *CachedTrustBaseStorage
}

// NewStorage creates a new MongoDB storage instance
func NewStorage(ctx context.Context, config config.Config) (*Storage, error) {
	cfg := config.Database
	// Create client options
	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetConnectTimeout(cfg.ConnectTimeout).
		SetServerSelectionTimeout(cfg.ServerSelectionTimeout).
		SetSocketTimeout(cfg.SocketTimeout).
		SetMaxPoolSize(cfg.MaxPoolSize).
		SetMinPoolSize(cfg.MinPoolSize).
		SetMaxConnIdleTime(cfg.MaxConnIdleTime)

	// Use write concern W1 (primary acknowledged) for lower latency
	clientOpts.SetWriteConcern(writeconcern.W1())

	// Connect to MongoDB
	connectCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()

	client, err := mongo.Connect(connectCtx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping to verify connection
	if err := client.Ping(connectCtx, readpref.Primary()); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	database := client.Database(cfg.Database)

	storage := &Storage{
		client:   client,
		database: database,
		config:   &cfg,
	}

	// Initialize storage implementations
	storage.commitmentStorage = NewCommitmentStorage(database)
	storage.aggregatorRecordStorage = NewAggregatorRecordStorage(database)
	storage.blockStorage = NewBlockStorage(database)
	storage.smtStorage = NewSmtStorage(database)
	storage.blockRecordsStorage = NewBlockRecordsStorage(database)
	storage.leadershipStorage = NewLeadershipStorage(database, config.HA.LockTTLSeconds)
	storage.cachedTrustBaseStorage = NewCachedTrustBaseStorage(NewTrustBaseStorage(database))

	// init trust base store cache
	if err := storage.cachedTrustBaseStorage.ReloadCache(ctx); err != nil {
		return nil, fmt.Errorf("failed to init cached trust base storage cache: %w", err)
	}

	// Create indexes
	if err := storage.createIndexes(ctx); err != nil {
		return nil, fmt.Errorf("failed to create indexes: %w", err)
	}

	return storage, nil
}

func (s *Storage) Initialize(ctx context.Context) error {
	return nil
}

// CommitmentQueue returns the commitment queue implementation
func (s *Storage) CommitmentQueue() interfaces.CommitmentQueue {
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

// TrustBaseStorage returns the trust base storage implementation
func (s *Storage) TrustBaseStorage() interfaces.TrustBaseStorage {
	return s.cachedTrustBaseStorage
}

// Ping verifies the database connection
func (s *Storage) Ping(ctx context.Context) error {
	return s.client.Ping(ctx, readpref.Primary())
}

// Close closes the database connection
func (s *Storage) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

// CleanAllCollections drops all collections in the database (useful for testing)
func (s *Storage) CleanAllCollections(ctx context.Context) error {
	collections, err := s.database.ListCollectionNames(ctx, map[string]interface{}{})
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	for _, collName := range collections {
		if err := s.database.Collection(collName).Drop(ctx); err != nil {
			return fmt.Errorf("failed to drop collection %s: %w", collName, err)
		}
	}

	return nil
}

// MongoDB transaction error labels
const (
	labelTransientTransaction     = "TransientTransactionError"
	labelUnknownTransactionCommit = "UnknownTransactionCommitResult"
)

// WithTransaction executes a function within a MongoDB transaction with limited retries.
// Only retries on transient errors (TransientTransactionError label).
func (s *Storage) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	const maxRetries = 3

	session, err := s.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = session.StartTransaction()
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}

		err = mongo.WithSession(ctx, session, func(sessCtx mongo.SessionContext) error {
			return fn(sessCtx)
		})

		if err != nil {
			// Abort the transaction on error
			_ = session.AbortTransaction(ctx)

			// Check if this is a transient error that we should retry
			if cmdErr, ok := err.(mongo.CommandError); ok {
				if cmdErr.HasErrorLabel(labelTransientTransaction) && attempt < maxRetries {
					lastErr = err
					continue // Retry
				}
			}
			// Non-transient error or max retries reached
			return fmt.Errorf("transaction failed (attempt %d/%d): %w", attempt, maxRetries, err)
		}

		// Try to commit with retry for unknown commit result
		for commitAttempt := 1; commitAttempt <= maxRetries; commitAttempt++ {
			err = session.CommitTransaction(ctx)
			if err == nil {
				return nil // Success
			}

			cmdErr, ok := err.(mongo.CommandError)
			if !ok {
				return fmt.Errorf("transaction commit failed (attempt %d/%d): %w", attempt, maxRetries, err)
			}

			// TransientTransactionError on commit - retry whole transaction
			if cmdErr.HasErrorLabel(labelTransientTransaction) && attempt < maxRetries {
				lastErr = err
				break // Break inner loop, continue outer loop to retry whole transaction
			}

			// UnknownTransactionCommitResult - retry just the commit
			if cmdErr.HasErrorLabel(labelUnknownTransactionCommit) && commitAttempt < maxRetries {
				lastErr = err
				continue // Retry commit
			}

			return fmt.Errorf("transaction commit failed (attempt %d/%d, commit %d/%d): %w",
				attempt, maxRetries, commitAttempt, maxRetries, err)
		}

		// If we broke out of commit loop due to transient error, continue to retry whole transaction
		if lastErr != nil {
			continue
		}

		// Success
		return nil
	}

	// Should not reach here, but just in case
	return fmt.Errorf("transaction failed after %d retries: %w", maxRetries, lastErr)
}

// createIndexes creates all necessary database indexes
func (s *Storage) createIndexes(ctx context.Context) error {
	// Create indexes for each collection
	if err := s.commitmentStorage.CreateIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create certification request indexes: %w", err)
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

	if err := s.cachedTrustBaseStorage.storage.CreateIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create leadership indexes: %w", err)
	}

	return nil
}
