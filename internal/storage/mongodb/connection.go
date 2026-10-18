package mongodb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
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
	leadershipStorage       *LeadershipStorage
	cachedTrustBaseStorage  *CachedTrustBaseStorage

	// background build of the partition view index (see startPartitionViewIndexBuild)
	indexBuildCancel context.CancelFunc
	indexBuildReady  chan struct{}
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

	clientOpts.SetWriteConcern(mongoWriteConcern(cfg))

	// Connect to MongoDB
	connectCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()

	client, err := mongo.Connect(connectCtx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping to verify connection
	if err := client.Ping(connectCtx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	// Verify MongoDB is running as a replica set (required for transactions)
	if err := verifyReplicaSet(connectCtx, client); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	if err := waitForWritablePrimary(connectCtx, client); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}

	database := client.Database(cfg.Database)

	storage := &Storage{
		client:   client,
		database: database,
		config:   &cfg,
	}
	finalizationInsertOpts := finalizationInsertOptions{
		chunkSize: cfg.FinalizationInsertChunkSize,
		workers:   cfg.FinalizationInsertChunkWorkers,
	}

	// Initialize storage implementations
	storage.commitmentStorage = NewCommitmentStorage(database)
	storage.aggregatorRecordStorage = NewAggregatorRecordStorage(database, finalizationInsertOpts)
	storage.blockStorage = NewBlockStorage(database)
	storage.smtStorage = NewSmtStorage(database, finalizationInsertOpts)
	storage.leadershipStorage = NewLeadershipStorage(database, config.HA.LockTTLSeconds)
	storage.cachedTrustBaseStorage = NewCachedTrustBaseStorage(NewTrustBaseStorage(database))

	// init trust base store cache
	if err := storage.cachedTrustBaseStorage.ReloadCache(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("failed to init cached trust base storage cache: %w", err)
	}

	// Create indexes
	indexCtx, indexCancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer indexCancel()
	if err := retryDuringPrimaryTransition(indexCtx, client, storage.createIndexes); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("failed to create indexes: %w", err)
	}
	storage.startPartitionViewIndexBuild()

	return storage, nil
}

// Timing of the background build of the partition view index. Variables so that tests can shorten them.
var (
	// partitionViewIndexBuildTimeout bounds one attempt. It is deliberately far above the startup budget
	// (MONGODB_CONNECT_TIMEOUT): on a mainnet sized collection the build is a full scan.
	partitionViewIndexBuildTimeout = 30 * time.Minute
	// partitionViewIndexRetryDelays are the waits after a failed attempt; the last one repeats.
	partitionViewIndexRetryDelays = []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute}
	// ensurePartitionViewIndex is the attempt itself.
	ensurePartitionViewIndex = func(ctx context.Context, bs *BlockStorage) error { return bs.EnsurePartitionViewIndex(ctx) }
)

// startPartitionViewIndexBuild builds the partition view index outside the fatal startup path. The index only speeds
// up the pruning of superseded partition views, which is correct without it, so a slow or failed build never keeps
// the aggregator from starting: it runs in the background with its own context (not the startup budget), is logged
// and retried until it succeeds or the storage is closed. A restart during the build simply starts it again (the
// server may even have finished it meanwhile, then the attempt is a no-op).
func (s *Storage) startPartitionViewIndexBuild() {
	ctx, cancel := context.WithCancel(context.Background())
	s.indexBuildCancel = cancel
	s.indexBuildReady = make(chan struct{})
	go func() {
		started := time.Now()
		for attempt := 0; ; attempt++ {
			attemptCtx, attemptCancel := context.WithTimeout(ctx, partitionViewIndexBuildTimeout)
			err := ensurePartitionViewIndex(attemptCtx, s.blockStorage)
			attemptCancel()
			if err == nil {
				slog.Info("Partition view index is ready", "index", partitionViewIndexName, "attempts", attempt+1, "elapsed", time.Since(started).String())
				close(s.indexBuildReady)
				return
			}
			if ctx.Err() != nil {
				return
			}
			delay := partitionViewIndexRetryDelays[min(attempt, len(partitionViewIndexRetryDelays)-1)]
			slog.Warn("Building the partition view index failed; pruning works without it and the build is retried",
				"index", partitionViewIndexName, "attempt", attempt+1, "retryIn", delay.String(), "error", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
	}()
}

func mongoWriteConcern(cfg config.DatabaseConfig) *writeconcern.WriteConcern {
	writeConcern := strings.TrimSpace(cfg.WriteConcern)
	var w any = "majority"
	if writeConcern == "1" {
		w = 1
	} else if strings.EqualFold(writeConcern, "majority") {
		w = "majority"
	}
	journaled := cfg.WriteJournal || writeConcern == ""
	if journaled {
		journal := true
		return &writeconcern.WriteConcern{W: w, Journal: &journal}
	}
	return &writeconcern.WriteConcern{W: w}
}

func (s *Storage) Initialize(ctx context.Context) error {
	return nil
}

// verifyReplicaSet checks if MongoDB is running as a replica set.
// This is required for the aggregator to function correctly (transactions, change streams, etc.).
// Uses the unprivileged 'hello' command which doesn't require admin access.
func verifyReplicaSet(ctx context.Context, client *mongo.Client) error {
	var result bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&result); err != nil {
		return fmt.Errorf("failed to run hello command: %w", err)
	}

	// Check if 'setName' field exists - this indicates a replica set
	if _, ok := result["setName"]; !ok {
		return fmt.Errorf("MongoDB is not running as a replica set. " +
			"The aggregator requires MongoDB to be configured as a replica set for transactions and change streams. " +
			"Please start MongoDB with --replSet flag and initialize the replica set")
	}

	return nil
}

func waitForWritablePrimary(ctx context.Context, client *mongo.Client) error {
	const initialDelay = 25 * time.Millisecond
	delay := initialDelay
	var lastErr error

	for {
		var result bson.M
		err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&result)
		if err == nil && (boolResultField(result, "isWritablePrimary") || boolResultField(result, "ismaster")) {
			return nil
		}
		if err != nil {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("timed out waiting for MongoDB writable primary: %w", lastErr)
			}
			return fmt.Errorf("timed out waiting for MongoDB writable primary: %w", ctx.Err())
		case <-time.After(delay):
		}
		if delay < 250*time.Millisecond {
			delay *= 2
		}
	}
}

func retryDuringPrimaryTransition(ctx context.Context, client *mongo.Client, fn func(context.Context) error) error {
	const maxAttempts = 8
	delay := 25 * time.Millisecond
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if !isPrimaryTransitionError(err) {
			return err
		}
		lastErr = err

		if waitErr := waitForWritablePrimary(ctx, client); waitErr != nil {
			return fmt.Errorf("MongoDB primary transition did not settle after index creation failure: %w", waitErr)
		}
		if attempt == maxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("MongoDB primary transition retry cancelled: %w", ctx.Err())
		case <-time.After(delay):
		}
		if delay < 250*time.Millisecond {
			delay *= 2
		}
	}

	return fmt.Errorf("MongoDB primary transition persisted after %d attempts: %w", maxAttempts, lastErr)
}

func boolResultField(result bson.M, name string) bool {
	value, ok := result[name].(bool)
	return ok && value
}

func isPrimaryTransitionError(err error) bool {
	var cmdErr mongo.CommandError
	if !errors.As(err, &cmdErr) {
		return false
	}
	switch cmdErr.Code {
	case 91, 189, 10107, 11602, 13435, 13436:
		return true
	}
	switch cmdErr.Name {
	case "NotWritablePrimary", "NotPrimaryNoSecondaryOk", "InterruptedDueToReplStateChange", "PrimarySteppedDown", "ShutdownInProgress":
		return true
	default:
		return false
	}
}

func hasMongoErrorLabel(err error, label string) bool {
	var labeledErr mongo.LabeledError
	return errors.As(err, &labeledErr) && labeledErr.HasErrorLabel(label)
}

func isMongoMaxTimeMSExpiredError(err error) bool {
	var cmdErr mongo.CommandError
	return errors.As(err, &cmdErr) && cmdErr.IsMaxTimeMSExpiredError()
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
	if s.indexBuildCancel != nil {
		s.indexBuildCancel()
	}
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

// WithTransaction executes a function within a MongoDB transaction with limited attempts.
// Transient errors retry the transaction; unknown commit results retry only the commit.
func (s *Storage) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	const maxAttempts = 3

	session, err := s.client.StartSession()
	if err != nil {
		return fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
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

			// Check if this is a transient error that we should retry.
			// Storage methods wrap driver errors with %w for context, so use
			// errors.As instead of a direct type assertion.
			if hasMongoErrorLabel(err, labelTransientTransaction) && attempt < maxAttempts {
				lastErr = err
				continue // Retry
			}
			// Non-transient error or final attempt reached.
			return fmt.Errorf("transaction failed (attempt %d/%d): %w", attempt, maxAttempts, err)
		}

		// Try to commit with retry for unknown commit result
		for commitAttempt := 1; commitAttempt <= maxAttempts; commitAttempt++ {
			err = session.CommitTransaction(ctx)
			if err == nil {
				return nil // Success
			}

			// TransientTransactionError on commit - retry whole transaction
			if hasMongoErrorLabel(err, labelTransientTransaction) && attempt < maxAttempts {
				lastErr = err
				break // Break inner loop, continue outer loop to retry whole transaction
			}

			// UnknownTransactionCommitResult - retry just the commit
			if hasMongoErrorLabel(err, labelUnknownTransactionCommit) &&
				!isMongoMaxTimeMSExpiredError(err) &&
				commitAttempt < maxAttempts {
				lastErr = err
				continue // Retry commit
			}

			return fmt.Errorf("transaction commit failed (attempt %d/%d, commit %d/%d): %w",
				attempt, maxAttempts, commitAttempt, maxAttempts, err)
		}

		// The commit loop reaches here only for a transient error that requires
		// retrying the whole transaction.
	}

	return fmt.Errorf("transaction failed after %d attempts: %w", maxAttempts, lastErr)
}

// createIndexes creates all necessary database indexes
func (s *Storage) createIndexes(ctx context.Context) error {
	// Create indexes for each collection
	if err := s.commitmentStorage.CreateIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create commitment storage indexes: %w", err)
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

	if err := s.leadershipStorage.CreateIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create leadership indexes: %w", err)
	}

	if err := s.cachedTrustBaseStorage.storage.CreateIndexes(ctx); err != nil {
		return fmt.Errorf("failed to create leadership indexes: %w", err)
	}

	return nil
}
