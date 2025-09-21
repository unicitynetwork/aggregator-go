package interfaces

import (
	"context"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// CommitmentStorage handles temporary commitment storage
type CommitmentStorage interface {
	// Store stores a new commitment
	Store(ctx context.Context, commitment *models.Commitment) error

	// GetByRequestID retrieves a commitment by request ID
	GetByRequestID(ctx context.Context, requestID api.RequestID) (*models.Commitment, error)

	// GetUnprocessedBatch retrieves a batch of unprocessed commitments
	GetUnprocessedBatch(ctx context.Context, limit int) ([]*models.Commitment, error)

	// GetUnprocessedBatchWithCursor retrieves a batch with cursor-based pagination
	GetUnprocessedBatchWithCursor(ctx context.Context, lastID string, limit int) ([]*models.Commitment, string, error)

	// MarkProcessed marks commitments as processed
	MarkProcessed(ctx context.Context, requestIDs []api.RequestID) error

	// Delete removes processed commitments
	Delete(ctx context.Context, requestIDs []api.RequestID) error

	// Count returns the total number of commitments
	Count(ctx context.Context) (int64, error)

	// CountUnprocessed returns the number of unprocessed commitments
	CountUnprocessed(ctx context.Context) (int64, error)
}

// AggregatorRecordStorage handles finalized aggregator records
type AggregatorRecordStorage interface {
	// Store stores a new aggregator record
	Store(ctx context.Context, record *models.AggregatorRecord) error

	// StoreBatch stores multiple aggregator records
	StoreBatch(ctx context.Context, records []*models.AggregatorRecord) error

	// GetByRequestID retrieves an aggregator record by request ID
	GetByRequestID(ctx context.Context, requestID api.RequestID) (*models.AggregatorRecord, error)

	// GetByBlockNumber retrieves all records for a specific block
	GetByBlockNumber(ctx context.Context, blockNumber *api.BigInt) ([]*models.AggregatorRecord, error)

	// Count returns the total number of records
	Count(ctx context.Context) (int64, error)

	// GetLatest retrieves the most recent records
	GetLatest(ctx context.Context, limit int) ([]*models.AggregatorRecord, error)
}

// BlockStorage handles blockchain block storage
type BlockStorage interface {
	// Store stores a new block
	Store(ctx context.Context, block *models.Block) error

	// GetByNumber retrieves a block by number
	GetByNumber(ctx context.Context, blockNumber *api.BigInt) (*models.Block, error)

	// GetLatest retrieves the latest block
	GetLatest(ctx context.Context) (*models.Block, error)

	// GetLatestNumber retrieves the latest block number
	GetLatestNumber(ctx context.Context) (*api.BigInt, error)

	// GetLatestByRootHash retrieves the latest block with the given root hash
	GetLatestByRootHash(ctx context.Context, rootHash api.HexBytes) (*models.Block, error)

	// Count returns the total number of blocks
	Count(ctx context.Context) (int64, error)

	// GetRange retrieves blocks in a range
	GetRange(ctx context.Context, fromBlock, toBlock *api.BigInt) ([]*models.Block, error)
}

// SmtStorage handles Sparse Merkle Tree node storage
type SmtStorage interface {
	// Store stores a new SMT node
	Store(ctx context.Context, node *models.SmtNode) error

	// StoreBatch stores multiple SMT nodes
	StoreBatch(ctx context.Context, nodes []*models.SmtNode) error

	// GetByKey retrieves an SMT node by key
	GetByKey(ctx context.Context, key api.HexBytes) (*models.SmtNode, error)

	// Delete removes an SMT node
	Delete(ctx context.Context, key api.HexBytes) error

	// DeleteBatch removes multiple SMT nodes
	DeleteBatch(ctx context.Context, keys []api.HexBytes) error

	// Count returns the total number of nodes
	Count(ctx context.Context) (int64, error)

	// GetAll retrieves all SMT nodes (use with caution)
	GetAll(ctx context.Context) ([]*models.SmtNode, error)

	// GetChunked retrieves SMT nodes in chunks for efficient loading
	GetChunked(ctx context.Context, offset, limit int) ([]*models.SmtNode, error)
}

// BlockRecordsStorage handles block to request ID mappings
type BlockRecordsStorage interface {
	// Store stores a new block records entry
	Store(ctx context.Context, records *models.BlockRecords) error

	// GetByBlockNumber retrieves block records by block number
	GetByBlockNumber(ctx context.Context, blockNumber *api.BigInt) (*models.BlockRecords, error)

	// GetByRequestID retrieves the block number for a request ID
	GetByRequestID(ctx context.Context, requestID api.RequestID) (*api.BigInt, error)

	// Count returns the total number of block records
	Count(ctx context.Context) (int64, error)
}

// LeadershipStorage handles high availability leadership state
type LeadershipStorage interface {
	// AcquireLock attempts to acquire the leadership lock
	AcquireLock(ctx context.Context, serverID string, ttlSeconds int) (bool, error)

	// RenewLock renews the leadership lock
	RenewLock(ctx context.Context, serverID string, ttlSeconds int) error

	// ReleaseLock releases the leadership lock
	ReleaseLock(ctx context.Context, serverID string) error

	// GetCurrentLeader retrieves the current leader information
	GetCurrentLeader(ctx context.Context) (*models.LeadershipLock, error)

	// IsLeader checks if the given server is the current leader
	IsLeader(ctx context.Context, serverID string) (bool, error)
}

// Storage represents the complete storage interface
type Storage interface {
	CommitmentStorage() CommitmentStorage
	AggregatorRecordStorage() AggregatorRecordStorage
	BlockStorage() BlockStorage
	SmtStorage() SmtStorage
	BlockRecordsStorage() BlockRecordsStorage
	LeadershipStorage() LeadershipStorage

	// Database operations
	Ping(ctx context.Context) error
	Close(ctx context.Context) error

	// Transaction support
	WithTransaction(ctx context.Context, fn func(context.Context) error) error
}
