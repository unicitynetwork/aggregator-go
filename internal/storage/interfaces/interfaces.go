package interfaces

import (
	"context"
	"errors"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ErrDuplicateKey is returned when a storage operation fails due to a duplicate key.
var ErrDuplicateKey = errors.New("duplicate key error")

// CommitmentQueue handles commitment queue operations
type CommitmentQueue interface {
	// Store stores a new commitment
	Store(ctx context.Context, commitment *models.Commitment) error

	// GetByRequestID retrieves a commitment by request ID
	GetByRequestID(ctx context.Context, requestID api.RequestID) (*models.Commitment, error)

	// GetUnprocessedBatch retrieves a batch of unprocessed commitments
	GetUnprocessedBatch(ctx context.Context, limit int) ([]*models.Commitment, error)

	// GetUnprocessedBatchWithCursor retrieves a batch with cursor-based pagination
	GetUnprocessedBatchWithCursor(ctx context.Context, lastID string, limit int) ([]*models.Commitment, string, error)

	// StreamCommitments continuously streams commitments to the provided channel
	StreamCommitments(ctx context.Context, commitmentChan chan<- *models.Commitment) error

	// MarkProcessed marks commitments as processed
	MarkProcessed(ctx context.Context, entries []CommitmentAck) error

	// Delete removes processed commitments
	Delete(ctx context.Context, requestIDs []api.RequestID) error

	// Count returns the total number of commitments
	Count(ctx context.Context) (int64, error)

	// CountUnprocessed returns the number of unprocessed commitments
	CountUnprocessed(ctx context.Context) (int64, error)

	// GetAllPending retrieves all pending (unacknowledged) commitments
	// Used to cleanup already-processed pending commitments on startup
	GetAllPending(ctx context.Context) ([]*models.Commitment, error)

	// GetByRequestIDs retrieves commitments matching the given request IDs.
	// Streams through data in batches to avoid loading everything into memory.
	GetByRequestIDs(ctx context.Context, requestIDs []api.RequestID) (map[string]*models.Commitment, error)

	// Lifecycle methods
	Initialize(ctx context.Context) error
	Close(ctx context.Context) error
}

// CommitmentAck represents the metadata required to acknowledge a commitment.
type CommitmentAck struct {
	RequestID api.RequestID
	StreamID  string
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

	// GetExistingRequestIDs returns which of the given request IDs already exist
	GetExistingRequestIDs(ctx context.Context, requestIDs []string) (map[string]bool, error)
}

// BlockStorage handles blockchain block storage
type BlockStorage interface {
	// Store stores a new block
	Store(ctx context.Context, block *models.Block) error

	// GetByNumber retrieves a block by number
	GetByNumber(ctx context.Context, blockNumber *api.BigInt) (*models.Block, error)

	// GetLatest retrieves the latest finalized block
	GetLatest(ctx context.Context) (*models.Block, error)

	// GetLatestNumber retrieves the latest finalized block number
	GetLatestNumber(ctx context.Context) (*api.BigInt, error)

	// GetLatestByRootHash retrieves the latest block with the given root hash
	GetLatestByRootHash(ctx context.Context, rootHash api.HexBytes) (*models.Block, error)

	// Count returns the total number of blocks
	Count(ctx context.Context) (int64, error)

	// GetRange retrieves blocks in a range
	GetRange(ctx context.Context, fromBlock, toBlock *api.BigInt) ([]*models.Block, error)

	// SetFinalized marks a block as finalized or unfinalized
	SetFinalized(ctx context.Context, blockNumber *api.BigInt, finalized bool) error

	// GetUnfinalized returns all unfinalized blocks (should be at most 1)
	GetUnfinalized(ctx context.Context) ([]*models.Block, error)
}

// SmtStorage handles Sparse Merkle Tree node storage
type SmtStorage interface {
	// Store stores a new SMT node
	Store(ctx context.Context, node *models.SmtNode) error

	// StoreBatch stores multiple SMT nodes
	StoreBatch(ctx context.Context, nodes []*models.SmtNode) error

	// UpsertBatch stores or updates multiple SMT nodes, replacing existing values
	UpsertBatch(ctx context.Context, nodes []*models.SmtNode) error

	// GetByKey retrieves an SMT node by key
	GetByKey(ctx context.Context, key api.HexBytes) (*models.SmtNode, error)

	// GetByKeys retrieves multiple SMT nodes by their keys
	GetByKeys(ctx context.Context, keys []api.HexBytes) ([]*models.SmtNode, error)

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

	// GetExistingKeys returns which of the given keys already exist in the database
	GetExistingKeys(ctx context.Context, keys []string) (map[string]bool, error)
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

	// GetNextBlock retrieves the first block after the given block number.
	// If blockNumber is nil then returns the very first block.
	GetNextBlock(ctx context.Context, blockNumber *api.BigInt) (*models.BlockRecords, error)

	// GetLatestBlockNumber retrieves the latest block
	GetLatestBlockNumber(ctx context.Context) (*api.BigInt, error)
}

// LeadershipStorage handles high availability leadership state
type LeadershipStorage interface {
	// TryAcquireLock attempts to acquire the leadership lock,
	// returns true if the lock was successfully acquired,
	// returns false if the lock is already granted for this server.
	TryAcquireLock(ctx context.Context, lockID string, serverID string) (bool, error)

	// ReleaseLock releases the leadership lock, returns true if the lock was released, false otherwise.
	ReleaseLock(ctx context.Context, lockID string, serverID string) (bool, error)

	// UpdateHeartbeat updates the heartbeat timestamp to maintain leadership,
	// returns true if the lock document was successfully updated, false otherwise.
	UpdateHeartbeat(ctx context.Context, lockID string, serverID string) (bool, error)

	// IsLeader checks if the given server is the current leader.
	IsLeader(ctx context.Context, lockID string, serverID string) (bool, error)
}

var ErrTrustBaseNotFound = errors.New("trust base not found")
var ErrTrustBaseAlreadyExists = errors.New("trust base already exists")

type TrustBaseStorage interface {
	Store(ctx context.Context, trustBase types.RootTrustBase) error
	GetByEpoch(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error)
	GetLatest(ctx context.Context) (*types.RootTrustBaseV1, error)
	GetTrustBases(ctx context.Context, from, to uint64) ([]*types.RootTrustBaseV1, error)
}

type TrustBaseProvider interface {
	GetByEpoch(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error)
}

// Storage handles persistent data storage
type Storage interface {
	AggregatorRecordStorage() AggregatorRecordStorage
	BlockStorage() BlockStorage
	SmtStorage() SmtStorage
	BlockRecordsStorage() BlockRecordsStorage
	LeadershipStorage() LeadershipStorage
	TrustBaseStorage() TrustBaseStorage

	Initialize(ctx context.Context) error
	Ping(ctx context.Context) error
	Close(ctx context.Context) error

	// Transaction support
	WithTransaction(ctx context.Context, fn func(context.Context) error) error
}
