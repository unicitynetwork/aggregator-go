package storage

import (
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type Store interface {
	ReadStore
	Close() error
	CommittedState() (CommittedState, error)
	NewBatch() Batch
	Counters() Counters
	Metrics() Metrics
}

type ReadStore interface {
	GetNode(disk.NodeKey) ([]byte, bool, error)
	GetNodes([]disk.NodeKey, bool) ([]NodeReadResult, error)
}

type ReadSnapshotter interface {
	NewReadSnapshot() (ReadStore, func(), error)
}

type Batch interface {
	SetNode(disk.NodeKey, []byte) error
	DeleteNode(disk.NodeKey) error
	SetCommittedState(disk.Hash, *api.BigInt) error
	Commit() error
	Close() error
}

type BulkBatch interface {
	Batch
	SetNodes(map[disk.NodeKey][]byte) error
	DeleteNodes([]disk.NodeKey) error
}

type Counters struct {
	PointReads       int64
	MetaPointReads   int64
	NodePointReads   int64
	OpenPointReads   int64
	Iterators        int64
	BatchSets        int64
	NodeSets         int64
	NodeDeletes      int64
	MetaSets         int64
	BatchesCommitted int64
	Checkpoints      int64
}

type NodeReadResult struct {
	Value []byte
	Found bool
}

type NodeWrite struct {
	Key   disk.NodeKey
	Value []byte
}

type BulkEntryBatch interface {
	BulkBatch
	SetNodeEntries([]NodeWrite) error
}

type Metrics struct {
	BlockCacheSize         int64
	BlockCacheCount        int64
	BlockCacheHits         int64
	BlockCacheMisses       int64
	BlockCacheIndexHits    int64
	BlockCacheIndexMisses  int64
	BlockCacheFilterHits   int64
	BlockCacheFilterMisses int64
	BlockCacheDataHits     int64
	BlockCacheDataMisses   int64
	BlockCacheBytesRead    int64
	BlockCacheBytesWrite   int64
	BloomFilterUseful      int64
	TableCacheSize         int64
	TableCacheCount        int64
	TableCacheHits         int64
	TableCacheMisses       int64

	CompactEstimatedDebt   uint64
	CompactInProgressBytes int64
	CompactNumInProgress   int64
	CompactCount           int64
	FlushCount             int64

	MemTableSize  uint64
	MemTableCount int64
	WALSize       uint64
	WALFiles      int64

	L0NumFiles  int64
	L0Sublevels int32
	L0Size      int64
	L0Score     float64
}

type CommittedState struct {
	RootHash    disk.Hash
	BlockNumber *api.BigInt
}
