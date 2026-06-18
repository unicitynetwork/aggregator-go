//go:build rocksdb

package rocksstore

/*
#cgo linux LDFLAGS: -lrocksdb -lstdc++ -lm -lz -lbz2 -llz4 -lzstd -lsnappy
#include <stdbool.h>
#include <stdlib.h>
#include <rocksdb/c.h>

// These wrappers keep large SMT reads/writes inside one cgo call. Calling the
// RocksDB C API once per node from Go was much slower in benchmarks because
// every node paid the Go<->C boundary cost.
static void go_rocksdb_batched_multi_get_cf(
	rocksdb_t* db,
	const rocksdb_readoptions_t* options,
	rocksdb_column_family_handle_t* column_family,
	size_t num_keys,
	const char* const* keys_list,
	const size_t* keys_list_sizes,
	rocksdb_pinnableslice_t** values,
	char** errs,
	unsigned char sorted_input) {
	rocksdb_batched_multi_get_cf(db, options, column_family, num_keys, keys_list, keys_list_sizes, values, errs, sorted_input != 0);
}

static void go_rocksdb_writebatch_put_many_cf(
	rocksdb_writebatch_t* batch,
	rocksdb_column_family_handle_t* column_family,
	int count,
	const char* const* keys_list,
	const size_t* keys_list_sizes,
	const char* const* values_list,
	const size_t* values_list_sizes) {
	for (int i = 0; i < count; i++) {
		rocksdb_writebatch_put_cf(batch, column_family, keys_list[i], keys_list_sizes[i], values_list[i], values_list_sizes[i]);
	}
}

static void go_rocksdb_writebatch_delete_many_cf(
	rocksdb_writebatch_t* batch,
	rocksdb_column_family_handle_t* column_family,
	int count,
	const char* const* keys_list,
	const size_t* keys_list_sizes) {
	for (int i = 0; i < count; i++) {
		rocksdb_writebatch_delete_cf(batch, column_family, keys_list[i], keys_list_sizes[i]);
	}
}
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const (
	SchemaVersion = "1"
	TreeLayout    = "yellowpaper-rsmt-sha256-v1"
	KeyBits       = "256"

	maxCInt = int(^uint32(0) >> 1)
)

const (
	rocksDBTickerBlockCacheMiss       = 0
	rocksDBTickerBlockCacheHit        = 1
	rocksDBTickerBlockCacheIndexMiss  = 4
	rocksDBTickerBlockCacheIndexHit   = 5
	rocksDBTickerBlockCacheFilterMiss = 8
	rocksDBTickerBlockCacheFilterHit  = 9
	rocksDBTickerBlockCacheDataMiss   = 12
	rocksDBTickerBlockCacheDataHit    = 13
	rocksDBTickerBlockCacheBytesRead  = 16
	rocksDBTickerBlockCacheBytesWrite = 17
	rocksDBTickerBloomFilterUseful    = 18
)

const (
	cfDefault = "default"
	cfNodes   = "smt_nodes"
	cfMeta    = "smt_meta"

	metaRoot          = "root"
	metaRootBlock     = "root_block"
	metaSchemaVersion = "schema_version"
	metaTreeLayout    = "tree_layout"
	metaKeyBits       = "key_bits"
)

type Options struct {
	ReadOnly                 bool
	CacheSizeBytes           int64
	NoSyncWrites             bool
	DisableWAL               bool
	MaxBackgroundJobs        int
	MaxBackgroundCompactions int
	MaxBackgroundFlushes     int
	MaxSubcompactions        int
	MemTableSizeBytes        uint64
	NoCompression            bool
	EnableStatistics         bool
	BloomBitsPerKey          float64
	BlockSizeBytes           int
}

type Store struct {
	db        *C.rocksdb_t
	opts      *C.rocksdb_options_t
	readOpts  *C.rocksdb_readoptions_t
	writeOpts *C.rocksdb_writeoptions_t
	cache     *C.rocksdb_cache_t
	filter    *C.rocksdb_filterpolicy_t
	stats     bool

	defaultCF *C.rocksdb_column_family_handle_t
	nodesCF   *C.rocksdb_column_family_handle_t
	metaCF    *C.rocksdb_column_family_handle_t

	// Guards RocksDB C handle lifetime. Read snapshots hold this for their
	// whole lifetime so Close cannot free DB handles while RocksDB still uses
	// a pinned snapshot.
	closeMu sync.RWMutex
	closed  bool

	mu    sync.RWMutex
	root  disk.Hash
	block []byte

	counters storeCounters
}

type ReadSnapshot struct {
	store    *Store
	snapshot *C.rocksdb_snapshot_t
	readOpts *C.rocksdb_readoptions_t
	release  func()
	mu       sync.RWMutex
	once     sync.Once
}

var _ storage.ReadSnapshotter = (*Store)(nil)
var _ storage.ReadStore = (*ReadSnapshot)(nil)

type storeCounters struct {
	pointReads       atomic.Int64
	metaPointReads   atomic.Int64
	nodePointReads   atomic.Int64
	openPointReads   atomic.Int64
	iterators        atomic.Int64
	batchSets        atomic.Int64
	nodeSets         atomic.Int64
	nodeDeletes      atomic.Int64
	metaSets         atomic.Int64
	batchesCommitted atomic.Int64
	checkpoints      atomic.Int64
}

func Open(path string, opts Options) (*Store, error) {
	dbOpts := C.rocksdb_options_create()
	if dbOpts == nil {
		return nil, fmt.Errorf("create RocksDB options")
	}
	C.rocksdb_options_set_create_if_missing(dbOpts, boolToUChar(!opts.ReadOnly))
	C.rocksdb_options_set_create_missing_column_families(dbOpts, boolToUChar(!opts.ReadOnly))
	C.rocksdb_options_set_max_open_files(dbOpts, 1024)
	if opts.EnableStatistics {
		C.rocksdb_options_enable_statistics(dbOpts)
		C.rocksdb_options_set_statistics_level(dbOpts, C.rocksdb_statistics_level_except_timers)
	}
	if opts.MaxBackgroundJobs > 0 {
		C.rocksdb_options_set_max_background_jobs(dbOpts, C.int(opts.MaxBackgroundJobs))
	}
	if opts.MaxBackgroundCompactions > 0 {
		C.rocksdb_options_set_max_background_compactions(dbOpts, C.int(opts.MaxBackgroundCompactions))
	}
	if opts.MaxBackgroundFlushes > 0 {
		C.rocksdb_options_set_max_background_flushes(dbOpts, C.int(opts.MaxBackgroundFlushes))
	}
	if opts.MaxSubcompactions > 0 {
		C.rocksdb_options_set_max_subcompactions(dbOpts, C.uint32_t(opts.MaxSubcompactions))
	}
	if opts.MemTableSizeBytes > 0 {
		C.rocksdb_options_set_write_buffer_size(dbOpts, C.size_t(opts.MemTableSizeBytes))
	}
	if opts.NoCompression {
		C.rocksdb_options_set_compression(dbOpts, C.rocksdb_no_compression)
	} else {
		// Rugregator configures the SMT node column family with LZ4. Use the
		// same default for the Go benchmark backend so RocksDB comparisons are
		// not skewed by RocksDB's library default compression.
		C.rocksdb_options_set_compression(dbOpts, C.rocksdb_lz4_compression)
	}

	var cache *C.rocksdb_cache_t
	var filter *C.rocksdb_filterpolicy_t
	if opts.CacheSizeBytes > 0 {
		cache = C.rocksdb_cache_create_lru(C.size_t(opts.CacheSizeBytes))
	}
	if opts.BloomBitsPerKey > 0 {
		filter = C.rocksdb_filterpolicy_create_bloom_full(C.double(opts.BloomBitsPerKey))
	}
	if cache != nil || filter != nil || opts.BlockSizeBytes > 0 {
		tableOpts := C.rocksdb_block_based_options_create()
		if tableOpts == nil {
			C.rocksdb_options_destroy(dbOpts)
			if cache != nil {
				C.rocksdb_cache_destroy(cache)
			}
			if filter != nil {
				C.rocksdb_filterpolicy_destroy(filter)
			}
			return nil, fmt.Errorf("create RocksDB block table options")
		}
		if cache != nil {
			C.rocksdb_block_based_options_set_block_cache(tableOpts, cache)
		}
		if filter != nil {
			C.rocksdb_block_based_options_set_filter_policy(tableOpts, filter)
			C.rocksdb_block_based_options_set_whole_key_filtering(tableOpts, boolToUChar(true))
		}
		if opts.BlockSizeBytes > 0 {
			C.rocksdb_block_based_options_set_block_size(tableOpts, C.size_t(opts.BlockSizeBytes))
		}
		C.rocksdb_options_set_block_based_table_factory(dbOpts, tableOpts)
		C.rocksdb_block_based_options_destroy(tableOpts)
	}

	readOpts := C.rocksdb_readoptions_create()
	writeOpts := C.rocksdb_writeoptions_create()
	if readOpts == nil || writeOpts == nil {
		destroyOpenResources(nil, dbOpts, readOpts, writeOpts, cache, filter, nil)
		return nil, fmt.Errorf("create RocksDB read/write options")
	}
	C.rocksdb_writeoptions_set_sync(writeOpts, boolToUChar(!opts.NoSyncWrites && !opts.DisableWAL))
	C.rocksdb_writeoptions_disable_WAL(writeOpts, boolToInt(opts.DisableWAL))

	names := []string{cfDefault, cfNodes, cfMeta}
	cNames, freeNames := cStringArray(names)
	defer freeNames()

	cfOpts := make([]*C.rocksdb_options_t, len(names))
	for i := range cfOpts {
		cfOpts[i] = dbOpts
	}
	handles := make([]*C.rocksdb_column_family_handle_t, len(names))

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var errPtr *C.char
	var db *C.rocksdb_t
	if opts.ReadOnly {
		db = C.rocksdb_open_for_read_only_column_families(
			dbOpts,
			cPath,
			C.int(len(names)),
			(**C.char)(unsafe.Pointer(&cNames[0])),
			(**C.rocksdb_options_t)(unsafe.Pointer(&cfOpts[0])),
			(**C.rocksdb_column_family_handle_t)(unsafe.Pointer(&handles[0])),
			0,
			&errPtr,
		)
	} else {
		db = C.rocksdb_open_column_families(
			dbOpts,
			cPath,
			C.int(len(names)),
			(**C.char)(unsafe.Pointer(&cNames[0])),
			(**C.rocksdb_options_t)(unsafe.Pointer(&cfOpts[0])),
			(**C.rocksdb_column_family_handle_t)(unsafe.Pointer(&handles[0])),
			&errPtr,
		)
	}
	if err := takeError(errPtr); err != nil {
		destroyOpenResources(db, dbOpts, readOpts, writeOpts, cache, filter, handles)
		return nil, fmt.Errorf("open RocksDB SMT store: %w", err)
	}
	if db == nil {
		destroyOpenResources(db, dbOpts, readOpts, writeOpts, cache, filter, handles)
		return nil, fmt.Errorf("open RocksDB SMT store returned nil")
	}

	s := &Store{
		db:        db,
		opts:      dbOpts,
		readOpts:  readOpts,
		writeOpts: writeOpts,
		cache:     cache,
		filter:    filter,
		stats:     opts.EnableStatistics,
		defaultCF: handles[0],
		nodesCF:   handles[1],
		metaCF:    handles[2],
	}
	if err := s.loadOrInitMetadata(opts.ReadOnly); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func destroyOpenResources(db *C.rocksdb_t, opts *C.rocksdb_options_t, readOpts *C.rocksdb_readoptions_t, writeOpts *C.rocksdb_writeoptions_t, cache *C.rocksdb_cache_t, filter *C.rocksdb_filterpolicy_t, handles []*C.rocksdb_column_family_handle_t) {
	for _, handle := range handles {
		if handle != nil {
			C.rocksdb_column_family_handle_destroy(handle)
		}
	}
	if db != nil {
		C.rocksdb_close(db)
	}
	if readOpts != nil {
		C.rocksdb_readoptions_destroy(readOpts)
	}
	if writeOpts != nil {
		C.rocksdb_writeoptions_destroy(writeOpts)
	}
	if opts != nil {
		C.rocksdb_options_destroy(opts)
	}
	if cache != nil {
		C.rocksdb_cache_destroy(cache)
	}
	// The block-based table factory keeps a reference to the filter policy. With
	// the RocksDB 8.10 C API used by the benchmark backend, destroying the policy
	// after installing it in table options can double-free during DB shutdown.
	// This benchmark process creates at most one policy per store open, so keep
	// the policy owned by RocksDB/table options instead of destroying it here.
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	s.closed = true
	if s.defaultCF != nil {
		C.rocksdb_column_family_handle_destroy(s.defaultCF)
		s.defaultCF = nil
	}
	if s.nodesCF != nil {
		C.rocksdb_column_family_handle_destroy(s.nodesCF)
		s.nodesCF = nil
	}
	if s.metaCF != nil {
		C.rocksdb_column_family_handle_destroy(s.metaCF)
		s.metaCF = nil
	}
	if s.db != nil {
		C.rocksdb_close(s.db)
		s.db = nil
	}
	if s.readOpts != nil {
		C.rocksdb_readoptions_destroy(s.readOpts)
		s.readOpts = nil
	}
	if s.writeOpts != nil {
		C.rocksdb_writeoptions_destroy(s.writeOpts)
		s.writeOpts = nil
	}
	if s.opts != nil {
		C.rocksdb_options_destroy(s.opts)
		s.opts = nil
	}
	if s.cache != nil {
		C.rocksdb_cache_destroy(s.cache)
		s.cache = nil
	}
	s.filter = nil
	return nil
}

func (s *Store) CommittedState() (storage.CommittedState, error) {
	if s == nil {
		return storage.CommittedState{}, fmt.Errorf("nil RocksDB SMT store")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	blockNumber, err := decodeBlockNumber(s.block)
	if err != nil {
		return storage.CommittedState{}, err
	}
	return storage.CommittedState{
		RootHash:    s.root,
		BlockNumber: blockNumber,
	}, nil
}

func (s *Store) GetNode(key disk.NodeKey) ([]byte, bool, error) {
	value, ok, err := s.getCF(columnFamilyNodes, nodeKey(key), readKindNode)
	if err != nil {
		return nil, false, err
	}
	return value, ok, nil
}

func (s *Store) GetNodes(keys []disk.NodeKey, sortedInput bool) ([]storage.NodeReadResult, error) {
	return s.getNodesWithReadOptions(keys, sortedInput)
}

func (s *Store) NewReadSnapshot() (storage.ReadStore, func(), error) {
	if s == nil {
		return nil, nil, fmt.Errorf("nil RocksDB SMT store")
	}
	s.closeMu.RLock()
	if s.closed || s.db == nil {
		s.closeMu.RUnlock()
		return nil, nil, fmt.Errorf("closed RocksDB SMT store")
	}
	readOpts := C.rocksdb_readoptions_create()
	if readOpts == nil {
		s.closeMu.RUnlock()
		return nil, nil, fmt.Errorf("create RocksDB snapshot read options")
	}
	snapshot := C.rocksdb_create_snapshot(s.db)
	if snapshot == nil {
		C.rocksdb_readoptions_destroy(readOpts)
		s.closeMu.RUnlock()
		return nil, nil, fmt.Errorf("create RocksDB snapshot")
	}
	C.rocksdb_readoptions_set_snapshot(readOpts, snapshot)
	readSnapshot := &ReadSnapshot{
		store:    s,
		snapshot: snapshot,
		readOpts: readOpts,
		release:  s.closeMu.RUnlock,
	}
	return readSnapshot, readSnapshot.Close, nil
}

func (s *Store) NumSnapshots() uint64 {
	return s.propertyUint("rocksdb.num-snapshots")
}

func (r *ReadSnapshot) Close() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		if r.readOpts != nil {
			C.rocksdb_readoptions_destroy(r.readOpts)
			r.readOpts = nil
		}
		if r.store != nil && r.store.db != nil && r.snapshot != nil {
			C.rocksdb_release_snapshot(r.store.db, r.snapshot)
			r.snapshot = nil
		}
		if r.release != nil {
			r.release()
			r.release = nil
		}
	})
}

func (r *ReadSnapshot) GetNode(key disk.NodeKey) ([]byte, bool, error) {
	if r == nil || r.store == nil {
		return nil, false, fmt.Errorf("closed RocksDB SMT read snapshot")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.readOpts == nil {
		return nil, false, fmt.Errorf("closed RocksDB SMT read snapshot")
	}
	value, ok, err := r.store.getCFWithReadOptionsOpen(r.store.nodesCF, nodeKey(key), readKindNode, r.readOpts)
	if err != nil {
		return nil, false, err
	}
	return value, ok, nil
}

func (r *ReadSnapshot) GetNodes(keys []disk.NodeKey, sortedInput bool) ([]storage.NodeReadResult, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("closed RocksDB SMT read snapshot")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.readOpts == nil {
		return nil, fmt.Errorf("closed RocksDB SMT read snapshot")
	}
	return r.store.getNodesWithReadOptionsOpen(keys, sortedInput, r.readOpts)
}

func (s *Store) getNodesWithReadOptions(keys []disk.NodeKey, sortedInput bool) ([]storage.NodeReadResult, error) {
	if s == nil {
		return nil, fmt.Errorf("nil RocksDB SMT store")
	}
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	return s.getNodesWithReadOptionsOpen(keys, sortedInput, s.readOpts)
}

// getNodesWithReadOptionsOpen assumes s.closeMu is already held for reading.
func (s *Store) getNodesWithReadOptionsOpen(keys []disk.NodeKey, sortedInput bool, readOpts *C.rocksdb_readoptions_t) ([]storage.NodeReadResult, error) {
	if s == nil {
		return nil, fmt.Errorf("nil RocksDB SMT store")
	}
	if s.closed || s.db == nil || s.nodesCF == nil {
		return nil, fmt.Errorf("closed RocksDB SMT store")
	}
	if readOpts == nil {
		return nil, fmt.Errorf("nil RocksDB read options")
	}
	results := make([]storage.NodeReadResult, len(keys))
	if len(keys) == 0 {
		return results, nil
	}
	if len(keys) == 1 {
		value, ok, err := s.getCFWithReadOptionsOpen(s.nodesCF, nodeKey(keys[0]), readKindNode, readOpts)
		if err != nil {
			return nil, err
		}
		results[0] = storage.NodeReadResult{Value: value, Found: ok}
		return results, nil
	}

	encodedKeys := make([][]byte, len(keys))
	totalKeyBytes := 0
	for i, key := range keys {
		encodedKeys[i] = nodeKey(key)
		totalKeyBytes += len(encodedKeys[i])
	}

	keyBuf := C.malloc(C.size_t(totalKeyBytes))
	keyPtrsMem := C.malloc(C.size_t(len(keys)) * C.size_t(unsafe.Sizeof(uintptr(0))))
	keyLensMem := C.malloc(C.size_t(len(keys)) * C.size_t(unsafe.Sizeof(C.size_t(0))))
	valuesMem := C.malloc(C.size_t(len(keys)) * C.size_t(unsafe.Sizeof(uintptr(0))))
	errsMem := C.malloc(C.size_t(len(keys)) * C.size_t(unsafe.Sizeof(uintptr(0))))
	if keyBuf == nil || keyPtrsMem == nil || keyLensMem == nil || valuesMem == nil || errsMem == nil {
		C.free(keyBuf)
		C.free(keyPtrsMem)
		C.free(keyLensMem)
		C.free(valuesMem)
		C.free(errsMem)
		return nil, fmt.Errorf("allocate RocksDB MultiGet buffers")
	}
	defer C.free(keyBuf)
	defer C.free(keyPtrsMem)
	defer C.free(keyLensMem)
	defer C.free(valuesMem)
	defer C.free(errsMem)

	keyPtrs := unsafe.Slice((**C.char)(keyPtrsMem), len(keys))
	keyLens := unsafe.Slice((*C.size_t)(keyLensMem), len(keys))
	values := unsafe.Slice((**C.rocksdb_pinnableslice_t)(valuesMem), len(keys))
	errs := unsafe.Slice((**C.char)(errsMem), len(keys))
	keyBytes := unsafe.Slice((*byte)(keyBuf), totalKeyBytes)
	offset := 0
	for i, key := range encodedKeys {
		copy(keyBytes[offset:], key)
		keyPtrs[i] = (*C.char)(unsafe.Add(keyBuf, offset))
		keyLens[i] = C.size_t(len(key))
		values[i] = nil
		errs[i] = nil
		offset += len(key)
	}

	C.go_rocksdb_batched_multi_get_cf(
		s.db,
		readOpts,
		s.nodesCF,
		C.size_t(len(keys)),
		(**C.char)(keyPtrsMem),
		(*C.size_t)(keyLensMem),
		(**C.rocksdb_pinnableslice_t)(valuesMem),
		(**C.char)(errsMem),
		boolToUChar(sortedInput),
	)
	s.counters.pointReads.Add(int64(len(keys)))
	s.counters.nodePointReads.Add(int64(len(keys)))

	var firstErr error
	for i := range keys {
		errPtr := errs[i]
		if errPtr != nil {
			err := C.GoString(errPtr)
			C.rocksdb_free(unsafe.Pointer(errPtr))
			if firstErr == nil {
				firstErr = fmt.Errorf("rocksdb batched multiget SMT node: %s", err)
			}
		}
		value := values[i]
		if value == nil {
			continue
		}
		var valueLen C.size_t
		valuePtr := C.rocksdb_pinnableslice_value(value, &valueLen)
		if valueLen > C.size_t(maxCInt) {
			C.rocksdb_pinnableslice_destroy(value)
			if firstErr == nil {
				firstErr = fmt.Errorf("rocksdb SMT node value too large: %d bytes", uint64(valueLen))
			}
			continue
		}
		if firstErr == nil {
			results[i] = storage.NodeReadResult{
				Value: C.GoBytes(unsafe.Pointer(valuePtr), C.int(valueLen)),
				Found: true,
			}
		}
		C.rocksdb_pinnableslice_destroy(value)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

func (s *Store) NewBatch() storage.Batch {
	return &Batch{
		store: s,
		batch: C.rocksdb_writebatch_create(),
	}
}

func (s *Store) Counters() storage.Counters {
	if s == nil {
		return storage.Counters{}
	}
	return storage.Counters{
		PointReads:       s.counters.pointReads.Load(),
		MetaPointReads:   s.counters.metaPointReads.Load(),
		NodePointReads:   s.counters.nodePointReads.Load(),
		OpenPointReads:   s.counters.openPointReads.Load(),
		Iterators:        s.counters.iterators.Load(),
		BatchSets:        s.counters.batchSets.Load(),
		NodeSets:         s.counters.nodeSets.Load(),
		NodeDeletes:      s.counters.nodeDeletes.Load(),
		MetaSets:         s.counters.metaSets.Load(),
		BatchesCommitted: s.counters.batchesCommitted.Load(),
		Checkpoints:      s.counters.checkpoints.Load(),
	}
}

func (s *Store) Metrics() storage.Metrics {
	if s == nil {
		return storage.Metrics{}
	}
	var metrics storage.Metrics
	s.closeMu.RLock()
	if s.closed || s.db == nil {
		s.closeMu.RUnlock()
		return storage.Metrics{}
	}
	if s.cache != nil {
		metrics.BlockCacheSize = int64(C.rocksdb_cache_get_usage(s.cache))
	}
	if s.opts != nil && s.stats {
		metrics.BlockCacheMisses = int64(C.rocksdb_options_statistics_get_ticker_count(s.opts, C.uint32_t(rocksDBTickerBlockCacheMiss)))
		metrics.BlockCacheHits = int64(C.rocksdb_options_statistics_get_ticker_count(s.opts, C.uint32_t(rocksDBTickerBlockCacheHit)))
		metrics.BlockCacheIndexMisses = int64(C.rocksdb_options_statistics_get_ticker_count(s.opts, C.uint32_t(rocksDBTickerBlockCacheIndexMiss)))
		metrics.BlockCacheIndexHits = int64(C.rocksdb_options_statistics_get_ticker_count(s.opts, C.uint32_t(rocksDBTickerBlockCacheIndexHit)))
		metrics.BlockCacheFilterMisses = int64(C.rocksdb_options_statistics_get_ticker_count(s.opts, C.uint32_t(rocksDBTickerBlockCacheFilterMiss)))
		metrics.BlockCacheFilterHits = int64(C.rocksdb_options_statistics_get_ticker_count(s.opts, C.uint32_t(rocksDBTickerBlockCacheFilterHit)))
		metrics.BlockCacheDataMisses = int64(C.rocksdb_options_statistics_get_ticker_count(s.opts, C.uint32_t(rocksDBTickerBlockCacheDataMiss)))
		metrics.BlockCacheDataHits = int64(C.rocksdb_options_statistics_get_ticker_count(s.opts, C.uint32_t(rocksDBTickerBlockCacheDataHit)))
		metrics.BlockCacheBytesRead = int64(C.rocksdb_options_statistics_get_ticker_count(s.opts, C.uint32_t(rocksDBTickerBlockCacheBytesRead)))
		metrics.BlockCacheBytesWrite = int64(C.rocksdb_options_statistics_get_ticker_count(s.opts, C.uint32_t(rocksDBTickerBlockCacheBytesWrite)))
		metrics.BloomFilterUseful = int64(C.rocksdb_options_statistics_get_ticker_count(s.opts, C.uint32_t(rocksDBTickerBloomFilterUseful)))
	}
	s.closeMu.RUnlock()
	metrics.CompactEstimatedDebt = s.propertyUintCF("rocksdb.estimate-pending-compaction-bytes")
	metrics.CompactNumInProgress = int64(s.propertyUintCF("rocksdb.num-running-compactions"))
	metrics.MemTableSize = s.propertyUintCF("rocksdb.cur-size-all-mem-tables")
	metrics.L0NumFiles = int64(s.propertyUintCF("rocksdb.num-files-at-level0"))
	return metrics
}

func (s *Store) CompactNodes() error {
	if s == nil {
		return fmt.Errorf("nil RocksDB SMT store")
	}
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed || s.db == nil || s.nodesCF == nil {
		return fmt.Errorf("closed RocksDB SMT store")
	}
	C.rocksdb_compact_range_cf(s.db, s.nodesCF, nil, 0, nil, 0)
	return nil
}

func (s *Store) Checkpoint(path string) error {
	if s == nil {
		return fmt.Errorf("nil RocksDB SMT store")
	}
	if path == "" {
		return fmt.Errorf("RocksDB SMT checkpoint path is required")
	}

	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed || s.db == nil {
		return fmt.Errorf("closed RocksDB SMT store")
	}
	var errPtr *C.char
	checkpoint := C.rocksdb_checkpoint_object_create(s.db, &errPtr)
	if err := takeError(errPtr); err != nil {
		return fmt.Errorf("create RocksDB SMT checkpoint object: %w", err)
	}
	if checkpoint == nil {
		return fmt.Errorf("create RocksDB SMT checkpoint object returned nil")
	}
	defer C.rocksdb_checkpoint_object_destroy(checkpoint)

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	C.rocksdb_checkpoint_create(checkpoint, cPath, 0, &errPtr)
	if err := takeError(errPtr); err != nil {
		return fmt.Errorf("create RocksDB SMT checkpoint at %q: %w", path, err)
	}
	s.counters.checkpoints.Add(1)
	return nil
}

func (s *Store) DiagnosticProperties() map[string]string {
	names := []string{
		"rocksdb.levelstats",
		"rocksdb.cfstats",
		"rocksdb.dbstats",
		"rocksdb.cf-file-histogram",
		"rocksdb.sstables",
	}
	properties := make(map[string]string, len(names))
	for _, name := range names {
		if value, ok := s.propertyValueCF(name); ok {
			properties[name] = value
		}
	}
	return properties
}

type readKind int

const (
	readKindMeta readKind = iota
	readKindNode
	readKindOpen
	readKindProof
)

type columnFamilyID int

const (
	columnFamilyDefault columnFamilyID = iota
	columnFamilyNodes
	columnFamilyMeta
)

var (
	proofResponseKeyPrefix = []byte("proof:")
)

func (s *Store) columnFamily(cf columnFamilyID) *C.rocksdb_column_family_handle_t {
	switch cf {
	case columnFamilyDefault:
		return s.defaultCF
	case columnFamilyNodes:
		return s.nodesCF
	case columnFamilyMeta:
		return s.metaCF
	default:
		return nil
	}
}

func (s *Store) getCF(cf columnFamilyID, key []byte, kind readKind) ([]byte, bool, error) {
	if s == nil {
		return nil, false, fmt.Errorf("nil RocksDB SMT store")
	}
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	return s.getCFWithReadOptionsOpen(s.columnFamily(cf), key, kind, s.readOpts)
}

// getCFWithReadOptionsOpen assumes s.closeMu is already held for reading.
func (s *Store) getCFWithReadOptionsOpen(cf *C.rocksdb_column_family_handle_t, key []byte, kind readKind, readOpts *C.rocksdb_readoptions_t) ([]byte, bool, error) {
	if s == nil {
		return nil, false, fmt.Errorf("nil RocksDB SMT store")
	}
	if s.closed || s.db == nil || cf == nil {
		return nil, false, fmt.Errorf("closed RocksDB SMT store")
	}
	if readOpts == nil {
		return nil, false, fmt.Errorf("nil RocksDB read options")
	}
	s.counters.pointReads.Add(1)
	switch kind {
	case readKindMeta:
		s.counters.metaPointReads.Add(1)
	case readKindNode:
		s.counters.nodePointReads.Add(1)
	case readKindOpen:
		s.counters.metaPointReads.Add(1)
		s.counters.openPointReads.Add(1)
	case readKindProof:
	}

	keyPtr, keyLen := bytesPointer(key)
	var valueLen C.size_t
	var errPtr *C.char
	value := C.rocksdb_get_cf(s.db, readOpts, cf, keyPtr, keyLen, &valueLen, &errPtr)
	if err := takeError(errPtr); err != nil {
		return nil, false, fmt.Errorf("rocksdb get %q: %w", string(key), err)
	}
	if value == nil {
		return nil, false, nil
	}
	defer C.rocksdb_free(unsafe.Pointer(value))
	if valueLen > C.size_t(maxCInt) {
		return nil, false, fmt.Errorf("rocksdb value for %q too large: %d bytes", string(key), uint64(valueLen))
	}
	return C.GoBytes(unsafe.Pointer(value), C.int(valueLen)), true, nil
}

func (s *Store) StoreProofResponses(responses []storage.ProofResponseWrite) error {
	if s == nil {
		return fmt.Errorf("nil RocksDB SMT store")
	}
	if len(responses) == 0 {
		return nil
	}
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed || s.db == nil || s.defaultCF == nil {
		return fmt.Errorf("closed RocksDB SMT store")
	}
	batch := C.rocksdb_writebatch_create()
	if batch == nil {
		return fmt.Errorf("create RocksDB proof response batch")
	}
	defer C.rocksdb_writebatch_destroy(batch)

	for i, response := range responses {
		key, err := proofResponseKey(response.StateID)
		if err != nil {
			return fmt.Errorf("encode proof response key %d: %w", i, err)
		}
		writeBatchPutCF(batch, s.defaultCF, key, response.Response)
	}

	var errPtr *C.char
	C.rocksdb_write(s.db, s.writeOpts, batch, &errPtr)
	if err := takeError(errPtr); err != nil {
		return fmt.Errorf("commit RocksDB proof responses: %w", err)
	}
	s.counters.batchSets.Add(int64(len(responses)))
	s.counters.batchesCommitted.Add(1)
	return nil
}

func (s *Store) GetProofResponse(stateID api.StateID) ([]byte, bool, error) {
	key, err := proofResponseKey(stateID)
	if err != nil {
		return nil, false, err
	}
	return s.getCF(columnFamilyDefault, key, readKindProof)
}

func (s *Store) propertyUintCF(name string) uint64 {
	if s == nil {
		return 0
	}
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed || s.db == nil || s.nodesCF == nil {
		return 0
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var out C.uint64_t
	if C.rocksdb_property_int_cf(s.db, s.nodesCF, cName, &out) != 0 {
		return 0
	}
	return uint64(out)
}

func (s *Store) propertyUint(name string) uint64 {
	if s == nil {
		return 0
	}
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed || s.db == nil {
		return 0
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var out C.uint64_t
	if C.rocksdb_property_int(s.db, cName, &out) != 0 {
		return 0
	}
	return uint64(out)
}

func (s *Store) propertyValueCF(name string) (string, bool) {
	if s == nil {
		return "", false
	}
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed || s.db == nil || s.nodesCF == nil {
		return "", false
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	value := C.rocksdb_property_value_cf(s.db, s.nodesCF, cName)
	if value == nil {
		return "", false
	}
	defer C.rocksdb_free(unsafe.Pointer(value))
	return C.GoString(value), true
}

func (s *Store) loadOrInitMetadata(readOnly bool) error {
	schema, ok, err := s.getCF(columnFamilyMeta, metaKey(metaSchemaVersion), readKindOpen)
	if err != nil {
		return err
	}
	if !ok {
		if readOnly {
			return fmt.Errorf("disk SMT metadata missing in read-only RocksDB store")
		}
		return s.initMetadata()
	}
	if string(schema) != SchemaVersion {
		return fmt.Errorf("unsupported disk SMT schema version %q, want %q", string(schema), SchemaVersion)
	}

	treeLayout, ok, err := s.getCF(columnFamilyMeta, metaKey(metaTreeLayout), readKindOpen)
	if err != nil {
		return err
	}
	if !ok || string(treeLayout) != TreeLayout {
		return fmt.Errorf("unsupported disk SMT tree layout %q, want %q", string(treeLayout), TreeLayout)
	}

	keyBits, ok, err := s.getCF(columnFamilyMeta, metaKey(metaKeyBits), readKindOpen)
	if err != nil {
		return err
	}
	if !ok || string(keyBits) != KeyBits {
		return fmt.Errorf("unsupported disk SMT key bits %q, want %q", string(keyBits), KeyBits)
	}

	rootBytes, ok, err := s.getCF(columnFamilyMeta, metaKey(metaRoot), readKindOpen)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("disk SMT root metadata missing")
	}
	root, err := disk.HashFromBytes(rootBytes)
	if err != nil {
		return fmt.Errorf("decode disk SMT root metadata: %w", err)
	}

	blockBytes, ok, err := s.getCF(columnFamilyMeta, metaKey(metaRootBlock), readKindOpen)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("disk SMT root block metadata missing")
	}
	if _, err := decodeBlockNumber(blockBytes); err != nil {
		return err
	}

	s.mu.Lock()
	s.root = root
	s.block = append([]byte(nil), blockBytes...)
	s.mu.Unlock()
	return nil
}

func (s *Store) initMetadata() error {
	root := disk.EmptyRootHash()
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed || s.db == nil || s.metaCF == nil {
		return fmt.Errorf("closed RocksDB SMT store")
	}
	batch := C.rocksdb_writebatch_create()
	if batch == nil {
		return fmt.Errorf("create RocksDB metadata batch")
	}
	defer C.rocksdb_writebatch_destroy(batch)

	entries := []struct {
		key   []byte
		value []byte
	}{
		{metaKey(metaSchemaVersion), []byte(SchemaVersion)},
		{metaKey(metaTreeLayout), []byte(TreeLayout)},
		{metaKey(metaKeyBits), []byte(KeyBits)},
		{metaKey(metaRoot), root[:]},
		{metaKey(metaRootBlock), []byte{}},
	}
	for _, entry := range entries {
		writeBatchPutCF(batch, s.metaCF, entry.key, entry.value)
	}
	var errPtr *C.char
	C.rocksdb_write(s.db, s.writeOpts, batch, &errPtr)
	if err := takeError(errPtr); err != nil {
		return fmt.Errorf("commit RocksDB SMT metadata: %w", err)
	}

	s.mu.Lock()
	s.root = root
	s.block = []byte{}
	s.mu.Unlock()
	return nil
}

func metaKey(name string) []byte {
	return []byte(name)
}

func nodeKey(key disk.NodeKey) []byte {
	return key.Bytes()
}

func proofResponseKey(stateID api.StateID) ([]byte, error) {
	return prefixedStateKey(proofResponseKeyPrefix, stateID)
}

func prefixedStateKey(prefix []byte, stateID api.StateID) ([]byte, error) {
	key, err := stateID.GetTreeKey()
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(prefix)+len(key))
	copy(out, prefix)
	copy(out[len(prefix):], key)
	return out, nil
}

func encodeBlockNumber(blockNumber *api.BigInt) []byte {
	if blockNumber == nil || blockNumber.Int == nil {
		return []byte{}
	}
	return []byte(blockNumber.String())
}

func decodeBlockNumber(data []byte) (*api.BigInt, error) {
	if len(data) == 0 {
		return nil, nil
	}
	blockNumber, err := api.NewBigIntFromString(string(data))
	if err != nil {
		return nil, fmt.Errorf("decode root block metadata: %w", err)
	}
	return blockNumber, nil
}

func takeError(errPtr *C.char) error {
	if errPtr == nil {
		return nil
	}
	defer C.rocksdb_free(unsafe.Pointer(errPtr))
	return errors.New(C.GoString(errPtr))
}

func cStringArray(values []string) ([]*C.char, func()) {
	out := make([]*C.char, len(values))
	for i, value := range values {
		out[i] = C.CString(value)
	}
	return out, func() {
		for _, value := range out {
			C.free(unsafe.Pointer(value))
		}
	}
}

func boolToUChar(value bool) C.uchar {
	if value {
		return 1
	}
	return 0
}

func boolToInt(value bool) C.int {
	if value {
		return 1
	}
	return 0
}

func bytesPointer(data []byte) (*C.char, C.size_t) {
	if len(data) == 0 {
		return nil, 0
	}
	return (*C.char)(unsafe.Pointer(&data[0])), C.size_t(len(data))
}

func writeBatchPutCF(batch *C.rocksdb_writebatch_t, cf *C.rocksdb_column_family_handle_t, key []byte, value []byte) {
	keyPtr, keyLen := bytesPointer(key)
	valuePtr, valueLen := bytesPointer(value)
	C.rocksdb_writebatch_put_cf(batch, cf, keyPtr, keyLen, valuePtr, valueLen)
}

func writeBatchDeleteCF(batch *C.rocksdb_writebatch_t, cf *C.rocksdb_column_family_handle_t, key []byte) {
	keyPtr, keyLen := bytesPointer(key)
	C.rocksdb_writebatch_delete_cf(batch, cf, keyPtr, keyLen)
}

func writeBatchPutEntriesCF(batch *C.rocksdb_writebatch_t, cf *C.rocksdb_column_family_handle_t, nodes []storage.NodeWrite) error {
	if len(nodes) > maxCInt {
		return fmt.Errorf("too many RocksDB batch put entries: %d", len(nodes))
	}
	keyPtrsMem, keyLensMem, keyBuf, valuePtrsMem, valueLensMem, valueBuf, err := encodeNodeEntriesForC(nodes)
	if err != nil {
		return err
	}
	defer C.free(keyPtrsMem)
	defer C.free(keyLensMem)
	defer C.free(keyBuf)
	defer C.free(valuePtrsMem)
	defer C.free(valueLensMem)
	defer C.free(valueBuf)

	C.go_rocksdb_writebatch_put_many_cf(
		batch,
		cf,
		C.int(len(nodes)),
		(**C.char)(keyPtrsMem),
		(*C.size_t)(keyLensMem),
		(**C.char)(valuePtrsMem),
		(*C.size_t)(valueLensMem),
	)
	return nil
}

func writeBatchDeleteManyCF(batch *C.rocksdb_writebatch_t, cf *C.rocksdb_column_family_handle_t, keys []disk.NodeKey) error {
	if len(keys) > maxCInt {
		return fmt.Errorf("too many RocksDB batch delete entries: %d", len(keys))
	}
	keyPtrsMem, keyLensMem, keyBuf, err := encodeNodeKeysForC(keys)
	if err != nil {
		return err
	}
	defer C.free(keyPtrsMem)
	defer C.free(keyLensMem)
	defer C.free(keyBuf)

	C.go_rocksdb_writebatch_delete_many_cf(
		batch,
		cf,
		C.int(len(keys)),
		(**C.char)(keyPtrsMem),
		(*C.size_t)(keyLensMem),
	)
	return nil
}

func encodeNodeEntriesForC(nodes []storage.NodeWrite) (unsafe.Pointer, unsafe.Pointer, unsafe.Pointer, unsafe.Pointer, unsafe.Pointer, unsafe.Pointer, error) {
	totalValueBytes := 0
	keys := make([]disk.NodeKey, len(nodes))
	for i, node := range nodes {
		keys[i] = node.Key
		totalValueBytes += len(node.Value)
	}

	keyPtrsMem, keyLensMem, keyBuf, err := encodeNodeKeysForC(keys)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	valuePtrsMem := C.malloc(C.size_t(len(nodes)) * C.size_t(unsafe.Sizeof(uintptr(0))))
	valueLensMem := C.malloc(C.size_t(len(nodes)) * C.size_t(unsafe.Sizeof(C.size_t(0))))
	valueBuf := C.malloc(C.size_t(totalValueBytes))
	if valuePtrsMem == nil || valueLensMem == nil || (totalValueBytes > 0 && valueBuf == nil) {
		C.free(keyPtrsMem)
		C.free(keyLensMem)
		C.free(keyBuf)
		C.free(valuePtrsMem)
		C.free(valueLensMem)
		C.free(valueBuf)
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("allocate RocksDB write value buffers")
	}
	valuePtrs := unsafe.Slice((*uintptr)(valuePtrsMem), len(nodes))
	valueLens := unsafe.Slice((*C.size_t)(valueLensMem), len(nodes))
	valueBytes := unsafe.Slice((*byte)(valueBuf), totalValueBytes)

	offset := 0
	for i, node := range nodes {
		valuePtrs[i] = 0
		if len(node.Value) > 0 {
			copy(valueBytes[offset:], node.Value)
			valuePtrs[i] = uintptr(unsafe.Add(valueBuf, offset))
		}
		valueLens[i] = C.size_t(len(node.Value))
		offset += len(node.Value)
	}
	return keyPtrsMem, keyLensMem, keyBuf, valuePtrsMem, valueLensMem, valueBuf, nil
}

func encodeNodeKeysForC(keys []disk.NodeKey) (unsafe.Pointer, unsafe.Pointer, unsafe.Pointer, error) {
	totalKeyBytes := 0
	encoded := make([][]byte, len(keys))
	for i, key := range keys {
		encoded[i] = nodeKey(key)
		totalKeyBytes += len(encoded[i])
	}

	keyPtrsMem := C.malloc(C.size_t(len(keys)) * C.size_t(unsafe.Sizeof(uintptr(0))))
	keyLensMem := C.malloc(C.size_t(len(keys)) * C.size_t(unsafe.Sizeof(C.size_t(0))))
	keyBuf := C.malloc(C.size_t(totalKeyBytes))
	if keyPtrsMem == nil || keyLensMem == nil || (totalKeyBytes > 0 && keyBuf == nil) {
		C.free(keyPtrsMem)
		C.free(keyLensMem)
		C.free(keyBuf)
		return nil, nil, nil, fmt.Errorf("allocate RocksDB write key buffers")
	}
	keyPtrs := unsafe.Slice((*uintptr)(keyPtrsMem), len(keys))
	keyLens := unsafe.Slice((*C.size_t)(keyLensMem), len(keys))
	keyBytes := unsafe.Slice((*byte)(keyBuf), totalKeyBytes)

	offset := 0
	for i, key := range encoded {
		copy(keyBytes[offset:], key)
		keyPtrs[i] = uintptr(unsafe.Add(keyBuf, offset))
		keyLens[i] = C.size_t(len(key))
		offset += len(key)
	}
	return keyPtrsMem, keyLensMem, keyBuf, nil
}
