package store

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
)

type Options struct {
	ReadOnly                 bool
	CacheSizeBytes           int64
	NoSyncWrites             bool
	DisableWAL               bool
	MaxConcurrentCompactions int
	MemTableSizeBytes        uint64
	BlockSizeBytes           int
	NoCompression            bool
}

type Store struct {
	db           *pebble.DB
	cache        *pebble.Cache
	writeOptions *pebble.WriteOptions

	mu    sync.RWMutex
	root  disk.Hash
	block []byte

	counters storeCounters
}

type Counters = storage.Counters
type NodeReadResult = storage.NodeReadResult
type Metrics = storage.Metrics

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
	pebbleOpts := &pebble.Options{
		ReadOnly:   opts.ReadOnly,
		DisableWAL: opts.DisableWAL,
	}
	if opts.MaxConcurrentCompactions > 0 {
		maxCompactions := opts.MaxConcurrentCompactions
		pebbleOpts.MaxConcurrentCompactions = func() int {
			return maxCompactions
		}
	}
	if opts.MemTableSizeBytes > 0 {
		pebbleOpts.MemTableSize = opts.MemTableSizeBytes
	}
	if opts.NoCompression || opts.BlockSizeBytes > 0 {
		pebbleOpts.Levels = make([]pebble.LevelOptions, 7)
		for i := range pebbleOpts.Levels {
			if opts.NoCompression {
				pebbleOpts.Levels[i].Compression = pebble.NoCompression
			}
			if opts.BlockSizeBytes > 0 {
				pebbleOpts.Levels[i].BlockSize = opts.BlockSizeBytes
				pebbleOpts.Levels[i].IndexBlockSize = opts.BlockSizeBytes
			}
		}
	}
	var cache *pebble.Cache
	if opts.CacheSizeBytes > 0 {
		cache = pebble.NewCache(opts.CacheSizeBytes)
		pebbleOpts.Cache = cache
	}
	db, err := pebble.Open(path, pebbleOpts)
	if err != nil {
		if cache != nil {
			cache.Unref()
		}
		return nil, fmt.Errorf("open pebble SMT store: %w", err)
	}

	writeOptions := pebble.Sync
	if opts.NoSyncWrites || opts.DisableWAL {
		writeOptions = pebble.NoSync
	}
	s := &Store{db: db, cache: cache, writeOptions: writeOptions}
	if err := s.loadOrInitMetadata(opts.ReadOnly); err != nil {
		_ = db.Close()
		if cache != nil {
			cache.Unref()
		}
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	if s.cache != nil {
		s.cache.Unref()
		s.cache = nil
	}
	return err
}

func (s *Store) CommittedState() (CommittedState, error) {
	if s == nil {
		return CommittedState{}, fmt.Errorf("nil SMT store")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	blockNumber, err := decodeBlockNumber(s.block)
	if err != nil {
		return CommittedState{}, err
	}
	return CommittedState{
		RootHash:    s.root,
		BlockNumber: blockNumber,
	}, nil
}

func (s *Store) GetNode(key disk.NodeKey) ([]byte, bool, error) {
	value, ok, err := s.get(nodeKey(key), readKindNode)
	if err != nil {
		return nil, false, err
	}
	return value, ok, nil
}

func (s *Store) GetNodes(keys []disk.NodeKey, useIterator bool) ([]NodeReadResult, error) {
	if !useIterator {
		results := make([]NodeReadResult, len(keys))
		for i, key := range keys {
			value, ok, err := s.GetNode(key)
			if err != nil {
				return nil, err
			}
			results[i] = NodeReadResult{Value: value, Found: ok}
		}
		return results, nil
	}
	return s.getNodesWithIterator(keys)
}

func (s *Store) getNodesWithIterator(keys []disk.NodeKey) ([]NodeReadResult, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("nil SMT store")
	}
	results := make([]NodeReadResult, len(keys))
	if len(keys) == 0 {
		return results, nil
	}

	lowerBound := appendNodeKey(nil, keys[0])
	upperBound := lexicographicUpperBound(appendNodeKey(nil, keys[len(keys)-1]))
	if upperBound == nil {
		upperBound = []byte("n0")
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("create SMT node iterator: %w", err)
	}
	s.counters.iterators.Add(1)
	defer iter.Close()

	encodedKey := make([]byte, 0, len(upperBound))
	for i, key := range keys {
		encodedKey = appendNodeKey(encodedKey[:0], key)
		s.counters.pointReads.Add(1)
		s.counters.nodePointReads.Add(1)
		if !iter.SeekGE(encodedKey) || !bytes.Equal(iter.Key(), encodedKey) {
			continue
		}
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebble iterator get %q: %w", string(encodedKey), err)
		}
		out := make([]byte, len(value))
		copy(out, value)
		results[i] = NodeReadResult{Value: out, Found: true}
	}
	return results, nil
}

func lexicographicUpperBound(key []byte) []byte {
	upper := append([]byte(nil), key...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] == 0xff {
			continue
		}
		upper[i]++
		return upper[:i+1]
	}
	return nil
}

func (s *Store) NewBatch() storage.Batch {
	return &Batch{
		store: s,
		batch: s.db.NewBatch(),
	}
}

func (s *Store) Checkpoint(destDir string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("nil SMT store")
	}
	if err := s.db.Checkpoint(destDir); err != nil {
		return fmt.Errorf("checkpoint pebble SMT store: %w", err)
	}
	s.counters.checkpoints.Add(1)
	return nil
}

func (s *Store) Flush() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("nil SMT store")
	}
	if err := s.db.Flush(); err != nil {
		return fmt.Errorf("flush Pebble SMT store: %w", err)
	}
	return nil
}

func (s *Store) CompactNodes() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("nil SMT store")
	}
	if err := s.db.Compact([]byte(nodePrefix), []byte("n0"), true); err != nil {
		return fmt.Errorf("compact Pebble SMT node keyspace: %w", err)
	}
	return nil
}

func (s *Store) Counters() Counters {
	if s == nil {
		return Counters{}
	}
	return Counters{
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

func (s *Store) Metrics() Metrics {
	if s == nil || s.db == nil {
		return Metrics{}
	}
	m := s.db.Metrics()
	return Metrics{
		BlockCacheSize:         m.BlockCache.Size,
		BlockCacheCount:        m.BlockCache.Count,
		BlockCacheHits:         m.BlockCache.Hits,
		BlockCacheMisses:       m.BlockCache.Misses,
		TableCacheSize:         m.TableCache.Size,
		TableCacheCount:        m.TableCache.Count,
		TableCacheHits:         m.TableCache.Hits,
		TableCacheMisses:       m.TableCache.Misses,
		CompactEstimatedDebt:   m.Compact.EstimatedDebt,
		CompactInProgressBytes: m.Compact.InProgressBytes,
		CompactNumInProgress:   m.Compact.NumInProgress,
		CompactCount:           m.Compact.Count,
		FlushCount:             m.Flush.Count,
		MemTableSize:           m.MemTable.Size,
		MemTableCount:          m.MemTable.Count,
		WALSize:                m.WAL.Size,
		WALFiles:               m.WAL.Files,
		L0NumFiles:             m.Levels[0].NumFiles,
		L0Sublevels:            m.Levels[0].Sublevels,
		L0Size:                 m.Levels[0].Size,
		L0Score:                m.Levels[0].Score,
	}
}

type readKind int

const (
	readKindMeta readKind = iota
	readKindNode
	readKindOpen
)

func (s *Store) get(key []byte, kind readKind) ([]byte, bool, error) {
	if s == nil || s.db == nil {
		return nil, false, fmt.Errorf("nil SMT store")
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
	}

	value, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("pebble get %q: %w", string(key), err)
	}
	defer closer.Close()

	out := make([]byte, len(value))
	copy(out, value)
	return out, true, nil
}

func (s *Store) loadOrInitMetadata(readOnly bool) error {
	schema, ok, err := s.get(metaKey(metaSchemaVersion), readKindOpen)
	if err != nil {
		return err
	}
	if !ok {
		if readOnly {
			return fmt.Errorf("disk SMT metadata missing in read-only store")
		}
		return s.initMetadata()
	}
	if string(schema) != SchemaVersion {
		return fmt.Errorf("unsupported disk SMT schema version %q, want %q", string(schema), SchemaVersion)
	}

	treeLayout, ok, err := s.get(metaKey(metaTreeLayout), readKindOpen)
	if err != nil {
		return err
	}
	if !ok || string(treeLayout) != TreeLayout {
		return fmt.Errorf("unsupported disk SMT tree layout %q, want %q", string(treeLayout), TreeLayout)
	}

	keyBits, ok, err := s.get(metaKey(metaKeyBits), readKindOpen)
	if err != nil {
		return err
	}
	if !ok || string(keyBits) != KeyBits {
		return fmt.Errorf("unsupported disk SMT key bits %q, want %q", string(keyBits), KeyBits)
	}

	rootBytes, ok, err := s.get(metaKey(metaRoot), readKindOpen)
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

	blockBytes, ok, err := s.get(metaKey(metaRootBlock), readKindOpen)
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
	batch := s.db.NewBatch()
	defer batch.Close()

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
		if err := batch.Set(entry.key, entry.value, nil); err != nil {
			return fmt.Errorf("initialize disk SMT metadata: %w", err)
		}
	}
	if err := batch.Commit(s.writeOptions); err != nil {
		return fmt.Errorf("commit disk SMT metadata: %w", err)
	}

	s.mu.Lock()
	s.root = root
	s.block = []byte{}
	s.mu.Unlock()
	return nil
}
