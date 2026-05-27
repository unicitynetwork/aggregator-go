//go:build rocksdb

package rocksstore

/*
#include <stdlib.h>
#include <rocksdb/c.h>
*/
import "C"

import (
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type Batch struct {
	store *Store
	batch *C.rocksdb_writebatch_t

	committedRoot  *disk.Hash
	committedBlock []byte
	closed         bool
}

func (b *Batch) SetNode(key disk.NodeKey, value []byte) error {
	if b == nil || b.batch == nil {
		return fmt.Errorf("nil RocksDB SMT store batch")
	}
	if b.closed {
		return fmt.Errorf("RocksDB SMT store batch is closed")
	}
	writeBatchPutCF(b.batch, b.store.nodesCF, nodeKey(key), value)
	b.store.counters.batchSets.Add(1)
	b.store.counters.nodeSets.Add(1)
	return nil
}

func (b *Batch) SetNodes(nodes map[disk.NodeKey][]byte) error {
	if b == nil || b.batch == nil {
		return fmt.Errorf("nil RocksDB SMT store batch")
	}
	if b.closed {
		return fmt.Errorf("RocksDB SMT store batch is closed")
	}
	if len(nodes) == 0 {
		return nil
	}
	writeBatchPutManyCF(b.batch, b.store.nodesCF, nodes)
	count := int64(len(nodes))
	b.store.counters.batchSets.Add(count)
	b.store.counters.nodeSets.Add(count)
	return nil
}

func (b *Batch) SetNodeEntries(nodes []storage.NodeWrite) error {
	if b == nil || b.batch == nil {
		return fmt.Errorf("nil RocksDB SMT store batch")
	}
	if b.closed {
		return fmt.Errorf("RocksDB SMT store batch is closed")
	}
	if len(nodes) == 0 {
		return nil
	}
	writeBatchPutEntriesCF(b.batch, b.store.nodesCF, nodes)
	count := int64(len(nodes))
	b.store.counters.batchSets.Add(count)
	b.store.counters.nodeSets.Add(count)
	return nil
}

func (b *Batch) DeleteNode(key disk.NodeKey) error {
	if b == nil || b.batch == nil {
		return fmt.Errorf("nil RocksDB SMT store batch")
	}
	if b.closed {
		return fmt.Errorf("RocksDB SMT store batch is closed")
	}
	writeBatchDeleteCF(b.batch, b.store.nodesCF, nodeKey(key))
	b.store.counters.batchSets.Add(1)
	b.store.counters.nodeDeletes.Add(1)
	return nil
}

func (b *Batch) DeleteNodes(keys []disk.NodeKey) error {
	if b == nil || b.batch == nil {
		return fmt.Errorf("nil RocksDB SMT store batch")
	}
	if b.closed {
		return fmt.Errorf("RocksDB SMT store batch is closed")
	}
	if len(keys) == 0 {
		return nil
	}
	writeBatchDeleteManyCF(b.batch, b.store.nodesCF, keys)
	count := int64(len(keys))
	b.store.counters.batchSets.Add(count)
	b.store.counters.nodeDeletes.Add(count)
	return nil
}

func (b *Batch) SetCommittedState(root disk.Hash, blockNumber *api.BigInt) error {
	if b == nil || b.batch == nil {
		return fmt.Errorf("nil RocksDB SMT store batch")
	}
	if b.closed {
		return fmt.Errorf("RocksDB SMT store batch is closed")
	}
	blockBytes := encodeBlockNumber(blockNumber)
	writeBatchPutCF(b.batch, b.store.metaCF, metaKey(metaRoot), root[:])
	writeBatchPutCF(b.batch, b.store.metaCF, metaKey(metaRootBlock), blockBytes)
	rootCopy := root
	b.committedRoot = &rootCopy
	b.committedBlock = append([]byte(nil), blockBytes...)
	b.store.counters.batchSets.Add(2)
	b.store.counters.metaSets.Add(2)
	return nil
}

func (b *Batch) Commit() error {
	if b == nil || b.batch == nil {
		return fmt.Errorf("nil RocksDB SMT store batch")
	}
	if b.closed {
		return fmt.Errorf("RocksDB SMT store batch is closed")
	}
	var errPtr *C.char
	C.rocksdb_write(b.store.db, b.store.writeOpts, b.batch, &errPtr)
	if err := takeError(errPtr); err != nil {
		b.closed = true
		C.rocksdb_writebatch_destroy(b.batch)
		b.batch = nil
		return fmt.Errorf("commit RocksDB SMT store batch: %w", err)
	}
	C.rocksdb_writebatch_destroy(b.batch)
	b.batch = nil
	b.closed = true
	b.store.counters.batchesCommitted.Add(1)
	if b.committedRoot != nil {
		b.store.mu.Lock()
		b.store.root = *b.committedRoot
		b.store.block = append([]byte(nil), b.committedBlock...)
		b.store.mu.Unlock()
	}
	return nil
}

func (b *Batch) Close() error {
	if b == nil || b.batch == nil || b.closed {
		return nil
	}
	b.closed = true
	C.rocksdb_writebatch_destroy(b.batch)
	b.batch = nil
	return nil
}
