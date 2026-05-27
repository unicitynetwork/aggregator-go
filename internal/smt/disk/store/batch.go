package store

import (
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type Batch struct {
	store *Store
	batch *pebble.Batch

	committedRoot  *disk.Hash
	committedBlock []byte
	closed         bool
}

func (b *Batch) SetNode(key disk.NodeKey, value []byte) error {
	if b == nil || b.batch == nil {
		return fmt.Errorf("nil SMT store batch")
	}
	if b.closed {
		return fmt.Errorf("SMT store batch is closed")
	}
	if err := b.batch.Set(nodeKey(key), value, nil); err != nil {
		return fmt.Errorf("set SMT node: %w", err)
	}
	b.store.counters.batchSets.Add(1)
	b.store.counters.nodeSets.Add(1)
	return nil
}

func (b *Batch) DeleteNode(key disk.NodeKey) error {
	if b == nil || b.batch == nil {
		return fmt.Errorf("nil SMT store batch")
	}
	if b.closed {
		return fmt.Errorf("SMT store batch is closed")
	}
	if err := b.batch.Delete(nodeKey(key), nil); err != nil {
		return fmt.Errorf("delete SMT node: %w", err)
	}
	b.store.counters.batchSets.Add(1)
	b.store.counters.nodeDeletes.Add(1)
	return nil
}

func (b *Batch) SetCommittedState(root disk.Hash, blockNumber *api.BigInt) error {
	if b == nil || b.batch == nil {
		return fmt.Errorf("nil SMT store batch")
	}
	if b.closed {
		return fmt.Errorf("SMT store batch is closed")
	}
	blockBytes := encodeBlockNumber(blockNumber)
	if err := b.batch.Set(metaKey(metaRoot), root[:], nil); err != nil {
		return fmt.Errorf("set SMT root metadata: %w", err)
	}
	if err := b.batch.Set(metaKey(metaRootBlock), blockBytes, nil); err != nil {
		return fmt.Errorf("set SMT root block metadata: %w", err)
	}
	rootCopy := root
	b.committedRoot = &rootCopy
	b.committedBlock = append([]byte(nil), blockBytes...)
	b.store.counters.batchSets.Add(2)
	b.store.counters.metaSets.Add(2)
	return nil
}

func (b *Batch) Commit() error {
	if b == nil || b.batch == nil {
		return fmt.Errorf("nil SMT store batch")
	}
	if b.closed {
		return fmt.Errorf("SMT store batch is closed")
	}
	if err := b.batch.Commit(b.store.writeOptions); err != nil {
		b.closed = true
		if closeErr := b.batch.Close(); closeErr != nil {
			return fmt.Errorf("commit SMT store batch: %w; close failed: %v", err, closeErr)
		}
		return fmt.Errorf("commit SMT store batch: %w", err)
	}
	if err := b.batch.Close(); err != nil {
		return fmt.Errorf("close committed SMT store batch: %w", err)
	}
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
	return b.batch.Close()
}
