//go:build rocksdb

package rocksstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestBatchWritesNodesAndMetadataAtomically(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir, Options{DisableWAL: true, NoSyncWrites: true})

	nodeKey := disk.RootNodeKey()
	nodeBytes := []byte("serialized-node")
	root := disk.HashLeaf(mustKeyWithFirstByte(t, 0x01), []byte("value-one"))
	blockNumber := api.NewBigIntFromUint64(7)

	batch := store.NewBatch()
	require.NoError(t, batch.SetNode(nodeKey, nodeBytes))
	require.NoError(t, batch.SetCommittedState(root, blockNumber))
	require.NoError(t, batch.Commit())
	require.NoError(t, store.Close())

	reopened := openTestStore(t, dir, Options{DisableWAL: true, NoSyncWrites: true})
	defer reopened.Close()

	state, err := reopened.CommittedState()
	require.NoError(t, err)
	require.Equal(t, root, state.RootHash)
	require.NotNil(t, state.BlockNumber)
	require.Equal(t, blockNumber.String(), state.BlockNumber.String())

	got, ok, err := reopened.GetNode(nodeKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, nodeBytes, got)
}

func TestGetNodesUsesBatchedMultiGet(t *testing.T) {
	store := openTestStore(t, t.TempDir(), Options{DisableWAL: true, NoSyncWrites: true})
	defer store.Close()

	keyA := disk.RootNodeKey()
	keyB, err := disk.NewNodeKey(5, disk.PrefixBits{0x12})
	require.NoError(t, err)
	batch := store.NewBatch()
	require.NoError(t, batch.SetNode(keyA, []byte("a")))
	require.NoError(t, batch.SetNode(keyB, []byte("b")))
	require.NoError(t, batch.SetCommittedState(disk.HashLeaf(mustKeyWithFirstByte(t, 0x02), []byte("value-two")), api.NewBigIntFromUint64(2)))
	require.NoError(t, batch.Commit())

	before := store.Counters()
	results, err := store.GetNodes([]disk.NodeKey{keyA, keyB}, false)
	require.NoError(t, err)
	after := store.Counters()

	require.Len(t, results, 2)
	require.True(t, results[0].Found)
	require.Equal(t, []byte("a"), results[0].Value)
	require.True(t, results[1].Found)
	require.Equal(t, []byte("b"), results[1].Value)
	require.Equal(t, int64(2), after.NodePointReads-before.NodePointReads)
	require.Equal(t, int64(0), after.Iterators-before.Iterators)
}

func TestReadSnapshotSeesStableViewAndCloses(t *testing.T) {
	store := openTestStore(t, t.TempDir(), Options{DisableWAL: true, NoSyncWrites: true})
	defer store.Close()

	key := disk.RootNodeKey()
	require.NoError(t, writeSingleNode(store, key, []byte("before"), 1))

	baseline := store.NumSnapshots()
	reader, closeSnapshot, err := store.NewReadSnapshot()
	require.NoError(t, err)
	require.Equal(t, baseline+1, store.NumSnapshots())

	require.NoError(t, writeSingleNode(store, key, []byte("after"), 2))

	got, ok, err := reader.GetNode(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("before"), got)
	results, err := reader.GetNodes([]disk.NodeKey{key}, false)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.True(t, results[0].Found)
	require.Equal(t, []byte("before"), results[0].Value)

	closeSnapshot()
	require.Equal(t, baseline, store.NumSnapshots())

	got, ok, err = store.GetNode(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("after"), got)
}

func TestReadSnapshotOpenReadCloseLoopDoesNotLeakSnapshots(t *testing.T) {
	store := openTestStore(t, t.TempDir(), Options{DisableWAL: true, NoSyncWrites: true})
	defer store.Close()

	key := disk.RootNodeKey()
	require.NoError(t, writeSingleNode(store, key, []byte("value"), 1))
	baseline := store.NumSnapshots()

	for i := 0; i < 10_000; i++ {
		reader, closeSnapshot, err := store.NewReadSnapshot()
		require.NoError(t, err)
		got, ok, err := reader.GetNode(key)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []byte("value"), got)
		closeSnapshot()
	}
	require.Equal(t, baseline, store.NumSnapshots())
}

func TestCloseWaitsForOpenReadSnapshot(t *testing.T) {
	store := openTestStore(t, t.TempDir(), Options{DisableWAL: true, NoSyncWrites: true})

	_, closeSnapshot, err := store.NewReadSnapshot()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- store.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
		t.Fatal("store closed while a read snapshot was still open")
	case <-time.After(100 * time.Millisecond):
	}

	closeSnapshot()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("store close did not finish after read snapshot closed")
	}
}

func writeSingleNode(store *Store, key disk.NodeKey, value []byte, block uint64) error {
	batch := store.NewBatch()
	defer batch.Close()
	if err := batch.SetNode(key, value); err != nil {
		return err
	}
	root := disk.HashLeaf(keyWithFirstByte(byte(block)), value)
	if err := batch.SetCommittedState(root, api.NewBigIntFromUint64(block)); err != nil {
		return err
	}
	return batch.Commit()
}

func openTestStore(t *testing.T, dir string, opts Options) *Store {
	t.Helper()
	store, err := Open(dir, opts)
	require.NoError(t, err)
	return store
}

func mustKeyWithFirstByte(t *testing.T, firstByte byte) disk.Key {
	t.Helper()
	return keyWithFirstByte(firstByte)
}

func keyWithFirstByte(firstByte byte) disk.Key {
	var keyBytes [disk.KeySize]byte
	keyBytes[0] = firstByte
	key, err := disk.KeyFromBytes(keyBytes[:])
	if err != nil {
		panic(err)
	}
	return key
}
