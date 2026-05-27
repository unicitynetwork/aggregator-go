//go:build rocksdb

package rocksstore

import (
	"testing"

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

func openTestStore(t *testing.T, dir string, opts Options) *Store {
	t.Helper()
	store, err := Open(dir, opts)
	require.NoError(t, err)
	return store
}

func mustKeyWithFirstByte(t *testing.T, firstByte byte) disk.Key {
	t.Helper()
	var keyBytes [disk.KeySize]byte
	keyBytes[0] = firstByte
	key, err := disk.KeyFromBytes(keyBytes[:])
	require.NoError(t, err)
	return key
}
