package store

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestOpenInitializesFreshStoreMetadata(t *testing.T) {
	s := openTestStore(t, t.TempDir(), Options{})
	defer s.Close()

	state, err := s.CommittedState()
	require.NoError(t, err)
	require.Equal(t, disk.EmptyRootHash(), state.RootHash)
	require.Nil(t, state.BlockNumber)

	counters := s.Counters()
	require.Equal(t, int64(1), counters.OpenPointReads)
	require.Equal(t, int64(1), counters.MetaPointReads)
	require.Equal(t, int64(0), counters.NodePointReads)
	require.Equal(t, int64(0), counters.Iterators)
}

func TestOpenRejectsIncompatibleMetadata(t *testing.T) {
	tests := []struct {
		name        string
		metaName    string
		value       string
		wantMessage string
	}{
		{
			name:        "schema version",
			metaName:    metaSchemaVersion,
			value:       "2",
			wantMessage: "schema version",
		},
		{
			name:        "tree layout",
			metaName:    metaTreeLayout,
			value:       "wrong-layout",
			wantMessage: "tree layout",
		},
		{
			name:        "key bits",
			metaName:    metaKeyBits,
			value:       "128",
			wantMessage: "key bits",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s := openTestStore(t, dir, Options{})
			require.NoError(t, s.Close())

			db, err := pebble.Open(dir, nil)
			require.NoError(t, err)
			require.NoError(t, db.Set(metaKey(tt.metaName), []byte(tt.value), pebble.Sync))
			require.NoError(t, db.Close())

			_, err = Open(dir, Options{})
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantMessage)
		})
	}
}

func TestBatchWritesNodesAndMetadataAtomically(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, Options{})

	nodeKey := disk.RootNodeKey()
	nodeBytes := serializedTestLeaf(t, 0x01, []byte("value-one"))
	root := mustHash(t, "20563433422d651813394a07697b9c09f9c2ab2ddb95eaa8ed2dc3211de3e869")
	blockNumber := api.NewBigIntFromUint64(7)

	batch := s.NewBatch()
	require.NoError(t, batch.SetNode(nodeKey, nodeBytes))
	require.NoError(t, batch.SetCommittedState(root, blockNumber))
	require.NoError(t, batch.Commit())
	counters := s.Counters()
	require.Equal(t, int64(3), counters.BatchSets)
	require.Equal(t, int64(1), counters.NodeSets)
	require.Equal(t, int64(0), counters.NodeDeletes)
	require.Equal(t, int64(2), counters.MetaSets)
	require.Equal(t, int64(1), counters.BatchesCommitted)

	state, err := s.CommittedState()
	require.NoError(t, err)
	require.Equal(t, root, state.RootHash)
	require.NotNil(t, state.BlockNumber)
	require.Equal(t, blockNumber.String(), state.BlockNumber.String())

	got, ok, err := s.GetNode(nodeKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, nodeBytes, got)
	require.NoError(t, s.Close())

	reopened := openTestStore(t, dir, Options{})
	defer reopened.Close()
	reopenedState, err := reopened.CommittedState()
	require.NoError(t, err)
	require.Equal(t, root, reopenedState.RootHash)
	require.NotNil(t, reopenedState.BlockNumber)
	require.Equal(t, blockNumber.String(), reopenedState.BlockNumber.String())

	got, ok, err = reopened.GetNode(nodeKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, nodeBytes, got)
}

func TestReopenReadsOnlyFixedMetadataKeys(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, Options{})

	nodeKey := disk.RootNodeKey()
	nodeBytes := serializedTestLeaf(t, 0x02, []byte("value-two"))
	root := disk.HashLeaf(mustKeyWithFirstByte(t, 0x02), []byte("value-two"))
	batch := s.NewBatch()
	require.NoError(t, batch.SetNode(nodeKey, nodeBytes))
	require.NoError(t, batch.SetCommittedState(root, api.NewBigIntFromUint64(9)))
	require.NoError(t, batch.Commit())
	require.NoError(t, s.Close())

	reopened := openTestStore(t, dir, Options{})
	defer reopened.Close()

	counters := reopened.Counters()
	require.Equal(t, int64(5), counters.OpenPointReads)
	require.Equal(t, int64(5), counters.PointReads)
	require.Equal(t, int64(5), counters.MetaPointReads)
	require.Equal(t, int64(0), counters.NodePointReads)
	require.Equal(t, int64(0), counters.Iterators)

	got, ok, err := reopened.GetNode(nodeKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, nodeBytes, got)

	counters = reopened.Counters()
	require.Equal(t, int64(1), counters.NodePointReads)
	require.Equal(t, int64(6), counters.PointReads)
	require.Equal(t, int64(0), counters.Iterators)
}

func TestCheckpointCanOpenAsSeparateReadOnlyStore(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, Options{})
	defer s.Close()

	nodeKey := disk.RootNodeKey()
	nodeBytes := serializedTestLeaf(t, 0x03, []byte("value-three"))
	root := disk.HashLeaf(mustKeyWithFirstByte(t, 0x03), []byte("value-three"))
	batch := s.NewBatch()
	require.NoError(t, batch.SetNode(nodeKey, nodeBytes))
	require.NoError(t, batch.SetCommittedState(root, api.NewBigIntFromUint64(11)))
	require.NoError(t, batch.Commit())

	checkpointDir := filepath.Join(t.TempDir(), "checkpoint")
	require.NoError(t, s.Checkpoint(checkpointDir))
	require.Equal(t, int64(1), s.Counters().Checkpoints)

	checkpoint := openTestStore(t, checkpointDir, Options{ReadOnly: true})
	defer checkpoint.Close()

	state, err := checkpoint.CommittedState()
	require.NoError(t, err)
	require.Equal(t, root, state.RootHash)
	require.NotNil(t, state.BlockNumber)
	require.Equal(t, "11", state.BlockNumber.String())

	got, ok, err := checkpoint.GetNode(nodeKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, nodeBytes, got)

	got, ok, err = s.GetNode(nodeKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, nodeBytes, got)
}

func TestLexicographicUpperBound(t *testing.T) {
	require.Equal(t, []byte{0x02}, lexicographicUpperBound([]byte{0x01, 0xff}))
	require.Equal(t, []byte{0x6e, 0x30}, lexicographicUpperBound([]byte("n/\xff\xff")))
	require.Nil(t, lexicographicUpperBound([]byte{0xff, 0xff}))
}

func openTestStore(t *testing.T, dir string, opts Options) *Store {
	t.Helper()
	s, err := Open(dir, opts)
	require.NoError(t, err)
	return s
}

func serializedTestLeaf(t *testing.T, firstKeyByte byte, value []byte) []byte {
	t.Helper()
	key := mustKeyWithFirstByte(t, firstKeyByte)
	branch := disk.NewLeaf(key, value)
	encoded, err := disk.MarshalLeaf(branch.Leaf)
	require.NoError(t, err)
	return encoded
}

func mustKeyWithFirstByte(t *testing.T, firstByte byte) disk.Key {
	t.Helper()
	var keyBytes [disk.KeySize]byte
	keyBytes[0] = firstByte
	key, err := disk.KeyFromBytes(keyBytes[:])
	require.NoError(t, err)
	return key
}

func mustHash(t *testing.T, hexHash string) disk.Hash {
	t.Helper()
	data, err := api.NewHexBytesFromString(hexHash)
	require.NoError(t, err)
	hash, err := disk.HashFromBytes(data)
	require.NoError(t, err)
	return hash
}
