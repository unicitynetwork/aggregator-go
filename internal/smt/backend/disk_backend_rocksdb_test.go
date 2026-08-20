//go:build rocksdb

package backend

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestDiskBackendRocksDBNodeKeyFormatLifecycleParity(t *testing.T) {
	batches := [][]LeafInput{
		{
			diskLifecycleLeaf(0x00, 0x00, 0x10, 11),
			diskLifecycleLeaf(0x00, 0x00, 0x80, 12),
			diskLifecycleLeaf(0x80, 0x00, 0x01, 13),
		},
		{
			diskLifecycleLeaf(0x00, 0x00, 0x11, 21),
			diskLifecycleLeaf(0x00, 0x00, 0x12, 22),
			diskLifecycleLeaf(0x40, 0x80, 0x01, 23),
		},
		{
			diskLifecycleLeaf(0x00, 0x00, 0x13, 31),
			diskLifecycleLeaf(0xc0, 0x40, 0x01, 32),
			diskLifecycleLeaf(0x20, 0x20, 0x01, 33),
		},
	}

	results := make(map[rocksstore.NodeKeyFormat]diskLifecycleResult, 2)
	for _, format := range []rocksstore.NodeKeyFormat{
		rocksstore.NodeKeyFormatDepthMajor,
		rocksstore.NodeKeyFormatPrefixMajor,
	} {
		t.Run(string(format), func(t *testing.T) {
			results[format] = runDiskBackendRocksDBLifecycle(t, format, batches)
		})
	}

	require.Len(t, results, 2)
	depth := results[rocksstore.NodeKeyFormatDepthMajor]
	prefix := results[rocksstore.NodeKeyFormatPrefixMajor]
	require.Equal(t, depth.roots, prefix.roots)
	require.Equal(t, depth.finalState.BlockNumber.String(), prefix.finalState.BlockNumber.String())
	require.Equal(t, depth.finalState.RootHash, prefix.finalState.RootHash)
}

type diskLifecycleResult struct {
	roots      [][]byte
	finalState CommittedState
}

func runDiskBackendRocksDBLifecycle(
	t *testing.T,
	format rocksstore.NodeKeyFormat,
	batches [][]LeafInput,
) diskLifecycleResult {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	allLeaves := make([]LeafInput, 0)
	roots := make([][]byte, 0, len(batches))

	backend := openDiskLifecycleBackend(t, dir, format)
	for block, batch := range batches[:2] {
		allLeaves = append(allLeaves, batch...)
		root := commitDiskLifecycleBatch(t, ctx, backend, uint64(block+1), batch, allLeaves)
		roots = append(roots, root)
		verifyDiskLifecycleProofs(t, ctx, backend, root, allLeaves)
	}
	require.NoError(t, backend.Close())

	wrongFormat := rocksstore.NodeKeyFormatPrefixMajor
	if format == rocksstore.NodeKeyFormatPrefixMajor {
		wrongFormat = rocksstore.NodeKeyFormatDepthMajor
	}
	_, err := rocksstore.Open(dir, diskLifecycleStoreOptions(wrongFormat))
	require.Error(t, err)
	require.True(t, rocksstore.IsLayoutMismatch(err), "got %v", err)

	backend = openDiskLifecycleBackend(t, dir, format)
	state, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.BlockNumber.Uint64())
	require.Equal(t, roots[1], state.RootHash)
	verifyDiskLifecycleProofs(t, ctx, backend, roots[1], allLeaves)

	allLeaves = append(allLeaves, batches[2]...)
	root := commitDiskLifecycleBatch(t, ctx, backend, 3, batches[2], allLeaves)
	roots = append(roots, root)
	verifyDiskLifecycleProofs(t, ctx, backend, root, allLeaves)
	require.NoError(t, backend.Close())

	backend = openDiskLifecycleBackend(t, dir, format)
	defer func() { require.NoError(t, backend.Close()) }()
	finalState, err := backend.CommittedState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), finalState.BlockNumber.Uint64())
	require.Equal(t, roots[2], finalState.RootHash)
	verifyDiskLifecycleProofs(t, ctx, backend, roots[2], allLeaves)

	return diskLifecycleResult{
		roots:      roots,
		finalState: finalState,
	}
}

func commitDiskLifecycleBatch(
	t *testing.T,
	ctx context.Context,
	backend *DiskBackend,
	block uint64,
	batch []LeafInput,
	allLeaves []LeafInput,
) []byte {
	t.Helper()
	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, batch)
	require.NoError(t, err)
	require.NoError(t, result.ValidateAllAccepted(len(batch)))

	expectedRoot := legacyRoot(t, allLeaves)
	require.Equal(t, expectedRoot, result.CandidateRoot)
	require.NoError(t, snapshot.Commit(ctx, CommitMetadata{
		BlockNumber: api.NewBigIntFromUint64(block),
		RootHash:    expectedRoot,
	}))
	require.NoError(t, backend.RefreshPublishedProofView(ctx, expectedRoot))

	return append([]byte(nil), expectedRoot...)
}

func verifyDiskLifecycleProofs(
	t *testing.T,
	ctx context.Context,
	backend *DiskBackend,
	root []byte,
	leaves []LeafInput,
) {
	t.Helper()
	actualRoot, err := backend.RootHashRaw(ctx)
	require.NoError(t, err)
	require.Equal(t, root, actualRoot)

	publishedRoot, err := backend.PublishedRoot(ctx)
	require.NoError(t, err)
	require.Equal(t, root, publishedRoot)

	keys := make([][]byte, len(leaves))
	for i, leaf := range leaves {
		keys[len(leaves)-1-i] = leaf.Key

		cert, err := backend.GetInclusionCert(ctx, leaf.Key)
		require.NoError(t, err)
		require.NoError(t, cert.Verify(leaf.Key, leaf.Value, root, api.InclusionProofV2HashAlgorithm))

		publishedCert, err := backend.GetPublishedInclusionCertAtRoot(ctx, publishedRoot, leaf.Key)
		require.NoError(t, err)
		require.NoError(t, publishedCert.Verify(leaf.Key, leaf.Value, root, api.InclusionProofV2HashAlgorithm))
	}

	certs, err := backend.GetInclusionCerts(ctx, keys)
	require.NoError(t, err)
	require.Len(t, certs, len(keys))
	for i, cert := range certs {
		leaf := leaves[len(leaves)-1-i]
		require.NoError(t, cert.Verify(leaf.Key, leaf.Value, root, api.InclusionProofV2HashAlgorithm))
	}
}

func openDiskLifecycleBackend(t *testing.T, dir string, format rocksstore.NodeKeyFormat) *DiskBackend {
	t.Helper()
	store, err := rocksstore.Open(dir, diskLifecycleStoreOptions(format))
	require.NoError(t, err)
	backend, err := NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	return backend
}

func diskLifecycleStoreOptions(format rocksstore.NodeKeyFormat) rocksstore.Options {
	return rocksstore.Options{
		DisableWAL:    true,
		NoSyncWrites:  true,
		NodeKeyFormat: format,
	}
}

func diskLifecycleLeaf(first, middle, last, value byte) LeafInput {
	key := make([]byte, api.StateTreeKeyLengthBytes)
	key[0] = first
	key[len(key)/2] = middle
	key[len(key)-1] = last
	val := make([]byte, 32)
	val[len(val)-1] = value
	return LeafInput{Key: key, Value: val}
}

func TestDiskBackendRocksDBRootParity(t *testing.T) {
	ctx := context.Background()
	inputs := []LeafInput{
		testLeafInput(1, 11),
		testLeafInput(2, 22),
		testLeafInput(3, 33),
	}
	expectedRoot := legacyRoot(t, inputs)

	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{
		DisableWAL:   true,
		NoSyncWrites: true,
	})
	require.NoError(t, err)
	backend, err := NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close()) }()

	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, inputs)
	require.NoError(t, err)
	require.NoError(t, result.ValidateAllAccepted(len(inputs)))
	require.Equal(t, expectedRoot, result.CandidateRoot)

	require.NoError(t, snapshot.Commit(ctx, CommitMetadata{
		BlockNumber: api.NewBigIntFromUint64(7),
		RootHash:    expectedRoot,
	}))

	actualRoot, err := backend.RootHashRaw(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedRoot, actualRoot)
}

func TestDiskBackendRocksDBInclusionCertUsesAndClosesSnapshot(t *testing.T) {
	ctx := context.Background()
	inputs := []LeafInput{
		testLeafInput(1, 11),
		testLeafInput(2, 22),
		testLeafInput(3, 33),
	}
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	wrapped := &countingSnapshotStore{Store: store, snapshotter: store}
	backend, err := NewDiskBackend(wrapped, persist.DefaultOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close()) }()

	snapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	result, err := snapshot.AddLeavesClassified(ctx, inputs)
	require.NoError(t, err)
	require.NoError(t, snapshot.Commit(ctx, CommitMetadata{
		BlockNumber: api.NewBigIntFromUint64(1),
		RootHash:    result.CandidateRoot,
	}))

	baseline := store.NumSnapshots()
	cert, err := backend.GetInclusionCert(ctx, inputs[1].Key)
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.Equal(t, baseline, store.NumSnapshots())
	require.Equal(t, int64(1), wrapped.snapshots.Load())

	missing := testLeafInput(99, 99)
	cert, err = backend.GetInclusionCert(ctx, missing.Key)
	require.Error(t, err)
	require.Nil(t, cert)
	require.Equal(t, baseline, store.NumSnapshots())
	require.Equal(t, int64(2), wrapped.snapshots.Load())
}

func TestDiskBackendRocksDBEmptyRootDoesNotOpenSnapshot(t *testing.T) {
	ctx := context.Background()
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	wrapped := &countingSnapshotStore{Store: store, snapshotter: store}
	backend, err := NewDiskBackend(wrapped, persist.DefaultOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close()) }()

	cert, err := backend.GetInclusionCert(ctx, testLeafInput(1, 1).Key)
	require.Error(t, err)
	require.Nil(t, cert)
	require.Zero(t, wrapped.snapshots.Load())
}

func TestDiskBackendRocksDBPrecomputedProofResponsesRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	backend, err := NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close()) }()

	stateID := api.ImprintV2(bytesOf(32, 9))
	expected := &api.GetInclusionProofResponseV2{
		BlockNumber: 9,
		InclusionProof: &api.InclusionProofV2{
			Version: 1,
			CertificationData: &api.CertificationData{
				Version:         1,
				TransactionHash: api.TransactionHash(bytesOf(32, 7)),
				Timeout:         testutil.RequestTimeout(),
			},
			CertificateBytes:   []byte{1, 2, 3},
			UnicityCertificate: []byte{0x43, 4, 5, 6},
		},
	}
	require.NoError(t, backend.StorePrecomputedProofResponses(ctx, []PrecomputedProofResponse{{
		StateID:  stateID,
		Response: expected,
	}}))

	actual, found, err := backend.GetPrecomputedProofResponse(ctx, stateID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, expected, actual)

	missing, found, err := backend.GetPrecomputedProofResponse(ctx, api.ImprintV2(bytesOf(32, 10)))
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, missing)
}

func TestRocksDBSnapshotProofSurvivesMultipleCommits(t *testing.T) {
	ctx := context.Background()
	firstInputs := []LeafInput{
		testLeafInput(1, 11),
		testLeafInput(2, 22),
		testLeafInput(3, 33),
	}
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	backend, err := NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close()) }()

	firstSnapshot, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	firstResult, err := firstSnapshot.AddLeavesClassified(ctx, firstInputs)
	require.NoError(t, err)
	require.NoError(t, firstSnapshot.Commit(ctx, CommitMetadata{
		BlockNumber: api.NewBigIntFromUint64(1),
		RootHash:    firstResult.CandidateRoot,
	}))
	rootOne, err := disk.HashFromBytes(firstResult.CandidateRoot)
	require.NoError(t, err)

	reader, closeSnapshot, err := store.NewReadSnapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), store.NumSnapshots())

	for block := uint64(2); block <= 4; block++ {
		nextSnapshot, err := backend.CreateSnapshot(ctx)
		require.NoError(t, err)
		leaf := testLeafInput(byte(block+10), byte(block+100))
		result, err := nextSnapshot.AddLeavesClassified(ctx, []LeafInput{leaf})
		require.NoError(t, err)
		require.NoError(t, nextSnapshot.Commit(ctx, CommitMetadata{
			BlockNumber: api.NewBigIntFromUint64(block),
			RootHash:    result.CandidateRoot,
		}))
	}

	cert, err := persist.BuildInclusionCert(rootOne, reader, firstInputs[1].Key)
	require.NoError(t, err)
	require.NoError(t, cert.Verify(firstInputs[1].Key, firstInputs[1].Value, firstResult.CandidateRoot, api.InclusionProofV2HashAlgorithm))
	closeSnapshot()
	require.Zero(t, store.NumSnapshots())
}

type countingSnapshotStore struct {
	storage.Store
	snapshotter storage.ReadSnapshotter
	snapshots   atomic.Int64
}

func (s *countingSnapshotStore) NewReadSnapshot() (storage.ReadStore, func(), error) {
	s.snapshots.Add(1)
	return s.snapshotter.NewReadSnapshot()
}

func bytesOf(length int, value byte) []byte {
	out := make([]byte, length)
	for i := range out {
		out[i] = value
	}
	return out
}
