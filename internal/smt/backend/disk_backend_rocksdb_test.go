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
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

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
