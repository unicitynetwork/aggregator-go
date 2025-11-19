package mongodb

import (
	"crypto"
	"testing"

	cryptolibp2p "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	cryptobft "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

func TestTrustBaseStorage_Store(t *testing.T) {
	storage := newTrustBaseStorage(t)

	// create trust base for epoch 0 and 1
	epoch0Nodes := newRootNodes(t, 3)
	epoch1Nodes := newRootNodes(t, 3)

	trustBaseEpoch0, err := types.NewTrustBase(types.NetworkLocal, epoch0Nodes,
		types.WithEpoch(0),
	)
	require.NoError(t, err)
	trustBaseEpoch0Hash, err := trustBaseEpoch0.Hash(crypto.SHA256)
	require.NoError(t, err)

	trustBaseEpoch1, err := types.NewTrustBase(types.NetworkLocal, epoch1Nodes,
		types.WithEpoch(1),
		types.WithEpochStart(1000),
		types.WithPreviousTrustBaseHash(trustBaseEpoch0Hash),
	)
	require.NoError(t, err)

	t.Run("should store valid trust bases", func(t *testing.T) {
		// store trust base epoch 0 and 1
		ctx := t.Context()
		require.NoError(t, storage.collection.Drop(ctx))
		require.NoError(t, storage.CreateIndexes(ctx))
		require.NoError(t, storage.Store(ctx, trustBaseEpoch0))
		require.NoError(t, storage.Store(ctx, trustBaseEpoch1))

		// verify it was stored correctly
		storedEpoch0, err := storage.GetByEpoch(ctx, 0)
		require.NoError(t, err)
		require.NotNil(t, storedEpoch0)
		require.Equal(t, trustBaseEpoch0, storedEpoch0)

		storedEpoch1, err := storage.GetByEpoch(ctx, 1)
		require.NoError(t, err)
		require.NotNil(t, storedEpoch1)
		require.Equal(t, trustBaseEpoch1, storedEpoch1)
	})

	t.Run("should fail to store a duplicate trust base", func(t *testing.T) {
		// store trust base epoch 0
		ctx := t.Context()
		require.NoError(t, storage.collection.Drop(ctx))
		require.NoError(t, storage.CreateIndexes(ctx))
		require.NoError(t, storage.Store(ctx, trustBaseEpoch0))

		// try to store it again
		err = storage.Store(ctx, trustBaseEpoch0)
		require.ErrorIs(t, err, interfaces.ErrTrustBaseAlreadyExists)
	})
}

func TestTrustBaseStorage_GetByEpoch(t *testing.T) {
	storage := newTrustBaseStorage(t)

	// create trust base for epoch 0 and 1
	epoch0Nodes := newRootNodes(t, 3)
	epoch1Nodes := newRootNodes(t, 3)
	trustBaseEpoch0, err := types.NewTrustBase(types.NetworkLocal, epoch0Nodes,
		types.WithEpoch(0),
	)
	require.NoError(t, err)

	trustBaseEpoch0Hash, err := trustBaseEpoch0.Hash(crypto.SHA256)
	require.NoError(t, err)

	trustBaseEpoch1, err := types.NewTrustBase(types.NetworkLocal, epoch1Nodes,
		types.WithEpoch(1),
		types.WithEpochStart(1000),
		types.WithPreviousTrustBaseHash(trustBaseEpoch0Hash),
	)
	require.NoError(t, err)

	t.Run("should retrieve a stored trust base by epoch", func(t *testing.T) {
		// empty state
		ctx := t.Context()
		require.NoError(t, storage.collection.Drop(ctx))
		require.NoError(t, storage.CreateIndexes(ctx))

		// store the trust base epoch 0
		require.NoError(t, storage.Store(ctx, trustBaseEpoch0))

		// retrieve it by epoch
		retrievedTrustBaseEpoch0, err := storage.GetByEpoch(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, trustBaseEpoch0, retrievedTrustBaseEpoch0)

		// store the trust base epoch 1
		require.NoError(t, storage.Store(ctx, trustBaseEpoch1))

		// retrieve it by epoch
		retrievedTrustBaseEpoch1, err := storage.GetByEpoch(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, trustBaseEpoch1, retrievedTrustBaseEpoch1)

		// epoch 0 still returns trust base 0
		retrievedTrustBaseEpoch0, err = storage.GetByEpoch(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, trustBaseEpoch0, retrievedTrustBaseEpoch0)
	})

	t.Run("should return ErrTrustBaseNotFound if trust base does not exist", func(t *testing.T) {
		// empty state
		ctx := t.Context()
		require.NoError(t, storage.collection.Drop(ctx))
		require.NoError(t, storage.CreateIndexes(ctx))
		require.NoError(t, storage.Store(ctx, trustBaseEpoch0))

		// try to retrieve a non-existent trust base
		retrievedTrustBase, err := storage.GetByEpoch(ctx, 1)
		require.ErrorIs(t, err, interfaces.ErrTrustBaseNotFound)
		require.Nil(t, retrievedTrustBase)
	})
}

func TestTrustBaseStorage_GetByRound(t *testing.T) {
	storage := newTrustBaseStorage(t)
	nodes := newRootNodes(t, 3)

	t.Run("should retrieve the correct trust base for a given round", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, storage.collection.Drop(ctx))
		require.NoError(t, storage.CreateIndexes(ctx))

		// create and store multiple trust bases
		trustBaseEpoch0, err := types.NewTrustBase(types.NetworkLocal, nodes,
			types.WithEpoch(0),
			types.WithEpochStart(0),
		)
		require.NoError(t, err)
		require.NoError(t, storage.Store(ctx, trustBaseEpoch0))

		trustBaseEpoch0Hash, err := trustBaseEpoch0.Hash(crypto.SHA256)
		require.NoError(t, err)
		trustBaseEpoch1, err := types.NewTrustBase(types.NetworkLocal, nodes,
			types.WithEpoch(1),
			types.WithEpochStart(100),
			types.WithPreviousTrustBaseHash(trustBaseEpoch0Hash),
		)
		require.NoError(t, err)
		require.NoError(t, storage.Store(ctx, trustBaseEpoch1))

		// round 50 should return epoch 0
		retrieved, err := storage.GetByRound(ctx, 50)
		require.NoError(t, err)
		require.Equal(t, trustBaseEpoch0, retrieved)

		// round 100 should return epoch 1
		retrieved, err = storage.GetByRound(ctx, 100)
		require.NoError(t, err)
		require.Equal(t, trustBaseEpoch1, retrieved)

		// round 150 should return epoch 1
		retrieved, err = storage.GetByRound(ctx, 150)
		require.NoError(t, err)
		require.Equal(t, trustBaseEpoch1, retrieved)
	})

	t.Run("should return ErrTrustBaseNotFound if not found", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, storage.collection.Drop(ctx))
		require.NoError(t, storage.CreateIndexes(ctx))

		// empty collection
		_, err := storage.GetByRound(ctx, uint64(100))
		require.ErrorIs(t, err, interfaces.ErrTrustBaseNotFound)

		// create trust base with non-zero epoch start
		trustBase, err := types.NewTrustBase(types.NetworkLocal, nodes,
			types.WithEpoch(0),
			types.WithEpochStart(100),
		)
		require.NoError(t, err)
		require.NoError(t, storage.Store(ctx, trustBase))

		// verify round before first epoch start returns error
		_, err = storage.GetByRound(ctx, uint64(99))
		require.ErrorIs(t, err, interfaces.ErrTrustBaseNotFound)
	})
}

func TestTrustBaseStorage_GetAll(t *testing.T) {
	storage := newTrustBaseStorage(t)

	t.Run("should return all stored trust bases", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, storage.collection.Drop(ctx))
		require.NoError(t, storage.CreateIndexes(ctx))

		// create multiple trust bases
		nodes := newRootNodes(t, 3)
		trustBase0, err := types.NewTrustBase(types.NetworkLocal, nodes,
			types.WithEpoch(0),
			types.WithEpochStart(0),
		)
		require.NoError(t, err)
		require.NoError(t, storage.Store(ctx, trustBase0))

		trustBase0Hash, err := trustBase0.Hash(crypto.SHA256)
		require.NoError(t, err)
		trustBase1, err := types.NewTrustBase(types.NetworkLocal, nodes,
			types.WithEpoch(1),
			types.WithEpochStart(100),
			types.WithPreviousTrustBaseHash(trustBase0Hash),
		)
		require.NoError(t, err)
		require.NoError(t, storage.Store(ctx, trustBase1))

		trustBase1Hash, err := trustBase1.Hash(crypto.SHA256)
		require.NoError(t, err)
		trustBase2, err := types.NewTrustBase(types.NetworkLocal, nodes,
			types.WithEpoch(2),
			types.WithEpochStart(200),
			types.WithPreviousTrustBaseHash(trustBase1Hash),
		)
		require.NoError(t, err)
		require.NoError(t, storage.Store(ctx, trustBase2))

		// retrieve all trust bases
		actualTrustBases, err := storage.GetAll(ctx)
		require.NoError(t, err)
		require.Len(t, actualTrustBases, 3)

		// verify the retrieved trust bases
		expectedTrustBases := []types.RootTrustBase{trustBase0, trustBase1, trustBase2}
		require.Equal(t, expectedTrustBases, actualTrustBases)
	})

	t.Run("should return empty slice if no trust bases are stored", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, storage.collection.Drop(ctx))
		require.NoError(t, storage.CreateIndexes(ctx))

		allTrustBases, err := storage.GetAll(ctx)
		require.NoError(t, err)
		require.Len(t, allTrustBases, 0)
		require.Empty(t, allTrustBases)
	})
}

func newTrustBaseStorage(t *testing.T) *TrustBaseStorage {
	db := setupTestDB(t)
	storage := NewTrustBaseStorage(db)
	require.NoError(t, storage.CreateIndexes(t.Context()))
	return storage
}

func newRootNodes(t *testing.T, n int) []*types.NodeInfo {
	var nodes []*types.NodeInfo
	for range n {
		signer, err := cryptobft.NewInMemorySecp256K1Signer()
		require.NoError(t, err)
		verifier, err := signer.Verifier()
		require.NoError(t, err)

		pubKeyBytes, err := verifier.MarshalPublicKey()
		require.NoError(t, err)
		pubKey, err := cryptolibp2p.UnmarshalSecp256k1PublicKey(pubKeyBytes)
		require.NoError(t, err)
		peerID, err := peer.IDFromPublicKey(pubKey)
		require.NoError(t, err)

		nodes = append(nodes, &types.NodeInfo{
			NodeID: peerID.String(),
			SigKey: pubKeyBytes,
			Stake:  1,
		})
	}
	return nodes
}
