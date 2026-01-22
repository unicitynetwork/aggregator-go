package mongodb

import (
	"crypto"
	"testing"

	cryptolibp2p "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	cryptobft "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"
	"github.com/unicitynetwork/bft-go-base/types/hex"

	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

func TestTrustBaseStorage_StoreAndGetByEpoch(t *testing.T) {
	storage := newTrustBaseStorage(t)
	tbs := createTrustBases(t, 2)
	for _, tb := range tbs {
		require.NoError(t, storage.Store(t.Context(), tb))
	}

	t.Run("should retrieve a stored trust base by epoch", func(t *testing.T) {
		// retrieve tb1
		retrievedTrustBaseEpoch1, err := storage.GetByEpoch(t.Context(), 1)
		require.NoError(t, err)
		require.Equal(t, tbs[0], retrievedTrustBaseEpoch1)

		// retrieve tb2
		retrievedTrustBaseEpoch2, err := storage.GetByEpoch(t.Context(), 2)
		require.NoError(t, err)
		require.Equal(t, tbs[1], retrievedTrustBaseEpoch2)
	})

	t.Run("should return ErrTrustBaseNotFound if trust base does not exist", func(t *testing.T) {
		// try to retrieve a non-existent trust base
		retrievedTrustBase, err := storage.GetByEpoch(t.Context(), 3)
		require.ErrorIs(t, err, interfaces.ErrTrustBaseNotFound)
		require.Nil(t, retrievedTrustBase)
	})
}

func TestTrustBaseStorage_GetByRound(t *testing.T) {
	storage := newTrustBaseStorage(t)
	tbs := createTrustBases(t, 2)
	for _, tb := range tbs {
		require.NoError(t, storage.Store(t.Context(), tb))
	}

	t.Run("should retrieve the correct trust base for a given round", func(t *testing.T) {
		// round 101 should return epoch 1
		retrieved, err := storage.GetByRound(t.Context(), 101)
		require.NoError(t, err)
		require.Equal(t, tbs[0], retrieved)

		// round 201 should return epoch 2
		retrieved, err = storage.GetByRound(t.Context(), 201)
		require.NoError(t, err)
		require.Equal(t, tbs[1], retrieved)

		// round 250 should also return epoch 2
		retrieved, err = storage.GetByRound(t.Context(), 250)
		require.NoError(t, err)
		require.Equal(t, tbs[1], retrieved)
	})

	t.Run("should return ErrTrustBaseNotFound if not found", func(t *testing.T) {
		// verify round before first epoch start returns error
		_, err := storage.GetByRound(t.Context(), uint64(99))
		require.ErrorIs(t, err, interfaces.ErrTrustBaseNotFound)
	})
}

func TestTrustBaseStorage_GetAll(t *testing.T) {
	storage := newTrustBaseStorage(t)
	tbs := createTrustBases(t, 3)
	for _, tb := range tbs {
		require.NoError(t, storage.Store(t.Context(), tb))
	}

	t.Run("should return all stored trust bases", func(t *testing.T) {
		// retrieve all trust bases
		actualTrustBases, err := storage.GetAll(t.Context())
		require.NoError(t, err)
		require.Len(t, actualTrustBases, 3)

		// verify the retrieved trust bases
		require.Equal(t, tbs, actualTrustBases)
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

func TestTrustBaseStorage_GetLatest(t *testing.T) {
	storage := newTrustBaseStorage(t)
	tbs := createTrustBases(t, 3)
	for _, tb := range tbs {
		require.NoError(t, storage.Store(t.Context(), tb))
	}

	t.Run("should return latest", func(t *testing.T) {
		// retrieve all trust bases
		latestTrustBase, err := storage.GetLatest(t.Context())
		require.NoError(t, err)
		require.Equal(t, tbs[2], latestTrustBase)
	})
}

func createTrustBases(t *testing.T, count int) []*types.RootTrustBaseV1 {
	// create multiple trust bases
	var tbs []*types.RootTrustBaseV1
	var prevHash hex.Bytes
	for i := 0; i < count; i++ {
		epoch := uint64(i + 1)

		nodes := newRootNodes(t, 3)
		tb, err := types.NewTrustBase(types.NetworkLocal, nodes,
			types.WithEpoch(epoch),
			types.WithEpochStart(epoch+100),
			types.WithPreviousTrustBaseHash(prevHash),
		)
		require.NoError(t, err)
		tbs = append(tbs, tb)

		prevHash, err = tb.Hash(crypto.SHA256)
		require.NoError(t, err)
	}

	return tbs
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
