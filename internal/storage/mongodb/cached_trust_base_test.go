package mongodb

import (
	"crypto"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

func TestCachedTrustBaseStorage(t *testing.T) {
	cachedStorage := NewCachedTrustBaseStorage(newTrustBaseStorage(t))

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

	t.Run("initially empty", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, cachedStorage.storage.collection.Drop(ctx))
		require.NoError(t, cachedStorage.UpdateCache(ctx))

		_, err := cachedStorage.GetByEpoch(ctx, 0)
		require.ErrorIs(t, err, ErrTrustBaseNotFound)

		_, err = cachedStorage.GetByRound(ctx, 0)
		require.ErrorIs(t, err, ErrTrustBaseNotFound)

		all, err := cachedStorage.GetAll(ctx)
		require.NoError(t, err)
		require.Empty(t, all)
	})

	t.Run("store and retrieve", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, cachedStorage.storage.collection.Drop(ctx))
		require.NoError(t, cachedStorage.UpdateCache(ctx))

		// store epoch 0
		require.NoError(t, cachedStorage.Store(ctx, trustBaseEpoch0))

		// GetByEpoch 0 returns epoch 0
		tb, err := cachedStorage.GetByEpoch(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, trustBaseEpoch0, tb)

		// GetByRound (any value) returns epoch 0
		tb, err = cachedStorage.GetByRound(ctx, 9999)
		require.NoError(t, err)
		require.Equal(t, trustBaseEpoch0, tb)

		// GetAll returns only epoch 0
		all, err := cachedStorage.GetAll(ctx)
		require.NoError(t, err)
		require.Equal(t, []types.RootTrustBase{trustBaseEpoch0}, all)

		// store epoch 1
		require.NoError(t, cachedStorage.Store(ctx, trustBaseEpoch1))

		// GetByEpoch 1 returns epoch 1
		tb, err = cachedStorage.GetByEpoch(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, trustBaseEpoch1, tb)

		// GetByRound for round >= 1000 returns epoch 1
		tb, err = cachedStorage.GetByRound(ctx, 1000)
		require.NoError(t, err)
		require.Equal(t, trustBaseEpoch1, tb)

		// GetByRound for round < 1000 returns epoch 0
		tb, err = cachedStorage.GetByRound(ctx, 999)
		require.NoError(t, err)
		require.Equal(t, trustBaseEpoch0, tb)

		// GetAll returns both epochs
		all, err = cachedStorage.GetAll(ctx)
		require.NoError(t, err)
		require.Equal(t, []types.RootTrustBase{trustBaseEpoch0, trustBaseEpoch1}, all)
	})
}
