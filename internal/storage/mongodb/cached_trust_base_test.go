package mongodb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

func TestCachedTrustBaseStorage(t *testing.T) {
	cachedStorage := NewCachedTrustBaseStorage(newTrustBaseStorage(t))
	tbs := createTrustBases(t, 3)

	t.Run("initially empty", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, cachedStorage.storage.collection.Drop(ctx))
		require.NoError(t, cachedStorage.ReloadCache(ctx))

		_, err := cachedStorage.GetByEpoch(ctx, 1)
		require.ErrorIs(t, err, interfaces.ErrTrustBaseNotFound)
	})

	t.Run("store and retrieve", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, cachedStorage.storage.collection.Drop(ctx))
		require.NoError(t, cachedStorage.ReloadCache(ctx))
		for _, tb := range tbs {
			require.NoError(t, cachedStorage.Store(t.Context(), tb))
		}

		// GetByEpoch 1 returns epoch 1
		tb, err := cachedStorage.GetByEpoch(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, tbs[0], tb)

		// GetByEpoch 2 returns epoch 2
		tb, err = cachedStorage.GetByEpoch(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, tbs[1], tb)

		// GetByEpoch 3 returns epoch 3
		tb, err = cachedStorage.GetByEpoch(ctx, 3)
		require.NoError(t, err)
		require.Equal(t, tbs[2], tb)

		// GetLatest returns epoch 3
		tb, err = cachedStorage.GetLatest(ctx)
		require.NoError(t, err)
		require.Equal(t, tbs[2], tb)

		// GetTrustBases from=1 to=2
		actualTbs, err := cachedStorage.GetTrustBases(ctx, 1, 2)
		require.NoError(t, err)
		require.Equal(t, tbs[:2], actualTbs)

		// GetTrustBases from=1 to=3
		actualTbs, err = cachedStorage.GetTrustBases(ctx, 1, 3)
		require.NoError(t, err)
		require.Equal(t, tbs[:3], actualTbs)

		// invalid GetTrustBases returns errors
		tbs, err = cachedStorage.GetTrustBases(ctx, 1, 4)
		require.ErrorIs(t, err, interfaces.ErrTrustBaseNotFound)
	})
}
