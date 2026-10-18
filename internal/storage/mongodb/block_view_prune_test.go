package mongodb

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// viewBlock is a block of the given number with its own UC and (optionally) view bytes.
func viewBlock(n int64, uc string, view string) *models.Block {
	b := createTestBlock(api.NewBigInt(big.NewInt(n)))
	b.UnicityCertificate = api.NewHexBytes([]byte(uc))
	if view != "" {
		b.PartitionView = api.NewHexBytes([]byte(view))
	}
	return b
}

func finalizeWithView(t *testing.T, bs *BlockStorage, b *models.Block) {
	t.Helper()
	ctx := context.Background()
	b.Finalized = false
	require.NoError(t, bs.Store(ctx, b))
	require.NoError(t, bs.SetFinalizedWithCertificate(ctx, b))
}

func viewOf(t *testing.T, bs *BlockStorage, n int64) api.HexBytes {
	t.Helper()
	b, err := bs.GetByNumberAnyFinalization(context.Background(), api.NewBigInt(big.NewInt(n)))
	require.NoError(t, err)
	require.NotNil(t, b)
	return b.PartitionView
}

// Retention of the partition view sidecar (unset-on-previous): only the latest finalized block and an
// in-flight finalizing candidate keep theirs, the UC of a block is never touched by the cleanup.
func TestBlockStorage_PrunePartitionViews(t *testing.T) {
	db, cleanup := setupBlockTestDB(t)
	defer cleanup()
	ctx := context.Background()
	bs := NewBlockStorage(db)
	require.NoError(t, bs.CreateIndexes(ctx))
	pb := func(n int64) *api.BigInt { return api.NewBigInt(big.NewInt(n)) }
	reset := func() { require.NoError(t, bs.collection.Drop(ctx)); require.NoError(t, bs.CreateIndexes(ctx)) }

	t.Run("previous views are removed, the new finalized pair stays", func(t *testing.T) {
		reset()
		for n := int64(1); n <= 5; n++ {
			finalizeWithView(t, bs, viewBlock(n, fmt.Sprintf("uc%d", n), fmt.Sprintf("view%d", n)))
		}
		got, err := bs.PrunePartitionViews(ctx, pb(5), 100)
		require.NoError(t, err)
		require.Equal(t, 4, got)
		for n := int64(1); n <= 4; n++ {
			require.Empty(t, viewOf(t, bs, n), "block %d", n)
		}
		latest, err := bs.GetLatest(ctx)
		require.NoError(t, err)
		require.Equal(t, api.HexBytes("view5"), latest.PartitionView)
		require.Equal(t, api.HexBytes("uc5"), latest.UnicityCertificate)
		old, err := bs.GetByNumber(ctx, pb(2))
		require.NoError(t, err)
		require.Equal(t, api.HexBytes("uc2"), old.UnicityCertificate, "the UC of a cleaned block is untouched")

		got, err = bs.PrunePartitionViews(ctx, pb(5), 100)
		require.NoError(t, err)
		require.Zero(t, got, "idempotent")
	})

	t.Run("crash before the new finalization keeps the old attachment", func(t *testing.T) {
		reset()
		finalizeWithView(t, bs, viewBlock(4, "uc4", "view4"))
		// block 5 is proposed / finalizing with its view, but not finalized yet
		cand := viewBlock(5, "uc5", "view5")
		cand.Finalized = false
		require.NoError(t, bs.Store(ctx, cand))
		require.NoError(t, bs.SetFinalizingWithCertificate(ctx, cand))

		// a cleanup keyed to the finalized head (4) cannot touch the head or the candidate
		_, err := bs.PrunePartitionViews(ctx, pb(4), 100)
		require.NoError(t, err)
		latest, err := bs.GetLatest(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 4, latest.Index.Uint64())
		require.Equal(t, api.HexBytes("view4"), latest.PartitionView, "the last finalized attachment survives a finalizing record")
		require.Equal(t, api.HexBytes("view5"), viewOf(t, bs, 5), "the in-flight candidate keeps its view")
	})

	t.Run("crash after finalization before cleanup leaves harmless extra copies", func(t *testing.T) {
		reset()
		finalizeWithView(t, bs, viewBlock(4, "uc4", "view4"))
		finalizeWithView(t, bs, viewBlock(5, "uc5", "view5"))
		latest, err := bs.GetLatest(ctx)
		require.NoError(t, err)
		require.Equal(t, api.HexBytes("view5"), latest.PartitionView, "readers use the latest block's own pair")
		require.Equal(t, api.HexBytes("view4"), viewOf(t, bs, 4), "the leftover is still there")
		// the retry (next finalization or the startup sweep) removes it
		_, err = bs.PrunePartitionViews(ctx, latest.Index, 100)
		require.NoError(t, err)
		require.Empty(t, viewOf(t, bs, 4))
		require.Equal(t, api.HexBytes("view5"), viewOf(t, bs, 5))
	})

	t.Run("null-view latest: the previous view is still removed", func(t *testing.T) {
		reset()
		finalizeWithView(t, bs, viewBlock(4, "uc4", "view4"))
		finalizeWithView(t, bs, viewBlock(5, "uc5", ""))
		_, err := bs.PrunePartitionViews(ctx, pb(5), 100)
		require.NoError(t, err)
		latest, err := bs.GetLatest(ctx)
		require.NoError(t, err)
		require.Empty(t, latest.PartitionView, "no older view is substituted for the null-view latest")
		require.Empty(t, viewOf(t, bs, 4))
	})

	t.Run("same root, different UC: each block is addressed by its number", func(t *testing.T) {
		reset()
		a, b := viewBlock(4, "ucA", "viewA"), viewBlock(5, "ucB", "viewB")
		b.RootHash = a.RootHash
		finalizeWithView(t, bs, a)
		finalizeWithView(t, bs, b)
		_, err := bs.PrunePartitionViews(ctx, pb(5), 100)
		require.NoError(t, err)
		latest, err := bs.GetLatestByRootHash(ctx, a.RootHash)
		require.NoError(t, err)
		require.Equal(t, api.HexBytes("ucB"), latest.UnicityCertificate)
		require.Equal(t, api.HexBytes("viewB"), latest.PartitionView, "the latest certification keeps its own view")
		require.Empty(t, viewOf(t, bs, 4))
	})

	t.Run("a stale writer's cutoff is monotonic: it never removes the head", func(t *testing.T) {
		reset()
		for n := int64(1); n <= 5; n++ {
			finalizeWithView(t, bs, viewBlock(n, fmt.Sprintf("uc%d", n), fmt.Sprintf("view%d", n)))
		}
		got, err := bs.PrunePartitionViews(ctx, pb(3), 100) // finalized height 3 seen by a lagging caller
		require.NoError(t, err)
		require.Equal(t, 2, got)
		require.Equal(t, api.HexBytes("view4"), viewOf(t, bs, 4))
		require.Equal(t, api.HexBytes("view5"), viewOf(t, bs, 5))
	})

	t.Run("bounded batches clean pre-upgrade history over several runs", func(t *testing.T) {
		reset()
		for n := int64(1); n <= 25; n++ {
			finalizeWithView(t, bs, viewBlock(n, fmt.Sprintf("uc%d", n), "v"))
		}
		total, runs := 0, 0
		for {
			got, err := bs.PrunePartitionViews(ctx, pb(25), 10)
			require.NoError(t, err)
			require.LessOrEqual(t, got, 10)
			if got == 0 {
				break
			}
			total += got
			runs++
		}
		require.Equal(t, 24, total)
		require.Equal(t, 3, runs)
		require.Equal(t, api.HexBytes("v"), viewOf(t, bs, 25))
	})

	t.Run("recent views are cleaned by one update bounded to a window", func(t *testing.T) {
		reset()
		for n := int64(1); n <= 25; n++ {
			finalizeWithView(t, bs, viewBlock(n, fmt.Sprintf("uc%d", n), "v"))
		}
		got, err := bs.PruneRecentPartitionViews(ctx, pb(25), 5)
		require.NoError(t, err)
		require.Equal(t, 5, got, "heights 20..24")
		require.Empty(t, viewOf(t, bs, 24))
		require.Empty(t, viewOf(t, bs, 20))
		require.Equal(t, api.HexBytes("v"), viewOf(t, bs, 19), "older history is the startup sweep's")
		require.Equal(t, api.HexBytes("v"), viewOf(t, bs, 25), "the cutoff block keeps its view")
		got, err = bs.PruneRecentPartitionViews(ctx, pb(3), 100) // window reaching below block 1
		require.NoError(t, err)
		require.Equal(t, 2, got)
		_, err = bs.PruneRecentPartitionViews(ctx, nil, 5)
		require.Error(t, err)
		_, err = bs.PruneRecentPartitionViews(ctx, pb(3), 0)
		require.Error(t, err)
	})

	t.Run("invalid arguments", func(t *testing.T) {
		_, err := bs.PrunePartitionViews(ctx, nil, 10)
		require.Error(t, err)
		_, err = bs.PrunePartitionViews(ctx, pb(3), 0)
		require.Error(t, err)
	})

	t.Run("the cleanup uses the partial index", func(t *testing.T) {
		specs := listIndexSpecsByName(t, ctx, bs.collection)
		require.Contains(t, specs, partitionViewIndexName)
	})

	t.Run("old binaries decode blocks with and without the sidecar", func(t *testing.T) {
		reset()
		finalizeWithView(t, bs, viewBlock(1, "uc1", "view1"))
		finalizeWithView(t, bs, viewBlock(2, "uc2", ""))
		// the block document as the previous release reads it: no partitionView field
		type oldBlock struct {
			Unicity string `bson:"unicityCertificate"`
			Root    string `bson:"rootHash"`
		}
		for n := int64(1); n <= 2; n++ {
			var raw bson.Raw
			require.NoError(t, bs.collection.FindOne(ctx, bson.M{"index": bigIntToDecimal128(pb(n))}).Decode(&raw))
			var old oldBlock
			require.NoError(t, bson.Unmarshal(raw, &old), "block %d", n)
			require.NotEmpty(t, old.Unicity)
		}
	})
}
