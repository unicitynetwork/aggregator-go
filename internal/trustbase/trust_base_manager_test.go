package trustbase

import (
	"context"
	"crypto"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

func TestTrustBaseManager(t *testing.T) {
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	// create trust base for epoch 1
	rootNodes, signers := newRootNodes(t, 3)
	trustBaseEpoch1, err := types.NewTrustBase(
		types.NetworkLocal,
		rootNodes,
		types.WithEpoch(1),
	)
	require.NoError(t, err)
	for nodeID, signer := range signers {
		require.NoError(t, trustBaseEpoch1.Sign(nodeID, signer))
	}

	t.Run("test ok", func(t *testing.T) {
		// no trust base in store
		trustBaseStore := &mockStorage{
			GetByEpochFunc: func(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
				return nil, interfaces.ErrTrustBaseNotFound
			},
		}
		// default validator
		trustBaseValidator := NewTrustBaseValidator(trustBaseStore)

		// BFT REST client returns the trust base for epoch 1
		bftClient := &mockBFTRestClient{
			GetTrustBasesFunc: func(ctx context.Context, epoch1, epoch2 uint64) ([]*types.RootTrustBaseV1, error) {
				return []*types.RootTrustBaseV1{trustBaseEpoch1}, nil
			},
		}
		trustBaseManager := NewTrustBaseManager(log, trustBaseStore, bftClient, trustBaseValidator)
		tb, err := trustBaseManager.GetByEpoch(t.Context(), 1)
		require.NoError(t, err)

		// verify returned trust base
		require.Equal(t, uint64(1), tb.GetEpoch())

		// verify trust base was stored
		storedTB, ok := trustBaseStore.stored[1]
		require.True(t, ok, "trust base was not stored")
		require.Equal(t, tb.GetEpoch(), storedTB.GetEpoch())
	})
}

type mockBFTRestClient struct {
	GetTrustBasesFunc func(ctx context.Context, epoch1, epoch2 uint64) ([]*types.RootTrustBaseV1, error)
}

func (m *mockBFTRestClient) GetTrustBases(ctx context.Context, epoch1, epoch2 uint64) ([]*types.RootTrustBaseV1, error) {
	return m.GetTrustBasesFunc(ctx, epoch1, epoch2)
}

func TestTrustBaseManager_AdmitNext(t *testing.T) {
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	rootNodes, signers := newRootNodes(t, 3)
	tb1, err := types.NewTrustBase(types.NetworkLocal, rootNodes, types.WithEpoch(1))
	require.NoError(t, err)
	for id, s := range signers {
		require.NoError(t, tb1.Sign(id, s))
	}
	tb1Hash, err := tb1.Hash(crypto.SHA256)
	require.NoError(t, err)
	nodes2, _ := newRootNodes(t, 3)
	tb2, err := types.NewTrustBase(types.NetworkLocal, nodes2, types.WithEpoch(2), types.WithEpochStart(1000), types.WithPreviousTrustBaseHash(tb1Hash))
	require.NoError(t, err)
	for id, s := range signers {
		require.NoError(t, tb2.Sign(id, s))
	}

	newManager := func(rest *mockBFTRestClient) (*TrustBaseManager, *mockStorage) {
		store := &mockStorage{}
		store.stored = map[uint64]types.RootTrustBase{1: tb1}
		store.GetByEpochFunc = func(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
			if tb, ok := store.stored[epoch]; ok {
				return tb.(*types.RootTrustBaseV1), nil
			}
			return nil, interfaces.ErrTrustBaseNotFound
		}
		store.GetLatestFunc = func(ctx context.Context) (*types.RootTrustBaseV1, error) {
			var best *types.RootTrustBaseV1
			for _, tb := range store.stored {
				if best == nil || tb.GetEpoch() > best.GetEpoch() {
					best = tb.(*types.RootTrustBaseV1)
				}
			}
			return best, nil
		}
		return NewTrustBaseManager(log, store, rest, NewTrustBaseValidator(store)), store
	}

	t.Run("admits only the verified successor of the head", func(t *testing.T) {
		var asked [][2]uint64
		rest := &mockBFTRestClient{GetTrustBasesFunc: func(ctx context.Context, a, b uint64) ([]*types.RootTrustBaseV1, error) {
			asked = append(asked, [2]uint64{a, b})
			return []*types.RootTrustBaseV1{tb2}, nil
		}}
		m, store := newManager(rest)
		head, err := m.Head(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 1, head)
		epoch, err := m.AdmitNext(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 2, epoch)
		require.Equal(t, [][2]uint64{{2, 2}}, asked, "only the next epoch is requested")
		require.Contains(t, store.stored, uint64(2))
	})

	t.Run("an unsigned or mismatching answer is not admitted", func(t *testing.T) {
		unsigned, err := types.NewTrustBase(types.NetworkLocal, nodes2, types.WithEpoch(2), types.WithEpochStart(1000), types.WithPreviousTrustBaseHash(tb1Hash))
		require.NoError(t, err)
		for _, answer := range [][]*types.RootTrustBaseV1{{unsigned}, {tb1}, nil, {tb2, tb2}} {
			rest := &mockBFTRestClient{GetTrustBasesFunc: func(context.Context, uint64, uint64) ([]*types.RootTrustBaseV1, error) {
				return answer, nil
			}}
			m, store := newManager(rest)
			_, err := m.AdmitNext(t.Context())
			require.Error(t, err)
			require.NotContains(t, store.stored, uint64(2))
		}
	})

	t.Run("the local lookup never calls the BFT node", func(t *testing.T) {
		rest := &mockBFTRestClient{GetTrustBasesFunc: func(context.Context, uint64, uint64) ([]*types.RootTrustBaseV1, error) {
			t.Fatal("unexpected REST call")
			return nil, nil
		}}
		m, _ := newManager(rest)
		_, err := m.GetByEpochLocal(t.Context(), 7)
		require.ErrorIs(t, err, interfaces.ErrTrustBaseNotFound)
	})
}
