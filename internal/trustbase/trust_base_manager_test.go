package trustbase

import (
	"context"
	"errors"
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

// GetAdmitted reports only locally admitted history: it never asks the BFT node and never stores.
func TestTrustBaseManager_GetAdmittedIsLocalOnly(t *testing.T) {
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	rootNodes, _ := newRootNodes(t, 3)
	tb, err := types.NewTrustBase(types.NetworkLocal, rootNodes, types.WithEpoch(2), types.WithEpochStart(80))
	require.NoError(t, err)

	fetched := false
	rest := &mockBFTRestClient{GetTrustBasesFunc: func(context.Context, uint64, uint64) ([]*types.RootTrustBaseV1, error) {
		fetched = true
		return []*types.RootTrustBaseV1{tb}, nil
	}}
	store := &mockStorage{GetByEpochFunc: func(_ context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
		if epoch == 2 {
			return tb, nil
		}
		return nil, interfaces.ErrTrustBaseNotFound
	}}
	m := NewTrustBaseManager(log, store, rest, NewTrustBaseValidator(store))

	got, ok, err := m.GetAdmitted(t.Context(), 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(80), got.GetEpochStart())

	got, ok, err = m.GetAdmitted(t.Context(), 3)
	require.NoError(t, err)
	require.False(t, ok, "unknown history is not an error")
	require.Nil(t, got)

	store.GetByEpochFunc = func(context.Context, uint64) (*types.RootTrustBaseV1, error) { return nil, errors.New("database down") }
	_, ok, err = m.GetAdmitted(t.Context(), 3)
	require.Error(t, err, "a storage failure is not 'unknown'")
	require.False(t, ok)

	require.False(t, fetched, "the BFT node is never asked")
	require.Empty(t, store.stored, "nothing is stored")
}
