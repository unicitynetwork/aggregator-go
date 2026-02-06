package trustbase

import (
	"context"
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
