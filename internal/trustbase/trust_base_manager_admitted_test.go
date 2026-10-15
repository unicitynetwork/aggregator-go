package trustbase

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
)

type failingStore struct{ *memStore }

func (failingStore) GetByEpoch(context.Context, uint64) (*types.RootTrustBaseV1, error) {
	return nil, errors.New("database down")
}

// GetAdmitted reports only locally admitted history: it never asks the BFT node and never stores.
func TestTrustBaseManager_GetAdmittedIsLocalOnly(t *testing.T) {
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	anchor, anchorSigners := genesis(t, types.NetworkLocal, 3)
	e2, _ := next(t, anchor, types.NetworkLocal, anchorSigners)

	fetched := false
	rest := &mockBFTRestClient{GetTrustBasesFunc: func(context.Context, uint64, uint64) ([]*types.RootTrustBaseV1, error) {
		fetched = true
		return []*types.RootTrustBaseV1{e2}, nil
	}}
	store := &memStore{tbs: map[uint64]*types.RootTrustBaseV1{1: anchor}}
	m := NewTrustBaseManager(log, store, rest, NewTrustBaseValidator(store))

	got, ok, err := m.GetAdmitted(t.Context(), 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, anchor, got)

	// epoch 2 exists at the BFT node but is not admitted locally: unknown, never fetched or stored
	got, ok, err = m.GetAdmitted(t.Context(), 2)
	require.NoError(t, err)
	require.False(t, ok, "unknown history is not an error")
	require.Nil(t, got)
	require.False(t, fetched, "the BFT node is never asked")
	require.NotContains(t, store.tbs, uint64(2), "nothing is stored")

	// once admitted through the normal path it is known
	_, err = m.GetByEpoch(t.Context(), 2)
	require.NoError(t, err)
	got, ok, err = m.GetAdmitted(t.Context(), 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(2), got.GetEpoch())

	// a storage failure is not "unknown"
	bad := NewTrustBaseManager(log, failingStore{store}, rest, NewTrustBaseValidator(store))
	_, ok, err = bad.GetAdmitted(t.Context(), 3)
	require.Error(t, err)
	require.False(t, ok)
}
