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

// AdmitNext extends the history by exactly one verified successor of the head and nothing else.
func TestTrustBaseManager_AdmitNext(t *testing.T) {
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	anchor, anchorSigners := genesis(t, types.NetworkLocal, 3)
	e2, e2Signers := next(t, anchor, types.NetworkLocal, anchorSigners)
	e3, _ := next(t, e2, types.NetworkLocal, e2Signers)

	t.Run("admits the successor and reports the head", func(t *testing.T) {
		var asked [][2]uint64
		rest := &mockBFTRestClient{GetTrustBasesFunc: func(_ context.Context, a, b uint64) ([]*types.RootTrustBaseV1, error) {
			asked = append(asked, [2]uint64{a, b})
			return []*types.RootTrustBaseV1{e2}, nil
		}}
		store := &memStore{tbs: map[uint64]*types.RootTrustBaseV1{1: anchor}}
		m := NewTrustBaseManager(log, store, rest, NewTrustBaseValidator(store))
		epoch, err := m.AdmitNext(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 2, epoch)
		require.Equal(t, [][2]uint64{{2, 2}}, asked, "only the next epoch is requested")
		head, err := m.Head(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 2, head)
	})

	t.Run("a skipped, unsigned or foreign-network answer is not admitted", func(t *testing.T) {
		unsigned, err := types.NewTrustBase(types.NetworkLocal, e2.RootNodes, types.WithEpoch(2), types.WithEpochStart(e2.GetEpochStart()), types.WithPreviousTrustBaseHash(e2.PreviousEntryHash))
		require.NoError(t, err)
		foreign, fSigners := genesis(t, types.NetworkMainNet, 3)
		foreign2, _ := next(t, foreign, types.NetworkMainNet, fSigners)
		for name, answer := range map[string][]*types.RootTrustBaseV1{"skips an epoch": {e3}, "unsigned": {unsigned}, "foreign network": {foreign2}, "empty": nil} {
			rest := &mockBFTRestClient{GetTrustBasesFunc: func(context.Context, uint64, uint64) ([]*types.RootTrustBaseV1, error) {
				return answer, nil
			}}
			store := &memStore{tbs: map[uint64]*types.RootTrustBaseV1{1: anchor}}
			m := NewTrustBaseManager(log, store, rest, NewTrustBaseValidator(store))
			_, err := m.AdmitNext(t.Context())
			require.Error(t, err, name)
			require.NotContains(t, store.tbs, uint64(2), name)
			require.NotContains(t, store.tbs, uint64(3), name)
		}
	})

	t.Run("the local lookup never calls the BFT node", func(t *testing.T) {
		rest := &mockBFTRestClient{GetTrustBasesFunc: func(context.Context, uint64, uint64) ([]*types.RootTrustBaseV1, error) {
			t.Fatal("unexpected REST call")
			return nil, nil
		}}
		store := &memStore{tbs: map[uint64]*types.RootTrustBaseV1{1: anchor}}
		m := NewTrustBaseManager(log, store, rest, NewTrustBaseValidator(store))
		_, err := m.GetByEpochLocal(t.Context(), 9)
		require.ErrorIs(t, err, interfaces.ErrTrustBaseNotFound)
	})
}
