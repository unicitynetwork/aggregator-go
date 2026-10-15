package trustbase

import (
	"context"
	"crypto"
	"testing"

	"github.com/stretchr/testify/require"
	cryptobft "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

// memStore is a map-backed TrustBaseStore with already-exists semantics.
type memStore struct {
	tbs map[uint64]*types.RootTrustBaseV1
}

func (m *memStore) GetByEpoch(_ context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	if tb, ok := m.tbs[epoch]; ok {
		return tb, nil
	}
	return nil, interfaces.ErrTrustBaseNotFound
}

func (m *memStore) Store(_ context.Context, tb types.RootTrustBase) error {
	if _, ok := m.tbs[tb.GetEpoch()]; ok {
		return interfaces.ErrTrustBaseAlreadyExists
	}
	m.tbs[tb.GetEpoch()] = tb.(*types.RootTrustBaseV1)
	return nil
}

func genesis(t *testing.T, network types.NetworkID, n int) (*types.RootTrustBaseV1, map[string]cryptobft.Signer) {
	nodes, signers := newRootNodes(t, n)
	tb, err := types.NewTrustBase(network, nodes, types.WithEpoch(1))
	require.NoError(t, err)
	for id, s := range signers {
		require.NoError(t, tb.Sign(id, s))
	}
	return tb, signers
}

// next builds epoch prev+1 linked to prev and signed by prevSigners.
func next(t *testing.T, prev *types.RootTrustBaseV1, network types.NetworkID, prevSigners map[string]cryptobft.Signer) (*types.RootTrustBaseV1, map[string]cryptobft.Signer) {
	h, err := prev.Hash(crypto.SHA256)
	require.NoError(t, err)
	nodes, signers := newRootNodes(t, 3)
	tb, err := types.NewTrustBase(network, nodes,
		types.WithEpoch(prev.GetEpoch()+1),
		types.WithEpochStart(prev.GetEpochStart()+1000),
		types.WithPreviousTrustBaseHash(h),
	)
	require.NoError(t, err)
	for id, s := range prevSigners {
		require.NoError(t, tb.Sign(id, s))
	}
	return tb, signers
}

func restReturning(tbs ...*types.RootTrustBaseV1) *mockBFTRestClient {
	return &mockBFTRestClient{GetTrustBasesFunc: func(context.Context, uint64, uint64) ([]*types.RootTrustBaseV1, error) {
		return tbs, nil
	}}
}

func TestTrustBaseManager(t *testing.T) {
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	anchor, anchorSigners := genesis(t, types.NetworkLocal, 3)
	newManager := func(store *memStore, rest BFTRestClient) *TrustBaseManager {
		return NewTrustBaseManager(log, store, rest, NewTrustBaseValidator(store))
	}
	installed := func() *memStore {
		return &memStore{tbs: map[uint64]*types.RootTrustBaseV1{1: anchor}}
	}

	t.Run("extends installed chain with predecessor-signed epoch", func(t *testing.T) {
		e2, _ := next(t, anchor, types.NetworkLocal, anchorSigners)
		store := installed()
		got, err := newManager(store, restReturning(e2)).GetByEpoch(t.Context(), 2)
		require.NoError(t, err)
		require.Same(t, store.tbs[2], got)
		require.Equal(t, uint64(2), got.GetEpoch())
	})

	t.Run("serves installed anchor from store", func(t *testing.T) {
		got, err := newManager(installed(), restReturning()).GetByEpoch(t.Context(), 1)
		require.NoError(t, err)
		require.Same(t, anchor, got)
	})

	t.Run("hostile REST: self-signed replacement epoch 1 is never installed", func(t *testing.T) {
		evil, _ := genesis(t, types.NetworkLocal, 3)
		store := &memStore{tbs: map[uint64]*types.RootTrustBaseV1{}}
		_, err := newManager(store, restReturning(evil)).GetByEpoch(t.Context(), 1)
		require.ErrorContains(t, err, "anchor")
		require.Empty(t, store.tbs)
	})

	t.Run("hostile REST: self-signed epoch 1 served for requested epoch 2", func(t *testing.T) {
		evil, _ := genesis(t, types.NetworkLocal, 3)
		store := installed()
		_, err := newManager(store, restReturning(evil)).GetByEpoch(t.Context(), 2)
		require.ErrorContains(t, err, "requested epoch 2")
		require.Len(t, store.tbs, 1)
		require.Same(t, anchor, store.tbs[1])
	})

	t.Run("hostile REST: valid-looking wrong epoch", func(t *testing.T) {
		e2, _ := next(t, anchor, types.NetworkLocal, anchorSigners)
		store := installed()
		_, err := newManager(store, restReturning(e2)).GetByEpoch(t.Context(), 3)
		// epoch 2 is the predecessor of 3: fetched via REST, but REST then returns e2 for epoch 3 too
		require.ErrorContains(t, err, "requested epoch 3")
		require.NotContains(t, store.tbs, uint64(3))
	})

	t.Run("hostile REST: wrong network", func(t *testing.T) {
		e2, _ := next(t, anchor, types.NetworkLocal+1, anchorSigners)
		store := installed()
		_, err := newManager(store, restReturning(e2)).GetByEpoch(t.Context(), 2)
		require.ErrorContains(t, err, "network")
		require.NotContains(t, store.tbs, uint64(2))
	})

	t.Run("hostile REST: epoch 2 signed by attacker keys, not predecessor", func(t *testing.T) {
		_, evilSigners := genesis(t, types.NetworkLocal, 3)
		e2, _ := next(t, anchor, types.NetworkLocal, evilSigners)
		store := installed()
		_, err := newManager(store, restReturning(e2)).GetByEpoch(t.Context(), 2)
		require.Error(t, err)
		require.NotContains(t, store.tbs, uint64(2))
	})

	t.Run("hostile REST: epoch 2 not hash-linked to installed anchor", func(t *testing.T) {
		otherAnchor, otherSigners := genesis(t, types.NetworkLocal, 3)
		e2, _ := next(t, otherAnchor, types.NetworkLocal, otherSigners)
		store := installed()
		_, err := newManager(store, restReturning(e2)).GetByEpoch(t.Context(), 2)
		require.Error(t, err)
		require.NotContains(t, store.tbs, uint64(2))
	})

	t.Run("no installed anchor: nothing is bootstrapped from REST", func(t *testing.T) {
		evil, evilSigners := genesis(t, types.NetworkLocal, 3)
		e2, _ := next(t, evil, types.NetworkLocal, evilSigners)
		store := &memStore{tbs: map[uint64]*types.RootTrustBaseV1{}}
		_, err := newManager(store, restReturning(e2)).GetByEpoch(t.Context(), 2)
		require.Error(t, err)
		require.Empty(t, store.tbs)
	})

	t.Run("nil and empty REST results are rejected", func(t *testing.T) {
		store := installed()
		_, err := newManager(store, restReturning(nil)).GetByEpoch(t.Context(), 2)
		require.Error(t, err)
		_, err = newManager(store, restReturning()).GetByEpoch(t.Context(), 2)
		require.Error(t, err)
	})

	t.Run("already-exists conflict returns stored admitted entry, not received object", func(t *testing.T) {
		e2, _ := next(t, anchor, types.NetworkLocal, anchorSigners)
		admitted, _ := next(t, anchor, types.NetworkLocal, anchorSigners) // different validator set, same epoch
		store := installed()
		// the cache-miss read sees nothing, then a racing writer admits another entry before Store
		wrapped := &racingStore{memStore: store, onStore: func() { store.tbs[2] = admitted }}
		m := NewTrustBaseManager(log, wrapped, restReturning(e2), NewTrustBaseValidator(wrapped))
		got, err := m.GetByEpoch(t.Context(), 2)
		require.NoError(t, err)
		require.Same(t, admitted, got)
		require.NotSame(t, e2, got)
	})
}

// racingStore simulates a concurrent writer that wins between the cache miss and Store.
type racingStore struct {
	*memStore
	onStore func()
}

func (r *racingStore) Store(ctx context.Context, tb types.RootTrustBase) error {
	r.onStore()
	return r.memStore.Store(ctx, tb)
}

type mockBFTRestClient struct {
	GetTrustBasesFunc func(ctx context.Context, epoch1, epoch2 uint64) ([]*types.RootTrustBaseV1, error)
}

func (m *mockBFTRestClient) GetTrustBases(ctx context.Context, epoch1, epoch2 uint64) ([]*types.RootTrustBaseV1, error) {
	return m.GetTrustBasesFunc(ctx, epoch1, epoch2)
}

type countingStore struct {
	*memStore
	gets int
}

func (c *countingStore) GetByEpoch(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	c.gets++
	return c.memStore.GetByEpoch(ctx, epoch)
}

func TestTrustBaseManagerBoundedCatchUp(t *testing.T) {
	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	anchor, anchorSigners := genesis(t, types.NetworkLocal, 3)

	t.Run("multi-epoch catch-up from installed head", func(t *testing.T) {
		chain := []*types.RootTrustBaseV1{anchor}
		signers := anchorSigners
		for range 4 {
			tb, s := next(t, chain[len(chain)-1], types.NetworkLocal, signers)
			chain = append(chain, tb)
			signers = s
		}
		store := &memStore{tbs: map[uint64]*types.RootTrustBaseV1{1: anchor}}
		rest := &mockBFTRestClient{GetTrustBasesFunc: func(_ context.Context, e, _ uint64) ([]*types.RootTrustBaseV1, error) {
			return []*types.RootTrustBaseV1{chain[e-1]}, nil
		}}
		m := NewTrustBaseManager(log, store, rest, NewTrustBaseValidator(store))
		got, err := m.GetByEpoch(t.Context(), 5)
		require.NoError(t, err)
		require.Equal(t, uint64(5), got.GetEpoch())
		require.Len(t, store.tbs, 5)
	})

	t.Run("far-future epoch costs bounded lookups and no REST call", func(t *testing.T) {
		for _, epoch := range []uint64{maxCatchUpEpochs + 2, 4096, 1_000_000_000, ^uint64(0)} {
			store := &countingStore{memStore: &memStore{tbs: map[uint64]*types.RootTrustBaseV1{1: anchor}}}
			restCalls := 0
			rest := &mockBFTRestClient{GetTrustBasesFunc: func(context.Context, uint64, uint64) ([]*types.RootTrustBaseV1, error) {
				restCalls++
				return nil, nil
			}}
			m := NewTrustBaseManager(log, store, rest, NewTrustBaseValidator(store))
			_, err := m.GetByEpoch(t.Context(), epoch)
			require.ErrorContains(t, err, "beyond the installed chain")
			require.Zero(t, restCalls)
			require.LessOrEqual(t, store.gets, int(maxCatchUpEpochs)+2)
			require.Less(t, len(err.Error()), 300)
		}
	})

	t.Run("REST failure stops catch-up at first unavailable epoch", func(t *testing.T) {
		store := &countingStore{memStore: &memStore{tbs: map[uint64]*types.RootTrustBaseV1{1: anchor}}}
		restCalls := 0
		rest := &mockBFTRestClient{GetTrustBasesFunc: func(context.Context, uint64, uint64) ([]*types.RootTrustBaseV1, error) {
			restCalls++
			return nil, nil
		}}
		m := NewTrustBaseManager(log, store, rest, NewTrustBaseValidator(store))
		_, err := m.GetByEpoch(t.Context(), 10)
		require.Error(t, err)
		require.Equal(t, 1, restCalls)
		require.Less(t, len(err.Error()), 500)
	})

	t.Run("cancelled context aborts before any work", func(t *testing.T) {
		store := &countingStore{memStore: &memStore{tbs: map[uint64]*types.RootTrustBaseV1{1: anchor}}}
		rest := &mockBFTRestClient{GetTrustBasesFunc: func(context.Context, uint64, uint64) ([]*types.RootTrustBaseV1, error) {
			t.Fatal("REST must not be called after cancellation")
			return nil, nil
		}}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		m := NewTrustBaseManager(log, store, rest, NewTrustBaseValidator(store))
		_, err := m.GetByEpoch(ctx, 5)
		require.ErrorIs(t, err, context.Canceled)
	})
}
