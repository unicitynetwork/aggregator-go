package mongodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/trustbase/utb"
	"github.com/unicitynetwork/aggregator-go/internal/trustbase/utb/utbtest"
)

// The store carries a real history through Open, Append, a restart and a second process sharing it.
func TestTrustBaseHistoryStore(t *testing.T) {
	ctx := context.Background()
	store := NewTrustBaseHistoryStore(setupTestDB(t))
	c := utbtest.NewChain(t, 4, 6, 0)
	cfg := utb.LoadConfig{Path: utbtest.WriteFile(t, "b.cbor", c.Bundle(t, 3)), Pin: c.Pin(t), Network: utbtest.Network}

	h, err := utb.Open(ctx, cfg, store)
	require.NoError(t, err)
	_, err = h.Append(ctx, c.Raw[3])
	require.NoError(t, err)

	// the bundle's own entries and the appended one are stored, and the tip is acknowledged
	meta, entries, err := store.Load(ctx, 0)
	require.NoError(t, err)
	require.Len(t, entries, 4)
	require.Equal(t, uint64(4), meta.TipEpoch)
	require.Equal(t, c.Pin(t), meta.Pin)
	raw, err := store.Get(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, c.Raw[1], raw)
	_, err = store.Get(ctx, 9)
	require.ErrorIs(t, err, utb.ErrNotStored)

	// create-only, and the tip never moves backwards
	require.ErrorIs(t, store.Put(ctx, 2, []byte{1}), utb.ErrExists)
	id1 := c.ID(t, 1)
	require.NoError(t, store.Advance(ctx, 1, id1))
	meta, _, err = store.Load(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(4), meta.TipEpoch)

	// a restart with the original bundle and a second process sharing the database
	h2, err := utb.Open(ctx, cfg, store)
	require.NoError(t, err)
	tip, _ := h2.Tip()
	require.Equal(t, uint64(4), tip)
	_, err = h.Append(ctx, c.Raw[4])
	require.NoError(t, err)
	n, err := h2.Refresh(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// entries from a later epoch only
	_, entries, err = store.Load(ctx, 5)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// bound to one network and genesis
	require.ErrorIs(t, store.Bind(ctx, types.NetworkID(utbtest.Network+1), c.Pin(t)), utb.ErrStoreMismatch)
	other := utbtest.NewChainOn(t, utbtest.Network, 5, 2, 0)
	require.ErrorIs(t, store.Bind(ctx, utbtest.Network, other.Pin(t)), utb.ErrStoreMismatch)
	require.NoError(t, store.Bind(ctx, utbtest.Network, c.Pin(t)))
}
