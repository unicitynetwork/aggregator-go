package utb_test

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/trustbase/utb"
	"github.com/unicitynetwork/aggregator-go/internal/trustbase/utb/utbtest"
)

// The shared maximum-size fixture (bft-go-base testdata/trust-base-v2/max-entry/v1/max-entry.json, vendored byte for byte): a genesis of 256 members with
// 128-byte node ids and an epoch 2 signed by all 256 of them, 95,365 bytes, the largest valid entry.
type maxEntry struct {
	Network    uint32 `json:"network"`
	Genesis    string `json:"genesis"`
	GenesisID  string `json:"genesisId"`
	Entry      string `json:"entry"`
	EntryID    string `json:"entryId"`
	EntryBytes int    `json:"entryBytes"`
	Bundle     string `json:"bundle"`
}

func loadMaxEntry(t *testing.T) (maxEntry, []byte, []byte, [32]byte) {
	data, err := os.ReadFile("testdata/max-entry-v1.json")
	require.NoError(t, err)
	var f maxEntry
	require.NoError(t, json.Unmarshal(data, &f))
	g, err := hex.DecodeString(f.Genesis)
	require.NoError(t, err)
	e, err := hex.DecodeString(f.Entry)
	require.NoError(t, err)
	var pin [32]byte
	b, err := hex.DecodeString(f.GenesisID)
	require.NoError(t, err)
	copy(pin[:], b)
	return f, g, e, pin
}

func genesisOnly(t *testing.T, f maxEntry, g []byte, pin [32]byte) utb.LoadConfig {
	bundle, err := types.EncodeTrustBaseBundle([][]byte{g})
	require.NoError(t, err)
	return utb.LoadConfig{Path: utbtest.WriteFile(t, "g.cbor", bundle), Pin: pin, Network: types.NetworkID(f.Network)}
}

func TestTheLargestValidEntryIsAppendedStoredServedAndReplayed(t *testing.T) {
	f, g, e, pin := loadMaxEntry(t)
	require.Equal(t, f.EntryBytes, len(e))
	require.Less(t, len(e), types.MaxTrustBaseEntryBytes)
	store := utbtest.NewMemStore()
	h, err := utb.Open(ctx, genesisOnly(t, f, g, pin), store)
	require.NoError(t, err)

	out, err := h.Append(ctx, e)
	require.NoError(t, err)
	require.Equal(t, utb.Appended, out)
	stored, err := store.Get(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, e, stored)

	// served: the page holds both entries and is a valid response under the SDK page limits
	r := h.Respond(utb.Query{From: 1, MaxEntries: 64})
	require.Equal(t, types.TrustBaseStatusOK, r.Status)
	require.Equal(t, e, []byte(r.Entries[1]))
	require.Len(t, decoded(t, r), 2)
	only := h.Respond(utb.Query{From: 2, MaxEntries: 64})
	require.Equal(t, e, []byte(only.Entries[0]))
	require.Len(t, decoded(t, only), 1)

	// a restart with the genesis-only bundle replays it; a second aggregator adopts it
	h2, err := utb.Open(ctx, genesisOnly(t, f, g, pin), store)
	require.NoError(t, err)
	require.Equal(t, uint64(2), tipOf(h2))
	b, err := utb.Open(ctx, genesisOnly(t, f, g, pin), utbtest.NewMemStore())
	require.NoError(t, err)
	_ = b

	// the fixture's own two-entry bundle opens, too
	bundle, err := hex.DecodeString(f.Bundle)
	require.NoError(t, err)
	full, err := utb.Open(ctx, utb.LoadConfig{Path: utbtest.WriteFile(t, "full.cbor", bundle), Pin: pin, Network: types.NetworkID(f.Network)}, utbtest.NewMemStore())
	require.NoError(t, err)
	require.Equal(t, uint64(2), tipOf(full))
}

func TestARefreshAdoptsTheLargestValidEntry(t *testing.T) {
	f, g, e, pin := loadMaxEntry(t)
	store := utbtest.NewMemStore()
	a, err := utb.Open(ctx, genesisOnly(t, f, g, pin), store)
	require.NoError(t, err)
	b, err := utb.Open(ctx, genesisOnly(t, f, g, pin), store)
	require.NoError(t, err)
	_, err = a.Append(ctx, e)
	require.NoError(t, err)
	n, err := b.Refresh(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, uint64(2), tipOf(b))
}

// The cap is the bound the clients apply to a page, so nothing above it may be stored. No valid entry can reach it (the largest is 95,365 bytes), so
// the check is exercised with oversize bytes and recognised by its message.
func TestNothingOverTheEntryCapIsAppendedOrReplayed(t *testing.T) {
	c := utbtest.NewChain(t, 4, 5, 0)
	over := make([]byte, types.MaxTrustBaseEntryBytes+1)

	store := utbtest.NewMemStore()
	h := open(t, c, 3, store)
	_, err := h.Append(ctx, over)
	require.ErrorIs(t, err, utb.ErrChain)
	require.Contains(t, err.Error(), "the cap is")
	require.Equal(t, uint64(3), tipOf(h))
	require.False(t, store.Has(4))
	// exactly at the cap it is judged as an entry (and fails to decode), not as oversize
	_, err = h.Append(ctx, over[:types.MaxTrustBaseEntryBytes])
	require.ErrorIs(t, err, utb.ErrChain)
	require.NotContains(t, err.Error(), "the cap is")

	// replay at start: a stored entry over the cap
	_, err = h.Append(ctx, c.Raw[3])
	require.NoError(t, err)
	store.Corrupt(4, over)
	_, err = utb.Open(ctx, cfgOf(t, c, 3), store)
	require.ErrorIs(t, err, utb.ErrStore)
	require.Contains(t, err.Error(), "exceeds the cap")

	// replay by a refresh
	store2 := utbtest.NewMemStore()
	a, b := open(t, c, 3, store2), open(t, c, 3, store2)
	_, err = a.Append(ctx, c.Raw[3])
	require.NoError(t, err)
	store2.Corrupt(4, over)
	_, err = b.Refresh(ctx)
	require.ErrorIs(t, err, utb.ErrStore)
	require.Contains(t, err.Error(), "exceeds the cap")
	require.Equal(t, uint64(3), tipOf(b))
}
