package utb_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/trustbase/utb"
	"github.com/unicitynetwork/aggregator-go/internal/trustbase/utb/utbtest"
)

func u64(v uint64) *uint64 { return &v }

var ctx = context.Background()

func cfgOf(t *testing.T, c *utbtest.Chain, n int) utb.LoadConfig {
	t.Helper()
	return utb.LoadConfig{Path: utbtest.WriteFile(t, "b.cbor", c.Bundle(t, n)), Pin: c.Pin(t), Network: utbtest.Network}
}

func open(t *testing.T, c *utbtest.Chain, n int, store utb.Store) *utb.History {
	t.Helper()
	h, err := utb.Open(ctx, cfgOf(t, c, n), store)
	require.NoError(t, err)
	return h
}

func tipOf(h *utb.History) uint64 { e, _ := h.Tip(); return e }

// decoded proves every answer is a well-formed response under the shared codec and returns it with its entries.
func decoded(t *testing.T, r *types.TrustBaseResponse) []*types.RootTrustBaseV2 {
	t.Helper()
	b, err := r.Encode()
	require.NoError(t, err)
	got, entries, err := types.DecodeTrustBaseResponse(b, types.TrustBasePageLimits)
	require.NoError(t, err)
	require.Equal(t, r.Status, got.Status)
	return entries
}

func TestOpenRefusesEverythingThatIsNotTheConfiguredCompleteHistory(t *testing.T) {
	c := utbtest.NewChain(t, 4, 6, 0)
	good := c.Bundle(t, 6)
	cfg := func(data []byte) utb.LoadConfig {
		return utb.LoadConfig{Path: utbtest.WriteFile(t, "b.cbor", data), Pin: c.Pin(t), Network: utbtest.Network}
	}
	_, err := utb.Open(ctx, cfg(good), utbtest.NewMemStore())
	require.NoError(t, err)

	// the encoder itself refuses a partial range, so the file is built by hand: epochs 3..6, a page that ends at the tip with no cursor
	id6 := c.ID(t, 6)
	partial := &types.TrustBaseResponse{Version: 1, Network: utbtest.Network, Status: types.TrustBaseStatusOK, FromEpoch: 3, EffectiveEnd: 6, EndEntryID: id6[:]}
	for _, r := range c.Raw[2:] {
		partial.Entries = append(partial.Entries, r)
	}
	partialBytes, err := partial.Encode()
	require.NoError(t, err)

	wrongPin := cfg(good)
	wrongPin.Pin[0] ^= 1
	otherNet := cfg(good)
	otherNet.Network = utbtest.Network + 1
	noNet := cfg(good)
	noNet.Network = 0
	small := cfg(good)
	small.MaxFileBytes = len(good) - 1
	few := cfg(good)
	few.MaxEntries = 5

	for name, lc := range map[string]utb.LoadConfig{
		"a partial range":              cfg(partialBytes),
		"wrong pin":                    wrongPin,
		"wrong network":                otherNet,
		"no expected network":          noNet,
		"trailing bytes":               cfg(append(bytes.Clone(good), 0)),
		"truncated":                    cfg(good[:len(good)-1]),
		"empty file":                   cfg(nil),
		"missing file":                 {Path: "/no/such/file", Pin: c.Pin(t), Network: utbtest.Network},
		"over the byte budget":         small,
		"more entries than the budget": few,
	} {
		_, err := utb.Open(ctx, lc, utbtest.NewMemStore())
		require.ErrorIs(t, err, utb.ErrLoad, name)
	}

	// a quorum short by one in epoch 4 (the structure and links are fine, the signatures are not)
	weak := utbtest.NewChain(t, 4, 6, 0)
	weak.Entries[3].Signatures = nil
	for i := 0; i < 2; i++ {
		require.NoError(t, weak.Entries[3].Sign(weak.Entries[2].RootNodes[i].NodeID, weak.Signers[i]))
	}
	raw := append([][]byte{}, weak.Raw...)
	raw[3], err = weak.Entries[3].MarshalCBOR()
	require.NoError(t, err)
	data, err := types.EncodeTrustBaseBundle(raw)
	require.NoError(t, err)
	_, err = utb.Open(ctx, cfg(data), utbtest.NewMemStore())
	require.ErrorIs(t, err, utb.ErrLoad, "short quorum")
	require.Contains(t, err.Error(), "epoch 4")

	_, err = utb.Open(ctx, cfg(good), nil)
	require.ErrorIs(t, err, utb.ErrStore, "no durable store: the aggregator does not start without one")
}

func TestOpenAcceptsALargeCommitteeInASingleEntryBundle(t *testing.T) {
	c := utbtest.NewChain(t, 256, 1, 0)
	h := open(t, c, 1, utbtest.NewMemStore())
	require.Len(t, h.Entry(1).RootNodes, 256)
}

func TestRespondStatusesAndShapes(t *testing.T) {
	c := utbtest.NewChain(t, 4, 7, 0)
	h := open(t, c, 7, utbtest.NewMemStore())
	tip := c.ID(t, 7)
	q := func(from uint64) utb.Query { return utb.Query{From: from, MaxEntries: 64} }

	t.Run("the complete history", func(t *testing.T) {
		r := h.Respond(q(1))
		require.Equal(t, types.TrustBaseStatusOK, r.Status)
		require.Equal(t, utbtest.Network, r.Network)
		require.Equal(t, uint64(1), r.FromEpoch)
		require.Equal(t, uint64(7), r.EffectiveEnd)
		require.Equal(t, tip[:], r.EndEntryID)
		require.Nil(t, r.NextEpoch)
		require.Len(t, decoded(t, r), 7)
	})
	t.Run("a range from the middle, and the tip alone", func(t *testing.T) {
		require.Len(t, decoded(t, h.Respond(q(4))), 4)
		es := decoded(t, h.Respond(q(7)))
		require.Len(t, es, 1)
		require.Equal(t, uint64(7), es[0].Epoch)
	})
	t.Run("an end below the tip", func(t *testing.T) {
		r := h.Respond(utb.Query{From: 2, To: u64(4), MaxEntries: 64})
		id4 := c.ID(t, 4)
		require.Equal(t, uint64(4), r.EffectiveEnd)
		require.Equal(t, id4[:], r.EndEntryID)
		require.Len(t, decoded(t, r), 3)
	})
	t.Run("an end beyond the tip is clamped, not an error", func(t *testing.T) {
		r := h.Respond(utb.Query{From: 5, To: u64(1000), MaxEntries: 64})
		require.Equal(t, types.TrustBaseStatusOK, r.Status)
		require.Equal(t, uint64(7), r.EffectiveEnd)
		require.Len(t, decoded(t, r), 3)
	})
	t.Run("from beyond the tip is NotYetAvailable with the actual tip", func(t *testing.T) {
		r := h.Respond(q(8))
		require.Equal(t, types.TrustBaseStatusNotYetAvailable, r.Status)
		require.Equal(t, uint64(7), r.EffectiveEnd)
		require.Equal(t, tip[:], r.EndEntryID)
		require.Empty(t, r.Entries)
		decoded(t, r)
		r = h.Respond(utb.Query{From: 9, To: u64(12), MaxEntries: 64})
		require.Equal(t, types.TrustBaseStatusNotYetAvailable, r.Status)
	})
	t.Run("invalid requests", func(t *testing.T) {
		for name, qq := range map[string]utb.Query{
			"from below the genesis":      {From: 0, MaxEntries: 64},
			"end below from":              {From: 4, To: u64(3), MaxEntries: 64},
			"snapshot id without end":     {From: 1, SnapshotEntryID: tip[:], MaxEntries: 64},
			"snapshot end without id":     {From: 1, SnapshotEnd: u64(7), MaxEntries: 64},
			"short snapshot id":           {From: 1, SnapshotEnd: u64(7), SnapshotEntryID: tip[:5], MaxEntries: 64},
			"from beyond the snapshot":    {From: 8, SnapshotEnd: u64(7), SnapshotEntryID: tip[:], MaxEntries: 64},
			"end below the snapshot":      {From: 1, To: u64(5), SnapshotEnd: u64(7), SnapshotEntryID: tip[:], MaxEntries: 64},
			"no entries allowed":          {From: 1, MaxEntries: 0},
			"more entries than the limit": {From: 1, MaxEntries: 65},
		} {
			r := h.Respond(qq)
			require.Equal(t, types.TrustBaseStatusInvalidRequest, r.Status, name)
			require.Empty(t, r.Entries, name)
			decoded(t, r)
		}
	})
	t.Run("unavailable carries nothing", func(t *testing.T) {
		r := h.Unavailable()
		require.Equal(t, types.TrustBaseStatusUnavailable, r.Status)
		decoded(t, r)
	})
}

// A client pages through the history by echoing (effectiveEnd, endEntryID) as the snapshot; the end must not move and the cursor must advance
// strictly to the end.
func TestPagingFreezesTheSnapshotAndAdvancesStrictly(t *testing.T) {
	c := utbtest.NewChain(t, 4, 9, 0)
	h := open(t, c, 9, utbtest.NewMemStore())

	var got []uint64
	query := utb.Query{From: 1, MaxEntries: 2}
	pages := 0
	var end uint64
	for {
		r := h.Respond(query)
		require.Equal(t, types.TrustBaseStatusOK, r.Status)
		if pages == 0 {
			end = r.EffectiveEnd
			require.Equal(t, uint64(9), end)
		}
		require.Equal(t, end, r.EffectiveEnd, "the end does not move")
		for _, e := range decoded(t, r) {
			got = append(got, e.Epoch)
		}
		pages++
		require.LessOrEqual(t, pages, 5, "no progress")
		if r.NextEpoch == nil {
			break
		}
		require.Equal(t, got[len(got)-1]+1, *r.NextEpoch, "the cursor is the next epoch")
		query = utb.Query{From: *r.NextEpoch, MaxEntries: 2, SnapshotEnd: u64(r.EffectiveEnd), SnapshotEntryID: r.EndEntryID}
	}
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
	require.Equal(t, 5, pages)
}

func TestAnAppendDuringPagingDoesNotMoveTheFrozenSnapshot(t *testing.T) {
	c := utbtest.NewChain(t, 4, 8, 0)
	h := open(t, c, 6, utbtest.NewMemStore())
	r := h.Respond(utb.Query{From: 1, MaxEntries: 2})
	require.Equal(t, uint64(6), r.EffectiveEnd)
	var got []uint64
	for _, e := range decoded(t, r) {
		got = append(got, e.Epoch)
	}
	_, err := h.Append(ctx, c.Raw[6])
	require.NoError(t, err)
	_, err = h.Append(ctx, c.Raw[7])
	require.NoError(t, err)
	for r.NextEpoch != nil {
		r = h.Respond(utb.Query{From: *r.NextEpoch, MaxEntries: 2, SnapshotEnd: u64(r.EffectiveEnd), SnapshotEntryID: r.EndEntryID})
		require.Equal(t, types.TrustBaseStatusOK, r.Status)
		require.Equal(t, uint64(6), r.EffectiveEnd, "the snapshot end stays")
		for _, e := range decoded(t, r) {
			got = append(got, e.Epoch)
		}
	}
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, got)
	// a new query sees the longer history, and a client that held 6 continues from there
	fresh := h.Respond(utb.Query{From: 7, MaxEntries: 64})
	require.Equal(t, uint64(8), fresh.EffectiveEnd)
	require.Len(t, decoded(t, fresh), 2)
}

func TestAnOlderReplicaAndAnotherHistoryDuringAFrozenSnapshot(t *testing.T) {
	c := utbtest.NewChain(t, 4, 9, 0)
	full, behind := open(t, c, 9, utbtest.NewMemStore()), open(t, c, 5, utbtest.NewMemStore())
	id9 := c.ID(t, 9)

	snap := utb.Query{From: 6, MaxEntries: 64, SnapshotEnd: u64(9), SnapshotEntryID: id9[:]}
	require.Equal(t, types.TrustBaseStatusOK, full.Respond(snap).Status)

	r := behind.Respond(snap)
	require.Equal(t, types.TrustBaseStatusNotYetAvailable, r.Status, "a replica behind the frozen end serves its shorter valid history")
	require.Equal(t, uint64(5), r.EffectiveEnd, "it reports its own tip")
	decoded(t, r)
}

func TestAHistoryWithAnotherEntryAtTheSnapshotEpochIsAMismatch(t *testing.T) {
	a := utbtest.NewChain(t, 4, 9, 0)
	// b shares a's genesis and epochs 1..8 and differs at epoch 9 only
	b := utbtest.NewChain(t, 4, 9, 0)
	b.Entries[8].StateHash = bytes.Repeat([]byte{0x77}, 32)
	b.Entries[8].Signatures = nil
	for i := 0; i < int(b.Entries[8].QuorumThreshold); i++ {
		require.NoError(t, b.Entries[8].Sign(b.Entries[7].RootNodes[i].NodeID, b.Signers[i]))
	}
	raw, err := b.Entries[8].MarshalCBOR()
	require.NoError(t, err)
	b.Raw[8] = raw

	ha, hb := open(t, a, 9, utbtest.NewMemStore()), open(t, b, 9, utbtest.NewMemStore())
	idA := a.ID(t, 9)
	r := hb.Respond(utb.Query{From: 5, MaxEntries: 64, SnapshotEnd: u64(9), SnapshotEntryID: idA[:]})
	require.Equal(t, types.TrustBaseStatusHistoryMismatch, r.Status)
	require.Empty(t, r.Entries)
	decoded(t, r)
	require.Equal(t, types.TrustBaseStatusOK, ha.Respond(utb.Query{From: 5, MaxEntries: 64, SnapshotEnd: u64(9), SnapshotEntryID: idA[:]}).Status)
}

// With a large committee every entry is tens of kilobytes: a page ends at the byte budget long before the 64-entry cap, and the cursor continues.
func TestAPageEndsAtTheByteBudgetWithALargeCommittee(t *testing.T) {
	c := utbtest.NewChain(t, 256, 52, 0)
	h := open(t, c, 52, utbtest.NewMemStore())
	r := h.Respond(utb.Query{From: 1, MaxEntries: 64})
	require.Equal(t, types.TrustBaseStatusOK, r.Status)
	require.NotNil(t, r.NextEpoch, "52 large entries do not fit one page")
	require.Less(t, len(r.Entries), 52)
	body, err := r.Encode()
	require.NoError(t, err)
	require.LessOrEqual(t, len(body), types.DefaultTrustBaseMaxResponse)
	used := 0
	for _, e := range r.Entries {
		used += len(e)
	}
	// the next entry would not have fitted
	require.Greater(t, used+len(c.Raw[*r.NextEpoch-1]), types.DefaultTrustBaseMaxResponse-types.TrustBaseResponseEnvelopeBound)

	var epochs []uint64
	query := utb.Query{From: 1, MaxEntries: 64}
	for pages := 0; ; pages++ {
		require.Less(t, pages, 10)
		r := h.Respond(query)
		for _, e := range decoded(t, r) {
			epochs = append(epochs, e.Epoch)
		}
		if r.NextEpoch == nil {
			break
		}
		query = utb.Query{From: *r.NextEpoch, MaxEntries: 64, SnapshotEnd: u64(r.EffectiveEnd), SnapshotEntryID: r.EndEntryID}
	}
	require.Len(t, epochs, 52)
}

func TestAppendAddsTheNextEntryAndRefusesEverythingElse(t *testing.T) {
	c := utbtest.NewChain(t, 4, 9, 0)
	fork := utbtest.NewChain(t, 4, 9, 1) // same genesis, other entries from epoch 2
	store := utbtest.NewMemStore()
	h := open(t, c, 3, store)

	out, err := h.Append(ctx, c.Raw[3])
	require.NoError(t, err)
	require.Equal(t, utb.Appended, out)
	require.Equal(t, uint64(4), tipOf(h))
	require.True(t, store.Has(4), "persisted")
	require.Equal(t, uint64(4), store.Tip())
	require.Equal(t, uint64(4), h.Respond(utb.Query{From: 4, MaxEntries: 64}).EffectiveEnd, "served at once")

	out, err = h.Append(ctx, c.Raw[3])
	require.NoError(t, err)
	require.Equal(t, utb.NoOp, out, "the same entry again")
	out, err = h.Append(ctx, c.Raw[1])
	require.NoError(t, err)
	require.Equal(t, utb.NoOp, out, "a held older entry")
	_, err = h.Append(ctx, c.Raw[0])
	require.NoError(t, err, "the genesis again")

	for name, tc := range map[string]struct {
		raw []byte
		err error
	}{
		"a different entry at a held epoch": {fork.Raw[3], utb.ErrConflict},
		"a different entry at epoch 2":      {fork.Raw[1], utb.ErrConflict},
		"ahead of the next epoch":           {c.Raw[5], utb.ErrGap},
		"garbage":                           {[]byte{1, 2, 3}, utb.ErrChain},
	} {
		_, err := h.Append(ctx, tc.raw)
		require.ErrorIs(t, err, tc.err, name)
	}
	require.Equal(t, uint64(4), tipOf(h), "nothing changed")

	// the next epoch of another history does not extend the held tip
	_, err = h.Append(ctx, fork.Raw[4])
	require.ErrorIs(t, err, utb.ErrChain, "epoch 5 of the other history links to its own epoch 4")
	// an entry whose predecessor quorum is short does not extend it
	weak := utbtest.NewChain(t, 4, 5, 0)
	weak.Entries[4].Signatures = nil
	for i := 0; i < 2; i++ {
		require.NoError(t, weak.Entries[4].Sign(weak.Entries[3].RootNodes[i].NodeID, weak.Signers[i]))
	}
	rawWeak, err := weak.Entries[4].MarshalCBOR()
	require.NoError(t, err)
	_, err = h.Append(ctx, rawWeak)
	require.ErrorIs(t, err, utb.ErrChain, "short previous quorum")
	require.False(t, store.Has(5), "a refused entry is never persisted")
	// a same-epoch repeat with a bad witness is not a silent no-op
	bad := utbtest.NewChain(t, 4, 5, 0)
	bad.Entries[3].Signatures = nil
	require.NoError(t, bad.Entries[3].Sign(bad.Entries[2].RootNodes[0].NodeID, bad.Signers[0]))
	rawBad, err := bad.Entries[3].MarshalCBOR()
	require.NoError(t, err)
	_, err = h.Append(ctx, rawBad)
	require.ErrorIs(t, err, utb.ErrChain, "the same entry id with a witness that does not verify")
}

func TestARestartWithTheOriginalBundleKeepsEveryPersistedEntry(t *testing.T) {
	c := utbtest.NewChain(t, 4, 8, 0)
	store := utbtest.NewMemStore()
	h := open(t, c, 3, store) // the bundle stops at epoch 3
	for _, raw := range c.Raw[3:6] {
		_, err := h.Append(ctx, raw)
		require.NoError(t, err)
	}
	require.Equal(t, uint64(6), tipOf(h))
	for e := uint64(1); e <= 6; e++ {
		require.True(t, store.Has(e), "epoch %d is stored, the bundle's own entries included", e)
	}

	// "crash": a new process, the same original bundle file, the same store
	h2 := open(t, c, 3, store)
	require.Equal(t, uint64(6), tipOf(h2), "no published epoch disappears on a restart")
	r := h2.Respond(utb.Query{From: 1, MaxEntries: 64})
	require.Len(t, decoded(t, r), 6)

	// a longer bundle later fills nothing it must not and keeps the tip; then an older matching bundle again
	h3 := open(t, c, 8, store)
	require.Equal(t, uint64(8), tipOf(h3))
	for e := uint64(1); e <= 8; e++ {
		require.True(t, store.Has(e), "the longer bundle's entries were never stored and are now")
	}
	h4 := open(t, c, 3, store)
	require.Equal(t, uint64(8), tipOf(h4), "an older matching bundle after a longer one")
	require.Equal(t, uint64(8), store.Tip())
}

func TestAFailedOrAmbiguousWriteLeavesNothingPublishedAndTheRetryIsSafe(t *testing.T) {
	c := utbtest.NewChain(t, 4, 6, 0)
	store := utbtest.NewMemStore()
	h := open(t, c, 3, store)

	// 1. the write fails and nothing is written
	store.PutHook = func(uint64) (bool, error) { return false, errors.New("disk full") }
	_, err := h.Append(ctx, c.Raw[3])
	require.Error(t, err)
	require.Equal(t, uint64(3), tipOf(h))
	require.False(t, store.Has(4))
	require.Equal(t, types.TrustBaseStatusNotYetAvailable, h.Respond(utb.Query{From: 4, MaxEntries: 64}).Status)

	// 2. the write is ambiguous: it reached the store but the caller saw an error. Nothing is published; the retry adopts the stored copy
	store.PutHook = func(uint64) (bool, error) { return true, errors.New("timeout") }
	_, err = h.Append(ctx, c.Raw[3])
	require.Error(t, err)
	require.True(t, store.Has(4))
	require.Equal(t, uint64(3), tipOf(h), "not published on an unacknowledged write")
	store.PutHook = nil
	out, err := h.Append(ctx, c.Raw[3])
	require.NoError(t, err)
	require.Equal(t, utb.Appended, out, "new to this process although the store held it")
	require.Equal(t, uint64(4), tipOf(h))
	require.Equal(t, uint64(4), store.Tip())

	// 3. the entry is written but the tip cannot advance: not published, durable tip unchanged, retry completes
	store.AdvanceHook = func(uint64) error { return errors.New("primary stepped down") }
	_, err = h.Append(ctx, c.Raw[4])
	require.Error(t, err)
	require.Equal(t, uint64(4), tipOf(h))
	require.True(t, store.Has(5))
	require.Equal(t, uint64(4), store.Tip())
	store.AdvanceHook = nil
	_, err = h.Append(ctx, c.Raw[4])
	require.NoError(t, err)
	require.Equal(t, uint64(5), tipOf(h))
	require.Equal(t, uint64(5), store.Tip())
}

func TestTwoAggregatorsSharingOneStoreConvergeWithoutLosingEntries(t *testing.T) {
	c := utbtest.NewChain(t, 4, 8, 0)
	fork := utbtest.NewChain(t, 4, 8, 1)
	store := utbtest.NewMemStore()
	a, b := open(t, c, 3, store), open(t, c, 3, store)

	_, err := a.Append(ctx, c.Raw[3])
	require.NoError(t, err)
	_, err = a.Append(ctx, c.Raw[4])
	require.NoError(t, err)
	require.Equal(t, uint64(3), tipOf(b), "b has not looked yet")

	n, err := b.Refresh(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, uint64(5), tipOf(b))
	n, err = b.Refresh(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
	lastErr, at := b.Status()
	require.NoError(t, lastErr)
	require.False(t, at.IsZero())
	require.Equal(t, store.Puts, 5, "the refresh wrote nothing: 3 bundle entries + 2 appended")

	// b is handed an entry a already stored: adopted, not written again; it is new to b
	_, err = a.Append(ctx, c.Raw[5])
	require.NoError(t, err)
	out, err := b.Append(ctx, c.Raw[5])
	require.NoError(t, err)
	require.Equal(t, utb.Appended, out)
	require.Equal(t, uint64(6), tipOf(b))
	puts := store.Puts

	// b is handed the other history's entry for an epoch a already stored: a conflict, nothing changes
	_, err = a.Append(ctx, c.Raw[6])
	require.NoError(t, err)
	b2 := open(t, c, 3, store) // a fresh process catches up from the store at start
	require.Equal(t, uint64(7), tipOf(b2))
	_, err = b.Append(ctx, fork.Raw[6])
	require.Error(t, err)
	require.Equal(t, uint64(6), tipOf(b))
	require.Equal(t, puts+1, store.Puts)
}

func TestRefreshAdoptsOnlyAcknowledgedEntries(t *testing.T) {
	c := utbtest.NewChain(t, 4, 6, 0)
	store := utbtest.NewMemStore()
	a, b := open(t, c, 3, store), open(t, c, 3, store)

	store.AdvanceHook = func(uint64) error { return errors.New("interrupted before the tip advanced") }
	_, err := a.Append(ctx, c.Raw[3])
	require.Error(t, err)
	require.True(t, store.Has(4))
	n, err := b.Refresh(ctx)
	require.NoError(t, err)
	require.Zero(t, n, "an entry beyond the durable tip is not adopted")
	require.Equal(t, uint64(3), tipOf(b))

	store.AdvanceHook = nil
	_, err = a.Append(ctx, c.Raw[3])
	require.NoError(t, err)
	n, err = b.Refresh(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// a corrupt stored entry is reported, not served
	_, err = a.Append(ctx, c.Raw[4])
	require.NoError(t, err)
	store.Corrupt(5, []byte{1, 2, 3})
	_, err = b.Refresh(ctx)
	require.ErrorIs(t, err, utb.ErrStore)
	require.Equal(t, uint64(4), tipOf(b))
	lastErr, _ := b.Status()
	require.ErrorIs(t, lastErr, utb.ErrStore, "the failure is visible to the health check")
}

func TestOpenRefusesAStoreThatIsNotThisHistoryOrIsMissingItsTail(t *testing.T) {
	c := utbtest.NewChain(t, 4, 8, 0)
	fork := utbtest.NewChain(t, 4, 8, 1)
	seeded := func() *utbtest.MemStore {
		s := utbtest.NewMemStore()
		h := open(t, c, 3, s)
		for _, raw := range c.Raw[3:6] {
			_, err := h.Append(ctx, raw)
			require.NoError(t, err)
		}
		return s
	}
	openWith := func(s *utbtest.MemStore) error {
		_, err := utb.Open(ctx, cfgOf(t, c, 3), s)
		return err
	}
	require.NoError(t, openWith(seeded()))

	s := seeded()
	s.Drop(6)
	require.ErrorIs(t, openWith(s), utb.ErrStore, "the store acknowledged epoch 6 but holds 5: a missing tail")

	s = seeded()
	s.Corrupt(6, fork.Raw[5])
	require.ErrorIs(t, openWith(s), utb.ErrStore, "another entry at the durable tip")

	s = seeded()
	s.Corrupt(6, []byte{9, 9})
	require.ErrorIs(t, openWith(s), utb.ErrStore, "a corrupt tail")

	s = seeded()
	s.Drop(4)
	require.ErrorIs(t, openWith(s), utb.ErrStore, "a hole")

	s = seeded()
	s.Corrupt(2, fork.Raw[1])
	require.ErrorIs(t, openWith(s), utb.ErrStore, "a stored epoch the bundle covers with another entry")

	s = seeded()
	_, err := utb.Open(ctx, utb.LoadConfig{Path: utbtest.WriteFile(t, "b.cbor", fork.Bundle(t, 3)), Pin: fork.Pin(t), Network: utbtest.Network + 1}, s)
	require.Error(t, err, "another network")
	other := utbtest.NewChainOn(t, utbtest.Network, 5, 3, 0) // another genesis, same network
	_, err = utb.Open(ctx, utb.LoadConfig{Path: utbtest.WriteFile(t, "o.cbor", other.Bundle(t, 3)), Pin: other.Pin(t), Network: utbtest.Network}, s)
	require.ErrorIs(t, err, utb.ErrStore, "a store bound to another genesis")
	require.ErrorIs(t, err, utb.ErrStoreMismatch)
}

func TestTheGuardDistinguishesBehindFromConflict(t *testing.T) {
	c := utbtest.NewChain(t, 4, 9, 0)
	h := open(t, c, 5, utbtest.NewMemStore())
	var latest *types.RootTrustBaseV1
	var failure error
	g := utb.NewGuard(h, func(context.Context) (*types.RootTrustBaseV1, error) {
		if failure != nil {
			return nil, failure
		}
		if latest == nil {
			return nil, interfaces.ErrTrustBaseNotFound
		}
		return latest, nil
	})

	require.NoError(t, g.Check(ctx), "nothing installed yet: nothing to contradict")
	latest = c.V1(5)
	require.NoError(t, g.Check(ctx))

	// the aggregator's active authority is past the held history: not a fork, the prefix is served and readiness is degraded
	latest = c.V1(7)
	err := g.Check(ctx)
	require.ErrorIs(t, err, utb.ErrBehind)
	require.NotErrorIs(t, err, utb.ErrAuthority)
	require.Equal(t, types.TrustBaseStatusNotYetAvailable, h.Respond(utb.Query{From: 6, MaxEntries: 64}).Status)

	// appending the missing epochs restores readiness without a replacement bundle
	for _, raw := range c.Raw[5:7] {
		_, err := h.Append(ctx, raw)
		require.NoError(t, err)
	}
	require.NoError(t, g.Check(ctx), "the verdict is not remembered past an append")

	// the same epoch with another authority is a conflict, even right after a good verdict
	latest = c.V1(7)
	require.NoError(t, g.Check(ctx))
	changed := c.V1(7)
	changed.EpochStart++
	latest = changed
	require.ErrorIs(t, g.Check(ctx), utb.ErrAuthority)
	other := c.V1(7)
	other.RootNodes = other.RootNodes[1:]
	latest = other
	require.ErrorIs(t, g.Check(ctx), utb.ErrAuthority, "another member set at the same epoch and start round")

	// a transient read failure is reported and not remembered
	failure = errors.New("db down")
	require.Error(t, g.Check(ctx))
	failure = nil
	latest = c.V1(6)
	require.NoError(t, g.Check(ctx))

	// another network
	otherNet := c.V1(6)
	otherNet.NetworkID++
	latest = otherNet
	require.ErrorIs(t, g.Check(ctx), utb.ErrAuthority)
}

func TestCheckAuthorityComparesTheFieldsBothRecordsCarry(t *testing.T) {
	c := utbtest.NewChain(t, 4, 5, 0)
	h := open(t, c, 5, utbtest.NewMemStore())
	require.NoError(t, h.CheckAuthority(c.V1(3)))

	edit := func(f func(v1 *types.RootTrustBaseV1)) error {
		v1 := c.V1(3)
		v1.RootNodes = append([]*types.NodeInfo{}, v1.RootNodes...)
		f(v1)
		return h.CheckAuthority(v1)
	}
	for name, f := range map[string]func(*types.RootTrustBaseV1){
		"network":      func(v *types.RootTrustBaseV1) { v.NetworkID++ },
		"epoch start":  func(v *types.RootTrustBaseV1) { v.EpochStart++ },
		"threshold":    func(v *types.RootTrustBaseV1) { v.QuorumThreshold-- },
		"a member out": func(v *types.RootTrustBaseV1) { v.RootNodes = v.RootNodes[1:] },
		"stake": func(v *types.RootTrustBaseV1) {
			v.RootNodes[0] = &types.NodeInfo{NodeID: v.RootNodes[0].NodeID, SigKey: v.RootNodes[0].SigKey, Stake: v.RootNodes[0].Stake + 1}
		},
		"key": func(v *types.RootTrustBaseV1) {
			v.RootNodes[0] = &types.NodeInfo{NodeID: v.RootNodes[0].NodeID, SigKey: c.Entries[0].RootNodes[1].SigKey, Stake: v.RootNodes[0].Stake}
		},
		"node id": func(v *types.RootTrustBaseV1) {
			v.RootNodes[0] = &types.NodeInfo{NodeID: "zzz", SigKey: v.RootNodes[0].SigKey, Stake: v.RootNodes[0].Stake}
		},
	} {
		require.ErrorIs(t, edit(f), utb.ErrAuthority, name)
	}
	require.ErrorIs(t, h.CheckAuthority(utbtest.NewChain(t, 4, 7, 0).V1(7)), utb.ErrBehind, "an epoch not held yet is behind, not a conflict")
}

func TestCheckInstalledAtStartup(t *testing.T) {
	c := utbtest.NewChain(t, 4, 9, 0)
	h := open(t, c, 6, utbtest.NewMemStore())
	list := func(all ...*types.RootTrustBaseV1) func(context.Context, uint64, uint64) ([]*types.RootTrustBaseV1, error) {
		return func(_ context.Context, from, to uint64) ([]*types.RootTrustBaseV1, error) {
			var out []*types.RootTrustBaseV1
			for _, v := range all {
				if v.GetEpoch() >= from && v.GetEpoch() <= to {
					out = append(out, v)
				}
			}
			return out, nil
		}
	}
	head := func(v *types.RootTrustBaseV1) utb.LatestV1 {
		return func(context.Context) (*types.RootTrustBaseV1, error) { return v, nil }
	}

	require.NoError(t, utb.NewGuard(h, head(c.V1(4))).CheckInstalled(ctx, list(c.V1(1), c.V1(2), c.V1(3), c.V1(4))))
	require.NoError(t, utb.NewGuard(h, func(context.Context) (*types.RootTrustBaseV1, error) { return nil, interfaces.ErrTrustBaseNotFound }).
		CheckInstalled(ctx, list()), "empty V1 store")

	// an older installed epoch that contradicts the history, while the head agrees
	bad := c.V1(2)
	bad.EpochStart += 7
	require.ErrorIs(t, utb.NewGuard(h, head(c.V1(4))).CheckInstalled(ctx, list(c.V1(1), bad, c.V1(3), c.V1(4))), utb.ErrAuthority)

	// the active epoch is beyond the held history, and a future scheduled entry too: the aggregator starts (behind), it does not refuse
	require.NoError(t, utb.NewGuard(h, head(c.V1(8))).CheckInstalled(ctx, list(c.V1(5), c.V1(6), c.V1(8))))
	err := utb.NewGuard(h, head(c.V1(8))).Check(ctx)
	require.ErrorIs(t, err, utb.ErrBehind)

	// the other network is a conflict
	otherNet := c.V1(4)
	otherNet.NetworkID++
	require.ErrorIs(t, utb.NewGuard(h, head(otherNet)).CheckInstalled(ctx, list(otherNet)), utb.ErrAuthority)
}

// epoch4 re-signs epoch 4 of a fresh copy of the chain: a different state hash makes another entry (another EntryID), and a different signer subset
// makes the same entry with another witness.
func epoch4(t *testing.T, stateHash []byte, signers []int) []byte {
	c := utbtest.NewChain(t, 4, 5, 0)
	e := c.Entries[3]
	if stateHash != nil {
		e.StateHash = stateHash
	}
	e.Signatures = nil
	for _, i := range signers {
		require.NoError(t, e.Sign(c.Entries[2].RootNodes[i].NodeID, c.Signers[i]))
	}
	raw, err := e.MarshalCBOR()
	require.NoError(t, err)
	return raw
}

func TestAnotherProcessStoredADifferentEntryOrADifferentWitnessForTheEpoch(t *testing.T) {
	c := utbtest.NewChain(t, 4, 5, 0)
	store := utbtest.NewMemStore()
	a, b := open(t, c, 3, store), open(t, c, 3, store)

	// a stores epoch 4 (witness: members 0,1,2)
	_, err := a.Append(ctx, c.Raw[3])
	require.NoError(t, err)

	// b is offered a valid but different entry for epoch 4: it extends b's tip, but the store holds another one. A conflict; nothing changes.
	other := epoch4(t, bytes.Repeat([]byte{0x55}, 32), []int{0, 1, 2})
	_, err = b.Append(ctx, other)
	require.ErrorIs(t, err, utb.ErrConflict)
	require.Equal(t, uint64(3), tipOf(b))
	stored, err := store.Get(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, c.Raw[3], stored, "the stored entry is untouched")

	// b is offered the same entry (same EntryID) with another valid witness (members 1,2,3): adopted from the store, and the stored copy is what is served
	again := epoch4(t, nil, []int{1, 2, 3})
	require.NotEqual(t, c.Raw[3], again, "a different witness")
	out, err := b.Append(ctx, again)
	require.NoError(t, err)
	require.Equal(t, utb.Appended, out)
	served := b.Respond(utb.Query{From: 4, MaxEntries: 1})
	require.Equal(t, c.Raw[3], []byte(served.Entries[0]), "the first stored witness is kept")
}

func TestARefreshThatCannotReadUpToTheAcknowledgedTipReportsAMissingTail(t *testing.T) {
	c := utbtest.NewChain(t, 4, 6, 0)
	store := utbtest.NewMemStore()
	a, b := open(t, c, 3, store), open(t, c, 3, store)
	for _, raw := range c.Raw[3:6] {
		_, err := a.Append(ctx, raw)
		require.NoError(t, err)
	}
	store.Drop(6) // the store acknowledged epoch 6 but no longer holds it
	_, err := b.Refresh(ctx)
	require.ErrorIs(t, err, utb.ErrStore)
	require.Contains(t, err.Error(), "missing tail")
}
