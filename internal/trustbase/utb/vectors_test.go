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

// The shared GetTrustBase response vectors (bft-core#928, byte-identical to bft-core gettrustbase/testdata/v1/vectors.json and to bft-go-base
// testdata/trust-base-response/v1/vectors.json) are what every SDK decodes. Every vector this server can produce must be reproduced byte for byte by
// Respond; the rest are exactly the answers a static HTTP history cannot give, listed by name so that a new vector cannot be skipped silently.
func TestRespondReproducesTheSharedResponseVectorsByteForByte(t *testing.T) {
	data, err := os.ReadFile("testdata/gettrustbase-response-v1.json")
	require.NoError(t, err)
	var set struct {
		Network uint32   `json:"network"`
		Entries []string `json:"entries"`
		Cases   []struct {
			Name  string `json:"name"`
			Hex   string `json:"hex"`
			Error string `json:"error"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(data, &set))
	require.Len(t, set.Entries, 3)

	var raws [][]byte
	var ids [][32]byte
	for _, h := range set.Entries {
		b, err := hex.DecodeString(h)
		require.NoError(t, err)
		e, err := types.DecodeRootTrustBaseV2(b)
		require.NoError(t, err)
		id, err := e.EntryID()
		require.NoError(t, err)
		raws, ids = append(raws, b), append(ids, id)
	}
	cfg := func(n int) utb.LoadConfig {
		bundle, err := types.EncodeTrustBaseBundle(raws[:n])
		require.NoError(t, err)
		return utb.LoadConfig{Path: utbtest.WriteFile(t, "b.cbor", bundle), Pin: ids[0], Network: types.NetworkID(set.Network)}
	}
	full, err := utb.Open(ctx, cfg(3), utbtest.NewMemStore())
	require.NoError(t, err)
	lagging, err := utb.Open(ctx, cfg(1), utbtest.NewMemStore()) // a replica whose history stops at the genesis
	require.NoError(t, err)

	junk := make([]byte, 32)
	for i := range junk {
		junk[i] = 0xee
	}
	snap := func(epoch uint64, id [32]byte) (*uint64, []byte) { return u64(epoch), id[:] }
	end3, id3 := snap(3, ids[2])
	endJunk, idJunk := snap(3, [32]byte(junk))

	type produced struct {
		h *utb.History
		q utb.Query
	}
	answer := map[string]produced{
		"complete history from the genesis (the release bundle)":                   {full, utb.Query{From: 1, MaxEntries: 64}},
		"range from epoch 2 to the tip":                                            {full, utb.Query{From: 2, MaxEntries: 64}},
		"range of the tip entry only":                                              {full, utb.Query{From: 3, MaxEntries: 64}},
		"range of one middle entry served up to a requested end (to=2), no cursor": {full, utb.Query{From: 2, To: u64(2), MaxEntries: 64}},
		"page 1 of a paged range: cursor 3, end id of the snapshot end":            {full, utb.Query{From: 1, MaxEntries: 2}},
		"page 1 of one entry, cursor 2":                                            {full, utb.Query{From: 1, MaxEntries: 1}},
		"page 2, the last page of the paged range":                                 {full, utb.Query{From: 3, MaxEntries: 2, SnapshotEnd: end3, SnapshotEntryID: id3}},
		"page 2 starting mid-history: from 2, cursor 3, effective end 3":           {full, utb.Query{From: 2, MaxEntries: 1, SnapshotEnd: end3, SnapshotEntryID: id3}},
		"status 1 (InvalidRequest), nothing else":                                  {full, utb.Query{From: 0, MaxEntries: 64}},
		"status 4 (HistoryMismatch), nothing else":                                 {full, utb.Query{From: 2, MaxEntries: 64, SnapshotEnd: endJunk, SnapshotEntryID: idJunk}},
		"status 3 NotYetAvailable: from 4 is past the tip (3): nothing newer":      {full, utb.Query{From: 4, MaxEntries: 64}},
		"status 3 NotYetAvailable: a lagging peer (tip 1) asked from 2":            {lagging, utb.Query{From: 2, MaxEntries: 64}},
	}
	// what a static HTTP history cannot say: a WrongNetwork/LimitTooSmall/Busy answer (the query carries no network or byte budget and the page cap
	// always fits an entry), an unchecked end id on a cursor page, a NotYetAvailable whose end is not below `from`, and advisory metadata on a mismatch
	cannot := map[string]bool{
		"status 2 (WrongNetwork), nothing else":                                                  true,
		"status 5 (LimitTooSmall), nothing else":                                                 true,
		"status 6 (Busy), nothing else":                                                          true,
		"page 1 with an end id that matches nothing: the end id on a cursor page is not checked": true,
		"status 3 NotYetAvailable: effective end not below from (inconsistent for a caller; still a well-formed response)": true,
		"status 4 HistoryMismatch with advisory metadata":                                                                  true,
	}

	matched := 0
	for _, c := range set.Cases {
		if c.Error != "" {
			continue // refusals are the decoder's vectors (bft-go-base), not answers a server gives
		}
		want, err := hex.DecodeString(c.Hex)
		require.NoError(t, err)
		if c.Name == "status 7 (Unavailable), nothing else" {
			got, err := full.Unavailable().Encode()
			require.NoError(t, err)
			require.Equal(t, want, got, c.Name)
			matched++
			continue
		}
		if p, ok := answer[c.Name]; ok {
			got, err := p.h.Respond(p.q).Encode()
			require.NoError(t, err)
			require.Equal(t, hex.EncodeToString(want), hex.EncodeToString(got), c.Name)
			matched++
			continue
		}
		require.True(t, cannot[c.Name], "vector %q is neither reproduced nor on the list of answers this server cannot give", c.Name)
	}
	require.Equal(t, len(answer)+1, matched, "every listed answer has its vector")
}
