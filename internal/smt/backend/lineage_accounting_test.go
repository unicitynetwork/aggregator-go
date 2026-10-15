package backend

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// refLineage is the slow reference model of the retention lineage: the quadratic
// algorithm which scans the entries (kept only to define the semantics).
type refLineage struct {
	entries []*refEntry
	visits  int
}

type refEntry struct {
	delta uint64
	alive bool
}

func (r *refLineage) add(delta uint64) *refEntry {
	e := &refEntry{delta: delta, alive: true}
	r.entries = append(r.entries, e)
	return e
}

func (r *refLineage) retained(e *refEntry) uint64 {
	if !e.alive {
		return 0
	}
	total := uint64(smt.ReadViewOverheadBytes)
	after := false
	for _, x := range r.entries {
		r.visits++
		if x == e {
			after = true
			continue
		}
		if !after {
			continue
		}
		total += x.delta
		if x.alive {
			break
		}
	}
	return total
}

func (r *refLineage) total() uint64 {
	var sum uint64
	for _, e := range r.entries {
		sum += r.retained(e)
	}
	return sum
}

func (r *refLineage) close(e *refEntry) {
	e.alive = false
	i := 0
	for i < len(r.entries) && !r.entries[i].alive {
		i++
	}
	r.entries = append([]*refEntry(nil), r.entries[i:]...)
	r.visits += len(r.entries)
}

// Random captures and out-of-order closes/retirements: every charge and the total match the
// reference model exactly, with no entry ever examined by the accounting.
func TestLineageMatchesReferenceModel(t *testing.T) {
	for seed := int64(1); seed <= 30; seed++ {
		rng := rand.New(rand.NewSource(seed))
		var l lineage
		var ref refLineage
		type pair struct {
			e *lineageEntry
			r *refEntry
		}
		var all []pair
		ops := 0 // accounting operations performed on the lineage
		for step := 0; step < 400; step++ {
			switch op := rng.Intn(10); {
			case op < 4: // capture
				d := uint64(rng.Intn(5000))
				all = append(all, pair{l.addDelta(d), ref.add(d)})
				ops++
			case op < 8 && len(all) > 0: // close or retire (idempotent) any entry, any order
				p := all[rng.Intn(len(all))]
				l.close(p.e)
				ref.close(p.r)
				ops++
			}
			for _, p := range all {
				require.Equal(t, ref.retained(p.r), l.retained(p.e), "seed %d step %d", seed, step)
				ops++
			}
			require.Equal(t, ref.total(), l.totalRetained(), "seed %d step %d", seed, step)
			ops++
		}
		// every operation touches a constant number of entries (itself and at most two neighbors)
		require.LessOrEqual(t, l.visits, uint64(3*ops), "seed %d: %d visits for %d operations", seed, l.visits, ops)
		require.Positive(t, l.visits)
	}
}

// The bound, counted: with H live entries the reference examines Θ(H) entries per charge
// query (Θ(H²) per total), the lineage none, however many entries are alive.
func TestLineageAccountingWorkIsIndependentOfLiveEntries(t *testing.T) {
	var prevRef int
	for _, h := range []int{16, 64, 256, 1024} {
		var l lineage
		var ref refLineage
		var first *lineageEntry
		var rfirst *refEntry
		for i := 0; i < h; i++ {
			e, r := l.addDelta(100), ref.add(100)
			if i == 0 {
				first, rfirst = e, r
			}
		}
		ref.visits = 0
		l.visits = 0
		require.Equal(t, ref.retained(rfirst), l.retained(first))
		_ = l.totalRetained()
		l.close(first) // the interval of the oldest entry is inherited by nobody, the next becomes first
		_ = ref.total()
		require.LessOrEqual(t, l.visits, uint64(6), "H=%d: a charge query, a total and a close touch a constant number of entries", h)
		require.Positive(t, l.visits)
		require.Greater(t, ref.visits, prevRef, "the reference model must scale with H (otherwise this test proves nothing)")
		prevRef = ref.visits
	}
}

func TestMemoryBackendRetainedBytesTotalIsTheSumOfCharges(t *testing.T) {
	ctx := context.Background()
	b := NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	rng := rand.New(rand.NewSource(7))
	var open []ReadView
	ops := 0
	next := 0
	commit := func() {
		snap, err := b.CreateSnapshot(ctx)
		require.NoError(t, err)
		var in []LeafInput
		for i := 0; i < 1+rng.Intn(8); i++ {
			next++
			in = append(in, testLeafInput(byte(next), byte(next>>8)))
			in[len(in)-1].Key[0] = byte(next * 37)
			in[len(in)-1].Key[1] = byte(next >> 8)
		}
		res, err := snap.AddLeavesClassified(ctx, in)
		require.NoError(t, err)
		require.NoError(t, snap.Commit(ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(uint64(next)), RootHash: res.CandidateRoot}))
	}
	check := func(step int) {
		var sum uint64
		for _, v := range open {
			sum += v.RetainedBytes()
			ops++
		}
		require.Equal(t, sum, b.RetainedBytesTotal(), "step %d", step)
		ops++
	}
	for step := 0; step < 300; step++ {
		switch op := rng.Intn(10); {
		case op < 3:
			commit()
		case op < 6:
			v, err := b.RetainReadView(ctx)
			require.NoError(t, err)
			open = append(open, v)
			ops++
		case op < 8 && len(open) > 0: // out-of-order close
			i := rng.Intn(len(open))
			require.NoError(t, open[i].Close())
			ops++
			open = append(open[:i], open[i+1:]...)
		case len(open) > 0: // retire from the accounting, stays readable until closed
			i := rng.Intn(len(open))
			open[i].(AccountingRetirer).RetireAccounting()
			require.Zero(t, open[i].RetainedBytes())
			ops += 2
			_, err := open[i].GetInclusionCert(ctx, testLeafInput(1, 1).Key)
			require.NotErrorIs(t, err, ErrReadViewClosed)
			open = append(open[:i], open[i+1:]...)
		}
		check(step)
	}
	require.LessOrEqual(t, b.lineage.visits, uint64(3*ops), "visits stay constant per accounting operation")
}
