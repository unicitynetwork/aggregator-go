package proofview

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/testutil/measure"
)

// Scaling curves of the retained-view catalog over a real memory backend (see
// docs/uc-consolidation-measurements.md). Opt in with UC_MEASURE=1.
//
// Hardware independent metric: the number of RetainedBytes() reads the catalog
// performs per operation (each one scans the reader's lineage), counted with an
// instrumented reader; plus relative time, whose slope exposes the cost of the scans
// themselves.

type countingReader struct {
	backend.ReadView
	reads *atomic.Int64
}

func (c countingReader) RetainedBytes() uint64 {
	c.reads.Add(1)
	return c.ReadView.RetainedBytes()
}

func bigBudget() Limits {
	l := DefaultLimits()
	l.RetentionBudgetBytes = 1 << 40
	l.MaxConcurrentPins = 1 << 12
	return l
}

// Sweep: number of retained views V = 1..256 (budget high, no pressure).
func Test_ScalingCatalogRetainedViews(t *testing.T) {
	measure.Require(t)
	ctx := context.Background()
	var sb strings.Builder
	sb.WriteString("N = 2,000, B = 10 new leaves per block, V retained views, one attachment each\n\n| V | RetainedBytes reads per Publish | per AcquireLatest+Release | per Stats | Publish time (relative to V=1) | AcquireLatest+Release time (relative) | Stats time (relative) |\n|---|---|---|---|---|---|---|\n")
	var xs, pubReads, acqReads, statReads, pubT, acqT, statT []float64
	var p1, a1, s1 float64
	for _, V := range geometricInts(1, 256) {
		limits := bigBudget()
		limits.MaxViews = V
		var reads atomic.Int64
		m := newMemChain(t, limits)
		m.wrap = func(r backend.ReadView) backend.ReadView { return countingReader{r, &reads} }
		m.publishBlock(2000)
		for i := 1; i < V; i++ {
			m.publishBlock(10)
		}
		require.Equal(t, V, m.c.Stats().Views)

		reads.Store(0)
		m.publishBlock(10) // one more: the oldest view is evicted, V stays
		pr := reads.Load()
		reads.Store(0)
		l, err := m.c.AcquireLatest(ctx)
		require.NoError(t, err)
		l.Release()
		ar := reads.Load()
		reads.Store(0)
		_ = m.c.Stats()
		sr := reads.Load()

		pt := measure.CostOf(40*time.Millisecond, func() { m.publishBlock(10) })
		at := measure.CostOf(40*time.Millisecond, func() {
			l, _ := m.c.AcquireLatest(ctx)
			l.Release()
		})
		st := measure.CostOf(40*time.Millisecond, func() { _ = m.c.Stats() })
		if V == 1 {
			p1, a1, s1 = pt.Nanos, at.Nanos, st.Nanos
		}
		fmt.Fprintf(&sb, "| %d | %d | %d | %d | %.1fx | %.1fx | %.1fx |\n", V, pr, ar, sr, pt.Nanos/p1, at.Nanos/a1, st.Nanos/s1)
		xs = append(xs, float64(V))
		pubReads = append(pubReads, float64(pr))
		acqReads = append(acqReads, float64(ar))
		statReads = append(statReads, float64(sr))
		pubT = append(pubT, pt.Nanos)
		acqT = append(acqT, at.Nanos)
		statT = append(statT, st.Nanos)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over V = 1..256: RetainedBytes reads per Publish %s, per AcquireLatest+Release %s, per Stats %s (a constant-time catalog would be 0; each retained reader is read once per sweep, so V is slope 1); "+
		"time: Publish %s, AcquireLatest+Release %s, Stats %s (each read also scans the reader's lineage, so time exceeds the read count by about one more power of V)\n",
		measure.FmtSlope(xs, pubReads), measure.FmtSlope(xs, acqReads), measure.FmtSlope(xs, statReads), measure.FmtSlope(xs, pubT), measure.FmtSlope(xs, acqT), measure.FmtSlope(xs, statT))
	t.Logf("catalog: retained views sweep\n%s", sb.String())
}

func geometricInts(from, to int) []int {
	var out []int
	for x := from; x <= to; x *= 2 {
		out = append(out, x)
	}
	return out
}

// Sweep: capture count R while the oldest view stays pinned (V = 4). The memory lineage keeps
// an entry for every capture since the pinned view. Each point is measured at exactly the
// labelled R (no captures happen during a measurement). The hardware independent metrics are
// the lineage entries kept (H) and the lineage entries visited by one Stats call (every
// retained-byte query scans from the oldest entry); time is relative information.
func Test_ScalingCatalogPinnedOldest(t *testing.T) {
	measure.Require(t)
	ctx := context.Background()
	limits := bigBudget()
	limits.MaxViews = 4
	m := newMemChain(t, limits)
	var reads atomic.Int64
	m.wrap = func(r backend.ReadView) backend.ReadView { return countingReader{r, &reads} }
	rootA := m.publishBlock(2000)
	pin, err := m.c.AcquireRoot(ctx, rootA)
	require.NoError(t, err)
	defer pin.Release()

	var sb strings.Builder
	sb.WriteString("V = 4 retained views, oldest view pinned by a lease, N = 2,000, B = 10; R = captures after the pinned view (exact)\n\n| R | lineage entries kept (H) | lineage entries visited per Stats call | RetainedBytes reads per Stats call | Stats time (relative to R=4) | AcquireLatest+Release time (relative) | retained bytes (accounted) |\n|---|---|---|---|---|---|---|\n")
	var xs, hs, visited, st, at []float64
	var s0, a0 float64
	captured := 0
	for _, R := range []int{4, 16, 64, 256, 1024} {
		for ; captured < R; captured++ {
			m.publishBlock(10)
		}
		entries, v0 := m.b.LineageStats()
		reads.Store(0)
		_ = m.c.Stats()
		_, v1 := m.b.LineageStats()
		perStats := v1 - v0
		statReads := reads.Load()
		stc := measure.CostOf(40*time.Millisecond, func() { _ = m.c.Stats() })
		atc := measure.CostOf(40*time.Millisecond, func() {
			l, err := m.c.AcquireLatest(ctx)
			if err == nil {
				l.Release()
			}
		})
		if s0 == 0 {
			s0, a0 = stc.Nanos, atc.Nanos
		}
		fmt.Fprintf(&sb, "| %d | %d | %d | %d | %.1fx | %.1fx | %d |\n", R, entries, perStats, statReads, stc.Nanos/s0, atc.Nanos/a0, m.c.Stats().RetainedBytes)
		xs = append(xs, float64(R))
		hs = append(hs, float64(entries))
		visited = append(visited, float64(perStats))
		st = append(st, stc.Nanos)
		at = append(at, atc.Nanos)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over R = 4..1024: lineage entries kept %s, entries visited per Stats call %s (both expect 1: the metadata and the per-query scan grow with the captures since the pinned view although only V = 4 views are retained; "+
		"a bounded structure would give 0); relative time: Stats %s, AcquireLatest+Release %s.\n", measure.FmtSlope(xs, hs), measure.FmtSlope(xs, visited), measure.FmtSlope(xs, st), measure.FmtSlope(xs, at))
	t.Logf("catalog: pinned oldest view sweep\n%s", sb.String())
}

// Sweep: attachments per view U = 1..64 (V = 4, same state, newer seals), real UCs
// (4-signature seal, 8-shard partition view). Counters: attachment digest comparisons per
// attach, allocations, and the catalog's own retained-byte accounting (measured, not
// computed from the inputs): its growth over the U = 1 catalog is the attachment storage.
func Test_ScalingCatalogAttachments(t *testing.T) {
	measure.Require(t)
	ctx := context.Background()
	var sb strings.Builder
	sb.WriteString("V = 4, each state keeps up to U attachments (UC with 4-signature seal and an 8-shard view)\n\n| U | catalog retained bytes (measured) | attachment bytes retained (measured: growth over U=1) | digest comparisons per attach | allocs per attach | attach time (relative to U=1) |\n|---|---|---|---|---|---|\n")
	var xs, grown, cmps, at []float64
	var a0 float64
	var base uint64
	for _, U := range geometricInts(1, 64) {
		limits := bigBudget()
		limits.MaxViews = 4
		limits.MaxAttachmentsPerView = U
		m := newMemChain(t, limits)
		m.publishBlock(500)
		var latestRoot []byte
		for i := 0; i < 3; i++ {
			latestRoot = m.publishBlock(10)
		}
		// build the certifications first so the timed region is the catalog operation only
		const timed = 150
		certs := make([]measure.Certification, 0, U+timed)
		for i := 0; i < U+timed; i++ {
			certs = append(certs, measure.NewCertificationRound(t, 8, 4, latestRoot, uint64(1000+i)))
		}
		for _, cc := range certs[:U] { // fill the view to U attachments
			require.NoError(t, m.c.AttachCertification(ctx, block(m.block), latestRoot, Certification{UC: cc.UC, PartitionView: cc.View}))
		}
		retained := m.c.Stats().RetainedBytes // U attachments on the newest state; the others hold the original one
		next := U
		compBefore := m.c.Stats().AttachComparisons
		c := measure.CostOfN(timed, func() { // each newer UC evicts the oldest once the view is full
			cc := certs[next]
			next++
			require.NoError(t, m.c.AttachCertification(ctx, block(m.block), latestRoot, Certification{UC: cc.UC, PartitionView: cc.View}))
		})
		perAttach := float64(m.c.Stats().AttachComparisons-compBefore) / timed
		if base == 0 {
			base, a0 = retained, c.Nanos
		}
		growth := int64(retained) - int64(base)
		fmt.Fprintf(&sb, "| %d | %d | %d | %.1f | %.0f | %.1fx |\n", U, retained, growth+int64(len(certs[0].UC)+len(certs[0].View)+attachmentOverhead), perAttach, c.Allocs, c.Nanos/a0)
		xs = append(xs, float64(U))
		grown = append(grown, float64(growth+int64(len(certs[0].UC)+len(certs[0].View)+attachmentOverhead)))
		cmps = append(cmps, perAttach)
		at = append(at, c.Nanos)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over U = 1..64: measured attachment bytes retained %s (expect 1: storage is linear in U by design), digest comparisons per attach %s (expect 1 for a linear dedup scan, 0 for an index), attach time %s (relative)\n",
		measure.FmtSlope(xs, grown), measure.FmtSlope(xs, cmps), measure.FmtSlope(xs, at))
	t.Logf("catalog: attachments sweep\n%s", sb.String())
}
