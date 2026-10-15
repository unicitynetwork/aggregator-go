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

// Sweep: capture count R while the oldest view stays pinned (V = 4). The memory
// lineage keeps an entry for every capture since the pinned view.
func Test_ScalingCatalogPinnedOldest(t *testing.T) {
	measure.Require(t)
	ctx := context.Background()
	limits := bigBudget()
	limits.MaxViews = 4
	m := newMemChain(t, limits)
	rootA := m.publishBlock(2000)
	pin, err := m.c.AcquireRoot(ctx, rootA)
	require.NoError(t, err)
	defer pin.Release()

	var sb strings.Builder
	sb.WriteString("V = 4 retained views, oldest view pinned by a lease, N = 2,000, B = 10; R captures after the pinned view\n\n| R | Stats time (relative to R=4) | AcquireLatest+Release time (relative) | Publish time (relative) | retained bytes (accounted) |\n|---|---|---|---|---|\n")
	var xs, st, at, pt []float64
	var s0, a0, p0 float64
	captured := 0
	for _, R := range []int{4, 16, 64, 256, 1024} {
		for ; captured < R; captured++ {
			m.publishBlock(10)
		}
		stc := measure.CostOf(40*time.Millisecond, func() { _ = m.c.Stats() })
		atc := measure.CostOf(40*time.Millisecond, func() {
			l, err := m.c.AcquireLatest(ctx)
			if err == nil {
				l.Release()
			}
		})
		ptc := measure.CostOf(40*time.Millisecond, func() { m.publishBlock(10); captured++ })
		if s0 == 0 {
			s0, a0, p0 = stc.Nanos, atc.Nanos, ptc.Nanos
		}
		fmt.Fprintf(&sb, "| %d | %.1fx | %.1fx | %.1fx | %d |\n", R, stc.Nanos/s0, atc.Nanos/a0, ptc.Nanos/p0, m.c.Stats().RetainedBytes)
		xs = append(xs, float64(R))
		st = append(st, stc.Nanos)
		at = append(at, atc.Nanos)
		pt = append(pt, ptc.Nanos)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over R = 4..1024: Stats %s, AcquireLatest+Release %s, Publish %s. With V bounded these would be 0 if catalog bookkeeping were independent of captures since the oldest pin; "+
		"a positive slope is the lineage metadata growing with R (the audit's H dependence).\n", measure.FmtSlope(xs, st), measure.FmtSlope(xs, at), measure.FmtSlope(xs, pt))
	t.Logf("catalog: pinned oldest view sweep\n%s", sb.String())
}

// Sweep: attachments per view U = 1..64 (V = 4, same state, newer seals), real UCs
// (4-signature seal, 8-shard partition view).
func Test_ScalingCatalogAttachments(t *testing.T) {
	measure.Require(t)
	ctx := context.Background()
	var sb strings.Builder
	sb.WriteString("V = 4, each state keeps up to U attachments (UC with 4-signature seal and an 8-shard view)\n\n| U | attachment bytes retained per view | catalog retained bytes (attachments + readers) | AttachCertification time (relative to U=1) | allocs per attach |\n|---|---|---|---|---|\n")
	var xs, bytesCol, at []float64
	var a0 float64
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
		next := U
		c := measure.CostOfN(timed, func() { // each newer UC evicts the oldest once the view is full
			cc := certs[next]
			next++
			require.NoError(t, m.c.AttachCertification(ctx, block(m.block), latestRoot, Certification{UC: cc.UC, PartitionView: cc.View}))
		})
		if a0 == 0 {
			a0 = c.Nanos
		}
		per := len(certs[0].UC) + len(certs[0].View) + attachmentOverhead
		fmt.Fprintf(&sb, "| %d | %d | %d | %.1fx | %.0f |\n", U, U*per, m.c.Stats().RetainedBytes, c.Nanos/a0, c.Allocs)
		xs = append(xs, float64(U))
		bytesCol = append(bytesCol, float64(U*per))
		at = append(at, c.Nanos)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over U = 1..64: retained attachment bytes %s (expect 1: storage is linear in U by design), attach time %s (a hash lookup would be 0; the audit expects a scan of the attachment list)\n",
		measure.FmtSlope(xs, bytesCol), measure.FmtSlope(xs, at))
	t.Logf("catalog: attachments sweep\n%s", sb.String())
}
