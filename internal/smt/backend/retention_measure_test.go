package backend

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/testutil/measure"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Retention cost of retained read views as functions of the SMT size N and the
// block size B (see docs/uc-consolidation-measurements.md). Opt in with
// UC_MEASURE=1. This measures the bare backend (RetainReadView and its lineage
// accounting); the A4 publication path and the catalog are measured separately
// in internal/proofview.
//
// Every block adds B new leaves and retains a view; retentionViews views are kept.
// "Measured per view" is the Go heap with all views retained minus the heap with
// only the newest, divided by the number of older views; "accounted" is the
// RetainedBytes charge (the budget currency).

const retentionViews = 16

func churnLeaf(n uint64) LeafInput {
	key := make([]byte, api.StateTreeKeyLengthBytes)
	x := n*0x9e3779b97f4a7c15 + 0x7f4a7c15
	binary.BigEndian.PutUint64(key[:8], x^(x>>29))
	binary.BigEndian.PutUint64(key[8:16], x*0xbf58476d1ce4e5b9)
	binary.BigEndian.PutUint64(key[len(key)-8:], n)
	value := make([]byte, 32)
	binary.BigEndian.PutUint64(value[24:], n+1)
	return LeafInput{Key: key, Value: value}
}

func heapInUse() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func commitLeaves(t testing.TB, b Backend, block uint64, from, to uint64) []byte {
	ctx := context.Background()
	inputs := make([]LeafInput, 0, to-from)
	for n := from; n < to; n++ {
		inputs = append(inputs, churnLeaf(n))
	}
	snap, err := b.CreateSnapshot(ctx)
	require.NoError(t, err)
	res, err := snap.AddLeavesClassified(ctx, inputs)
	require.NoError(t, err)
	require.NoError(t, res.ValidateAllAccepted(len(inputs)))
	require.NoError(t, snap.Commit(ctx, CommitMetadata{BlockNumber: api.NewBigIntFromUint64(block), RootHash: res.CandidateRoot}))
	return res.CandidateRoot
}

// buildBase commits n leaves in chunks and returns the next free leaf number and block.
func buildBase(t testing.TB, b Backend, n uint64) (next, block uint64) {
	const chunk = 50_000
	for next < n {
		to := min(next+chunk, n)
		block++
		commitLeaves(t, b, block, next, to)
		next = to
	}
	return next, block
}

type retentionResult struct {
	accountedPerView, measuredPerView, liveHeap uint64
	retainNanos                                 float64
}

func churnMemory(t testing.TB, baseN, per uint64) retentionResult {
	ctx := context.Background()
	heap0 := heapInUse()
	b := newMemory()
	next, block := buildBase(t, b, baseN)
	var views []ReadView
	var retain time.Duration
	for i := 0; i < retentionViews; i++ {
		block++
		commitLeaves(t, b, block, next, next+per)
		next += per
		start := time.Now()
		v, err := b.RetainReadView(ctx)
		retain += time.Since(start)
		require.NoError(t, err)
		views = append(views, v)
	}
	var accounted uint64
	for _, v := range views {
		accounted += v.RetainedBytes()
	}
	withAll := heapInUse()
	for _, v := range views[:len(views)-1] {
		require.NoError(t, v.Close())
	}
	withNewest := heapInUse()
	res := retentionResult{accountedPerView: accounted / retentionViews, retainNanos: float64(retain.Nanoseconds()) / retentionViews}
	if withAll > withNewest {
		res.measuredPerView = (withAll - withNewest) / (retentionViews - 1)
	}
	if withNewest > heap0 {
		res.liveHeap = withNewest - heap0
	}
	runtime.KeepAlive(views)
	runtime.KeepAlive(b)
	return res
}

// Sweep: SMT size N at fixed block size B = 100. Retention cost must follow the
// churn (B x depth), not N; only the live tree itself is linear in N.
func Test_ScalingRetentionMemorySMTSize(t *testing.T) {
	measure.Require(t)
	var sb strings.Builder
	sb.WriteString("B = 100 new leaves per block, 16 retained views\n\n| N | live tree heap (B) | live heap per leaf (B) | accounted per view (B) | measured per view (B) | accounted / measured | `RetainReadView` time relative to N=1e3 |\n|---|---|---|---|---|---|---|\n")
	var xs, live, acc, meas, rt []float64
	var t0 float64
	for _, N := range []uint64{1_000, 10_000, 100_000, 1_000_000} {
		r := churnMemory(t, N, 100)
		if t0 == 0 {
			t0 = r.retainNanos
		}
		ratio := "n/a"
		if r.measuredPerView > 0 {
			ratio = fmt.Sprintf("%.2fx", float64(r.accountedPerView)/float64(r.measuredPerView))
		}
		fmt.Fprintf(&sb, "| %d | %d | %d | %d | %d | %s | %.2fx |\n", N, r.liveHeap, r.liveHeap/N, r.accountedPerView, r.measuredPerView, ratio, r.retainNanos/t0)
		xs = append(xs, float64(N))
		live = append(live, float64(r.liveHeap))
		acc = append(acc, float64(r.accountedPerView))
		meas = append(meas, float64(r.measuredPerView))
		rt = append(rt, r.retainNanos)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over N = 1e3..1e6: live heap %s (expect 1), accounted per view %s (expect ~0.1: one more tree level per doubling), measured per view %s (expect ~0.1), retain time %s (expect ~0: capture does not copy the tree)\n",
		measure.FmtSlope(xs, live), measure.FmtSlope(xs, acc), measure.FmtSlope(xs, meas), measure.FmtSlope(xs, rt))
	t.Logf("memory backend: retention vs SMT size\n%s", sb.String())
}

// Sweep: block size B at fixed N = 100,000.
func Test_ScalingRetentionMemoryBlockSize(t *testing.T) {
	measure.Require(t)
	var sb strings.Builder
	sb.WriteString("N = 100,000 leaves, 16 retained views\n\n| B | accounted per view (B) | accounted per new leaf (B) | measured per view (B) | accounted / measured | `RetainReadView` time (relative to B=1) |\n|---|---|---|---|---|---|\n")
	var xs, acc, meas, rt []float64
	var t0 float64
	for _, B := range []uint64{1, 4, 16, 64, 256, 1024, 4096, 16384} {
		r := churnMemory(t, 100_000, B)
		if t0 == 0 {
			t0 = r.retainNanos
		}
		ratio := "n/a"
		if r.measuredPerView > 0 {
			ratio = fmt.Sprintf("%.2fx", float64(r.accountedPerView)/float64(r.measuredPerView))
		}
		fmt.Fprintf(&sb, "| %d | %d | %d | %d | %s | %.1fx |\n", B, r.accountedPerView, r.accountedPerView/B, r.measuredPerView, ratio, r.retainNanos/t0)
		xs = append(xs, float64(B))
		acc = append(acc, float64(r.accountedPerView))
		meas = append(meas, float64(r.measuredPerView))
		rt = append(rt, r.retainNanos)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over B = 1..16384 (whole range): accounted per view %s, measured per view %s, retain time %s. Over the upper range B = 256..16384: accounted %s, measured %s, retain time %s (below 1: paths shared by many new leaves are copied once, so cost grows sublinearly as B approaches N).\n",
		measure.FmtSlope(xs, acc), measure.FmtSlope(xs, meas), measure.FmtSlope(xs, rt),
		measure.FmtSlope(xs[4:], acc[4:]), measure.FmtSlope(xs[4:], meas[4:]), measure.FmtSlope(xs[4:], rt[4:]))
	t.Logf("memory backend: retention vs block size\n%s", sb.String())
}
