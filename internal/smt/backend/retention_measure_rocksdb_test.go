//go:build rocksdb

package backend

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/internal/testutil/measure"
)

func dirBytes(t testing.TB, dir string) uint64 {
	var total uint64
	require.NoError(t, filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err == nil {
			total += uint64(info.Size())
		}
		return err
	}))
	return total
}

type diskChurn struct {
	accountedPerView uint64
	physicalPerView  uint64 // on-disk growth with the snapshots pinned vs not, after full compaction
	retainNanos      float64
	diskNoViews      uint64
	diskPinned       uint64
}

// churnRocks runs the churn on a fresh store and returns the on-disk size after
// a full compaction and the accounted charge. With pin=false no view is retained.
func churnRocks(t *testing.T, baseN, per uint64, pin bool) (disk uint64, accounted uint64, retain time.Duration) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := rocksstore.Open(dir, rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	b, err := NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)

	next, block := buildBase(t, b, baseN)
	require.NoError(t, store.CompactNodes())
	var views []ReadView
	for i := 0; i < retentionViews; i++ {
		block++
		if pin {
			start := time.Now()
			v, err := b.RetainReadView(ctx)
			retain += time.Since(start)
			require.NoError(t, err)
			views = append(views, v)
		}
		commitLeaves(t, b, block, next, next+per)
		next += per
	}
	for _, v := range views {
		accounted += v.RetainedBytes()
	}
	// compaction with the snapshots pinned cannot drop the old node versions
	require.NoError(t, store.CompactNodes())
	disk = dirBytes(t, dir)
	for _, v := range views {
		require.NoError(t, v.Close())
	}
	require.NoError(t, b.Close())
	return disk, accounted, retain / retentionViews
}

func churnDisk(t *testing.T, baseN, per uint64) diskChurn {
	base, _, _ := churnRocks(t, baseN, per, false)
	pinned, accounted, retain := churnRocks(t, baseN, per, true)
	r := diskChurn{accountedPerView: accounted / retentionViews, retainNanos: float64(retain.Nanoseconds()), diskNoViews: base, diskPinned: pinned}
	if pinned > base {
		r.physicalPerView = (pinned - base) / retentionViews
	}
	return r
}

func ratio(a, b uint64) string {
	if b == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2fx", float64(a)/float64(b))
}

// Sweep: SMT size N at fixed block size B = 1,000 (RocksDB). Physical cost is the on-disk
// growth of an identical churn run with 16 read snapshots pinned versus none.
func Test_ScalingRetentionRocksDBSMTSize(t *testing.T) {
	measure.Require(t)
	var sb strings.Builder
	sb.WriteString("B = 1,000 new leaves per block, 16 retained views\n\n| N | disk without views (B) | disk with views (B) | accounted per view (B) | physical per view (B) | accounted / physical | `RetainReadView` time relative to N=1e4 |\n|---|---|---|---|---|---|---|\n")
	var xs, base, acc, phys, rt []float64
	var t0 float64
	for _, N := range []uint64{10_000, 100_000, 1_000_000} {
		r := churnDisk(t, N, 1000)
		if t0 == 0 {
			t0 = r.retainNanos
		}
		fmt.Fprintf(&sb, "| %d | %d | %d | %d | %d | %s | %.2fx |\n", N, r.diskNoViews, r.diskPinned, r.accountedPerView, r.physicalPerView, ratio(r.accountedPerView, r.physicalPerView), r.retainNanos/t0)
		xs = append(xs, float64(N))
		base = append(base, float64(r.diskNoViews))
		acc = append(acc, float64(r.accountedPerView))
		phys = append(phys, float64(r.physicalPerView))
		rt = append(rt, r.retainNanos)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over N = 1e4..1e6: database size %s (expect 1), accounted per view %s, physical per view %s (both expect ~0.1: churn, not tree size), retain time %s (expect ~0)\n",
		measure.FmtSlope(xs, base), measure.FmtSlope(xs, acc), measure.FmtSlope(xs, phys), measure.FmtSlope(xs, rt))
	t.Logf("rocksdb backend: retention vs SMT size\n%s", sb.String())
}

// Sweep: block size B at fixed N = 100,000 (RocksDB).
func Test_ScalingRetentionRocksDBBlockSize(t *testing.T) {
	measure.Require(t)
	var sb strings.Builder
	sb.WriteString("N = 100,000 leaves, 16 retained views\n\n| B | accounted per view (B) | physical per view (B) | accounted / physical | `RetainReadView` time relative to B=10 |\n|---|---|---|---|---|\n")
	var xs, acc, phys, rt []float64
	var t0 float64
	for _, B := range []uint64{10, 100, 1000, 10_000} {
		r := churnDisk(t, 100_000, B)
		if t0 == 0 {
			t0 = r.retainNanos
		}
		fmt.Fprintf(&sb, "| %d | %d | %d | %s | %.2fx |\n", B, r.accountedPerView, r.physicalPerView, ratio(r.accountedPerView, r.physicalPerView), r.retainNanos/t0)
		xs = append(xs, float64(B))
		acc = append(acc, float64(r.accountedPerView))
		phys = append(phys, float64(r.physicalPerView))
		rt = append(rt, r.retainNanos)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over B = 10..10,000: accounted per view %s, physical per view %s (expect ~1: bytes written follow the churn), retain time %s (expect 0: a pinned snapshot does not depend on B)\n",
		measure.FmtSlope(xs, acc), measure.FmtSlope(xs, phys), measure.FmtSlope(xs, rt))
	t.Logf("rocksdb backend: retention vs block size\n%s", sb.String())
}

// countingStore records every node the proof builder asks the snapshot for.
type countingStore struct {
	inner  storage.ReadStore
	reads  int // node reads requested (GetNode calls + keys passed to GetNodes)
	calls  int // GetNode + GetNodes calls (round trips to the snapshot)
	unique map[disk.NodeKey]struct{}
}

func newCountingStore(inner storage.ReadStore) *countingStore {
	return &countingStore{inner: inner, unique: map[disk.NodeKey]struct{}{}}
}

func (c *countingStore) GetNode(k disk.NodeKey) ([]byte, bool, error) {
	c.reads++
	c.calls++
	c.unique[k] = struct{}{}
	return c.inner.GetNode(k)
}

func (c *countingStore) GetNodes(ks []disk.NodeKey, sorted bool) ([]storage.NodeReadResult, error) {
	c.reads += len(ks)
	c.calls++
	for _, k := range ks {
		c.unique[k] = struct{}{}
	}
	return c.inner.GetNodes(ks, sorted)
}

// Sweep: proofs per batch L against a pinned RocksDB snapshot at N = 100,000. Compares building
// L proofs one by one (the path GetInclusionCert takes today) with the existing union-path
// builder (persist.BuildInclusionCerts), counting node reads and the distinct nodes touched.
func Test_ScalingRocksDBProofReads(t *testing.T) {
	measure.Require(t)
	ctx := context.Background()
	store, err := rocksstore.Open(t.TempDir(), rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	b, err := NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	const N = 100_000
	buildBase(t, b, N)
	v, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	dv := v.(*diskReadView)

	var sb strings.Builder
	sb.WriteString("N = 100,000, random existing keys; one pinned snapshot\n\n| L | single proofs: node reads | single: GetNode(s) calls | single: distinct nodes | union build: node reads | union: GetNode(s) calls | union: distinct nodes | reads saved by the union build | single time | union time |\n|---|---|---|---|---|---|---|---|---|---|\n")
	var xs, sReads, uReads, uniq []float64
	for _, L := range geometric(1, 256, 2) {
		keys := make([][]byte, L)
		for i := range keys {
			keys[i] = churnLeaf(uint64(i*397+11) % N).Key
		}
		sc := newCountingStore(dv.reader)
		start := time.Now()
		for _, k := range keys {
			_, err := persist.BuildInclusionCert(dv.root, sc, k)
			require.NoError(t, err)
		}
		sTime := time.Since(start)
		uc := newCountingStore(dv.reader)
		start = time.Now()
		certs, err := persist.BuildInclusionCerts(dv.root, uc, keys)
		require.NoError(t, err)
		uTime := time.Since(start)
		require.Len(t, certs, L)
		fmt.Fprintf(&sb, "| %d | %d | %d | %d | %d | %d | %d | %.2fx | %s | %s |\n", L, sc.reads, sc.calls, len(sc.unique), uc.reads, uc.calls, len(uc.unique),
			float64(sc.reads)/float64(max(uc.reads, 1)), sTime.Round(time.Microsecond), uTime.Round(time.Microsecond))
		xs = append(xs, float64(L))
		sReads = append(sReads, float64(sc.reads))
		uReads = append(uReads, float64(uc.reads))
		uniq = append(uniq, float64(len(uc.unique)))
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over L = 1..256: single-proof node reads %s (expect 1: L x depth), union-build node reads %s and distinct nodes %s (expect < 1: shared upper path nodes are read once; "+
		"for large L the distinct nodes approach N)\n", measure.FmtSlope(xs, sReads), measure.FmtSlope(xs, uReads), measure.FmtSlope(xs, uniq))
	t.Logf("rocksdb: proof node reads\n%s", sb.String())
	require.NoError(t, v.Close())
}

func geometric(from, to, factor int) []int {
	var out []int
	for x := from; x <= to; x *= factor {
		out = append(out, x)
	}
	return out
}
