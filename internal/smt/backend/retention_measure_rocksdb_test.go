//go:build rocksdb

package backend

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/persist"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/rocksstore"
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

// churnRocks runs the churn on a fresh store and returns the on-disk size after
// a full compaction, the sum of the accounted view charges, and the average cost
// of RetainReadView. With pin=false no views are retained (the baseline).
func churnRocks(t *testing.T, per uint32, pin bool) (disk uint64, accounted uint64, retain time.Duration) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := rocksstore.Open(dir, rocksstore.Options{DisableWAL: true, NoSyncWrites: true})
	require.NoError(t, err)
	b, err := NewDiskBackend(store, persist.DefaultOptions())
	require.NoError(t, err)

	next := uint32(retentionBase)
	commitLeaves(t, b, 1, 0, next)
	block := uint64(1)
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

// Test_MeasureRetentionRocksDB mirrors Test_MeasureRetentionMemory on the disk
// backend. The measured cost is the on-disk growth of an identical churn run with
// the read snapshots pinned versus without any, both fully compacted at the end
// (old node versions cannot be compacted away under a pinned snapshot).
func Test_MeasureRetentionRocksDB(t *testing.T) {
	if os.Getenv("UC_MEASURE") == "" {
		t.Skip("set UC_MEASURE=1 to run the measurement")
	}
	var sb strings.Builder
	sb.WriteString("| leaves per block | accounted per view (B) | measured per view (B) | accounted / measured | retain call | views in 256 MiB (accounted) | disk without views (B) | disk with views (B) |\n|---|---|---|---|---|---|---|---|\n")
	for _, per := range []uint32{100, 1000, 10000} {
		base, _, _ := churnRocks(t, per, false)
		pinned, accounted, retain := churnRocks(t, per, true)
		accPer := accounted / retentionViews
		measPer := uint64(0)
		if pinned > base {
			measPer = (pinned - base) / retentionViews
		}
		ratio := "n/a"
		if measPer > 0 {
			ratio = fmt.Sprintf("%.2fx", float64(accPer)/float64(measPer))
		}
		fmt.Fprintf(&sb, "| %d | %d | %d | %s | %s | %d | %d | %d |\n", per, accPer, measPer, ratio,
			retain.Round(time.Microsecond), uint64(256<<20)/max(accPer, 1), base, pinned)
	}
	t.Logf("rocksdb backend: %d base leaves, %d retained views (each captured before its block)\n%s", retentionBase, retentionViews, sb.String())
}
