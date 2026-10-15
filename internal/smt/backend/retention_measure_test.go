package backend

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Retention cost of retained finalized read views under churn (see
// docs/uc-consolidation-measurements.md). Opt in with UC_MEASURE=1.
//
// A base tree of retentionBase leaves receives blocks of B new leaves; every
// block retains a view and at most retentionViews views are kept (the oldest is
// closed, as the catalog does). The actual incremental heap per retained view is
// the heap with all views kept minus the heap with only the newest kept, divided
// by the number of older views; it is compared with the accounted RetainedBytes.

const (
	retentionBase  = 100_000
	retentionViews = 16
)

func churnLeaf(n uint32) LeafInput {
	key := make([]byte, api.StateTreeKeyLengthBytes)
	binary.BigEndian.PutUint32(key[:4], n*2654435761)
	binary.BigEndian.PutUint32(key[len(key)-4:], n)
	value := make([]byte, 32)
	binary.BigEndian.PutUint32(value[28:], n+1)
	return LeafInput{Key: key, Value: value}
}

func heapInUse() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func commitLeaves(t testing.TB, b Backend, block uint64, from, to uint32) []byte {
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

func Test_MeasureRetentionMemory(t *testing.T) {
	if os.Getenv("UC_MEASURE") == "" {
		t.Skip("set UC_MEASURE=1 to run the measurement")
	}
	ctx := context.Background()
	var sb strings.Builder
	sb.WriteString("| leaves per block | accounted per view (B) | measured per view (B) | accounted / measured | retain call | views in 256 MiB (accounted) |\n|---|---|---|---|---|---|\n")
	for _, per := range []uint32{100, 1000, 10000} {
		b := newMemory()
		next := uint32(retentionBase)
		commitLeaves(t, b, 1, 0, next)
		block := uint64(1)

		// steady state: retentionViews views, each one block apart
		var views []ReadView
		var retainTime time.Duration
		for i := 0; i < retentionViews; i++ {
			block++
			commitLeaves(t, b, block, next, next+per)
			next += per
			start := time.Now()
			v, err := b.RetainReadView(ctx)
			retainTime += time.Since(start)
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
		measured := int64(withAll) - int64(withNewest)
		older := uint64(retentionViews - 1)
		accPer := accounted / retentionViews
		measPer := uint64(0)
		if measured > 0 {
			measPer = uint64(measured) / older
		}
		ratio := "n/a"
		if measPer > 0 {
			ratio = fmt.Sprintf("%.2fx", float64(accPer)/float64(measPer))
		}
		fmt.Fprintf(&sb, "| %d | %d | %d | %s | %s | %d |\n", per, accPer, measPer, ratio,
			(retainTime / retentionViews).Round(time.Microsecond), uint64(256<<20)/max(accPer, 1))
		runtime.KeepAlive(views)
	}
	t.Logf("memory backend: %d base leaves, %d retained views\n%s", retentionBase, retentionViews, sb.String())
}
