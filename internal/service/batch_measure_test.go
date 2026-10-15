package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	mongostore "github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Measurement of get_inclusion_proofs.v1 (see docs/uc-consolidation-measurements.md):
// service latency and serialized response size for 1/16/256 state IDs over a
// retained in-memory view of a tree with batchMeasureStates leaves.
// Opt in with UC_MEASURE=1 (the Mongo variant also needs Docker).

const batchMeasureStates = 4096

func measureStats(d []time.Duration) (p50, p95, p99 time.Duration) {
	slices.Sort(d)
	at := func(q float64) time.Duration { return d[int(float64(len(d)-1)*q)] }
	return at(0.50), at(0.95), at(0.99)
}

type measureRow struct {
	ids        int
	respBytes  int
	p50, p95   time.Duration
	p99        time.Duration
	iterations int
}

func (e *batchEnv) measure(t *testing.T, ids []api.StateID, iters int) measureRow {
	ctx := context.Background()
	req := e.request(ids, nil)
	var respBytes int
	one := func() time.Duration {
		start := time.Now()
		resp, err := e.svc.GetInclusionProofsV1(ctx, req)
		if err != nil {
			t.Fatalf("batch call failed: %v (%v)", err, err.Internal)
		}
		data, jerr := json.Marshal(jsonrpc.NewResponse(resp, 1))
		require.NoError(t, jerr)
		respBytes = len(data)
		return time.Since(start)
	}
	for range 3 {
		one()
	}
	d := make([]time.Duration, iters)
	for i := range d {
		d[i] = one()
	}
	over := 0
	for _, x := range d {
		if x > BatchMaxExecution {
			over++
		}
	}
	if over > 0 {
		t.Logf("%d IDs: %d of %d calls exceeded the default %s execution deadline", len(ids), over, iters, BatchMaxExecution)
	}
	p50, p95, p99 := measureStats(d)
	return measureRow{ids: len(ids), respBytes: respBytes, p50: p50, p95: p95, p99: p99, iterations: iters}
}

func formatRows(rows []measureRow) string {
	var sb strings.Builder
	sb.WriteString("| IDs | response (B) | B/ID | p50 | p95 | p99 | iterations |\n|---|---|---|---|---|---|---|\n")
	for _, r := range rows {
		fmt.Fprintf(&sb, "| %d | %d | %d | %s | %s | %s | %d |\n", r.ids, r.respBytes, r.respBytes/r.ids,
			r.p50.Round(time.Microsecond), r.p95.Round(time.Microsecond), r.p99.Round(time.Microsecond), r.iterations)
	}
	return sb.String()
}

func Test_MeasureBatchProofs(t *testing.T) {
	if os.Getenv("UC_MEASURE") == "" {
		t.Skip("set UC_MEASURE=1 to run the measurement")
	}
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(batchMeasureStates, 1755000000)
	view := make([]byte, 1544) // typical 8-shard partition view
	e.finalize(view)

	var rows []measureRow
	for _, n := range []int{1, 16, 256} {
		rows = append(rows, e.measure(t, ids[:n], 400))
	}
	t.Logf("in-process record source (no database latency), %d leaves in the retained view\n%s", batchMeasureStates, formatRows(rows))
}

func Test_MeasureBatchProofsMongo(t *testing.T) {
	if os.Getenv("UC_MEASURE") == "" {
		t.Skip("set UC_MEASURE=1 to run the measurement")
	}
	ctx := context.Background()
	// lease and execution deadlines are lifted so the full latency is measured; the
	// fraction of calls above the default 5 s deadline is reported separately
	limits := proofview.DefaultLimits()
	limits.MaxLeaseDuration = 5 * time.Minute
	e := newBatchEnv(t, limits)
	e.svc.timeout = 5 * time.Minute
	ids := e.addRecords(batchMeasureStates, 1755000000)
	e.finalize(make([]byte, 1544))

	mc, err := mongodb.Run(ctx, "mongo:7.0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = mc.Terminate(context.Background()) })
	uri, err := mc.ConnectionString(ctx)
	require.NoError(t, err)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	db := client.Database("measure")

	store := mongostore.NewAggregatorRecordStorage(db)
	require.NoError(t, store.CreateIndexes(ctx))
	recs := make([]*models.AggregatorRecord, 0, len(ids))
	for i, id := range ids {
		r := *e.records[string(id)]
		r.ProposalID = "proposal-1"
		r.LeafIndex = api.NewBigInt(big.NewInt(int64(i)))
		recs = append(recs, &r)
	}
	require.NoError(t, store.StoreBatch(ctx, recs))
	block := models.NewBlock(api.NewBigIntFromUint64(1), "measure", 0, "1", "test", api.HexBytes(make([]byte, 32)), nil, nil, 1755000000)
	block.Finalized, block.Status, block.ProposalID = true, models.FinalityStatusFinalized, "proposal-1"
	require.NoError(t, mongostore.NewBlockStorage(db).Store(ctx, block))

	e.svc.records = store
	pstart := time.Now()
	for range 50 {
		require.NoError(t, client.Ping(ctx, nil))
	}
	t.Logf("driver round trip (Ping): %s", time.Since(pstart)/50)
	// per-query cost, to attribute the batch latency
	for _, q := range []struct {
		name string
		fn   func(id api.StateID) error
	}{
		{"size projection", func(id api.StateID) error { _, _, err := store.GetStoredSizeByStateID(ctx, id); return err }},
		{"record fetch", func(id api.StateID) error { _, err := store.GetByStateID(ctx, id); return err }},
	} {
		start := time.Now()
		for _, id := range ids[:200] {
			require.NoError(t, q.fn(id))
		}
		t.Logf("%s: %s per query (200 sequential queries)", q.name, time.Since(start)/200)
	}
	var rows []measureRow
	for _, n := range []int{1, 16, 256} {
		iters := 60
		if n == 16 {
			iters = 30
		}
		if n == 256 {
			iters = 12
		}
		rows = append(rows, e.measure(t, ids[:n], iters))
	}
	t.Logf("MongoDB 7.0 record source (docker, same host), %d records, 2 queries per ID (size projection + record)\n%s", batchMeasureStates, formatRows(rows))
}
