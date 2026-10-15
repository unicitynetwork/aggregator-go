package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"math/bits"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	mongostore "github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/internal/testutil/measure"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Scaling curves of get_inclusion_proofs.v1 (see docs/uc-consolidation-measurements.md).
// Opt in with UC_MEASURE=1; the Mongo sweep also needs Docker.

// finalizeCertified publishes the current tree as the next block with native,
// verified certification material for an S-shard partition (4-signature seal),
// bound to the tree root; the UC is the ordinary UC repeated in every proof.
func (e *batchEnv) finalizeCertified(shards int) measure.Certification {
	e.blocks++
	var prevSeq uint64
	if e.prev != nil {
		prevSeq = e.prev.Seq()
	}
	rv := e.tree.FreezeReadView(prevSeq)
	e.prev = rv
	root := rv.RootHashRaw()
	cert := measure.NewCertification(e.t, shards, 4, root)
	require.NoError(e.t, e.catalog.Publish(context.Background(), proofview.PublishInput{
		Block: proofview.BlockIdentity{Number: api.NewBigIntFromUint64(e.blocks), ConfigLineage: []byte("cfg")},
		Root:  root, Reader: readerView{v: rv},
		Cert: proofview.Certification{UC: cert.UC, PartitionView: cert.View},
	}))
	e.roots = append(e.roots, root)
	return cert
}

// fill grows the tree with n anonymous leaves (no records): the SMT size N of a
// shard, of which a request only ever names a few.
func (e *batchEnv) fill(n int, seed uint64) {
	const chunk = 50_000
	for done := 0; done < n; {
		m := min(chunk, n-done)
		snap := e.tree.CreateSnapshot()
		leaves := make([]*smt.Leaf, 0, m)
		for i := 0; i < m; i++ {
			x := seed + uint64(done+i)
			key := make([]byte, api.StateTreeKeyLengthBytes)
			for j := 0; j < 4; j++ { // spread over the key space (splitmix-like)
				x += 0x9e3779b97f4a7c15
				z := x
				z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
				z = (z ^ (z >> 27)) * 0x94d049bb133111eb
				z ^= z >> 31
				for b := 0; b < 8; b++ {
					key[j*8+b] = byte(z >> (8 * b))
				}
			}
			path, err := api.FixedBytesToPath(key, api.StateTreeKeyLengthBits)
			require.NoError(e.t, err)
			leaves = append(leaves, smt.NewLeaf(path, key[:32]))
		}
		res := snap.AddLeavesClassified(leaves)
		require.Empty(e.t, res.Rejected)
		snap.Commit(e.tree)
		done += m
	}
}

type batchRow struct {
	ids         int
	respBytes   int
	pathNodes   int // sum over proofs of SMT siblings: nodes the proofs traverse
	sizeQueries int
	loadQueries int
	cost        measure.Cost
}

// callBatch runs one batch (service call + JSON serialization) and returns exact
// counters; cost measures the same operation.
func (e *batchEnv) callBatch(t *testing.T, ids []api.StateID, latest bool, costTime time.Duration) batchRow {
	ctx := context.Background()
	req := e.request(ids, nil)
	if !latest {
		req.Root = e.roots[len(e.roots)-1]
	}
	e.recs.mu.Lock()
	e.recs.sizeQ, e.recs.calls = 0, 0
	e.recs.mu.Unlock()
	resp, berr := e.svc.GetInclusionProofsV1(ctx, req)
	if berr != nil {
		t.Fatalf("batch of %d failed: %v (%v)", len(ids), berr, berr.Internal)
	}
	data, err := json.Marshal(jsonrpc.NewResponse(resp, 1))
	require.NoError(t, err)
	row := batchRow{ids: len(ids), respBytes: len(data)}
	for _, p := range resp.Proofs {
		cb := p.InclusionProof.CertificateBytes
		for _, b := range cb[:api.BitmapSize] {
			row.pathNodes += bits.OnesCount8(b)
		}
	}
	e.recs.mu.Lock()
	row.sizeQueries, row.loadQueries = e.recs.sizeQ, e.recs.calls
	e.recs.mu.Unlock()
	row.cost = measure.CostOf(costTime, func() {
		r, _ := e.svc.GetInclusionProofsV1(ctx, req)
		_, _ = json.Marshal(jsonrpc.NewResponse(r, 1))
	})
	return row
}

func geometric(from, to, factor int) []int {
	var out []int
	for x := from; x <= to; x *= factor {
		out = append(out, x)
	}
	return out
}

// Sweep 1: IDs per batch L, memory record source (no database latency).
func Test_ScalingBatchIDs(t *testing.T) {
	measure.Require(t)
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(256, 1755000000)
	e.fill(4096-256, 1)
	cert := e.finalizeCertified(8)

	var sb strings.Builder
	fmt.Fprintf(&sb, "N = 4096 leaves, 8-shard partition, ordinary UC %d B (4 x 65 B seal signatures), partition view %d B\n\n", len(cert.UC), len(cert.View))
	sb.WriteString("| L | response B | B per ID | proof path nodes (sum) | nodes per ID | DB size queries | DB record queries | allocs | alloc B | alloc B per ID | time relative to L=1 | time per ID relative |\n|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	var xs, resp, nodes, allocs, abytes, tm, dbq []float64
	var t1 float64
	for _, L := range geometric(1, 256, 2) {
		r := e.callBatch(t, ids[:L], true, 60*time.Millisecond)
		if L == 1 {
			t1 = r.cost.Nanos
		}
		fmt.Fprintf(&sb, "| %d | %d | %d | %d | %.1f | %d | %d | %.0f | %.0f | %.0f | %.1fx | %.2fx |\n", L, r.respBytes, r.respBytes/L, r.pathNodes, float64(r.pathNodes)/float64(L),
			r.sizeQueries, r.loadQueries, r.cost.Allocs, r.cost.Bytes, r.cost.Bytes/float64(L), r.cost.Nanos/t1, r.cost.Nanos/t1/float64(L))
		xs = append(xs, float64(L))
		resp = append(resp, float64(r.respBytes))
		nodes = append(nodes, float64(r.pathNodes))
		allocs = append(allocs, r.cost.Allocs)
		abytes = append(abytes, r.cost.Bytes)
		tm = append(tm, r.cost.Nanos)
		dbq = append(dbq, float64(r.sizeQueries+r.loadQueries))
		require.Equal(t, 1, r.sizeQueries)
		require.Equal(t, 1, r.loadQueries)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over L = 1..256: response bytes %s (expect 1), path nodes %s (expect 1: L x depth), DB queries %s (expect 0: constant 2), allocations %s (expect 1), allocated bytes %s (expect 1), time %s (expect ~1)\n",
		measure.FmtSlope(xs, resp), measure.FmtSlope(xs, nodes), measure.FmtSlope(xs, dbq), measure.FmtSlope(xs, allocs), measure.FmtSlope(xs, abytes), measure.FmtSlope(xs, tm))
	t.Logf("IDs-per-batch sweep, memory record source\n%s", sb.String())
}

// Sweep 2: SMT size N at fixed L = 16. Proofs traverse O(log N) nodes per ID.
func Test_ScalingBatchSMTSize(t *testing.T) {
	measure.Require(t)
	limits := proofview.DefaultLimits()
	limits.RetentionBudgetBytes = 1 << 40 // the retention budget is not under test; growing N in steps charges the old views
	e := newBatchEnv(t, limits)
	ids := e.addRecords(16, 1755000000)
	filled := 16

	var sb strings.Builder
	sb.WriteString("L = 16, 8-shard partition, ordinary UC with 4-signature seal\n\n| N (leaves) | log2 N | path nodes per ID | nodes per ID / log2 N | response B per ID | allocs per ID | alloc B per ID | time per ID relative to N=1e3 | DB queries |\n|---|---|---|---|---|---|---|---|---|\n")
	var xs, depth, rb, al, tm []float64
	var t0 float64
	for _, N := range []int{1_000, 3_000, 10_000, 30_000, 100_000, 300_000, 1_000_000} {
		e.fill(N-filled, uint64(N))
		filled = N
		e.finalizeCertified(8)
		r := e.callBatch(t, ids, true, 80*time.Millisecond)
		per := float64(r.pathNodes) / 16
		if t0 == 0 {
			t0 = r.cost.Nanos
		}
		fmt.Fprintf(&sb, "| %d | %.1f | %.2f | %.2f | %d | %.0f | %.0f | %.2fx | %d |\n", N, log2(float64(N)), per, per/log2(float64(N)), r.respBytes/16, r.cost.Allocs/16, r.cost.Bytes/16, r.cost.Nanos/t0, r.sizeQueries+r.loadQueries)
		xs = append(xs, float64(N))
		depth = append(depth, per)
		rb = append(rb, float64(r.respBytes))
		al = append(al, r.cost.Allocs)
		tm = append(tm, r.cost.Nanos)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over N = 1e3..1e6 (a logarithm has a slope near 1/ln N, here about 0.1-0.15): path nodes %s, response bytes %s, allocations %s, time %s. "+
		"A linear dependence on N would be slope 1; the proof cost is flat-to-logarithmic in N.\n", measure.FmtSlope(xs, depth), measure.FmtSlope(xs, rb), measure.FmtSlope(xs, al), measure.FmtSlope(xs, tm))
	t.Logf("SMT size sweep\n%s", sb.String())
}

func log2(x float64) float64 { return math.Log2(x) }

// Sweep 3: shards S of the partition: the ordinary UC (and the partition view)
// grow with log S and S respectively; exact-root responses omit the view.
func Test_ScalingBatchPartitionShards(t *testing.T) {
	measure.Require(t)
	var sb strings.Builder
	sb.WriteString("N = 4096, L = 16\n\n| S | ordinary UC B | partition view B | latest response B | exact-root response B | difference B (= view hex + key) |\n|---|---|---|---|---|---|\n")
	var xs, uc, latestB []float64
	for _, S := range []int{1, 2, 4, 8, 16} {
		e := newBatchEnv(t, proofview.DefaultLimits())
		ids := e.addRecords(16, 1755000000)
		e.fill(4096-16, 7)
		cert := e.finalizeCertified(S)
		exact := e.callBatch(t, ids, false, 0)
		if S > 8 {
			// no view above the production cap: latest cannot be served, only exact roots
			fmt.Fprintf(&sb, "| %d | %d | none (above the 8-shard cap) | n/a | %d | n/a |\n", S, len(cert.UC), exact.respBytes)
		} else {
			latest := e.callBatch(t, ids, true, 0)
			fmt.Fprintf(&sb, "| %d | %d | %d | %d | %d | %d |\n", S, len(cert.UC), len(cert.View), latest.respBytes, exact.respBytes, latest.respBytes-exact.respBytes)
		}
		xs = append(xs, float64(S))
		uc = append(uc, float64(len(cert.UC)))
		latestB = append(latestB, float64(exact.respBytes))
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over S = 1..16: ordinary UC bytes %s, exact-root response bytes %s (a logarithmic path term, slope well below 1)\n", measure.FmtSlope(xs, uc), measure.FmtSlope(xs, latestB))
	t.Logf("partition shard sweep\n%s", sb.String())
}

// Sweep 4: IDs per batch L against real MongoDB: the number of database commands is
// counted with a command monitor and must not depend on L.
func Test_ScalingBatchIDsMongo(t *testing.T) {
	measure.Require(t)
	ctx := context.Background()
	limits := proofview.DefaultLimits()
	limits.MaxLeaseDuration = 5 * time.Minute
	e := newBatchEnv(t, limits)
	e.svc.timeout = 5 * time.Minute
	ids := e.addRecords(256, 1755000000)
	e.fill(4096-256, 3)
	e.finalizeCertified(8)

	mc, err := mongodb.Run(ctx, "mongo:7.0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = mc.Terminate(context.Background()) })
	uri, err := mc.ConnectionString(ctx)
	require.NoError(t, err)
	commands := map[string]int{}
	monitor := &event.CommandMonitor{Started: func(_ context.Context, ev *event.CommandStartedEvent) { commands[ev.CommandName]++ }}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetMonitor(monitor))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	db := client.Database("scaling")
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
	block := models.NewBlock(api.NewBigIntFromUint64(1), "scaling", 0, "1", "test", api.HexBytes(make([]byte, 32)), nil, nil, 1755000000)
	block.Finalized, block.Status, block.ProposalID = true, models.FinalityStatusFinalized, "proposal-1"
	require.NoError(t, mongostore.NewBlockStorage(db).Store(ctx, block))
	e.svc.records = store

	var sb strings.Builder
	sb.WriteString("MongoDB 7.0 in a container, 256 finalized records. Before the batching change the service issued 2 x L sequential queries per batch.\n\n| L | aggregate commands per batch | all commands per batch | response B | time relative to L=1 | time per ID relative |\n|---|---|---|---|---|---|\n")
	var xs, cmds, tm []float64
	var t1 float64
	for _, L := range geometric(1, 256, 2) {
		req := e.request(ids[:L], nil)
		before := map[string]int{}
		for k, v := range commands {
			before[k] = v
		}
		resp, berr := e.svc.GetInclusionProofsV1(ctx, req)
		require.Nil(t, berr)
		agg := commands["aggregate"] - before["aggregate"]
		total := 0
		for k, v := range commands {
			total += v - before[k]
		}
		data, _ := json.Marshal(jsonrpc.NewResponse(resp, 1))
		c := measure.CostOf(300*time.Millisecond, func() { _, _ = e.svc.GetInclusionProofsV1(ctx, req) })
		if L == 1 {
			t1 = c.Nanos
		}
		fmt.Fprintf(&sb, "| %d | %d | %d | %d | %.1fx | %.2fx |\n", L, agg, total, len(data), c.Nanos/t1, c.Nanos/t1/float64(L))
		require.Equal(t, 2, agg, "L=%d: one size projection and one record load", L)
		xs = append(xs, float64(L))
		cmds = append(cmds, float64(agg))
		tm = append(tm, c.Nanos)
	}
	fmt.Fprintf(&sb, "\nLog-log slopes over L = 1..256: database aggregate commands %s (expect 0: constant), time %s (the fixed round trips dominate small L, so the curve is concave: the slope over the upper range L = 16..256 is %s)\n",
		measure.FmtSlope(xs, cmds), measure.FmtSlope(xs, tm), measure.FmtSlope(xs[4:], tm[4:]))
	t.Logf("IDs-per-batch sweep, real MongoDB\n%s", sb.String())
}
