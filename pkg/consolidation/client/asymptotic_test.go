package client_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/consolidationsim"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation/client"
)

// build certifies perShard states on each of the first `used` shards of an n-shard
// partition in a single finalization and returns the original items.
func build(t *testing.T, n, used, perShard int) (*consolidationsim.Sim, []client.Item) {
	sim, err := consolidationsim.New(n)
	require.NoError(t, err)
	t.Cleanup(sim.Close)
	var reqs [][]*api.CertificationRequest
	for i := 0; i < used; i++ {
		reqs = append(reqs, sim.Add(i, perShard))
	}
	f := sim.Finalize()
	var items []client.Item
	for i := 0; i < used; i++ {
		for _, r := range reqs[i] {
			items = append(items, sim.Item(i, r, f))
		}
	}
	sim.Finalize()
	return sim, items
}

func run(t *testing.T, sim *consolidationsim.Sim, items []client.Item) *client.Result {
	c, err := client.New(sim.ClientConfig())
	require.NoError(t, err)
	res, err := c.Consolidate(context.Background(), items)
	require.NoError(t, err)
	return res
}

// Sweeping the number of leaves L at a fixed partition: topology work, UC decodes and body
// checks are flat (one per distinct UC), only per-input leaf work grows, linearly.
func TestSession_WorkDoesNotGrowWithLeavesBeyondLeafChecks(t *testing.T) {
	const S = 4
	for _, perShard := range []int{1, 4, 16, 64} {
		sim, items := build(t, S, S, perShard)
		res := run(t, sim, items)
		c := res.Counters
		L := S * perShard
		require.Equal(t, 1, c.TopologyValidations, "L=%d: topology is validated once per session", L)
		require.Equal(t, L, c.LeafChecks, "leaf checks are linear in L")
		require.Equal(t, S, c.EvidenceBodyChecks, "L=%d: one evidence body check per distinct UC", L)
		require.Equal(t, 2*S+1, c.UCDecodes, "L=%d: S evidence UCs + master + m source UCs, independent of L", L)
		require.Equal(t, 1, c.QuorumChecks)
		require.Equal(t, S, c.BodyChecks)
		require.Equal(t, S, c.UCDerivations, "only the m participating UCs are derived")
		require.Equal(t, 2*S-1, c.ShardLeafProbes, "one probe per lookup: m-1 peer roots + m completions")
		require.Equal(t, S, res.Stats.Requests)
	}
}

// Sweeping the partition size S at a fixed number of participating shards m=2 (and fixed L):
// nothing grows with S.
func TestSession_WorkDoesNotGrowWithPartitionSize(t *testing.T) {
	const m, perShard = 2, 3
	var base *client.Result
	for _, S := range []int{2, 4, 8} {
		sim, items := build(t, S, m, perShard)
		res := run(t, sim, items)
		c := res.Counters
		require.Equal(t, 1, c.TopologyValidations, "S=%d", S)
		require.Equal(t, m, c.UCDerivations, "S=%d: lazy derivation, not S certificates", S)
		require.Equal(t, 2*m-1, c.ShardLeafProbes, "S=%d: indexed shard lookups", S)
		require.Equal(t, m*perShard, c.LeafChecks)
		require.Equal(t, m, c.BodyChecks)
		if base == nil {
			base = res
		}
		require.Equal(t, base.Counters.UCDecodes, c.UCDecodes, "S=%d", S)
		require.Equal(t, base.Counters.EvidenceBodyChecks, c.EvidenceBodyChecks, "S=%d", S)
		require.Equal(t, m, res.Stats.Requests, "only the participating shards are contacted")
	}
}

// Evidence UC work is linear in the number of DISTINCT certificates (E), not in the leaves:
// states certified in F separate finalizations give F distinct evidence UCs.
func TestSession_EvidenceWorkIsLinearInDistinctCertificates(t *testing.T) {
	for _, F := range []int{1, 2, 4, 8} {
		sim, err := consolidationsim.New(2)
		require.NoError(t, err)
		t.Cleanup(sim.Close)
		var items []client.Item
		for f := 0; f < F; f++ {
			reqs := sim.Add(0, 5) // 5 states per finalization, all on shard 0
			fin := sim.Finalize()
			for _, r := range reqs {
				items = append(items, sim.Item(0, r, fin))
			}
		}
		sim.Finalize()
		res := run(t, sim, items)
		c := res.Counters
		require.Equal(t, F, c.EvidenceBodyChecks, "F=%d: one body check per distinct evidence UC", F)
		require.Equal(t, F+1+1, c.UCDecodes, "F=%d: F evidence UCs + master + 1 source (m=1)", F)
		require.Equal(t, F, c.EvidenceQuorumChecks, "one seal verification per distinct evidence seal")
		require.Equal(t, 5*F, c.LeafChecks)
		require.Equal(t, 1, c.TopologyValidations)
		t.Log(fmt.Sprintf("F=%d decodes=%d bodies=%d", F, c.UCDecodes, c.EvidenceBodyChecks))
	}
}

func TestClient_RoutingVisitsAreDepthPerItem(t *testing.T) {
	for _, S := range []int{1, 2, 4, 8} {
		sim, items := build(t, S, S, 3)
		res := run(t, sim, items)
		depth := map[int]int{1: 0, 2: 1, 4: 2, 8: 3}[S]
		require.Equal(t, len(items)*(depth+1), res.Stats.RoutingVisits, "S=%d: L·(depth+1) trie visits", S)
	}
}

// Certificate derivation never scans the view per participating shard: the view is visited a
// constant number of times per master (index + private copy + tree build = 3S), whatever m is,
// and each derivation costs only its shard path depth.
func TestSession_DerivationDoesNotScanTheViewPerShard(t *testing.T) {
	const S = 8
	for _, m := range []int{1, 2, 4, 8} {
		sim, items := build(t, S, m, 2)
		c := run(t, sim, items).Counters
		require.Equal(t, m, c.UCDerivations)
		require.Equal(t, 3*S, c.ViewLeafVisits, "m=%d: Θ(S) once, not Θ(m·S)", m)
		require.Equal(t, 3*m, c.ShardPathSteps, "m=%d: the sum of the %d shard depths (3 each)", m, m)
	}
	// and the visits grow linearly with S (not with m·S) when sweeping the partition size
	for _, S := range []int{2, 4, 8} {
		sim, items := build(t, S, 2, 2)
		c := run(t, sim, items).Counters
		require.Equal(t, 3*S, c.ViewLeafVisits, "S=%d", S)
		depth := map[int]int{2: 1, 4: 2, 8: 3}[S]
		require.Equal(t, 2*depth, c.ShardPathSteps, "S=%d", S)
	}
}
