// Command consolidate is a runnable, self-contained example of master-first
// proof consolidation. It starts an in-process partition of N shard
// aggregators (N = 1, 2, 4 or 8), certifies a few states per shard in two
// separate rounds (so the original proofs carry different seals), then
// consolidates them under one master seal with pkg/consolidation/client.
//
//	go run ./examples/consolidate -shards 8
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/unicitynetwork/aggregator-go/internal/consolidationsim"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation/client"
)

func main() {
	shards := flag.Int("shards", 4, "number of shards (1, 2, 4 or 8)")
	perShard := flag.Int("states", 3, "states per shard")
	flag.Parse()
	if err := run(*shards, *perShard); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(n, perShard int) error {
	sim, err := consolidationsim.New(n)
	if err != nil {
		return err
	}
	defer sim.Close()
	// an unsharded partition needs no partition view: the UC alone is the target
	sim.OmitView = n == 1

	var items []client.Item
	var reqs [][]*api.CertificationRequest
	for i := range sim.Shards {
		reqs = append(reqs, sim.Add(i, perShard))
	}
	f1 := sim.Finalize()
	for i := range sim.Shards {
		for _, r := range reqs[i] {
			items = append(items, sim.Item(i, r, f1))
		}
	}
	last := len(sim.Shards) - 1
	more := sim.Add(last, 1)
	items = append(items, sim.Item(last, more[0], sim.Finalize()))
	sim.Finalize() // the partition keeps certifying: latest is newer than every original proof

	fmt.Printf("partition of %d shard(s), %d original proofs under %d different seals\n", n, len(items), 2)
	c, err := client.New(sim.ClientConfig())
	if err != nil {
		return err
	}
	res, err := c.Consolidate(context.Background(), items)
	if err != nil {
		return err
	}
	fmt.Printf("master shard %q, master seal root round %d\n", res.MasterShard, res.RootRound)
	fmt.Printf("requests: %d in %d wave(s); partition view: %d bytes\n", res.Stats.Requests, res.Stats.Waves, res.Stats.PartitionViewBytes)
	fmt.Printf("verification: %d quorum check(s), %d shard body check(s), %d leaf check(s)\n",
		res.Counters.QuorumChecks, res.Counters.BodyChecks, res.Counters.LeafChecks)
	for i, it := range items {
		pos := res.Order[i]
		so := res.Shards[pos.Shard]
		p := so.Proofs[pos.Proof].InclusionProof
		err := p.Verify(it.Request, &api.VerifierContext{TrustBase: sim.TB, PartitionID: consolidationsim.PartitionID,
			ExpectedShardID: so.ShardID, ShardConfHash: sim.Policy.Topology.ShardConfHashes[so.ShardID.Key()]})
		if err != nil {
			return fmt.Errorf("proof %d fails the full verifier: %w", i, err)
		}
	}
	fmt.Printf("all %d consolidated proofs pass the full v2 inclusion proof verifier under the common seal\n", len(items))
	return nil
}
