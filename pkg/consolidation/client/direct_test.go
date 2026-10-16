package client_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/consolidationsim"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation/client"
)

func buildOmit(t *testing.T, n, perShard int, omit bool) (*consolidationsim.Sim, []client.Item) {
	sim, items := build(t, n, n, perShard)
	sim.OmitView = omit
	return sim, items
}

// An unsharded deployment consolidates naturally: one latest request, no view, no exact-root
// wave, no derivation, and the per-proof work is the ordinary one.
func TestDirect_UnshardedIsOneRequestWithoutAView(t *testing.T) {
	for _, perShard := range []int{1, 8, 64} {
		sim, items := buildOmit(t, 1, perShard, true)
		res := run(t, sim, items)
		c := res.Counters
		require.Equal(t, 1, res.Stats.Requests, "L=%d", perShard)
		require.Equal(t, 1, res.Stats.Waves)
		require.Zero(t, res.Stats.PartitionViewBytes)
		require.Len(t, sim.Shards[0].Calls(), 1)
		require.Equal(t, "latest", sim.Shards[0].Calls()[0].StateRoot)
		require.Zero(t, c.ViewLeafVisits)
		require.Zero(t, c.UCDerivations)
		require.Zero(t, c.ShardLeafProbes)
		require.Zero(t, c.ShardPathSteps)
		require.Equal(t, 1, c.QuorumChecks)
		require.Equal(t, 1, c.BodyChecks)
		require.Equal(t, perShard, c.LeafChecks, "per-proof checks are all kept")
		require.Equal(t, 1, c.TopologyValidations)

		// the replacement proofs pass the full legacy verifier under the signed configuration hash
		for i, it := range items {
			so := res.Shards[res.Order[i].Shard]
			p := so.Proofs[res.Order[i].Proof].InclusionProof
			require.NoError(t, p.Verify(it.Request, &api.VerifierContext{TrustBase: sim.TB, PartitionID: consolidationsim.PartitionID,
				ExpectedShardID: so.ShardID, ShardConfHash: sim.Policy.Topology.ShardConfHashes[so.ShardID.Key()]}))
		}
	}
}

// The unsharded path also works without any pinned topology.
func TestDirect_UnshardedWithoutPins(t *testing.T) {
	sim, items := buildOmit(t, 1, 3, true)
	c, err := client.New(sim.UnpinnedClientConfig())
	require.NoError(t, err)
	res, err := c.Consolidate(context.Background(), items)
	require.NoError(t, err)
	require.Equal(t, 1, res.Stats.Requests)
}

// The view stays required for a sharded partition: stripping the sidecar is refused, with the
// existing outcome, and no peer is contacted.
func TestDirect_ShardedMissingViewIsRefused(t *testing.T) {
	for _, n := range []int{2, 4, 8} {
		sim, items := buildOmit(t, n, 2, true)
		c, err := client.New(sim.ClientConfig())
		require.NoError(t, err)
		_, err = c.Consolidate(context.Background(), items)
		ce := requireClientErr(t, err, client.KindUnavailable, "partition_view_unavailable")
		require.Equal(t, consolidation.OutcomeUnavailable, ce.Outcome)
		contacted := 0
		for _, sh := range sim.Shards {
			contacted += len(sh.Calls())
		}
		require.Equal(t, 1, contacted, "n=%d: only the master was asked", n)
	}
}

// tamper rewrites every proof's UC of the master response through mut.
func tamperUC(t *testing.T, sim *consolidationsim.Sim, mut func(uc *types.UnicityCertificate)) {
	sh := sim.Shards[0]
	sh.Hook = func(w http.ResponseWriter, body []byte) bool {
		sh.Hook = nil
		env := serveReal(sh, body)
		res := env["result"].(map[string]any)
		proofs := res["proofs"].([]any)
		for i, p := range proofs {
			raw := mustHex(t, p.(string))
			var pr api.GetInclusionProofResponseV2
			require.NoError(t, types.Cbor.Unmarshal(raw, &pr))
			var uc types.UnicityCertificate
			require.NoError(t, types.Cbor.Unmarshal(pr.InclusionProof.UnicityCertificate, &uc))
			mut(&uc)
			var err error
			pr.InclusionProof.UnicityCertificate, err = types.Cbor.Marshal(&uc)
			require.NoError(t, err)
			enc, err := types.Cbor.Marshal(&pr)
			require.NoError(t, err)
			proofs[i] = hexString(enc)
		}
		_ = json.NewEncoder(w).Encode(env)
		return true
	}
}

func TestDirect_TamperedUnshardedUCIsAnIntegrityError(t *testing.T) {
	zero := make([]byte, 32)
	for name, mut := range map[string]func(*types.UnicityCertificate){
		"nonempty shard path under an empty shard ID": func(uc *types.UnicityCertificate) { uc.ShardTreeCertificate.SiblingHashes = [][]byte{zero} },
		"altered signature witness": func(uc *types.UnicityCertificate) {
			for k := range uc.UnicitySeal.Signatures {
				uc.UnicitySeal.Signatures[k][0] ^= 0xff
			}
		},
		"missing unicity tree certificate": func(uc *types.UnicityCertificate) { uc.UnicityTreeCertificate = nil },
	} {
		t.Run(name, func(t *testing.T) {
			sim, items := buildOmit(t, 1, 2, true)
			tamperUC(t, sim, mut)
			c, err := client.New(sim.ClientConfig())
			require.NoError(t, err)
			_, err = c.Consolidate(context.Background(), items)
			ce := requireClientErr(t, err, client.KindVerification, "")
			require.NotEqual(t, consolidation.OutcomeUnavailable, ce.Outcome, "a malformed or unauthenticated UC is never 'unavailable'")
			require.Len(t, sim.Shards[0].Calls(), 1, "no retry, no fallback request")
		})
	}
}

// Items routed to several endpoint groups cannot be served by one unsharded UC: no speculative peer requests.
func TestDirect_MixedRoutingGroupsFailWithoutPeerRequests(t *testing.T) {
	simA, items := buildOmit(t, 1, 6, true) // an unsharded partition's states
	simB, _ := buildOmit(t, 2, 1, true)     // a sharded deployment's endpoints
	cfg := simA.ClientConfig()
	cfg.Policy = simB.Policy
	cfg.Endpoints = simB.ClientConfig().Endpoints
	cfg.Resolver = simA.Resolver()
	mshard := simB.Shards[0].ID
	cfg.Master = &mshard
	c, err := client.New(cfg)
	require.NoError(t, err)
	_, err = c.Consolidate(context.Background(), items)
	require.Error(t, err)
	for _, sh := range simB.Shards[1:] {
		require.Empty(t, sh.Calls(), "no peer request")
	}
}

func mustHex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func hexString(b []byte) string { return hex.EncodeToString(b) }
