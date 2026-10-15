package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/consolidationsim"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation/client"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// scenario certifies perShard states on every shard of an n-shard partition,
// in two separate finalizations (so the original proofs carry mixed seals),
// then finalizes once more so "latest" moves on from the originals.
func scenario(t *testing.T, n, perShard int) (*consolidationsim.Sim, []client.Item) {
	sim, err := consolidationsim.New(n)
	require.NoError(t, err)
	t.Cleanup(sim.Close)
	var reqs [][]*api.CertificationRequest
	for i := range sim.Shards {
		reqs = append(reqs, sim.Add(i, perShard))
	}
	f1 := sim.Finalize()
	var items []client.Item
	for i := range sim.Shards {
		for _, r := range reqs[i] {
			items = append(items, sim.Item(i, r, f1))
		}
	}
	// every other shard certifies one more state in a second finalization, and the
	// last shard in a third: the newest evidence picks the last shard as master
	last := len(sim.Shards) - 1
	if last > 0 {
		var extra []*api.CertificationRequest
		for i := 0; i < last; i++ {
			extra = append(extra, sim.Add(i, 1)...)
		}
		f2 := sim.Finalize()
		for i := 0; i < last; i++ {
			items = append(items, sim.Item(i, extra[i], f2))
		}
	}
	more := sim.Add(last, 1)
	f3 := sim.Finalize()
	items = append(items, sim.Item(last, more[0], f3))
	sim.Finalize() // latest moves on: every shard's root differs from the originals
	return sim, items
}

func newClient(t *testing.T, sim *consolidationsim.Sim, mut func(*client.Config)) *client.Client {
	cfg := sim.ClientConfig()
	if mut != nil {
		mut(&cfg)
	}
	c, err := client.New(cfg)
	require.NoError(t, err)
	return c
}

func TestConsolidate_MasterFirstRequestPattern(t *testing.T) {
	for _, n := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("%d shards", n), func(t *testing.T) {
			sim, items := scenario(t, n, 3)
			res, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
			require.NoError(t, err)

			// m requests in two waves (one request for m=1); exactly one per shard
			require.Equal(t, n, res.Stats.Requests)
			wantWaves := 2
			if n == 1 {
				wantWaves = 1
			}
			require.Equal(t, wantWaves, res.Stats.Waves)
			latest, exact := 0, 0
			for i, sh := range sim.Shards {
				calls := sh.Calls()
				require.Len(t, calls, 1, "shard %d", i)
				require.Equal(t, client.Method, calls[0].Method)
				if calls[0].StateRoot == "latest" {
					latest++
					require.Equal(t, len(sim.Shards)-1, i, "the newest evidence's shard is master")
				} else {
					exact++
				}
			}
			require.Equal(t, 1, latest)
			require.Equal(t, n-1, exact)

			// instrumentation: 1 master quorum check, m bodies, L leaves
			require.Equal(t, 1, res.Counters.QuorumChecks)
			require.Equal(t, n, res.Counters.BodyChecks)
			require.Equal(t, len(items), res.Counters.LeafChecks)
			require.Greater(t, res.Stats.PartitionViewBytes, 0)

			// every item maps to a replacement proof under the common master seal that the legacy verifier accepts
			masterSeal, err := res.Shards[0].UC.UnicitySeal.MarshalCBOR()
			require.NoError(t, err)
			for i, it := range items {
				pos := res.Order[i]
				so := res.Shards[pos.Shard]
				sb, err := so.UC.UnicitySeal.MarshalCBOR()
				require.NoError(t, err)
				require.Equal(t, masterSeal, sb)
				p := so.Proofs[pos.Proof].InclusionProof
				require.NoError(t, p.Verify(it.Request, &api.VerifierContext{TrustBase: sim.TB, PartitionID: consolidationsim.PartitionID,
					ExpectedShardID: so.ShardID, ShardConfHash: sim.Policy.Topology.ShardConfHashes[so.ShardID.Key()]}))
				require.Equal(t, *it.Proof.ReferenceTime, *p.ReferenceTime)
			}
		})
	}
}

func TestConsolidate_CallerSelectedMaster(t *testing.T) {
	sim, items := scenario(t, 4, 2)
	m := sim.Shards[1].ID
	res, err := newClient(t, sim, func(c *client.Config) { c.Master = &m }).Consolidate(context.Background(), items)
	require.NoError(t, err)
	require.True(t, res.MasterShard.Equal(m))
	require.Equal(t, "latest", sim.Shards[1].Calls()[0].StateRoot)

	sim2, items2 := scenario(t, 2, 1)
	bad := types.ShardID{}
	_, err = newClient(t, sim2, func(c *client.Config) { c.Master = &bad }).Consolidate(context.Background(), items2)
	var ce *client.Error
	require.ErrorAs(t, err, &ce)
	require.Equal(t, client.KindInput, ce.Kind)
}

func TestConsolidate_InputsUntouchedAndNeverSubmits(t *testing.T) {
	sim, items := scenario(t, 4, 2)
	before := snapshotItems(t, items)
	var other atomic.Int32
	for _, sh := range sim.Shards {
		sh.Hook = func(w http.ResponseWriter, body []byte) bool {
			var r struct{ Method string }
			_ = json.Unmarshal(body, &r)
			if r.Method != client.Method {
				other.Add(1)
			}
			return false
		}
	}
	_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
	require.NoError(t, err)
	require.Equal(t, before, snapshotItems(t, items), "inputs are never modified")
	require.Zero(t, other.Load(), "only get_inclusion_proofs.v1 is ever called")

	// also after a failure
	sim.Shards[2].Hook = func(w http.ResponseWriter, body []byte) bool {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1,
			"error": jsonrpc.NewDetailedError(jsonrpc.ProofUnavailableCode, "Proof unavailable", "proof_unavailable", "view_not_retained", true, nil)})
		return true
	}
	_, err = newClient(t, sim, nil).Consolidate(context.Background(), items)
	require.Error(t, err)
	require.Equal(t, before, snapshotItems(t, items))
	require.Zero(t, other.Load())
}

func snapshotItems(t *testing.T, items []client.Item) [][]byte {
	var out [][]byte
	for _, it := range items {
		a, err := types.Cbor.Marshal(it.Request)
		require.NoError(t, err)
		b, err := types.Cbor.Marshal(it.Proof)
		require.NoError(t, err)
		out = append(out, a, b)
	}
	return out
}

func requireClientErr(t *testing.T, err error, kind client.Kind, detail string) *client.Error {
	t.Helper()
	var ce *client.Error
	require.ErrorAs(t, err, &ce, "%v", err)
	require.Equal(t, kind, ce.Kind, ce.Error())
	if detail != "" {
		require.Equal(t, detail, ce.Detail, ce.Error())
	}
	return ce
}

func TestConsolidate_UnavailableSemantics(t *testing.T) {
	t.Run("evicted peer view", func(t *testing.T) {
		sim, items := scenario(t, 4, 2)
		sim.Retain = 1
		master := sim.Shards[len(sim.Shards)-1]
		master.Hook = func(w http.ResponseWriter, body []byte) bool {
			// the partition certifies again between the two waves: the roots in the
			// master view are no longer the newest and retention keeps only one
			master.Hook = nil
			return false
		}
		peer := sim.Shards[0]
		peer.Hook = func(w http.ResponseWriter, body []byte) bool {
			sim.Add(0, 1)
			sim.Finalize()
			sim.Add(0, 1)
			sim.Finalize()
			peer.Hook = nil
			return false
		}
		_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
		ce := requireClientErr(t, err, client.KindUnavailable, "view_not_retained")
		require.Equal(t, jsonrpc.ProofUnavailableCode, ce.Code)
	})
	t.Run("snapshot excludes a required leaf", func(t *testing.T) {
		sim, items := scenario(t, 2, 1)
		extra := sim.Add(0, 1)
		// a state that was never finalized: no snapshot includes it
		items = append(items, client.Item{Request: extra[0], Proof: items[0].Proof})
		_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
		require.Error(t, err)
		var ce *client.Error
		require.ErrorAs(t, err, &ce)
		require.True(t, ce.Kind == client.KindVerification || ce.Kind == client.KindUnavailable)
	})
	t.Run("unknown R_min is unavailable", func(t *testing.T) {
		sim, _ := scenario(t, 2, 1)
		_, err := newClient(t, sim, nil).Consolidate(context.Background(), nil)
		requireClientErr(t, err, client.KindInput, "")
	})
	t.Run("rpc proof_unavailable and busy", func(t *testing.T) {
		for _, tc := range []struct {
			code           int
			reason, detail string
		}{
			{jsonrpc.ProofUnavailableCode, "proof_unavailable", "view_not_retained"},
			{jsonrpc.ProofUnavailableCode, "proof_unavailable", "state_not_included"},
			{jsonrpc.ConcurrencyLimitCode, "busy", "pinned_views"},
		} {
			sim, items := scenario(t, 2, 1)
			sim.Shards[len(sim.Shards)-1].Hook = func(w http.ResponseWriter, body []byte) bool {
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1,
					"error": jsonrpc.NewDetailedError(tc.code, "x", tc.reason, tc.detail, true, nil)})
				return true
			}
			_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
			ce := requireClientErr(t, err, client.KindUnavailable, tc.detail)
			require.Equal(t, tc.code, ce.Code)
			require.True(t, client.IsUnavailable(err))
		}
	})
	t.Run("no retry after a failure", func(t *testing.T) {
		sim, items := scenario(t, 4, 1)
		var hits atomic.Int32
		master := sim.Shards[len(sim.Shards)-1]
		master.Hook = func(w http.ResponseWriter, body []byte) bool {
			hits.Add(1)
			http.Error(w, "boom", http.StatusBadGateway)
			return true
		}
		_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
		requireClientErr(t, err, client.KindUnavailable, "http_502")
		require.EqualValues(t, 1, hits.Load())
		for i, sh := range sim.Shards[:len(sim.Shards)-1] {
			require.Empty(t, sh.Calls(), "peer %d is not contacted when the master fails", i)
		}
	})
	t.Run("rejected request is not retryable unavailability", func(t *testing.T) {
		sim, items := scenario(t, 2, 1)
		sim.Shards[1].Hook = func(w http.ResponseWriter, body []byte) bool {
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1,
				"error": jsonrpc.NewDetailedError(jsonrpc.LimitExceededCode, "Limit exceeded", "limit_exceeded", "state_count", false, nil)})
			return true
		}
		_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
		require.False(t, client.IsUnavailable(err))
	})
}

func TestConsolidate_DeadlineAndCancellation(t *testing.T) {
	sim, items := scenario(t, 4, 1)
	for _, sh := range sim.Shards {
		sh.Delay = 2 * time.Second
	}
	start := time.Now()
	_, err := newClient(t, sim, func(c *client.Config) { c.Timeout = 100 * time.Millisecond }).Consolidate(context.Background(), items)
	requireClientErr(t, err, client.KindUnavailable, "deadline")
	require.Less(t, time.Since(start), time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	start = time.Now()
	_, err = newClient(t, sim, nil).Consolidate(ctx, items)
	requireClientErr(t, err, client.KindUnavailable, "cancelled")
	require.Less(t, time.Since(start), time.Second)
}

func TestConsolidate_SecondWaveIsBoundedParallel(t *testing.T) {
	sim, items := scenario(t, 8, 1)
	var cur, peak atomic.Int32
	for _, sh := range sim.Shards {
		sh.Hook = func(w http.ResponseWriter, body []byte) bool {
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(40 * time.Millisecond)
			cur.Add(-1)
			return false
		}
	}
	_, err := newClient(t, sim, func(c *client.Config) { c.MaxParallel = 3 }).Consolidate(context.Background(), items)
	require.NoError(t, err)
	require.LessOrEqual(t, peak.Load(), int32(3))
	require.GreaterOrEqual(t, peak.Load(), int32(2), "second wave runs in parallel")
}

func TestConsolidate_VerificationFailuresAreTyped(t *testing.T) {
	t.Run("tampered peer response", func(t *testing.T) {
		sim, items := scenario(t, 2, 1)
		// peer answers a different (older) root's proofs: wrong root
		sim.Shards[0].Hook = func(w http.ResponseWriter, body []byte) bool {
			var r struct {
				Params json.RawMessage `json:"params"`
			}
			_ = json.Unmarshal(body, &r)
			pr, _ := api.ParseGetInclusionProofsRequestV1(r.Params)
			if pr == nil || pr.Root == nil {
				return false
			}
			// reply as if rooted elsewhere: serve the original (older) proofs
			var proofs []*api.GetInclusionProofResponseV2
			for _, i := range []int{0, 2} { // shard 0's items with their original (older) proofs
				proofs = append(proofs, &api.GetInclusionProofResponseV2{BlockNumber: 1, InclusionProof: items[i].Proof})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"proofs": proofs}})
			return true
		}
		_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
		ce := requireClientErr(t, err, client.KindVerification, "")
		require.Contains(t, []consolidation.Outcome{consolidation.OutcomeBindingMismatch, consolidation.OutcomeInvalidInput}, ce.Outcome)
	})
	t.Run("wrong authority", func(t *testing.T) {
		sim, items := scenario(t, 2, 1)
		other, err := consolidationsim.NewWithSeed(2, "another authority")
		require.NoError(t, err)
		defer other.Close()
		_, err = newClient(t, sim, func(c *client.Config) { c.Resolver = other.Resolver() }).Consolidate(context.Background(), items)
		ce := requireClientErr(t, err, client.KindVerification, "")
		require.Equal(t, consolidation.OutcomeUnauthenticated, ce.Outcome)
	})
	t.Run("garbage response", func(t *testing.T) {
		sim, items := scenario(t, 2, 1)
		sim.Shards[1].Hook = func(w http.ResponseWriter, body []byte) bool {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"proofs":["zz"]}}`))
			return true
		}
		_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
		requireClientErr(t, err, client.KindInvalidResponse, "")
	})
	t.Run("missing partition view", func(t *testing.T) {
		sim, items := scenario(t, 2, 1)
		m := sim.Shards[1]
		m.Hook = func(w http.ResponseWriter, body []byte) bool {
			var r struct {
				ID json.RawMessage `json:"id"`
			}
			_ = json.Unmarshal(body, &r)
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": map[string]any{"proofs": []any{}}})
			return true
		}
		_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
		requireClientErr(t, err, client.KindInvalidResponse, "")
	})
}

func TestNew_Validation(t *testing.T) {
	sim, _ := scenario(t, 2, 1)
	cfg := sim.ClientConfig()
	cfg.Resolver = nil
	_, err := client.New(cfg)
	requireClientErr(t, err, client.KindInput, "")
	cfg = sim.ClientConfig()
	cfg.Endpoints = append(cfg.Endpoints, cfg.Endpoints[0])
	_, err = client.New(cfg)
	requireClientErr(t, err, client.KindInput, "")
	cfg = sim.ClientConfig()
	cfg.Endpoints[0].URL = ""
	_, err = client.New(cfg)
	requireClientErr(t, err, client.KindInput, "")
}
