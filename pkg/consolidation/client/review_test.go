package client_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/consolidationsim"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation/client"
)

func peerCalls(sim *consolidationsim.Sim) int {
	n := 0
	for _, sh := range sim.Shards[:len(sim.Shards)-1] {
		n += len(sh.Calls())
	}
	return n
}

// tamperMaster rewrites the master's response through fn (a decoded result object).
func tamperMaster(sim *consolidationsim.Sim, fn func(res map[string]any)) {
	m := sim.Shards[len(sim.Shards)-1]
	m.Hook = func(w http.ResponseWriter, body []byte) bool {
		m.Hook = nil
		env := serveReal(m, body)
		fn(env["result"].(map[string]any))
		_ = json.NewEncoder(w).Encode(env)
		return true
	}
}

// serveReal runs the shard's real handler and returns the decoded envelope.
func serveReal(sh *consolidationsim.Shard, body []byte) map[string]any {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	sh.Server.Config.Handler.ServeHTTP(rec, req)
	var env map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	return env
}

func corruptSeal(t *testing.T, res map[string]any) {
	proofs := res["proofs"].([]any)
	for i, p := range proofs {
		raw, err := hex.DecodeString(p.(string))
		require.NoError(t, err)
		var pr api.GetInclusionProofResponseV2
		require.NoError(t, types.Cbor.Unmarshal(raw, &pr))
		var uc types.UnicityCertificate
		require.NoError(t, types.Cbor.Unmarshal(pr.InclusionProof.UnicityCertificate, &uc))
		for k := range uc.UnicitySeal.Signatures {
			uc.UnicitySeal.Signatures[k][0] ^= 0xff
		}
		pr.InclusionProof.UnicityCertificate, err = types.Cbor.Marshal(&uc)
		require.NoError(t, err)
		raw, err = types.Cbor.Marshal(&pr)
		require.NoError(t, err)
		proofs[i] = hex.EncodeToString(raw)
	}
}

func corruptView(t *testing.T, res map[string]any) {
	raw, err := hex.DecodeString(res["partitionView"].(string))
	require.NoError(t, err)
	v, err := types.DecodePartitionViewV1(raw)
	require.NoError(t, err)
	v.Leaves[0].InputRecord.RoundNumber++
	raw, err = v.MarshalCBOR()
	require.NoError(t, err)
	res["partitionView"] = hex.EncodeToString(raw)
}

// Master authentication happens before any peer request.
func TestConsolidate_MasterAuthenticatedBeforePeers(t *testing.T) {
	t.Run("bad master seal signature", func(t *testing.T) {
		sim, items := scenario(t, 4, 1)
		for _, sh := range sim.Shards[:3] {
			sh.Hook = func(w http.ResponseWriter, _ []byte) bool {
				http.Error(w, "down", http.StatusServiceUnavailable)
				return true
			}
		}
		tamperMaster(sim, func(res map[string]any) { corruptSeal(t, res) })
		_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
		ce := requireClientErr(t, err, client.KindVerification, "")
		require.Equal(t, consolidation.OutcomeUnauthenticated, ce.Outcome)
		require.Zero(t, peerCalls(sim), "no peer is contacted for an invalid master")
	})
	t.Run("view of another partition state", func(t *testing.T) {
		sim, items := scenario(t, 4, 1)
		tamperMaster(sim, func(res map[string]any) { corruptView(t, res) })
		_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
		ce := requireClientErr(t, err, client.KindVerification, "")
		require.Contains(t, []consolidation.Outcome{consolidation.OutcomeInvalidInput, consolidation.OutcomeUnauthenticated}, ce.Outcome)
		require.Zero(t, peerCalls(sim))
	})
	t.Run("insufficient master round", func(t *testing.T) {
		sim, items := scenario(t, 4, 1)
		// the newest evidence is in finalization 3; a master that answers with the
		// (genuine) finalization 2 is below R_min
		m := sim.Shards[0]
		m.Hook = func(w http.ResponseWriter, body []byte) bool {
			m.Hook = nil
			sim.UseFinalization(2)
			return false
		}
		master := sim.Shards[0].ID
		_, err := newClient(t, sim, func(c *client.Config) { c.Master = &master }).Consolidate(context.Background(), items)
		ce := requireClientErr(t, err, client.KindVerification, "")
		require.Equal(t, consolidation.OutcomeStale, ce.Outcome)
		require.Equal(t, 0, peerCalls2(sim, 0))
	})
}

func peerCalls2(sim *consolidationsim.Sim, master int) int {
	n := 0
	for i, sh := range sim.Shards {
		if i != master {
			n += len(sh.Calls())
		}
	}
	return n
}

// Cancellation and deadlines are honored during local verification and before success.
func TestConsolidate_ContextHonoredDuringVerification(t *testing.T) {
	t.Run("cancelled by the resolver during final verification (one shard)", func(t *testing.T) {
		sim, items := scenario(t, 1, 2)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var calls atomic.Int32
		res, err := newClient(t, sim, func(c *client.Config) {
			inner := c.Resolver
			c.Resolver = func(s *types.UnicitySeal) (types.RootTrustBase, error) {
				if calls.Add(1) > 2 { // after evidence + master authentication: final stage
					cancel()
				}
				return inner(s)
			}
		}).Consolidate(ctx, items)
		require.Nil(t, res, "no success for a cancelled attempt")
		ce := requireClientErr(t, err, client.KindCancelled, "cancelled")
		require.False(t, ce.Retryable, "caller cancellation is never retryable")
		require.True(t, client.IsCancelled(err))
		require.False(t, client.IsUnavailable(err))
	})
	t.Run("deadline expiring inside the resolver", func(t *testing.T) {
		sim, items := scenario(t, 2, 1)
		res, err := newClient(t, sim, func(c *client.Config) {
			c.Timeout = 80 * time.Millisecond
			inner := c.Resolver
			c.Resolver = func(s *types.UnicitySeal) (types.RootTrustBase, error) {
				time.Sleep(150 * time.Millisecond)
				return inner(s)
			}
		}).Consolidate(context.Background(), items)
		require.Nil(t, res)
		requireClientErr(t, err, client.KindTimeout, "deadline")
	})
	t.Run("already cancelled", func(t *testing.T) {
		sim, items := scenario(t, 2, 1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := newClient(t, sim, nil).Consolidate(ctx, items)
		require.True(t, client.IsCancelled(err), "%v", err)
	})
}

// A deadline reaching queued second-wave work is typed unavailable.
func TestConsolidate_DeadlineWithQueuedPeers(t *testing.T) {
	sim, items := scenario(t, 8, 1)
	for _, sh := range sim.Shards[:7] {
		sh.Delay = 2 * time.Second
	}
	_, err := newClient(t, sim, func(c *client.Config) { c.MaxParallel = 1; c.Timeout = 300 * time.Millisecond }).Consolidate(context.Background(), items)
	requireClientErr(t, err, client.KindTimeout, "deadline")
	require.False(t, client.IsUnavailable(err), "an expired deadline is not proof unavailability")
}

func TestConsolidate_QueuedCancellationKeepsOriginatingFailure(t *testing.T) {
	sim, items := scenario(t, 8, 1)
	// peer 0 fails fast with a rejection; queued peers (MaxParallel=1) are cancelled afterwards
	sim.Shards[0].Hook = func(w http.ResponseWriter, _ []byte) bool {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1,
			"error": map[string]any{"code": -32602, "message": "x", "data": map[string]any{"reason": "invalid_request", "detail": "wrong_shard"}}})
		return true
	}
	for _, sh := range sim.Shards[1:7] {
		sh.Delay = time.Second
	}
	_, err := newClient(t, sim, func(c *client.Config) { c.MaxParallel = 1 }).Consolidate(context.Background(), items)
	requireClientErr(t, err, client.KindRejected, "wrong_shard")
}

// The JSON-RPC envelope is validated before its content is classified.
func TestConsolidate_EnvelopeValidation(t *testing.T) {
	cases := map[string]func(env map[string]any){
		"wrong id":         func(env map[string]any) { env["id"] = 99 },
		"null id":          func(env map[string]any) { env["id"] = nil },
		"wrong version":    func(env map[string]any) { env["jsonrpc"] = "1.0" },
		"missing version":  func(env map[string]any) { delete(env, "jsonrpc") },
		"result and error": func(env map[string]any) { env["error"] = map[string]any{"code": -32008, "message": "x"} },
		"neither":          func(env map[string]any) { delete(env, "result") },
		"null result":      func(env map[string]any) { env["result"] = nil },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			sim, items := scenario(t, 2, 1)
			m := sim.Shards[len(sim.Shards)-1]
			m.Hook = func(w http.ResponseWriter, body []byte) bool {
				m.Hook = nil
				env := serveReal(m, body)
				mut(env)
				_ = json.NewEncoder(w).Encode(env)
				return true
			}
			_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
			requireClientErr(t, err, client.KindInvalidResponse, "")
		})
	}
}

var _ = api.StateTreeKeyLengthBytes

// A blocked trust resolver cannot hold the attempt past its deadline.
func TestConsolidate_BlockedResolverIsBoundedByDeadline(t *testing.T) {
	sim, items := scenario(t, 2, 1)
	release := make(chan struct{})
	var finished atomic.Int32
	c := newClient(t, sim, func(cfg *client.Config) {
		cfg.Timeout = 20 * time.Millisecond
		inner := cfg.Resolver
		cfg.Resolver = func(s *types.UnicitySeal) (types.RootTrustBase, error) {
			<-release
			defer finished.Add(1)
			return inner(s)
		}
	})
	start := time.Now()
	res, err := c.Consolidate(context.Background(), items)
	elapsed := time.Since(start)
	require.Nil(t, res)
	requireClientErr(t, err, client.KindTimeout, "deadline")
	require.Less(t, elapsed, 150*time.Millisecond, "the attempt must not wait for the blocked resolver")

	// outstanding work completes once released and nothing is left blocked
	close(release)
	require.Eventually(t, func() bool { return finished.Load() >= 1 }, 2*time.Second, 5*time.Millisecond)
}

func TestConsolidate_BlockedResolverCancellation(t *testing.T) {
	sim, items := scenario(t, 2, 1)
	release := make(chan struct{})
	defer close(release)
	c := newClient(t, sim, func(cfg *client.Config) {
		cfg.Timeout = 10 * time.Second
		cfg.Resolver = func(*types.UnicitySeal) (types.RootTrustBase, error) { <-release; return nil, nil }
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := c.Consolidate(ctx, items)
	requireClientErr(t, err, client.KindCancelled, "cancelled")
	require.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestConsolidate_ContextResolverReceivesAttemptContext(t *testing.T) {
	sim, items := scenario(t, 2, 1)
	var sawDeadline atomic.Bool
	c := newClient(t, sim, func(cfg *client.Config) {
		cfg.Timeout = 50 * time.Millisecond
		inner := cfg.Resolver
		cfg.Resolver = nil
		cfg.ResolverContext = func(ctx context.Context, s *types.UnicitySeal) (types.RootTrustBase, error) {
			if _, ok := ctx.Deadline(); ok {
				sawDeadline.Store(true)
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Second):
				return inner(s)
			}
		}
	})
	start := time.Now()
	_, err := c.Consolidate(context.Background(), items)
	requireClientErr(t, err, client.KindTimeout, "deadline")
	require.True(t, sawDeadline.Load())
	require.Less(t, time.Since(start), time.Second)

	// and a working context resolver succeeds
	good := newClient(t, sim, func(cfg *client.Config) {
		inner := cfg.Resolver
		cfg.Resolver = nil
		cfg.ResolverContext = func(_ context.Context, s *types.UnicitySeal) (types.RootTrustBase, error) { return inner(s) }
	})
	_, err = good.Consolidate(context.Background(), items)
	require.NoError(t, err)
}

// Operational errors keep their own typed kinds; unavailable is reserved for the
// specified material/pressure outcomes.
func TestConsolidate_ErrorTyping(t *testing.T) {
	rpcErrHook := func(code int, data map[string]any) func(http.ResponseWriter, []byte) bool {
		return func(w http.ResponseWriter, _ []byte) bool {
			e := map[string]any{"code": code, "message": "x"}
			if data != nil {
				e["data"] = data
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "error": e})
			return true
		}
	}
	for name, tc := range map[string]struct {
		hook      func(http.ResponseWriter, []byte) bool
		kind      client.Kind
		detail    string
		retryable bool
	}{
		"method not found":           {rpcErrHook(-32601, nil), client.KindUnsupported, "method_not_found", false},
		"unsupported configuration":  {rpcErrHook(-32008, map[string]any{"reason": "proof_unavailable", "detail": "unsupported_configuration", "retryable": false}), client.KindUnsupported, "unsupported_configuration", false},
		"state not included":         {rpcErrHook(-32008, map[string]any{"reason": "proof_unavailable", "detail": "state_not_included", "retryable": true}), client.KindUnavailable, "state_not_included", true},
		"partition view unavailable": {rpcErrHook(-32008, map[string]any{"reason": "proof_unavailable", "detail": "partition_view_unavailable", "retryable": true}), client.KindUnavailable, "partition_view_unavailable", true},
		"busy":                       {rpcErrHook(-32006, map[string]any{"reason": "busy", "detail": "pinned_views", "retryable": true}), client.KindUnavailable, "pinned_views", true},
		"database error":             {rpcErrHook(-32004, nil), client.KindServer, "", false},
		"internal error":             {rpcErrHook(-32603, nil), client.KindServer, "", false},
		"unknown code":               {rpcErrHook(-32099, nil), client.KindServer, "", false},
		"invalid params":             {rpcErrHook(-32602, map[string]any{"reason": "invalid_request", "detail": "wrong_shard"}), client.KindRejected, "wrong_shard", false},
		"limit exceeded":             {rpcErrHook(-32007, map[string]any{"reason": "limit_exceeded", "detail": "state_count", "retryable": true}), client.KindRejected, "state_count", false},
		"http 500 without body":      {func(w http.ResponseWriter, _ []byte) bool { http.Error(w, "x", 500); return true }, client.KindTransport, "http_500", true},
	} {
		t.Run(name, func(t *testing.T) {
			sim, items := scenario(t, 2, 1)
			sim.Shards[len(sim.Shards)-1].Hook = tc.hook // the master
			_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
			ce := requireClientErr(t, err, tc.kind, tc.detail)
			require.Equal(t, tc.retryable, ce.Retryable)
			require.Equal(t, tc.kind == client.KindUnavailable, client.IsUnavailable(err))
		})
	}

	t.Run("connection failure is transport", func(t *testing.T) {
		sim, items := scenario(t, 2, 1)
		sim.Shards[len(sim.Shards)-1].Server.Close()
		_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
		ce := requireClientErr(t, err, client.KindTransport, "transport")
		require.True(t, ce.Retryable)
		require.False(t, client.IsUnavailable(err))
	})
}

// A signature failure is never ordinary unavailability, also when the context ended meanwhile
// or a peer is down.
func TestConsolidate_SignatureFailureIsNeverUnavailable(t *testing.T) {
	sim, items := scenario(t, 4, 1)
	for _, sh := range sim.Shards[:3] {
		sh.Hook = func(w http.ResponseWriter, _ []byte) bool {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return true
		}
	}
	tamperMaster(sim, func(res map[string]any) { corruptSeal(t, res) })
	_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
	ce := requireClientErr(t, err, client.KindVerification, "")
	require.Equal(t, consolidation.OutcomeUnauthenticated, ce.Outcome)
	require.False(t, client.IsUnavailable(err))

	// an authority that rejects the seal (not a context error) stays verification even under a done context
	sim2, items2 := scenario(t, 2, 1)
	ctx, cancel := context.WithCancel(context.Background())
	_, err = newClient(t, sim2, func(c *client.Config) {
		other, _ := consolidationsim.NewWithSeed(2, "bad authority")
		inner := other.Resolver()
		c.Resolver = func(s *types.UnicitySeal) (types.RootTrustBase, error) { cancel(); return inner(s) }
	}).Consolidate(ctx, items2)
	require.Error(t, err)
	require.False(t, client.IsUnavailable(err))
}

// A resolver's own timeout (attempt context still live) is not caller cancellation.
func TestConsolidate_ResolverOwnTimeoutIsPreserved(t *testing.T) {
	sim, items := scenario(t, 2, 1)
	c := newClient(t, sim, func(cfg *client.Config) {
		cfg.Timeout = 30 * time.Second
		cfg.Resolver = nil
		cfg.ResolverContext = func(ctx context.Context, _ *types.UnicitySeal) (types.RootTrustBase, error) {
			return nil, context.DeadlineExceeded // the resolver's own shorter timeout
		}
	})
	_, err := c.Consolidate(context.Background(), items)
	ce := requireClientErr(t, err, client.KindTimeout, "resolver_timeout")
	require.True(t, ce.Retryable)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the cause is preserved")
	require.False(t, client.IsCancelled(err))

	c = newClient(t, sim, func(cfg *client.Config) {
		cfg.Resolver = nil
		cfg.ResolverContext = func(context.Context, *types.UnicitySeal) (types.RootTrustBase, error) {
			return nil, context.Canceled // cancelled inside the resolver, not by the caller
		}
	})
	_, err = c.Consolidate(context.Background(), items)
	ce = requireClientErr(t, err, client.KindServer, "resolver_cancelled")
	require.False(t, ce.Retryable)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, client.IsCancelled(err), "the caller never cancelled")
}

// HTTP failures are operational whatever their body looks like.
func TestConsolidate_HTTPStatusIndependentOfBody(t *testing.T) {
	for name, tc := range map[string]struct {
		status    int
		body      string
		kind      client.Kind
		detail    string
		retryable bool
		rpcCode   int
	}{
		"503 plain text":              {503, "Service Unavailable", client.KindTransport, "http_503", true, 0},
		"503 json without envelope":   {503, `{"message":"Service Unavailable"}`, client.KindTransport, "http_503", true, 0},
		"503 empty":                   {503, ``, client.KindTransport, "http_503", true, 0},
		"502 html":                    {502, `<html>Bad gateway</html>`, client.KindTransport, "http_502", true, 0},
		"429 json":                    {429, `{"error":"slow down"}`, client.KindTransport, "http_429", true, 0},
		"404 json":                    {404, `{"error":"no such path"}`, client.KindTransport, "http_404", false, 0},
		"503 with rpc error keeps it": {503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32006,"message":"busy","data":{"reason":"busy","detail":"pinned_views","retryable":true}}}`, client.KindUnavailable, "pinned_views", true, -32006},
		"200 malformed body":          {200, `{"message":"hello"}`, client.KindInvalidResponse, "", false, 0},
	} {
		t.Run(name, func(t *testing.T) {
			sim, items := scenario(t, 2, 1)
			sim.Shards[len(sim.Shards)-1].Hook = func(w http.ResponseWriter, _ []byte) bool {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
				return true
			}
			_, err := newClient(t, sim, nil).Consolidate(context.Background(), items)
			ce := requireClientErr(t, err, tc.kind, tc.detail)
			require.Equal(t, tc.retryable, ce.Retryable)
			require.Equal(t, tc.rpcCode, ce.Code)
		})
	}
}
