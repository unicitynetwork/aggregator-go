package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/consolidationsim"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation/client"
)

type env struct {
	dir, cfg, in, out string
	sim               *consolidationsim.Sim
	items             []client.Item
}

func setup(t *testing.T, n int) *env {
	sim, err := consolidationsim.New(n)
	require.NoError(t, err)
	t.Cleanup(sim.Close)
	e := &env{dir: t.TempDir(), sim: sim}

	var reqs [][]*api.CertificationRequest
	for i := range sim.Shards {
		reqs = append(reqs, sim.Add(i, 2))
	}
	f1 := sim.Finalize()
	for i := range sim.Shards {
		for _, r := range reqs[i] {
			e.items = append(e.items, sim.Item(i, r, f1))
		}
	}
	sim.Finalize()

	tbJSON, err := json.Marshal(sim.TB)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(e.dir, "tb.json"), tbJSON, 0o600))
	cfg := Config{PartitionID: uint32(consolidationsim.PartitionID), TrustBases: []string{"tb.json"}, TimeoutSeconds: 5}
	for _, sh := range sim.Shards {
		cfg.Shards = append(cfg.Shards, ShardConfig{ShardBits: sh.ID.String(), URL: sh.URL, ConfHash: hex.EncodeToString(sim.Policy.Topology.ShardConfHashes[sh.ID.Key()])})
	}
	e.cfg = writeJSON(t, e.dir, "config.json", cfg)

	var in Items
	for _, it := range e.items {
		rb, err := types.Cbor.Marshal(it.Request)
		require.NoError(t, err)
		pb, err := types.Cbor.Marshal(&api.GetInclusionProofResponseV2{BlockNumber: 1, InclusionProof: it.Proof})
		require.NoError(t, err)
		in.Items = append(in.Items, ItemJSON{Request: hex.EncodeToString(rb), Proof: hex.EncodeToString(pb)})
	}
	e.in = writeJSON(t, e.dir, "items.json", in)
	e.out = filepath.Join(e.dir, "bundle.json")
	return e
}

func writeJSON(t *testing.T, dir, name string, v any) string {
	b, err := json.Marshal(v)
	require.NoError(t, err)
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, b, 0o600))
	return p
}

func TestCLI_ConsolidatesFourShards(t *testing.T) {
	e := setup(t, 4)
	var stdout, stderr bytes.Buffer
	code := run([]string{"-config", e.cfg, "-in", e.in, "-out", e.out}, &stdout, &stderr)
	require.Equal(t, exitOK, code, stderr.String())

	raw, err := os.ReadFile(e.out)
	require.NoError(t, err)
	var b Bundle
	require.NoError(t, json.Unmarshal(raw, &b))
	require.Equal(t, 1, b.Version)
	require.Len(t, b.Proofs, len(e.items))
	require.Len(t, b.UnicityCertificates, 4)
	require.Equal(t, 1, b.Counters.QuorumChecks)
	require.Equal(t, 4, b.Counters.BodyChecks)
	require.Equal(t, len(e.items), b.Counters.LeafChecks)
	require.Equal(t, 4, b.Stats.Requests)
	require.Equal(t, 2, b.Stats.Waves)

	// every output proof decodes as an ordinary v2 result and passes the full legacy verifier
	for i, bp := range b.Proofs {
		require.Equal(t, i, bp.Input)
		pb, err := hex.DecodeString(bp.Proof)
		require.NoError(t, err)
		var pr api.GetInclusionProofResponseV2
		require.NoError(t, types.Cbor.Unmarshal(pb, &pr))
		var shard types.ShardID
		for _, sh := range e.sim.Shards {
			if sh.ID.String() == bp.Shard {
				shard = sh.ID
			}
		}
		require.NoError(t, pr.InclusionProof.Verify(e.items[i].Request, &api.VerifierContext{TrustBase: e.sim.TB,
			PartitionID: consolidationsim.PartitionID, ExpectedShardID: shard, ShardConfHash: e.sim.Policy.Topology.ShardConfHashes[shard.Key()]}))
	}
	// the output directory holds no leftovers
	ents, _ := os.ReadDir(e.dir)
	for _, en := range ents {
		require.NotContains(t, en.Name(), ".tmp")
	}
}

func TestCLI_StdoutAndSingleShard(t *testing.T) {
	e := setup(t, 1)
	var stdout, stderr bytes.Buffer
	require.Equal(t, exitOK, run([]string{"-config", e.cfg, "-in", e.in}, &stdout, &stderr), stderr.String())
	var b Bundle
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &b))
	require.Equal(t, 1, b.Stats.Requests)
	require.Equal(t, 1, b.Stats.Waves)
}

func TestCLI_FailureLeavesOutputAndInputsUntouched(t *testing.T) {
	e := setup(t, 4)
	require.NoError(t, os.WriteFile(e.out, []byte("previous"), 0o600))
	inBefore, _ := os.ReadFile(e.in)
	e.sim.Shards[3].Hook = func(w http.ResponseWriter, _ []byte) bool {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32008,"message":"Proof unavailable","data":{"reason":"proof_unavailable","detail":"view_not_retained","retryable":true}}}`))
		return true
	}
	e.sim.Shards[0].Hook = e.sim.Shards[3].Hook
	e.sim.Shards[1].Hook = e.sim.Shards[3].Hook
	e.sim.Shards[2].Hook = e.sim.Shards[3].Hook

	var stdout, stderr bytes.Buffer
	code := run([]string{"-config", e.cfg, "-in", e.in, "-out", e.out}, &stdout, &stderr)
	require.Equal(t, exitUnavailable, code, stderr.String())
	var msg struct {
		Error struct {
			Kind, Detail string
			Retryable    bool
		}
	}
	require.NoError(t, json.Unmarshal(stderr.Bytes(), &msg))
	require.Equal(t, "unavailable", msg.Error.Kind)
	got, _ := os.ReadFile(e.out)
	require.Equal(t, "previous", string(got), "no partial output")
	inAfter, _ := os.ReadFile(e.in)
	require.Equal(t, inBefore, inAfter)
	require.Empty(t, stdout.String())
}

func TestCLI_ExitCodes(t *testing.T) {
	e := setup(t, 2)
	var out, errb bytes.Buffer
	require.Equal(t, exitInput, run(nil, &out, &errb))
	require.Equal(t, exitInput, run([]string{"-config", filepath.Join(e.dir, "missing.json"), "-in", e.in}, &out, &errb))
	require.Equal(t, exitInput, run([]string{"-config", e.cfg, "-in", e.in, "-master", "x"}, &out, &errb))

	// a configured authority that is not the deployment's: verification refusal
	other, err := consolidationsim.NewWithSeed(2, "other")
	require.NoError(t, err)
	defer other.Close()
	tbJSON, _ := json.Marshal(other.TB)
	require.NoError(t, os.WriteFile(filepath.Join(e.dir, "tb.json"), tbJSON, 0o600))
	errb.Reset()
	require.Equal(t, exitRejected, run([]string{"-config", e.cfg, "-in", e.in}, &out, &errb), errb.String())
	require.Contains(t, errb.String(), `"outcome":"unauthenticated"`)
}

type failingWriter struct{ n int }

func (w failingWriter) Write(p []byte) (int, error) {
	if w.n < 0 {
		return 0, io.ErrClosedPipe
	}
	return w.n, nil // short write
}

func TestCLI_StdoutWriteFailureIsNotSuccess(t *testing.T) {
	e := setup(t, 2)
	for name, w := range map[string]failingWriter{"closed pipe": {-1}, "short write": {3}} {
		var errb bytes.Buffer
		code := run([]string{"-config", e.cfg, "-in", e.in}, w, &errb)
		require.Equal(t, exitOutput, code, name)
		require.Contains(t, errb.String(), `"kind":"output"`)
	}
}

func TestCLI_UnwritableOutputFile(t *testing.T) {
	e := setup(t, 2)
	var out, errb bytes.Buffer
	code := run([]string{"-config", e.cfg, "-in", e.in, "-out", filepath.Join(e.dir, "no-such-dir", "b.json")}, &out, &errb)
	require.Equal(t, exitOutput, code)
}

func TestCLI_OperationalExitCodes(t *testing.T) {
	e := setup(t, 2)
	// connection failure: operational, not unavailable
	for _, sh := range e.sim.Shards {
		sh.Server.Close()
	}
	var out, errb bytes.Buffer
	require.Equal(t, exitOperational, run([]string{"-config", e.cfg, "-in", e.in}, &out, &errb), errb.String())
	require.Contains(t, errb.String(), `"kind":"transport"`)

	// unsupported endpoint is classified explicitly (refusal exit, kind unsupported)
	e = setup(t, 2)
	for _, sh := range e.sim.Shards {
		sh.Hook = func(w http.ResponseWriter, _ []byte) bool {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}`))
			return true
		}
	}
	errb.Reset()
	require.Equal(t, exitRejected, run([]string{"-config", e.cfg, "-in", e.in}, &out, &errb), errb.String())
	require.Contains(t, errb.String(), `"kind":"unsupported"`)

	// deadline: timeout is operational
	e = setup(t, 2)
	for _, sh := range e.sim.Shards {
		sh.Delay = 3 * time.Second
	}
	errb.Reset()
	require.Equal(t, exitOperational, run([]string{"-config", e.cfg, "-in", e.in, "-timeout", "200ms"}, &out, &errb), errb.String())
	require.Contains(t, errb.String(), `"kind":"timeout"`)
}

func TestCLI_HTTPFailureBodiesAreOperational(t *testing.T) {
	for name, body := range map[string]string{"plain": "Service Unavailable", "json": `{"message":"Service Unavailable"}`} {
		e := setup(t, 2)
		for _, sh := range e.sim.Shards {
			sh.Hook = func(w http.ResponseWriter, _ []byte) bool {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(body))
				return true
			}
		}
		var out, errb bytes.Buffer
		require.Equal(t, exitOperational, run([]string{"-config", e.cfg, "-in", e.in}, &out, &errb), name+": "+errb.String())
		require.Contains(t, errb.String(), `"kind":"transport"`, name)
		require.Contains(t, errb.String(), `"detail":"http_503"`, name)
	}
}

func TestCLI_UnpinnedConfig(t *testing.T) {
	e := setup(t, 4)
	var cfg Config
	raw, _ := os.ReadFile(e.cfg)
	require.NoError(t, json.Unmarshal(raw, &cfg))
	for i := range cfg.Shards {
		cfg.Shards[i].ConfHash = ""
	}
	e.cfg = writeJSON(t, e.dir, "config-unpinned.json", cfg)
	var out, errb bytes.Buffer
	require.Equal(t, exitOK, run([]string{"-config", e.cfg, "-in", e.in}, &out, &errb), errb.String())

	// pinning is all-or-none
	cfg.Shards[0].ConfHash = hex.EncodeToString(e.sim.Policy.Topology.ShardConfHashes[e.sim.Shards[0].ID.Key()])
	e.cfg = writeJSON(t, e.dir, "config-partial.json", cfg)
	errb.Reset()
	require.Equal(t, exitInput, run([]string{"-config", e.cfg, "-in", e.in}, &out, &errb))
}
