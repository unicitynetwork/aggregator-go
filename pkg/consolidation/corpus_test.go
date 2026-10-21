package consolidation_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/consolidationcorpus"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation"
)

// Consumer tests of the portable fixture corpus. They read only the committed
// files (never regenerate): the same bytes the SDK consumers pin.

const corpusDir = "../../testdata/uc-consolidation/v1"

type manifest struct {
	Version   int `json:"version"`
	Generator struct {
		Commits map[string]string `json:"commits"`
		Modules map[string]string `json:"modules"`
	} `json:"generator"`
	Cases []struct {
		Name    string `json:"name"`
		Kind    string `json:"kind"`
		Stage   string `json:"stage"`
		Outcome string `json:"outcome"`
	} `json:"cases"`
	SHA256 map[string]string `json:"sha256"`
}

func loadManifest(t *testing.T) (manifest, consolidationcorpus.Files) {
	all, err := consolidationcorpus.ReadDir(corpusDir)
	require.NoError(t, err)
	var m manifest
	require.NoError(t, json.Unmarshal(all["manifest.json"], &m))
	return m, all
}

func caseFiles(all consolidationcorpus.Files, dir string) consolidationcorpus.Files {
	f := consolidationcorpus.Files{}
	for n, b := range all {
		if rel, ok := strings.CutPrefix(n, dir+"/"); ok {
			f[rel] = b
		}
	}
	return f
}

func TestCorpusManifestPinsEveryFile(t *testing.T) {
	m, all := loadManifest(t)
	require.Equal(t, consolidationcorpus.Version, m.Version)
	for name, want := range m.SHA256 {
		b, ok := all[name]
		require.True(t, ok, "listed file %s is missing", name)
		sum := sha256.Sum256(b)
		require.Equal(t, want, hex.EncodeToString(sum[:]), name)
	}
	for name := range all {
		if name != "manifest.json" {
			require.Contains(t, m.SHA256, name, "file %s is not pinned by the manifest", name)
		}
	}
	// provenance: all three generator code commits are recorded, none is the manifest's own checksum
	for _, k := range []string{"bft-go-base", "bft-core", "aggregator-go"} {
		require.NotEmpty(t, m.Generator.Commits[k], k)
	}
	require.Contains(t, m.Generator.Modules, "github.com/unicitynetwork/bft-core")
	// every case of the index exists
	for _, c := range m.Cases {
		require.NotEmpty(t, caseFiles(all, c.Name)[consolidationcorpus.FileExpected], c.Name)
	}
}

func positives(m manifest) (names []string) {
	for _, c := range m.Cases {
		if c.Kind == "positive" {
			names = append(names, c.Name)
		}
	}
	return names
}

func TestCorpusPositiveCases(t *testing.T) {
	m, all := loadManifest(t)
	require.ElementsMatch(t, []string{"shards-1", "shards-4", "shards-8", "shards-4-timeout-unchanged", "large-values", "witness-alt", "nil-vs-empty"}, positives(m))
	for _, name := range positives(m) {
		t.Run(name, func(t *testing.T) {
			files := caseFiles(all, name)
			var exp struct {
				Stage     string                            `json:"stage"`
				Outcome   string                            `json:"outcome"`
				RMin      string                            `json:"rMin"`
				RootRound string                            `json:"rootRound"`
				Master    string                            `json:"master"`
				Counters  consolidationcorpus.Counters      `json:"counters"`
				Bundle    []consolidationcorpus.ShardBundle `json:"bundle"`
				Shards    []struct {
					Shard        string   `json:"shard"`
					StateRoot    string   `json:"stateRoot"`
					UCSha256     string   `json:"ucSha256"`
					ProofSha256s []string `json:"proofSha256"`
				} `json:"shards"`
			}
			require.NoError(t, files.Decode(consolidationcorpus.FileExpected, &exp))

			res, err := consolidationcorpus.Evaluate(files)
			require.NoError(t, err)
			require.Equal(t, consolidationcorpus.StageOK, res.Stage)
			require.Equal(t, exp.RMin, res.RMin)
			require.Equal(t, exp.RootRound, res.RootRound)
			require.Equal(t, exp.Master, res.Master)
			// one quorum check for the output, m body checks, L leaf checks
			require.Equal(t, exp.Counters, *res.Counters)
			require.Equal(t, 1, res.Counters.QuorumChecks)
			require.Equal(t, len(exp.Shards), res.Counters.BodyChecks)
			require.Equal(t, 2, res.Counters.EvidenceQuorumChecks, "original input authentication is reported separately")
			require.Equal(t, exp.Bundle, res.Bundle)

			l, err := consolidationcorpus.Load(files)
			require.NoError(t, err)
			view, err := types.DecodePartitionViewV1(l.Input.PartitionView)
			require.NoError(t, err)
			require.Len(t, view.Leaves, len(l.Context.Shards))
			require.Equal(t, files[consolidationcorpus.FilePartitionView], []byte(l.Input.PartitionView))

			var masterSeal []byte
			for i, b := range res.Bundle {
				ucb, err := consolidationcorpus.UnHex(b.UC)
				require.NoError(t, err)
				// byte-equivalent to the certificate the real root chain block executor produced
				shardName := b.Shard
				if shardName == "" {
					shardName = "root"
				}
				require.Equal(t, files[consolidationcorpus.DirUCs+shardName+".cbor"], ucb, "shard %q", b.Shard)
				require.Equal(t, exp.Shards[i].UCSha256, consolidationcorpus.Digest(ucb))
				var uc types.UnicityCertificate
				require.NoError(t, types.Cbor.Unmarshal(ucb, &uc))
				require.Equal(t, exp.Shards[i].StateRoot, hex.EncodeToString(uc.InputRecord.Hash))
				seal, err := uc.UnicitySeal.MarshalCBOR()
				require.NoError(t, err)
				if masterSeal == nil {
					masterSeal = seal
				}
				require.Equal(t, masterSeal, seal, "identical complete master seal in every shard UC")

				// native verification of every shard UC and of every proof by the legacy full verifier
				id, err := consolidationcorpus.ParseShard(b.Shard)
				require.NoError(t, err)
				confHash := l.Policy.Topology.ShardConfHashes[id.Key()]
				require.NoError(t, uc.Verify(l.TrustBase, 5, api.AggregationPartitionID, id, confHash))
				vctx := &api.VerifierContext{TrustBase: l.TrustBase, PartitionID: api.AggregationPartitionID, ExpectedShardID: id, ShardConfHash: confHash}
				var reqs []*api.CertificationRequest
				if b.Shard == l.Input.Master.ShardID.String() {
					reqs = l.Input.Master.Requests
				}
				for _, p := range l.Input.Peers {
					if p.ShardID.String() == b.Shard {
						reqs = p.Requests
					}
				}
				require.Len(t, reqs, len(b.Proofs))
				for j, ph := range b.Proofs {
					pb, err := consolidationcorpus.UnHex(ph)
					require.NoError(t, err)
					require.Equal(t, exp.Shards[i].ProofSha256s[j], consolidationcorpus.Digest(pb))
					var p api.GetInclusionProofResponseV2
					require.NoError(t, types.Cbor.Unmarshal(pb, &p))
					require.NoError(t, p.InclusionProof.Verify(reqs[j], vctx))
				}
			}

			// the producer's legacy response is the four element array; v2 wraps the same one
			require.EqualValues(t, 0x84, files[consolidationcorpus.FileResponseV1][0])
			require.EqualValues(t, 0x83, files[consolidationcorpus.FileResponseV2][0])
			require.Equal(t, uint8(2), files[consolidationcorpus.FileResponseV2][1])
			require.True(t, bytes.Contains(files[consolidationcorpus.FileResponseV2], files[consolidationcorpus.FilePartitionView]))
			require.False(t, bytes.Contains(files[consolidationcorpus.FileResponseV1], files[consolidationcorpus.FilePartitionView]))
		})
	}
}

func decodedBundle(t *testing.T, all consolidationcorpus.Files, name string) (*consolidationcorpus.Result, *consolidationcorpus.Loaded) {
	f := caseFiles(all, name)
	res, err := consolidationcorpus.Evaluate(f)
	require.NoError(t, err)
	l, err := consolidationcorpus.Load(f)
	require.NoError(t, err)
	return res, l
}

func TestCorpusTopologyCoverage(t *testing.T) {
	_, all := loadManifest(t)
	for name, shards := range map[string]int{"shards-1": 1, "shards-4": 4, "shards-8": 8} {
		res, l := decodedBundle(t, all, name)
		require.Len(t, l.Context.Shards, shards)
		require.Equal(t, consolidationcorpus.StageOK, res.Stage)
	}
	// fewer participants than shards
	_, l := decodedBundle(t, all, "shards-4")
	require.Len(t, l.Context.Participants, 2)
	require.Len(t, l.Context.Shards, 4)
	// several transactions per participating shard
	require.Greater(t, len(l.Input.Master.Requests), 1)
	// peer local seal is older than the master seal while its state root matches
	view, err := types.DecodePartitionViewV1(l.Input.PartitionView)
	require.NoError(t, err)
	var master types.UnicityCertificate
	require.NoError(t, types.Cbor.Unmarshal(l.Input.Master.Proofs[0].InclusionProof.UnicityCertificate, &master))
	var peer types.UnicityCertificate
	require.NoError(t, types.Cbor.Unmarshal(l.Input.Peers[0].Proofs[0].InclusionProof.UnicityCertificate, &peer))
	require.Less(t, peer.UnicitySeal.RootChainRoundNumber, master.UnicitySeal.RootChainRoundNumber)
	require.NotEqual(t, peer.UnicitySeal.Hash, master.UnicitySeal.Hash)
	idx := -1
	for i, lf := range view.Leaves {
		if lf.ShardID.Equal(peer.ShardTreeCertificate.Shard) {
			idx = i
		}
	}
	require.GreaterOrEqual(t, idx, 0)
	require.Equal(t, []byte(peer.InputRecord.Hash), []byte(view.Leaves[idx].InputRecord.Hash))
	// the singleton shard ID is the native empty bit string
	_, l1 := decodedBundle(t, all, "shards-1")
	v1, err := types.DecodePartitionViewV1(l1.Input.PartitionView)
	require.NoError(t, err)
	enc, err := v1.Leaves[0].ShardID.MarshalCBOR()
	require.NoError(t, err)
	require.Equal(t, []byte{0x41, 0x80}, enc)
}

func TestCorpusChangedAndUnchangedLeaves(t *testing.T) {
	_, all := loadManifest(t)
	_, l := decodedBundle(t, all, "shards-4-timeout-unchanged")
	view, err := types.DecodePartitionViewV1(l.Input.PartitionView)
	require.NoError(t, err)
	// shard 00: timeout (record repeated, never written), shard 01 master: empty block record,
	// shard 10: unchanged since block P, shard 11: a new write
	require.Len(t, view.Leaves, 4)
	master := view.Leaves[1].InputRecord
	require.Equal(t, master.Hash, master.PreviousHash)
	require.Empty(t, master.BlockHash)
	require.Equal(t, uint64(0), view.Leaves[0].InputRecord.RoundNumber, "the timeout repeats the initial record")
	require.NotEmpty(t, view.Leaves[3].InputRecord.BlockHash)
	require.NotEqual(t, view.Leaves[0].TRHash, view.Leaves[2].TRHash)
}

func TestCorpusLargeUint64Values(t *testing.T) {
	_, all := loadManifest(t)
	_, l := decodedBundle(t, all, "large-values")
	view, err := types.DecodePartitionViewV1(l.Input.PartitionView)
	require.NoError(t, err)
	const jsSafe = uint64(1) << 53
	var bigTime, bigFees int
	for _, lf := range view.Leaves {
		if lf.InputRecord.Timestamp > jsSafe {
			bigTime++
		}
		if lf.InputRecord.SumOfEarnedFees > jsSafe {
			bigFees++
		}
	}
	require.Positive(t, bigTime)
	require.Positive(t, bigFees)
	require.Greater(t, *l.Input.Master.Proofs[len(l.Input.Master.Proofs)-1].InclusionProof.ReferenceTime, jsSafe)
}

func TestCorpusNilVersusEmptyIRFields(t *testing.T) {
	_, all := loadManifest(t)
	_, l := decodedBundle(t, all, "nil-vs-empty")
	view, err := types.DecodePartitionViewV1(l.Input.PartitionView)
	require.NoError(t, err)
	require.NotNil(t, view.Leaves[0].InputRecord.ETHash)
	require.Empty(t, view.Leaves[0].InputRecord.ETHash)
	require.Nil(t, view.Leaves[1].InputRecord.ETHash)
}

// Different valid signature witnesses of the same unsigned seal give different
// UC bytes: UC equivalence is by unsigned content, signatures are witnesses.
func TestCorpusAlternateWitness(t *testing.T) {
	_, all := loadManifest(t)
	_, base := decodedBundle(t, all, "shards-4")
	_, alt := decodedBundle(t, all, "witness-alt")
	seal := func(l *consolidationcorpus.Loaded) *types.UnicitySeal {
		var uc types.UnicityCertificate
		require.NoError(t, types.Cbor.Unmarshal(l.Input.Master.Proofs[0].InclusionProof.UnicityCertificate, &uc))
		return uc.UnicitySeal
	}
	a, b := seal(base), seal(alt)
	as, err := a.SigBytes()
	require.NoError(t, err)
	bs, err := b.SigBytes()
	require.NoError(t, err)
	require.Equal(t, as, bs, "same unsigned seal")
	require.NotEqual(t, sortedKeys(a.Signatures), sortedKeys(b.Signatures))
	ab, _ := a.MarshalCBOR()
	bb, _ := b.MarshalCBOR()
	require.NotEqual(t, ab, bb)
}

func sortedKeys(m types.SignatureMap) string {
	var k []string
	for n := range m {
		k = append(k, n)
	}
	sort.Strings(k)
	return strings.Join(k, ",")
}

func TestCorpusNegativeCases(t *testing.T) {
	m, all := loadManifest(t)
	base := caseFiles(all, "shards-4")
	var n int
	for _, c := range m.Cases {
		if c.Kind != "negative" {
			continue
		}
		n++
		t.Run(c.Name, func(t *testing.T) {
			dir := caseFiles(all, c.Name)
			var exp struct {
				Base            string                          `json:"base"`
				Stage           string                          `json:"stage"`
				Outcome         string                          `json:"outcome"`
				PipelineOutcome string                          `json:"pipelineOutcome"`
				RPC             *consolidationcorpus.RPCFailure `json:"rpc"`
				ChangedFiles    []string                        `json:"changedFiles"`
				RemovedFiles    []string                        `json:"removedFiles"`
			}
			require.NoError(t, dir.Decode(consolidationcorpus.FileExpected, &exp))
			require.Equal(t, "shards-4", exp.Base)
			files := base.Clone()
			for _, name := range exp.ChangedFiles {
				b, ok := dir[name]
				require.True(t, ok, "complete mutated file %s is stored", name)
				files[name] = b
			}
			for _, name := range exp.RemovedFiles {
				delete(files, name)
			}
			res, err := consolidationcorpus.Evaluate(files)
			require.NoError(t, err)
			require.Equal(t, exp.Stage, res.Stage)
			require.Equal(t, exp.Outcome, res.Outcome)
			require.Equal(t, exp.PipelineOutcome, res.PipelineOutcome)
			require.Equal(t, exp.RPC, res.RPC)
			require.Equal(t, c.Stage, exp.Stage)
			require.Equal(t, c.Outcome, exp.Outcome)
			require.NotEqual(t, consolidationcorpus.StageOK, res.Stage, "a tampered input is never accepted")
			require.Empty(t, res.Bundle)
		})
	}
	require.GreaterOrEqual(t, n, 80)
}

// Every unauthenticated outcome is exercised, and no tampered case may be
// satisfied by a verification cached from an untampered one.
func TestCorpusTamperedSealsNeverHitTheSessionCache(t *testing.T) {
	_, all := loadManifest(t)
	l, err := consolidationcorpus.Load(caseFiles(all, "shards-4"))
	require.NoError(t, err)
	sess, err := l.NewSession()
	require.NoError(t, err)
	bound, err := sess.AuthenticateEvidence(l.Evidence)
	require.NoError(t, err)
	in := *l.Input
	in.Bound = bound
	_, err = sess.Consolidate(&in)
	require.NoError(t, err)
	require.Equal(t, 1, sess.Counters().QuorumChecks)

	for name, edit := range map[string]func(s *types.UnicitySeal){
		"altered signature": func(s *types.UnicitySeal) {
			for k, v := range s.Signatures {
				s.Signatures[k] = append([]byte{v[0] ^ 1}, v[1:]...)
			}
		},
		"missing signatures": func(s *types.UnicitySeal) { s.Signatures = nil },
		"one signature fewer": func(s *types.UnicitySeal) {
			for k := range s.Signatures {
				delete(s.Signatures, k)
				return
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			tam := in
			tam.Master.Proofs = nil
			for _, p := range in.Master.Proofs {
				var uc types.UnicityCertificate
				require.NoError(t, types.Cbor.Unmarshal(p.InclusionProof.UnicityCertificate, &uc))
				sigs := types.SignatureMap{}
				for k, v := range uc.UnicitySeal.Signatures {
					sigs[k] = bytes.Clone(v)
				}
				uc.UnicitySeal.Signatures = sigs
				edit(uc.UnicitySeal)
				b, err := uc.MarshalCBOR()
				require.NoError(t, err)
				np := *p.InclusionProof
				np.UnicityCertificate = b
				tam.Master.Proofs = append(tam.Master.Proofs, &api.GetInclusionProofResponseV2{BlockNumber: p.BlockNumber, InclusionProof: &np})
			}
			_, err := sess.Consolidate(&tam)
			require.Error(t, err)
			o, _ := consolidation.OutcomeOf(err)
			require.Contains(t, []consolidation.Outcome{consolidation.OutcomeUnauthenticated, consolidation.OutcomeInvalidInput}, o)
		})
	}
	// the untampered input still succeeds on the same session, served by the memo of the genuine seal
	before := sess.Counters().QuorumChecks
	_, err = sess.Consolidate(&in)
	require.NoError(t, err)
	require.Equal(t, before, sess.Counters().QuorumChecks)
}

func TestCorpusIsNotRegeneratedByTests(t *testing.T) {
	// the corpus directory is read only for this package; the generator lives in cmd/
	st, err := os.Stat(filepath.Join(corpusDir, "manifest.json"))
	require.NoError(t, err)
	require.False(t, st.IsDir())
}

// Failed batch calls keep what the wire said: availability, capacity and
// unsupported configuration are distinguishable from integrity failures.
func TestCorpusRPCFailuresAreNotIntegrityFailures(t *testing.T) {
	m, all := loadManifest(t)
	base := caseFiles(all, "shards-4")
	reasons := map[string]bool{}
	classes := map[string]bool{}
	roles := map[string]bool{}
	for _, c := range m.Cases {
		if c.Kind != "negative" || c.Stage != consolidationcorpus.StageRPC {
			continue
		}
		dir := caseFiles(all, c.Name)
		files := base.Clone()
		for n, b := range dir {
			if n != consolidationcorpus.FileExpected {
				files[n] = b
			}
		}
		var resps consolidationcorpus.Responses
		require.NoError(t, files.Decode(consolidationcorpus.FileResponses, &resps))
		res, err := consolidationcorpus.Evaluate(files)
		require.NoError(t, err)
		require.Equal(t, consolidationcorpus.StageRPC, res.Stage, c.Name)
		require.NotNil(t, res.RPC)
		// the outcome is a wire classification, never an integrity outcome of the verifier
		require.NotContains(t, []string{"binding_mismatch", "unauthenticated", "stale", "expired", "reference_time_after_target"}, res.Outcome)
		failed := resps.Master.Error
		if res.RPC.Role == "peer" {
			failed = resps.Peers[0].Response.Error
		}
		require.NotNil(t, failed)
		require.Equal(t, failed.Code, res.RPC.Code)
		if failed.Data != nil {
			require.Equal(t, failed.Data.Retryable, res.RPC.Retryable, "retryability comes from the wire")
			require.Equal(t, failed.Data.Detail, res.RPC.Detail)
			reasons[failed.Data.Reason+"/"+failed.Data.Detail] = true
		}
		classes[res.Outcome] = true
		roles[res.RPC.Role] = true
		// nonretryable classes are never retryable by default
		if res.Outcome == consolidationcorpus.RPCLimitExceeded || res.Outcome == consolidationcorpus.RPCUnsupported || res.Outcome == consolidationcorpus.RPCInvalid {
			require.False(t, res.RPC.Retryable, c.Name)
		}
		if res.Outcome == consolidationcorpus.RPCLimitExceeded {
			require.NotNil(t, res.RPC.Limit, "limit errors carry the limit")
		}
	}
	for _, want := range []string{
		"proof_unavailable/view_not_retained", "proof_unavailable/no_finalized_view", "proof_unavailable/partition_view_unavailable",
		"proof_unavailable/state_pending", "proof_unavailable/state_expired", "proof_unavailable/state_unknown", "proof_unavailable/state_not_in_root",
		"proof_unavailable/unsupported_configuration",
		"busy/active_requests", "busy/pinned_views", "busy/retention_budget", "busy/deadline",
		"limit_exceeded/state_count", "limit_exceeded/request_bytes", "limit_exceeded/response_bytes",
		"invalid_request/duplicate_state_id", "invalid_request/wrong_shard",
	} {
		require.True(t, reasons[want], "no fixture for %s", want)
	}
	for _, class := range []string{"unavailable", "unsupported", "busy", "limit_exceeded", "invalid_request", "unsupported_method", "infrastructure"} {
		require.True(t, classes[class], class)
	}
	require.True(t, roles["master"] && roles["peer"])
}

func TestCorpusContextNetworkIsEnforced(t *testing.T) {
	_, all := loadManifest(t)
	files := caseFiles(all, "shards-4")
	res, err := consolidationcorpus.Evaluate(files)
	require.NoError(t, err)
	require.Equal(t, consolidationcorpus.StageOK, res.Stage)
	var ctx consolidationcorpus.Context
	require.NoError(t, files.Decode(consolidationcorpus.FileContext, &ctx))
	ctx.NetworkID++
	require.NoError(t, files.Encode(consolidationcorpus.FileContext, ctx))
	res, err = consolidationcorpus.Evaluate(files)
	require.NoError(t, err)
	require.Equal(t, consolidationcorpus.StagePolicy, res.Stage)
	require.Equal(t, "incompatible_lineage", res.Outcome)
}

// The independently admitted topology of the context is optional: without it the scheme and
// configuration hashes are those the seal commits to, with identical results.
func TestCorpusPositiveCasesWithoutPinnedTopology(t *testing.T) {
	m, all := loadManifest(t)
	for _, name := range positives(m) {
		files := caseFiles(all, name)
		pinned, err := consolidationcorpus.Evaluate(files)
		require.NoError(t, err)
		unpinned, err := consolidationcorpus.EvaluateWith(files, consolidationcorpus.Options{Unpinned: true})
		require.NoError(t, err)
		require.Equal(t, consolidationcorpus.StageOK, unpinned.Stage, name)
		require.Equal(t, pinned.Bundle, unpinned.Bundle, name)
		require.Equal(t, *pinned.Counters, *unpinned.Counters, name)
	}
}

// Dropping the pins never turns a tampered case into an accepted one.
func TestCorpusNegativeCasesAreRefusedWithoutPinnedTopology(t *testing.T) {
	m, all := loadManifest(t)
	base := caseFiles(all, "shards-4")
	for _, c := range m.Cases {
		if c.Kind != "negative" {
			continue
		}
		dir := caseFiles(all, c.Name)
		var exp struct {
			ChangedFiles []string `json:"changedFiles"`
		}
		require.NoError(t, dir.Decode(consolidationcorpus.FileExpected, &exp))
		files := base.Clone()
		for _, n := range exp.ChangedFiles {
			files[n] = dir[n]
		}
		res, err := consolidationcorpus.EvaluateWith(files, consolidationcorpus.Options{Unpinned: true})
		require.NoError(t, err, c.Name)
		switch c.Name {
		case "negative/context-wrong-config-participant", "negative/context-incomplete-scheme", "negative/context-overlapping-scheme":
			// these tamper with the pins themselves
			continue
		}
		require.NotEqual(t, consolidationcorpus.StageOK, res.Stage, "%s must be refused without pins as well", c.Name)
	}
}
