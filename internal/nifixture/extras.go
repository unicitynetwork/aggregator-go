package nifixture

import (
	"crypto"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"

	"github.com/unicitynetwork/bft-core/rootchain/testutils/ucfixture"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"
)

// Large uint64 values of the large-uint64-fields vector. bigShardEpoch is 2^53+1, the first integer a float64
// cannot represent.
const (
	bigRootRound  uint64 = 18446744073709551613
	bigTimestamp  uint64 = 18446744073709551612
	bigRootEpoch  uint64 = 18446744073709551611
	bigShardRound uint64 = 18446744073709551610
	bigShardEpoch uint64 = 9007199254740993
)

// resignWith applies the mutation to a copy of the UC and then re-derives what depends on it: the shard tree
// and unicity tree paths up to the seal hash, and the signatures of the fixture quorum. Nothing is patched.
func resignWith(uc *types.UnicityCertificate, mutate func(*types.UnicityCertificate), key func(int) abcrypto.Signer) (*types.UnicityCertificate, []byte, error) {
	raw, err := uc.MarshalCBOR()
	if err != nil {
		return nil, nil, err
	}
	var c types.UnicityCertificate
	if err := types.Cbor.Unmarshal(raw, &c); err != nil {
		return nil, nil, err
	}
	mutate(&c)
	shardRoot, err := c.ShardTreeCertificate.ComputeCertificateHash(c.InputRecord, c.TRHash, c.ShardConfHash, crypto.SHA256)
	if err != nil {
		return nil, nil, err
	}
	root, err := c.UnicityTreeCertificate.EvalAuthPath(shardRoot, crypto.SHA256)
	if err != nil {
		return nil, nil, err
	}
	c.UnicitySeal.Hash = root
	c.UnicitySeal.Signatures = nil
	for i := 0; i < 3; i++ {
		if err := c.UnicitySeal.Sign(ucfixture.RootNodeID(i), key(i)); err != nil {
			return nil, nil, err
		}
	}
	out, err := c.MarshalCBOR()
	return &c, out, err
}

// shardNegatives are the shard and partition-structure rejections, derived from the sharded absence vector sa
// (shard 00) and the two same-seal vectors.
func shardNegatives(sa, sameA, sameB Vector) ([]Vector, error) {
	var out []Vector
	withUC := func(name, desc, verdict, ctx string, mutate func(*types.UnicityCertificate), acceptable ...string) {
		var uc types.UnicityCertificate
		if err := types.Cbor.Unmarshal(unhex(sa.UC), &uc); err != nil {
			panic(err)
		}
		mutate(&uc)
		ucb, err := uc.MarshalCBOR()
		if err != nil {
			panic(err)
		}
		out = append(out, derive(sa, name, desc, verdict, func(v *Vector) {
			v.UC = hex.EncodeToString(ucb)
			v.Wrapper = hex.EncodeToString(EncodeWrapper(100, unhex(sa.Certificate), ucb))
			v.Context = ctx
			v.Expected.Acceptable = acceptable
		}))
	}
	withCtx := func(base Vector, name, desc, verdict, ctx string) {
		out = append(out, derive(base, name, desc, verdict, func(v *Vector) { v.Context = ctx }))
	}
	withCtx(sa, "shard-context-of-another-shard", "the shard 00 UC presented to a verifier of shard 01", VerdictUCInvalid, "shard-01")
	withCtx(sa, "shard-unsharded-context", "a shard UC never satisfies an unsharded verifier", VerdictUCInvalid, "default")
	withCtx(sameB, "shard-same-seal-other-shard", "the shard 01 UC (same seal as shard 00) presented to a verifier of shard 00", VerdictUCInvalid, "shard-00")
	withCtx(sameA, "shard-same-seal-other-shard-reversed", "the shard 00 UC (same seal as shard 01) presented to a verifier of shard 01", VerdictUCInvalid, "shard-01")
	withCtx(sa, "shard-config-pin-ok", "shard 00 pinning its own configuration hash", VerdictAbsent, "shard-00-config-pin-ok")
	withCtx(sa, "shard-config-pin-other", "shard 00 pinning another configuration hash", VerdictUCInvalid, "shard-00-config-pin-other")
	withUC("shard-path-short", "the shard path lacks its last sibling", VerdictUCInvalid, "shard-00", func(uc *types.UnicityCertificate) {
		s := uc.ShardTreeCertificate.SiblingHashes
		uc.ShardTreeCertificate.SiblingHashes = s[:len(s)-1]
	})
	withUC("shard-path-missing", "no shard path at all for a shard of two bits", VerdictUCInvalid, "shard-00", func(uc *types.UnicityCertificate) {
		uc.ShardTreeCertificate.SiblingHashes = nil
	}, VerdictMalformed) // a decoder which checks the path length while decoding stops earlier (JS SDK)
	withUC("shard-path-sibling-altered", "a shard path sibling altered: the path no longer reaches the seal hash", VerdictUCInvalid, "shard-00", func(uc *types.UnicityCertificate) {
		s := append([]byte(nil), uc.ShardTreeCertificate.SiblingHashes[0]...)
		s[0] ^= 1
		uc.ShardTreeCertificate.SiblingHashes[0] = s
	})
	withUC("shard-id-claims-another-shard", "the UC claims shard 01 with the shard 00 path", VerdictUCInvalid, "shard-00", func(uc *types.UnicityCertificate) {
		_, b := types.ShardID{}.Split()
		uc.ShardTreeCertificate.Shard = b
	}, VerdictMalformed)
	return out, nil
}

// Checkpoint is the caller's independently trusted requirement of the policy examples.
type Checkpoint struct {
	NetworkID uint32 `json:"networkId"`
	// RequiredRootRound is the smallest acceptable root round (decimal).
	RequiredRootRound string `json:"requiredRootRound"`
	// MinSealTimestampSeconds, if not empty, is the smallest acceptable certified seal timestamp (decimal).
	MinSealTimestampSeconds string `json:"minSealTimestampSeconds,omitempty"`
}

// PolicyDecision is the reference caller policy of the examples: the snapshot must be of the checkpoint's network,
// not older than its root round and (if given) its seal timestamp. It is the caller's decision after verification;
// the verifier never makes it.
func PolicyDecision(s Snapshot, cp Checkpoint) string {
	n := func(v string) *big.Int {
		x, ok := new(big.Int).SetString(v, 10)
		if !ok {
			panic("not a decimal: " + v)
		}
		return x
	}
	if s.NetworkID != cp.NetworkID || n(s.RootRound).Cmp(n(cp.RequiredRootRound)) < 0 {
		return "reject"
	}
	if cp.MinSealTimestampSeconds != "" && n(s.SealTimestampSeconds).Cmp(n(cp.MinSealTimestampSeconds)) < 0 {
		return "reject"
	}
	return "accept"
}

func plus(v string, d int64) string {
	x, _ := new(big.Int).SetString(v, 10)
	return x.Add(x, big.NewInt(d)).String()
}

type policyExample struct {
	Name       string     `json:"name"`
	Vector     string     `json:"vector"`
	Snapshot   Snapshot   `json:"snapshot"`
	Checkpoint Checkpoint `json:"checkpoint"`
	Decision   string     `json:"decision"`
	Note       string     `json:"note"`
}

type seqStep struct {
	Vector   string   `json:"vector"`
	Expected Expected `json:"expected"`
}

// policyAndSequences writes policy.json (caller policy examples over verified snapshots) and sequences.json
// (ordered verifications on ONE verifier, which memoizes authenticated seals).
func policyAndSequences(files Files, byPath map[string]Vector, evaluate func(Vector, string) (string, *Snapshot, error)) error {
	snapshot := func(path string) (Snapshot, error) {
		v := byPath[path]
		verdict, snap, err := evaluate(v, v.Context)
		if err != nil || snap == nil {
			return Snapshot{}, fmt.Errorf("%s: verdict %s has no snapshot (%v)", path, verdict, err)
		}
		return *snap, nil
	}
	var ex []policyExample
	add := func(name, path string, cp func(Snapshot) Checkpoint, note string) error {
		s, err := snapshot(path)
		if err != nil {
			return err
		}
		c := cp(s)
		ex = append(ex, policyExample{Name: name, Vector: path, Snapshot: s, Checkpoint: c, Decision: PolicyDecision(s, c), Note: note})
		return nil
	}
	hist, err := snapshot("cases/historical-snapshot-absent")
	if err != nil {
		return err
	}
	later, err := snapshot("cases/later-snapshot-included")
	if err != nil {
		return err
	}
	net := func(s Snapshot) uint32 { return s.NetworkID }
	for _, e := range []struct {
		name, path string
		cp         func(Snapshot) Checkpoint
		note       string
	}{
		{"historical-absence-meets-its-own-round", "cases/historical-snapshot-absent", func(s Snapshot) Checkpoint { return Checkpoint{net(s), s.RootRound, ""} },
			"the requirement equals the snapshot round: accepted (the comparison is not strict)"},
		{"historical-absence-is-older-than-required", "cases/historical-snapshot-absent", func(s Snapshot) Checkpoint { return Checkpoint{net(s), later.RootRound, ""} },
			"valid absence at an older snapshot; the application requires a root round of the later snapshot, in which the state is included"},
		{"historical-absence-wrong-network", "cases/historical-snapshot-absent", func(s Snapshot) Checkpoint { return Checkpoint{1, "1", ""} },
			"the snapshot is of another network than the checkpoint (network 1): rejected whatever the round"},
		{"later-snapshot-included-is-not-absence", "cases/later-snapshot-included", func(s Snapshot) Checkpoint { return Checkpoint{net(s), hist.RootRound, ""} },
			"policy accepts the snapshot, but its outcome is included_at_snapshot: the caller must check the outcome before the policy"},
		{"timestamp-policy-accepts-equal", "cases/historical-snapshot-absent", func(s Snapshot) Checkpoint { return Checkpoint{net(s), "1", s.SealTimestampSeconds} },
			"certified seal timestamp policy: equal accepted"},
		{"timestamp-policy-rejects-older", "cases/historical-snapshot-absent", func(s Snapshot) Checkpoint { return Checkpoint{net(s), "1", plus(s.SealTimestampSeconds, 1)} },
			"certified seal timestamp one second older than required: rejected"},
		{"large-round-meets-requirement", "cases/large-uint64-fields", func(s Snapshot) Checkpoint { return Checkpoint{net(s), s.RootRound, ""} },
			"root round near 2^64: exact uint64 comparison"},
		{"large-round-one-below-requirement", "cases/large-uint64-fields", func(s Snapshot) Checkpoint { return Checkpoint{net(s), plus(s.RootRound, 1), ""} },
			"requirement one above a root round near 2^64: rejected (a float or int64 comparison gets this wrong)"},
		{"large-round-vs-2-53", "cases/large-uint64-fields", func(s Snapshot) Checkpoint { return Checkpoint{net(s), "9007199254740993", ""} },
			"requirement of 2^53+1: accepted"},
		{"sharded-absence-meets-requirement", "cases/sharded-absent", func(s Snapshot) Checkpoint { return Checkpoint{net(s), s.RootRound, ""} },
			"a sharded snapshot is judged by the same network and root round rule"},
	} {
		if err := add(e.name, e.path, e.cp, e.note); err != nil {
			return err
		}
	}
	files["policy.json"] = jsonOut(map[string]any{
		"description": "Caller policy examples, applied AFTER verification to the authenticated snapshot with a checkpoint obtained independently of the proof and the server response. The verifier has no freshness setting and no stale verdict. decision is accept when the snapshot's networkId equals the checkpoint's, its rootRound >= requiredRootRound and, if given, its sealTimestampSeconds >= minSealTimestampSeconds (all uint64 comparisons exact). The seal timestamp is a consensus-certified time, not proof of real-time freshness.",
		"examples":    ex,
	})

	type sequence struct {
		Name        string    `json:"name"`
		Description string    `json:"description"`
		Context     string    `json:"context"`
		Steps       []seqStep `json:"steps"`
	}
	mkSeq := func(name, desc, ctx string, paths ...string) (sequence, error) {
		sq := sequence{Name: name, Description: desc, Context: ctx}
		for _, p := range paths {
			v := byPath[p]
			verdict, snap, err := evaluate(v, ctx)
			if err != nil {
				return sq, err
			}
			sq.Steps = append(sq.Steps, seqStep{Vector: p, Expected: expectedOf(verdict, snap, v.Query, v.Expected.Acceptable)})
		}
		return sq, nil
	}
	var seqs []sequence
	for _, c := range []struct {
		name, desc, ctx string
		paths           []string
	}{
		{"same-seal-other-shard", "ONE verifier of shard 00: the seal of block B is authenticated by the first step; the UC of shard 01 of the same block must still be rejected by the shard rule, and the first proof still verifies afterwards (no state leaks between steps)", "shard-00",
			[]string{"cases/sharded-same-seal-00", "negative/shard-same-seal-other-shard", "cases/sharded-same-seal-00"}},
		{"rejection-between-successes", "ONE verifier: a rejected proof (altered certificate, wrong partition UC, wrong query) between two successes changes nothing", "default",
			[]string{"cases/shared-prefix", "negative/cert-sibling-flipped", "negative/uc-wrong-partition-validly-signed", "negative/query-other-branch", "cases/shared-prefix"}},
		{"cache-hit-keeps-the-snapshot", "ONE verifier: the second verification of the same proof is a memo hit and returns the same snapshot; the included outcome of another query on the same seal is not influenced", "default",
			[]string{"cases/shared-prefix", "cases/shared-prefix", "negative/query-is-terminal"}},
	} {
		sq, err := mkSeq(c.name, c.desc, c.ctx, c.paths...)
		if err != nil {
			return err
		}
		seqs = append(seqs, sq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i].Name < seqs[j].Name })
	files["sequences.json"] = jsonOut(map[string]any{
		"description": "Ordered verifications on ONE verifier instance (which memoizes authenticated seals). Each step's expected result is the result for that vector under the sequence context.",
		"sequences":   seqs,
	})
	return nil
}
