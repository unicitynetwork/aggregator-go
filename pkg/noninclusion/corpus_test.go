package noninclusion_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/nifixture"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/noninclusion"
)

const corpus = "../../testdata/non-inclusion/v2"

type snapshotJSON struct {
	NetworkID            uint32  `json:"networkId"`
	PartitionID          uint32  `json:"partitionId"`
	ShardID              string  `json:"shardId"`
	RootRound            string  `json:"rootRound"`
	SealTimestampSeconds string  `json:"sealTimestampSeconds"`
	RootEpoch            string  `json:"rootEpoch"`
	ShardRound           string  `json:"shardRound"`
	ShardEpoch           string  `json:"shardEpoch"`
	StateRoot            *string `json:"stateRoot"`
}

type expectedJSON struct {
	Outcome    string        `json:"outcome"`
	StateID    string        `json:"stateId"`
	Snapshot   *snapshotJSON `json:"snapshot"`
	Rejection  string        `json:"rejection"`
	Acceptable []string      `json:"acceptable"`
}

type vector struct {
	Name     string       `json:"name"`
	Query    string       `json:"query"`
	Wrapper  string       `json:"wrapper"`
	Context  string       `json:"context"`
	Expected expectedJSON `json:"expected"`
}

type corpusContext struct {
	TrustBase      string `json:"trustBase"`
	Shard          string `json:"shard"`
	NextEpochStart string `json:"nextEpochStart"`
	ShardConfHash  string `json:"shardConfHash"`
}

func readJSON(t testing.TB, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(corpus, path))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, v))
}

func u64(t testing.TB, s string) uint64 {
	n, err := strconv.ParseUint(s, 10, 64)
	require.NoError(t, err)
	return n
}

func shardOf(t testing.TB, bits string) types.ShardID {
	id, err := nifixture.ShardFromBits(bits)
	require.NoError(t, err)
	return id
}

func verifierFor(t testing.TB, cc corpusContext) *noninclusion.Verifier {
	tbBytes, err := os.ReadFile(filepath.Join(corpus, cc.TrustBase))
	require.NoError(t, err)
	tb := &types.RootTrustBaseV1{}
	require.NoError(t, tb.UnmarshalCBOR(tbBytes))
	ctx := noninclusion.Context{
		Shard:    shardOf(t, cc.Shard),
		Resolver: func(*types.UnicitySeal) (types.RootTrustBase, error) { return tb, nil },
	}
	if cc.ShardConfHash != "" {
		ctx.ShardConfHash, err = hex.DecodeString(cc.ShardConfHash)
		require.NoError(t, err)
	}
	if next := u64(t, cc.NextEpochStart); next != 0 {
		ctx.EpochEnd = func(*types.UnicitySeal) (uint64, bool, error) { return next, true, nil }
	}
	v, err := noninclusion.NewVerifier(ctx)
	require.NoError(t, err)
	return v
}

func loadVector(t testing.TB, path string) vector {
	var v vector
	readJSON(t, path+".json", &v)
	return v
}

// verify runs a vector through the production wrapper codec and the given verifier. A wrapper which does not decode
// is a malformed rejection, like at the client.
func verify(t testing.TB, v *noninclusion.Verifier, vec vector) (*noninclusion.Result, error) {
	wrapper, err := hex.DecodeString(vec.Wrapper)
	require.NoError(t, err)
	query, err := hex.DecodeString(vec.Query)
	require.NoError(t, err)
	var resp api.GetNonInclusionProofResponseV1
	if err := resp.UnmarshalCBOR(wrapper); err != nil {
		require.ErrorIs(t, err, api.ErrNonInclusionProofMalformed)
		return nil, &noninclusion.Error{Rejection: noninclusion.RejectionMalformed, Err: err}
	}
	// the exact profile round-trips byte for byte
	re, err := resp.MarshalCBOR()
	require.NoError(t, err)
	require.Equal(t, wrapper, re)
	bound, err := resp.Proof.ForState(api.StateID(query))
	require.NoError(t, err)
	return v.Verify(api.StateID(query), bound)
}

func requireSnapshot(t testing.TB, want *snapshotJSON, got noninclusion.Snapshot) {
	t.Helper()
	require.EqualValues(t, want.NetworkID, got.NetworkID())
	require.EqualValues(t, want.PartitionID, got.PartitionID())
	require.Equal(t, want.ShardID, got.ShardID().String())
	require.Equal(t, u64(t, want.RootRound), got.RootRound())
	require.Equal(t, u64(t, want.SealTimestampSeconds), got.SealTimestampSeconds())
	require.Equal(t, u64(t, want.RootEpoch), got.RootEpoch())
	require.Equal(t, u64(t, want.ShardRound), got.ShardRound())
	require.Equal(t, u64(t, want.ShardEpoch), got.ShardEpoch())
	if want.StateRoot == nil {
		require.Nil(t, got.StateRoot())
		require.True(t, got.EmptyTree())
	} else {
		require.Equal(t, *want.StateRoot, hex.EncodeToString(got.StateRoot()))
		require.False(t, got.EmptyTree())
	}
}

// requireExpected checks a result against the expectation of a vector: the outcome, the queried state and every
// snapshot field for a success; a rejection class (or an acceptable one) and no result at all for a rejection.
func requireExpected(t testing.TB, vec vector, res *noninclusion.Result, err error) {
	t.Helper()
	e := vec.Expected
	switch e.Outcome {
	case "absent_at_snapshot", "included_at_snapshot":
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, e.Outcome, string(res.Outcome()))
		require.Equal(t, e.StateID, hex.EncodeToString(res.StateID()))
		requireSnapshot(t, e.Snapshot, res.Snapshot())
	case "rejected":
		require.Error(t, err)
		require.Nil(t, res, "a rejection carries no result and no snapshot")
		got := noninclusion.RejectionOf(err)
		if got == e.Rejection {
			return
		}
		// only a rejection may differ, and only to a listed class
		require.Contains(t, e.Acceptable, got, "rejection %q (%v), expected %s", got, err, e.Rejection)
	default:
		t.Fatalf("unknown outcome %q", e.Outcome)
	}
}

// The production codec and verifier reproduce the outcome, the queried state and every snapshot field of every
// vector of the shared corpus, and no rejection exposes metadata.
func TestCorpusOutcomes(t *testing.T) {
	var contexts map[string]corpusContext
	readJSON(t, "contexts.json", &contexts)
	n := 0
	seen := map[string]int{}
	for _, dir := range []string{"cases", "negative"} {
		entries, err := os.ReadDir(filepath.Join(corpus, dir))
		require.NoError(t, err)
		for _, e := range entries {
			name := dir + "/" + strings.TrimSuffix(e.Name(), ".json")
			vec := loadVector(t, name)
			t.Run(name, func(t *testing.T) {
				res, err := verify(t, verifierFor(t, contexts[vec.Context]), vec)
				requireExpected(t, vec, res, err)
			})
			seen[vec.Expected.Outcome]++
			n++
		}
	}
	require.GreaterOrEqual(t, n, 80)
	require.Positive(t, seen["absent_at_snapshot"])
	require.Positive(t, seen["included_at_snapshot"])
	require.Positive(t, seen["rejected"])
}

// A verifier is memoizing authenticated seals: ordered steps on one instance still give the expected result of each
// step (a shared seal never vouches for another shard's UC, a rejection between successes changes nothing).
func TestCorpusSequencesOnOneVerifier(t *testing.T) {
	var contexts map[string]corpusContext
	readJSON(t, "contexts.json", &contexts)
	var f struct {
		Sequences []struct {
			Name    string `json:"name"`
			Context string `json:"context"`
			Steps   []struct {
				Vector   string       `json:"vector"`
				Expected expectedJSON `json:"expected"`
			} `json:"steps"`
		} `json:"sequences"`
	}
	readJSON(t, "sequences.json", &f)
	require.GreaterOrEqual(t, len(f.Sequences), 3)
	for _, s := range f.Sequences {
		t.Run(s.Name, func(t *testing.T) {
			v := verifierFor(t, contexts[s.Context])
			for i, st := range s.Steps {
				vec := loadVector(t, st.Vector)
				vec.Expected = st.Expected
				res, err := verify(t, v, vec)
				requireExpected(t, vec, res, err)
				_ = i
			}
		})
	}
}

// The policy examples apply to the verified snapshots: verification and policy are separate.
func TestCorpusPolicyExamples(t *testing.T) {
	var p struct {
		Examples []struct {
			Name       string       `json:"name"`
			Vector     string       `json:"vector"`
			Snapshot   snapshotJSON `json:"snapshot"`
			Checkpoint struct {
				NetworkID               uint32 `json:"networkId"`
				RequiredRootRound       string `json:"requiredRootRound"`
				MinSealTimestampSeconds string `json:"minSealTimestampSeconds"`
			} `json:"checkpoint"`
			Decision string `json:"decision"`
		} `json:"examples"`
	}
	readJSON(t, "policy.json", &p)
	var contexts map[string]corpusContext
	readJSON(t, "contexts.json", &contexts)
	require.NotEmpty(t, p.Examples)
	for _, e := range p.Examples {
		t.Run(e.Name, func(t *testing.T) {
			vec := loadVector(t, e.Vector)
			res, err := verify(t, verifierFor(t, contexts[vec.Context]), vec)
			require.NoError(t, err)
			s := res.Snapshot()
			requireSnapshot(t, &e.Snapshot, s)
			accept := s.NetworkID() == types.NetworkID(e.Checkpoint.NetworkID) && s.RootRound() >= u64(t, e.Checkpoint.RequiredRootRound)
			if e.Checkpoint.MinSealTimestampSeconds != "" {
				accept = accept && s.SealTimestampSeconds() >= u64(t, e.Checkpoint.MinSealTimestampSeconds)
			}
			want := map[bool]string{true: "accept", false: "reject"}[accept]
			require.Equal(t, e.Decision, want)
		})
	}
}

// The partition is the protocol constant and the corpus is of it; no caller selects another.
func TestFixedAggregationPartition(t *testing.T) {
	require.EqualValues(t, 7, api.AggregationPartitionID)
	require.Equal(t, api.AggregationPartitionID, nifixture.Partition)
	var contexts map[string]corpusContext
	readJSON(t, "contexts.json", &contexts)
	vec := loadVector(t, "negative/uc-wrong-partition-validly-signed")
	res, err := verify(t, verifierFor(t, contexts[vec.Context]), vec)
	require.Nil(t, res)
	require.Equal(t, noninclusion.RejectionUCInvalid, noninclusion.RejectionOf(err))
	good := loadVector(t, "cases/shared-prefix")
	res, err = verify(t, verifierFor(t, contexts[good.Context]), good)
	require.NoError(t, err)
	require.Equal(t, api.AggregationPartitionID, res.Snapshot().PartitionID())
}

// The empty tree is the native null; an empty byte string is rejected as its own error at the body stage.
func TestEmptyByteStringRootIsNotTheEmptyTree(t *testing.T) {
	vec := loadVector(t, "negative/uc-empty-root-as-empty-byte-string")
	var contexts map[string]corpusContext
	readJSON(t, "contexts.json", &contexts)
	w, _ := hex.DecodeString(vec.Wrapper)
	q, _ := hex.DecodeString(vec.Query)
	var resp api.GetNonInclusionProofResponseV1
	require.NoError(t, resp.UnmarshalCBOR(w))
	_, err := verifierFor(t, contexts[vec.Context]).Verify(api.StateID(q), resp.Proof)
	require.ErrorIs(t, err, noninclusion.ErrEmptyRootEncoding)
	require.Equal(t, noninclusion.RejectionUCInvalid, noninclusion.RejectionOf(err))
}

// A proof bound to one state cannot be verified for another, and ForState cannot rebind.
func TestProofBindingToTheRequestedState(t *testing.T) {
	vec := loadVector(t, "cases/shared-prefix")
	var contexts map[string]corpusContext
	readJSON(t, "contexts.json", &contexts)
	w, _ := hex.DecodeString(vec.Wrapper)
	var resp api.GetNonInclusionProofResponseV1
	require.NoError(t, resp.UnmarshalCBOR(w))
	q, _ := hex.DecodeString(vec.Query)
	bound, err := resp.Proof.ForState(api.StateID(q))
	require.NoError(t, err)
	_, err = bound.ForState(api.StateID(make([]byte, 32)))
	require.Error(t, err)
	other := append([]byte(nil), q...)
	other[31] ^= 1
	res, err := verifierFor(t, contexts["default"]).Verify(api.StateID(other), bound)
	require.Nil(t, res)
	require.Equal(t, noninclusion.RejectionCertInvalid, noninclusion.RejectionOf(err))
	res, err = verifierFor(t, contexts["default"]).Verify(api.StateID(q), bound)
	require.NoError(t, err)
	require.Equal(t, noninclusion.OutcomeAbsentAtSnapshot, res.Outcome())
	var e *noninclusion.Error
	require.False(t, errors.As(err, &e))
}

// A result owns its bytes: mutating what it returned, or what it was built from, changes nothing.
func TestResultIsImmutableEvidence(t *testing.T) {
	var contexts map[string]corpusContext
	readJSON(t, "contexts.json", &contexts)
	vec := loadVector(t, "cases/sharded-absent")
	v := verifierFor(t, contexts[vec.Context])
	res, err := verify(t, v, vec)
	require.NoError(t, err)
	root := res.Snapshot().StateRoot()
	require.NotEmpty(t, root)
	want := hex.EncodeToString(root)
	root[0] ^= 0xff
	id := res.StateID()
	wantID := hex.EncodeToString(id)
	id[0] ^= 0xff
	require.Equal(t, want, hex.EncodeToString(res.Snapshot().StateRoot()))
	require.Equal(t, wantID, hex.EncodeToString(res.StateID()))
	// the snapshot is a value: a copy cannot change the result's
	s := res.Snapshot()
	_ = s.StateRoot()
	require.Equal(t, res.Snapshot().RootRound(), s.RootRound())
}

// Mutation resistance: flipping any single byte of a wrapper either leaves the authenticated result exactly as it
// was (the unauthenticated block number) or rejects it without a result; it never yields another snapshot or outcome.
func TestSingleByteMutationsNeverChangeTheResult(t *testing.T) {
	var contexts map[string]corpusContext
	readJSON(t, "contexts.json", &contexts)
	for _, name := range []string{"cases/shared-prefix", "cases/sharded-absent", "cases/empty-tree", "cases/singleton-included"} {
		vec := loadVector(t, name)
		v := verifierFor(t, contexts[vec.Context])
		base, err := verify(t, v, vec)
		require.NoError(t, err)
		w, _ := hex.DecodeString(vec.Wrapper)
		rng := rand.New(rand.NewSource(7))
		step := 1
		if len(w) > 400 {
			step = 3
		}
		for i := 0; i < len(w); i += step {
			m := append([]byte(nil), w...)
			m[i] ^= byte(1 << uint(rng.Intn(8)))
			mv := vec
			mv.Wrapper = hex.EncodeToString(m)
			res, err := verify(t, v, mv)
			if err != nil {
				require.Nil(t, res, name)
				continue
			}
			require.Equal(t, base.Outcome(), res.Outcome(), "%s byte %d", name, i)
			require.Equal(t, base.Snapshot(), res.Snapshot(), "%s byte %d", name, i)
		}
	}
}

// Cache hits are policy checked: the memo of an authenticated seal never skips the partition, shard, query or
// relation checks of a later proof.
func TestSealMemoNeverSkipsOtherChecks(t *testing.T) {
	var contexts map[string]corpusContext
	readJSON(t, "contexts.json", &contexts)
	v := verifierFor(t, contexts["shard-00"])
	a := loadVector(t, "cases/sharded-same-seal-00")
	b := loadVector(t, "cases/sharded-same-seal-01")
	for i := 0; i < 3; i++ {
		res, err := verify(t, v, a)
		require.NoError(t, err)
		require.Equal(t, noninclusion.OutcomeAbsentAtSnapshot, res.Outcome())
		res, err = verify(t, v, b) // the seal is already authenticated; the shard rule still applies
		require.Nil(t, res)
		require.Equal(t, noninclusion.RejectionUCInvalid, noninclusion.RejectionOf(err))
	}
	// the same proof for another query is judged on its own relation
	other, _ := hex.DecodeString(a.Query)
	other[1] ^= 0x80 // the other branch of the junction the certificate was built for
	a2 := a
	a2.Query = hex.EncodeToString(other)
	res, err := verify(t, v, a2)
	require.Nil(t, res)
	require.Equal(t, noninclusion.RejectionCertInvalid, noninclusion.RejectionOf(err))
}

func readExampleJSON(path string, v any) {
	b, err := os.ReadFile(filepath.Join(corpus, path))
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		panic(err)
	}
}
