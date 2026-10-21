package nifixture

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

const corpusDir = "../../testdata/non-inclusion/v2"

func readCorpus(t *testing.T) Files {
	t.Helper()
	files := Files{}
	require.NoError(t, filepath.Walk(corpusDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(corpusDir, p)
		b, err := os.ReadFile(p)
		files[filepath.ToSlash(rel)] = b
		return err
	}))
	return files
}

// The committed corpus is exactly what the generator writes (regenerate deliberately).
func TestCommittedCorpusIsTheGeneratorOutput(t *testing.T) {
	var m struct {
		Generator struct{ Commit string } `json:"generator"`
	}
	committed := readCorpus(t)
	require.NoError(t, json.Unmarshal(committed["manifest.json"], &m))
	gen, err := Generate(m.Generator.Commit)
	require.NoError(t, err)
	require.Equal(t, len(gen), len(committed))
	for name, b := range gen {
		require.Equal(t, string(b), string(committed[name]), name)
	}
}

func TestManifestChecksums(t *testing.T) {
	files := readCorpus(t)
	var m struct {
		SHA256 map[string]string `json:"sha256"`
	}
	require.NoError(t, json.Unmarshal(files["manifest.json"], &m))
	for name, b := range files {
		if name == "manifest.json" {
			continue
		}
		h := sha256.Sum256(b)
		require.Equal(t, m.SHA256[name], hex.EncodeToString(h[:]), name)
	}
	require.Len(t, m.SHA256, len(files)-1)
}

// Every vector reproduces: the reference model rebuilds the tree, certificate, root and wrapper from the
// leaves; the committed UC authenticates (for positives) and the verdict is the expected one.
func TestVectorsAgreeWithTheReferenceModel(t *testing.T) {
	files := readCorpus(t)
	var contexts map[string]Context
	require.NoError(t, json.Unmarshal(files["contexts.json"], &contexts))
	var positives, negatives int
	for name, b := range files {
		isPos, isNeg := strings.HasPrefix(name, "cases/"), strings.HasPrefix(name, "negative/")
		if !isPos && !isNeg {
			continue
		}
		var v Vector
		require.NoError(t, json.Unmarshal(b, &v), name)
		c, ok := contexts[v.Context]
		require.True(t, ok, name)
		var tb types.RootTrustBase = decodeTB(t, files[c.TrustBase])
		w, q := unhex(v.Wrapper), [32]byte(unhex(v.Query))
		verdict, snap := Verdict(w, q, c, tb)
		want := expectedOf(verdict, snap, v.Query, v.Expected.Acceptable)
		want.Verdict = "" // the intended classification is not part of the files
		require.Equal(t, want, v.Expected, name)
		if want.Outcome == OutcomeRejected {
			require.Nil(t, v.Expected.Snapshot, name+": a rejection carries no snapshot")
			require.Empty(t, v.Expected.StateID, name)
		} else {
			require.Equal(t, v.Query, v.Expected.StateID, name)
			require.NotNil(t, v.Expected.Snapshot, name)
		}
		if isPos {
			positives++
			var leaves []Leaf
			for _, l := range v.Leaves {
				leaves = append(leaves, Leaf{Key: [32]byte(unhex(l.Key)), Value: [32]byte(unhex(l.Value))})
			}
			tree, err := Build(leaves)
			require.NoError(t, err)
			if r, ok := tree.Root(); ok {
				require.Equal(t, v.Root, hex.EncodeToString(r[:]), name)
			} else if v.Name == "zero-root" {
				require.Equal(t, strings.Repeat("00", 32), v.Root)
			} else {
				require.Equal(t, "", v.Root, name)
			}
			if v.Name != "zero-root" {
				require.Equal(t, v.Certificate, hex.EncodeToString(tree.Certificate(q)), name)
			}
			_, cert, _, err := DecodeWrapper(w)
			require.NoError(t, err)
			require.Equal(t, v.Certificate, hex.EncodeToString(cert))
		} else {
			negatives++
		}
	}
	require.GreaterOrEqual(t, positives, 25)
	require.GreaterOrEqual(t, negatives, 55)
}

func decodeTB(t *testing.T, b []byte) types.RootTrustBase {
	tb := &types.RootTrustBaseV1{}
	require.NoError(t, tb.UnmarshalCBOR(b))
	return tb
}

// Property check of the reference relation on random trees: a query is absent iff the tree lacks it, a
// certificate for another key never verifies for a query choosing another branch, and any single bit flip of a
// verifying certificate stops it verifying as absent.
func TestReferenceRelationOnRandomTrees(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for iter := 0; iter < 300; iter++ {
		n := rng.Intn(40)
		var leaves []Leaf
		seen := map[[32]byte]bool{}
		for len(leaves) < n {
			var l Leaf
			rng.Read(l.Key[:])
			if rng.Intn(3) == 0 && len(leaves) > 0 { // dense prefix sharing
				l.Key = leaves[rng.Intn(len(leaves))].Key
				l.Key[rng.Intn(32)] ^= 1 << uint(rng.Intn(8))
			}
			rng.Read(l.Value[:])
			if !seen[l.Key] {
				seen[l.Key] = true
				leaves = append(leaves, l)
			}
		}
		tree, err := Build(leaves)
		require.NoError(t, err)
		root, ok := tree.Root()
		var rp *[32]byte
		if ok {
			rp = &root
		}
		var q [32]byte
		rng.Read(q[:])
		if n > 0 && rng.Intn(4) == 0 {
			q = leaves[rng.Intn(n)].Key
		}
		cert := tree.Certificate(q)
		err = Verify(cert, q, rp)
		if tree.Contains(q) {
			require.ErrorIs(t, err, ErrStateIncluded)
			continue
		}
		require.NoError(t, err)
		for k := 0; k < 8 && len(cert) > 0; k++ {
			m := bytes.Clone(cert)
			m[rng.Intn(len(m))] ^= 1 << uint(rng.Intn(8))
			require.Error(t, Verify(m, q, rp), "mutated certificate verifies as absent")
		}
	}
}

// The SDKs read the JSON form: it must be the same authority as the CBOR one, and acceptable verdicts are rejections.
func TestJSONTrustBasesAndAcceptableVerdicts(t *testing.T) {
	files := readCorpus(t)
	var contexts map[string]Context
	require.NoError(t, json.Unmarshal(files["contexts.json"], &contexts))
	for name, c := range contexts {
		cb, ok := files[c.TrustBase]
		require.True(t, ok, name)
		jb, ok := files[c.TrustBaseJSON]
		require.True(t, ok, name)
		a, b := &types.RootTrustBaseV1{}, &types.RootTrustBaseV1{}
		require.NoError(t, a.UnmarshalCBOR(cb))
		require.NoError(t, json.Unmarshal(jb, b), name)
		ab, err := a.MarshalCBOR()
		require.NoError(t, err)
		bb, err := b.MarshalCBOR()
		require.NoError(t, err)
		require.Equal(t, ab, bb, name)
	}
	for name, b := range files {
		if !strings.HasPrefix(name, "negative/") {
			continue
		}
		var v Vector
		require.NoError(t, json.Unmarshal(b, &v))
		for _, a := range v.Expected.Acceptable {
			require.Contains(t, []string{VerdictMalformed, VerdictUCInvalid, VerdictAuthority, VerdictCertInvalid}, a, name)
		}
		if v.Expected.Outcome != OutcomeRejected {
			require.Empty(t, v.Expected.Acceptable, name)
		}
	}
}

// The corpus partition is the fixed aggregation partition and every snapshot is of it; the schema has no freshness
// verdict or option.
func TestSchemaHasNoFreshnessAndTheFixedPartition(t *testing.T) {
	require.EqualValues(t, 7, Partition)
	files := readCorpus(t)
	require.NotContains(t, string(files["contexts.json"]), "minRootRound")
	require.NotContains(t, string(files["contexts.json"]), `"partition"`)
	for name, b := range files {
		if strings.HasPrefix(name, "cases/") || strings.HasPrefix(name, "negative/") {
			require.NotContains(t, string(b), "stale", name)
			var v Vector
			require.NoError(t, json.Unmarshal(b, &v))
			if v.Expected.Snapshot != nil {
				require.EqualValues(t, 7, v.Expected.Snapshot.PartitionID, name)
			}
		}
	}
}

// Every sharded and every large-value case is present with the snapshot fields the UC authenticates.
func TestSnapshotsOfTheCorpus(t *testing.T) {
	files := readCorpus(t)
	read := func(name string) Vector {
		var v Vector
		require.NoError(t, json.Unmarshal(files[name+".json"], &v), name)
		return v
	}
	big := read("cases/large-uint64-fields").Expected.Snapshot
	require.Equal(t, "18446744073709551613", big.RootRound)
	require.Equal(t, "18446744073709551612", big.SealTimestampSeconds)
	require.Equal(t, "18446744073709551611", big.RootEpoch)
	require.Equal(t, "18446744073709551610", big.ShardRound)
	require.Equal(t, "9007199254740993", big.ShardEpoch)
	empty := read("cases/empty-tree").Expected.Snapshot
	require.Nil(t, empty.StateRoot, "the certified empty tree is a null state root")
	sharded := read("cases/sharded-absent").Expected.Snapshot
	require.Equal(t, "00", sharded.ShardID)
	require.Equal(t, "", read("cases/shared-prefix").Expected.Snapshot.ShardID)
	require.NotNil(t, sharded.StateRoot)
	require.Equal(t, "01", read("cases/sharded-empty-tree").Expected.Snapshot.ShardID)
	require.Nil(t, read("cases/sharded-empty-tree").Expected.Snapshot.StateRoot)
	for _, n := range []string{"sharded-included", "sharded-shard-01-absent", "sharded-shard-11-singleton", "sharded-random-16", "sharded-same-seal-00", "sharded-same-seal-01"} {
		require.NotEmpty(t, read("cases/"+n).Expected.Snapshot.ShardID, n)
	}
	require.Equal(t, OutcomeIncluded, read("cases/sharded-included").Expected.Outcome)
	require.Equal(t, read("cases/sharded-same-seal-00").Expected.Snapshot.RootRound, read("cases/sharded-same-seal-01").Expected.Snapshot.RootRound)
}

// The policy examples follow from the reference policy over the verified snapshots.
func TestPolicyExamplesAreConsistent(t *testing.T) {
	files := readCorpus(t)
	var p struct {
		Examples []policyExample `json:"examples"`
	}
	require.NoError(t, json.Unmarshal(files["policy.json"], &p))
	require.GreaterOrEqual(t, len(p.Examples), 10)
	seen := map[string]bool{}
	for _, e := range p.Examples {
		require.Equal(t, e.Decision, PolicyDecision(e.Snapshot, e.Checkpoint), e.Name)
		seen[e.Decision] = true
		var v Vector
		require.NoError(t, json.Unmarshal(files[e.Vector+".json"], &v))
		require.Equal(t, *v.Expected.Snapshot, e.Snapshot, e.Name)
	}
	require.True(t, seen["accept"] && seen["reject"])
}
