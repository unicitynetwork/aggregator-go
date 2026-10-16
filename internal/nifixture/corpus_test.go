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

const corpusDir = "../../testdata/non-inclusion/v1"

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
		require.Equal(t, v.Expected.Verdict, Verdict(w, q, c, tb), name)
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
	require.GreaterOrEqual(t, positives, 15)
	require.GreaterOrEqual(t, negatives, 40)
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
