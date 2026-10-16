package noninclusion_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/noninclusion"
)

const corpus = "../../testdata/non-inclusion/v1"

type vector struct {
	Name     string `json:"name"`
	Query    string `json:"query"`
	Wrapper  string `json:"wrapper"`
	Context  string `json:"context"`
	Expected struct {
		Verdict string `json:"verdict"`
	} `json:"expected"`
}

type corpusContext struct {
	TrustBase      string `json:"trustBase"`
	Partition      uint32 `json:"partition"`
	NextEpochStart string `json:"nextEpochStart"`
	MinRootRound   string `json:"minRootRound"`
	ShardConfHash  string `json:"shardConfHash"`
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(corpus, path))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, v))
}

func u64(t *testing.T, s string) uint64 {
	n, err := strconv.ParseUint(s, 10, 64)
	require.NoError(t, err)
	return n
}

func verifierFor(t *testing.T, cc corpusContext) *noninclusion.Verifier {
	tbBytes, err := os.ReadFile(filepath.Join(corpus, cc.TrustBase))
	require.NoError(t, err)
	tb := &types.RootTrustBaseV1{}
	require.NoError(t, tb.UnmarshalCBOR(tbBytes))
	ctx := noninclusion.Context{
		Partition:    types.PartitionID(cc.Partition),
		MinRootRound: u64(t, cc.MinRootRound),
		Resolver:     func(*types.UnicitySeal) (types.RootTrustBase, error) { return tb, nil },
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

// The production codec and verifier reproduce the verdict of every vector of the shared corpus.
func TestCorpusVerdicts(t *testing.T) {
	var contexts map[string]corpusContext
	readJSON(t, "contexts.json", &contexts)
	n := 0
	for _, dir := range []string{"cases", "negative"} {
		entries, err := os.ReadDir(filepath.Join(corpus, dir))
		require.NoError(t, err)
		for _, e := range entries {
			var v vector
			readJSON(t, dir+"/"+e.Name(), &v)
			t.Run(dir+"/"+strings.TrimSuffix(e.Name(), ".json"), func(t *testing.T) {
				wrapper, err := hex.DecodeString(v.Wrapper)
				require.NoError(t, err)
				query, err := hex.DecodeString(v.Query)
				require.NoError(t, err)
				var verdict string
				var resp api.GetNonInclusionProofResponseV1
				if err := resp.UnmarshalCBOR(wrapper); err != nil {
					require.ErrorIs(t, err, api.ErrNonInclusionProofMalformed)
					verdict = noninclusion.VerdictMalformed
				} else {
					// the exact profile round-trips byte for byte
					re, err := resp.MarshalCBOR()
					require.NoError(t, err)
					require.Equal(t, wrapper, re)
					bound, err := resp.Proof.ForState(api.StateID(query))
					require.NoError(t, err)
					_, err = verifierFor(t, contexts[v.Context]).VerifyFor(api.StateID(query), bound)
					verdict = noninclusion.VerdictOf(err)
					if verdict == "" {
						require.NoError(t, err)
					}
				}
				require.Equal(t, v.Expected.Verdict, verdict)
			})
			n++
		}
	}
	require.GreaterOrEqual(t, n, 55)
}

// A proof bound to one state cannot be verified for another, and ForState cannot rebind.
func TestProofBindingToTheRequestedState(t *testing.T) {
	var v vector
	readJSON(t, "cases/shared-prefix.json", &v)
	var contexts map[string]corpusContext
	readJSON(t, "contexts.json", &contexts)
	w, _ := hex.DecodeString(v.Wrapper)
	var resp api.GetNonInclusionProofResponseV1
	require.NoError(t, resp.UnmarshalCBOR(w))
	q, _ := hex.DecodeString(v.Query)
	bound, err := resp.Proof.ForState(api.StateID(q))
	require.NoError(t, err)
	_, err = bound.ForState(api.StateID(make([]byte, 32)))
	require.Error(t, err)
	other := append([]byte(nil), q...)
	other[31] ^= 1
	_, err = verifierFor(t, contexts["default"]).VerifyFor(api.StateID(other), bound)
	require.Equal(t, noninclusion.VerdictCertInvalid, noninclusion.VerdictOf(err))
	_, err = verifierFor(t, contexts["default"]).VerifyFor(api.StateID(q), bound)
	require.NoError(t, err)
	var e *noninclusion.Error
	require.False(t, errors.As(err, &e))
}
