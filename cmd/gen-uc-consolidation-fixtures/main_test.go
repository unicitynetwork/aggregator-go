package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/consolidationcorpus"
)

const committedCorpus = "../../testdata/uc-consolidation/v1"

// The generator is deterministic and the committed corpus is exactly its output
// (for the pinned module versions). The test never writes the corpus: regenerate
// it deliberately with the command in the manifest.
func TestCommittedCorpusIsTheGeneratorOutput(t *testing.T) {
	committed, err := consolidationcorpus.ReadDir(committedCorpus)
	require.NoError(t, err)
	var m manifest
	require.NoError(t, json.Unmarshal(committed["manifest.json"], &m))

	// the manifest pins the same module versions this build uses
	probe, err := buildManifest(consolidationcorpus.Files{}, nil, "")
	require.NoError(t, err)
	var pm manifest
	require.NoError(t, json.Unmarshal(probe, &pm))
	require.Equal(t, m.Generator.Modules, pm.Generator.Modules, "module pins changed: regenerate the corpus")

	first, err := generate(m.Generator.Commits["aggregator-go"])
	require.NoError(t, err)
	second, err := generate(m.Generator.Commits["aggregator-go"])
	require.NoError(t, err)
	require.Equal(t, first, second, "generator is not deterministic")

	require.Len(t, first, len(committed))
	for name, b := range first {
		got, ok := committed[name]
		require.True(t, ok, "%s is not committed", name)
		if name == "manifest.json" {
			// toolchain version may legitimately differ between machines
			var a, c manifest
			require.NoError(t, json.Unmarshal(b, &a))
			require.NoError(t, json.Unmarshal(got, &c))
			require.Equal(t, a.Files, c.Files)
			require.Equal(t, a.Cases, c.Cases)
			continue
		}
		require.Equal(t, string(b), string(got), "%s differs from the committed corpus", name)
	}
}
