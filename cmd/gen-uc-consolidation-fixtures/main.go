// Command gen-uc-consolidation-fixtures generates the portable UC consolidation
// fixture corpus (testdata/uc-consolidation/v1).
//
// The corpus is generated from real components: the aggregator's state trees
// (RSMT) provide shard state roots and inclusion certificates; the root chain
// material (signed seals, certification responses, partition views, shard and
// unicity tree certificates) is produced by the bft-core block executor
// through its test-only deterministic producer (rootchain/testutils/ucfixture);
// the expected outcomes come from the public reference library
// (pkg/consolidation). Nothing is random or time dependent.
//
// Regenerate deliberately (the optional subset is copied verbatim into bft-core):
//
//	UC_FIXTURE_AGGREGATOR_COMMIT=$(git rev-parse HEAD) go run ./cmd/gen-uc-consolidation-fixtures -out testdata/uc-consolidation/v1 \
//	    [-producer-subset ../bft-core/rootchain/consensus/storage/testdata/uc-consolidation/v1]
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"strings"

	"github.com/unicitynetwork/bft-core/rootchain/testutils/ucfixture"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/consolidationcorpus"
)

func main() {
	out := flag.String("out", "testdata/uc-consolidation/v1", "output directory (must not contain stale files)")
	subset := flag.String("producer-subset", "", "also write the producer subset (for bft-core) to this directory")
	flag.Parse()
	files, err := generate(os.Getenv("UC_FIXTURE_AGGREGATOR_COMMIT"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if err := os.RemoveAll(*out); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if err := files.WriteDir(*out); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %d files to %s\n", len(files), *out)
	if *subset != "" {
		sub, err := producerSubset(files)
		if err == nil {
			err = os.RemoveAll(*subset)
		}
		if err == nil {
			err = sub.WriteDir(*subset)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Printf("wrote %d files to %s\n", len(sub), *subset)
	}
}

func caseSpecs() []caseSpec {
	return []caseSpec{
		{name: "shards-1", desc: "single shard partition: empty shard ID, trivial view, two partitions in the root tree", bits: 0, master: 0,
			txs: map[int][2]int{0: {2, 3}}, fees: 11},
		{name: "shards-4", desc: "four shards; the master and one peer participate (fewer participants than shards), several transactions per shard, older peer seal", bits: 2, master: 0, peers: []int{3},
			txs: map[int][2]int{0: {2, 3}, 3: {1, 2}}, fees: 21},
		{name: "shards-8", desc: "eight shards; master and three peers participate with different transaction counts", bits: 3, master: 2, peers: []int{0, 5, 7},
			txs: map[int][2]int{2: {2, 2}, 0: {1, 2}, 5: {3, 1}, 7: {1, 1}}, fees: 31},
		{name: "shards-4-timeout-unchanged", desc: "changed and unchanged leaves under one seal: a timeout (technical record changes, input record repeated), an empty block record, a new write and an unchanged peer", bits: 2, master: 1, peers: []int{2},
			txs: map[int][2]int{1: {2, 2}, 2: {1, 2}}, fees: 41,
			fExtra: []ucfixture.Change{{Shard: 0, Timeout: true}, {Shard: 3, Hash: hash32("other write"), Fees: 5}}},
		{name: "large-values", desc: "uint64 values above the JS safe integer range in timestamps, reference times and fees", bits: 1, master: 1, peers: []int{0},
			txs: map[int][2]int{1: {1, 2}, 0: {2, 1}}, baseTime: 1 << 60, fees: 1 << 63},
		{name: "witness-alt", salt: "shards-4", desc: "same content as shards-4 with the master seal signed by another valid quorum subset", bits: 2, master: 0, peers: []int{3},
			txs: map[int][2]int{0: {2, 3}, 3: {1, 2}}, fees: 21, fSigners: []int{1, 2, 3}},
		{name: "nil-vs-empty", desc: "nil versus empty executed transactions hash in the input records", bits: 1, master: 0, peers: []int{1},
			txs: map[int][2]int{0: {1, 1}, 1: {1, 1}}, fees: 51, etHash: map[int][]byte{0: {}, 1: nil}},
	}
}

// generate builds the corpus files by relative path, including manifest.json.
func generate(aggregatorCommit string) (consolidationcorpus.Files, error) {
	all := consolidationcorpus.Files{}
	var index []manifestCase
	var shards4 *builtCase
	for _, spec := range caseSpecs() {
		bc, err := buildCase(spec)
		if err != nil {
			return nil, fmt.Errorf("case %s: %w", spec.name, err)
		}
		for n, b := range bc.files {
			all[spec.name+"/"+n] = b
		}
		index = append(index, manifestCase{Name: spec.name, Kind: "positive", Outcome: consolidationcorpus.OutcomeOK, Description: spec.desc})
		if spec.name == "shards-4" {
			shards4 = bc
		}
	}
	for _, n := range negatives(shards4) {
		f := shards4.files.Clone()
		if err := n.mutate(shards4, f); err != nil {
			return nil, fmt.Errorf("negative %s: %w", n.name, err)
		}
		res, err := consolidationcorpus.Evaluate(f)
		if err != nil {
			return nil, fmt.Errorf("negative %s: %w", n.name, err)
		}
		if res.Stage != n.stage || res.Outcome != n.outcome || res.PipelineOutcome != n.pipeline {
			return nil, fmt.Errorf("negative %s: observed %s/%s (pipeline %q), intended %s/%s (pipeline %q)",
				n.name, res.Stage, res.Outcome, res.PipelineOutcome, n.stage, n.outcome, n.pipeline)
		}
		exp := negativeExpected{Base: "shards-4", Description: n.desc, Stage: res.Stage, Outcome: res.Outcome, PipelineOutcome: res.PipelineOutcome}
		prefix := "negative/" + n.name + "/"
		for name, b := range f {
			if name == consolidationcorpus.FileExpected {
				continue
			}
			if base, ok := shards4.files[name]; !ok || string(base) != string(b) {
				all[prefix+name] = b
				exp.ChangedFiles = append(exp.ChangedFiles, name)
			}
		}
		for name := range shards4.files {
			if _, ok := f[name]; !ok {
				exp.RemovedFiles = append(exp.RemovedFiles, name)
			}
		}
		sort.Strings(exp.ChangedFiles)
		sort.Strings(exp.RemovedFiles)
		if len(exp.ChangedFiles) == 0 {
			return nil, fmt.Errorf("negative %s changes nothing", n.name)
		}
		eb := consolidationcorpus.Files{}
		if err := eb.Encode(consolidationcorpus.FileExpected, exp); err != nil {
			return nil, err
		}
		all[prefix+consolidationcorpus.FileExpected] = eb[consolidationcorpus.FileExpected]
		index = append(index, manifestCase{Name: "negative/" + n.name, Kind: "negative", Stage: n.stage, Outcome: n.outcome, Description: n.desc})
	}

	m, err := buildManifest(all, index, aggregatorCommit)
	if err != nil {
		return nil, err
	}
	all["manifest.json"] = m
	return all, nil
}

type negativeExpected struct {
	Base            string   `json:"base"`
	Description     string   `json:"description"`
	Stage           string   `json:"stage"`
	Outcome         string   `json:"outcome"`
	PipelineOutcome string   `json:"pipelineOutcome,omitempty"`
	ChangedFiles    []string `json:"changedFiles"`
	RemovedFiles    []string `json:"removedFiles,omitempty"`
}

type manifestCase struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Stage       string `json:"stage,omitempty"`
	Outcome     string `json:"outcome"`
	Description string `json:"description"`
}

type manifest struct {
	Corpus    string            `json:"corpus"`
	Version   int               `json:"version"`
	Generator manifestGenerator `json:"generator"`
	Limits    map[string]uint64 `json:"limits"`
	Notes     []string          `json:"notes"`
	Cases     []manifestCase    `json:"cases"`
	Files     map[string]string `json:"sha256"`
}

type manifestGenerator struct {
	Command   string            `json:"command"`
	Toolchain string            `json:"toolchain"`
	Commits   map[string]string `json:"commits"`
	Modules   map[string]string `json:"modules"`
}

func buildManifest(files consolidationcorpus.Files, cases []manifestCase, aggregatorCommit string) ([]byte, error) {
	mods := map[string]string{}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			switch d.Path {
			case "github.com/unicitynetwork/bft-go-base", "github.com/unicitynetwork/bft-core":
				mods[d.Path] = d.Version
			}
		}
	}
	commits := map[string]string{"aggregator-go": aggregatorCommit}
	for path, v := range mods {
		name := path[strings.LastIndex(path, "/")+1:]
		commits[name] = v[strings.LastIndex(v, "-")+1:] // pseudo-version commit prefix
	}
	m := manifest{
		Corpus: "uc-consolidation", Version: consolidationcorpus.Version,
		Generator: manifestGenerator{
			Command:   "UC_FIXTURE_AGGREGATOR_COMMIT=$(git rev-parse HEAD) go run ./cmd/gen-uc-consolidation-fixtures -out testdata/uc-consolidation/v1",
			Toolchain: runtime.Version(), Commits: commits, Modules: mods,
		},
		Limits: map[string]uint64{
			"maxPartitionViewLeaves": types.MaxPartitionViewLeaves, "maxPartitionViewBytes": types.MaxPartitionViewBytes,
			"maxShardIdBits": types.MaxPartitionViewShardIDBits, "maxInputRecordBytes": types.MaxPartitionViewIRBytes,
			"maxSummaryValueBytes": types.MaxPartitionViewSummaryValueBytes,
		},
		Notes: []string{
			"uint64 values in JSON are decimal strings; CBOR and proofs are lowercase hex or .cbor files",
			"a case directory holds the files of testdata layout; a negative directory holds only the complete mutated files plus expected.json naming its base case",
			"evaluation order: view_codec (partition view alone), evidence (authenticate originals, R_min), consolidate",
			"producer.json replays the root chain blocks byte for byte (bft-core rootchain/testutils/ucfixture)",
			"seal signatures are deterministic secp256k1 with public test keys; no production credentials",
		},
		Cases: cases, Files: map[string]string{},
	}
	for name, b := range files {
		h := sha256.Sum256(b)
		m.Files[name] = hex.EncodeToString(h[:])
	}
	b, err := json.MarshalIndent(m, "", "  ")
	return append(b, '\n'), err
}

// otherAuthority is a root authority with other keys than the fixture producer.
func otherAuthority() (*types.RootTrustBaseV1, error) {
	var nodes []*types.NodeInfo
	for i := 0; i < ucfixture.RootNodes; i++ {
		k := hash32("other authority", i)
		s, err := abcrypto.NewInMemorySecp256K1SignerFromKey(k)
		if err != nil {
			return nil, err
		}
		v, err := s.Verifier()
		if err != nil {
			return nil, err
		}
		pk, err := v.MarshalPublicKey()
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, &types.NodeInfo{NodeID: ucfixture.RootNodeID(i), SigKey: pk, Stake: 1})
	}
	return types.NewTrustBase(ucfixture.NetworkID, nodes, types.WithEpochStart(1))
}

// producerFiles are the files of a case which the producer (bft-core) owns.
var producerFiles = []string{
	consolidationcorpus.FileContext, consolidationcorpus.FileTrustBase, consolidationcorpus.FileProducer,
	consolidationcorpus.FileResponseV1, consolidationcorpus.FileResponseV2, consolidationcorpus.FilePartitionView,
}

// producerSubset selects the producer owned files of the positive cases, with a
// manifest of their checksums and the digest of the corpus manifest they come from.
func producerSubset(all consolidationcorpus.Files) (consolidationcorpus.Files, error) {
	var m manifest
	if err := json.Unmarshal(all["manifest.json"], &m); err != nil {
		return nil, err
	}
	sub := consolidationcorpus.Files{}
	for _, c := range m.Cases {
		if c.Kind != "positive" {
			continue
		}
		for name, b := range all {
			rel, ok := strings.CutPrefix(name, c.Name+"/")
			if !ok {
				continue
			}
			if slices.Contains(producerFiles, rel) || strings.HasPrefix(rel, consolidationcorpus.DirShardConfigs) || strings.HasPrefix(rel, consolidationcorpus.DirUCs) {
				sub[name] = b
			}
		}
	}
	sum := sha256.Sum256(all["manifest.json"])
	sm := struct {
		Corpus    string            `json:"corpus"`
		Version   int               `json:"version"`
		SourceSHA string            `json:"sourceManifestSha256"`
		Commits   map[string]string `json:"generatorCommits"`
		Files     map[string]string `json:"sha256"`
	}{"uc-consolidation-producer-subset", consolidationcorpus.Version, hex.EncodeToString(sum[:]), m.Generator.Commits, map[string]string{}}
	for name, b := range sub {
		h := sha256.Sum256(b)
		sm.Files[name] = hex.EncodeToString(h[:])
	}
	b, err := json.MarshalIndent(sm, "", "  ")
	if err != nil {
		return nil, err
	}
	sub["manifest.json"] = append(b, '\n')
	return sub, nil
}
