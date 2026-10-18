// Command consolidate-proofs consolidates inclusion proofs of one partition
// under a single master unicity seal (master-first, see pkg/consolidation).
//
//	consolidate-proofs -config config.json -in items.json -out bundle.json
//
// It only reads from the shard aggregators (get_inclusion_proofs.v1); it never
// submits a certification request, never modifies its inputs and writes the
// output file atomically only on success. The result is an ordinary proof
// bundle, not a token format.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation/client"
)

// Exit codes.
const (
	exitOK          = 0
	exitInput       = 2   // bad flags, configuration or input files
	exitUnavailable = 3   // explicit unavailable outcome; inputs intact, nothing resubmitted
	exitRejected    = 4   // refused: server rejection, malformed response or verification failure
	exitOutput      = 5   // the bundle could not be delivered (stdout or output file)
	exitOperational = 6   // transport failure, attempt deadline or server-side operational error
	exitCancelled   = 130 // cancelled by the caller
)

// Config is the trusted deployment configuration.
type Config struct {
	PartitionID    uint32        `json:"partitionId"`
	Shards         []ShardConfig `json:"shards"`
	TrustBases     []string      `json:"trustBases"`
	TimeoutSeconds float64       `json:"timeoutSeconds,omitempty"`
	MaxParallel    int           `json:"maxParallel,omitempty"`
}

// ShardConfig is one shard endpoint. ShardBits is the shard ID as a bit string
// ("" for a single-shard partition, "01" for shard 01); ConfHash is the
// OPTIONAL independently admitted shard configuration hash (hex, 32 bytes); all shards or none.
type ShardConfig struct {
	ShardBits string `json:"shardBits"`
	URL       string `json:"url"`
	ConfHash  string `json:"confHash"`
}

// Items is the input file: original requests and the proofs that certified them.
type Items struct {
	Items []ItemJSON `json:"items"`
}

// ItemJSON carries hex CBOR: a CertificationRequest and its get_inclusion_proof.v2 result.
type ItemJSON struct {
	Request string `json:"request"`
	Proof   string `json:"proof"`
}

// Bundle is the output file.
type Bundle struct {
	Version     int                    `json:"version"`
	MasterShard string                 `json:"masterShard"`
	RootRound   uint64                 `json:"rootRound"`
	Stats       client.Stats           `json:"stats"`
	Counters    consolidation.Counters `json:"counters"`
	// Proofs are in input order; each is a get_inclusion_proof.v2 result (hex CBOR)
	// whose UC is rooted in the common master seal.
	Proofs []BundleProof `json:"proofs"`
	// UnicityCertificates by shard bits (hex CBOR), all with the identical master seal.
	UnicityCertificates map[string]string `json:"unicityCertificates"`
}

// BundleProof is one consolidated proof.
type BundleProof struct {
	Input   int    `json:"input"`
	StateID string `json:"stateId"`
	Shard   string `json:"shard"`
	Proof   string `json:"proof"`
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("consolidate-proofs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("config", "", "deployment configuration (JSON)")
	inPath := fs.String("in", "", "input items (JSON)")
	outPath := fs.String("out", "", "output bundle (JSON); stdout when empty")
	master := fs.String("master", "", "master shard bits (default: shard of the newest evidence)")
	timeout := fs.Duration("timeout", 0, "total deadline of the attempt (overrides the configuration)")
	if err := fs.Parse(args); err != nil {
		return exitInput
	}
	if *cfgPath == "" || *inPath == "" {
		fmt.Fprintln(stderr, "usage: consolidate-proofs -config config.json -in items.json [-out bundle.json] [-master bits] [-timeout 10s]")
		return exitInput
	}

	cfg, err := loadConfig(*cfgPath, *master, *timeout)
	if err != nil {
		return fail(stderr, exitInput, "input", err)
	}
	items, err := loadItems(*inPath)
	if err != nil {
		return fail(stderr, exitInput, "input", err)
	}
	c, err := client.New(*cfg)
	if err != nil {
		return fail(stderr, exitInput, "input", err)
	}

	res, err := c.Consolidate(context.Background(), items)
	if err != nil {
		var ce *client.Error
		if errors.As(err, &ce) {
			code := exitRejected
			switch ce.Kind {
			case client.KindUnavailable:
				code = exitUnavailable
			case client.KindTransport, client.KindTimeout, client.KindServer:
				code = exitOperational
			case client.KindCancelled:
				code = exitCancelled
			case client.KindInput:
				code = exitInput
			}
			return fail(stderr, code, string(ce.Kind), err)
		}
		return fail(stderr, exitRejected, "error", err)
	}

	b, err := buildBundle(res, items)
	if err != nil {
		return fail(stderr, exitRejected, "output", err)
	}
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return fail(stderr, exitRejected, "output", err)
	}
	data = append(data, '\n')
	if *outPath == "" {
		n, err := stdout.Write(data)
		if err == nil && n < len(data) {
			err = io.ErrShortWrite
		}
		if err != nil {
			return fail(stderr, exitOutput, "output", fmt.Errorf("writing bundle to stdout: %w", err))
		}
		return exitOK
	}
	if err := writeAtomic(*outPath, data); err != nil {
		return fail(stderr, exitOutput, "output", err)
	}
	return exitOK
}

func fail(stderr io.Writer, code int, kind string, err error) int {
	msg := map[string]any{"error": map[string]any{"kind": kind, "message": err.Error()}}
	var ce *client.Error
	if errors.As(err, &ce) {
		m := msg["error"].(map[string]any)
		m["detail"], m["retryable"] = ce.Detail, ce.Retryable
		if ce.Outcome != "" {
			m["outcome"] = string(ce.Outcome)
		}
		if ce.Code != 0 {
			m["rpcCode"] = ce.Code
		}
	}
	_ = json.NewEncoder(stderr).Encode(msg)
	return code
}

func shardFromBits(bits string) (types.ShardID, error) {
	id := types.ShardID{}
	for _, c := range bits {
		a, b := id.Split()
		switch c {
		case '0':
			id = a
		case '1':
			id = b
		default:
			return id, fmt.Errorf("invalid shard bits %q", bits)
		}
	}
	return id, nil
}

func loadConfig(path, master string, timeout time.Duration) (*client.Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cf Config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cf); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if len(cf.Shards) == 0 || len(cf.TrustBases) == 0 {
		return nil, errors.New("config needs shards and trustBases")
	}
	cfg := &client.Config{MaxParallel: cf.MaxParallel, Timeout: time.Duration(cf.TimeoutSeconds * float64(time.Second))}
	if timeout > 0 {
		cfg.Timeout = timeout
	}
	var scheme types.ShardingScheme
	hashes := map[string][]byte{}
	for _, s := range cf.Shards {
		id, err := shardFromBits(s.ShardBits)
		if err != nil {
			return nil, err
		}
		if s.ConfHash != "" { // optional pin; the authenticated view carries the topology
			h, err := hex.DecodeString(s.ConfHash)
			if err != nil || len(h) != 32 {
				return nil, fmt.Errorf("shard %q: confHash must be 32 bytes hex", s.ShardBits)
			}
			hashes[id.Key()] = h
		}
		cfg.Endpoints = append(cfg.Endpoints, client.Endpoint{ShardID: id, URL: s.URL})
		if id.Length() > 0 {
			scheme = append(scheme, id)
		}
	}
	if n := len(hashes); n != 0 && n != len(cf.Shards) {
		return nil, errors.New("confHash must be given for every shard or for none (pinning is all-or-none)")
	}
	cfg.Policy = consolidation.Policy{PartitionID: types.PartitionID(cf.PartitionID)}
	if len(hashes) > 0 {
		cfg.Policy.Topology = types.PartitionViewTopology{Scheme: scheme, ShardConfHashes: hashes}
	}
	if master != "" {
		m, err := shardFromBits(master)
		if err != nil {
			return nil, err
		}
		cfg.Master = &m
	}
	var tbs []*types.RootTrustBaseV1
	base := filepath.Dir(path)
	for _, f := range cf.TrustBases {
		if !filepath.IsAbs(f) {
			f = filepath.Join(base, f)
		}
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		tb := &types.RootTrustBaseV1{}
		if err := json.Unmarshal(b, tb); err != nil {
			return nil, fmt.Errorf("trust base %s: %w", f, err)
		}
		tbs = append(tbs, tb)
	}
	// the installed history must be a verified chain: every epoch is signed by its predecessor
	byEpoch := map[uint64]map[types.NetworkID]*types.RootTrustBaseV1{}
	for _, tb := range tbs {
		if byEpoch[tb.GetEpoch()] == nil {
			byEpoch[tb.GetEpoch()] = map[types.NetworkID]*types.RootTrustBaseV1{}
		}
		if byEpoch[tb.GetEpoch()][tb.GetNetworkID()] != nil {
			return nil, fmt.Errorf("duplicate trust base for network %d epoch %d", tb.GetNetworkID(), tb.GetEpoch())
		}
		byEpoch[tb.GetEpoch()][tb.GetNetworkID()] = tb
	}
	// the lowest installed epoch of a network is the configured anchor, every higher one must verify against
	// its installed predecessor: a gap would leave the epochs above it unverified (as in TrustBaseManager)
	lowest := map[types.NetworkID]uint64{}
	for _, tb := range tbs {
		if l, ok := lowest[tb.GetNetworkID()]; !ok || tb.GetEpoch() < l {
			lowest[tb.GetNetworkID()] = tb.GetEpoch()
		}
	}
	for _, tb := range tbs {
		if tb.GetEpoch() == lowest[tb.GetNetworkID()] {
			continue
		}
		prev := byEpoch[tb.GetEpoch()-1][tb.GetNetworkID()]
		if prev == nil {
			return nil, fmt.Errorf("trust base epoch %d of network %d has no installed predecessor (epoch %d is missing)", tb.GetEpoch(), tb.GetNetworkID(), tb.GetEpoch()-1)
		}
		if err := tb.Verify(prev); err != nil {
			return nil, fmt.Errorf("trust base epoch %d does not verify against epoch %d: %w", tb.GetEpoch(), prev.GetEpoch(), err)
		}
	}
	// a known successor epoch bounds the interval its predecessor's authority may certify in
	cfg.EpochEnd = func(seal *types.UnicitySeal) (uint64, bool, error) {
		if next := byEpoch[seal.Epoch+1][seal.NetworkID]; next != nil {
			return next.GetEpochStart(), true, nil
		}
		return 0, false, nil
	}
	cfg.Resolver = func(seal *types.UnicitySeal) (types.RootTrustBase, error) {
		for _, tb := range tbs {
			if tb.GetNetworkID() == seal.NetworkID && tb.GetEpoch() == seal.Epoch {
				return tb, nil
			}
		}
		return nil, fmt.Errorf("no configured trust base for network %d epoch %d", seal.NetworkID, seal.Epoch)
	}
	return cfg, nil
}

func loadItems(path string) ([]client.Item, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var in Items
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, fmt.Errorf("items: %w", err)
	}
	if len(in.Items) == 0 {
		return nil, errors.New("items: no items")
	}
	var out []client.Item
	for i, it := range in.Items {
		rb, err := hex.DecodeString(it.Request)
		if err != nil {
			return nil, fmt.Errorf("item %d request: %w", i, err)
		}
		req := &api.CertificationRequest{}
		if err := api.UnmarshalCertificationRequestCBOR(rb, req); err != nil {
			return nil, fmt.Errorf("item %d request: %w", i, err)
		}
		pb, err := hex.DecodeString(it.Proof)
		if err != nil {
			return nil, fmt.Errorf("item %d proof: %w", i, err)
		}
		var pr api.GetInclusionProofResponseV2
		if err := types.Cbor.Unmarshal(pb, &pr); err != nil || pr.InclusionProof == nil {
			return nil, fmt.Errorf("item %d proof: not a get_inclusion_proof.v2 result (%v)", i, err)
		}
		out = append(out, client.Item{Request: req, Proof: pr.InclusionProof})
	}
	return out, nil
}

func bits(id types.ShardID) string {
	return id.String()
}

func buildBundle(res *client.Result, items []client.Item) (*Bundle, error) {
	b := &Bundle{Version: 1, MasterShard: bits(res.MasterShard), RootRound: res.RootRound, Stats: res.Stats, Counters: res.Counters,
		UnicityCertificates: map[string]string{}}
	for _, so := range res.Shards {
		ucb, err := so.UC.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		b.UnicityCertificates[bits(so.ShardID)] = hex.EncodeToString(ucb)
	}
	for i, it := range items {
		pos := res.Order[i]
		so := res.Shards[pos.Shard]
		pb, err := types.Cbor.Marshal(so.Proofs[pos.Proof])
		if err != nil {
			return nil, err
		}
		b.Proofs = append(b.Proofs, BundleProof{Input: i, StateID: it.Request.StateID.String(), Shard: bits(so.ShardID), Proof: hex.EncodeToString(pb)})
	}
	return b, nil
}

// writeAtomic publishes the file completely or not at all.
func writeAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".consolidate-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
