package consolidationcorpus

import (
	"crypto/sha256"
	"fmt"
	"strconv"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation"
)

// Stages of the consumer pipeline, in the order they run (view_codec is an
// additional check of the partition view bytes alone, run first).
const (
	StageViewCodec = "view_codec"
	// StagePolicy: the trusted context itself is unusable or inconsistent.
	StagePolicy      = "policy"
	StageEvidence    = "evidence"
	StageRPC         = "rpc"
	StageConsolidate = "consolidate"
	StageOK          = "ok"

	// OutcomeRejected is the outcome of the codec stage (no typed consolidation outcome applies).
	OutcomeRejected = "rejected"
	OutcomeOK       = "ok"
)

// Counters are the session instrumentation counters.
type Counters struct {
	QuorumChecks         int `json:"quorumChecks"`
	BodyChecks           int `json:"bodyChecks"`
	LeafChecks           int `json:"leafChecks"`
	EvidenceQuorumChecks int `json:"evidenceQuorumChecks"`
}

// ShardBundle is the consolidated output of one shard.
type ShardBundle struct {
	Shard string `json:"shard"`
	// UC is the hex CBOR of the assembled ordinary UC.
	UC string `json:"uc"`
	// Proofs are hex CBOR api.GetInclusionProofResponseV2, in request order.
	Proofs []string `json:"proofs"`
}

// RPCFailure is a JSON-RPC error of one of the batch calls, classified without
// losing what the wire said. Availability and capacity failures are not
// integrity failures: a client may retry them as a new consolidation attempt
// when Retryable.
type RPCFailure struct {
	// Role is "master" or "peer", Shard the shard whose call failed.
	Role  string `json:"role"`
	Shard string `json:"shard"`
	Code  int    `json:"code"`
	// Reason, Detail, Retryable and Limit are the error data (absent on bare errors;
	// Retryable then follows the code class).
	Reason    string  `json:"reason,omitempty"`
	Detail    string  `json:"detail,omitempty"`
	Retryable bool    `json:"retryable"`
	Limit     *uint64 `json:"limit,omitempty"`
}

// Result of evaluating a case.
type Result struct {
	Stage   string `json:"stage"`
	Outcome string `json:"outcome"`
	// PipelineOutcome is, for the codec stage, the outcome the full pipeline
	// reports for the same input (always a refusal).
	PipelineOutcome string `json:"pipelineOutcome,omitempty"`
	// RPC is set for the rpc stage.
	RPC *RPCFailure `json:"rpc,omitempty"`

	RMin      string        `json:"rMin,omitempty"`
	RootRound string        `json:"rootRound,omitempty"`
	Master    string        `json:"master,omitempty"`
	Counters  *Counters     `json:"counters,omitempty"`
	Bundle    []ShardBundle `json:"bundle,omitempty"`
}

// Classes of rpc stage outcomes.
const (
	RPCUnavailable   = "unavailable"        // -32008 availability: retry as a new attempt
	RPCUnsupported   = "unsupported"        // -32008 unsupported_configuration: never retry
	RPCBusy          = "busy"               // -32006
	RPCLimitExceeded = "limit_exceeded"     // -32007: nonretryable with unchanged input
	RPCInvalid       = "invalid_request"    // -32600, -32602
	RPCNoMethod      = "unsupported_method" // -32601: the aggregator has no batch capability
	RPCInfra         = "infrastructure"     // -32004
	RPCOther         = "rpc_error"
)

// Wire codes of the consolidation errors (arch-plan 2.4).
const (
	CodeLimitExceeded    = -32007
	CodeProofUnavailable = -32008
)

func classifyRPC(role, shard string, e *RPCError) (string, *RPCFailure) {
	f := &RPCFailure{Role: role, Shard: shard, Code: e.Code}
	if e.Data != nil {
		f.Reason, f.Detail, f.Retryable, f.Limit = e.Data.Reason, e.Data.Detail, e.Data.Retryable, e.Data.Limit
	}
	class := RPCOther
	switch e.Code {
	case CodeProofUnavailable:
		class = RPCUnavailable
		if f.Detail == "unsupported_configuration" {
			class = RPCUnsupported
		}
	case -32006:
		class = RPCBusy
	case CodeLimitExceeded:
		class = RPCLimitExceeded
	case -32600, -32602:
		class = RPCInvalid
	case -32601:
		class = RPCNoMethod
	case -32004:
		class = RPCInfra
	}
	if e.Data == nil {
		// bare error: the class decides
		f.Retryable = class == RPCBusy || class == RPCUnavailable || class == RPCInfra
	}
	return class, f
}

// Evaluate runs the consumer pipeline over the case files: partition view
// codec check, the trusted context, authentication of the original evidence
// (R_min), classification of failed batch calls, and consolidation. A returned
// error means the files are malformed as a corpus (not a typed refusal).
func Evaluate(files Files) (*Result, error) { return EvaluateWith(files, Options{}) }

// Options of EvaluateWith.
type Options struct {
	// Unpinned drops the admitted topology of the context: the scheme and the
	// configuration hashes are then those committed by the authenticated seal.
	Unpinned bool
}

// EvaluateWith is Evaluate with options.
func EvaluateWith(files Files, opts Options) (*Result, error) {
	l, err := Load(files)
	if err != nil {
		return nil, err
	}
	if opts.Unpinned {
		l.Policy.Topology = types.PartitionViewTopology{}
	}
	var reqs Requests
	var resps Responses
	if err := files.Decode(FileRequests, &reqs); err != nil {
		return nil, err
	}
	if err := files.Decode(FileResponses, &resps); err != nil {
		return nil, err
	}
	res := &Result{Master: l.Context.Master}

	runPipeline := func() (stage, outcome string, err error) {
		// the context's partition is only an equality assertion: the partition is the fixed aggregation partition
		if types.PartitionID(l.Context.PartitionID) != api.AggregationPartitionID {
			return StagePolicy, string(consolidation.OutcomeInvalidInput), nil
		}
		// the trusted context must be consistent with the installed authority
		if l.Context.NetworkID != uint32(l.TrustBase.GetNetworkID()) {
			return StagePolicy, string(consolidation.OutcomeIncompatibleLineage), nil
		}
		sess, err := l.NewSession()
		if err != nil {
			o, ok := consolidation.OutcomeOf(err)
			if !ok {
				return "", "", err
			}
			return StagePolicy, string(o), nil
		}
		bound, err := sess.AuthenticateEvidence(l.Evidence)
		if err != nil {
			o, ok := consolidation.OutcomeOf(err)
			if !ok {
				return "", "", err
			}
			return StageEvidence, string(o), nil
		}
		res.RMin = strconv.FormatUint(bound.Round(), 10)

		// failed batch calls: availability is not an integrity failure
		if resps.Master.Error != nil {
			class, f := classifyRPC("master", reqs.Master.Shard, resps.Master.Error)
			res.RPC = f
			return StageRPC, class, nil
		}
		for _, p := range resps.Peers {
			if p.Response.Error != nil {
				class, f := classifyRPC("peer", p.Shard, p.Response.Error)
				res.RPC = f
				return StageRPC, class, nil
			}
		}

		in := *l.Input
		in.Bound = bound
		out, err := sess.Consolidate(&in)
		c := sess.Counters()
		res.Counters = &Counters{QuorumChecks: c.QuorumChecks, BodyChecks: c.BodyChecks, LeafChecks: c.LeafChecks, EvidenceQuorumChecks: c.EvidenceQuorumChecks}
		if err != nil {
			o, ok := consolidation.OutcomeOf(err)
			if !ok {
				return "", "", err
			}
			return StageConsolidate, string(o), nil
		}
		res.RootRound = strconv.FormatUint(out.RootRound, 10)
		for _, so := range out.Shards {
			ucb, err := so.UC.MarshalCBOR()
			if err != nil {
				return "", "", err
			}
			sb := ShardBundle{Shard: ShardName(so.ShardID), UC: Hex(ucb)}
			for _, p := range so.Proofs {
				pb, err := types.Cbor.Marshal(p)
				if err != nil {
					return "", "", err
				}
				sb.Proofs = append(sb.Proofs, Hex(pb))
			}
			res.Bundle = append(res.Bundle, sb)
		}
		return StageOK, OutcomeOK, nil
	}

	if len(l.Input.PartitionView) > 0 {
		if _, err := types.DecodePartitionViewV1(l.Input.PartitionView); err != nil {
			res.Stage, res.Outcome = StageViewCodec, OutcomeRejected
			// the full pipeline must refuse as well
			_, o, err := runPipeline()
			if err != nil {
				return nil, err
			}
			res.PipelineOutcome = o
			res.Counters, res.RMin, res.RootRound, res.Bundle, res.RPC = nil, "", "", nil, nil
			return res, nil
		}
	}
	res.Stage, res.Outcome, err = runPipeline()
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Loaded is a decoded case: everything a consumer needs to run the pipeline.
type Loaded struct {
	Context   Context
	TrustBase *types.RootTrustBaseV1
	Policy    consolidation.Policy
	Input     *consolidation.Input
	Evidence  []consolidation.Evidence
}

// Load decodes the case files.
func Load(files Files) (*Loaded, error) {
	l := &Loaded{}
	var reqs Requests
	var resps Responses
	for name, v := range map[string]any{FileContext: &l.Context, FileRequests: &reqs, FileResponses: &resps} {
		if err := files.Decode(name, v); err != nil {
			return nil, err
		}
	}
	l.TrustBase = &types.RootTrustBaseV1{}
	if err := l.TrustBase.UnmarshalCBOR(files[l.Context.TrustBase]); err != nil {
		return nil, fmt.Errorf("trust base: %w", err)
	}
	var err error
	if l.Policy, err = policyOf(&l.Context); err != nil {
		return nil, err
	}
	if l.Input, l.Evidence, _, err = inputOf(&l.Context, &reqs, &resps); err != nil {
		return nil, err
	}
	return l, nil
}

// NewSession creates a verification session with the trusted authority of the case.
func (l *Loaded) NewSession() (*consolidation.Session, error) {
	return consolidation.NewSession(l.Policy, func(*types.UnicitySeal) (types.RootTrustBase, error) { return l.TrustBase, nil })
}

func policyOf(ctx *Context) (consolidation.Policy, error) {
	p := consolidation.Policy{}
	p.Topology.ShardConfHashes = map[string][]byte{}
	for _, s := range ctx.Shards {
		if s == "" {
			continue
		}
		id, err := ParseShard(s)
		if err != nil {
			return p, err
		}
		p.Topology.Scheme = append(p.Topology.Scheme, id)
	}
	for _, c := range ctx.ShardConfigs {
		id, err := ParseShard(c.Shard)
		if err != nil {
			return p, err
		}
		h, err := UnHex(c.Hash)
		if err != nil {
			return p, err
		}
		p.Topology.ShardConfHashes[id.Key()] = h
	}
	return p, nil
}

func inputOf(ctx *Context, reqs *Requests, resps *Responses) (*consolidation.Input, []consolidation.Evidence, []byte, error) {
	shardResult := func(b Batch, r RPCResponse) (consolidation.ShardResult, error) {
		id, err := ParseShard(b.Shard)
		if err != nil {
			return consolidation.ShardResult{}, err
		}
		sr := consolidation.ShardResult{ShardID: id}
		for _, h := range b.Requests {
			req, err := decodeRequest(h)
			if err != nil {
				return sr, err
			}
			sr.Requests = append(sr.Requests, req)
		}
		if r.Result == nil {
			return sr, nil // a failed call: classified by the rpc stage
		}
		for _, h := range r.Result.Proofs {
			p, err := decodeProof(h)
			if err != nil {
				return sr, err
			}
			sr.Proofs = append(sr.Proofs, p)
		}
		return sr, nil
	}
	in := &consolidation.Input{}
	var err error
	if in.Master, err = shardResult(reqs.Master, resps.Master); err != nil {
		return nil, nil, nil, err
	}
	if resps.Master.Result != nil && resps.Master.Result.PartitionView != "" {
		if in.PartitionView, err = UnHex(resps.Master.Result.PartitionView); err != nil {
			return nil, nil, nil, err
		}
	}
	if len(reqs.Peers) != len(resps.Peers) {
		return nil, nil, nil, fmt.Errorf("%d peer batches but %d peer responses", len(reqs.Peers), len(resps.Peers))
	}
	for i, b := range reqs.Peers {
		if resps.Peers[i].Shard != b.Shard {
			return nil, nil, nil, fmt.Errorf("peer %d: response of shard %q for batch of %q", i, resps.Peers[i].Shard, b.Shard)
		}
		sr, err := shardResult(b, resps.Peers[i].Response)
		if err != nil {
			return nil, nil, nil, err
		}
		in.Peers = append(in.Peers, sr)
	}
	var evidence []consolidation.Evidence
	for _, e := range reqs.Evidence {
		req, err := decodeRequest(e.Request)
		if err != nil {
			return nil, nil, nil, err
		}
		p, err := decodeProof(e.Proof)
		if err != nil {
			return nil, nil, nil, err
		}
		evidence = append(evidence, consolidation.Evidence{Request: req, Proof: p.InclusionProof})
	}
	return in, evidence, in.PartitionView, nil
}

func decodeRequest(h string) (*api.CertificationRequest, error) {
	b, err := UnHex(h)
	if err != nil {
		return nil, err
	}
	var r api.CertificationRequest
	if err := types.Cbor.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("certification request: %w", err)
	}
	return &r, nil
}

func decodeProof(h string) (*api.GetInclusionProofResponseV2, error) {
	b, err := UnHex(h)
	if err != nil {
		return nil, err
	}
	var p api.GetInclusionProofResponseV2
	if err := types.Cbor.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("inclusion proof: %w", err)
	}
	return &p, nil
}

// EncodeRequest and EncodeProof produce the corpus hex forms.
func EncodeRequest(r *api.CertificationRequest) (string, error) {
	b, err := types.Cbor.Marshal(r)
	return Hex(b), err
}

func EncodeProof(p *api.GetInclusionProofResponseV2) (string, error) {
	b, err := types.Cbor.Marshal(p)
	return Hex(b), err
}

// DecodeRequest and DecodeProof parse the corpus hex forms.
func DecodeRequest(h string) (*api.CertificationRequest, error)      { return decodeRequest(h) }
func DecodeProof(h string) (*api.GetInclusionProofResponseV2, error) { return decodeProof(h) }

// Digest is the lowercase hex SHA-256 of b.
func Digest(b []byte) string {
	h := sha256.Sum256(b)
	return Hex(h[:])
}
