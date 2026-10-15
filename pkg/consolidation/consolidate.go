// Package consolidation is the pure reference implementation of proof
// consolidation: given original certification requests, per-shard inclusion
// proofs and the master partition view, it authenticates the master view and
// rebuilds every proof under the single master unicity seal. It performs no
// I/O; fetching the responses is the job of the HTTP client.
package consolidation

import (
	"bytes"
	"crypto"
	"errors"
	"slices"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ShardResult is one shard's original requests and the proofs returned for
// them, in request order.
type ShardResult struct {
	ShardID  types.ShardID
	Requests []*api.CertificationRequest
	Proofs   []*api.GetInclusionProofResponseV2
}

// Input of one consolidation attempt.
type Input struct {
	// Master is the shard whose latest batch supplied PartitionView; its
	// proofs are bound to the UC enclosing the view.
	Master ShardResult
	// PartitionView is the raw hex-decoded CBOR partition view of the master response.
	PartitionView []byte
	// Peers are the other participating shards, fetched at the exact roots of the view.
	Peers []ShardResult
	// Bound is the authenticated minimal root chain round (R_min) the master
	// seal must reach, produced by Session.AuthenticateEvidence. It cannot be
	// constructed by callers. Nil means unknown: the result is unavailable.
	Bound *RootRoundBound
}

// ShardOutput is the consolidated result of one shard.
type ShardOutput struct {
	ShardID types.ShardID
	// UC is the ordinary UC assembled under the master seal.
	UC *types.UnicityCertificate
	// Proofs are replacement proofs, in request order. They keep the original
	// certification data, reference time and certificate bytes.
	Proofs []*api.GetInclusionProofResponseV2
	// SourceUC is the raw UC the source response carried (provenance only).
	SourceUC []byte
}

// Output is the consolidated proof bundle.
type Output struct {
	// MasterShard is the shard that supplied the view.
	MasterShard types.ShardID
	// RootRound is the authenticated root chain round of the master seal.
	RootRound uint64
	// Shards are in input order: master first, then peers.
	Shards []ShardOutput
}

// AuthenticatedMaster is the master view after it was authenticated: the
// common master UC/view binding, the trusted seal (one quorum check) and the
// round bound. It is the only way to obtain peer roots, so peers are never
// contacted for an unauthenticated master, and its verified material is reused
// for the final assembly. It can only be produced by Session.AuthenticateMaster.
type AuthenticatedMaster struct {
	session   *Session
	master    ShardResult
	bound     *RootRoundBound
	view      *types.PartitionViewV1
	enclosing *types.UnicityCertificate
	ucs       []*types.UnicityCertificate
	round     uint64
}

// Round is the authenticated root chain round of the master seal.
func (m *AuthenticatedMaster) Round() uint64 { return m.round }

// PeerRoot returns the target state root the authenticated view commits to for
// a shard (the exact root to request from that shard's aggregator).
func (m *AuthenticatedMaster) PeerRoot(shard types.ShardID) ([]byte, error) {
	idx := slices.IndexFunc(m.view.Leaves, func(l types.PartitionViewLeafV1) bool { return l.ShardID.Equal(shard) })
	if idx < 0 || m.view.Leaves[idx].InputRecord == nil {
		return nil, fail(OutcomeBindingMismatch, -1, nil, "shard %q is not in the master view", shard)
	}
	return slices.Clone(m.view.Leaves[idx].InputRecord.Hash), nil
}

// AuthenticateMaster authenticates the master's latest batch and partition view
// before anything is requested from other shards: bound issuance and coverage of
// the master's requests, view/UC binding, the trusted seal and R >= R_min.
func (s *Session) AuthenticateMaster(bound *RootRoundBound, master ShardResult, partitionView []byte) (*AuthenticatedMaster, error) {
	if bound == nil {
		return nil, fail(OutcomeUnavailable, -1, nil, "authenticated minimal root round is unknown")
	}
	if bound.session != s || !bytes.Equal(bound.lineage, s.policy.id()) {
		return nil, fail(OutcomeIncompatibleLineage, -1, nil, "root round bound was not issued by this verification session")
	}
	if len(partitionView) == 0 {
		return nil, fail(OutcomeUnavailable, -1, nil, "master response carries no partition view")
	}
	if err := s.checkBatches([]ShardResult{master}); err != nil {
		return nil, err
	}
	if err := bound.covers([]ShardResult{master}); err != nil {
		return nil, err
	}
	view, err := types.DecodePartitionViewV1(partitionView)
	if err != nil {
		return nil, fail(OutcomeInvalidInput, -1, err, "partition view")
	}
	enclosing, err := decodeCommonUC(master.Proofs, 0)
	if err != nil {
		return nil, err
	}
	if !enclosing.ShardTreeCertificate.Shard.Equal(master.ShardID) {
		return nil, fail(OutcomeBindingMismatch, 0, nil, "master UC is of shard %q, not %q", enclosing.ShardTreeCertificate.Shard, master.ShardID)
	}

	// 1 quorum check (memoized per complete seal)
	ucs, err := view.AssembleCertificates(enclosing, s.policy.PartitionID, s.policy.Topology, s, crypto.SHA256)
	if err != nil {
		var se *sealError
		if errors.As(err, &se) {
			return nil, fail(OutcomeUnauthenticated, -1, err, "master seal")
		}
		return nil, fail(OutcomeInvalidInput, -1, err, "master partition view")
	}
	if enclosing.UnicitySeal.NetworkID != bound.network {
		return nil, fail(OutcomeIncompatibleLineage, -1, nil, "master seal is of network %d, bound of network %d", enclosing.UnicitySeal.NetworkID, bound.network)
	}
	round := enclosing.UnicitySeal.RootChainRoundNumber
	if round < bound.round {
		return nil, fail(OutcomeStale, -1, nil, "master seal root round %d is below required %d", round, bound.round)
	}
	return &AuthenticatedMaster{session: s, master: master, bound: bound, view: view, enclosing: enclosing, ucs: ucs, round: round}, nil
}

// Complete verifies the peers' exact-root batches against the authenticated
// master and rebuilds every shard's proofs.
func (s *Session) Complete(m *AuthenticatedMaster, peers []ShardResult) (*Output, error) {
	if m == nil || m.session != s {
		return nil, fail(OutcomeInvalidInput, -1, nil, "master was not authenticated by this session")
	}
	batches := append([]ShardResult{m.master}, peers...)
	if err := s.checkBatches(batches); err != nil {
		return nil, err
	}
	if err := m.bound.covers(batches); err != nil {
		return nil, err
	}
	out := &Output{MasterShard: m.master.ShardID, RootRound: m.round}
	for bi, b := range batches {
		idx := slices.IndexFunc(m.view.Leaves, func(l types.PartitionViewLeafV1) bool { return l.ShardID.Equal(b.ShardID) })
		if idx < 0 {
			return nil, fail(OutcomeBindingMismatch, bi, nil, "shard %q is not in the master view", b.ShardID)
		}
		so, err := s.consolidateShard(bi, b, m.ucs[idx])
		if err != nil {
			return nil, err
		}
		out.Shards = append(out.Shards, *so)
	}
	return out, nil
}

// Consolidate authenticates the master view and rebuilds all proofs
// (AuthenticateMaster followed by Complete). Counters of the session report the
// verification work done.
func (s *Session) Consolidate(in *Input) (*Output, error) {
	if in == nil {
		return nil, fail(OutcomeInvalidInput, -1, nil, "nil input")
	}
	m, err := s.AuthenticateMaster(in.Bound, in.Master, in.PartitionView)
	if err != nil {
		return nil, err
	}
	return s.Complete(m, in.Peers)
}

// checkBatches validates the shape of the inputs.
func (s *Session) checkBatches(batches []ShardResult) error {
	shards := map[string]bool{}
	states := map[string]bool{}
	for bi, b := range batches {
		if shards[b.ShardID.Key()] {
			return fail(OutcomeInvalidInput, bi, nil, "duplicate shard %q", b.ShardID)
		}
		shards[b.ShardID.Key()] = true
		if len(b.Requests) == 0 || len(b.Requests) != len(b.Proofs) {
			return fail(OutcomeInvalidInput, bi, nil, "shard %q has %d requests and %d proofs", b.ShardID, len(b.Requests), len(b.Proofs))
		}
		for i, r := range b.Requests {
			if r == nil {
				return fail(OutcomeInvalidInput, bi, nil, "nil request %d", i)
			}
			if b.Proofs[i] == nil || b.Proofs[i].InclusionProof == nil {
				return fail(OutcomeInvalidInput, bi, nil, "nil proof %d", i)
			}
			k := string(r.StateID)
			if states[k] {
				return fail(OutcomeInvalidInput, bi, nil, "duplicate state ID %s", r.StateID)
			}
			states[k] = true
		}
	}
	return nil
}

// decodeCommonUC requires every proof to carry byte-identical canonical UC bytes.
func decodeCommonUC(proofs []*api.GetInclusionProofResponseV2, bi int) (*types.UnicityCertificate, error) {
	raw := proofs[0].InclusionProof.UnicityCertificate
	for i, p := range proofs {
		if !bytes.Equal(raw, p.InclusionProof.UnicityCertificate) {
			return nil, fail(OutcomeInvalidInput, bi, nil, "proof %d carries a different unicity certificate", i)
		}
	}
	var uc types.UnicityCertificate
	if err := types.Cbor.Unmarshal(raw, &uc); err != nil {
		return nil, fail(OutcomeInvalidInput, bi, err, "decoding unicity certificate")
	}
	if re, err := uc.MarshalCBOR(); err != nil || !bytes.Equal(re, raw) {
		return nil, fail(OutcomeUnsupported, bi, err, "unicity certificate is not in the canonical encoding")
	}
	if uc.UnicitySeal == nil || uc.InputRecord == nil {
		return nil, fail(OutcomeInvalidInput, bi, nil, "unicity certificate lacks seal or input record")
	}
	return &uc, nil
}

func (s *Session) consolidateShard(bi int, b ShardResult, target *types.UnicityCertificate) (*ShardOutput, error) {
	local, err := decodeCommonUC(b.Proofs, bi)
	if err != nil {
		return nil, err
	}
	// exact root: the source proofs are rooted at the state the master view commits to
	if !bytes.Equal(local.InputRecord.Hash, target.InputRecord.Hash) {
		return nil, fail(OutcomeBindingMismatch, bi, nil, "source root %X differs from the master view root %X", local.InputRecord.Hash, target.InputRecord.Hash)
	}
	if !local.ShardTreeCertificate.Shard.Equal(b.ShardID) || local.UnicityTreeCertificate == nil ||
		local.UnicityTreeCertificate.Partition != s.policy.PartitionID {
		return nil, fail(OutcomeBindingMismatch, bi, nil, "source unicity certificate identity differs from shard %q", b.ShardID)
	}
	targetBytes, err := target.MarshalCBOR()
	if err != nil {
		return nil, fail(OutcomeInvalidInput, bi, err, "encoding assembled unicity certificate")
	}
	if err := s.checkBody(bi, b.ShardID, target, targetBytes); err != nil {
		return nil, err
	}

	so := &ShardOutput{ShardID: b.ShardID, UC: target, SourceUC: slices.Clone(b.Proofs[0].InclusionProof.UnicityCertificate)}
	for i, req := range b.Requests {
		p := b.Proofs[i]
		if err := s.checkLeaf(bi, req, p.InclusionProof, target); err != nil {
			return nil, err
		}
		np := *p.InclusionProof
		np.CertificationData = np.CertificationData.Clone()
		if np.ReferenceTime != nil {
			rt := *np.ReferenceTime
			np.ReferenceTime = &rt
		}
		np.CertificateBytes = slices.Clone(np.CertificateBytes)
		np.UnicityCertificate = slices.Clone(targetBytes)
		so.Proofs = append(so.Proofs, &api.GetInclusionProofResponseV2{BlockNumber: p.BlockNumber, InclusionProof: &np})
	}
	return so, nil
}

// checkBody is the shard-body check of an assembled UC: structure, shard and
// partition paths up to the seal hash. The seal itself is authenticated once
// by the master check. Memoized by the complete UC bytes within the session.
func (s *Session) checkBody(bi int, shard types.ShardID, uc *types.UnicityCertificate, ucBytes []byte) error {
	key := string(appendLP(nil, ucBytes))
	if s.seen(s.bodies, key) {
		return nil
	}
	s.count(func(c *Counters) { c.BodyChecks++ })
	if err := checkPathSteps(bi, uc); err != nil {
		return err
	}
	if err := uc.IsValid(s.policy.PartitionID, shard, s.policy.Topology.ShardConfHashes[shard.Key()]); err != nil {
		return fail(OutcomeInvalidInput, bi, err, "assembled unicity certificate")
	}
	shardRoot, err := uc.ShardTreeCertificate.ComputeCertificateHash(uc.InputRecord, uc.TRHash, uc.ShardConfHash, crypto.SHA256)
	if err != nil {
		return fail(OutcomeInvalidInput, bi, err, "shard tree certificate")
	}
	root, err := uc.UnicityTreeCertificate.EvalAuthPath(shardRoot, crypto.SHA256)
	if err != nil {
		return fail(OutcomeInvalidInput, bi, err, "unicity tree certificate")
	}
	if !bytes.Equal(root, uc.UnicitySeal.Hash) {
		return fail(OutcomeBindingMismatch, bi, nil, "shard path does not reach the seal hash")
	}
	s.remember(s.bodies, key)
	return nil
}

// checkLeaf verifies one input against the replacement UC: request binding,
// expiry, original reference time not after the target timestamp, shard
// membership and the path to the target input record hash.
func (s *Session) checkLeaf(bi int, req *api.CertificationRequest, p *api.InclusionProofV2, target *types.UnicityCertificate) error {
	s.count(func(c *Counters) { c.LeafChecks++ })
	return leafRules(bi, req, p, target)
}

// leafRules are the complete per-input rules, shared by output verification
// and original-evidence authentication (which counts separately).
func leafRules(bi int, req *api.CertificationRequest, p *api.InclusionProofV2, target *types.UnicityCertificate) error {
	if err := p.CheckRequestBinding(req); err != nil {
		return fail(OutcomeBindingMismatch, bi, err, "state %s", req.StateID)
	}
	if p.ReferenceTime == nil {
		return fail(OutcomeInvalidInput, bi, nil, "state %s: missing reference time", req.StateID)
	}
	if exp := req.CertificationData.ExpiresAt; exp != nil && *p.ReferenceTime >= *exp {
		return fail(OutcomeExpired, bi, nil, "state %s: reference time %d, expiry %d", req.StateID, *p.ReferenceTime, *exp)
	}
	if *p.ReferenceTime > target.InputRecord.Timestamp {
		return fail(OutcomeReferenceTime, bi, nil, "state %s: reference time %d is after the target timestamp %d",
			req.StateID, *p.ReferenceTime, target.InputRecord.Timestamp)
	}
	key, err := req.StateID.GetTreeKey()
	if err != nil {
		return fail(OutcomeInvalidInput, bi, err, "state %s", req.StateID)
	}
	if shard := target.ShardTreeCertificate.Shard; shard.Length() > uint(len(key)*8) || !shard.Comparator()(key) {
		return fail(OutcomeBindingMismatch, bi, nil, "state %s does not belong to shard %q", req.StateID, shard)
	}
	if len(target.InputRecord.Hash) != api.StateTreeKeyLengthBytes {
		return fail(OutcomeUnsupported, bi, nil, "target state root has %d bytes", len(target.InputRecord.Hash))
	}
	if err := p.VerifyLeafPath(req, target.InputRecord.Hash); err != nil {
		return fail(OutcomeBindingMismatch, bi, err, "state %s", req.StateID)
	}
	return nil
}

// checkPathSteps rejects null entries in the partition path before it is
// evaluated: the native evaluation dereferences every step, and the UC checks
// do not validate them.
func checkPathSteps(idx int, uc *types.UnicityCertificate) error {
	if uc.UnicityTreeCertificate == nil {
		return fail(OutcomeInvalidInput, idx, nil, "unicity certificate lacks the unicity tree certificate")
	}
	for i, step := range uc.UnicityTreeCertificate.HashSteps {
		if step == nil {
			return fail(OutcomeInvalidInput, idx, nil, "unicity tree certificate hash step %d is null", i)
		}
	}
	return nil
}
