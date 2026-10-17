package consolidation

import (
	"bytes"
	"crypto"
	"crypto/sha256"
	"errors"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Evidence is one original input with the proof it was certified by. Proofs of
// different shards typically carry different (mixed) seals.
type Evidence struct {
	Request *api.CertificationRequest
	Proof   *api.InclusionProofV2
}

// RootRoundBound is the authenticated minimal root chain round (R_min). It can
// only be produced by Session.AuthenticateEvidence and is only accepted by that
// same session: it is bound to the verifying session (its authority resolver and
// policy), the network, and a commitment to the original inputs it was derived
// from. Consolidate requires every request of the consolidation to be covered by
// that commitment, so evidence that is omitted or substituted never yields a
// usable bound. Its fields are unexported so it cannot be forged or lowered.
type RootRoundBound struct {
	round   uint64
	shard   types.ShardID
	network types.NetworkID
	lineage []byte
	session *Session
	// inputs commits to every authenticated original input: state ID -> digest
	// of its certification data.
	inputs map[string]evidenceInput
}

// evidenceInput is what an authenticated original commits the consolidation to.
type evidenceInput struct {
	digest [sha256.Size]byte
	// shard is the shard the original was certified in. Consolidation never
	// reinterprets a state under another shard: topology migrations are unsupported.
	shard types.ShardID
}

// Round returns the authenticated minimal root chain round.
func (b *RootRoundBound) Round() uint64 { return b.round }

// Shard returns the shard of the (first) evidence that carries the maximum
// round: the preferred master.
func (b *RootRoundBound) Shard() types.ShardID { return b.shard }

func inputDigest(req *api.CertificationRequest) ([sha256.Size]byte, error) {
	b, err := types.Cbor.Marshal(&req.CertificationData)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(b), nil
}

// covers checks that every request of the consolidation is one of the
// authenticated required inputs, byte-for-byte in its certification data.
func (b *RootRoundBound) covers(batches []ShardResult) error {
	for bi, sr := range batches {
		for _, r := range sr.Requests {
			want, ok := b.inputs[string(r.StateID)]
			if !ok {
				return fail(OutcomeIncompatibleLineage, bi, nil, "state %s is not covered by the authenticated evidence", r.StateID)
			}
			got, err := inputDigest(r)
			if err != nil || got != want.digest {
				return fail(OutcomeIncompatibleLineage, bi, err, "state %s differs from the authenticated evidence", r.StateID)
			}
		}
	}
	return nil
}

// coversShard checks that every request of the batch was certified, in its original,
// in the very shard it is consolidated in: consolidation never reinterprets a state under
// another shard (topology migrations are unsupported).
func (b *RootRoundBound) coversShard(bi int, sr ShardResult) error {
	for _, r := range sr.Requests {
		if want, ok := b.inputs[string(r.StateID)]; ok && !want.shard.Equal(sr.ShardID) {
			return fail(OutcomeIncompatibleLineage, bi, nil, "state %s was certified in shard %q but is consolidated in shard %q (topology change)", r.StateID, want.shard, sr.ShardID)
		}
	}
	return nil
}

// AuthenticateEvidence authenticates every original proof (request binding,
// leaf path to its UC, shard and partition path to its seal hash, and the seal
// against the trusted authority) and returns the bound R_min: the largest seal
// round among them. All seals must be of one network. Native seal checks it
// runs are counted as EvidenceQuorumChecks.
func (s *Session) AuthenticateEvidence(ev []Evidence) (*RootRoundBound, error) {
	if len(ev) == 0 {
		return nil, fail(OutcomeUnavailable, -1, nil, "no original evidence")
	}
	var round uint64
	var network types.NetworkID
	var bestShard types.ShardID
	inputs := map[string]evidenceInput{}
	for i, e := range ev {
		if e.Request == nil || e.Proof == nil {
			return nil, fail(OutcomeInvalidInput, i, nil, "nil evidence")
		}
		uc, err := s.checkEvidenceProof(i, e)
		if err != nil {
			return nil, err
		}
		d, err := inputDigest(e.Request)
		if err != nil {
			return nil, fail(OutcomeInvalidInput, i, err, "encoding certification data")
		}
		if _, dup := inputs[string(e.Request.StateID)]; dup {
			return nil, fail(OutcomeInvalidInput, i, nil, "duplicate evidence for state %s", e.Request.StateID)
		}
		inputs[string(e.Request.StateID)] = evidenceInput{digest: d, shard: uc.ShardTreeCertificate.Shard}
		seal := uc.UnicitySeal
		if i == 0 {
			network = seal.NetworkID
		} else if seal.NetworkID != network {
			return nil, fail(OutcomeIncompatibleLineage, i, nil, "evidence seal is of network %d, earlier evidence of %d", seal.NetworkID, network)
		}
		if err := s.verifySeal(seal, func(c *Counters) { c.EvidenceQuorumChecks++ }); err != nil {
			return nil, fail(OutcomeUnauthenticated, i, err, "evidence seal")
		}
		if i == 0 || seal.RootChainRoundNumber > round {
			round, bestShard = seal.RootChainRoundNumber, uc.ShardTreeCertificate.Shard
		}
	}
	return &RootRoundBound{round: round, shard: bestShard, network: network, lineage: s.policyID, session: s, inputs: inputs}, nil
}

func (s *Session) checkEvidenceProof(i int, e Evidence) (*types.UnicityCertificate, error) {
	if e.Proof.CertificationData == nil {
		return nil, fail(OutcomeInvalidInput, i, nil, "evidence is not an inclusion proof")
	}
	uc, err := s.evidenceUC(i, e.Proof.UnicityCertificate)
	if err != nil {
		return nil, err
	}
	// per-input rules, never memoized: binding, expiry, reference time not after the UC
	// timestamp, certified-shard membership, path to the root
	if err := leafRules(i, e.Request, e.Proof, uc); err != nil {
		return nil, err
	}
	return uc, nil
}

// evidenceUC decodes and body-verifies an evidence UC (structure, shard and partition
// path up to its seal hash) once per distinct complete canonical byte string of the
// session (the policy is fixed per session; the seal itself is authenticated separately
// against the resolved authority). The returned certificate is shared and must not be
// modified. Failures are not cached.
func (s *Session) evidenceUC(i int, raw []byte) (*types.UnicityCertificate, error) {
	s.mu.Lock()
	uc, ok := s.evUCs[string(raw)]
	s.mu.Unlock()
	if ok {
		return uc, nil
	}
	s.count(func(c *Counters) { c.UCDecodes++ })
	uc, err := decodeUC(raw, i)
	if err != nil {
		return nil, err
	}
	if err := checkPathSteps(i, uc); err != nil {
		return nil, err
	}
	s.count(func(c *Counters) { c.EvidenceBodyChecks++ })
	shard := uc.ShardTreeCertificate.Shard
	// the configuration hash of an original may legitimately differ from the target's: it is
	// only checked against independent pins, otherwise the shard is bound at consolidation
	var pinnedConf []byte
	if s.policy.pinned() {
		if _, ok := s.pinnedShards[shard.Key()]; !ok {
			return nil, fail(OutcomeIncompatibleLineage, i, nil, "evidence shard %q is not in the policy topology", shard)
		}
		pinnedConf = s.policy.Topology.ShardConfHashes[shard.Key()]
	}
	if err := uc.IsValid(s.policy.PartitionID, shard, pinnedConf); err != nil {
		return nil, fail(OutcomeIncompatibleLineage, i, err, "evidence unicity certificate")
	}
	shardRoot, err := uc.ShardTreeCertificate.ComputeCertificateHash(uc.InputRecord, uc.TRHash, uc.ShardConfHash, crypto.SHA256)
	if err != nil {
		return nil, fail(OutcomeInvalidInput, i, err, "evidence shard tree certificate")
	}
	root, err := uc.UnicityTreeCertificate.EvalAuthPath(shardRoot, crypto.SHA256)
	if err != nil {
		return nil, fail(OutcomeInvalidInput, i, err, "evidence unicity tree certificate")
	}
	if !bytes.Equal(root, uc.UnicitySeal.Hash) {
		return nil, fail(OutcomeBindingMismatch, i, nil, "evidence shard path does not reach its seal hash")
	}
	s.mu.Lock()
	s.evUCs[string(raw)] = uc
	s.mu.Unlock()
	return uc, nil
}

func decodeUC(raw []byte, idx int) (*types.UnicityCertificate, error) {
	if len(raw) == 0 {
		return nil, fail(OutcomeInvalidInput, idx, errors.New("empty"), "unicity certificate")
	}
	var uc types.UnicityCertificate
	if err := types.Cbor.Unmarshal(raw, &uc); err != nil {
		return nil, fail(OutcomeInvalidInput, idx, err, "decoding unicity certificate")
	}
	if uc.UnicitySeal == nil || uc.InputRecord == nil || uc.UnicityTreeCertificate == nil {
		return nil, fail(OutcomeInvalidInput, idx, nil, "unicity certificate lacks seal, input record or tree certificate")
	}
	return &uc, nil
}
