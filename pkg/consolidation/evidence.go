package consolidation

import (
	"bytes"
	"crypto"
	"errors"
	"slices"

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
// only be produced by Session.AuthenticateEvidence: it records the network and
// the policy it was derived under, and Consolidate refuses it for any other
// lineage. Its fields are unexported so a bound cannot be forged or lowered.
type RootRoundBound struct {
	round   uint64
	network types.NetworkID
	lineage []byte
}

// Round returns the authenticated minimal root chain round.
func (b *RootRoundBound) Round() uint64 { return b.round }

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
	for i, e := range ev {
		if e.Request == nil || e.Proof == nil {
			return nil, fail(OutcomeInvalidInput, i, nil, "nil evidence")
		}
		uc, err := s.checkEvidenceProof(i, e)
		if err != nil {
			return nil, err
		}
		seal := uc.UnicitySeal
		if i == 0 {
			network = seal.NetworkID
		} else if seal.NetworkID != network {
			return nil, fail(OutcomeIncompatibleLineage, i, nil, "evidence seal is of network %d, earlier evidence of %d", seal.NetworkID, network)
		}
		if err := s.verifySeal(seal, func(c *Counters) { c.EvidenceQuorumChecks++ }); err != nil {
			return nil, fail(OutcomeUnauthenticated, i, err, "evidence seal")
		}
		round = max(round, seal.RootChainRoundNumber)
	}
	return &RootRoundBound{round: round, network: network, lineage: s.policy.id()}, nil
}

func (s *Session) checkEvidenceProof(i int, e Evidence) (*types.UnicityCertificate, error) {
	if err := e.Proof.CheckRequestBinding(e.Request); err != nil {
		return nil, fail(OutcomeBindingMismatch, i, err, "evidence state %s", e.Request.StateID)
	}
	uc, err := decodeUC(e.Proof.UnicityCertificate, i)
	if err != nil {
		return nil, err
	}
	shard := uc.ShardTreeCertificate.Shard
	order, _ := types.CanonicalShardOrder(s.policy.Topology.Scheme)
	if !slices.ContainsFunc(order, shard.Equal) {
		return nil, fail(OutcomeIncompatibleLineage, i, nil, "evidence shard %q is not in the policy topology", shard)
	}
	if err := uc.IsValid(s.policy.PartitionID, shard, s.policy.Topology.ShardConfHashes[shard.Key()]); err != nil {
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
	if len(uc.InputRecord.Hash) != api.StateTreeKeyLengthBytes {
		return nil, fail(OutcomeUnsupported, i, nil, "evidence state root has %d bytes", len(uc.InputRecord.Hash))
	}
	if exp := e.Request.CertificationData.ExpiresAt; exp != nil && e.Proof.ReferenceTime != nil && *e.Proof.ReferenceTime >= *exp {
		return nil, fail(OutcomeExpired, i, nil, "evidence state %s expired at its reference time", e.Request.StateID)
	}
	if err := e.Proof.VerifyLeafPath(e.Request, uc.InputRecord.Hash); err != nil {
		return nil, fail(OutcomeBindingMismatch, i, err, "evidence state %s", e.Request.StateID)
	}
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
