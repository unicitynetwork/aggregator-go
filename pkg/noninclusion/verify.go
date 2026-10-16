// Package noninclusion is the complete verifier of get_non_inclusion_proof.v1: it
// authenticates the unicity certificate of a proof against a trusted authority and then
// checks the absence relation of its certificate. A proof proves absence of the state
// at ONE certified snapshot, never current absence; freshness is the caller's policy
// (MinRootRound).
//
// Everything the verdict depends on comes from the Context (the verifier's own
// configuration), never from the response. Verification order and verdict semantics are
// those of testdata/non-inclusion/v1/SPEC.md.
package noninclusion

import (
	"bytes"
	"crypto"
	"errors"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation"
)

// Verdicts (shared with the corpus).
const (
	VerdictMalformed       = "malformed"
	VerdictUCInvalid       = "uc_invalid"
	VerdictAuthority       = "authority_rejected"
	VerdictStale           = "stale"
	VerdictCertInvalid     = "certificate_invalid"
	VerdictStateIncluded   = "state_included"
	verdictAbsent          = "absent"
	maxUCBytes             = 64 << 10
	maxCertificateRootSize = api.SiblingSize
)

// Error is a verification failure with its verdict.
type Error struct {
	Verdict string
	Err     error
}

func (e *Error) Error() string { return e.Verdict + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// VerdictOf returns the verdict of err ("absent" for nil, "" for other errors).
func VerdictOf(err error) string {
	if err == nil {
		return verdictAbsent
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Verdict
	}
	return ""
}

// ErrEmptyRootEncoding: the input record hash is the empty byte string. The certified empty tree is the
// native null hash only (the corpus SPEC); an empty byte string is a different encoding and is not it.
var ErrEmptyRootEncoding = errors.New("the input record hash is an empty byte string, not the native null of the empty tree")

func fail(verdict string, err error) error { return &Error{Verdict: verdict, Err: err} }

// Context is the trusted verification context.
type Context struct {
	// Partition is the expected application partition (required).
	Partition types.PartitionID
	// Shard is the expected shard of the partition: the empty shard ID is the unsharded
	// form (no shard path); a nonempty one requires its shard path and that the query and
	// terminal keys belong to the shard.
	Shard types.ShardID
	// ShardConfHash optionally pins the shard configuration hash committed in the UC.
	ShardConfHash []byte
	// Resolver resolves the trusted authority of a seal (exact network/epoch/start are
	// enforced against it). Required.
	Resolver consolidation.TrustBaseResolver
	// EpochEnd optionally provides the authenticated end boundary of the seal's epoch.
	EpochEnd consolidation.EpochEndResolver
	// MinRootRound, if not zero, is the minimum certified root chain round the caller
	// requires. An older snapshot is authenticated but stale.
	MinRootRound uint64
}

// Result describes a verified absence.
type Result struct {
	// UC is the authenticated unicity certificate.
	UC *types.UnicityCertificate
	// RootRound is the certified root chain round of the snapshot.
	RootRound uint64
	// EmptyTree is true when the certified state is the empty tree.
	EmptyTree bool
}

// Verifier verifies proofs under one Context. It is operation scoped: it memoizes the
// seals it authenticated (keyed by the complete seal, authority and policy) and must
// not outlive the operation.
type Verifier struct {
	ctx     Context
	session *consolidation.Session
}

// NewVerifier validates the context.
func NewVerifier(ctx Context) (*Verifier, error) {
	if ctx.Partition == 0 {
		return nil, errors.New("noninclusion: partition is required")
	}
	if ctx.Resolver == nil {
		return nil, errors.New("noninclusion: a trust base resolver is required")
	}
	var opts []consolidation.SessionOption
	if ctx.EpochEnd != nil {
		opts = append(opts, consolidation.WithEpochEnd(ctx.EpochEnd))
	}
	s, err := consolidation.NewSession(consolidation.Policy{PartitionID: ctx.Partition}, ctx.Resolver, opts...)
	if err != nil {
		return nil, err
	}
	ctx.ShardConfHash = bytes.Clone(ctx.ShardConfHash)
	return &Verifier{ctx: ctx, session: s}, nil
}

// VerifyFor verifies the proof for the state: it returns the authenticated result when the
// state is absent at the proof's certified root, or an *Error carrying the verdict.
func (v *Verifier) VerifyFor(stateID api.StateID, proof *api.NonInclusionProofV1) (*Result, error) {
	key, err := stateID.GetTreeKey()
	if err != nil {
		return nil, fail(VerdictCertInvalid, err)
	}
	if proof == nil || proof.Certificate == nil {
		return nil, fail(VerdictMalformed, errors.New("incomplete proof"))
	}
	if req := proof.RequestedStateID(); len(req) != 0 && !bytes.Equal(req, stateID) {
		return nil, fail(VerdictCertInvalid, errors.New("the proof is bound to another state ID"))
	}
	uc, err := v.decodeUC(proof.UnicityCertificate)
	if err != nil {
		return nil, fail(VerdictMalformed, err)
	}
	if err := v.checkBody(uc); err != nil {
		return nil, fail(VerdictUCInvalid, err)
	}
	if err := v.session.VerifySeal(uc.UnicitySeal); err != nil {
		return nil, fail(VerdictAuthority, err)
	}
	round := uc.UnicitySeal.RootChainRoundNumber
	if v.ctx.MinRootRound != 0 && round < v.ctx.MinRootRound {
		return nil, fail(VerdictStale, fmt.Errorf("certified root round %d is below the required %d", round, v.ctx.MinRootRound))
	}
	root := uc.InputRecord.Hash
	if n := len(root); n != 0 && n != maxCertificateRootSize {
		return nil, fail(VerdictCertInvalid, fmt.Errorf("state root of %d bytes", n))
	}
	if shard := v.ctx.Shard; shard.Length() > 0 {
		// the query (and the authenticated terminal) must belong to the certified shard
		if !shard.Comparator()(key) {
			return nil, fail(VerdictCertInvalid, errors.New("the state does not belong to the shard"))
		}
		if tk := proof.Certificate.TerminalKey(); tk != nil && !shard.Comparator()(tk) {
			return nil, fail(VerdictCertInvalid, errors.New("the terminal leaf does not belong to the shard"))
		}
	}
	switch err := proof.Certificate.VerifyAgainstTrustedRoot(key, root, api.SHA256); {
	case err == nil:
		return &Result{UC: uc, RootRound: round, EmptyTree: len(root) == 0}, nil
	case errors.Is(err, api.ErrStateIncluded):
		return nil, fail(VerdictStateIncluded, err)
	case errors.Is(err, api.ErrNonInclusionMalformed):
		return nil, fail(VerdictMalformed, err)
	default:
		return nil, fail(VerdictCertInvalid, err)
	}
}

func (v *Verifier) decodeUC(raw []byte) (*types.UnicityCertificate, error) {
	if len(raw) == 0 || len(raw) > maxUCBytes {
		return nil, errors.New("unicity certificate missing or too large")
	}
	var uc types.UnicityCertificate
	if err := types.Cbor.Unmarshal(raw, &uc); err != nil {
		return nil, fmt.Errorf("decoding unicity certificate: %w", err)
	}
	if re, err := uc.MarshalCBOR(); err != nil || !bytes.Equal(re, raw) {
		return nil, errors.New("unicity certificate is not in the canonical encoding")
	}
	return &uc, nil
}

// checkBody checks everything about the UC except the seal's signatures: components present,
// no null path entries, structure for the expected partition and shard, configuration pin, and
// the shard and partition paths up to the seal hash.
func (v *Verifier) checkBody(uc *types.UnicityCertificate) error {
	if uc.InputRecord == nil || uc.UnicitySeal == nil || uc.UnicityTreeCertificate == nil {
		return errors.New("unicity certificate lacks a component")
	}
	if uc.InputRecord.Hash != nil && len(uc.InputRecord.Hash) == 0 {
		return ErrEmptyRootEncoding
	}
	for i, s := range uc.UnicityTreeCertificate.HashSteps {
		if s == nil {
			return fmt.Errorf("unicity tree certificate hash step %d is null", i)
		}
	}
	if err := uc.IsValid(v.ctx.Partition, v.ctx.Shard, v.ctx.ShardConfHash); err != nil {
		return err
	}
	if !uc.ShardTreeCertificate.Shard.Equal(v.ctx.Shard) {
		return errors.New("unicity certificate is of another shard")
	}
	if v.ctx.Shard.Length() == 0 && len(uc.ShardTreeCertificate.SiblingHashes) != 0 {
		return errors.New("the empty shard ID carries a shard path")
	}
	shardRoot, err := uc.ShardTreeCertificate.ComputeCertificateHash(uc.InputRecord, uc.TRHash, uc.ShardConfHash, crypto.SHA256)
	if err != nil {
		return err
	}
	root, err := uc.UnicityTreeCertificate.EvalAuthPath(shardRoot, crypto.SHA256)
	if err != nil {
		return err
	}
	if !bytes.Equal(root, uc.UnicitySeal.Hash) {
		return errors.New("the shard path does not reach the seal hash")
	}
	return nil
}
