// Package noninclusion is the complete verifier of get_non_inclusion_proof.v1: it
// authenticates the unicity certificate of a proof against a trusted authority and then
// checks the absence relation of its certificate.
//
// Non-inclusion proves absence only at the authenticated snapshot returned by verification. It does not prove absence now, continuous absence, or that a token can be spent. Compare the snapshot’s network and root round (or its certified seal timestamp under your application’s time policy) with an independently trusted requirement. The seal timestamp is a consensus-certified time, not proof of real-time freshness. Do not use unverified response metadata for this check.
//
// Verification returns a discriminated Result: absent_at_snapshot, or included_at_snapshot (a valid path proves
// the exact key present), each with the authenticated Snapshot, or a *Error carrying one rejection class and no
// snapshot. The verifier has no freshness setting; freshness is the caller's decision after verification:
//
//	res, err := v.Verify(stateID, proof)
//	if err != nil { /* rejected: malformed, UC, authority or certificate failure; no snapshot */ }
//	if res.Outcome() != noninclusion.OutcomeAbsentAtSnapshot { /* the state is included at the snapshot */ }
//	snap := res.Snapshot()
//	if snap.NetworkID() != checkpoint.NetworkID || snap.RootRound() < checkpoint.RequiredRootRound {
//		/* the snapshot does not meet the application policy (checkpoint obtained independently of the proof) */
//	}
//	// absence at snap only; any spend or finality decision needs its own policy
//
// The partition is the fixed api.AggregationPartitionID; a proof never selects its authority or partition.
// Everything the result depends on comes from the Context (the verifier's own configuration), never from the
// response. Verification order and outcome semantics are those of testdata/non-inclusion/v2/SPEC.md.
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

// Rejection classes (shared with the corpus). A rejected proof carries no snapshot.
const (
	RejectionMalformed   = "malformed"
	RejectionUCInvalid   = "uc_invalid"
	RejectionAuthority   = "authority_rejected"
	RejectionCertInvalid = "certificate_invalid"

	maxUCBytes             = 64 << 10
	maxCertificateRootSize = api.SiblingSize
)

// Outcome is the successful result of a verification.
type Outcome string

const (
	// OutcomeAbsentAtSnapshot: the state is absent at the authenticated snapshot.
	OutcomeAbsentAtSnapshot Outcome = "absent_at_snapshot"
	// OutcomeIncludedAtSnapshot: a valid path proves the exact key present at the authenticated snapshot.
	OutcomeIncludedAtSnapshot Outcome = "included_at_snapshot"
)

// Error is a rejected proof with its rejection class. It is the only failure of a proof; transport and usage errors
// are other error types.
type Error struct {
	Rejection string
	Err       error
}

func (e *Error) Error() string { return e.Rejection + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// RejectionOf returns the rejection class of err ("" if err is nil or not a rejection).
func RejectionOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Rejection
	}
	return ""
}

// ErrEmptyRootEncoding: the input record hash is the empty byte string. The certified empty tree is the
// native null hash only (the corpus SPEC); an empty byte string is a different encoding and is not it.
var ErrEmptyRootEncoding = errors.New("the input record hash is an empty byte string, not the native null of the empty tree")

func fail(rejection string, err error) error { return &Error{Rejection: rejection, Err: err} }

// Context is the trusted verification context. The partition is not part of it: it is the fixed
// api.AggregationPartitionID.
type Context struct {
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
}

// Snapshot is the authenticated certified state a successful verification returns. Every value comes from the
// authenticated unicity certificate (seal and input record), never from RPC block metadata or receipt time. It is
// an immutable value: byte fields are copied on the way in and out.
type Snapshot struct {
	networkID     types.NetworkID
	partitionID   types.PartitionID
	shardID       types.ShardID
	rootRound     uint64
	sealTimestamp uint64
	rootEpoch     uint64
	shardRound    uint64
	shardEpoch    uint64
	stateRoot     []byte
}

// NetworkID is the network of the authenticated seal.
func (s Snapshot) NetworkID() types.NetworkID { return s.networkID }

// PartitionID is the partition of the certificate (always api.AggregationPartitionID).
func (s Snapshot) PartitionID() types.PartitionID { return s.partitionID }

// ShardID is the shard of the certificate (the empty shard ID for the unsharded partition).
func (s Snapshot) ShardID() types.ShardID { return s.shardID }

// RootRound is the certified root chain round of the snapshot.
func (s Snapshot) RootRound() uint64 { return s.rootRound }

// SealTimestampSeconds is the consensus-certified timestamp of the seal in Unix seconds. It is not proof of
// real-time freshness.
func (s Snapshot) SealTimestampSeconds() uint64 { return s.sealTimestamp }

// RootEpoch is the root chain epoch of the seal.
func (s Snapshot) RootEpoch() uint64 { return s.rootEpoch }

// ShardRound is the round of the shard's certified input record.
func (s Snapshot) ShardRound() uint64 { return s.shardRound }

// ShardEpoch is the epoch of the shard's certified input record.
func (s Snapshot) ShardEpoch() uint64 { return s.shardEpoch }

// StateRoot returns a copy of the certified state root; nil for the certified empty tree.
func (s Snapshot) StateRoot() []byte { return bytes.Clone(s.stateRoot) }

// EmptyTree reports whether the certified state is the empty tree.
func (s Snapshot) EmptyTree() bool { return len(s.stateRoot) == 0 }

// Result is a successful verification: the outcome for the queried state and the authenticated snapshot.
//
// Non-inclusion proves absence only at the authenticated snapshot returned by verification. It does not prove absence now, continuous absence, or that a token can be spent. Compare the snapshot’s network and root round (or its certified seal timestamp under your application’s time policy) with an independently trusted requirement. The seal timestamp is a consensus-certified time, not proof of real-time freshness. Do not use unverified response metadata for this check.
//
// A Result of OutcomeIncludedAtSnapshot is not an absence: check Outcome before the snapshot policy.
type Result struct {
	outcome  Outcome
	stateID  api.StateID
	snapshot Snapshot
}

// Outcome is absent_at_snapshot or included_at_snapshot.
func (r *Result) Outcome() Outcome { return r.outcome }

// Absent reports whether the outcome is OutcomeAbsentAtSnapshot.
func (r *Result) Absent() bool { return r.outcome == OutcomeAbsentAtSnapshot }

// StateID returns a copy of the queried state ID.
func (r *Result) StateID() api.StateID { return bytes.Clone(r.stateID) }

// Snapshot returns the authenticated snapshot.
func (r *Result) Snapshot() Snapshot { return r.snapshot }

// Verifier verifies proofs under one Context. It is operation scoped: it memoizes the
// seals it authenticated (keyed by the complete seal, authority and policy) and must
// not outlive the operation.
type Verifier struct {
	ctx     Context
	session *consolidation.Session
}

// NewVerifier validates the context.
func NewVerifier(ctx Context) (*Verifier, error) {
	if ctx.Resolver == nil {
		return nil, errors.New("noninclusion: a trust base resolver is required")
	}
	var opts []consolidation.SessionOption
	if ctx.EpochEnd != nil {
		opts = append(opts, consolidation.WithEpochEnd(ctx.EpochEnd))
	}
	s, err := consolidation.NewSession(consolidation.Policy{PartitionID: api.AggregationPartitionID}, ctx.Resolver, opts...)
	if err != nil {
		return nil, err
	}
	ctx.ShardConfHash = bytes.Clone(ctx.ShardConfHash)
	return &Verifier{ctx: ctx, session: s}, nil
}

// Verify verifies the proof for the state. It authenticates the shape, the network, authority and epoch interval,
// the fixed partition and shard commitments, the query binding and the complete state-tree relation before it
// exposes any snapshot, and returns either the Result (absent_at_snapshot or included_at_snapshot, with the
// authenticated Snapshot) or an *Error with one rejection class and no snapshot.
//
// Non-inclusion proves absence only at the authenticated snapshot returned by verification. It does not prove absence now, continuous absence, or that a token can be spent. Compare the snapshot’s network and root round (or its certified seal timestamp under your application’s time policy) with an independently trusted requirement. The seal timestamp is a consensus-certified time, not proof of real-time freshness. Do not use unverified response metadata for this check.
func (v *Verifier) Verify(stateID api.StateID, proof *api.NonInclusionProofV1) (*Result, error) {
	key, err := stateID.GetTreeKey()
	if err != nil {
		return nil, fail(RejectionCertInvalid, err)
	}
	if proof == nil || proof.Certificate == nil {
		return nil, fail(RejectionMalformed, errors.New("incomplete proof"))
	}
	if req := proof.RequestedStateID(); len(req) != 0 && !bytes.Equal(req, stateID) {
		return nil, fail(RejectionCertInvalid, errors.New("the proof is bound to another state ID"))
	}
	uc, err := v.decodeUC(proof.UnicityCertificate)
	if err != nil {
		return nil, fail(RejectionMalformed, err)
	}
	if err := v.checkBody(uc); err != nil {
		return nil, fail(RejectionUCInvalid, err)
	}
	if err := v.session.VerifySeal(uc.UnicitySeal); err != nil {
		return nil, fail(RejectionAuthority, err)
	}
	root := uc.InputRecord.Hash
	if n := len(root); n != 0 && n != maxCertificateRootSize {
		return nil, fail(RejectionCertInvalid, fmt.Errorf("state root of %d bytes", n))
	}
	if shard := v.ctx.Shard; shard.Length() > 0 {
		// the query (and the authenticated terminal) must belong to the certified shard
		if !shard.Comparator()(key) {
			return nil, fail(RejectionCertInvalid, errors.New("the state does not belong to the shard"))
		}
		if tk := proof.Certificate.TerminalKey(); tk != nil && !shard.Comparator()(tk) {
			return nil, fail(RejectionCertInvalid, errors.New("the terminal leaf does not belong to the shard"))
		}
	}
	switch err := proof.Certificate.VerifyAgainstTrustedRoot(key, root, api.SHA256); {
	case err == nil:
		return v.result(OutcomeAbsentAtSnapshot, stateID, uc), nil
	case errors.Is(err, api.ErrStateIncluded):
		return v.result(OutcomeIncludedAtSnapshot, stateID, uc), nil
	case errors.Is(err, api.ErrNonInclusionMalformed):
		return nil, fail(RejectionMalformed, err)
	default:
		return nil, fail(RejectionCertInvalid, err)
	}
}

// result builds the Result from the authenticated UC; nothing aliases the decoded certificate.
func (v *Verifier) result(outcome Outcome, stateID api.StateID, uc *types.UnicityCertificate) *Result {
	return &Result{
		outcome: outcome,
		stateID: bytes.Clone(stateID),
		snapshot: Snapshot{
			networkID:     uc.UnicitySeal.NetworkID,
			partitionID:   uc.UnicityTreeCertificate.Partition,
			shardID:       uc.ShardTreeCertificate.Shard,
			rootRound:     uc.UnicitySeal.RootChainRoundNumber,
			sealTimestamp: uc.UnicitySeal.Timestamp,
			rootEpoch:     uc.UnicitySeal.Epoch,
			shardRound:    uc.InputRecord.RoundNumber,
			shardEpoch:    uc.InputRecord.Epoch,
			stateRoot:     bytes.Clone(uc.InputRecord.Hash),
		},
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
	if err := uc.IsValid(api.AggregationPartitionID, v.ctx.Shard, v.ctx.ShardConfHash); err != nil {
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
