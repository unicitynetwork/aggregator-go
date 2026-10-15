package consolidation

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/unicitynetwork/bft-go-base/types"
)

// TrustBaseResolver resolves the trusted authority (root trust base of the
// right epoch) for a seal. It is the existing trusted epoch resolver; it must
// not derive trust from the seal itself.
type TrustBaseResolver func(seal *types.UnicitySeal) (types.RootTrustBase, error)

// Counters are the verification instrumentation of one session.
type Counters struct {
	// QuorumChecks is the number of native seal (quorum signature) verifications.
	QuorumChecks int
	// BodyChecks is the number of distinct shard-body verifications.
	BodyChecks int
	// LeafChecks is the number of leaf (per input) verifications.
	LeafChecks int
	// EvidenceQuorumChecks is the number of native seal verifications done to
	// authenticate the original evidence for R_min. They are reported
	// separately from the output-bundle QuorumChecks.
	EvidenceQuorumChecks int

	// Hardware-independent work metrics (asymptotic regression tests count these):

	// TopologyValidations is the number of policy topology validations (canonical
	// ordering, completeness, digest). One per Session, whatever the input size.
	TopologyValidations int
	// UCDecodes is the number of unicity certificate decodes of original evidence and
	// source responses; evidence UCs are decoded once per distinct complete byte string.
	UCDecodes int
	// EvidenceBodyChecks is the number of distinct evidence UC body verifications
	// (structure, shard and partition path to the seal hash).
	EvidenceBodyChecks int
	// ShardLeafProbes is the number of authenticated view leaves examined by shard
	// lookups: one hash probe per lookup with the shard index.
	ShardLeafProbes int
	// UCDerivations is the number of shard certificates derived from the authenticated
	// view: only the m participating shards, lazily.
	UCDerivations int
}

// Session is the operation scope of one consolidation. It memoizes successful
// complete-seal and shard-body checks; it must not outlive the operation and
// is not shared between policies.
type Session struct {
	policy   Policy
	resolver TrustBaseResolver
	epochEnd EpochEndResolver

	// computed once at construction from the defensive policy snapshot
	policyID     []byte
	pinnedShards map[string]struct{} // canonical membership index of a pinned topology

	mu       sync.Mutex
	seals    map[string]struct{}
	bodies   map[string]struct{}
	evUCs    map[string]*types.UnicityCertificate // verified evidence UCs by complete canonical bytes
	counters Counters
}

// EpochEndResolver returns the start round of the epoch following the seal's epoch if the
// resolver's authenticated history already knows it (known=false otherwise). A seal at or
// after that round is outside the interval its epoch's authority may certify in.
type EpochEndResolver func(seal *types.UnicitySeal) (nextEpochStart uint64, known bool, err error)

// SessionOption configures a Session.
type SessionOption func(*Session)

// WithEpochEnd makes the session enforce the known end boundary of the seal's epoch on
// every authentication, also for seals it has authenticated before.
func WithEpochEnd(r EpochEndResolver) SessionOption { return func(s *Session) { s.epochEnd = r } }

// NewSession creates the operation session for the policy.
func NewSession(policy Policy, resolver TrustBaseResolver, opts ...SessionOption) (*Session, error) {
	if resolver == nil {
		return nil, fail(OutcomeInvalidInput, -1, nil, "trust base resolver is required")
	}
	policy = policy.clone() // later changes by the caller never affect the session
	var topoOps int
	order, err := policy.analyze(&topoOps)
	if err != nil {
		return nil, err
	}
	s := &Session{policy: policy, resolver: resolver, seals: map[string]struct{}{}, bodies: map[string]struct{}{},
		evUCs: map[string]*types.UnicityCertificate{}, policyID: policy.idFor(order)}
	s.counters.TopologyValidations = topoOps
	if policy.pinned() {
		s.pinnedShards = make(map[string]struct{}, len(order))
		for _, id := range order {
			s.pinnedShards[id.Key()] = struct{}{}
		}
	}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// Counters returns a snapshot of the instrumentation counters.
func (s *Session) Counters() Counters {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counters
}

func (s *Session) count(f func(c *Counters)) {
	s.mu.Lock()
	f(&s.counters)
	s.mu.Unlock()
}

func (s *Session) seen(cache map[string]struct{}, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := cache[key]
	return ok
}

func (s *Session) remember(cache map[string]struct{}, key string) {
	s.mu.Lock()
	cache[key] = struct{}{}
	s.mu.Unlock()
}

// VerifySeal implements types.SealVerifier. A successful verification is
// reused only for the identical complete seal bytes, authority and policy.
// Failures are never cached.
func (s *Session) VerifySeal(seal *types.UnicitySeal) error {
	return s.verifySeal(seal, func(c *Counters) { c.QuorumChecks++ })
}

func (s *Session) verifySeal(seal *types.UnicitySeal, count func(*Counters)) error {
	if seal == nil {
		return types.ErrUnicitySealIsNil
	}
	sealBytes, err := seal.MarshalCBOR()
	if err != nil {
		return fmt.Errorf("encoding unicity seal: %w", err)
	}
	tb, err := s.resolver(seal)
	if err != nil {
		return &sealError{fmt.Errorf("resolving trust base: %w", err)}
	}
	if tb == nil {
		return &sealError{types.ErrRootValidatorInfoMissing}
	}
	authority, err := authorityIdentity(tb)
	if err != nil {
		return &sealError{err}
	}
	if s.epochEnd != nil {
		// before the memo: history learned after a successful check still applies
		next, known, err := s.epochEnd(seal)
		if err != nil {
			return &sealError{fmt.Errorf("resolving epoch end: %w", err)}
		}
		if known && seal.RootChainRoundNumber >= next {
			return &sealError{fmt.Errorf("seal round %d is at or after the start of the next epoch (round %d)", seal.RootChainRoundNumber, next)}
		}
	}
	key := sealKey(sealBytes, authority, s.policyID)
	if s.seen(s.seals, key) {
		return nil
	}
	// the native check, never replaced by a permissive trust base
	s.count(count)
	if err := types.TrustBaseSealVerifier(tb).VerifySeal(seal); err != nil {
		return &sealError{err}
	}
	s.remember(s.seals, key)
	return nil
}

// sealError marks a failure of seal authentication.
type sealError struct{ err error }

func (e *sealError) Error() string { return e.err.Error() }
func (e *sealError) Unwrap() error { return e.err }

func appendLP(dst, b []byte) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(len(b)))
	return append(dst, b...)
}

func sealKey(seal, authority, policy []byte) string {
	var k []byte
	k = appendLP(k, seal)
	k = appendLP(k, authority)
	k = appendLP(k, policy)
	return string(k)
}

// authorityIdentity is the content digest of everything the trust base
// contributes to seal verification: network, epoch, epoch start, quorum and
// the complete root node set.
func authorityIdentity(tb types.RootTrustBase) ([]byte, error) {
	var b []byte
	b = binary.BigEndian.AppendUint64(b, uint64(tb.GetVersion()))
	b = binary.BigEndian.AppendUint64(b, uint64(tb.GetNetworkID()))
	b = binary.BigEndian.AppendUint64(b, tb.GetEpoch())
	b = binary.BigEndian.AppendUint64(b, tb.GetEpochStart())
	b = binary.BigEndian.AppendUint64(b, tb.GetQuorumThreshold())
	b = binary.BigEndian.AppendUint64(b, tb.GetMaxFaultyNodes())
	nodes := tb.GetRootNodes()
	b = binary.BigEndian.AppendUint64(b, uint64(len(nodes)))
	for _, n := range nodes {
		if n == nil {
			return nil, errors.New("trust base has nil root node")
		}
		nb, err := types.Cbor.Marshal(n)
		if err != nil {
			return nil, fmt.Errorf("encoding root node: %w", err)
		}
		b = appendLP(b, nb)
	}
	h := sha256.Sum256(b)
	return h[:], nil
}

// Policy is the trusted verification context. Everything in it comes from the
// verifier's own configuration, never from a response.
type Policy struct {
	// PartitionID of the consolidated partition.
	PartitionID types.PartitionID
	// Topology OPTIONALLY pins the admitted shard scheme and the configuration
	// hash of every shard. Without it the topology comes from the partition view
	// itself, authenticated by the seal (types.AuthenticateView): the root quorum
	// signature commits to the complete ordered vector and every configuration
	// hash. Pins are a stricter policy: they can only reject more.
	Topology types.PartitionViewTopology
	// ID identifies the policy content for memoization; derived from the
	// partition and topology when empty.
	ID []byte
}

// pinned reports whether the policy carries an independently admitted topology.
func (p Policy) pinned() bool {
	return len(p.Topology.Scheme) > 0 || len(p.Topology.ShardConfHashes) > 0
}

// clone is a deep copy.
func (p Policy) clone() Policy {
	c := p
	c.ID = append([]byte(nil), p.ID...)
	c.Topology.Scheme = slices.Clone(p.Topology.Scheme)
	if p.Topology.ShardConfHashes != nil {
		c.Topology.ShardConfHashes = make(map[string][]byte, len(p.Topology.ShardConfHashes))
		for k, v := range p.Topology.ShardConfHashes {
			c.Topology.ShardConfHashes[k] = bytes.Clone(v)
		}
	}
	return c
}

// analyze validates the policy and returns the canonical shard order of a pinned
// topology; it is the only place the topology is validated and sorted (once per Session).
func (p Policy) analyze(ops *int) ([]types.ShardID, error) {
	if p.PartitionID == 0 {
		return nil, fail(OutcomeInvalidInput, -1, nil, "policy partition ID is zero")
	}
	if !p.pinned() {
		return nil, nil
	}
	*ops++
	order, err := types.CanonicalShardOrder(p.Topology.Scheme)
	if err != nil {
		return nil, fail(OutcomeInvalidInput, -1, err, "policy topology")
	}
	for _, id := range order {
		if h := p.Topology.ShardConfHashes[id.Key()]; len(h) != sha256.Size {
			return nil, fail(OutcomeInvalidInput, -1, nil, "policy lacks configuration hash of shard %q", id)
		}
	}
	return order, nil
}

// idFor returns the policy identity bytes: explicit ID, partition and the canonical topology.
func (p Policy) idFor(order []types.ShardID) []byte {
	var b []byte
	b = appendLP(b, p.ID)
	b = binary.BigEndian.AppendUint64(b, uint64(p.PartitionID))
	if !p.pinned() {
		b = append(b, 0) // authenticated topology: no pins
		h := sha256.Sum256(b)
		return h[:]
	}
	b = append(b, 1)
	for _, id := range order {
		b = appendLP(b, []byte(id.String()))
		b = appendLP(b, p.Topology.ShardConfHashes[id.Key()])
	}
	h := sha256.Sum256(b)
	return h[:]
}

var _ types.SealVerifier = (*Session)(nil)
