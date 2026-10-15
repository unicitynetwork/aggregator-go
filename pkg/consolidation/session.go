package consolidation

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
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
}

// Session is the operation scope of one consolidation. It memoizes successful
// complete-seal and shard-body checks; it must not outlive the operation and
// is not shared between policies.
type Session struct {
	policy   Policy
	resolver TrustBaseResolver

	mu       sync.Mutex
	seals    map[string]struct{}
	bodies   map[string]struct{}
	counters Counters
}

// NewSession creates the operation session for the policy.
func NewSession(policy Policy, resolver TrustBaseResolver) (*Session, error) {
	if resolver == nil {
		return nil, fail(OutcomeInvalidInput, -1, nil, "trust base resolver is required")
	}
	if err := policy.validate(); err != nil {
		return nil, err
	}
	return &Session{policy: policy, resolver: resolver, seals: map[string]struct{}{}, bodies: map[string]struct{}{}}, nil
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
	key := sealKey(sealBytes, authority, s.policy.id())
	if s.seen(s.seals, key) {
		return nil
	}
	// the native check, never replaced by a permissive trust base
	s.count(func(c *Counters) { c.QuorumChecks++ })
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
	// Topology is the admitted shard scheme and shard configuration hashes of
	// every shard (required for UC assembly).
	Topology types.PartitionViewTopology
	// ID identifies the policy content for memoization; derived from the
	// partition and topology when empty.
	ID []byte
}

func (p Policy) validate() error {
	if p.PartitionID == 0 {
		return fail(OutcomeInvalidInput, -1, nil, "policy partition ID is zero")
	}
	order, err := types.CanonicalShardOrder(p.Topology.Scheme)
	if err != nil {
		return fail(OutcomeInvalidInput, -1, err, "policy topology")
	}
	for _, id := range order {
		if h := p.Topology.ShardConfHashes[id.Key()]; len(h) != sha256.Size {
			return fail(OutcomeInvalidInput, -1, nil, "policy lacks configuration hash of shard %q", id)
		}
	}
	return nil
}

// id returns the policy identity bytes; explicit ID or digest of partition and topology.
func (p Policy) id() []byte {
	var b []byte
	b = appendLP(b, p.ID)
	b = binary.BigEndian.AppendUint64(b, uint64(p.PartitionID))
	order, _ := types.CanonicalShardOrder(p.Topology.Scheme)
	for _, id := range order {
		b = appendLP(b, []byte(id.String()))
		b = appendLP(b, p.Topology.ShardConfHashes[id.Key()])
	}
	h := sha256.Sum256(b)
	return h[:]
}

var _ types.SealVerifier = (*Session)(nil)
