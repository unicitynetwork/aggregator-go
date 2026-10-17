// Package proofmaterial carries the optional partition view ("sidecar") received
// with a CertificationResponseV2 from transport intake to whoever publishes proof
// views. It never influences ordinary UC processing.
package proofmaterial

import (
	"context"
	"crypto"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"
)

// Digest identifies a complete unicity certificate (SHA-256 of its CBOR encoding).
type Digest [sha256.Size]byte

// DigestOfUC returns the digest and canonical encoding of the UC.
func DigestOfUC(uc *types.UnicityCertificate) (Digest, []byte, error) {
	if uc == nil {
		return Digest{}, nil, errors.New("nil unicity certificate")
	}
	b, err := types.Cbor.Marshal(uc)
	if err != nil {
		return Digest{}, nil, fmt.Errorf("encoding unicity certificate: %w", err)
	}
	return sha256.Sum256(b), b, nil
}

// IsUnshardedUC reports whether the UC is structurally that of an unsharded partition: empty shard
// ID and zero shard-tree siblings (its shard tree is a single leaf). Such a UC already commits to
// everything a one-leaf partition view would repeat, so no sidecar is needed, staged or served for it.
// Components are checked before they are dereferenced.
func IsUnshardedUC(uc *types.UnicityCertificate) bool {
	if uc == nil || uc.InputRecord == nil || uc.UnicitySeal == nil || uc.UnicityTreeCertificate == nil {
		return false
	}
	return uc.ShardTreeCertificate.Shard.Length() == 0 && len(uc.ShardTreeCertificate.SiblingHashes) == 0
}

// Kind tells why material is published.
type Kind int

const (
	// KindFinalized: UC accepted and the local state it certifies is finalized.
	KindFinalized Kind = iota
	// KindRepeat: operationally accepted repeat/timeout UC; no new local state was
	// finalized. Consumers must only attach it to a matching finalized state/IR lineage.
	KindRepeat
	// KindSidecarFill: a duplicate delivery of an already published UC added a
	// previously missing valid partition view.
	KindSidecarFill
)

func (k Kind) String() string {
	switch k {
	case KindFinalized:
		return "finalized"
	case KindRepeat:
		return "repeat"
	case KindSidecarFill:
		return "sidecar_fill"
	}
	return "unknown"
}

// Material is an immutable publication. Consumers must not modify the slices.
type Material struct {
	Kind Kind
	// UC is the accepted certificate, UCBytes its canonical encoding, UCDigest its digest.
	UC       *types.UnicityCertificate
	UCBytes  []byte
	UCDigest Digest
	// PartitionView is the validated canonical PartitionViewV1 CBOR bound to exactly
	// this UC, or nil when no valid sidecar was received for it.
	PartitionView []byte
	// BlockNumber is the local finalized block identity for KindFinalized, otherwise
	// nil/unset (HasBlock false). It is never derived from the technical record.
	BlockNumber uint64
	HasBlock    bool
}

// Sink consumes published proof material. A4 wires it to the retained-view catalog.
// Implementations must not block certification; errors are logged and counted by the
// caller and never fail finalization.
type Sink interface {
	PublishProofMaterial(ctx context.Context, m *Material) error
}

// NopSink discards material.
type NopSink struct{}

func (NopSink) PublishProofMaterial(context.Context, *Material) error { return nil }

// TopologyProvider optionally returns an independently pinned topology of the partition:
// its complete sharding scheme and the admitted configuration hash (exactly 32 bytes) of
// every shard. It must come from the deployment's own configuration, never from a
// response. Pins only restrict: without them the topology is the one committed by the
// authenticated seal. A partition it cannot resolve yields (_, false): no pins.
type TopologyProvider interface {
	Topology(partition types.PartitionID) (types.PartitionViewTopology, bool)
}

// Validation outcomes used as metric labels.
const (
	ResultOK        = "ok"
	ResultMissing   = "missing"
	ResultMalformed = "malformed"
	ResultInvalid   = "invalid"
	ResultTooLarge  = "too_large"
	// ResultNoTopology: the configured pinned topology is itself unusable.
	ResultNoTopology = "no_trusted_topology"
)

// ConfHashSize is the required size of every admitted shard configuration hash.
const ConfHashSize = 32

// ValidateView authenticates the partition view of a response end to end
// (types.AuthenticatePartitionView): the view is decoded as untrusted data, must
// hash into the seal of the enclosing UC under the caller's expected partition,
// match the enclosing UC's own leaf exactly, and the complete seal must be
// authenticated by "verifier" (network, exact epoch, epoch start, quorum). The
// scheme and configuration hashes are then those committed by the seal.
//
// "pins" is an OPTIONAL independently admitted topology: when it resolves the
// partition the view must also match it (stricter). Returns the outcome label and, on
// success, a private copy of the bytes. Any failure only means "no proof material".
func ValidateView(raw []byte, uc *types.UnicityCertificate, partition types.PartitionID, verifier types.SealVerifier, pins TopologyProvider) (string, []byte, error) {
	if len(raw) == 0 {
		return ResultMissing, nil, nil
	}
	if len(raw) > types.MaxPartitionViewBytes {
		return ResultTooLarge, nil, fmt.Errorf("partition view is %d bytes, limit %d", len(raw), types.MaxPartitionViewBytes)
	}
	view, err := types.DecodePartitionViewV1(raw)
	if err != nil {
		return ResultMalformed, nil, err
	}
	var opts []types.AuthOption
	if topo, ok := topologyFor(pins, partition); ok {
		if err := checkTopology(topo); err != nil {
			return ResultNoTopology, nil, err
		}
		opts = append(opts, types.WithPinnedTopology(topo))
	}
	if _, err := types.AuthenticateView(view, uc, partition, verifier, crypto.SHA256, opts...); err != nil {
		return ResultInvalid, nil, err
	}
	return ResultOK, append([]byte(nil), raw...), nil
}

// checkTopology requires a valid complete scheme and a 32-byte hash for every shard.
func checkTopology(topo types.PartitionViewTopology) error {
	order, err := types.CanonicalShardOrder(topo.Scheme)
	if err != nil {
		return fmt.Errorf("trusted topology: %w", err)
	}
	for _, id := range order {
		if h := topo.ShardConfHashes[id.Key()]; len(h) != ConfHashSize {
			return fmt.Errorf("trusted topology: shard %q has no %d-byte configuration hash", id, ConfHashSize)
		}
	}
	return nil
}

// StaticTopology is a fixed trusted topology for one partition.
type StaticTopology struct {
	partition types.PartitionID
	topo      types.PartitionViewTopology
}

// NewStaticTopology builds and checks a trusted topology: scheme must be complete and
// every shard needs a 32-byte configuration hash (keyed by ShardID.Key()).
func NewStaticTopology(partition types.PartitionID, scheme types.ShardingScheme, confHashes map[string][]byte) (*StaticTopology, error) {
	topo := types.PartitionViewTopology{Scheme: scheme, ShardConfHashes: map[string][]byte{}}
	for k, v := range confHashes {
		topo.ShardConfHashes[k] = append([]byte(nil), v...)
	}
	if err := checkTopology(topo); err != nil {
		return nil, err
	}
	return &StaticTopology{partition: partition, topo: topo}, nil
}

// SingleShardTopology is the trusted topology of an unsharded partition: the empty
// scheme with the deployment's own shard configuration hash.
func SingleShardTopology(partition types.PartitionID, shardConfHash []byte) (*StaticTopology, error) {
	return NewStaticTopology(partition, types.ShardingScheme{}, map[string][]byte{types.ShardID{}.Key(): shardConfHash})
}

func (s *StaticTopology) Topology(partition types.PartitionID) (types.PartitionViewTopology, bool) {
	if s == nil || partition != s.partition {
		return types.PartitionViewTopology{}, false
	}
	return s.topo, true
}

func topologyFor(p TopologyProvider, partition types.PartitionID) (types.PartitionViewTopology, bool) {
	if p == nil {
		return types.PartitionViewTopology{}, false
	}
	return p.Topology(partition)
}
