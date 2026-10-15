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

// TopologyProvider returns the trusted topology for the partition if the deployment
// has one configured. Without it the scheme is derived from the (seal-authenticated)
// view itself and the view is NOT bound to admitted shard configurations.
type TopologyProvider interface {
	Topology(partition types.PartitionID) (types.PartitionViewTopology, bool)
}

// Validation outcomes used as metric labels.
const (
	ResultOK              = "ok"
	ResultMissing         = "missing"
	ResultMalformed       = "malformed"
	ResultInvalid         = "invalid"
	ResultTooLarge        = "too_large"
	ResultDerivedTopology = "ok_derived_topology"
)

// ValidateView decodes the view bytes and checks them against the already
// authenticated enclosing UC (the caller has verified the seal). Returns the label
// of the outcome and, on success, the bytes to retain (a private copy).
// Any failure only means "no proof material".
func ValidateView(raw []byte, uc *types.UnicityCertificate, partition types.PartitionID, topos TopologyProvider) (string, []byte, error) {
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
	var topo types.PartitionViewTopology
	result := ResultOK
	if t, ok := topologyFor(topos, partition); ok {
		topo = t
	} else {
		scheme := make(types.ShardingScheme, 0, len(view.Leaves))
		for _, l := range view.Leaves {
			scheme = append(scheme, l.ShardID)
		}
		if len(scheme) == 1 && scheme[0].Length() == 0 {
			scheme = types.ShardingScheme{}
		}
		topo = types.PartitionViewTopology{Scheme: scheme}
		result = ResultDerivedTopology
	}
	if _, err := view.VerifyTree(uc, partition, topo, crypto.SHA256); err != nil {
		return ResultInvalid, nil, err
	}
	return result, append([]byte(nil), raw...), nil
}

func topologyFor(p TopologyProvider, partition types.PartitionID) (types.PartitionViewTopology, bool) {
	if p == nil {
		return types.PartitionViewTopology{}, false
	}
	return p.Topology(partition)
}
