package backend

import (
	"context"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// BatchReadView is implemented by read views which can build many proofs at
// once from the same pinned state more cheaply than one at a time (the disk
// view materializes the union of the proof paths with bulk frontier reads).
type BatchReadView interface {
	// GetInclusionCerts returns one certificate per key, in order, from the
	// retained root. It fails as a whole (like a single call would for the
	// first offending key); a key absent from the retained state is reported
	// as ErrPublishedProofLeafNotFound.
	GetInclusionCerts(ctx context.Context, keys [][]byte) ([]*api.InclusionCert, error)
}

// InclusionCerts builds the proofs for keys, using the batch path when the view
// has one and one call per key otherwise.
func InclusionCerts(ctx context.Context, v ReadView, keys [][]byte) ([]*api.InclusionCert, error) {
	if b, ok := v.(BatchReadView); ok {
		return b.GetInclusionCerts(ctx, keys)
	}
	certs := make([]*api.InclusionCert, len(keys))
	for i, key := range keys {
		cert, err := v.GetInclusionCert(ctx, key)
		if err != nil {
			return nil, err
		}
		certs[i] = cert
	}
	return certs, nil
}
