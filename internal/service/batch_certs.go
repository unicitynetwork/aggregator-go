package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// batchCertLease is implemented by leases which can build the proofs of many keys
// from the pinned state at once (*proofview.Lease). For RocksDB views that is one
// union-path materialization with bulk frontier reads, instead of one fresh
// materialization per key.
type batchCertLease interface {
	GetInclusionCerts(keys [][]byte) ([]*api.InclusionCert, error)
}

// prebuiltLease serves the inclusion certificates built up front and defers anything
// else to the underlying lease.
type prebuiltLease struct {
	BatchLease
	certs map[string]*api.InclusionCert
}

func (p prebuiltLease) GetInclusionCert(key []byte) (*api.InclusionCert, error) {
	if c, ok := p.certs[string(key)]; ok {
		return c, nil
	}
	return p.BatchLease.GetInclusionCert(key)
}

// prebuildCerts builds all proofs of the batch from the one pinned lease in a single
// pass when the lease supports it. A leaf missing from the pinned root is not an error
// here: the per-key path then reports it at the exact state (state_not_included).
func prebuildCerts(ctx context.Context, lease BatchLease, keys [][]byte) (BatchLease, *BatchError) {
	bl, ok := lease.(batchCertLease)
	if !ok || len(keys) < 2 {
		return lease, nil
	}
	certs, err := bl.GetInclusionCerts(keys)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, ctxError(cerr)
		}
		if cerr := lease.Context().Err(); cerr != nil {
			return nil, ctxError(cerr)
		}
		if errors.Is(err, backend.ErrPublishedProofLeafNotFound) || errors.Is(err, smt.ErrLeafNotFound) {
			return lease, nil
		}
		return nil, &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: fmt.Errorf("batch inclusion certs: %w", err)}
	}
	if len(certs) != len(keys) {
		return nil, &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: fmt.Errorf("batch inclusion certs: got %d for %d keys", len(certs), len(keys))}
	}
	m := make(map[string]*api.InclusionCert, len(keys))
	for i, key := range keys {
		m[string(key)] = certs[i]
	}
	return prebuiltLease{BatchLease: lease, certs: m}, nil
}
