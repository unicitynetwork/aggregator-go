// Package proofpub connects finalization to the retained-view catalog. It is the
// only place that retains read views and publishes them together with the block's
// certification.
//
// Contract with callers: Publish* is called only after the block is finalized in
// storage AND the backend's committed state is that block's state, under the same
// synchronization which orders commits (so the retained state cannot move). The
// publisher re-checks the retained root against the block root and never publishes
// a mismatching or uncertified state. Nothing here returns an error to the caller:
// proof-view problems never fail certification or finalization; instead they are
// logged, counted, and make "latest" unavailable (never an older view).
package proofpub

import (
	"bytes"
	"context"
	"errors"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Publication outcomes (metric labels).
const (
	ResultPublished    = "published"
	ResultNoCert       = "no_certificate"
	ResultRetainFailed = "retain_failed"
	ResultRootMismatch = "root_mismatch"
	ResultRejected     = "rejected"
	ResultUnsupported  = "unsupported_backend"
	ResultAttached     = "attached"
	ResultAttachMiss   = "attach_not_retained"
	ResultAttachFailed = "attach_failed"
)

// Publisher owns publication for one local backend/shard.
type Publisher struct {
	catalog  *proofview.Catalog
	retainer smtbackend.ReadViewRetainer
	lineage  []byte
	// partition is used to re-validate stored partition views against their UC
	// before publication (restored material is never trusted blindly).
	partition types.PartitionID
	log       *logger.Logger
}

// New returns a publisher, or nil when the backend cannot retain read views
// (the batch API is then unavailable for this deployment). A nil *Publisher is
// valid and does nothing.
func New(catalog *proofview.Catalog, backend smtbackend.Backend, partition types.PartitionID, configLineage []byte, log *logger.Logger) *Publisher {
	retainer, ok := backend.(smtbackend.ReadViewRetainer)
	if !ok || catalog == nil {
		return nil
	}
	return &Publisher{catalog: catalog, retainer: retainer, partition: partition, lineage: append([]byte(nil), configLineage...), log: log}
}

// Catalog returns the catalog (nil for a nil publisher).
func (p *Publisher) Catalog() *proofview.Catalog {
	if p == nil {
		return nil
	}
	return p.catalog
}

// PublishFinalized retains the backend's current committed state and publishes it
// with the block's certification. Use it at the point where the block has just been
// committed: if it cannot publish, the latest published view would be older than the
// local head, so latest is marked unavailable.
func (p *Publisher) PublishFinalized(ctx context.Context, block *models.Block) {
	p.publish(ctx, block, true)
}

// EnsurePublished publishes the committed state for the block when it is the
// backend's current state (startup, retries, follower replay). A failure leaves the
// catalog untouched: the state may legitimately have moved on.
func (p *Publisher) EnsurePublished(ctx context.Context, block *models.Block) {
	p.publish(ctx, block, false)
}

func (p *Publisher) publish(ctx context.Context, block *models.Block, strict bool) {
	if p == nil {
		return
	}
	fail := func(result string, err error) {
		metrics.ProofViewPublicationTotal.WithLabelValues(result).Inc()
		if err != nil {
			p.log.WithContext(ctx).Warn("Not publishing retained proof view", "result", result, "error", err.Error())
		} else {
			p.log.WithContext(ctx).Warn("Not publishing retained proof view", "result", result)
		}
		if strict {
			p.catalog.MarkLatestUnavailable()
		}
	}
	if block == nil || block.Index == nil || !block.Finalized || len(block.RootHash) == 0 {
		// never publish an unfinalized state
		fail(ResultRejected, errors.New("block is not finalized"))
		return
	}
	if len(block.UnicityCertificate) == 0 {
		fail(ResultNoCert, nil)
		return
	}
	rv, err := p.retainer.RetainReadView(ctx)
	if err != nil {
		fail(ResultRetainFailed, err)
		return
	}
	if !bytes.Equal(rv.RootHash(), block.RootHash) {
		_ = rv.Close()
		fail(ResultRootMismatch, nil)
		return
	}
	// Publish takes ownership of rv (closes it on failure).
	err = p.catalog.Publish(ctx, proofview.PublishInput{
		Block:  proofview.BlockIdentity{Number: block.Index, ConfigLineage: p.lineage},
		Root:   append([]byte(nil), block.RootHash...),
		Reader: rv,
		Cert:   proofview.Certification{UC: block.UnicityCertificate, PartitionView: p.revalidatedView(ctx, block)},
	})
	if err != nil {
		// ErrBudget already made latest unavailable inside the catalog
		fail(ResultRejected, err)
		return
	}
	metrics.ProofViewPublicationTotal.WithLabelValues(ResultPublished).Inc()
}

// revalidatedView re-checks the block's partition view against its own UC. A view
// that does not validate is dropped (the certification is published without it).
func (p *Publisher) revalidatedView(ctx context.Context, block *models.Block) []byte {
	if len(block.PartitionView) == 0 {
		return nil
	}
	var uc types.UnicityCertificate
	if err := types.Cbor.Unmarshal(block.UnicityCertificate, &uc); err == nil && p.partition != 0 {
		if _, view, err := proofmaterial.ValidateView(block.PartitionView, &uc, p.partition, nil); err == nil {
			return view
		} else {
			p.log.WithContext(ctx).Warn("Dropping stored partition view which does not validate against its UC",
				"block", block.Index.String(), "error", err.Error())
		}
	}
	metrics.ProofViewPublicationTotal.WithLabelValues("view_dropped").Inc()
	return nil
}

// PublishProofMaterial implements proofmaterial.Sink: it attaches accepted
// certifications (finalized-from-recovery with a late view, repeat/timeout UCs,
// duplicate deliveries that bring a view) to the retained finalized state they
// certify. It neither commits nor advances state, and never errors.
func (p *Publisher) PublishProofMaterial(ctx context.Context, m *proofmaterial.Material) error {
	if p == nil || m == nil || m.UC == nil || m.UC.InputRecord == nil {
		return nil
	}
	root := m.UC.InputRecord.Hash
	var id proofview.BlockIdentity
	if m.Kind == proofmaterial.KindFinalized && m.HasBlock {
		id = proofview.BlockIdentity{Number: api.NewBigIntFromUint64(m.BlockNumber), ConfigLineage: p.lineage}
	} else {
		var ok bool
		if id, ok = p.catalog.BlockForRoot(root); !ok {
			metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttachMiss).Inc()
			return nil
		}
	}
	err := p.catalog.AttachCertification(ctx, id, root, proofview.Certification{UC: m.UCBytes, PartitionView: m.PartitionView})
	switch {
	case err == nil:
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttached).Inc()
	case errors.Is(err, proofview.ErrViewNotRetained):
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttachMiss).Inc()
	default:
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttachFailed).Inc()
		p.log.WithContext(ctx).Warn("Attaching certification to retained view failed", "kind", m.Kind.String(), "error", err.Error())
	}
	return nil
}

var _ proofmaterial.Sink = (*Publisher)(nil)
