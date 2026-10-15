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
	"fmt"

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
	ResultStaleBlock   = "stale_block"
	ResultNoConfig     = "no_config_identity"
	ResultIncompatible = "attach_incompatible"
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
	backend  smtbackend.Backend
	// partition is used to re-validate stored partition views against their UC
	// before publication (restored material is never trusted blindly).
	partition types.PartitionID
	topology  proofmaterial.TopologyProvider
	log       *logger.Logger
	// sealVerifier authenticates stored views again (nil: stored views are not republished).
	sealVerifier func(ctx context.Context) types.SealVerifier
}

// New returns a publisher, or nil when the backend cannot retain read views
// (the batch API is then unavailable for this deployment). A nil *Publisher is
// valid and does nothing.
func New(catalog *proofview.Catalog, backend smtbackend.Backend, partition types.PartitionID, topology proofmaterial.TopologyProvider, log *logger.Logger) *Publisher {
	retainer, ok := backend.(smtbackend.ReadViewRetainer)
	if !ok || catalog == nil {
		return nil
	}
	return &Publisher{catalog: catalog, retainer: retainer, backend: backend, partition: partition, topology: topology, log: log}
}

// SetSealVerifier sets the trusted seal verifier used to re-authenticate stored
// partition views before they are published again (the BFT client's verifier).
func (p *Publisher) SetSealVerifier(f func(ctx context.Context) types.SealVerifier) {
	if p != nil {
		p.sealVerifier = f
	}
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
	p.publish(ctx, block, true, false)
}

// EnsurePublished publishes the committed state for the block when it is the
// backend's current state (startup, retries, follower replay). A failure leaves the
// catalog untouched: the state may legitimately have moved on.
func (p *Publisher) EnsurePublished(ctx context.Context, block *models.Block) {
	p.publish(ctx, block, false, false)
}

// EnsurePublishedAtStartup is EnsurePublished for a freshly restored backend which does
// not know its committed block number: the caller has verified that the restored root is
// the latest finalized block's root. It must only be given that latest block.
func (p *Publisher) EnsurePublishedAtStartup(ctx context.Context, latest *models.Block) {
	p.publish(ctx, latest, false, true)
}

// InvalidateLatest makes latest unavailable (state advanced but could not be published).
func (p *Publisher) InvalidateLatest() {
	if p != nil {
		p.catalog.MarkLatestUnavailable()
	}
}

func (p *Publisher) publish(ctx context.Context, block *models.Block, strict, unknownCommittedOK bool) {
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
	// The configuration identity of a state is the one its accepted UC carries, never a
	// process-wide startup value.
	lineage, err := ucLineage(block.UnicityCertificate)
	if err != nil {
		fail(ResultNoConfig, err)
		return
	}
	// The block must be the one the backend is committed at: a historical block whose
	// root merely equals the current one (eg consecutive empty rounds) must never be
	// recreated as a view, nor replace latest. Callers hold the commit ordering lock.
	st, err := p.backend.CommittedState(ctx)
	if err != nil {
		fail(ResultRetainFailed, err)
		return
	}
	switch {
	case st.BlockNumber != nil && st.BlockNumber.Cmp(block.Index.Int) != 0:
		fail(ResultStaleBlock, nil)
		return
	case st.BlockNumber == nil && !unknownCommittedOK:
		fail(ResultStaleBlock, nil)
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
		Block:  proofview.BlockIdentity{Number: block.Index, ConfigLineage: lineage},
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
	var verifier types.SealVerifier
	if p.sealVerifier != nil {
		verifier = p.sealVerifier(ctx)
	}
	// fail closed: without an authenticating verifier a stored view is not republished
	if err := types.Cbor.Unmarshal(block.UnicityCertificate, &uc); err == nil && p.partition != 0 && verifier != nil {
		if _, view, err := proofmaterial.ValidateView(block.PartitionView, &uc, p.partition, verifier, p.topology); err == nil {
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
// certify. It neither commits nor advances state. A missing retained state or a
// rejected attachment is returned so the caller can retry on a later delivery.
func (p *Publisher) PublishProofMaterial(ctx context.Context, m *proofmaterial.Material) error {
	if p == nil || m == nil || m.UC == nil || m.UC.InputRecord == nil {
		return nil
	}
	root := m.UC.InputRecord.Hash
	lineage := m.UC.ShardConfHash
	if len(lineage) != proofmaterial.ConfHashSize {
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultNoConfig).Inc()
		return errors.New("certification carries no shard configuration identity")
	}
	var id proofview.BlockIdentity
	if m.Kind == proofmaterial.KindFinalized && m.HasBlock {
		id = proofview.BlockIdentity{Number: api.NewBigIntFromUint64(m.BlockNumber), ConfigLineage: lineage}
	} else {
		// repeat / duplicate: only onto the retained state with a compatible accepted
		// identity, never selected by the state hash alone
		found, refUC, ok := p.catalog.ViewForRoot(root)
		if !ok {
			metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttachMiss).Inc()
			return proofview.ErrViewNotRetained
		}
		if err := compatible(refUC, m.UC); err != nil {
			metrics.ProofViewPublicationTotal.WithLabelValues(ResultIncompatible).Inc()
			return err
		}
		id = found
	}
	err := p.catalog.AttachCertification(ctx, id, root, proofview.Certification{UC: m.UCBytes, PartitionView: m.PartitionView})
	switch {
	case err == nil:
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttached).Inc()
	case errors.Is(err, proofview.ErrViewNotRetained):
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttachMiss).Inc()
	default:
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttachFailed).Inc()
	}
	// The error goes back to the BFT client, which only logs it (certification is never
	// failed) but keeps the sidecar fill retryable for later duplicate deliveries.
	return err
}

// ErrIncompatibleCertification: the certification is not for the retained state's accepted
// input record / shard configuration (an unsupported boundary, eg an epoch change).
var ErrIncompatibleCertification = errors.New("certification is incompatible with the retained state")

// ucLineage extracts the shard configuration identity of an accepted UC.
func ucLineage(ucBytes []byte) ([]byte, error) {
	var uc types.UnicityCertificate
	if err := types.Cbor.Unmarshal(ucBytes, &uc); err != nil {
		return nil, fmt.Errorf("decoding UC: %w", err)
	}
	if len(uc.ShardConfHash) != proofmaterial.ConfHashSize {
		return nil, errors.New("UC carries no shard configuration identity")
	}
	return append([]byte(nil), uc.ShardConfHash...), nil
}

// compatible requires the new certification to repeat the retained state's accepted
// input record (round, epoch, state, previous and block hashes) and shard configuration.
func compatible(refUCBytes []byte, uc *types.UnicityCertificate) error {
	var ref types.UnicityCertificate
	if err := types.Cbor.Unmarshal(refUCBytes, &ref); err != nil || ref.InputRecord == nil || uc.InputRecord == nil {
		return fmt.Errorf("%w: unreadable reference", ErrIncompatibleCertification)
	}
	a, b := ref.InputRecord, uc.InputRecord
	if a.RoundNumber != b.RoundNumber || a.Epoch != b.Epoch ||
		!bytes.Equal(a.Hash, b.Hash) || !bytes.Equal(a.PreviousHash, b.PreviousHash) || !bytes.Equal(a.BlockHash, b.BlockHash) {
		return fmt.Errorf("%w: input record differs", ErrIncompatibleCertification)
	}
	if !bytes.Equal(ref.ShardConfHash, uc.ShardConfHash) {
		return fmt.Errorf("%w: shard configuration differs", ErrIncompatibleCertification)
	}
	return nil
}

var _ proofmaterial.Sink = (*Publisher)(nil)
