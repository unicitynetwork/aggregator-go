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
	// notify wakes long-poll waiters after a publication outcome which no finalization path
	// reports itself: late certification material. Finalization publishes
	// first and its caller notifies afterwards, once the proof-ready update is done (a wake
	// from inside publish would precede markProofsReady). Called without publisher locks.
	notify func()
}

// SetNotify registers the wake-up callback (call before use). It must not block.
func (p *Publisher) SetNotify(f func()) {
	if p != nil {
		p.notify = f
	}
}

func (p *Publisher) wake() {
	if p != nil && p.notify != nil {
		p.notify()
	}
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
	p.publish(ctx, block, true, false, false)
}

// PublishFinalizedAdmitted is PublishFinalized for the live path where block.PartitionView
// is the view the BFT client authenticated at intake for exactly block.UnicityCertificate
// (taken from its staging cache, keyed by the complete UC digest). That admitted material
// is reused as is instead of being authenticated a second time inside the commit lock;
// restored, recovered and startup material always goes through the revalidating paths.
func (p *Publisher) PublishFinalizedAdmitted(ctx context.Context, block *models.Block) {
	p.publish(ctx, block, true, false, true)
}

// EnsurePublished publishes the committed state for the block when it is the
// backend's current state (startup, retries, follower replay). A failure leaves the
// catalog untouched: the state may legitimately have moved on.
func (p *Publisher) EnsurePublished(ctx context.Context, block *models.Block) {
	p.publish(ctx, block, false, false, false)
}

// EnsurePublishedAtStartup is EnsurePublished for a freshly restored backend which does
// not know its committed block number: the caller has verified that the restored root is
// the latest finalized block's root. It must only be given that latest block.
func (p *Publisher) EnsurePublishedAtStartup(ctx context.Context, latest *models.Block) {
	p.publish(ctx, latest, false, true, false)
}

// InvalidateLatest makes latest unavailable (state advanced but could not be published).
func (p *Publisher) InvalidateLatest() {
	if p != nil {
		p.catalog.MarkLatestUnavailable()
		// No wake here: its callers hold finalizationMu, and each of them signals after its own
		// completion and unlock, so the waiter still observes the unavailable outcome.
	}
}

func (p *Publisher) publish(ctx context.Context, block *models.Block, strict, unknownCommittedOK, admitted bool) {
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
	var uc types.UnicityCertificate // decoded once for lineage and view revalidation
	if err := types.Cbor.Unmarshal(block.UnicityCertificate, &uc); err != nil {
		fail(ResultNoConfig, fmt.Errorf("decoding UC: %w", err))
		return
	}
	if len(uc.ShardConfHash) != proofmaterial.ConfHashSize {
		fail(ResultNoConfig, errors.New("UC carries no shard configuration identity"))
		return
	}
	lineage := append([]byte(nil), uc.ShardConfHash...)
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
		Cert:   proofview.Certification{UC: block.UnicityCertificate, PartitionView: p.viewFor(ctx, block, &uc, admitted)},
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
func (p *Publisher) viewFor(ctx context.Context, block *models.Block, uc *types.UnicityCertificate, admitted bool) []byte {
	if len(block.PartitionView) == 0 {
		return nil
	}
	if admitted {
		metrics.ProofViewPublicationTotal.WithLabelValues("view_admitted").Inc()
		return append([]byte(nil), block.PartitionView...)
	}
	return p.revalidatedView(ctx, block, uc)
}

// revalidatedView re-authenticates a stored view against its own UC with the trusted seal
// verifier. A view that does not authenticate is dropped (the certification is published
// without it); without a verifier nothing stored is republished.
func (p *Publisher) revalidatedView(ctx context.Context, block *models.Block, uc *types.UnicityCertificate) []byte {
	var verifier types.SealVerifier
	if p.sealVerifier != nil {
		verifier = p.sealVerifier(ctx)
	}
	if p.partition != 0 && verifier != nil {
		if _, view, err := proofmaterial.ValidateView(block.PartitionView, uc, p.partition, verifier, p.topology); err == nil {
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
	if m.Kind == proofmaterial.KindInitial && len(root) != 0 {
		return nil // a retained state is published by the startup/finalization paths
	}
	if len(root) == 0 {
		return p.publishCertifiedEmpty(ctx, m, lineage)
	}
	var id proofview.BlockIdentity
	if m.Kind == proofmaterial.KindFinalized && m.HasBlock {
		id = proofview.BlockIdentity{Number: api.NewBigIntFromUint64(m.BlockNumber), ConfigLineage: lineage}
	} else {
		if m.Kind == proofmaterial.KindSidecarFill {
			// a late view belongs to the exact certification it arrived with
			var ok bool
			if id, _, ok = p.catalog.ViewForDigest(m.UCDigest); !ok {
				metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttachMiss).Inc()
				return proofview.ErrViewNotRetained
			}
		} else {
			// repeat/timeout UC: onto the retained state of the same root whose accepted
			// input record and configuration it repeats, never by the root alone
			var ok bool
			if id, ok = p.catalog.ViewForRootCompatible(root, proofview.CompatDigest(m.UC)); !ok {
				if p.catalog.HasCertifiedRoot(root) {
					metrics.ProofViewPublicationTotal.WithLabelValues(ResultIncompatible).Inc()
					return ErrIncompatibleCertification
				}
				metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttachMiss).Inc()
				return proofview.ErrViewNotRetained
			}
		}
	}
	err := p.catalog.AttachCertification(ctx, id, root, proofview.Certification{UC: m.UCBytes, PartitionView: m.PartitionView})
	switch {
	case err == nil:
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttached).Inc()
		p.wake() // e.g. a late partition view turns partition_view_unavailable into a proof
	case errors.Is(err, proofview.ErrViewNotRetained):
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttachMiss).Inc()
	default:
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttachFailed).Inc()
	}
	// The error goes back to the BFT client, which only logs it (certification is never
	// failed) but keeps the sidecar fill retryable for later duplicate deliveries.
	return err
}

// EmptyTreeBlockNumber is the block identity number of the certified empty tree: it precedes
// every real block (blocks start at 1) and is never served as a block.
const EmptyTreeBlockNumber = 0

// ErrEmptyTreeNotPublishable: the empty tree cannot be published as a certified snapshot now.
var ErrEmptyTreeNotPublishable = errors.New("the certified empty tree cannot be published")

// publishCertifiedEmpty publishes (or re-certifies) the certified EMPTY tree: an authenticated
// native UC whose input record carries the empty state hash, reconciled with the backend's explicit
// statement that its committed state is the empty tree of a node without any committed block. A
// UC with an empty hash never authorizes anything else: not a finalized block, not a state of a
// node which has committed history, not a 32 byte zero root.
func (p *Publisher) publishCertifiedEmpty(ctx context.Context, m *proofmaterial.Material, lineage []byte) error {
	reject := func(result string, err error) error {
		metrics.ProofViewPublicationTotal.WithLabelValues(result).Inc()
		return err
	}
	if m.Kind == proofmaterial.KindFinalized || m.Kind == proofmaterial.KindSidecarFill || m.HasBlock {
		return reject(ResultRejected, fmt.Errorf("%w: an empty root is not a finalized block", ErrEmptyTreeNotPublishable))
	}
	if m.UC.InputRecord.Hash == nil {
		// native null: the certified empty tree
	} else {
		// an empty byte string is a different encoding and is not the empty tree
		return reject(ResultRejected, fmt.Errorf("%w: the input record hash is an empty byte string, not null", ErrEmptyTreeNotPublishable))
	}
	id := proofview.BlockIdentity{Number: api.NewBigIntFromUint64(EmptyTreeBlockNumber), ConfigLineage: lineage}
	cert := proofview.Certification{UC: m.UCBytes}
	// a later UC of the same empty state (repeat/timeout) attaches to the retained snapshot
	if err := p.catalog.AttachCertification(ctx, id, nil, cert); err == nil {
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttached).Inc()
		return nil
	} else if !errors.Is(err, proofview.ErrViewNotRetained) {
		metrics.ProofViewPublicationTotal.WithLabelValues(ResultAttachFailed).Inc()
		return err
	}
	// first publication: the UC seal is authenticated again here, the backend must be at its
	// explicit empty state and have no committed block
	var verifier types.SealVerifier
	if p.sealVerifier != nil {
		verifier = p.sealVerifier(ctx)
	}
	if verifier == nil || verifier.VerifySeal(m.UC.UnicitySeal) != nil {
		return reject(ResultRejected, fmt.Errorf("%w: the unicity certificate is not authenticated", ErrEmptyTreeNotPublishable))
	}
	st, err := p.backend.CommittedState(ctx)
	if err != nil {
		return reject(ResultRetainFailed, err)
	}
	if st.BlockNumber != nil {
		return reject(ResultStaleBlock, fmt.Errorf("%w: the backend has committed blocks", ErrEmptyTreeNotPublishable))
	}
	rv, err := p.retainer.RetainReadView(ctx)
	if err != nil {
		return reject(ResultRetainFailed, err)
	}
	if !smtbackend.IsEmptyState(rv) {
		_ = rv.Close()
		return reject(ResultRootMismatch, fmt.Errorf("%w: the backend state is not empty", ErrEmptyTreeNotPublishable))
	}
	if err := p.catalog.Publish(ctx, proofview.PublishInput{Block: id, Reader: rv, Cert: cert, EmptyTree: true}); err != nil {
		return reject(ResultRejected, err)
	}
	metrics.ProofViewPublicationTotal.WithLabelValues(ResultPublished).Inc()
	return nil
}

// ErrIncompatibleCertification: the certification is not for the retained state's accepted
// input record / shard configuration (an unsupported boundary, eg an epoch change).
var ErrIncompatibleCertification = errors.New("certification is incompatible with the retained state")

var _ proofmaterial.Sink = (*Publisher)(nil)
