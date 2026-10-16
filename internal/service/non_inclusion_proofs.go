package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// get_non_inclusion_proof.v1: absence of one state key at the latest certified snapshot, as
// ONE pinned lease (UC, block and reader together). It proves absence at that snapshot only,
// never current absence or spendability. The wire is that of rugregator and the Rust SDK:
// params {"stateId"}, result the hex of [block, 39034([1, certificate, UC])]; codes -32020
// (no certified snapshot), -32022 (the state is included at the selected root), -32023 (busy).
// Go keeps HTTP 200 for JSON-RPC errors (rugregator uses 404/429): clients map by code.

// NonInclusionLease is one pinned certified snapshot. *proofview.Lease satisfies it.
type NonInclusionLease interface {
	Context() context.Context
	Root() []byte
	EmptyTree() bool
	Block() proofview.BlockIdentity
	UC() []byte
	GetNonInclusionCert(key []byte) (*api.NonInclusionCert, error)
	Release()
}

// NonInclusionViewSource acquires the latest certified snapshot. Public non-inclusion is
// latest-only: an exact-root selector is a later coordinated extension.
type NonInclusionViewSource interface {
	AcquireLatest(ctx context.Context) (NonInclusionLease, error)
}

// NonInclusionCatalogSource adapts a proofview.Catalog.
func NonInclusionCatalogSource(c *proofview.Catalog) NonInclusionViewSource {
	return niCatalogSource{c}
}

type niCatalogSource struct{ c *proofview.Catalog }

func (s niCatalogSource) AcquireLatest(ctx context.Context) (NonInclusionLease, error) {
	l, err := s.c.AcquireLatest(ctx)
	if err != nil {
		return nil, err
	}
	return l, nil
}

// NonInclusionConfig configures the service. Zero values take the defaults (8 active proofs,
// five second execution).
type NonInclusionConfig struct {
	MaxConcurrent int
	MaxExecution  time.Duration
}

// NonInclusionService serves get_non_inclusion_proof.v1 from one pinned snapshot.
type NonInclusionService struct {
	views   NonInclusionViewSource
	shard   BatchShardValidator
	slots   chan struct{}
	timeout time.Duration
}

// NewNonInclusionService creates the service.
func NewNonInclusionService(views NonInclusionViewSource, shard BatchShardValidator, cfg NonInclusionConfig) *NonInclusionService {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = DefaultBatchMaxConcurrent
	}
	if cfg.MaxExecution <= 0 {
		cfg.MaxExecution = BatchMaxExecution
	}
	return &NonInclusionService{views: views, shard: shard, slots: make(chan struct{}, cfg.MaxConcurrent), timeout: cfg.MaxExecution}
}

// niError is a typed failure mapped to a JSON-RPC error.
type niError struct {
	rpc      *jsonrpc.Error
	internal error
}

func (e *niError) Error() string {
	if e.internal != nil {
		return e.rpc.Message + ": " + e.internal.Error()
	}
	return e.rpc.Message
}
func (e *niError) Unwrap() error { return e.internal }

func niUnavailable(detail string, retryable bool) *niError {
	return &niError{rpc: jsonrpc.NewDetailedError(api.NonInclusionCodeNotFound, "No certified snapshot", "proof_unavailable", detail, retryable, nil)}
}

func niBusy(detail string) *niError {
	return &niError{rpc: jsonrpc.NewDetailedError(api.NonInclusionCodeBusy, "Server busy", "busy", detail, true, nil)}
}

func niIncluded() *niError {
	return &niError{rpc: jsonrpc.NewDetailedError(api.NonInclusionCodeStateIncluded, "State included", "state_included", "state_included", false, nil)}
}

func niInvalid(detail string) *niError {
	return &niError{rpc: jsonrpc.NewDetailedError(jsonrpc.InvalidParamsCode, "Invalid params", "invalid_request", detail, false, nil)}
}

func niInternal(err error) *niError {
	// no detail and no cause text for the client
	return &niError{rpc: jsonrpc.NewError(jsonrpc.InternalErrorCode, "Internal error", nil), internal: err}
}

func niCtx(err error) *niError {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return niBusy("deadline")
	}
	return niInternal(err)
}

// GetNonInclusionProofV1 returns the absence proof of the state at the latest certified snapshot.
// It generates the certificate from the leased reader and verifies it locally against the leased UC
// root before returning the UC.
func (s *NonInclusionService) GetNonInclusionProofV1(ctx context.Context, req *api.GetNonInclusionProofRequestV1) (*api.GetNonInclusionProofResponseV1, error) {
	if req == nil {
		return nil, niInvalid(api.DetailMalformedParams)
	}
	key, err := req.StateID.GetTreeKey()
	if err != nil {
		return nil, niInvalid(api.DetailInvalidStateID)
	}
	if err := s.shard.ValidateShardID(req.StateID); err != nil {
		return nil, niInvalid(api.DetailWrongShard)
	}

	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return nil, niBusy("active_requests")
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	lease, err := s.views.AcquireLatest(ctx)
	if err != nil {
		return nil, niAcquireError(err)
	}
	defer lease.Release()
	// work stops with the request, its deadline and the lease (pressure, lease deadline)
	work, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	stop := context.AfterFunc(lease.Context(), cancelWork)
	defer stop()
	if err := work.Err(); err != nil {
		return nil, niCtx(err)
	}

	blk := lease.Block()
	if blk.Number == nil {
		return nil, niInternal(errors.New("lease without block number"))
	}
	cert, err := lease.GetNonInclusionCert(key)
	switch {
	case err == nil:
	case errors.Is(err, api.ErrStateIncluded):
		return nil, niIncluded()
	case errors.Is(err, backend.ErrNonInclusionUnsupported):
		return nil, niUnavailable("unsupported_configuration", false)
	default:
		if cerr := work.Err(); cerr != nil {
			return nil, niCtx(cerr)
		}
		return nil, niInternal(fmt.Errorf("non-inclusion certificate: %w", err))
	}
	if err := work.Err(); err != nil {
		return nil, niCtx(err)
	}
	// the certificate must authenticate the root of the very UC returned with it
	switch verr := cert.VerifyAgainstTrustedRoot(key, lease.Root(), api.SHA256); {
	case verr == nil:
	case errors.Is(verr, api.ErrStateIncluded):
		return nil, niIncluded()
	case cert.IsEmptyTree() && !lease.EmptyTree():
		// the UC certifies a nonempty root (the 32 zero bytes this aggregator certifies for an
		// empty tree after genesis) which an empty tree certificate cannot prove: nothing
		// certified to prove absence against yet. It clears with the next certified commitment.
		return nil, niUnavailable("empty_snapshot_not_provable", true)
	default:
		return nil, niInternal(fmt.Errorf("generated certificate does not verify: %w", verr))
	}
	uc := lease.UC()
	if len(uc) == 0 || len(uc)+api.MaxNonInclusionCertBytes > api.NonInclusionMaxResponseBytes/2 {
		return nil, niInternal(errors.New("unicity certificate size"))
	}
	return &api.GetNonInclusionProofResponseV1{
		BlockNumber: blk.Number.Uint64(),
		Proof:       &api.NonInclusionProofV1{Certificate: cert, UnicityCertificate: types.RawCBOR(uc)},
	}, nil
}

func niAcquireError(err error) *niError {
	switch {
	case errors.Is(err, proofview.ErrNoFinalizedView):
		return niUnavailable("no_finalized_view", true)
	case errors.Is(err, proofview.ErrViewNotRetained):
		return niUnavailable("view_not_retained", true)
	case errors.Is(err, proofview.ErrBusyPins):
		return niBusy("pinned_views")
	case errors.Is(err, proofview.ErrBusyBudget):
		return niBusy("retention_budget")
	}
	return niCtx(err)
}

// HandleRPC is the JSON-RPC handler of get_non_inclusion_proof.v1.
func (s *NonInclusionService) HandleRPC(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	req, err := api.ParseGetNonInclusionProofRequestV1(params)
	if err != nil {
		return nil, niInvalid(api.DetailMalformedParams).rpc
	}
	resp, err := s.GetNonInclusionProofV1(ctx, req)
	if err != nil {
		var ne *niError
		if errors.As(err, &ne) {
			return nil, ne.rpc
		}
		return nil, niInternal(err).rpc
	}
	return resp, nil
}

// UnsupportedNonInclusionRPC is the handler for deployments which cannot serve the method.
func UnsupportedNonInclusionRPC(context.Context, json.RawMessage) (interface{}, *jsonrpc.Error) {
	return nil, niUnavailable("unsupported_configuration", false).rpc
}

// NewNonInclusionHandler builds the get_non_inclusion_proof.v1 handler for the deployment. Like
// the batch method it needs real BFT certification (never the development stub's skeletal UC),
// a retained-view catalog and a shard router; parent/child aggregation is unsupported (their
// trees compose differently and no catalog pins both levels). When it cannot serve it returns
// the explicit unsupported handler together with the reason.
func NewNonInclusionHandler(cfg *config.Config, rm any, svc any) (jsonrpc.HandlerFunc, string) {
	if m := cfg.Sharding.Mode; m.IsParent() || m.IsChild() {
		return UnsupportedNonInclusionRPC, "parent/child aggregation"
	}
	if !cfg.BFT.Enabled || cfg.BFT.ShardConf == nil {
		return UnsupportedNonInclusionRPC, "BFT disabled"
	}
	provider, ok := rm.(ProofViewProvider)
	if !ok || provider.ProofViewCatalog() == nil {
		return UnsupportedNonInclusionRPC, "SMT backend cannot retain read views"
	}
	routed, ok := svc.(interface{ ShardValidator() BatchShardValidator })
	if !ok {
		return UnsupportedNonInclusionRPC, "service cannot validate shard routing"
	}
	s := NewNonInclusionService(NonInclusionCatalogSource(provider.ProofViewCatalog()), routed.ShardValidator(), NonInclusionConfig{
		MaxConcurrent: cfg.SMT.MaxConcurrentBatches,
		MaxExecution:  cfg.SMT.BatchTimeout,
	})
	return s.HandleRPC, ""
}
