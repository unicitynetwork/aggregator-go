package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Batch inclusion proofs (get_inclusion_proofs.v1). The service consumes the
// retained-view catalog behind the narrow interfaces below, so it is tested
// with fakes and does not depend on how finalization publishes views.

const (
	// BatchMaxExecution is the maximum execution time of one batch.
	BatchMaxExecution = 5 * time.Second
	// DefaultBatchMaxConcurrent is the default number of concurrent batches.
	DefaultBatchMaxConcurrent = 8
)

// BatchLease is one pinned finalized view with one certification attachment.
// *proofview.Lease satisfies it.
type BatchLease interface {
	Context() context.Context
	Root() []byte
	Block() proofview.BlockIdentity
	UC() []byte
	PartitionView() []byte
	GetInclusionCert(key []byte) (*api.InclusionCert, error)
	Release()
}

// BatchViewSource selects a view only by "latest" or exact root.
type BatchViewSource interface {
	AcquireLatest(ctx context.Context) (BatchLease, error)
	AcquireRoot(ctx context.Context, root []byte) (BatchLease, error)
}

// CatalogViewSource adapts a proofview.Catalog.
func CatalogViewSource(c *proofview.Catalog) BatchViewSource { return catalogSource{c} }

type catalogSource struct{ c *proofview.Catalog }

func (s catalogSource) AcquireLatest(ctx context.Context) (BatchLease, error) {
	l, err := s.c.AcquireLatest(ctx)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (s catalogSource) AcquireRoot(ctx context.Context, root []byte) (BatchLease, error) {
	l, err := s.c.AcquireRoot(ctx, root)
	if err != nil {
		return nil, err
	}
	return l, nil
}

// BatchRecordSource loads finalized, immutable records by state ID. The
// storage's AggregatorRecordStorage satisfies it.
type BatchRecordSource interface {
	// GetStoredSizesByStateIDs returns the stored size of the finalized record of
	// each state ID (keyed by StateID.String(); absent when there is none)
	// without loading the records: one bounded server-side projection for the
	// whole batch. It is consulted before GetFinalizedByStateIDs so records that
	// cannot fit the response budget are never materialized.
	GetStoredSizesByStateIDs(ctx context.Context, stateIDs []api.StateID) (map[string]int64, error)
	// GetFinalizedByStateIDs loads the finalized records of the state IDs (keyed by
	// StateID.String(); absent when there is none): one query for the whole batch.
	GetFinalizedByStateIDs(ctx context.Context, stateIDs []api.StateID) (map[string]*models.AggregatorRecord, error)
}

// encodedSizeFactor converts a record's stored size to a conservative upper
// bound of its contribution to the serialized response (hex expansion of the
// CBOR proof element), and responseElementOverhead covers quoting, separator
// and the fixed proof fields.
const (
	encodedSizeFactor       = 2
	responseElementOverhead = 512
)

// BatchShardValidator checks that a state ID routes to this shard.
type BatchShardValidator interface {
	ValidateShardID(stateID api.StateID) error
}

// BatchError is a typed failure mapped to a JSON-RPC error by RPCError.
type BatchError struct {
	Code      int
	Message   string
	Reason    string
	Detail    string
	Retryable bool
	Limit     *int
	// Internal is the underlying cause of infrastructure errors; it is never
	// exposed to the client.
	Internal error
}

func (e *BatchError) Error() string {
	s := e.Reason + "/" + e.Detail
	if e.Internal != nil {
		s += ": " + e.Internal.Error()
	}
	return s
}

func (e *BatchError) Unwrap() error { return e.Internal }

// RPCError converts to the JSON-RPC error of plan §2.4.
func (e *BatchError) RPCError() *jsonrpc.Error {
	if e.Reason == "" { // infrastructure error: no database text, no detail
		return jsonrpc.NewError(e.Code, e.Message, nil)
	}
	return jsonrpc.NewDetailedError(e.Code, e.Message, e.Reason, e.Detail, e.Retryable, e.Limit)
}

func invalidParams(detail string) *BatchError {
	return &BatchError{Code: jsonrpc.InvalidParamsCode, Message: "Invalid params", Reason: "invalid_request", Detail: detail}
}

func unavailable(detail string) *BatchError {
	return &BatchError{Code: jsonrpc.ProofUnavailableCode, Message: "Proof unavailable", Reason: "proof_unavailable", Detail: detail, Retryable: true}
}

func busy(detail string) *BatchError {
	return &BatchError{Code: jsonrpc.ConcurrencyLimitCode, Message: "Concurrency limit exceeded", Reason: "busy", Detail: detail, Retryable: true}
}

func limitExceeded(detail string, limit int) *BatchError {
	return &BatchError{Code: jsonrpc.LimitExceededCode, Message: "Limit exceeded", Reason: "limit_exceeded", Detail: detail, Limit: &limit}
}

// ParamsBatchError maps a params decoding failure.
func ParamsBatchError(err error) *BatchError {
	var pe *api.InclusionProofsParamsError
	if errors.As(err, &pe) {
		if pe.IsLimit() {
			return limitExceeded(pe.Detail, pe.Limit)
		}
		return invalidParams(pe.Detail)
	}
	return invalidParams(api.DetailMalformedParams)
}

// BatchProofService serves get_inclusion_proofs.v1 from one pinned view.
type BatchProofService struct {
	views    BatchViewSource
	records  BatchRecordSource
	shard    BatchShardValidator
	slots    chan struct{}
	timeout  time.Duration
	maxBytes int
	mode     string
}

// BatchProofConfig configures the service. Zero values take the plan defaults.
type BatchProofConfig struct {
	MaxConcurrent int
	MaxExecution  time.Duration
	// MaxResponseBytes bounds the serialized response (estimated while building).
	MaxResponseBytes int
}

// NewBatchProofService creates the service.
func NewBatchProofService(views BatchViewSource, records BatchRecordSource, shard BatchShardValidator, cfg BatchProofConfig) *BatchProofService {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = DefaultBatchMaxConcurrent
	}
	if cfg.MaxExecution <= 0 {
		cfg.MaxExecution = BatchMaxExecution
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = api.InclusionProofsMaxResponseBytes
	}
	return &BatchProofService{views: views, records: records, shard: shard, slots: make(chan struct{}, cfg.MaxConcurrent),
		timeout: cfg.MaxExecution, maxBytes: cfg.MaxResponseBytes}
}

// GetInclusionProofsV1 returns proofs for every state ID, in request order,
// all from one pinned finalized view; any failure fails the whole call.
func (s *BatchProofService) GetInclusionProofsV1(ctx context.Context, req *api.GetInclusionProofsRequestV1) (*api.GetInclusionProofsResponseV1, *BatchError) {
	if req == nil || len(req.StateIDs) == 0 {
		return nil, invalidParams(api.DetailMalformedParams)
	}
	if len(req.StateIDs) > api.InclusionProofsMaxStateIDs {
		return nil, limitExceeded(api.DetailStateCount, api.InclusionProofsMaxStateIDs)
	}
	keys := make([][]byte, len(req.StateIDs))
	for i, id := range req.StateIDs {
		if len(id) != api.StateTreeKeyLengthBytes {
			return nil, invalidParams(api.DetailInvalidStateID)
		}
		key, err := id.GetTreeKey()
		if err != nil {
			return nil, invalidParams(api.DetailInvalidStateID)
		}
		keys[i] = key
		if err := s.shard.ValidateShardID(id); err != nil {
			return nil, invalidParams(api.DetailWrongShard)
		}
	}

	// bounded, nonblocking admission
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return nil, busy("active_requests")
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	latest := req.Root == nil
	var lease BatchLease
	var err error
	if latest {
		lease, err = s.views.AcquireLatest(ctx)
	} else {
		lease, err = s.views.AcquireRoot(ctx, req.Root)
	}
	if err != nil {
		return nil, acquireError(err)
	}
	defer lease.Release()

	// All database and proof work runs under a context cancelled by the request
	// (and its execution deadline) and by the lease, so pressure cancellation or
	// the lease deadline interrupts an in-flight database call too.
	work, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	stopWatch := context.AfterFunc(lease.Context(), cancelWork)
	defer stopWatch()
	ctx = work

	var view []byte
	if latest {
		if view = lease.PartitionView(); len(view) == 0 {
			// latest is the current certification; never fall back to an older one
			return nil, unavailable("partition_view_unavailable")
		}
	}
	blk := lease.Block()
	if blk.Number == nil {
		return nil, &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: errors.New("lease without block number")}
	}

	ucBytes := lease.UC()

	// Database access is constant per batch, independent of the number of IDs:
	// one size projection, then (only if the batch fits the response budget) one
	// record load. Both run under the request/lease context.
	if err := ctx.Err(); err != nil {
		return nil, ctxError(err)
	}
	sizes, serr := s.records.GetStoredSizesByStateIDs(ctx, req.StateIDs)
	if serr != nil {
		return nil, dbError(ctx, serr)
	}
	// Preflight in request order: a missing record or an over-budget estimate is
	// reported before any record is loaded.
	budget := int64(len(view)*2 + 128)
	for _, id := range req.StateIDs {
		stored, ok := sizes[id.String()]
		if !ok {
			return nil, unavailable("state_not_included")
		}
		budget += stored*encodedSizeFactor + responseElementOverhead
		if stored < 0 || budget > int64(s.maxBytes) {
			return nil, limitExceeded(api.DetailResponseBytes, s.maxBytes)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, ctxError(err)
	}
	records, lerr := s.records.GetFinalizedByStateIDs(ctx, req.StateIDs)
	if lerr != nil {
		return nil, dbError(ctx, lerr)
	}

	proofs := make([]*api.GetInclusionProofResponseV2, len(keys))
	size := len(view)*2 + 128
	lctx := lease.Context()
	for i, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, ctxError(err)
		}
		if err := lctx.Err(); err != nil {
			return nil, ctxError(err)
		}
		p, berr := s.buildProof(ctx, lease, records[req.StateIDs[i].String()], key, blk, ucBytes)
		if berr != nil {
			return nil, berr
		}
		proofs[i] = p
		// hex expansion of the CBOR element plus quoting and separator
		if enc, err := types.Cbor.Marshal(p); err == nil {
			size += len(enc)*2 + 4
		}
		if size > s.maxBytes {
			return nil, limitExceeded(api.DetailResponseBytes, s.maxBytes)
		}
	}
	return &api.GetInclusionProofsResponseV1{Proofs: proofs, PartitionView: view}, nil
}

func dbError(ctx context.Context, err error) *BatchError {
	if cerr := ctx.Err(); cerr != nil {
		return ctxError(cerr)
	}
	return &BatchError{Code: jsonrpc.DatabaseErrorCode, Message: "Database error", Internal: err}
}

func (s *BatchProofService) buildProof(ctx context.Context, lease BatchLease, rec *models.AggregatorRecord, key []byte, blk proofview.BlockIdentity, ucBytes []byte) (*api.GetInclusionProofResponseV2, *BatchError) {
	// the record must be a finalized state of the pinned view
	if rec == nil || rec.BlockNumber == nil || blk.Number == nil || rec.BlockNumber.Cmp(blk.Number.Int) > 0 {
		return nil, unavailable("state_not_included")
	}
	if rec.Version != 2 {
		return nil, &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: fmt.Errorf("aggregator record version %d, want 2", rec.Version)}
	}
	cert, err := lease.GetInclusionCert(key)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, ctxError(cerr)
		}
		if cerr := lease.Context().Err(); cerr != nil {
			return nil, ctxError(cerr)
		}
		if errors.Is(err, backend.ErrPublishedProofLeafNotFound) || errors.Is(err, smt.ErrLeafNotFound) {
			// the record is finalized but its leaf is not in the pinned root
			return nil, unavailable("state_not_included")
		}
		return nil, &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: fmt.Errorf("inclusion cert: %w", err)}
	}
	// the leaf must be in the pinned root with the stored data and reference time
	cd := rec.CertificationData.ToAPI()
	if len(cd.TransactionHash) == 0 {
		return nil, &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: errors.New("record without transaction hash")}
	}
	if err := cert.Verify(key, api.LeafValue(cd.TransactionHash.DataBytes(), rec.ReferenceTime), lease.Root(), api.InclusionProofV2HashAlgorithm); err != nil {
		return nil, unavailable("state_not_included")
	}
	certBytes, err := cert.MarshalBinary()
	if err != nil {
		return nil, &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: err}
	}
	rt := rec.ReferenceTime
	return &api.GetInclusionProofResponseV2{
		BlockNumber: blk.Number.Uint64(),
		InclusionProof: &api.InclusionProofV2{
			CertificationData:  cd,
			ReferenceTime:      &rt,
			CertificateBytes:   certBytes,
			UnicityCertificate: types.RawCBOR(ucBytes),
		},
	}, nil
}

func ctxError(err error) *BatchError {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return busy("deadline")
	}
	return &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: err}
}

func acquireError(err error) *BatchError {
	switch {
	case errors.Is(err, proofview.ErrNoFinalizedView):
		return unavailable("no_finalized_view")
	case errors.Is(err, proofview.ErrViewNotRetained):
		return unavailable("view_not_retained")
	case errors.Is(err, proofview.ErrBusyPins):
		return busy("pinned_views")
	case errors.Is(err, proofview.ErrBusyBudget):
		return busy("retention_budget")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return busy("deadline")
	}
	return &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: err}
}

// UnsupportedConfiguration is the error for deployments the method does not
// serve (parent/child aggregation).
func UnsupportedConfiguration() *BatchError {
	return unavailable("unsupported_configuration").nonRetryable()
}

func (e *BatchError) nonRetryable() *BatchError { e.Retryable = false; return e }

// HandleRPC is the JSON-RPC handler of get_inclusion_proofs.v1.
func (s *BatchProofService) HandleRPC(ctx context.Context, params json.RawMessage) (interface{}, *jsonrpc.Error) {
	req, err := api.ParseGetInclusionProofsRequestV1(params)
	if err != nil {
		return nil, ParamsBatchError(err).RPCError()
	}
	resp, berr := s.GetInclusionProofsV1(ctx, req)
	if berr != nil {
		return nil, berr.RPCError()
	}
	return resp, nil
}

// UnsupportedRPC is the handler for deployments that cannot serve the method.
func UnsupportedRPC(context.Context, json.RawMessage) (interface{}, *jsonrpc.Error) {
	return nil, UnsupportedConfiguration().RPCError()
}
