package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
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

// pinReserver is implemented by view sources whose leases are limited (the catalog): the resume
// scheduler sets a pin aside together with the batch slot, and the read converts it into its lease.
type pinReserver interface {
	TryReservePin(leave int) (*proofview.PinReservation, bool)
	AcquireLatestReserved(ctx context.Context, r *proofview.PinReservation) (BatchLease, error)
	PinLimit() int
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

func (s catalogSource) TryReservePin(leave int) (*proofview.PinReservation, bool) {
	return s.c.TryReservePin(leave)
}

func (s catalogSource) AcquireLatestReserved(ctx context.Context, r *proofview.PinReservation) (BatchLease, error) {
	l, err := s.c.AcquireLatestReserved(ctx, r)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (s catalogSource) PinLimit() int { return s.c.Limits().MaxConcurrentPins }

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
	// Indexes lists offending request indexes per class (state status errors).
	Indexes map[string][]int
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
	if len(e.Indexes) > 0 {
		return jsonrpc.NewDetailedErrorWithIndexes(e.Code, e.Message, e.Reason, e.Detail, e.Retryable, e.Indexes)
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
	status   BatchStatusSource
	longPoll *longpoll.Coordinator

	// resume admission (see TryAdmit)
	admitMu     sync.Mutex
	classOnce   sync.Once
	class       *longpoll.Class
	releaseHook atomic.Pointer[func()]

	// test hooks (nil in production)
	testHookAdmitChecked func() // in TryAdmit after the headroom check, under admitMu
	testHookFreshSlot    func() // in a fresh read right after it took its slot (outside admitMu), before its pin
}

// BatchProofConfig configures the service. Zero values take the plan defaults.
type BatchProofConfig struct {
	MaxConcurrent int
	MaxExecution  time.Duration
	// MaxResponseBytes bounds the serialized response (estimated while building).
	MaxResponseBytes int
	// Status answers the unauthenticated pending/expired/unknown hint of a state ID from
	// memory (the round manager). Nil answers unknown for every absent state.
	Status BatchStatusSource
	// LongPoll serves requests carrying waitMs; nil keeps the reservation-only behavior (waitMs is
	// validated and ignored, no echo).
	LongPoll *longpoll.Coordinator
}

// BatchStatusSource is the O(1) in-memory proof status lookup.
type BatchStatusSource interface {
	ProofStatus(stateID api.StateID) api.ProofStatus
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
		timeout: cfg.MaxExecution, maxBytes: cfg.MaxResponseBytes, status: cfg.Status, longPoll: cfg.LongPoll}
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

	// Whatever the read releases - slot, lease - may unblock a queued resumption: wake the
	// scheduler last, after the resources are back (registered first, so it runs last).
	defer s.fireRelease()

	// bounded, nonblocking admission; a resumed read arrives with the slot and the pin the
	// scheduler already set aside for it and does not go through a second admission
	granted, _ := longpoll.ResourcesFromContext(ctx).(*batchResources)
	if granted != nil && granted.svc != s {
		granted = nil
	}
	if granted != nil && granted.takeSlot() {
		defer func() { <-s.slots }()
	} else {
		if !s.acquireSlot() {
			return nil, busy("active_requests")
		}
		defer func() { <-s.slots }()
		if h := s.testHookFreshSlot; h != nil {
			h()
		}
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	latest := req.Root == nil
	var lease BatchLease
	var err error
	if latest {
		if pr := granted.takePin(); pr != nil {
			defer pr.Release() // a no-op once converted into the lease
			lease, err = s.views.(pinReserver).AcquireLatestReserved(ctx, pr)
		} else {
			lease, err = s.views.AcquireLatest(ctx)
		}
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
		// The decision is made on the admitted UC first: the UC of an unsharded partition is complete
		// by itself and is served without a partition view even if the catalog holds one (a sharded UC
		// always needs its view; latest is the current certification, never an older one).
		if !isUnshardedUC(lease.UC()) {
			if view = lease.PartitionView(); len(view) == 0 {
				return nil, unavailable("partition_view_unavailable")
			}
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
	// Status hints are snapshotted before the record query: a state which finalizes while the
	// request runs was pending when read (retryable) or has a record, and is never classified
	// from a hint cleared by its own finalization (state_unknown is not retryable).
	var hints []api.ProofStatus
	if latest && s.status != nil {
		hints = make([]api.ProofStatus, len(req.StateIDs))
		for i, id := range req.StateIDs {
			hints[i] = s.status.ProofStatus(id)
		}
	}
	sizes, serr := s.records.GetStoredSizesByStateIDs(ctx, req.StateIDs)
	if serr != nil {
		return nil, dbError(ctx, serr)
	}
	// States without a stored finalized record are classified from the one size projection
	// already in hand. Every other state is still examined, so that the error lists all
	// offending indexes; that costs the one record load of the successful path, never more:
	// the batch is at most two database commands.
	present := make([]api.StateID, 0, len(req.StateIDs))
	presentKeys := make([][]byte, 0, len(req.StateIDs))
	for i, id := range req.StateIDs {
		if _, ok := sizes[id.String()]; ok {
			present = append(present, id)
			presentKeys = append(presentKeys, keys[i])
		}
	}
	// Preflight in request order: an over-budget estimate is reported before any record
	// is loaded.
	budget := int64(len(view)*2 + 128)
	for _, id := range present {
		stored := sizes[id.String()]
		budget += stored*encodedSizeFactor + responseElementOverhead
		if stored < 0 || budget > int64(s.maxBytes) {
			return nil, limitExceeded(api.DetailResponseBytes, s.maxBytes)
		}
	}
	if len(present) == 0 {
		return nil, s.absentError(req, latest, hints, allIndexes(len(req.StateIDs)), nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, ctxError(err)
	}
	records, lerr := s.records.GetFinalizedByStateIDs(ctx, present)
	if lerr != nil {
		return nil, dbError(ctx, lerr)
	}

	// Proofs of the whole batch come from the one pinned lease in a single pass.
	lease, perr := prebuildCerts(ctx, lease, presentKeys)
	if perr != nil {
		return nil, perr
	}

	proofs := make([]*api.GetInclusionProofResponseV2, len(keys))
	var noRecord, absent []int
	size := len(view)*2 + 128
	lctx := lease.Context()
	for i, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, ctxError(err)
		}
		if err := lctx.Err(); err != nil {
			return nil, ctxError(err)
		}
		rec := records[req.StateIDs[i].String()]
		if rec == nil {
			noRecord = append(noRecord, i) // no stored finalized record (not in the projection, or not finalized)
			continue
		}
		p, berr := s.buildProof(ctx, lease, rec, key, blk, ucBytes)
		if berr == errAbsentFromView {
			absent = append(absent, i) // keep going: every offending index is reported
			continue
		}
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
	if len(noRecord) > 0 || len(absent) > 0 {
		return nil, s.absentError(req, latest, hints, noRecord, absent)
	}
	return &api.GetInclusionProofsResponseV1{Proofs: proofs, PartitionView: view}, nil
}

// errAbsentFromView is the internal marker of a state which has no proof in the pinned view.
var errAbsentFromView = &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: errors.New("state absent from the pinned view")}

// Status details of get_inclusion_proofs.v1 for states without a proof.
const (
	detailStatePending   = "state_pending"
	detailStateExpired   = "state_expired"
	detailStateUnknown   = "state_unknown"
	detailStateNotInRoot = "state_not_in_root"
)

func allIndexes(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// absentError turns the states without a proof into the typed error of the request kind.
//
// Exact-root requests: every such state is absent from that retained root (state_not_in_root,
// not retryable). Latest requests: noRecord are states without a stored finalized record, and
// absentInView have a record which the pinned view does not contain yet; each is classified from
// the in-memory status hint snapshotted before the record query. A state with a stored finalized record is always pending (finalized
// after the pinned view), whatever the hint says. With mixed outcomes unknown takes precedence over
// expired, which takes precedence over pending; only pending is retryable. The error data lists
// the offending request indexes per class.
func (s *BatchProofService) absentError(req *api.GetInclusionProofsRequestV1, latest bool, hints []api.ProofStatus, noRecord, absentInView []int) *BatchError {
	if !latest {
		idx := append(append([]int(nil), noRecord...), absentInView...)
		sort.Ints(idx)
		metrics.ProofStatusServedTotal.WithLabelValues("get_inclusion_proofs.v1", "not_in_root").Add(float64(len(idx)))
		e := unavailable(detailStateNotInRoot).nonRetryable()
		e.Indexes = map[string][]int{"not_in_root": idx}
		return e
	}
	classes := map[string][]int{}
	classify := func(i int, hasRecord bool) {
		// A stored finalized record means the state was certified, whatever the (volatile,
		// advisory) status memory says: it is only missing from the pinned view, so retry.
		st := api.ProofStatusPending
		if !hasRecord {
			st = api.ProofStatusUnknown
			if hints != nil {
				st = hints[i]
			}
		}
		classes[string(st)] = append(classes[string(st)], i)
	}
	for _, i := range noRecord {
		classify(i, false)
	}
	for _, i := range absentInView {
		classify(i, true)
	}
	for k := range classes {
		sort.Ints(classes[k])
		metrics.ProofStatusServedTotal.WithLabelValues("get_inclusion_proofs.v1", k).Add(float64(len(classes[k])))
	}
	var e *BatchError
	switch {
	case len(classes[string(api.ProofStatusUnknown)]) > 0:
		e = unavailable(detailStateUnknown).nonRetryable()
	case len(classes[string(api.ProofStatusExpired)]) > 0:
		e = unavailable(detailStateExpired).nonRetryable()
	default:
		e = unavailable(detailStatePending) // retryable
	}
	e.Indexes = classes
	return e
}

// isUnshardedUC reports whether the (admitted: the catalog publishes only authenticated
// certifications) UC is structurally that of an unsharded partition: empty shard ID and zero
// shard-tree siblings, so its shard tree is a single leaf and there are no peers whose roots a
// partition view would have to carry. Anything malformed or sharded is not (the view stays required).
func isUnshardedUC(raw []byte) bool {
	var uc types.UnicityCertificate
	if len(raw) == 0 || types.Cbor.Unmarshal(raw, &uc) != nil {
		return false
	}
	if uc.InputRecord == nil || uc.UnicitySeal == nil || uc.UnicityTreeCertificate == nil {
		return false
	}
	return uc.ShardTreeCertificate.Shard.Length() == 0 && len(uc.ShardTreeCertificate.SiblingHashes) == 0
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
		return nil, errAbsentFromView
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
			return nil, errAbsentFromView
		}
		return nil, &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: fmt.Errorf("inclusion cert: %w", err)}
	}
	// the leaf must be in the pinned root with the stored data and reference time
	cd := rec.CertificationData.ToAPI()
	if len(cd.TransactionHash) == 0 {
		return nil, &BatchError{Code: jsonrpc.InternalErrorCode, Message: "Internal error", Internal: errors.New("record without transaction hash")}
	}
	if err := cert.Verify(key, api.LeafValue(cd.TransactionHash.DataBytes(), rec.ReferenceTime), lease.Root(), api.InclusionProofV2HashAlgorithm); err != nil {
		return nil, errAbsentFromView
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
		rpcErr := ParamsBatchError(err).RPCError()
		if s.longPoll != nil {
			// A params object carrying a valid waitMs is a recognized wait-bearing request: its
			// validation errors echo zero (and tell the client the server understands waitMs).
			// An invalid or duplicate waitMs is not recognized: the same bytes as without support.
			if w, serr := api.ScanWaitMs(params); serr == nil && w.Present {
				rpcErr = rpcErr.WithWaited(0)
			}
		}
		return nil, rpcErr
	}
	if !req.WaitMs.Present || s.longPoll == nil {
		resp, berr := s.GetInclusionProofsV1(ctx, req)
		if berr != nil {
			return nil, berr.RPCError()
		}
		return resp, nil
	}
	resumeCap, canPark := s.resumeCapacity()
	wait := req.WaitMs.Value
	if !canPark {
		wait = 0 // B or P below two: ordinary immediate read with a zero echo
	} else {
		s.classOnce.Do(func() {
			s.class = s.longPoll.Reg.NewClass("batch", resumeCap, s)
			s.SetReleaseHook(s.longPoll.Reg.Schedule)
		})
	}
	res, waited := s.longPoll.RunClass(ctx, BatchProofsMethod, wait, s.class, func(ctx context.Context) longpoll.Result {
		resp, berr := s.GetInclusionProofsV1(ctx, req)
		if berr != nil {
			// Wait only while every non-included state is pending (the batch is all-or-nothing and
			// unknown/expired end the wait at once, the normal error precedence applies).
			pending := berr.Reason == "proof_unavailable" && berr.Detail == detailStatePending
			return longpoll.Result{Pending: pending, Err: berr.RPCError()}
		}
		return longpoll.Result{Value: resp}
	})
	if res.Err != nil {
		return nil, res.Err.WithWaited(waited)
	}
	resp := res.Value.(*api.GetInclusionProofsResponseV1)
	resp.WaitedMs = &waited
	return resp, nil
}

// batchResources is the batch capacity granted to one resumed read: a slot and a pin, set aside
// together by TryAdmit. The read consumes what it uses; Release returns what is left.
type batchResources struct {
	svc  *BatchProofService
	mu   sync.Mutex
	slot bool
	pin  *proofview.PinReservation
}

func (b *batchResources) takeSlot() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.slot {
		return false
	}
	b.slot = false
	return true
}

func (b *batchResources) takePin() *proofview.PinReservation {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.pin
	b.pin = nil
	return p
}

// Release returns the unused slot and pin (idempotent).
func (b *batchResources) Release() {
	b.mu.Lock()
	slot, pin := b.slot, b.pin
	b.slot, b.pin = false, nil
	b.mu.Unlock()
	if slot {
		<-b.svc.slots
	}
	pin.Release()
	if slot || pin != nil {
		b.svc.fireRelease()
	}
}

// acquireSlot is the fresh requests' non-blocking slot acquisition. It takes admitMu - the same
// short critical section in which TryAdmit checks the headroom and takes its slot - so that a
// resumption's "two slots are free" observation cannot be invalidated by a fresh read between
// the check and the take (the fresh read would then lose its guaranteed last slot).
func (s *BatchProofService) acquireSlot() bool {
	s.admitMu.Lock()
	defer s.admitMu.Unlock()
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// TryAdmit grants the batch capacity of one resumed read without blocking: a batch slot and a
// pin, together, leaving at least one of each free for fresh requests, or nothing (any partial
// grant is rolled back). It implements longpoll.Admitter; the scheduler calls it under its lock,
// so it takes no registry lock and never waits.
func (s *BatchProofService) TryAdmit() (longpoll.Resources, bool) {
	s.admitMu.Lock()
	defer s.admitMu.Unlock()
	if cap(s.slots)-len(s.slots) < 2 {
		return nil, false
	}
	if h := s.testHookAdmitChecked; h != nil {
		h() // tests: a fresh read started here must wait for this critical section
	}
	var pin *proofview.PinReservation
	if pr, ok := s.views.(pinReserver); ok {
		p, ok := pr.TryReservePin(1)
		if !ok {
			return nil, false
		}
		pin = p
	}
	select {
	case s.slots <- struct{}{}:
	default:
		pin.Release()
		return nil, false
	}
	return &batchResources{svc: s, slot: true, pin: pin}, true
}

// SetReleaseHook registers the function called after batch capacity (a slot, a pin) was
// released, so that queued resumptions get scheduled; the long-poll wiring sets the registry's.
func (s *BatchProofService) SetReleaseHook(f func()) {
	if f == nil {
		s.releaseHook.Store(nil)
		return
	}
	s.releaseHook.Store(&f)
}

func (s *BatchProofService) fireRelease() {
	if f := s.releaseHook.Load(); f != nil {
		(*f)()
	}
}

// resumeCapacity reports whether batch reads can be resumed at all and the cap of simultaneously
// resumed batch reads inside the global quota: min(B-1, P-1) for batch concurrency B and pin
// limit P (the global max(1, C/4) is applied by the registry). With B or P below two, parking is
// disabled: a resumption could not leave capacity to fresh requests.
func (s *BatchProofService) resumeCapacity() (n int, ok bool) {
	b, p := cap(s.slots), 1<<30
	if pr, isPR := s.views.(pinReserver); isPR {
		p = pr.PinLimit()
	}
	if b < 2 || p < 2 {
		return 0, false
	}
	return min(b-1, p-1), true
}

// BatchProofsMethod is the JSON-RPC method of the batch inclusion proof API.
const BatchProofsMethod = "get_inclusion_proofs.v1"

// UnsupportedRPC is the handler for deployments that cannot serve the method.
func UnsupportedRPC(context.Context, json.RawMessage) (interface{}, *jsonrpc.Error) {
	return nil, UnsupportedConfiguration().RPCError()
}
