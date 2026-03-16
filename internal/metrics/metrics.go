package metrics

import (
	"context"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// knownMethods is the complete set of registered JSON-RPC method names.
// Any method name not in this set is normalized to "unknown" to prevent
// unbounded label cardinality from arbitrary client input.
var knownMethods = map[string]struct{}{
	"submit_commitment":      {},
	"certification_request":  {},
	"get_inclusion_proof":    {},
	"get_inclusion_proof.v2": {},
	"get_no_deletion_proof":  {},
	"get_block_height":       {},
	"get_block":              {},
	"get_block_commitments":  {},
	"get_block_records":      {},
	"submit_shard_root":      {},
	"get_shard_proof":        {},
}

// NormalizeMethod returns the method name if it is a known RPC method,
// otherwise returns "unknown".
func NormalizeMethod(method string) string {
	if _, ok := knownMethods[method]; ok {
		return method
	}
	return "unknown"
}

var (
	commitmentQueueBacklogMu       sync.RWMutex
	commitmentQueueBacklogCountFn  func(context.Context) (int64, error)
	commitmentQueueBacklogLastRead atomic.Int64

	smtNodesPersistedMu       sync.RWMutex
	smtNodesPersistedCountFn  func(context.Context) (int64, error)
	smtNodesPersistedLastRead atomic.Int64
)

// SetCommitmentQueueBacklogFunc registers a callback used by the commitment
// backlog GaugeFunc. The callback runs on Prometheus scrape.
func SetCommitmentQueueBacklogFunc(fn func(context.Context) (int64, error)) {
	commitmentQueueBacklogMu.Lock()
	defer commitmentQueueBacklogMu.Unlock()
	commitmentQueueBacklogCountFn = fn
}

func getCommitmentQueueBacklog() float64 {
	commitmentQueueBacklogMu.RLock()
	fn := commitmentQueueBacklogCountFn
	commitmentQueueBacklogMu.RUnlock()

	if fn == nil {
		return float64(commitmentQueueBacklogLastRead.Load())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	count, err := fn(ctx)
	if err != nil || count < 0 {
		return float64(commitmentQueueBacklogLastRead.Load())
	}

	commitmentQueueBacklogLastRead.Store(count)
	return float64(count)
}

// SetSMTNodesPersistedCountFunc registers a callback used by the SMT persisted
// nodes GaugeFunc. The callback runs on Prometheus scrape, not on finalize path.
func SetSMTNodesPersistedCountFunc(fn func(context.Context) (int64, error)) {
	smtNodesPersistedMu.Lock()
	defer smtNodesPersistedMu.Unlock()
	smtNodesPersistedCountFn = fn
}

func getSMTNodesPersistedCount() float64 {
	smtNodesPersistedMu.RLock()
	fn := smtNodesPersistedCountFn
	smtNodesPersistedMu.RUnlock()

	if fn == nil {
		return float64(smtNodesPersistedLastRead.Load())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	count, err := fn(ctx)
	if err != nil || count < 0 {
		return float64(smtNodesPersistedLastRead.Load())
	}

	smtNodesPersistedLastRead.Store(count)
	return float64(count)
}

// SetBlockHeight updates the current finalized block height gauge.
func SetBlockHeight(height *big.Int) {
	if height == nil {
		return
	}
	v, _ := new(big.Float).SetInt(height).Float64()
	BlockHeight.Set(v)
}

var (
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aggregator_http_requests_total",
			Help: "Total JSON-RPC requests by method and result.",
		},
		[]string{"method", "result"},
	)

	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aggregator_http_request_duration_seconds",
			Help:    "JSON-RPC request latency by method.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	BlockCreationDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aggregator_block_creation_duration_seconds",
			Help:    "Time from round start to block finalized.",
			Buckets: []float64{1.0, 1.1, 1.2, 1.3, 1.5, 1.8, 2.2, 2.6, 3.0},
		},
	)

	BlockHeight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "aggregator_block_height",
			Help: "Latest finalized block height.",
		},
	)

	RoundCommitments = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aggregator_round_commitments",
			Help:    "Commitments processed per round.",
			Buckets: []float64{0, 1, 10, 100, 500, 1000, 5000, 10000, 50000, 100000},
		},
	)

	CommitmentQueueBacklog = promauto.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "aggregator_commitment_queue_backlog",
			Help: "Total unprocessed commitments in Redis consumer group (pending + lag).",
		},
		getCommitmentQueueBacklog,
	)

	CommitmentsProcessedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aggregator_commitments_processed_total",
			Help: "Cumulative commitments finalized in blocks.",
		},
	)

	BFTCertificationDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aggregator_bft_certification_duration_seconds",
			Help:    "Time from BFT proposal to UC received.",
			Buckets: []float64{1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0},
		},
	)

	BFTErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aggregator_bft_errors_total",
			Help: "BFT certification failures.",
		},
	)

	RoundPreparationDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aggregator_round_preparation_duration_seconds",
			Help:    "Standalone/parent: time from round start to BFT proposal sent.",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10},
		},
	)

	RoundFinalizationDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aggregator_round_finalization_duration_seconds",
			Help:    "All modes: time from proof/UC received to block stored and SMT snapshot committed.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1},
		},
	)

	ParentRootSubmissionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aggregator_parent_root_submission_duration_seconds",
			Help:    "Child: time to submit shard root hash to parent aggregator.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		},
	)

	ParentProofWaitDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aggregator_parent_proof_wait_duration_seconds",
			Help:    "Child: time from root submitted to inclusion proof received from parent.",
			Buckets: []float64{.1, .25, .5, 1, 2, 5, 10, 30},
		},
	)

	ProofReadinessDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aggregator_proof_readiness_seconds",
			Help:    "Commitment proof-readiness time (commitment created -> block finalized).",
			Buckets: []float64{.25, .5, .75, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.5, 3.0, 4.0, 5.0, 10.0},
		},
	)

	ProofReadinessMedian = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "aggregator_proof_readiness_median_seconds",
			Help: "Per-round median commitment proof-readiness time (commitment created -> block finalized).",
		},
	)

	ProofReadinessP95 = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "aggregator_proof_readiness_p95_seconds",
			Help: "Per-round p95 commitment proof-readiness time (commitment created -> block finalized).",
		},
	)

	ParentProofErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aggregator_parent_proof_errors_total",
			Help: "Child: parent proof timeouts and errors.",
		},
	)

	SMTAddLeavesDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aggregator_smt_addleaves_duration_seconds",
			Help:    "SMT batch update latency.",
			Buckets: prometheus.DefBuckets,
		},
	)

	SMTNodesPersistedTotal = promauto.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "aggregator_smt_nodes_persisted_total",
			Help: "Approximate persisted SMT node count from MongoDB EstimatedDocumentCount.",
		},
		getSMTNodesPersistedCount,
	)

	IsLeader = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "aggregator_is_leader",
			Help: "1 if this node is the HA leader, else 0.",
		},
	)

	LeaderTransitionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aggregator_leader_transitions_total",
			Help: "Total leadership changes (gain or loss).",
		},
	)
)
