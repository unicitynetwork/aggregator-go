package metrics

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Long-poll metrics. Every label is a small fixed set: no endpoint, IP, client or state-ID labels.

// LongPollGauges are the live occupancy numbers, read at scrape time.
type LongPollGauges struct {
	Reserved, Queued, Running int
}

var longPollGaugeSource atomic.Pointer[func() LongPollGauges]

// SetLongPollGaugeSource registers the occupancy reader (nil clears it).
func SetLongPollGaugeSource(f func() LongPollGauges) {
	if f == nil {
		longPollGaugeSource.Store(nil)
		return
	}
	longPollGaugeSource.Store(&f)
}

func longPollGauge(pick func(LongPollGauges) int) func() float64 {
	return func() float64 {
		if f := longPollGaugeSource.Load(); f != nil {
			return float64(pick((*f)()))
		}
		return 0
	}
}

var (
	_ = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "aggregator_longpoll_reservations",
		Help: "Admitted long-poll waits (parked, queued for resumption or resuming) against LONG_POLL_MAX_WAITERS.",
	}, longPollGauge(func(g LongPollGauges) int { return g.Reserved }))
	_ = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "aggregator_longpoll_resume_queued",
		Help: "Long-poll waits queued for active-work capacity to re-read.",
	}, longPollGauge(func(g LongPollGauges) int { return g.Queued }))
	_ = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "aggregator_longpoll_resume_running",
		Help: "Resumed long-poll reads currently running (bounded by max(1, C/4)).",
	}, longPollGauge(func(g LongPollGauges) int { return g.Running }))

	// LongPollRequestsTotal counts requests which carried waitMs by how they ended:
	// immediate_zero (waitMs 0), immediate_no_budget, shed_capacity, shed_draining,
	// immediate_final (the first read already decided), completed (parked, then decided),
	// timeout (parked until the cutoff, final read), drained, cancelled, busy (resume not served).
	LongPollRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aggregator_longpoll_requests_total",
		Help: "Requests carrying waitMs by outcome.",
	}, []string{"method", "outcome"})

	// LongPollWakesTotal counts why parked waits woke: signal, changed, timeout, cancel, drain.
	LongPollWakesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aggregator_longpoll_wakes_total",
		Help: "Wake causes of parked long-poll waits.",
	}, []string{"cause"})

	LongPollRechecksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aggregator_longpoll_rechecks_total",
		Help: "Re-reads performed by long-poll waits after a wake.",
	}, []string{"method"})

	LongPollNotifiesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "aggregator_longpoll_notifies_total",
		Help: "Wake-up events raised by publication, expiry and lifecycle sources.",
	})

	LongPollWaitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "aggregator_longpoll_wait_seconds",
		Help:    "Cumulative time a request spent waiting (the waitedMs echo).",
		Buckets: []float64{0.01, 0.05, 0.25, 1, 2.5, 5, 10, 20, 30},
	}, []string{"method"})

	LongPollResumeQueueSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "aggregator_longpoll_resume_queue_seconds",
		Help:    "Time a woken wait spent queued for active-work capacity.",
		Buckets: []float64{0.001, 0.005, 0.025, 0.1, 0.5, 1, 2, 5},
	})

	LongPollPublicationToResponseSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "aggregator_longpoll_publication_to_response_seconds",
		Help:    "From the last wake-up event to the end of the read it triggered.",
		Buckets: []float64{0.001, 0.005, 0.025, 0.1, 0.5, 1, 2, 5},
	}, []string{"method"})
)
