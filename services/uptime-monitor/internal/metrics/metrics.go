// Package metrics defines the Prometheus instrumentation shared across all
// services. Keeping every metric definition in one place avoids name/label
// collisions between the scheduler, prober, and result processor when
// they're scraped from the same Prometheus instance.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	ChecksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "uptime_checks_total",
		Help: "Total checks executed, labeled by region and outcome.",
	}, []string{"region", "outcome"}) // outcome: success|failure|breaker_skip

	CheckDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "uptime_check_duration_seconds",
		Help: "Check execution duration in seconds.",
		// Tuned for typical HTTP check latencies (tens of ms to tens of seconds).
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30},
	}, []string{"region"})

	AdaptiveTimeoutSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "uptime_adaptive_timeout_seconds",
		Help:    "Timeout value selected by the adaptive-timeout tracker for each check.",
		Buckets: []float64{0.5, 1, 2, 5, 10, 15, 20, 30},
	}, []string{"region"})

	WorkerPoolInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "uptime_worker_pool_in_flight",
		Help: "Currently executing checks per worker-pool lane.",
	}, []string{"lane"}) // lane: main|quarantine

	WorkerPoolCapacity = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "uptime_worker_pool_capacity",
		Help: "Configured concurrency capacity per worker-pool lane.",
	}, []string{"lane"})

	WorkerPoolDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "uptime_worker_pool_dropped_total",
		Help: "Jobs dropped because a worker-pool lane was saturated (backpressure onto the queue).",
	}, []string{"lane"})

	CircuitBreakerOpenCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "uptime_circuit_breaker_open_count",
		Help: "Number of monitors currently in an open circuit-breaker state on this probe.",
	})

	SchedulerOwnedMonitors = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "uptime_scheduler_owned_monitors",
		Help: "Number of monitors currently owned (scheduled) by this scheduler replica.",
	})

	JetStreamPublishErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "uptime_jetstream_publish_errors_total",
		Help: "Errors publishing to JetStream, labeled by stream.",
	}, []string{"stream"})

	ResultBatchWriteSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "uptime_result_batch_write_seconds",
		Help:    "Duration of a batched check_results write to Postgres.",
		Buckets: prometheus.DefBuckets,
	})

	ResultBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "uptime_result_batch_size",
		Help:    "Number of rows in each batched check_results write.",
		Buckets: []float64{1, 10, 50, 100, 250, 500, 1000, 2000},
	})

	IncidentsOpenedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "uptime_incidents_opened_total",
		Help: "Incidents opened, labeled by region.",
	}, []string{"region"})

	IncidentsResolvedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "uptime_incidents_resolved_total",
		Help: "Incidents resolved, labeled by region.",
	}, []string{"region"})

	NotificationsSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "uptime_notifications_sent_total",
		Help: "Notifications sent, labeled by channel type and result.",
	}, []string{"channel_type", "result"}) // result: success|failure
)

// Handler returns the /metrics HTTP handler for promhttp scraping.
func Handler() http.Handler {
	return promhttp.Handler()
}

// Serve starts a dedicated metrics HTTP server; run it in its own goroutine.
func Serve(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return http.ListenAndServe(addr, mux)
}
