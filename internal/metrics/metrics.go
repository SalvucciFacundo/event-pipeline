// Package metrics exposes low-cardinality Prometheus metrics for the
// event pipeline: ingested/processed event counters, an active-workers
// gauge, and an ingestion latency histogram.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metric label values are bounded by construction: event type and worker
// slot are validated upstream. Never label by payload, client ID, or
// Redis entry ID.
const (
	labelEventType = "type"
	labelWorker    = "worker"
	labelRoute     = "route"
)

// Metrics bundles the service's Prometheus collectors behind one registry.
type Metrics struct {
	registry *prometheus.Registry

	// EventsIngested counts events accepted by POST /events, by type.
	EventsIngested *prometheus.CounterVec
	// EventsProcessed counts events handled by workers, by type and worker.
	EventsProcessed *prometheus.CounterVec
	// ActiveWorkers is the current number of running workers.
	ActiveWorkers prometheus.Gauge
	// IngestionLatency measures POST /events handler latency by route.
	IngestionLatency *prometheus.HistogramVec
}

// New creates a Metrics with a fresh registry. The caller must not share
// the registry with another Metrics instance.
func New(registry *prometheus.Registry) *Metrics {
	m := &Metrics{
		registry: registry,
		EventsIngested: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "event_pipeline_events_ingested_total",
				Help: "Total events accepted by the ingestion endpoint, by type.",
			},
			[]string{labelEventType},
		),
		EventsProcessed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "event_pipeline_events_processed_total",
				Help: "Total events processed by workers, by type and worker.",
			},
			[]string{labelEventType, labelWorker},
		),
		ActiveWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "event_pipeline_active_workers",
			Help: "Current number of running workers.",
		}),
		IngestionLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "event_pipeline_ingestion_latency_seconds",
				Help:    "Ingestion handler latency in seconds, by route.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{labelRoute},
		),
	}

	registry.MustRegister(
		m.EventsIngested,
		m.EventsProcessed,
		m.ActiveWorkers,
		m.IngestionLatency,
	)

	return m
}

// Ingested increments the ingested counter for the given event type.
func (m *Metrics) Ingested(eventType string) {
	m.EventsIngested.WithLabelValues(eventType).Inc()
}

// Processed increments the processed counter for the given event type
// and worker slot.
func (m *Metrics) Processed(eventType, worker string) {
	m.EventsProcessed.WithLabelValues(eventType, worker).Inc()
}

// Handler returns the Prometheus text exposition handler for /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}