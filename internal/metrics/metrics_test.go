package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func newTestMetrics(t *testing.T) *Metrics {
	t.Helper()
	reg := prometheus.NewRegistry()
	return New(reg)
}

func TestIngestedCounter(t *testing.T) {
	m := newTestMetrics(t)

	m.Ingested("click")
	m.Ingested("click")
	m.Ingested("view")

	got := testutil.ToFloat64(m.EventsIngested.WithLabelValues("click"))
	if got != 2 {
		t.Errorf("click ingested = %v, want 2", got)
	}
	got = testutil.ToFloat64(m.EventsIngested.WithLabelValues("view"))
	if got != 1 {
		t.Errorf("view ingested = %v, want 1", got)
	}
}

func TestProcessedCounter(t *testing.T) {
	m := newTestMetrics(t)

	m.Processed("click", "worker-1")
	m.Processed("click", "worker-1")
	m.Processed("click", "worker-2")

	got := testutil.ToFloat64(m.EventsProcessed.WithLabelValues("click", "worker-1"))
	if got != 2 {
		t.Errorf("worker-1 click processed = %v, want 2", got)
	}
	got = testutil.ToFloat64(m.EventsProcessed.WithLabelValues("click", "worker-2"))
	if got != 1 {
		t.Errorf("worker-2 click processed = %v, want 1", got)
	}
}

func TestActiveWorkersGauge(t *testing.T) {
	m := newTestMetrics(t)

	m.ActiveWorkers.Set(4)
	got := testutil.ToFloat64(m.ActiveWorkers)
	if got != 4 {
		t.Errorf("active workers = %v, want 4", got)
	}

	m.ActiveWorkers.Set(2)
	got = testutil.ToFloat64(m.ActiveWorkers)
	if got != 2 {
		t.Errorf("active workers = %v, want 2", got)
	}
}

func TestHandlerServesPrometheusText(t *testing.T) {
	m := newTestMetrics(t)
	m.Ingested("click")
	m.Processed("click", "worker-1")
	m.ActiveWorkers.Set(4)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("handler status = %d, want %d", rec.Code, http.StatusOK)
	}
	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	text := string(body)
	for _, want := range []string{
		"event_pipeline_events_ingested_total",
		"event_pipeline_events_processed_total",
		"event_pipeline_active_workers",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics output missing %q\n%s", want, text)
		}
	}
}