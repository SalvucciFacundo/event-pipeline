// Package httpapi exposes the HTTP surface: event ingestion, the SSE
// stream, live worker configuration, health, and metrics routing.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"event-pipeline/internal/aggregation"
	"event-pipeline/internal/metrics"
	"event-pipeline/internal/sse"
	"event-pipeline/internal/stream"
	"event-pipeline/internal/worker"
)

const (
	maxEventBytes   = 64 * 1024
	maxEventTypeLen = 64
	maxSourceLen    = 128
	// ssePingInterval keeps idle SSE connections alive.
	ssePingInterval = 15 * time.Second
)

// errInvalidWorkers is returned when a worker count is outside the bounds.
var errInvalidWorkers = errors.New("worker count must be within [1, 64]")

// EventRequest is the bounded POST /events body.
type EventRequest struct {
	Type       string `json:"type"`
	Payload    string `json:"payload"`
	OccurredAt string `json:"occurred_at"`
	Source     string `json:"source,omitempty"`
}

// Options wires the server to its dependencies.
type Options struct {
	Redis      stream.StreamClient
	Registry   *workerRegistry
	Aggregator *aggregation.Aggregator
	SSE        *sse.Hub
	Metrics    *metrics.Metrics
	Logger     *slog.Logger
	// StaticFS serves the embedded React SPA for non-API routes. When nil,
	// only the API routes are registered.
	StaticFS http.FileSystem
	// EventTime returns the RFC3339 UTC timestamp stamped on ingestion;
	// tests override it for determinism. Defaults to time.Now.
	EventTime func() string
}

// Server owns the HTTP handler and the health/readiness loop.
type Server struct {
	redis      stream.StreamClient
	registry   *workerRegistry
	aggregator *aggregation.Aggregator
	sse        *sse.Hub
	metrics    *metrics.Metrics
	logger     *slog.Logger
	eventTime  func() string
	staticFS   http.FileSystem

	handler http.Handler
	wg      sync.WaitGroup
}

// New creates the server and its HTTP handler. Call Run in a goroutine,
// then Wait after cancellation.
func New(opts Options) *Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	eventTime := opts.EventTime
	if eventTime == nil {
		eventTime = func() string { return time.Now().UTC().Format(time.RFC3339) }
	}
	s := &Server{
		redis:      opts.Redis,
		registry:   opts.Registry,
		aggregator: opts.Aggregator,
		sse:        opts.SSE,
		metrics:    opts.Metrics,
		logger:     logger,
		eventTime:  eventTime,
		staticFS:   opts.StaticFS,
	}
	s.handler = s.routes()
	return s
}

// Handler returns the configured HTTP handler.
func (s *Server) Handler() http.Handler { return s.handler }

// Run serves the health loop until ctx is canceled.
func (s *Server) Run(ctx context.Context) {
	s.wg.Add(1)
	defer s.wg.Done()
	<-ctx.Done()
}

// Wait blocks until Run returns.
func (s *Server) Wait() { s.wg.Wait() }

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.handlePostEvent)
	mux.HandleFunc("GET /api/events/stream", s.handleSSE)
	mux.HandleFunc("PUT /api/config/workers", s.handlePutWorkers)
	mux.HandleFunc("GET /api/config/workers", s.handleGetWorkers)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	if s.metrics != nil {
		mux.Handle("GET /metrics", s.metrics.Handler())
	}

	if s.staticFS != nil {
		// Serve the embedded SPA. Non-API paths fall through to the SPA
		// index.html so client-side routing works, while API routes stay
		// exact.
		mux.HandleFunc("/", s.handleStatic)
	}
	return mux
}

func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		if _, err := s.staticFS.Open(r.URL.Path); err == nil {
			http.FileServer(s.staticFS).ServeHTTP(w, r)
			return
		}
	}
	// SPA fallback: always serve index.html for unknown paths.
	index, err := s.staticFS.Open("index.html")
	if err != nil {
		http.Error(w, "frontend not built", http.StatusInternalServerError)
		return
	}
	defer index.Close()
	http.ServeContent(w, r, "index.html", time.Time{}, index)
}

func (s *Server) handlePostEvent(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, maxEventBytes)
	data, err := io.ReadAll(body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	var req EventRequest
	if err := json.Unmarshal(data, &req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateEvent(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msg := stream.Message{
		Type:       req.Type,
		Payload:    req.Payload,
		OccurredAt: req.OccurredAt,
		Source:     req.Source,
	}
	id, err := s.redis.Add(r.Context(), msg)
	if err != nil {
		s.logger.Error("xadd failed", "error", err)
		http.Error(w, "ingestion failed", http.StatusInternalServerError)
		return
	}
	if s.metrics != nil {
		s.metrics.Ingested(req.Type)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"id": id})
}

func validateEvent(req *EventRequest) error {
	if req.Type == "" {
		return errors.New("type is required")
	}
	if len(req.Type) > maxEventTypeLen {
		return fmt.Errorf("type exceeds %d bytes", maxEventTypeLen)
	}
	if req.Payload == "" {
		return errors.New("payload is required")
	}
	if req.OccurredAt == "" {
		return errors.New("occurred_at is required")
	}
	if len(req.Source) > maxSourceLen {
		return fmt.Errorf("source exceeds %d bytes", maxSourceLen)
	}
	return nil
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	lastID := r.Header.Get("Last-Event-ID")
	if s.sse == nil {
		http.Error(w, "stream unavailable", http.StatusServiceUnavailable)
		return
	}
	client, err := s.sse.Subscribe(r.Context(), lastID)
	if err != nil {
		http.Error(w, "subscribe failed", http.StatusInternalServerError)
		return
	}
	defer s.sse.Unsubscribe(client)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ping := time.NewTicker(ssePingInterval)
	defer ping.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			_, _ = fmt.Fprintf(w, ": ping\n\n")
			flusher.Flush()
		case ev, ok := <-client.Events():
			if !ok {
				return
			}
			if ev.ID != "" {
				_, _ = fmt.Fprintf(w, "id: %s\n", ev.ID)
			}
			_, _ = fmt.Fprintf(w, "event: %s\n", ev.Kind)
			_, _ = fmt.Fprintf(w, "data: %s\n\n", ev.Data)
			flusher.Flush()
		}
	}
}

func (s *Server) handlePutWorkers(w http.ResponseWriter, r *http.Request) {
	if s.registry == nil {
		http.Error(w, "worker config unavailable", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1024))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var req struct {
		Count int32 `json:"count"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	desired, active, err := s.registry.Set(r.Context(), req.Count)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeWorkers(w, desired, active)
}

func (s *Server) handleGetWorkers(w http.ResponseWriter, r *http.Request) {
	if s.registry == nil {
		http.Error(w, "worker config unavailable", http.StatusServiceUnavailable)
		return
	}
	desired, active := s.registry.Get()
	writeWorkers(w, desired, active)
}

func writeWorkers(w http.ResponseWriter, desired, active int32) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int32{"desired": desired, "active": active})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.redis.Ping(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "redis unreachable")
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

// ensure the worker package is referenced (used by main via Registry).
var _ = worker.MinWorkers

// workerRegistry adapts the worker.Supervisor into a small interface the
// HTTP layer can use. A nil-registry registry keeps the handler testable
// without a live supervisor.
type workerRegistry struct {
	mu     sync.Mutex
	set    func(ctx context.Context, count int32) (int32, int32, error)
	get    func() (int32, int32)
	done   chan struct{}
	closed bool
}

// Set resizes the pool, or no-ops when unconfigured.
func (r *workerRegistry) Set(ctx context.Context, count int32) (int32, int32, error) {
	r.mu.Lock()
	if r.set != nil {
		// Hold the lock to serialize with Close; real registries back onto
		// a supervisor which has its own serialization.
		fn := r.set
		r.mu.Unlock()
		return fn(ctx, count)
	}
	desired, active := r.Get()
	r.mu.Unlock()
	return desired, active, nil
}

// Get returns the current desired/active counts.
func (r *workerRegistry) Get() (int32, int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.get != nil {
		d, a := r.get()
		return d, a
	}
	return 0, 0
}

// Close is a no-op placeholder; real registries close their supervisor.
func (r *workerRegistry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	close(r.done)
}

// NewRegistry wraps a supervisor for the HTTP layer.
func NewRegistry(s *worker.Supervisor) *workerRegistry {
	if s == nil {
		return nil
	}
	return &workerRegistry{
		set:  s.Resize,
		get:  func() (int32, int32) { return s.Desired(), s.Active() },
		done: make(chan struct{}),
	}
}
