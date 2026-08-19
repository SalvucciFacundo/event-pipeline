// Command event-pipeline runs the real-time event pipeline: it ingests
// events via HTTP, distributes them through Redis Streams to N workers,
// aggregates windowed counters, and pushes live updates to connected SSE
// clients. It serves the embedded React dashboard on the same port.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"event-pipeline/internal/aggregation"
	"event-pipeline/internal/config"
	"event-pipeline/internal/httpapi"
	"event-pipeline/internal/metrics"
	"event-pipeline/internal/sse"
	"event-pipeline/internal/stream"
	"event-pipeline/internal/web"
	"event-pipeline/internal/worker"

	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("event-pipeline exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// Root context canceled by SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redis, err := stream.NewRedis(cfg.RedisURL)
	if err != nil {
		return err
	}
	defer redis.Close()

	reg := prometheus.NewRegistry()
	metrics := metrics.New(reg)

	// aggInput carries processed events from workers to the aggregator.
	aggInput := make(chan stream.Message, 1024)
	aggregator := aggregation.New(aggInput, aggregation.Options{})
	go aggregator.Run(ctx)

	// The SSE hub fans out live events. Workers notify it after each
	// message; the aggregator publishes snapshots to it.
	hub := sse.NewHub(redis, sse.Options{})

	// Supervisor owns the worker pool. Workers dispatch to aggInput (for
	// aggregation) and notify the hub (for live SSE fan-out).
	supervisor := worker.NewSupervisor(redis, aggInput, logger, metrics, worker.SupervisorOptions{
		Notify: func(m stream.Message) {
			data, _ := json.Marshal(m)
			hub.Broadcast(sse.Event{ID: m.ID, Kind: "event", Data: string(data)})
		},
	})
	go supervisor.Run(ctx)

	// Seed the initial worker count, then keep the pool in sync with any
	// later configuration via the HTTP endpoint.
	initial := int32(cfg.EventWorkers)
	go func() {
		_, _, err := supervisor.Resize(ctx, initial)
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("initial worker resize failed", "error", err)
		}
	}()

	// Publish aggregation snapshots to the hub as the aggregator produces
	// them. Snapshots use the "snapshot" SSE event type.
	go func() {
		for snap := range aggregator.Snapshots() {
			data, _ := json.Marshal(snap)
			hub.Broadcast(sse.Event{ID: "", Kind: "snapshot", Data: string(data)})
		}
	}()

	registry := httpapi.NewRegistry(supervisor)
	// //go:embed dist nests the built assets under a dist/ subdir on the embed
	// FS, so root it there — otherwise index.html never resolves and the app
	// serves "frontend not built".
	var staticRoot fs.FS = web.FS()
	if sub, err := fs.Sub(staticRoot, "dist"); err == nil {
		staticRoot = sub
	} else {
		logger.Error("frontend embed missing dist/", "error", err)
	}
	api := httpapi.New(httpapi.Options{
		Redis:      redis,
		Registry:   registry,
		Aggregator: aggregator,
		SSE:        hub,
		Metrics:    metrics,
		Logger:     logger,
		StaticFS:   http.FS(staticRoot),
	})
	go api.Run(ctx)

	srv := &http.Server{
		Addr:         addr(cfg.Port),
		Handler:      api.Handler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // SSE requires no write timeout
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("event-pipeline listening", "addr", srv.Addr, "workers", initial)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Graceful shutdown: drain HTTP, then cancel the pipeline goroutines.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown", "error", err)
	}

	hub.Close()
	logger.Info("shutdown complete")
	return nil
}

func addr(port int) string {
	return ":" + strconv.Itoa(port)
}
