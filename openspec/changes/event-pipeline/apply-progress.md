# Apply Progress — event-pipeline

Change: event-pipeline
Updated: 2026-08-19

## Work Unit 1 — Module/config/foundations (COMPLETED)

Implemented directly by the orchestrator after two sdd-apply transport failures
(`sdd_task_result_empty`) consumed the attempt budget (2/2, 0 changed lines),
per explicit maintainer decision: "intenta de nuevo sino aplica los cambios tu".

### Tasks completed
- [x] 1.1 RED: config tests → GREEN: `go.mod`, `internal/config/config.go`, `.gitignore`, `Makefile`; validates REDIS_URL (required), EVENT_WORKERS (default 4, bounds 1..64), PORT (default 8080, bounds 1..65535).
- [x] 1.2 RED: metrics tests → GREEN: `internal/metrics/metrics.go` with bounded labels (type, worker, route), counters/gauges/histogram, Prometheus handler.

### Files created
- `go.mod` (module event-pipeline, go 1.26)
- `go.sum`
- `.gitignore`
- `Makefile` (build/test/vet/race/cover/tidy/run/frontend-build)
- `internal/config/config.go`, `internal/config/config_test.go`
- `internal/metrics/metrics.go`, `internal/metrics/metrics_test.go`

### Verification (focused)
- `go vet ./...` — PASS (no output)
- `go test ./...` — PASS (config ok, metrics ok)
- `go test -race -cover ./...` — PASS; coverage: config 96.4%, metrics 100.0% (target ≥80%)

### Runtime harness
- N/A for this unit (no HTTP server yet; runtime boundary arrives in WU4).

### Rollback boundary
- Remove `go.mod`, `go.sum`, `.gitignore`, `Makefile`, `internal/config/`, `internal/metrics/`, and this file — no other work depends on them yet.

### Commits
- Baseline: `chore: bootstrap SDD artifacts and spec baseline`
- WU1: `feat: add config validation and prometheus metrics foundations`

## Work Unit 2 — Redis/worker concurrency (COMPLETED)

Implemented directly by the orchestrator (same direct-application path as WU1).

### Tasks completed
- [x] 2.1 RED: fake tests → GREEN: `internal/stream/client.go` (StreamClient interface + Message + StreamKey/GroupName constants), `fake.go` (deterministic in-memory fake: XADD/XREADGROUP/XACK/XRANGE/XAUTOCLAIM/EnsureGroup, blocking reads via notify channel), `redis.go` (go-redis v9 implementation).
- [x] 2.2 RED: worker/recovery tests with goleak → GREEN: `internal/worker/worker.go` — claim-before-read (XAUTOCLAIM), immutable dispatch to out channel, success-only XACK, context exit.
- [x] 2.3 RED: resize/race tests → GREEN: `internal/worker/supervisor.go` — atomic target/active, control channel + reconcile loop, start up / newest-first stop with wait, readiness gauge, shutdown cancels all + waits.

### Files created
- `internal/stream/client.go`, `internal/stream/fake.go`, `internal/stream/redis.go`, `internal/stream/stream_test.go`
- `internal/worker/worker.go`, `internal/worker/supervisor.go`, `internal/worker/worker_test.go`, `internal/worker/supervisor_test.go`

### Verification (focused)
- `go vet ./...` — PASS
- `go test ./internal/...` — PASS (config, metrics, stream, worker)
- `go test -race -cover ./...` — PASS; coverage: config 96.4%, metrics 100.0%, stream 50.3% (Redis-backed code uncovered without integration tests), worker 73.1%. NOTE: repo-wide 80% target depends on tagged integration tests with REDIS_URL (task 5.1).

### Runtime harness
- N/A for this unit (worker pool exercised through the fake; real Redis path covered by integration tests in WU5).

### Rollback boundary
- Remove `internal/stream/` and `internal/worker/` — nothing else depends on them yet.

### Commits
- `feat(stream): add StreamClient interface, redis implementation, and deterministic fake`
- `feat(worker): add cancellable worker and resize supervisor`

## Work Unit 3 — Aggregation/SSE barrier (COMPLETED)

Implemented directly by the orchestrator (same direct-application path).

### Tasks completed
- [x] 2.4 RED: aggregation window/coalescing tests with injected clock → GREEN: `internal/aggregation/aggregator.go` — single-owner goroutine, per-second/per-minute counters by type + totals, 5-min retention cleanup (second/minute granularity), bounded snapshot publish with coalescing, final flush on cancel.
- [x] 3.1 RED: replay, fresh client, full queue, barrier tests with goleak → GREEN: `internal/sse/hub.go` — client registry, bounded per-client queue, two-phase replay barrier (register → exclusive XRANGE → enqueue → gate-open), slow-client disconnect, concurrent-safe send/close.

### Files created
- `internal/aggregation/aggregator.go`, `internal/aggregation/aggregator_test.go`
- `internal/sse/hub.go`, `internal/sse/hub_test.go`

### Verification (focused)
- `go vet ./...` — PASS
- `go test ./internal/...` — PASS (all 5 packages)
- `go test -race -cover ./...` — PASS; coverage: aggregation 89.4%, sse 81.8%.

### Runtime harness
- N/A (aggregator and hub exercised through fakes; SSE HTTP wiring arrives in WU4).

### Rollback boundary
- Remove `internal/aggregation/` and `internal/sse/` — nothing else depends on them yet.

### Bugs caught by tests
- Cleanup deleted ALL minute windows (compared minute keys against a second-based cutoff) — fixed with per-granularity cutoffs.
- Tests raced cancel-before-consume — restructured with snapshot-predicate waits.

### Commits
- `feat(aggregation): add single-owner windowed counters with bounded snapshots`
- `feat(sse): add hub with two-phase replay barrier and slow-client disconnect`

## Work Unit 4 — API/wiring/observability (COMPLETED)

Implemented directly by the orchestrator (same direct-application path).

### Tasks completed
- [x] 3.2 RED: httptest handler tests → GREEN: `internal/httpapi/server.go` — bounded `POST /events` (validation + MaxBytesReader, 202 + id), `GET /api/events/stream` (SSE), `PUT/GET /api/config/workers` (bounded, desired/active), `/healthz` (Redis ping 200/503), `/metrics` routing. `workerRegistry` adapter decouples handlers from the supervisor.
- [x] 3.3 composition/shutdown: `cmd/event-pipeline/main.go` — signal.NotifyContext root, wiring (Redis → supervisor → aggInput → aggregator → snapshots → hub; workers Notify → hub), initial worker resize, http.Server with graceful Shutdown (ReadTimeout, no WriteTimeout for SSE, IdleTimeout).

### Support additions (to existing packages, WU2/WU3)
- `stream.StreamClient.Ping` (+ fake + redis impl) for healthz.
- `worker.Options.Notify` (per-message callback) wired through SupervisorOptions.Notify.
- `aggregation.Aggregator.Done` (channel closed on Run return).
- `sse.Hub.Close` (disconnect all clients).

### Files created/changed
- `internal/httpapi/server.go`, `internal/httpapi/handler_test.go`
- `cmd/event-pipeline/main.go`
- Modified: `internal/stream/{client,fake,redis}.go`, `internal/worker/{worker,supervisor}.go`, `internal/aggregation/aggregator.go`, `internal/sse/hub.go`

### Verification (focused)
- `go vet ./...` — PASS
- `go test ./...` — PASS (all 7 packages)
- `go test -race -cover ./...` — PASS; coverage: httpapi 53.7% (SSE handler path + real Redis path under-covered without integration tests)
- `go build ./...` — PASS
- Runtime harness (partial): `go run ./cmd/event-pipeline` without REDIS_URL → fails fast with "REDIS_URL is required" (exit 1). Full end-to-end runtime requires Redis → integration tests (WU5/5.1).

### Rollback boundary
- Remove `internal/httpapi/` and `cmd/` — they are the last wiring layer; removing them leaves the reusable pipeline packages intact.

### Commits
- `feat(httpapi): add event ingestion, SSE, worker config, health, and metrics routes`
- `feat(cmd): add event-pipeline composition root with graceful shutdown`

## Next work unit
- WU5 — Frontend/embed/deployment: React SPA (Vite+TS+Tailwind, useEventStream hook with Last-Event-ID + dedup, dashboard components), internal/web embed, multi-stage Dockerfile, README with ASCII diagram. Plus integration tests (task 5.1, `//go:build integration` + REDIS_URL).