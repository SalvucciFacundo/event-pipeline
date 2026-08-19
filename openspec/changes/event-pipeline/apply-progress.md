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

## Next work unit
- WU3 — Aggregation/SSE barrier: `internal/aggregation` (single-owner windowed counters) + `internal/sse` (hub with replay activation barrier), race tests + goleak.