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

## Next work unit
- WU2 — Redis/worker concurrency: `internal/stream` (StreamClient interface + fake), `internal/worker` (worker + supervisor resize), race tests + goleak.