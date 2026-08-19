# Tasks: Event Pipeline

## Review Workload Forecast

| Field | Value |
|---|---|
| Estimated lines | 1,100–1,500; units 180–320 |
| Review budget (800) | Exceeds |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Strategy | ask-on-risk |
| Chain strategy | pending |

Decision needed before apply: Yes  
Chained PRs recommended: Yes  
Chain strategy: pending  
400-line budget risk: High

### Suggested Work Units

| Unit | Goal | Focused test | Runtime harness | Rollback boundary |
|---|---|---|---|---|
| 1 | Module/config/foundations | `go test ./internal/config/...` | N/A | `go.mod`, config, metrics |
| 2 | Redis/worker concurrency | `go test -race ./internal/{stream,worker}/...` | fake stream | stream/worker |
| 3 | Aggregation/SSE barrier | `go test -race ./internal/{aggregation,sse}/...` | fake replay/live | aggregation/SSE |
| 4 | API/wiring/observability | `go test ./internal/httpapi/...` | `go run ./cmd/event-pipeline` + curl | API/main/metrics |
| 5 | React/embed/deployment/docs | `npm run build && go test ./internal/web/...` | `docker build .` | web/embed/Docker/README |
| 6 | E2E proof/git baseline | `go test -race -cover ./...` | integration tests with Redis | integration/git |

## Phase 1: Foundation (RED → GREEN)

- [ ] 1.1 RED: config tests; GREEN: create `go.mod`, `internal/config/config.go`, `.gitignore`, Makefile; validate `REDIS_URL`, worker bounds/default 4, port 8080.
- [ ] 1.2 RED: metrics tests; GREEN: create `internal/metrics/metrics.go` with bounded labels, counters/gauges/histogram, Prometheus handler, JSON `slog`.

## Phase 2: Stream and Concurrency Core (RED → GREEN)

- [ ] 2.1 RED: fake tests; GREEN: create `internal/stream/client.go` and `fake.go` for XADD, XREADGROUP, XACK, XRANGE, XAUTOCLAIM, group init, cancellation.
- [ ] 2.2 RED: worker/recovery tests with goleak; GREEN: create `internal/worker/worker.go` for claim-before-read, immutable dispatch, success-only XACK, unique consumers, exit.
- [ ] 2.3 RED: resize/race tests; GREEN: create `internal/worker/supervisor.go` with atomic target/active, control channel, readiness, newest-first stop/wait.
- [ ] 2.4 RED: aggregation window/coalescing tests with injected clock; GREEN: create `internal/aggregation/aggregator.go` single-owner UTC maps, five-minute cleanup, bounded snapshots.

## Phase 3: SSE and HTTP (RED → GREEN)

- [ ] 3.1 RED: replay, fresh client, full queue, barrier tests with goleak; GREEN: create `internal/sse/hub.go` register→XRANGE→enqueue→gate, shared envelope, disconnect.
- [ ] 3.2 RED: `httptest` handler tests; GREEN: create `internal/httpapi/handler.go`, `router.go` for bounded `/events`, SSE, worker GET/PUT, health, metrics, middleware.
- [ ] 3.3 RED: composition/shutdown tests; GREEN: create `cmd/event-pipeline/main.go` with signal context, shutdown order, and all wiring.

## Phase 4: Frontend and Delivery

- [ ] 4.1 RED: UI fixtures; GREEN: create `web/` Vite+TS+Tailwind SPA (`useEventStream`, types, list, counters, generator, active/desired selector) targeting `internal/web/dist`.
- [ ] 4.2 RED: embed/build tests; GREEN: create `internal/web/embed.go`, multi-stage `Dockerfile`, and `README.md` ASCII diagram/demo; verify `npm run build && go test ./internal/web/...`.

## Phase 5: Verification and Git

- [ ] 5.1 Add `//go:build integration` Redis tests for XADD→XREADGROUP→XACK and XRANGE replay; run `go test -tags=integration ./...`.
- [ ] 5.2 Run `go test ./...`, `go test -race -cover ./...` (≥80%), `go build ./...`, frontend build, Docker smoke test on 8080.
- [ ] 5.3 Initialize Git; make work-unit Conventional Commits with tests; record each unit's test, harness, rollback.

Threat matrix: all rows are N/A; no RED threat tests are required.
