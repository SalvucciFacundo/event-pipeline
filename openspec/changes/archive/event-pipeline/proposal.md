# Proposal: Event Pipeline

## Intent

Build the portfolio differentiator: a real-time event pipeline demonstrating goroutines, channels, backpressure, and streaming — the concurrency story almost no junior candidate can tell. The project delivers a single Go binary that ingests events via HTTP, distributes them through Redis Streams to N concurrent workers, aggregates windowed counters, and pushes live updates to a React dashboard over SSE. Deployed on Dokploy with a shared Redis instance.

## Scope

### In Scope (MVP)
- `POST /events` with bounded JSON schema → Redis `XADD` to single stream `event-pipeline:events`
- N cancellable workers via `XREADGROUP` on shared consumer group `event-pipeline-workers`, success-based `XACK`, explicit pending-entry recovery policy
- Supervisor-managed live worker resize (atomic target + control channel, per-worker context cancellation)
- Single-owner aggregation goroutine: per-second/per-minute counters by event type, bounded window cleanup (5-min retention)
- SSE hub: bounded per-client queues, `Last-Event-ID` replay via exclusive `XRANGE`, replay activation barrier (register → replay → gate-open → live), slow-client disconnect policy
- React/Vite/TypeScript/Tailwind dashboard: live event list, aggregate counters, event generator button, worker-count selector (`PUT /api/config/workers`)
- `/metrics` (Prometheus, low-cardinality labels only), structured JSON `slog` logs, `/healthz`, graceful shutdown
- Unit tests + race tests + `goleak` leak checks co-located; Redis integration tests behind `//go:build integration` tag
- Single-binary `//go:embed` for React build output, Dockerfile, README with architecture diagram

### Out of Scope (Deferred)
- Postgres window persistence
- Dashboard event-type filters
- Reconnect backoff/jitter beyond native `EventSource`
- Multi-instance SSE coordination
- Dead-letter streams, advanced Redis retention/claim tooling
- Client-provided event IDs as SSE cursors (Redis stream IDs are authoritative)

## Capabilities

### New Capabilities
- `event-ingestion`: HTTP event intake with validation and Redis XADD
- `stream-worker-pool`: Redis Streams consumer group with supervisor-managed live resize
- `event-aggregation`: Single-owner windowed counters (per-second, per-minute, by type)
- `sse-delivery`: SSE hub with replay barrier, bounded queues, slow-client policy
- `dashboard`: React SPA with live events, aggregates, worker selector, event generator
- `observability`: Prometheus metrics, structured logging, health endpoint
- `deployment`: Dockerfile, embedded assets, Dokploy config, README with diagram

### Modified Capabilities
None (greenfield project)

## Approach

Single canonical Redis Stream (`event-pipeline:events`) with Redis-generated IDs as the only replay cursor and SSE `id`. One consumer group (`event-pipeline-workers`) with unique consumer names per worker slot. Supervisor pattern for live resize — NOT `errgroup.SetLimit` (unsuitable for long-lived independently cancellable workers). SSE replay uses a two-phase protocol: register with bounded queue → exclusive `XRANGE` from `Last-Event-ID` → open gate → live broadcasts flow. Aggregation is single-goroutine-owned, no mutex on hot path. Redis behind small interfaces for testability (deterministic fakes for unit tests, integration tests require `REDIS_URL`).

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `cmd/event-pipeline/main.go` | New | Composition root: config, Redis, supervisor, aggregation, SSE hub, HTTP routes, embed |
| `internal/config/` | New | Validated `REDIS_URL`, `EVENT_WORKERS`, HTTP settings |
| `internal/stream/` | New | Redis Streams adapter, group init, XADD/XREADGROUP/XACK/XRANGE |
| `internal/worker/` | New | Cancellable worker lifecycle, supervisor with live resize |
| `internal/aggregation/` | New | Single-owner window state, snapshot publication |
| `internal/sse/` | New | Client registry, replay barrier, bounded queues, disconnect cleanup |
| `internal/httpapi/` | New | Event ingestion, SSE endpoint, worker config, health, metrics routing |
| `internal/metrics/` | New | Low-cardinality Prometheus counters/gauges/histograms |
| `internal/web/` | New | Go embed package for Vite build output |
| `web/src/` | New | React SPA: `useEventStream` hook, components, types |
| `Dockerfile` | New | Multi-stage: Go build + Node build → scratch/alpine runtime |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Redis unavailable locally for development/testing | High | Interface-backed adapter; deterministic fakes for unit tests; `//go:build integration` tag |
| SSE replay race (lost or reordered events) | Medium | Two-phase protocol: register → XRANGE replay → gate-open → live; single envelope format |
| Worker stop leaves pending entries unclaimed | Medium | Document one recovery policy (XAUTOCLAIM on start); surface pending counts via metrics |
| Slow SSE client blocks publisher | Low | Bounded per-client queue; disconnect lagging clients; reconnect from last ID |
| Vite embed build-order trap | Medium | Frontend build output targets `internal/web/dist`; explicit build order in Dockerfile |
| In-memory aggregation lost on restart | Certain | Accepted MVP boundary for single-container demo; Postgres persistence is deferred |

## Rollback Plan

Greenfield project — rollback is `git revert` or branch deletion. No production state to migrate. Redis stream data is ephemeral for the demo. If a deployed container fails, Dokploy redeploys from the previous commit.

## Dependencies

- Redis instance (shared on Dokploy, `REDIS_URL` via internal host)
- Go 1.26.5 toolchain
- Node.js (for Vite/React build stage in Dockerfile)
- Dokploy instance for deployment

## Success Criteria

- [ ] Two browsers: events from one tab appear live in the other via SSE
- [ ] `EVENT_WORKERS=1` vs `4` produces measurable throughput difference visible in `/metrics`
- [ ] SSE reconnect with `Last-Event-ID` replays missed events in correct order
- [ ] Live worker resize via dashboard reflects actual active worker count
- [ ] `go test -race -cover ./...` passes with ≥80% coverage
- [ ] Single Docker image serves Go API + React dashboard on port 8080
- [ ] README with architecture diagram and demo instructions
