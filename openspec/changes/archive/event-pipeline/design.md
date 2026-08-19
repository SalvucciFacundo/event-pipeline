# Design: Event Pipeline

## Technical Approach

Single Go 1.26.5 binary on port 8080. Redis Streams is the sole transport and replay cursor. Data flows: `POST /events` → `XADD` → N workers via `XREADGROUP` → single-owner aggregation goroutine → SSE hub with replay barrier → React dashboard. All concurrency is structured: every goroutine has a clear owner, exit mechanism, and context propagation. Redis is behind a narrow interface for testability.

## Architecture Decisions

| # | Decision | Choice | Alternatives | Rationale |
|---|----------|--------|-------------|-----------|
| ADR-1 | Stream topology | Single stream `event-pipeline:events` + one consumer group `event-pipeline-workers` | Multi-stream by type | One ordered log, simple XRANGE replay, natural work distribution. MVP has no need for per-type retention. |
| ADR-2 | Event identity | Redis-generated stream IDs as sole cursor and SSE `id` | Client-provided IDs | Native ordering, XRANGE compatibility, no collision/ordering logic. |
| ADR-3 | Worker lifecycle | Supervisor with atomic target + control channel + per-worker context | `errgroup.SetLimit` | SetLimit cannot safely stop already-running long-lived consumers. Supervisor gives explicit ownership, deterministic resize, and clean cancellation. |
| ADR-4 | Pending recovery | XAUTOCLAIM on worker start before blocking reads | Leave for Redis GC | Explicit recovery policy, surfaced via metrics. One documented policy over ambiguity. |
| ADR-5 | SSE replay | Two-phase barrier: register → XRANGE replay → gate-open → live | Subscribe-then-replay or replay-then-subscribe | Prevents both lost events (subscribe-late) and reordered events (replay-late). Single envelope format for replay and live. |
| ADR-6 | Aggregation ownership | Single goroutine owns all counter state; workers send immutable values via channel | Mutex-protected shared maps | No mutex on hot path, no map races, deterministic window transitions, trivially testable. |
| ADR-7 | SSE backpressure | Bounded per-client queue; disconnect slow clients | Block publisher or drop silently | Isolates slow clients, preserves replay-from-last-ID recovery, prevents global backpressure. |
| ADR-8 | Redis testability | Narrow interface (`StreamClient`) with deterministic fake for unit tests | Mock library or integration-only | Unit tests run without Redis; integration tests behind `//go:build integration` + `REDIS_URL`. |

## Sequence Diagrams

### (a) Event Ingestion → Processing → SSE Broadcast

```
Client          HTTP API         Redis           Worker(N)       Aggregator       SSE Hub        SSE Client
  │  POST /events  │                │                │                │               │               │
  │───────────────→│  XADD          │                │                │               │               │
  │                │───────────────→│                │                │               │               │
  │  202 {id}      │                │  XREADGROUP    │                │               │               │
  │←───────────────│                │───────────────→│                │               │               │
  │                │                │                │  processed evt │               │               │
  │                │                │  XACK          │───────────────→│               │               │
  │                │                │←──────────────│                │  snapshot      │               │
  │                │                │               │                │──────────────→│  SSE event    │
  │                │                │               │                │               │──────────────→│
```

### (b) SSE Connect with Last-Event-ID Replay

```
SSE Client                    SSE Hub                         Redis
  │  GET /api/events/stream    │                               │
  │  Last-Event-ID: 1234-0     │                               │
  │───────────────────────────→│  register (bounded queue)      │
  │                            │──────────────────────────────→│
  │                            │  XRANGE (1234-0 + tail        │
  │                            │←──────────────────────────────│
  │                            │  enqueue replay entries        │
  │                            │  open gate                     │
  │  SSE: replayed events      │                               │
  │←───────────────────────────│                               │
  │  SSE: live events (gate open)                              │
  │←───────────────────────────│                               │
```

### (c) Live Worker Resize (Up)

```
Dashboard       HTTP API        Supervisor                 Worker(new)
  │  PUT count=4  │               │                           │
  │──────────────→│  atomic.Store │                           │
  │               │──────────────→│                           │
  │               │               │  start worker[3]          │
  │               │               │──────────────────────────→│
  │               │               │  start worker[4]          │
  │               │               │──────────────────────────→│
  │               │               │  wait ready               │
  │               │               │  gauge = 4                │
  │  200 {active:4}│              │                           │
  │←──────────────│               │                           │
```

## Concurrency Contract

| Aspect | Contract |
|--------|----------|
| **Goroutine ownership** | `main` owns supervisor, aggregator, SSE hub. Supervisor owns worker goroutines. Each HTTP handler owns its request goroutine. |
| **Channel ownership** | Creator closes. `aggInput chan<- ProcessedEvent` — workers send, aggregator receives. `controlChan` — supervisor sends resize signals, supervisor reads. Per-client `events chan<- SSEEvent` — hub sends, HTTP handler receives. |
| **Context propagation** | Root context from `signal.NotifyContext`. Supervisor derives per-worker contexts. HTTP handlers use `r.Context()`. All Redis calls use context variants. |
| **Graceful shutdown order** | 1) Signal → cancel root ctx. 2) HTTP server drains. 3) Supervisor cancels all workers, waits exit. 4) Close `aggInput`. 5) Aggregator flushes final snapshot. 6) SSE hub closes all client channels. 7) Redis client closes. |
| **Resize semantics** | `target` (atomic int32) vs `active` (atomic int32). Resize up: start workers until active==target. Resize down: cancel newest workers' contexts, wait exit, decrement active. API returns both values. |
| **Backpressure** | Per-client SSE queue bounded (e.g. 256). Full queue → disconnect. Aggregator input channel bounded (e.g. 1024). Full → worker blocks (natural Redis XREADGROUP backpressure). |

## Data Contracts

### Event Envelope (stored in Redis stream fields)

```
type:       string (required, bounded)
payload:    JSON string (required, bounded)
occurred_at: RFC3339 UTC (required)
source:     string (optional, bounded)
```

### SSE Event Format

```
id: <redis-stream-id>
event: event | snapshot | config
data: <JSON envelope>
```

### API Contracts

| Endpoint | Method | Request | Response |
|----------|--------|---------|----------|
| `/events` | POST | `{type, payload, occurred_at, source?}` bounded JSON | 202 `{id}` / 400 / 413 |
| `/api/config/workers` | PUT | `{count: int}` bounded | 200 `{desired, active}` |
| `/api/config/workers` | GET | — | 200 `{desired, active}` |
| `/api/events/stream` | GET | `Last-Event-ID` header optional | SSE stream |
| `/healthz` | GET | — | 200 / 503 |
| `/metrics` | GET | — | Prometheus text |

## File Changes

| File | Action | Description |
|------|--------|-------------|
| `cmd/event-pipeline/main.go` | Create | Composition root: config, Redis, supervisor, aggregator, SSE hub, HTTP routes, embed |
| `internal/config/config.go` | Create | Validated env: `REDIS_URL`, `EVENT_WORKERS`, HTTP addr, worker bounds |
| `internal/stream/client.go` | Create | `StreamClient` interface + Redis implementation: XADD/XREADGROUP/XACK/XRANGE/XAUTOCLAIM |
| `internal/stream/fake.go` | Create | Deterministic in-memory fake for unit tests |
| `internal/worker/worker.go` | Create | Single worker: XREADGROUP loop, aggregation dispatch, XACK, context exit |
| `internal/worker/supervisor.go` | Create | Lifecycle owner: atomic target, control channel, start/stop workers, resize API |
| `internal/aggregation/aggregator.go` | Create | Single goroutine: per-sec/per-min counters by type, cleanup ticker, snapshot publish |
| `internal/sse/hub.go` | Create | Client registry, replay barrier, bounded queues, slow-client disconnect |
| `internal/httpapi/handler.go` | Create | POST /events, SSE endpoint, worker config, health |
| `internal/httpapi/router.go` | Create | Route registration, middleware (logging, metrics, recovery) |
| `internal/metrics/metrics.go` | Create | Prometheus counters/gauges/histograms, low-cardinality labels |
| `internal/web/embed.go` | Create | `//go:embed dist/*` package for static assets |
| `web/src/` | Create | React SPA: hooks, components, types, Vite config → `internal/web/dist` |
| `Dockerfile` | Create | Multi-stage: node build → go build → scratch |
| `internal/web/dist/` | Create | Vite build output target (gitignored, built by Dockerfile) |
| `README.md` | Create | Architecture diagram (ASCII) + demo instructions following portafolio-go-guia.md conventions (two-browser SSE sync, worker resize, reconnect replay) |

## Testing Strategy

| Layer | What | Approach |
|-------|------|----------|
| Unit | Config validation, stream fake, worker loop, supervisor resize, aggregator windows, SSE replay barrier, HTTP handlers | Table-driven tests with `StreamClient` fake, injected clocks, `goleak` on every test |
| Race | All concurrent paths (supervisor resize, SSE fan-out, aggregator input) | `go test -race ./...` in CI |
| Integration | Redis Streams end-to-end, XADD → XREADGROUP → XACK, XRANGE replay | `//go:build integration` tag, requires `REDIS_URL` env |
| Coverage | All packages | ≥80% threshold via `go test -cover` |

## Threat Matrix

N/A — no routing/shell/subprocess/VCS/PR automation/executable-file classification boundary. The Dockerfile is a build artifact, not a runtime shell boundary. HTTP routing uses standard `net/http` with no path-based execution.

## Migration / Rollout

No migration required. Greenfield project. Single container deploy via Dokploy. Env vars: `REDIS_URL` (internal host), `EVENT_WORKERS` (default 4).

## Open Questions

- [ ] Exact bounded queue sizes (SSE per-client, aggregator input) — propose defaults, tune via metrics
- [ ] Worker consumer name format — propose `{hostname}-{slot}` with UUID suffix for uniqueness
