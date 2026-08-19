# Event Pipeline — real-time streaming demo

A real-time event pipeline demonstrating Go concurrency: goroutines, worker
pools, Redis Streams consumer groups, backpressure, and Server-Sent Events
(SSE) with lossless replay. A single Go binary serves the HTTP API and the
embedded React dashboard on one port.

**This is a portfolio differentiator**: most junior candidates cannot talk
about backpressure, consumer groups, pending-entry recovery, or SSE replay
from real implementation. This project exercises all of them.

## Architecture

```
                         ┌──────────────────────────────────────────────────┐
                         │                 event-pipeline (Go)             │
                         │                                                  │
  HTTP POST /events ───► │  ingestion ──XADD──► Redis Streams ──XREADGROUP► │
                         │                    (event-pipeline:events)      │
                         │                           │  N workers           │
                         │                           ▼                     │
  Dashboard (React) ◄─── │  SSE hub ◄──snapshots── aggregation ◄──channel── │
  (EventSource,          │     ▲                     (per-sec/min windows)  │
   Last-Event-ID replay) │     └──────────live events──────────┘            │
                         │                                                  │
                         │  /metrics (Prometheus) · /healthz · config       │
                         └──────────────────────────────────────────────────┘
```

- **Ingestion**: `POST /events` validates a bounded JSON body and appends it
  to a Redis Stream via `XADD`.
- **Workers**: N goroutines consume the stream through a shared consumer
  group (`event-pipeline-workers`) with `XREADGROUP`, acknowledging with
  `XACK` only after successful dispatch. Pending entries from dead
  consumers are reclaimed with `XAUTOCLAIM` on worker start.
- **Supervisor**: a live resize owner with atomic target/active counters.
  Changing the worker count at runtime starts/stops workers deterministically.
- **Aggregation**: one goroutine owns per-second/per-minute counters by event
  type, with bounded retention, and publishes immutable snapshots.
- **SSE**: the hub fans events out to connected dashboards with a two-phase
  replay barrier — a reconnecting client replays missed events via exclusive
  `XRANGE` from `Last-Event-ID` before receiving live events, in order.
- **Dashboard**: React + TypeScript + Tailwind (Industrial terminal visual
  direction). Live event stream, aggregate counters, a real event emitter,
  and a live worker-count selector.

## Run locally

Requires a Redis instance. Start it, then:

```bash
# backend
export REDIS_URL=redis://localhost:6379
export EVENT_WORKERS=4
make run

# frontend (dev, optional — proxies API to :8080)
cd web && npm install && npm run dev
```

Open `http://localhost:8080`. Use “emit event” to send real traffic.

## Test

```bash
make test          # unit tests
make race          # race + coverage
make vet           # static analysis
go test -tags=integration ./...   # Redis integration tests (needs REDIS_URL)
```

## Docker

```bash
docker build -t event-pipeline .
docker run -p 8080:8080 -e REDIS_URL=redis://host:6379 event-pipeline
```

Single multi-stage image (~15MB): builds the React frontend, then the Go
binary, then serves both from `scratch`.

## Deploy (Dokploy)

- One shared Redis instance (created once).
- Application → this repo → Dockerfile → port `8080`.
- Env: `REDIS_URL` (internal host), `EVENT_WORKERS=4`.
- GitHub auto-deploy: push to the configured branch redeploys.

## Demo script

1. Open the dashboard in **two browsers**.
2. Emit events from one tab — they appear live in the other via SSE.
3. Change `EVENT_WORKERS` with the selector; the active count and per-worker
   metrics update, showing throughput changes in `/metrics`.
4. Disconnect/reconnect a client — missed events replay in order via
   `Last-Event-ID`.

## Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/events` | POST | Ingest an event `{type, payload, occurred_at, source?}` → `202 {id}` |
| `/api/events/stream` | GET | SSE live stream (send `Last-Event-ID` to replay) |
| `/api/config/workers` | GET/PUT | Read / change the live worker count |
| `/healthz` | GET | `200` when Redis is reachable, else `503` |
| `/metrics` | GET | Prometheus metrics (low-cardinality labels) |
| `/` | GET | React dashboard (embedded) |
