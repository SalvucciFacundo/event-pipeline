# Event Pipeline Specification

Single Go 1.26.5 binary on port 8080. Data flow: `POST /events` → Redis `XADD` → N workers (`XREADGROUP`) → aggregation → SSE → React dashboard. Redis Streams is the only transport; Redis-generated IDs are the sole replay cursor and SSE `id`.

## event-ingestion

### Requirement: Bounded Event Ingestion
The API MUST accept `POST /events` with a bounded JSON body (`type`, `payload`, `occurred_at`, optional `source`). The handler SHALL validate the schema, reject malformed/oversized requests with 4xx, and on success perform Redis `XADD` to stream `event-pipeline:events`, returning 202 with the Redis stream ID.

#### Scenario: Valid event accepted
- GIVEN a POST /events with valid JSON within byte bounds
- WHEN the request is received
- THEN the response is 202 with the Redis stream ID
- AND the entry is visible in `event-pipeline:events`

#### Scenario: Invalid or oversized body rejected
- GIVEN a POST /events missing `type` OR exceeding the byte limit
- WHEN the request is received
- THEN the response is 400 or 413 respectively
- AND no XADD is performed

## stream-worker-pool

### Requirement: Supervised Worker Pool
The system SHALL run N workers (configurable via `EVENT_WORKERS`, default 4) on consumer group `event-pipeline-workers` via `XREADGROUP`. Each worker MUST have a unique consumer name, `XACK` only after successful aggregation/publication, and exit on context cancellation. Live resize uses a supervisor with atomic target + control channel — NOT `errgroup.SetLimit`.

#### Scenario: Concurrent consumption
- GIVEN `EVENT_WORKERS=4` and pending entries
- WHEN workers run
- THEN up to 4 entries process concurrently
- AND each entry is XACK'd exactly once after success

#### Scenario: Live resize up
- GIVEN 2 active workers
- WHEN `PUT /api/config/workers` sets `count=4`
- THEN the supervisor starts 2 new workers with fresh consumer names
- AND the active-count gauge reports 4 only after workers are ready

#### Scenario: Live resize down
- GIVEN 4 active workers
- WHEN the target is set to 2
- THEN the supervisor cancels 2 contexts and waits for exit
- AND pending entries owned by stopped consumers are surfaced via metrics

### Requirement: Pending Entry Recovery
On start, a worker SHALL reclaim pending entries owned by dead consumers via `XAUTOCLAIM` before blocking on new reads, and expose pending count as a metric.

#### Scenario: Recovery on start
- GIVEN a prior worker crashed leaving 3 pending entries
- WHEN a replacement starts
- THEN it claims those entries before reading new ones

## event-aggregation

### Requirement: Single-Owner Windowed Counters
A single goroutine SHALL own all counter state. Workers send immutable processed-event values to an input channel. The aggregator maintains per-second and per-minute counters keyed by UTC window and event type, with 5-minute retention cleanup driven by a ticker. Snapshots publish on a bounded cadence with coalescing; maps are never exposed directly.

#### Scenario: Counter increment
- GIVEN a window for `type="click"` at second T
- WHEN a processed click arrives
- THEN per-second (T, click) and per-minute (minute(T), click) counters each increment by 1

#### Scenario: Window expiry
- GIVEN counters older than 5 minutes
- WHEN the cleanup ticker fires
- THEN those entries are removed and absent from subsequent snapshots

## sse-delivery

### Requirement: Replay Activation Barrier
The SSE hub MUST implement two-phase subscription: (1) register client with bounded queue, (2) exclusive `XRANGE` from `Last-Event-ID` (exclusive) to tail, (3) enqueue replay entries, (4) open gate. Live events MUST NOT reach a client before its replay completes. Replay and live use the same envelope format.

#### Scenario: Reconnect replays missed events
- GIVEN client last received ID `1234-0` and 3 newer events exist
- WHEN reconnecting with `Last-Event-ID: 1234-0`
- THEN exactly the 3 newer events arrive in stream order
- AND subsequent live events follow

#### Scenario: Slow client disconnected
- GIVEN a client whose bounded queue is full
- WHEN the hub attempts to enqueue
- THEN the connection closes
- AND the client MAY reconnect from its last acknowledged ID

#### Scenario: Fresh client, no replay
- GIVEN a client without `Last-Event-ID`
- WHEN subscription establishes
- THEN replay produces zero entries and the gate opens immediately

## dashboard

### Requirement: Live Event Dashboard
The React SPA MUST render a scrolling live event list, aggregate counters (per-second, per-minute, by type), a test-event generator button, and a worker-count selector calling `PUT /api/config/workers`. The selector SHALL reflect the reported active/desired counts, not assume instantaneous resize.

#### Scenario: Cross-tab live rendering
- GIVEN the dashboard open in two browsers
- WHEN one triggers the generator
- THEN both display the new event within the SSE latency window

#### Scenario: Worker count change
- GIVEN selector shows 2 workers
- WHEN user sets count=4
- THEN the selector reflects the active count returned by the API

## observability

### Requirement: Metrics, Health, and Logs
The system MUST expose `/metrics` (Prometheus text) with low-cardinality labels only (event type, worker slot, route pattern). It MUST expose `/healthz` returning 200 when Redis is reachable and 503 otherwise. All application logs MUST be structured JSON via `slog`.

#### Scenario: Prometheus scrape
- GIVEN the service is running
- WHEN `GET /metrics` is called
- THEN counters for ingested/processed events, active-workers gauge, and ingestion-latency histogram are present

#### Scenario: Health degraded
- GIVEN Redis is unreachable
- WHEN `GET /healthz` is called
- THEN the response is 503

## deployment

### Requirement: Single-Binary Deployment
The Go binary MUST embed the Vite/React build output via `//go:embed` from `internal/web/dist`. The Dockerfile MUST build the frontend first, then the Go binary, producing one image serving API and static assets on port 8080. The README MUST include an architecture diagram and demo instructions.

#### Scenario: Embedded assets served
- GIVEN the binary is built
- WHEN `GET /` is called
- THEN the React SPA HTML is served from the embedded filesystem

#### Scenario: Docker image self-contained
- GIVEN `docker build` completes
- WHEN the container runs with only `REDIS_URL` set
- THEN API and dashboard are reachable on port 8080
