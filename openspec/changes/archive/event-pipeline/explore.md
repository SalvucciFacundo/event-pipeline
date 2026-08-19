## Exploration: event-pipeline

### Current State
The project is greenfield. `spec.txt` describes a real-time event pipeline using Redis Pub/Sub, but the SDD context has already resolved that transport decision to Redis Streams: `POST /events` performs `XADD`, a shared consumer group processes entries with `XREADGROUP`, aggregation produces windowed counters, and an SSE hub serves the React dashboard. The service is a single Go 1.26.5 binary on port 8080 with embedded Vite/React assets, JSON `slog` logging, and Prometheus metrics at `/metrics`.

The existing specification requires live worker-count changes, SSE reconnect/replay, backpressure, per-worker metrics, a live event list, aggregate counters, and a demo flow across two browsers. Optional follow-up work includes Postgres persistence, dashboard filters, and reconnect backoff. Local Docker and Redis are unavailable, so unit tests and Redis-independent contracts must be the first executable foundation; Redis integration tests should be separately tagged.

### Affected Areas
- `spec.txt` — product-level behavior and demo acceptance criteria; its Pub/Sub wording should be treated as superseded by the Redis Streams decision.
- `openspec/config.yaml` — authoritative stack, testing, concurrency, and artifact conventions for the change.
- `cmd/event-pipeline/main.go` — minimal composition root for configuration, Redis client, worker supervisor, aggregation, SSE hub, HTTP routes, metrics, and embedded assets.
- `internal/config` — validated `REDIS_URL`, `EVENT_WORKERS`, HTTP settings, and runtime worker resize configuration.
- `internal/stream` — Redis Streams adapter, stream/group initialization, XADD/XREADGROUP/XACK/XRANGE, and stream-entry mapping.
- `internal/worker` — cancellable worker lifecycle, bounded processing, ack/backpressure behavior, and live resize supervisor.
- `internal/aggregation` — single-owner window state, per-second/per-minute totals, cleanup, and snapshot publication.
- `internal/sse` — client registry, bounded per-client queues, replay activation barrier, disconnect cleanup, and slow-client policy.
- `internal/httpapi` — event ingestion, SSE connection, worker configuration endpoint, health, and metrics routing.
- `internal/metrics` — low-cardinality Prometheus counters/gauges/histograms, including processed events by worker and stream/backpressure state.
- `web/src` — Vite + TypeScript + Tailwind dashboard, EventSource lifecycle, event/aggregate rendering, and worker-count selector.
- `web/vite.config.ts` (or equivalent) — build output must target the Go embed package, for example `internal/web/dist`.
- `Dockerfile`, `README.md`, and a pipeline diagram — required for the demo/deployment slice, although not required to explore further architecture.

### Approaches
1. **Single canonical Redis Stream with a shared consumer group** — use one stream such as `event-pipeline:events`, Redis-generated IDs as the authoritative event/SSE IDs, and a stable group such as `event-pipeline-workers`. Store bounded fields: `type`, `payload` (JSON), `occurred_at`, and optional bounded `source`; use a separate client `correlation_id` only when supplied.
   - Pros: one ordered log, simple XADD/XRANGE replay, natural work distribution across workers, pending entries and XACK provide observable backpressure, and no cross-stream ordering problem.
   - Cons: all event types share retention and throughput; a very high-cardinality or very large payload can pressure one stream.
   - Effort: Low

2. **Multiple streams by event type or concern** — route events to separate Redis streams and either run a group per stream or add a merger.
   - Pros: independent retention and scaling policies.
   - Cons: ordering and replay across streams become ambiguous, consumer-group management expands, and the MVP gains configuration without a demonstrated need.
   - Effort: High

3. **Client-provided IDs as canonical IDs** — accept a client event ID and use it for deduplication and SSE `id` values.
   - Pros: producer-side idempotency can be modeled explicitly.
   - Cons: IDs are not Redis stream cursors, collisions/ordering must be handled, and `XRANGE`/`Last-Event-ID` no longer share one native identifier.
   - Effort: Medium

4. **`errgroup.SetLimit` as the resizeable worker pool** — model each Redis consumer as a long-lived goroutine in an errgroup and change the limit at runtime.
   - Pros: excellent for bounded short-lived concurrent calls and error propagation.
   - Cons: `SetLimit` is not a live worker-count controller; changing configuration does not safely stop already-running consumers, and an errgroup is awkward for independently starting/stopping long-lived workers.
   - Effort: High / unsuitable for live resize

5. **Supervisor-managed workers with per-worker cancellation** — keep a supervisor as the lifecycle owner, track desired count with an atomic/config value plus a control channel, start workers until the target is reached, and cancel/wait workers when reducing the target. Each worker has a unique consumer name and exits on context cancellation.
   - Pros: explicit ownership and shutdown, deterministic resize semantics, simple `XREADGROUP` consumers, and direct metrics for active workers; it follows structured concurrency without pretending `SetLimit` is dynamic.
   - Cons: requires a small amount of lifecycle code and a policy for entries pending when a worker stops.
   - Effort: Medium

6. **Mutex-protected SSE registry with bounded per-client channels** — the hub owns a map of client IDs to client structs, uses a mutex only for registry changes/snapshots, and sends immutable event values to each client's bounded queue. A slow client is disconnected or marked as lagging when its queue is full.
   - Pros: straightforward fan-out, isolated slow clients, easy disconnect cleanup, and no shared mutable payloads.
   - Cons: fan-out is O(clients) per event; queue-full behavior must be explicit.
   - Effort: Medium

7. **Single broadcast channel with per-client forwarding goroutines** — the hub publishes once to a broadcast channel and each client forwarder reads it.
   - Pros: central publisher API is simple.
   - Cons: a slow forwarder can still consume unbounded memory or distort delivery semantics; lifecycle and drop behavior are harder to reason about than direct bounded queues.
   - Effort: Medium / less suitable

8. **Aggregation owned by one goroutine** — workers send value copies to an aggregation input channel; one goroutine owns the second/minute maps, advances a ticker, expires old windows, and emits immutable snapshots.
   - Pros: no map races, deterministic window transitions, simple tests, and no mutex on the hot path.
   - Cons: one aggregation goroutine is a throughput ceiling; state is process-local and lost on restart.
   - Effort: Medium

### Recommendation
Use one canonical stream, `event-pipeline:events`, with Redis auto-generated IDs. Treat the Redis ID as the only replay cursor and SSE `id`; keep any producer correlation ID as a separate field. Use one stable consumer group, `event-pipeline-workers`, and unique consumer names containing instance identity plus worker slot. Start the group explicitly and use `XACK` only after successful aggregation/publication; retain pending entries for recovery and expose pending counts as metrics.

Implement live resizing with a supervisor, per-worker contexts, a control channel, and a wait for stopped workers before reporting the new active count. `errgroup.SetLimit` may still be used inside a worker for bounded short-lived processing, but it should not own the resizeable long-lived worker pool. The supervisor must define whether a stopped worker's pending entries are reclaimed with `XAUTOCLAIM`/claim-on-start or left for Redis recovery; the MVP should document and test one policy.

Make SSE replay an explicit two-phase subscription. A client is registered with a bounded queue and a replay gate; the handler reads `Last-Event-ID`, runs exclusive `XRANGE (lastID`, then opens the gate only after replay entries are queued. Live broadcasts are held behind that gate, preserving order. The same normalized event envelope must be used for replay and live delivery, and event IDs must remain Redis stream IDs. If aggregate snapshots are emitted separately, they need their own documented replay rule; the MVP should prefer replayable event messages plus periodic/current aggregate snapshots rather than claiming that a derived snapshot is recoverable from an event cursor.

Keep aggregation single-owner: workers send immutable processed-event values, the aggregator updates per-second and per-minute counters keyed by UTC window and event type, and a ticker removes windows older than a bounded retention period (for example, five minutes). Publish snapshots on a bounded cadence or after each event with coalescing. Do not expose the maps directly to HTTP or SSE code. Use a mutex only at the hub boundary and atomics for simple gauges/counters, not for compound aggregation state.

Recommended layout:

```text
cmd/event-pipeline/main.go
internal/config/
internal/stream/
internal/worker/
internal/aggregation/
internal/sse/
internal/httpapi/
internal/metrics/
internal/web/          # Go embed package and generated dist/
web/src/
web/src/components/
web/src/hooks/
web/src/lib/
web/src/types/
web/index.html
web/vite.config.ts
Dockerfile
README.md
```

The frontend should use a typed `useEventStream` hook around native `EventSource`, reconnecting with the browser-managed `Last-Event-ID` behavior and rendering a bounded client-side event list. It should not fabricate telemetry. The worker selector should call an authenticated-or-local admin endpoint such as `PUT /api/config/workers` with `{count}`, receive the validated active/desired counts, and refresh from `GET /api/config/workers`; the endpoint must reject invalid bounds and report a transition rather than pretending a resize is instantaneous.

### First-Slice Scope
The MVP MUST include:
- validated `POST /events` with a bounded JSON schema and Redis `XADD`;
- stream/group initialization and N cancellable consumers with `XREADGROUP`, success-based `XACK`, and an explicit pending-entry recovery policy;
- supervisor-controlled live worker resize through the configuration endpoint;
- per-second and per-minute in-memory counters by event type plus totals, with bounded cleanup;
- SSE live delivery, disconnect cleanup, bounded per-client queues, and `Last-Event-ID` replay via exclusive `XRANGE`;
- React/Vite/TypeScript/Tailwind dashboard with live events, aggregate counters, real event generation button, and worker-count selector;
- `/metrics`, structured JSON logs, health endpoint, graceful shutdown, and bounded operational settings;
- co-located unit tests, race tests, goroutine leak checks, and tagged Redis integration tests; the configured commands remain `go test ./...` and `go test -race -cover ./...` with 80% coverage target;
- single-binary embedding, Dockerfile, README, and the producer → Redis → workers → aggregation → SSE → dashboard diagram.

Follow-up slices MAY include Postgres window persistence, event-type dashboard filters, reconnect backoff/jitter beyond native EventSource behavior, richer retry/backoff policies, multi-instance SSE coordination, dead-letter streams, and advanced Redis retention/claim tooling.

### Risks
- Redis is not installed locally and Docker is unavailable, so Redis behavior cannot be validated by default in this environment. Keep Redis behind small interfaces, use deterministic fakes for unit tests, and run `//go:build integration` tests only when `REDIS_URL` is available.
- The original `spec.txt` says Pub/Sub while the resolved architecture says Streams. Proposal/spec artifacts MUST use Streams consistently to avoid implementing a non-replayable transport.
- Replay has a race window unless registration, queueing, and activation are designed as one protocol. A naive “XRANGE then subscribe” loses events; a naive “subscribe then XRANGE” reorders them.
- Bounded SSE queues require a clear slow-client policy. Blocking the publisher creates global backpressure; dropping silently breaks replay expectations. Prefer disconnecting lagging clients and allowing reconnect from their last acknowledged ID.
- Worker reduction can leave pending entries owned by a stopped consumer. Define claim/recovery behavior and surface pending counts before presenting resize as reliable.
- Redis stream IDs are ordered cursors, not globally unique client IDs supplied by producers. Do not overload the field schema or permit arbitrary client IDs as SSE cursors.
- In-memory aggregation disappears on restart and is not shared across replicas. This is acceptable for a single-container demo, but must be stated as an MVP boundary.
- Prometheus labels must remain bounded: event type and worker slot require validation/limits; never label by payload, client ID, or Redis entry ID.
- Testing ticker/window behavior and worker resize with real time can be flaky. Prefer injected clocks or deterministic synchronization, plus `goleak` and `-race` coverage for lifecycle paths.
- Embedding Vite output from a sibling source directory is a build-layout trap. Configure the frontend build output inside the Go embed package and make the build order explicit.

### Ready for Proposal
Yes — the architecture and first slice are sufficiently scoped for `sdd-propose`. The proposal should carry forward the Streams decision, supervisor-based live resize, replay activation barrier, single-owner aggregation, the Redis-unavailable integration-test boundary, and the explicit pending-entry recovery policy as acceptance criteria.
