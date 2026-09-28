# Architecture

## Architectural Goals

- Support real-time market-data ingestion and UI updates.
- Isolate ingestion from downstream processing.
- Scale processing workers independently.
- Preserve correctness during retries and worker restarts.
- Keep frequently accessed state available at low latency.
- Retain enough history for replay and analysis.
- Make throughput, latency, and failures observable.
- Run the same deterministic execution engine for live and replay inputs.
- Prevent restricted live market data from entering the public deployment.

## Component Model

```text
Alpaca IEX or Replay Source
            |
            v
     Ingestion / Replay
            |
            v
 Partitioned Redis Streams <------- FastAPI Order Commands
            |
       +----+----------------+
       |                     |
       v                     v
 Engine Workers       Persistence Worker
       |                     |
       v                     v
 Redis Materialized State   PostgreSQL
       |                     |
       +----------+----------+
                  |
                  v
              FastAPI
          REST + WebSockets
                  |
                  v
          Next.js / TypeScript
```

## Component Responsibilities

### Ingestion Service

- Maintains the external data connection.
- Converts Alpaca or fixture-specific messages into versioned internal events.
- Assigns ingestion timestamps and identifiers.
- Routes each event to a stable symbol partition.
- Publishes events without performing execution calculations.
- Exists only in private live mode when using Alpaca credentials.

### Replay Service

- Reads generated, licensed, or private recorded events.
- Preserves source event time while assigning replay metadata.
- Publishes through the same partitioning path as live ingestion.
- Supports 1x, 10x, and maximum safe playback.
- Uses a run-specific namespace so replay cannot modify live state.

### Redis Streams

- Uses a fixed number of symbol partitions chosen before a run.
- Routes a symbol with a stable hash so all of its market and order events share one partition.
- Provides separate engine and persistence consumer groups.
- Uses at-least-once delivery; workers must therefore be idempotent.
- Retains pending events for recovery and exposes consumer lag.
- Sends permanently invalid events to a dead-letter stream.

### Engine Workers

- Own one or more stream partitions, with only one active engine consumer per partition.
- Process unrelated symbols and partitions concurrently while keeping one symbol ordered.
- Activate simulated orders according to event time plus configured latency.
- Apply the documented top-of-book fill rules.
- Store processed-event and fill identifiers so retries cannot duplicate state.
- Produce order-state, fill, explanation, and metric events.

### Persistence Worker

- Consumes normalized market, order, fill, and replay events through an independent consumer group.
- Writes bounded batches to PostgreSQL.
- Retries transient failures without blocking engine workers.
- Exposes write backlog and batch-duration metrics.

### State Cache

- Stores current quote/trade state, active orders, positions, and session progress.
- Separates live and replay keys by namespace and run identifier.
- Uses expiration for inactive sessions, not for durable business records.
- Is rebuildable from PostgreSQL and retained stream events.

### PostgreSQL

- Stores normalized events, replay sessions, simulated orders, fills, explanations, and benchmark results.
- Supports historical queries and deterministic replay.
- Remains the source of truth for durable application data.
- Uses regular PostgreSQL tables for the MVP; TimescaleDB is deferred until benchmarks justify it.

### API and WebSocket Gateway

- Exposes symbols, current state, order submission, order results, and replay control.
- Validates user input.
- Enforces public-demo versus private-live mode.
- Coalesces backend events into browser updates at a bounded rate rather than forwarding every tick.
- Does not own long-running processing work.

## Repository Boundary

Use one repository with separately runnable applications:

```text
frontend/             Next.js application
services/api/         FastAPI REST and WebSocket gateway
services/ingestion/   Private Alpaca adapter
services/engine/      Partitioned execution workers
services/persistence/ PostgreSQL writer
services/replay/      Fixture and replay producer
shared/               Event and domain contracts
tests/                Unit, integration, replay, and load tests
infra/                Local containers and deployment configuration
```

The exact folder names may change during repository scaffolding, but service ownership must remain intact.

## Concurrency Model

- AsyncIO handles provider WebSockets, Redis I/O, database batching, API requests, and browser WebSockets.
- Separate processes provide failure isolation and parallel replay/engine execution.
- Symbols are assigned to fixed stream partitions with a stable hash.
- One active engine consumer owns each partition, preserving stream order within that partition.
- Different partitions execute concurrently across worker processes.
- Replay runs can execute concurrently because every run has an isolated namespace.
- Persistence may batch across symbols because it does not make execution decisions.
- The API never directly mutates order state; it appends an order command to the owning partition.

Scaling adds workers until every partition has an owner. Increasing the partition count requires starting a new run or a controlled repartitioning procedure; it is not changed while a run is active.

## Caching Strategy

| Key pattern | Contents | Owner | Durability |
| --- | --- | --- | --- |
| `state:{mode}:{run}:{symbol}` | Latest quote, trade, sequence, and freshness | Engine | Rebuildable |
| `order:{mode}:{run}:{order_id}` | Current order state and remaining quantity | Engine | PostgreSQL-backed |
| `active:{mode}:{run}:{symbol}` | Active order identifiers | Engine | Rebuildable |
| `session:{mode}:{run}` | Replay progress and status | Replay/engine | PostgreSQL-backed |
| `dedupe:{mode}:{run}:{partition}` | Recently processed event identifiers | Engine | Stream/database-backed |

Redis is the low-latency materialized view, not the sole durable source of truth. Session keys may expire after an inactivity window determined during implementation; live market-state keys remain freshness-stamped so stale data is visible rather than silently trusted.

## API Boundary

Initial REST resources:

- `GET /health` and `GET /ready`
- `GET /api/v1/symbols`
- `GET /api/v1/market/{symbol}`
- `POST /api/v1/orders`
- `GET /api/v1/orders/{order_id}`
- `POST /api/v1/replays`
- `GET /api/v1/replays/{run_id}`

Initial WebSocket resource:

- `GET /ws/v1/sessions/{run_id}` for market snapshots, order transitions, fills, replay progress, and connection health

Schemas are versioned. Unknown fields may be ignored for forward compatibility, but unknown schema versions are rejected.

## Failure Model

- **Market-data disconnection:** mark live data stale, stop new simulated live orders, reconnect with bounded exponential backoff, and never invent missing events.
- **Duplicate event:** detect the event identifier before applying state; acknowledge without creating another fill.
- **Stale or out-of-order event:** persist and count it, but do not replace newer current state or retroactively alter completed fills.
- **Worker termination:** leave unacknowledged entries pending; a replacement consumer claims them and resumes idempotently.
- **Redis unavailable:** fail readiness, stop accepting new orders, and reconnect without acknowledging unprocessed work.
- **Database slow or unavailable:** allow the persistence backlog to grow to a configured threshold; then pause producers rather than silently discard durable events.
- **Cache loss:** rebuild materialized state from durable events before accepting new orders.
- **Browser disconnect:** retain the session server-side; the client fetches current state and resumes WebSocket updates after reconnecting.

## Architecture Decisions

- [x] Redis Streams rather than Kafka for the MVP
- [x] PostgreSQL rather than TimescaleDB initially
- [x] One repository with independently runnable services
- [x] Stable symbol hash as the market/order partitioning key
- [x] At-least-once delivery with idempotent workers
- [x] One deterministic execution engine for live and replay modes
- [x] No user authentication in the public replay MVP
- [x] Private live mode controlled by operator deployment configuration
