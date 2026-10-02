# Data Pipeline

## Current implemented flow — pre-Phase 7

The sections below the historical-proposal heading describe initial intent, not current guarantees.

### Contracts and units

Pydantic quote/trade envelopes contain event ID, schema version, run ID, symbol, event/ingestion times, sequence, partition, and typed Decimal prices/whole-share quantities. Alpaca quotes report round lots; this MVP assumes 100 shares per lot, while trades already report shares. Verify symbol lot units before using other securities. Quote identifiers include timestamp, prices, and sizes to preserve liquidity-only changes.

Stream messages currently use `replay.started.v1`, `order.command.v1`, `market.event.v1`, `replay.completed.v1`, and `execution.result.v1`. Fills/transitions are fields in result snapshots, not separate event channels. Malformed messages are dead-lettered.

### Public replay

1. Validate one of nine generated datasets/order commands and reserve weighted monthly credits on public Fly.
2. Wake/check workers, then publish header, command, ordered events, and completion marker into an isolated run stream.
3. Reconstruct/finalize the simulation, verify against pure execution, cache state, and emit an execution result.
4. Persist source and result data; return after demand-dispatched persistence completes.
5. Deliver a completed WebSocket snapshot. The UI's 1×/5×/20× controls animate it locally.

Three versioned longer datasets each contain 240 quotes and 60 trades across about six event-time seconds, varying spread, visible liquidity, volatility, and gaps. CLI `--playback-speed` or `--events-per-second` paces backend publication, mutually exclusively. Dispatch occurs after the completion marker; finite replay does not incrementally consume a partially published fixture. Public HTTP submissions retain maximum-speed publication.

### Private live

1. Explicitly start one operator ingestion session, initialize partitions/groups, acquire ownership, and register run/symbol/connection status.
2. Authenticate, subscribe, normalize, and publish real incoming quotes/trades. Reconnect transient disconnects with bounded backoff; fatal provider rejection stops the feed; invalid messages are skipped.
3. Pause production when source consumer backlog reaches 10,000. Stop at 100,000 retained messages per partition rather than deleting recovery history.
4. Continuous engine consumers process commands and subsequent quotes; each independent order emits progress snapshots on state/quantity changes.
5. Continuous persistence commits batches and snapshots, retaining a running session and deterministic fill IDs.
6. Private UI/API expose ongoing quotes, received/provider quote age, subscription controls, event trace, and durable fill explanations.

Live orders require the current `run_id`, no generated scenario, a subscribed symbol, and fresh provider/received quotes plus a connected feed within 15 seconds. Public orders require a generated scenario. Subscription changes are validated/persisted and sent through `ingestion.control` without environment edits.

### Recovery, metrics, and limits

Market/order hashes retain their legacy `state:replay`/`order:replay` names but isolate live data by run UUID. PostgreSQL stores durable events/results; benchmark JSON stays outside the database. Private streams are bounded, retained operator history; public TTL/pruning does not delete them. Restart ingestion and both workers together to change sessions; stop private operation when finished.

Metrics include weighted fill price, fill rate, signed spread cost in currency per share, activation-to-fill times, and replay latency impact. Zero values remain zero. No separate brokerage-slippage, fee, basis-point spread, or market-impact model is implemented. Continuous certification samples active source/result backlog and event enqueue-to-durable-write latency; public tiny-fixture rates are not capacity evidence.

## Historical original proposal (not implemented guarantees)

## Objective

Define how market events enter the system, become trusted internal events, update distributed state, reach storage, and appear in the UI.

## Event Lifecycle

```text
Receive -> Validate -> Normalize -> Partition -> Publish -> Process -> Cache -> Persist -> Broadcast
```

## Ingestion

### Private Live Source

- **Provider:** Alpaca Basic IEX stock feed
- **Protocol:** Authenticated WebSocket
- **Events:** Trades, quotes, and provider status/correction messages needed by the selected symbols
- **Initial symbols:** Operator-configured list of up to 10 liquid U.S. equities
- **Timestamps:** Preserve provider timestamp and add local receive and publish timestamps
- **Connection behavior:** One ingestion connection with heartbeat/freshness monitoring and bounded reconnect backoff
- **Restrictions:** Live vendor data remains private and is never enabled in the public deployment

### Public Replay Source

- Generated deterministic fixtures are the default.
- Any external replay dataset must have documented redistribution rights before inclusion.
- Fixtures cover normal, volatile, illiquid, gap, duplicate, stale, malformed, and worker-failure scenarios.
- Replay uses the same normalized events and partitioning logic as live ingestion.

## Normalized Event Envelope

Every internal event should include:

- Unique event identifier
- Event type and schema version
- Instrument identifier
- Provider event timestamp
- Local ingestion timestamp
- Source and source sequence number when available
- Trace or correlation identifier
- Typed payload
- Run mode and replay/session identifier
- Partition identifier

Initial event types:

- `market.quote.v1`
- `market.trade.v1`
- `order.submitted.v1`
- `order.activated.v1`
- `order.cancelled.v1`
- `execution.fill.v1`
- `order.state_changed.v1`
- `replay.state_changed.v1`
- `pipeline.dead_lettered.v1`

## Ordering and Delivery

- Ordering is required per symbol within a run.
- A stable symbol hash selects one of a fixed set of Redis Stream partitions.
- Market events and order commands for the same symbol enter the same partition.
- The Redis Stream entry order is the canonical processing order after ingestion.
- Provider timestamps remain available for freshness checks and explanation.
- Stale provider events are persisted and counted but do not replace newer cached state.
- Every event has a deterministic idempotency identifier; every fill has a deterministic identifier derived from order and triggering event.
- Delivery is at least once. A worker records its idempotency decision before acknowledging an entry.
- Transient failures remain pending and are retried with a bounded count.
- Invalid or permanently failing events enter a dead-letter stream with their reason and original identifier.

## Stream Consumers

List each consumer and the state it owns.

| Consumer | Input | Output | Partition Key | Cached State |
| --- | --- | --- | --- | --- |
| Engine | Market events and order commands | Order transitions, fills, explanations | Symbol | Market and order state |
| Persistence | All normalized and derived events | PostgreSQL rows | Stream partition | Batch cursor only |
| WebSocket publisher | Materialized state changes | Coalesced client messages | Replay/session ID | Connection state |
| Metrics collector | Service and stream metrics | Prometheus-compatible samples | Service/partition | Rolling counters |

## Cache and Persistence

- Current, frequently read state belongs in Redis.
- Durable events and historical results belong in PostgreSQL.
- Cached state must be rebuildable.
- Persistence uses bounded batches based on row count and maximum wait time.
- The persistence worker acknowledges only after a successful database transaction.
- TimescaleDB remains a measured future optimization rather than an MVP dependency.

Initial durable entities:

- Replay session
- Normalized market event
- Simulated order
- Fill
- Order-state transition and explanation
- Benchmark run and result

## Replay

Replay uses the same normalized event model and engine as live processing.

- Generated fixtures are selected by dataset identifier and immutable version.
- Source event timing and ordering metadata are preserved.
- Playback supports 1x, 10x, and maximum safe speed.
- Every run receives isolated stream and cache namespaces.
- Identical dataset version, order commands, worker count, and engine version must produce identical fills and final state.
- Public replay never reads private recorded vendor data.

## Execution Calculations

- **Weighted average fill price:** quantity-weighted mean of fill prices.
- **Fill rate:** filled quantity divided by submitted quantity.
- **Time to first fill:** first fill event time minus order activation time.
- **Time to completion:** final fill event time minus order activation time.
- **Spread cost:** signed difference between fill price and arrival midpoint, reported in currency and basis points.
- **Latency impact:** difference between the configured-latency result and the same replayed order with zero configured latency.

Calculations must identify the quote and event timestamps used. Results with incomplete fills remain explicitly incomplete rather than extrapolated.

## Backpressure

- Queue depth, oldest pending age, and consumer lag are measured per partition and group.
- Ingestion pauses when the persistence backlog reaches a configured safety threshold.
- The API rejects new orders when engine lag or market-data freshness exceeds its threshold.
- Browser updates are coalesced to a bounded frequency; slow clients receive the newest snapshot instead of every intermediate UI update.
- Replay maximum speed adapts to lag rather than dropping events.
- Threshold values are configuration, recorded with benchmark results, and tuned during load testing.

## Pipeline Metrics

- Events received, accepted, rejected, and processed
- Events processed per second
- End-to-end and stage-level latency percentiles
- Queue depth and consumer lag
- Duplicate and out-of-order event counts
- Cache hit rate
- Database batch size and write duration
- Reconnects, retries, and dead-letter events
- Worker claims and recovery duration
- Fill count and duplicate-fill suppression count
- Browser snapshot rate and delivery age
