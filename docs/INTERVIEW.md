# Ticker Trace: Interview Narrative

## Thirty-second explanation

Ticker Trace submits simulated stock orders against private live quotes, saves a bounded interval, and replays two configurations against identical input. It explains the first source event where execution diverges, rather than only showing a chart. Python ingestion, execution, and persistence have independent process roles connected by Redis Streams; PostgreSQL retains quote-linked fills and Next.js exposes the evidence. Public demonstrations use generated data; actual IEX recordings stay local.

## Why the technology belongs

- **Concurrency:** asyncio handles feed/API I/O; fixed symbol partitions allow unrelated work to progress while preserving source order within each partition.
- **Distributed boundaries:** roles communicate through Redis, not shared Python memory, and restart independently. Demonstrated operation is local multi-process; measured scaling is local threads, not multiple machines.
- **Caching:** Redis holds quote/order state and freshness timestamps, avoiding SQL reads for each tick. Stale/disconnected input disables live orders.
- **Durability and retries:** persistence acknowledges after commit; stable identities and retained-source reconstruction avoid duplicated outcomes in scoped recovery tests. Delivery remains at least once; leases do not fence in-flight SQL.
- **Streaming:** live input is continuous. Finite recorded input is published before processing; pacing and browser animation are not live engine capacity.

## Differentiator

Same validated dataset, source-entry boundary, and model; vary one parameter; inspect the earliest eligibility/state/fill divergence and original quote. Final prices can match despite different fill times. This auditable counterfactual is narrower than a stock tracker, but replay is established technology and independent demand is unvalidated.

## Defensible measurements

Use the [benchmark table](BENCHMARKS.md#published-results-at-a-glance): either 120K generated events at a 200/sec target with 8.06 ms p95, or 169.2K at 282/sec with 37.57 ms p95. Latency ends at market-event SQL commit. Million-event dedupe is engine-only; actual-feed checks and recordings are separate correctness evidence.

## Hard questions

**Why not Kafka or TimescaleDB?** Redis consumer groups and PostgreSQL satisfy this bounded workload with fewer operational components. Add infrastructure only when measured retention, query, or partition needs justify it.

**Broker-realistic?** Independent top-of-book model, not queue position, depth, shared liquidity, fees, market impact, corrections/cancellations, or broker-exact fills. IEX is not consolidated market coverage; the adapter assumes 100-share quote lots.

**Remaining gaps?** Physical sleep/wake and network interruption, reliable unattended cold boot, post-extension fresh-live browser regression, and owner workflow completion without assistance. Demand, multi-machine scaling, and hosted idle quotas are not claimed.

**Execution or reconciliation?** Execution matches the implemented inspectable-order workflow. Reconciliation would require a separate ledger and break-detection product. For post-trade roles, discuss transferable durable processing, idempotency, and recovery without relabelling this as reconciliation.
