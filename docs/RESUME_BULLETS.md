# Resume Bullets

## Final compact version

**Ticker Trace — Python, FastAPI, asyncio, Redis Streams, PostgreSQL, Next.js, Docker**

Use these three bullets together:

- Built a stock-execution simulator with asyncio and Redis Streams, routing Alpaca IEX data through independent workers
- Sustained 120K generated events over 10 minutes with 8.06 ms p95 PostgreSQL write latency and zero lost or duplicate events
- Built a Next.js replay lab with immutable recordings, quote-linked fills, and first-divergence latency comparisons

## Alternative benchmark bullet

- Persisted 169.2K generated events over 10 minutes at a 282/sec target with 37.57 ms p95 write latency and zero event loss

Choose one benchmark bullet; never combine the rate from one test with the latency from another. Line length depends on resume fonts/margins; these are compact prose, not a verified LaTeX layout.

## Historical local evidence — October 2

- Built a Python/FastAPI stock-execution simulator with private Alpaca IEX streaming, Redis caching, PostgreSQL, and a Next.js UI; verified five live-quote-linked simulated fills without duplicates
- Sustained 120K generated events over 10 minutes through Redis Streams and PostgreSQL using four partitions and concurrent local consumers; measured 8.06 ms p95 durable-write latency with zero lost or duplicate events
- Engineered coordinated session rollover and retained-history recovery across ingestion, execution, and persistence processes; validated three generated-feed rollovers and four lease-expiry pause/resume cycles without duplicate fills

The latency bullet measures enqueue-to-market-event commit, not fill or browser latency. The endurance test used an arbitrary 200 events/sec target and local threads, not twice a measured live peak or distributed-machine scaling. The five real-feed fills predate the Part C update; pause/rollover tests use generated data. Keep these qualifiers available in the README and interview explanation.

## Metrics to Collect

- Events processed per second
- End-to-end p50, p95, and p99 latency
- Replay runtime and speedup with one versus multiple workers
- Number of events tested without duplicate fills
- Worker failure-recovery time
- Number of execution scenarios evaluated
- Cache hit rate
- Queue depth and consumer lag under peak load

Only add metrics after collecting reproducible evidence. Cache hit rate, fill/browser p95, and recovery duration have no final measured claims here.

The original finite-replay result used local threads, not multiple machines. Million-event dedupe is in-memory only. Actual private capture/offline reproduction passed October 6 under user-reported storage permission; see [actual-input evidence](../benchmarks/extension-live-recording-acceptance-2026-10-06.md). Recovery tests cover scoped retries/commit boundaries, not physical laptop recovery or exactly-once transport. Do not claim production adoption, validated demand, profitability, exchange-exact fills, a hosted private-live UI, fill/browser p95, or 24-hour quota safety.

Metric sources: [200/sec endurance JSON](../benchmarks/local-c-endurance.json), [282/sec certification JSON](../benchmarks/local-c-live-rate-2026-10-05.json), and [recovery evidence](../benchmarks/extension-abc-acceptance-2026-10-05.md). Both continuous tests use four partitions and two local threads per consumer role. Latency ends at PostgreSQL market-event commit, not fill-result commit or browser delivery.
