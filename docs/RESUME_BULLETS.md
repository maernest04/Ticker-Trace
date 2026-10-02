# Resume Bullets

## Project

**Market Execution Lab — Python, FastAPI, Redis Streams, PostgreSQL, Next.js, Docker**

## Draft Bullets

- Built a deployable stock-execution lab with generated replay experiments and a separate private Alpaca IEX pipeline, tracing simulated market/limit fills to their triggering quotes
- Engineered independently runnable Python consumers with Redis Streams, PostgreSQL, freshness checks, and retained-event recovery; validated `[X]` events through the documented local continuous-load workload
- Implemented deterministic replay, partial fills, and latency comparisons in Next.js; measured `[X]%` higher finite-replay throughput across `[X]` concurrent local benchmark threads

## Alternative UI-Focused First Bullet

- Built and deployed a generated-data stock-execution dashboard for comparing order size and latency, visualizing spread cost, fill rate, and quote-linked execution outcomes

## Metrics to Collect

- Events processed per second
- End-to-end p50, p95, and p99 latency
- Replay runtime and speedup with one versus multiple workers
- Number of events tested without duplicate fills
- Worker failure-recovery time
- Number of execution scenarios evaluated
- Cache hit rate
- Queue depth and consumer lag under peak load

Do not replace `[X]` placeholders until the corresponding result has been measured with a reproducible benchmark.

The original saved finite-replay result used local threads, not distributed processes. The one-million-event check is in-memory only. The pre-Phase 7 continuous smoke result is short local evidence, not sustained real-feed or production performance. Do not claim historical vendor-data replay, a deployed private-live UI, end-to-end fill/browser p95 latency, or 24-hour quota safety until separately verified.
