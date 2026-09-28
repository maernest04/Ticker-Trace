# Resume Bullets

## Project

**Market Execution Lab — Python, FastAPI, Redis Streams, PostgreSQL, Next.js, Docker**

## Draft Bullets

- Built a distributed stock-execution simulator that streams live and replays recorded market data, explaining fill price, slippage, liquidity, and latency across simulated market and limit orders
- Engineered an `asyncio` ingestion pipeline and idempotent Redis Streams workers, sustaining `[X]` events/sec at `[X] ms` p95 latency while preserving per-symbol event ordering
- Implemented deterministic replay and worker-failure recovery across `[X]` concurrent processes, reducing experiment runtime by `[X]%` with zero duplicate fills across `[X]` test events

## Alternative UI-Focused First Bullet

- Built and deployed an interactive stock-execution lab for comparing simulated orders across live and replayed market conditions, visualizing spread cost, slippage, fill rate, and latency impact in real time

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
