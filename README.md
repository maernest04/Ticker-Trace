# Market Execution Lab

A deployable stock-execution laboratory for testing simulated market and limit orders against live or replayed quote and trade streams. The application explains fill price, spread cost, slippage, time to fill, and the effect of artificial processing latency.

The system is an educational and research simulator. It does not place real orders, provide investment recommendations, reconstruct full Level 2 books, or claim exchange-accurate fills.

## Project Goals

- Help technically curious traders and quant students understand how timing and visible liquidity affect order execution.
- Provide a usable web interface for live simulation and deterministic replay.
- Process market and order events concurrently while preserving per-symbol ordering and idempotency.
- Recover unfinished work after worker failure without producing duplicate fills.
- Produce reproducible performance and reliability results suitable for technical interviews and resume bullets.
- Remain small enough for one developer to build, deploy, test, and explain thoroughly.

## Planned Documentation

- [Product specification](docs/PRODUCT_SPEC.md)
- [Implementation plan](docs/IMPLEMENTATION_PLAN.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Data pipeline](docs/DATA_PIPELINE.md)
- [UI plan](docs/UI_PLAN.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Testing](docs/TESTING.md)
- [Resume bullets and benchmark metrics](docs/RESUME_BULLETS.md)

## Current Status

- [x] Select the product concept.
- [x] Define the target user and primary workflow.
- [x] Confirm initial market-data availability and public-display restrictions.
- [x] Approve the MVP scope and non-goals.
- [x] Select the initial technology stack.
- [x] Define architecture, data flow, UI, deployment, and test boundaries.
- [x] Complete the core streaming pipeline.
- [x] Add operational health, readiness, structured logs, and pipeline metrics.
- [x] Add a credential-gated private Alpaca IEX ingestion adapter.
- [x] Add the public execution-workspace interface and simulated-order submission.

## Technology Stack

- Python, FastAPI, Pydantic, and AsyncIO for ingestion, APIs, and WebSockets
- Redis Streams consumer groups for event delivery
- Redis hashes and sorted sets for low-latency materialized state
- Independently runnable Python worker processes for execution and replay
- PostgreSQL for durable events, orders, fills, sessions, and benchmark results
- Next.js and TypeScript for the web interface
- Docker Compose for local orchestration and containers for deployment
- Prometheus-compatible metrics and Grafana dashboards

TimescaleDB is deferred until measured historical-query or retention requirements justify it. Kafka, real-money trading, authentication, options, smart order routing, and full market depth are not part of the MVP.

## Data Modes

- **Private live mode:** streams real IEX trades and quotes through an operator-owned Alpaca account. This mode is not publicly redistributed.
- **Public demo mode:** uses generated or explicitly redistribution-safe replay data and exposes the complete product workflow without vendor credentials.

## Run the Execution Workspace

Start Redis and PostgreSQL, apply migrations, and run the API in public replay mode. From `frontend`, copy `.env.example` to `.env.local`, then run `npm install` and `npm run dev`. The interface is available at `http://localhost:3000` and proxies browser API calls to `http://localhost:8000` by default.

The workspace queues a simulated order. The order-result timeline and live worker updates are Phase 4B.
