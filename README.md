# Market Execution Lab

A deployable stock-execution laboratory for explaining simulated market/limit fills, visible liquidity, spread cost, time to fill, and artificial latency. The public demo runs deterministic generated stock scenarios. A separate private deployment connects ongoing Alpaca IEX quotes to independently runnable engine and persistence workers.

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
- [Benchmark methodology](docs/BENCHMARKS.md)
- [Resume bullets and benchmark metrics](docs/RESUME_BULLETS.md)
- [Pre-Phase 7 build and acceptance checklist](docs/PRE_PHASE_7.md)

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
- [x] Add reproducible fixed-partition load benchmarks.
- [x] Add a reproducible high-volume validation command.
- [x] Connect private session order submission, continuous workers, quote freshness, subscription controls, and a private live UI.
- [x] Add three versioned 300-event public experiments alongside the six small regression fixtures.
- [x] Add backend CLI pacing and continuous offered-load/backlog/durable-write latency validation.
- [x] Verify actual private market-window fills against persisted subsequent IEX quotes and automatic same-session worker replacement.
- [x] Complete Local A bounded lease retry, graceful cleanup, and reconstruction ownership renewal.
- [ ] Complete Local B session lifecycle/retention, then Local C ten-minute certification at twice the measured live peak.
- [ ] Deferred cloud acceptance: redeployed concurrent-browser and 24-hour idle-provider-usage checks.

## Technology Stack

- Python, FastAPI, Pydantic, and AsyncIO for ingestion, APIs, and WebSockets
- Redis Streams consumer groups for event delivery
- Redis hashes for cached market/order state and connection status
- Independently runnable Python worker processes for execution and replay
- PostgreSQL for durable events, orders, fills, sessions, and watchlists; benchmark evidence is stored as JSON files
- Next.js and TypeScript for the web interface
- Docker Compose for local orchestration and containers for deployment
- Structured logs and per-replay Prometheus-compatible metrics; no bundled Grafana dashboards

TimescaleDB is deferred until measured historical-query or retention requirements justify it. Kafka, real-money trading, authentication, options, smart order routing, and full market depth are not part of the MVP.

## Data Modes

- **Private live mode:** operator-owned Alpaca IEX feed, continuous partition workers, simulated orders against subsequent quotes, and a private UI. Use separate local/private infrastructure, never the public Fly app. Actual quote-linked fills and automatic same-session worker replacement were verified locally October 2; coordinated session recovery and sustained-rate certification remain pending. Generated adapter integration tests are separate from actual provider evidence.
- **Public demo mode:** nine generated datasets without vendor credentials. Fly demand dispatch finishes engine and persistence work before returning the completed snapshot; the browser animates the recorded trace. This is not live stock pricing or intermediate-worker streaming.

## Run the Execution Workspace

Start Redis and PostgreSQL, apply migrations, run the API in public replay mode, then start `market-execution-engine --forever` and `market-execution-persistence --forever` as separate worker processes. From `frontend`, copy `.env.example` to `.env.local`, then run `npm install` and `npm run dev`. The interface is available at `http://localhost:3000` and proxies browser API calls to `http://localhost:8000` by default.

The workspace displays the simulated order's replay trace, lifecycle, fills, and execution metrics. The UI's 1×/5×/20× controls affect client animation only. Backend event-time or fixed-rate pacing is available through `market-execution-replay --playback-speed 10` or `--events-per-second 100`; neither is enabled for long-running public HTTP submissions.

## Private Live Setup

Local live execution is the current primary workflow; online deployment is deferred. It uses separate laptop-hosted services and needs internet only for the live feed, not hosted Redis/PostgreSQL. Generated replay remains the offline test path. The remaining Local A/B/C plan covers automatic restart recovery, coordinated session rollover/retention, and final ten-minute evidence in [IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md#local-first-completion-plan--october-2-2026).

Copy `infra/.env.private.example` to ignored `infra/.env.private`, supply operator credentials and a local database password, then run:

```bash
docker compose --env-file infra/.env.private -f infra/docker-compose.private.yml up --build
```

Open `http://localhost:3030`. The API binds to loopback on port 8030. Update up to ten symbols from the UI. A new session currently requires stopping ingestion and both workers, allowing old ownership leases to expire, starting ingestion, confirming the new session, and then starting both workers. Follow the [local runbook](docs/DEPLOYMENT.md#separate-private-live-stack), including the separate commands for the current `tickertrace-local` instance. Stop the private stack when finished; closing the browser alone does not stop it. Private endpoints have no authentication and must not be exposed to the internet; CORS is not access control.

Each order is an independent top-of-book experiment, not a shared-liquidity matching engine. Alpaca quote sizes are reported in round lots; this MVP converts them using a 100-share lot assumption. Use symbols with that lot size and verify units before experimenting with other securities. See [Alpaca's quote schema](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data).

Private source history is capped at 100,000 messages per partition and 256 orders per session; it is not silently trimmed or expired during operation. Reaching capacity stops ingestion and requires an operator-started new session. Worker replacement reconstructs retained input; missing source history cannot be recovered from the cache alone.

October 2 actual IEX testing first exposed a surviving-lease startup failure. Local A now waits safely for expiry and releases owned leases on graceful shutdown. The upgraded workers recovered without a second restart; immediate execution and independent persistence restarts preserved the active session and unique quote-linked simulated fills. Unattended multi-session operation is not yet certified. See [the recorded live evidence](docs/PRE_PHASE_7.md#october-2-2026-real-feed-local-acceptance).

## Evidence and Limitations

The original local thread benchmark increased throughput from approximately 1,143 to 2,152 events/sec across 8,000-event finite replay workloads. This is not multi-machine scaling or an end-to-end p95 latency claim. The current continuous validator separately samples active backlog and event enqueue-to-PostgreSQL-commit latency. See [benchmark methodology](docs/BENCHMARKS.md) and [local acceptance evidence](docs/PRE_PHASE_7.md).

The million-event duplicate-fill check is in-memory only. Public worker failures leave bounded abandoned runs; public resubmission creates a new run rather than automatically resuming it. Idle shutdown reduces resource use but does not guarantee zero billing or free-tier safety under arbitrary traffic.
