# Implementation Plan

Each phase must end with a verifiable result. Do not begin the next phase while required checks remain incomplete.

## Phase 0: Product Selection

### 0A — Product and User

- [x] Select Market Execution Lab as the product concept.
- [x] Identify the target user and return use cases.
- [x] Define the simulated-order and explanation workflow.

**Verification:** The product statement identifies a user, a valuable action, and a clear reason to return.

### 0B — Scope and Data Boundaries

- [x] Define deterministic replay and execution transparency as the differentiator.
- [x] Confirm private Alpaca IEX live mode and public replay restrictions.
- [x] Limit the MVP to top-of-book market and limit orders.

**Verification:** The execution model, public/private data boundary, and non-goals are explicit.

### 0C — Technical Direction

- [x] Select Redis Streams, Redis caching, PostgreSQL, FastAPI, Next.js, and Docker.
- [x] Define service boundaries, data flow, UI workflow, testing strategy, and deployment boundary.

**Verification:** Complete. `PRODUCT_SPEC.md` has no unresolved decision that changes the MVP architecture.

## Definition of Ready

- [x] Product scope and non-goals are explicit.
- [x] Data licensing boundaries are reflected in product and deployment modes.
- [x] The execution model and its limitations are documented.
- [x] Service boundaries, event ownership, ordering, and idempotency are defined.
- [x] The primary UI workflow and required states are defined.
- [x] Test and benchmark acceptance criteria are defined.
- [x] Resume metrics are placeholders until reproducibly measured.

Phase 1 is complete.

## Phase 1: Technical Proof of Concept

### 1A — Foundation and Contracts

- [x] Establish the repository structure for frontend, Python services, tests, and infrastructure.
- [x] Implement generated quote/trade fixtures before using vendor credentials.
- [x] Define typed envelopes for quotes, trades, order commands, fills, and state changes.
- [x] Add tests for valid and invalid events and orders.

**Verification:** Fixtures load successfully; contracts reject invalid inputs; all contract tests pass.

### 1B — Pure Execution Core

- [x] Implement an in-memory market-state model.
- [x] Implement market and limit order activation rules.
- [x] Implement partial fills, artificial latency, and execution calculations.
- [x] Implement deterministic event and fill identifiers.
- [x] Test complete, partial, unfilled, delayed, duplicate, and stale-event scenarios.

**Verification:** Every fixture produces the expected fill sequence, and three identical in-memory replays produce identical results.

### 1C — Local Durable and Streamed Proof

- [x] Start Redis and PostgreSQL through Docker Compose.
- [x] Add PostgreSQL migrations for events, replay runs, orders, fills, and transitions.
- [x] Publish normalized events to fixed symbol partitions in Redis Streams.
- [x] Implement one deterministic engine worker for a single partition.
- [x] Cache the current quote and order state in Redis.
- [x] Persist normalized events, orders, and fills in PostgreSQL.
- [x] Add a command-line replay entry point and report event count, final state, and latency.

**Verification:** The Redis-driven replay matches the in-memory result exactly on three consecutive runs. Alpaca remains out of scope until the generated-data path passes.

## Phase 2: Core Streaming Pipeline

### 2A — Service Separation

- [x] Separate replay publishing, engine, and persistence into independently runnable processes; private ingestion remains scheduled for 2C.
- [x] Create Redis Streams consumer groups for engine and persistence responsibilities.
- [x] Implement stable symbol partitioning and durable event idempotency rules.

**Verification:** The same fixture produces correct results when services run as separate processes.

### 2B — Recovery and Backpressure

- [x] Reclaim pending events after worker failure.
- [x] Route malformed or permanently failing events to a dead-letter stream.
- [x] Implement bounded retries and backpressure thresholds.

**Verification:** A killed worker recovers pending events; malformed events are observable and do not halt the pipeline.

### 2C — Observability and Live Adapter

- [x] Add health/readiness checks, structured logs, correlation IDs, and metrics.
- [x] Record throughput, queue depth, and processing latency.
- [x] Connect the private Alpaca IEX adapter after its normalized-event contract matches replay fixtures.

**Verification:** The pipeline survives a worker kill during replay, completes all events, produces the same state and fills as a failure-free run, and reports its health and lag.

## Phase 3: Application API

### 3A — Read APIs

- [x] Implement documented REST resources for symbols, market state, orders, and replay sessions.
- [x] Implement `GET /health`, `GET /ready`, symbol, market-state, order-result, and replay-status endpoints.
- [x] Add response schemas, request IDs, and API integration tests.
- [x] Store user-selected symbols, watchlists, and replay-session settings in PostgreSQL rather than environment variables.

**Verification:** A client can retrieve current and historical replay state without direct Redis or PostgreSQL access.

### 3B — Order and Replay Commands

- [x] Implement session-scoped WebSocket messages for market, order, fill, replay, and health updates.
- [x] Validate order side, type, quantity, price, symbol, mode, and latency.
- [x] Implement order submission and replay-control endpoints.
- [x] Publish commands to the owning stream partition rather than mutating engine state directly.
- [x] Add watchlist commands that update private-live Alpaca subscriptions without restarting the service.

**Verification:** An API-created order follows the same engine path and produces the same result as a command-line order; changing a watchlist updates the live subscription without editing environment files or restarting a service.

### 3C — Safety Boundary

- [x] Enforce public-demo mode at the server rather than trusting the client.
- [x] Add consistent errors, request IDs, and basic public rate limits.

**Verification:** Automated integration tests cover the primary API and streaming workflow; public mode cannot enable the live adapter or expose credentials.

## Phase 4: User Interface

### 4A — Trading Terminal Workspace

- [x] Replace the prototype card layout with one desktop-first execution terminal.
- [x] Add a terminal header with mode, selected symbol, replay dataset, and connection status.
- [x] Add a persistent left watchlist with symbols, last price, and change state from the selected replay data.
- [x] Place the selected symbol's bid, ask, spread, visible size, last trade, and event timestamp in the central market panel.
- [x] Place the simulated order ticket on the right with side, type, quantity, limit price, artificial latency, and submit controls.
- [x] Add a bottom execution panel with an explicit queued or empty state; real-time order details remain 4B.

**Verification:** A user can select a replay scenario, submit a valid simulated order, and understand the market context and queue state without leaving the terminal.

### 4B — Live Result Explanation

- [x] Add a bounded replay-event read endpoint and include recent events in session-scoped WebSocket updates.
- [x] Add a compact price trace and event tape to the central market panel without rendering every event received by the backend.
- [x] Add live WebSocket updates to the watchlist, quote state, and bottom execution panel.
- [x] Add clear loading, disconnected, empty, and error states.
- [x] Display quote freshness, order activation, triggering events, fills, and computed execution metrics.
- [x] Display execution-model limitations next to results.

**Verification:** A user can follow an order from submission to final state in the bottom execution panel and identify the market event that caused every fill.

### 4C — Replay Comparison and Health

- [x] Add client-side replay speed controls and a zero-latency parameter comparison in the terminal header and bottom panel.
- [x] Add a compact health and connection view after the primary terminal workflow is complete.
- [x] Confirm usability at common desktop dimensions before attempting a mobile layout.

**Verification:** A user can complete the MVP workflow without terminal access.

## Phase 5: Reliability and Performance

### 5A — Correctness and Fault Matrix

- [x] Create fixed generated normal, volatile, illiquid, and gap scenarios.
- [x] Test delayed, duplicated, malformed, missing, and stale events.
- [x] Test worker and dependency failures.

**Verification:** Fault scenarios produce documented, deterministic, and safe outcomes.

### 5B — Load and Scaling Benchmarks

- [x] Add load tests at increasing event rates.
- [x] Compare one worker with multiple workers using fixed stream partitions.
- [x] Identify and document the first bottleneck.

**Verification:** Benchmark data shows throughput, latency, lag, and replay-runtime behavior for each worker count.

### 5C — Final Measured Claims

- [x] Add a reproducible validation command and record the initial benchmark result.
- [ ] Record final benchmark results.
- [ ] Confirm one million generated events produce zero duplicate fills.
- [ ] Confirm the system sustains twice the observed live peak for ten minutes without unbounded lag.
- [ ] Add reproducible benchmark records to the project documentation.

**Verification:** One million generated events produce zero duplicate fills; the system sustains twice the observed live peak for ten minutes without unbounded lag; all reported results are reproducible.

## Phase 6: Deployment

### 6A — Containerized Application

- [x] Build deployable containers.
- [x] Configure secrets and environment-specific settings.
- [x] Run database migrations.
- [x] Add deployment health checks.

**Verification:** The complete replay stack starts locally with production-like configuration and passes a smoke test.

### 6B — Public Replay Deployment

- [x] Add Fly/Vercel configuration for persistent API and worker process groups.
- [x] Disable restricted live data in public mode.
- [x] Verify that public replay fixtures are deterministic generated fixtures with no vendor data.
- [ ] Provision Vercel frontend, Fly API/worker containers, managed Redis, and managed PostgreSQL.
- [ ] Complete the deployed-browser smoke test.

**Verification:** A clean browser completes the public replay workflow and public services cannot start live ingestion.

### 6C — Free-Tier Lifecycle Safety

- [x] Define the idle state for Vercel, Fly, Redis, and Supabase with no background keepalive traffic.
- [x] Add a Fly health endpoint that does not touch Redis or PostgreSQL.
- [x] Increase local queue-worker blocking intervals and remove acknowledged global jobs without trimming unread work.
- [x] Add a 15-minute expiration policy for completed replay streams, cached state, dead letters, and metrics.
- [x] Configure Fly API autostop with zero minimum running machines.
- [x] Start exactly one engine and one persistence machine on replay demand through the Fly Machines API.
- [x] Replace public worker queue polling with private HTTP job dispatch; retain queue mode for local benchmarks and private live operation.
- [x] Check startup readiness, tolerate concurrent start conflicts, reject duplicate in-flight run IDs, and shut workers down after 60 idle seconds without interrupting active work.
- [x] Expire published-but-abandoned source streams after one hour and completed replay state after 15 minutes.
- [x] Enforce a shared 1,000 replay-attempt monthly allowance and prune completed generated public runs older than seven days or beyond the newest 1,000, at most 100 runs per successful submission.
- [x] Close completed WebSockets and bound browser retries and incomplete server sessions.
- [x] Document manual provider usage checks without adding scheduled traffic that would defeat idle shutdown.
- [x] Add startup, idle shutdown, active-job protection, Redis-outage, retention, and abandoned-source expiration tests.
- [x] Stage the app-scoped Fly worker token, deploy with no spare machines, and redeploy the frontend.
- [ ] Verify deployed cold-start, concurrent replay behavior, and 24-hour idle provider usage.

**Local verification:** 60 tests passed against local Redis/PostgreSQL; the frontend production build and Fly configuration validation passed. Demand dispatch is tested through the API and both workers, with persisted results, TTLs, and WebSocket closure. Retention tests preserve active, private-live, and benchmark records.

**Deployment acceptance (pending):** After 24 hours with no dashboard activity, no application machines are running, Redis receives no application commands, and PostgreSQL receives no application keepalive traffic. Starting a replay wakes the two existing worker machines and returns to idle after completion. This reduces resource use; it does not guarantee zero Fly charges or immunity to arbitrary public traffic.

### 6D — Operational Validation

- [x] Document backend image/config rollback, database compatibility/forward-fix rules, frontend rollback, and incident recovery in `DEPLOYMENT.md`.
- [x] Enable structured worker logs, propagate request IDs, sanitize expected dependency errors, and preserve dependency-free Fly liveness checks.
- [x] Use comparable cross-machine metric timestamps and document clock-skew/TTL limitations; avoid scheduled probes that defeat idle shutdown.
- [x] Test killed-consumer recovery, refused database connection recovery, API recreation, Redis outage/readiness recovery, and completed-job retry acknowledgement.
- [x] Fetch final UI health once after completion, abort abandoned requests, and replace misleading persistence-waiting text.
- [x] Verify the Python suite, frontend production build, and local browser replay.

**Verification:** 70 tests pass against local Redis/PostgreSQL. The recovery test kills a real claimed-message consumer, completes through replacement workers, and confirms one fill after API recreation. Frontend production build passes. The local browser confirms completed replay health with zero queue depth and zero pending workers; the older local persistence container was already stopped, so the smoke replay used a temporary targeted worker. Existing public/private isolation tests remain enforced. The earlier deployed 6C smoke test confirmed a complete AAPL replay and all three Fly machines stopped afterward. 6D changes still require redeployment; production outage/rollback drills were not performed, and the 24-hour idle-provider-usage check remains outstanding.

## Phase 7: Project Presentation

### 7A — Technical Documentation

- [ ] Add an architecture diagram.
- [ ] Document important tradeoffs and limitations.

**Verification:** The README explains the user value, architecture, data modes, and model limitations accurately.

### 7B — Demonstration and Benchmarks

- [ ] Add UI screenshots or a short demonstration.
- [ ] Publish measured performance results with methodology.

**Verification:** A reviewer can see the end-to-end workflow and independently interpret benchmark conditions.

### 7C — Resume and Interview Narrative

- [ ] Write final resume bullets using measured values only.
- [ ] Prepare a concise explanation of concurrency, partitioning, idempotency, caching, and recovery tradeoffs.
- [ ] Verify all public project claims match the deployed behavior and test evidence.

**Verification:** Another engineer can understand, run, and evaluate the project from the repository documentation.
