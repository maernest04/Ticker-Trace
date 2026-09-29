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

- [ ] Implement documented REST resources for symbols, market state, orders, and replay sessions.
- [ ] Implement `GET /health`, `GET /ready`, symbol, market-state, order-result, and replay-status endpoints.
- [ ] Add response schemas, request IDs, and API integration tests.

**Verification:** A client can retrieve current and historical replay state without direct Redis or PostgreSQL access.

### 3B — Order and Replay Commands

- [ ] Implement session-scoped WebSocket messages for market, order, fill, replay, and health updates.
- [ ] Validate order side, type, quantity, price, symbol, mode, and latency.
- [ ] Implement order submission and replay-control endpoints.
- [ ] Publish commands to the owning stream partition rather than mutating engine state directly.

**Verification:** An API-created order follows the same engine path and produces the same result as a command-line order.

### 3C — Safety Boundary

- [ ] Enforce public-demo mode at the server rather than trusting the client.
- [ ] Add consistent errors, request IDs, and basic public rate limits.

**Verification:** Automated integration tests cover the primary API and streaming workflow; public mode cannot enable the live adapter or expose credentials.

## Phase 4: User Interface

### 4A — Execution Workspace

- [ ] Implement the single execution-lab workspace before secondary system views.
- [ ] Add mode, symbol, and replay-dataset selection.
- [ ] Add current quote/trade state and the simulated-order form.

**Verification:** A user can select a replay scenario and submit a valid simulated order through the browser.

### 4B — Live Result Explanation

- [ ] Add live WebSocket updates.
- [ ] Add clear loading, disconnected, empty, and error states.
- [ ] Display quote freshness, order activation, triggering events, fills, and computed execution metrics.
- [ ] Display execution-model limitations next to results.

**Verification:** A user can follow an order from submission to final state and identify the event that caused every fill.

### 4C — Replay Comparison and Health

- [ ] Add replay speed controls and parameter comparison.
- [ ] Add the system-health view after the primary workspace is complete.
- [ ] Confirm usability on common desktop dimensions.

**Verification:** A user can complete the MVP workflow without terminal access.

## Phase 5: Reliability and Performance

### 5A — Correctness and Fault Matrix

- [ ] Create fixed generated normal, volatile, illiquid, and gap scenarios.
- [ ] Test delayed, duplicated, malformed, missing, and stale events.
- [ ] Test worker and dependency failures.

**Verification:** Fault scenarios produce documented, deterministic, and safe outcomes.

### 5B — Load and Scaling Benchmarks

- [ ] Add load tests at increasing event rates.
- [ ] Compare one worker with multiple workers using fixed stream partitions.
- [ ] Identify and document the first bottleneck.

**Verification:** Benchmark data shows throughput, latency, lag, and replay-runtime behavior for each worker count.

### 5C — Final Measured Claims

- [ ] Record final benchmark results.
- [ ] Confirm one million generated events produce zero duplicate fills.
- [ ] Confirm the system sustains twice the observed live peak for ten minutes without unbounded lag.
- [ ] Add reproducible benchmark records to the project documentation.

**Verification:** One million generated events produce zero duplicate fills; the system sustains twice the observed live peak for ten minutes without unbounded lag; all reported results are reproducible.

## Phase 6: Deployment

### 6A — Containerized Application

- [ ] Build deployable containers.
- [ ] Configure secrets and environment-specific settings.
- [ ] Run database migrations.
- [ ] Add deployment health checks.

**Verification:** The complete replay stack starts locally with production-like configuration and passes a smoke test.

### 6B — Public Replay Deployment

- [ ] Provision Vercel frontend, persistent API/worker containers, managed Redis, and managed PostgreSQL.
- [ ] Disable restricted live data in public mode.
- [ ] Verify that public replay fixtures have redistribution-safe provenance.

**Verification:** A clean browser completes the public replay workflow and public services cannot start live ingestion.

### 6C — Operational Validation

- [ ] Document rollback and recovery procedures.
- [ ] Configure logs, metrics, and uptime/health checks.
- [ ] Verify restart behavior for API, worker, Redis, and database dependencies.

**Verification:** A clean browser completes the public replay workflow, while private live mode requires operator-controlled configuration and does not expose credentials or data publicly.

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
