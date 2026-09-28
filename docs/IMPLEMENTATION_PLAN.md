# Implementation Plan

Each phase must end with a verifiable result. Do not begin the next phase while required checks remain incomplete.

## Phase 0: Product Selection

- [x] Select Market Execution Lab as the product concept.
- [x] Identify the target user and return use cases.
- [x] Define the simulated-order and explanation workflow.
- [x] Define deterministic replay and execution transparency as the differentiator.
- [x] Confirm private Alpaca IEX live mode and public replay restrictions.
- [x] Limit the MVP to top-of-book market and limit orders.
- [x] Select Redis Streams, Redis caching, PostgreSQL, FastAPI, Next.js, and Docker.

**Verification:** Complete. `PRODUCT_SPEC.md` has no unresolved decision that changes the MVP architecture.

## Definition of Ready

- [x] Product scope and non-goals are explicit.
- [x] Data licensing boundaries are reflected in product and deployment modes.
- [x] The execution model and its limitations are documented.
- [x] Service boundaries, event ownership, ordering, and idempotency are defined.
- [x] The primary UI workflow and required states are defined.
- [x] Test and benchmark acceptance criteria are defined.
- [x] Resume metrics are placeholders until reproducibly measured.

The project is ready to enter Phase 1. No implementation work has started.

## Phase 1: Technical Proof of Concept

- [ ] Establish the repository structure for frontend, Python services, tests, and infrastructure.
- [ ] Start Redis and PostgreSQL through Docker Compose.
- [ ] Implement generated quote/trade fixtures before using vendor credentials.
- [ ] Define typed envelopes for quotes, trades, order commands, fills, and state changes.
- [ ] Publish normalized events to fixed symbol partitions in Redis Streams.
- [ ] Implement one deterministic engine worker for a single partition.
- [ ] Cache the current quote and order state in Redis.
- [ ] Persist normalized events, orders, and fills in PostgreSQL.
- [ ] Add a command-line replay entry point and report event count, final state, and latency.
- [ ] Connect Alpaca IEX only after the generated-data path passes deterministic tests.

**Verification:** A command-line replay produces identical fills on three consecutive runs, then private live mode updates cached market state without changing execution semantics.

## Phase 2: Core Streaming Pipeline

- [ ] Separate ingestion, engine, persistence, and replay into independently runnable processes.
- [ ] Create Redis Streams consumer groups for engine and persistence responsibilities.
- [ ] Implement idempotency and event-ordering rules.
- [ ] Reclaim pending events after worker failure.
- [ ] Route malformed or permanently failing events to a dead-letter stream.
- [ ] Implement bounded retries and backpressure thresholds.
- [ ] Add health/readiness checks, structured logs, correlation IDs, and metrics.
- [ ] Record throughput, queue depth, and processing latency.

**Verification:** The pipeline survives a worker kill during replay, completes all events, and produces the same state and fills as a failure-free run.

## Phase 3: Application API

- [ ] Implement documented REST resources for symbols, market state, orders, and replay sessions.
- [ ] Implement session-scoped WebSocket messages for market, order, fill, replay, and health updates.
- [ ] Validate order side, type, quantity, price, symbol, mode, and latency.
- [ ] Enforce public-demo mode at the server rather than trusting the client.
- [ ] Add consistent errors, request IDs, and basic public rate limits.

**Verification:** Automated integration tests cover the primary API and streaming workflow.

## Phase 4: User Interface

- [ ] Implement the single execution-lab workspace before secondary system views.
- [ ] Add live WebSocket updates.
- [ ] Add clear loading, disconnected, empty, and error states.
- [ ] Display quote freshness, order activation, triggering events, fills, and computed execution metrics.
- [ ] Add replay speed controls and parameter comparison.
- [ ] Display execution-model limitations next to results.
- [ ] Confirm usability on common desktop dimensions.

**Verification:** A user can complete the MVP workflow without terminal access.

## Phase 5: Reliability and Performance

- [ ] Create fixed generated normal, volatile, illiquid, and gap scenarios.
- [ ] Add load tests at increasing event rates.
- [ ] Test delayed, duplicated, malformed, missing, and stale events.
- [ ] Test worker and dependency failures.
- [ ] Compare one worker with multiple workers using fixed stream partitions.
- [ ] Identify and document the first bottleneck.
- [ ] Record final benchmark results.

**Verification:** One million generated events produce zero duplicate fills; the system sustains twice the observed live peak for ten minutes without unbounded lag; all reported results are reproducible.

## Phase 6: Deployment

- [ ] Build deployable containers.
- [ ] Provision Vercel frontend, persistent API/worker containers, managed Redis, and managed PostgreSQL.
- [ ] Configure secrets and environment-specific settings.
- [ ] Run database migrations.
- [ ] Add deployment health checks.
- [ ] Disable restricted live data in public mode.
- [ ] Verify that public replay fixtures have redistribution-safe provenance.
- [ ] Document rollback and recovery procedures.

**Verification:** A clean browser completes the public replay workflow, while private live mode requires operator-controlled configuration and does not expose credentials or data publicly.

## Phase 7: Project Presentation

- [ ] Add an architecture diagram.
- [ ] Add UI screenshots or a short demonstration.
- [ ] Publish measured performance results with methodology.
- [ ] Document important tradeoffs and limitations.
- [ ] Write final resume bullets using measured values only.

**Verification:** Another engineer can understand, run, and evaluate the project from the repository documentation.
