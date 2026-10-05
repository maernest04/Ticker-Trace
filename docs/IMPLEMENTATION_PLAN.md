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
- [x] Enforce a shared monthly admission allowance and prune completed generated public runs older than seven days or beyond the newest 1,000, at most 100 runs per successful submission. Pre-Phase 7 makes admission 1,000 weighted credits, charging longer fixtures proportionally.
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

## Pre-Phase 7: Product and Evidence Completion

Complete A (private live execution), B (meaningful replay), and C (continuous-load evidence and honest documentation) using the build and acceptance checklists in [PRE_PHASE_7.md](PRE_PHASE_7.md). Local implementation and external market/deployment/24-hour checks are tracked separately. Do not treat the older checked phase items as proof that this exit gate passed.

## Local-First Completion Plan — October 2, 2026

This is the current build priority. It supersedes cloud deployment as an acceptance gate, not the historical phase records above. Run Next.js, FastAPI, Redis, PostgreSQL, ingestion, execution, and persistence on the laptop. Internet is required for Alpaca; generated replay remains the offline testing path. No owner-authentication system, cloud deployment, cloud shutdown, or automatic provider monitoring is included. Existing cloud resources remain unchanged until explicitly managed separately.

The October 2 real-feed test completed two five-share AAPL simulations and verified both against persisted subsequent quotes. Immediate execution-worker restart failed on a retained ownership lease; a manual retry after expiry succeeded. See [the evidence record](PRE_PHASE_7.md#october-2-2026-real-feed-local-acceptance).

### Local A — Automatic Restart Recovery

Build:

- [x] Add bounded startup retry for occupied live-worker leases at the service entry point; keep fail-fast ownership checks available for tests and competing-worker diagnostics.
- [x] Release partially acquired leases before retrying; never delete or overwrite another owner's lease.
- [x] Handle graceful shutdown so ingestion and workers release only leases they still own.
- [x] Maintain ownership while reconstructing retained history; stop processing when ownership is lost rather than treating lease expiry as permission to continue.
- [x] Add an appropriate local worker restart policy and actionable waiting, recovery, timeout, and fatal-error logs. Do not endlessly retry invalid Alpaca credentials.
- [x] Preserve the active session during execution/persistence replacement. Ingestion/session replacement coordination belongs to Local B.

Verify:

- [x] Reproduce immediate restart with an unexpired old lease, then prove one restart recovers without a second manual retry.
- [x] Verify a healthy competing owner remains exclusive and the contender waits or times out safely.
- [x] Test partial acquisition, graceful termination, abrupt termination, slow reconstruction, and ownership loss with deterministic clocks where possible.
- [x] Replace execution and persistence independently against local generated input; assert unchanged fill IDs, quantities, and durable results.
- [x] Repeat the real-feed test: fresh quote, simulated fill, immediate worker restart, post-recovery fill, and exactly one durable fill per fully filled small order.

Acceptance passed October 2: worker entry points wait up to 45 seconds for the previous 30-second lease, then reconstruct retained input in 100-message batches with ownership checks and renewal. The startup deadline bounds acquisition, not reconstruction time. Library calls remain fail-fast by default. Graceful shutdown releases owned leases; abrupt process termination waits for expiry. Local engine/persistence use `on-failure:3` and a 20-second shutdown grace period; Part B adds the same bounded policy to ingestion, with fatal provider authentication exiting without restart. Tests cover SIGTERM/SIGKILL process replacement, slow reconstruction, partial acquisition, cancellation, and lost ownership. Real-feed replacement needed no second restart or manual lease cleanup; see `PRE_PHASE_7.md` for timings and durable quote-linked fills.

### Local B — Session Lifecycle and Bounded Retention

Build:

- [x] Make the active-session registry authoritative and have workers follow session changes without processing the wrong run.
- [x] Resume a recoverable session after an ordinary ingestion restart; restore persisted subscriptions instead of silently reverting UI changes.
- [x] Coordinate session rollover before time, source-message, or order capacity is exhausted. Choose configurable thresholds from measured local traffic rather than merely raising the existing caps.
- [x] Stop new submissions during rollover, establish a closing event boundary, and drain engine/persistence work through that boundary.
- [x] Preserve partial fills and explicitly cancel any unfilled remainder with a session-ended reason; do not silently carry an order into a new experiment.
- [x] Activate the replacement session only after the old one is durably closed; reconnect the UI and workers automatically.
- [x] Expire closed-session source/result streams, caches, control history, and diagnostics only after completion and a recovery grace period. Do not trim active reconstruction history.
- [x] Prune old private raw events and session/order history in bounded batches; preserve triggering quotes for as long as their fill explanations are retained.
- [x] Document retention settings, disk growth, startup/stop commands, session reset behavior, and recovery limitations. Retention duration is an explicit operator setting, not an unmeasured quota guarantee.

Verify:

- [x] Force time/message/order rollover with small test thresholds while an order is partially filled.
- [x] Inject interruptions after durable intent, boundary publication, completion, successor initialization, and activation; retry safely with one successor and no duplicate fills.
- [x] Restart ingestion and verify subscriptions, worker session IDs, freshness, and subsequent execution using generated-feed service processes; verify the same session survives an actual local ingestion-container restart.
- [x] Verify cleanup preserves active/pending work, retained fill quotes, and unrelated replay or benchmark records.
- [x] Run accelerated multiple-session tests and confirm Redis expiry and bounded PostgreSQL cleanup follow configured retention.
- [ ] Physical laptop/Docker suspension and fresh actual-feed execution after Part B: repeat during an active market window. Observed sleep/wake cycles exhausted three lease-loss retries; explicit container start recovered. Unattended suspension recovery is not accepted.

Implementation and generated acceptance passed: 120 total tests and the frontend build. Real service processes complete three generated-feed rollovers with unchanged worker PIDs; partial fills retain their IDs and the remainder closes as cancelled with reason `session ended`. The actual capped local session drained and activated one successor, preserving all five quote-linked fills. Defaults: 30-minute sessions, 90,000 messages per partition, 240 orders, 15-minute closed Redis grace, one-hour closed raw history, and 24-hour closed order/fill explanations. Thresholds leave headroom below unchanged hard caps; actual peak sizing remains Local C. Cleanup uses batches of at most 1,000 events/fills/transitions and one session per pass. Physical suspension and actual-feed freshness after this update remain separate unverified operational checks.

### Local C — Final Evidence and Presentation Readiness

- [x] Run the full Python suite and frontend production build after Local A/B/C changes: 125 passed; production build passed.
- [x] Repeat the local browser workflow with actual quotes, parameterized simulated orders, reconnects, and stale-feed protection. October 5: actual fresh quotes, browser-submitted fill, market/limit orders with artificial latency, separate engine/persistence restarts, stale API rejection, browser disabled-to-Fresh transition after ingestion restoration, and automatic browser reconnect after API stop/start all passed.
- [x] Measure a nonzero active-window peak for the documented symbol set. October 5: AAPL/MSFT Alpaca IEX, 1,483 events over 60.24 seconds, peak 141 events/sec.
- [x] Offer twice that measured rate for at least 600 seconds using isolated local generated load; retain offered-rate, active backlog, drain, latency, duplicate/loss, and error evidence. October 5: 169,200 events, 282/sec target, 281.97/sec offered, 37.57 ms p95 persistence latency, zero final backlog/lost/duplicate events/worker errors. See `benchmarks/local-c-live-acceptance-2026-10-05.md` and its JSON artifact.
- [x] Record hardware, software versions, partition/worker settings, local-thread versus separate-process methodology, and dataset/source-hash identifiers without invoking Git.
- [x] Keep in-memory dedupe, finite replay throughput, continuous durable-write latency, and real-feed execution as separate claims.
- [x] Review vendor permission before public visuals: public-display permission remains unresolved; publish generated data only. No raw live-data publication performed.
- [x] Update architecture, setup instructions, limitations, benchmark evidence, and resume drafts with measured results only: 120,000 generated events over 600 seconds; 8.06 ms p95 durable-write latency; not live-peak certification.
- [x] Verify stopping the local stack stops ingestion/workers while retaining database data; browser close alone is not shutdown. Existing five fills survived stop/start.
- [x] Add targeted lease-expiry recovery and test repeated process pauses plus an actual container pause beyond the normal 30-second leases, with no process replacements after the final fix. Physical laptop/network suspension remains a separate check.

The live browser checks, live-peak measurement and 600-second test at twice that measured peak passed October 5. Remaining acceptance includes deliberate physical sleep/wake/network recovery. A concurrent regression run also exposed a lifecycle ownership-readiness assertion failure; the individual test and subsequent complete suite (125 tests) passed after the benchmark ended, but the timing-sensitive failure remains unresolved. Preserve all results. Cloud redeployment, owner-only hosting, public concurrency, and 24-hour hosted idle-usage checks are deferred, not passed or required for this local-first milestone. The executable market-window runbook is in `PRE_PHASE_7.md`.

### Execution Order

1. Local A implementation and regression checks → real-feed restart retest.
2. Local B lifecycle and retention → accelerated rollover/failure checks.
3. Local C ten-minute evidence and documentation → Phase 7 presentation.

Local A/B and Local C's available local engineering/evidence work are implemented. Actual-feed simulated execution, browser transitions, separate worker restarts, and the live-peak-sized benchmark passed October 5. Deliberate physical laptop/network suspension and resolution of the observed timing-sensitive lifecycle-test readiness failure remain open; do not claim those passed from narrower process/container tests or successful reruns.

## Extension A / B / C — Recorded-Market Execution Comparisons

Planning added October 5, 2026. This extension is the next implementation track, before final Phase 7 presentation. Nothing in the checklists below is implemented or accepted merely because this plan exists. The earlier Local A/B/C sections and October 5 benchmark remain historical evidence, not checklists to restart.

Implementation requested for the entire extension on October 5. Engineering now includes bounded private capture, immutable validation, isolated offline subprocess replay, source-index entry boundaries, two-experiment first-divergence explanations, paged quote evidence, and `/recorded` UI. The full Python suite passed 168 tests, followed by 43 focused tests after the validation-message change. Five quiet and five isolated contended lifecycle repetitions passed. Evidence: [extension acceptance](../benchmarks/extension-abc-acceptance-2026-10-05.md). Remaining acceptance is tracked below; actual capture permission, physical suspension, and participant validation are not fabricated from generated tests.

### Product outcome and scope

Ticker Trace will let a private local user select a recorded stock-market interval, compare two simulated orders that differ in one parameter, and identify the first source event that explains their execution difference. The product is an inspectable execution-comparison tool, not another general stock dashboard, a trading recommendation, or a standalone fault-injection benchmark.

Default decisions for this extension:

- Retain execution simulation; do not add trade-book reconciliation.
- Keep Python/FastAPI, Redis Streams/cache, PostgreSQL, Next.js/TypeScript, and Docker. Reuse the existing event contracts, execution rules, partitioning, and recovery code where they fit.
- Keep live ingestion and recorded replay local/private. Recorded replay is not a public fixture source, does not use hosted dependencies, and must work without market-data credentials or internet once a recording has been saved.
- Capture normalized events from the existing live pipeline; do not open another Alpaca connection. Historical REST import is deferred.
- Allow at most one active capture, using one source run and at most two already-subscribed symbols. Comparisons select one symbol. End capture on run rollover or ingestion interruption; cross-session stitching and gap backfill are deferred.
- Initial bounds: ten minutes, 80,000 market events, or 64 MiB per recording, whichever is reached first; 256 MiB aggregate private artifacts and ten saved experiments. Recorded finite admission allows at most 80,003 messages, below the existing live hard cap; public finite admission stays 10,000. Reject new work without storage headroom; never silently evict saved recordings or raise global limits. Recorded SQL history remains operator-managed, not subject to a certified disk quota.
- Treat these bounds as v1 safety defaults, not performance guarantees. Recording endpoints must not automatically change subscriptions or continue recording after the user closes a capture interval.
- Keep existing public generated demos and safeguards unchanged. Real-data files must be ignored by Git and excluded from public bundles/images. Confirm applicable account recording/storage permissions before actual vendor capture; public display/redistribution remains unapproved.
- Preserve current top-of-book limitations: independent orders, no shared liquidity, queue priority, hidden depth, market impact, fees, or broker-exact fills. Comparisons describe the simulator, not what a real broker would necessarily have done.

### Extension A — Recovery Correctness

Goal: make the existing recovery evidence dependable, then establish reusable correctness checks for recorded replay. Reliability supports the product; it is not the new user-facing feature.

#### A1 — Reproduce and fix lifecycle-test readiness

Build/checklist:

- [ ] Reproduce the October 5 failure in `test_ingestion_restart_and_three_rollovers_with_real_service_processes`; preserve the observed 17-versus-33 ownership-key result.
- [x] Confirm whether the failure is test synchronization or worker behavior before changing application code. Fresh engine-cached quotes alone do not establish persistence readiness.
- [x] Before pausing processes, wait with a bounded deadline for both roles to own all expected partitions in the current successor run and for a persisted probe to demonstrate progress.
- [x] Retain process-liveness, unchanged-PID, unchanged-original-fill, and recovery assertions. Do not weaken expected ownership counts, add arbitrary sleeps, or increase retry budgets to hide failure.

Verify/gate:

- [x] Targeted lifecycle test passes five consecutive times without competing load.
- [x] It passes five consecutive times while an isolated generated workload runs, using a separate test database/Redis namespace from acceptance evidence. Record workload and resource contention; do not silently discard failed attempts.
- [x] Full Python suite passes against isolated local storage. Any application-code change must have a regression test reproducing its cause.

#### A2 — Retry and commit-boundary audit

Build/checklist:

- [x] Inventory existing tests first; add only missing cases for duplicate commands/events, pending redelivery, and separate engine/persistence replacement.
- [x] Verify source/result acknowledgements occur after their required durable or recoverable effects. Test failure before commit and commit-success followed by failure before acknowledgement.
- [x] Verify reconstruction keeps the same fill IDs within the same run and cannot overwrite completed progress with an older snapshot.
- [x] Check exclusive partition ownership, cooperative-lease limitations, missing-history failure, and bounded malformed-message/dead-letter behavior without introducing consensus or database fencing.

Verify/gate:

- [x] Each injected commit-boundary interruption recovers with the expected final state, quantities, and exactly one durable row per expected fill.
- [x] A generated partial-fill order survives replacement and completes without changing previously persisted fills; a deliberate unfillable order remains unfilled rather than gaining fabricated fills.
- [x] Duplicate delivery changes diagnostic counts, not execution outcomes. An intentional competing owner cannot process the same partition.
- [x] Save scoped results separately from throughput benchmarks; avoid claiming universal exactly-once delivery or recovery from destroyed history.

#### A3 — Local operational handoff

- [ ] Verify dependency-ordered local startup and document the October 5 cold-boot failure. If unattended startup is claimed, prove it; otherwise retain the explicit operator startup procedure.
- [ ] With the user's participation during actual fresh quotes, perform physical laptop sleep/wake and a bounded network interruption. Check renewed ownership, subscriptions, actual fresh cached quotes, subsequent simulated execution, and unchanged retained fills.
- [ ] Restore any temporarily stopped services. Save failures as well as successes; do not substitute process/container suspension for physical sleep/wake.
- [ ] Review startup/recovery limitations against recorded evidence before marking A complete.

The original 17-versus-33 failure remains documented in PRE_PHASE_7.md; it was not independently re-created with that exact count during this extension. Initial readiness and shared-database contention failures, subsequent fixes, and all final repetitions are retained in the extension evidence. Local startup remains explicit/operator-assisted, not a new unattended cold-boot certification.

Physical sleep/wake requires user participation and an active feed. Its absence does not block implementing B with generated test input, but remains an open acceptance item and must not be relabelled as passed.

### Extension B — Private Recording and Deterministic Replay

Goal: preserve a bounded real observed interval and replay it offline through the existing execution/persistence pipeline without corrupting live state or weakening public/private boundaries.

#### B1 — Recording contracts, boundaries, and storage

Build/checklist:

- [x] Define a versioned immutable manifest and normalized event file under a private local recording directory. Manifest includes recording ID, source feed/run, symbols, capture boundaries, event count, available timestamp precision, units/normalization version, schema version, engine/model provenance, checksum, and completion/quality status.
- [x] Base capture on retained source-stream entries with frozen start/end cursors per partition, not a second provider connection or a query sorted only by exchange timestamps. Preserve received per-symbol ordering, original identifiers, normalized prices/sizes, available event timestamps, and rejected/out-of-order limitations.
- [x] Define one recording control record and explicit states: capturing, finalizing, ready, incomplete, failed. Finalization validates the complete bounded source range; only validated immutable artifacts become replayable.
- [x] Separate capture wall time, source market time, and later replay ingestion time. Do not invent nanosecond precision lost by current normalization or claim that IEX observations represent the consolidated market.
- [x] Audit where disconnects, rejected messages, source resets, and retention loss can be detected. Record known quality limitations; absence of a reported gap is not proof of a complete vendor feed.
- [x] A user stop or normal time/event/byte bound may produce a ready bounded recording after validation; record its completion reason. On interruption/rollover or missing history, freeze the captured prefix and mark it incomplete rather than silently stitching sessions. Incomplete recordings remain visible but cannot be used for v1 comparisons.
- [x] Add recording-file ignore/container-exclusion rules before any real capture. Enforce event/time/byte/disk limits without automatic deletion. Generated test recordings may be versioned; vendor recordings may not.

Verify/gate:

- [x] Contract tests reject malformed events, unknown versions, missing boundaries, ambiguous ordering, wrong symbols/units, checksum mismatches, truncated files, and oversized input.
- [x] Simulated interrupted writes never yield a ready artifact. A ready manifest's counts/checksum match its immutable contents.
- [x] Normal session retention never deletes a ready recording file, and saving a recording never deletes, trims, or prolongs active recovery streams.
- [x] No secrets enter manifests or exported files. Public builds cannot discover/read recordings by ID, path, list endpoint, order lookup, or WebSocket.

#### B2 — Bounded capture and offline replay services

Build/checklist:

- [x] Add private start/stop/status/list controls for capture. Freeze exact cursor boundaries, validate/export before retained history expires, and stop at the first configured bound. Do not read moving stream tails indefinitely or represent a missing source prefix as complete.
- [x] Keep finalization bounded and outside a long-running browser request; expose progress/failure rather than marking queued work ready prematurely. Reuse current job/service mechanisms where correct, without adding a second orchestration framework.
- [x] On rollover, dependency failure, disk exhaustion, or application restart, stop/mark the affected recording clearly; no automatic capture resume or source reconstruction from made-up events.
- [x] Add a private recorded-replay source/run boundary independent of `LIVE_SESSION_KEY`, public fixture allowlists, and real-time freshness admission. Historical data must never appear as Fresh live quotes.
- [x] Reuse the current engine, partitioning, caching, persistence, and finite replay path; validate local recorded-run limits separately without loosening public caps. Publish a complete immutable input before finite dispatch, retaining the current completed-run processing model.
- [x] Define entry as an explicit insertion boundary in the selected symbol's source-entry order, with a recorded market-time anchor. Warm up only preceding market state, insert the order command at that boundary, and evaluate eligible subsequent events under existing rules. Never fill from pre-entry events, including earlier entries with equal timestamps, or use future quotes during warm-up. Preserve source ordering rather than sorting by timestamp.
- [x] Generate isolated run/order identifiers per experiment while retaining original source-event references. Refresh replay ingestion timestamps for operational latency; preserve source market time for execution.
- [x] Provide backend 1x/5x/20x/max publication pacing for the private local replay path. Keep client trace playback separately labelled; paced publication does not imply incremental finite engine execution.
- [x] Persist the dataset checksum, entry anchor, order configuration, and model version with each experiment so results can be reproduced and compared. Keep recorded artifacts independent of temporary run-result retention.

Verify/gate:

- [x] Replay a generated test recording offline with no Alpaca credentials and no provider connection.
- [x] Identical immutable inputs at different publication/playback speeds produce equivalent canonical outcomes: fill source references, prices/quantities, market-time transitions, remaining quantity, and metrics. Exclude new run/order IDs and operational wall-clock latency from cross-run equality.
- [x] Worker replacement within one recorded run preserves that run's existing fill IDs and final canonical outcome. Lost/partial publication remains a failed run, not a successful truncated replay.
- [ ] Live ingestion/registry/subscriptions remain unchanged while independent recorded experiments execute; public generated mode still passes its existing tests.
- [x] Controlled stale/out-of-order/duplicate input is handled according to the existing rules and recorded in quality diagnostics; no silent timestamp sorting or liquidity invention.

#### B3 — Private recording/replay UI and actual-data acceptance

- [x] Add private capture controls and a saved-recording selector showing feed, symbols, interval, counts, quality status, and immutable dataset identity.
- [x] Add a recorded-mode badge, one-symbol selection, market-time entry selection, simulated-order ticket, replay status, and durable quote-linked explanations.
- [x] Show capturing/finalizing/incomplete/failed states and actionable errors. Disable replay for unready/corrupt recordings; never label recorded prices live.
- [x] Use bounded/paged timeline reads and rendering for longer recordings; keep older triggering quotes retrievable without loading every event into the browser at once.
- [x] Keep frontend private-filesystem access mediated by the API and safe server-selected IDs; never accept arbitrary paths from browser requests.
- [ ] After account permission checks, capture one actual bounded IEX interval, stop capture, and demonstrate offline replay with immutable provenance. No real orders or public vendor screenshots are authorized by this checklist.
- [x] Run Python/integration tests, frontend production build, and private/public browser regressions. Record actual-input acceptance separately from generated fixture tests.

### Extension C — Two-Experiment Comparison and First Divergence

Goal: explain a controlled difference in simulated execution using the same recording and shared entry point. This is the distinctive workflow to validate with a user, not a claim that replay or fill simulation is a new invention.

#### C1 — Comparison contract and deterministic explanation

Build/checklist:

- [x] Define a comparison with one recording checksum/version, one symbol, one entry anchor, one side, one engine/model version, and two configurations. Runs have separate namespaces and independent liquidity under the current model.
- [x] Support exactly one varied parameter in v1: quantity, artificial latency, limit price for two limit orders, or market versus limit type with the required limit price. Other fields stay fixed; reject ambiguous multi-parameter comparisons.
- [x] Compare weighted fill price, fill rate/filled quantity, remaining quantity, existing spread cost, and activation-to-fill/completion times. Show configured latency and entry-to-fill time separately so existing activation-relative metrics are not mislabelled.
- [x] Represent no fill/no completion with an explicit unavailable value, not zero. Do not invent a separate broker-slippage metric or subtract unavailable values.
- [x] Compare event-indexed execution decisions from the same existing rules. Find the first source event where activation, eligibility, filled quantity, or terminal state differs; do not compare opaque fill IDs across independent runs or attribute every result to the final fill.
- [x] Explain only supported rule-level causes: activation delay, limit eligibility, or order quantity versus displayed liquidity. Provide source event, values/configuration, and the rule; no LLM or unconstrained causal narrative.
- [x] Distinguish first state divergence from first fill divergence and final outcome difference. Return explicit no-divergence/no-outcome-difference states when appropriate.
- [x] Use canonical deterministic explanation data generated from/reconstructed by the engine; do not duplicate fill-rule logic in the frontend or build a separate simulator.

Verify/gate:

- [x] Generated scenarios cover each supported varied parameter, equal-timestamp boundaries, different partial-fill progress, no fills, identical final outcomes despite intermediate divergence, and no divergence.
- [x] Explanation points to the earliest qualifying source event and matches both experiments' actual states, quotes, and parameters.
- [x] Inverting baseline/variant reverses signed result deltas without changing the source identity of the divergence.
- [x] Identical configurations form a test control with no divergence; different model/dataset/entry identities cannot be presented as a controlled comparison.
- [x] Replacement/replay speed changes preserve canonical comparison results and explanations.

#### C2 — Focused comparison UI

- [x] Add baseline/variant configuration, a single changed-parameter control, two result columns, and an aligned recorded-event timeline.
- [x] Highlight the first divergence and let the user inspect the responsible quote and decision on both sides. Keep timeline sampling separate from authoritative event-level explanation.
- [x] Show both run states, dataset identity, entry point, model limitations, and data quality. If either run fails or is incomplete, display comparison failure rather than ranking its performance.
- [x] Display signed deltas with units and side-aware price interpretation; lower prices are not automatically better for a sell order. Avoid investment recommendations or claims of real broker execution.
- [x] Verify source-event lookup, reconnect/result recovery, invalid configuration, loading/no-data states, and responsive bounded rendering in the browser.
- [ ] Regression-check existing live orders, standalone recorded replay, and public generated demos. Production frontend build and full isolated Python suite pass.

#### C3 — User validation and presentation gate

- [ ] Ask the user to recruit their stock-interested friend or another intended user; do not contact people or share private recordings without explicit authorization.
- [ ] Give the participant a concrete task: select a saved interval, vary one parameter, identify which experiment differs, and explain the responsible event without terminal access.
- [ ] Record observed task completion, time/help required, explanation accuracy, confusion, and an actual proposed reuse scenario. Distinguish interview intent from demonstrated repeat use; do not fabricate adoption metrics.
- [ ] If the task is confusing or has no useful return reason, make the smallest evidence-backed workflow adjustment before adding features.
- [x] Position the project as an auditable recorded-market execution comparison tool, not an unprecedented simulator or proven quant-company production tool.
- [x] Use generated recordings for public screenshots/demo until vendor display permissions are established; keep actual recordings private and out of the repository.
- [x] Update product/architecture/data/UI/setup descriptions and benchmark/resume claims only after the corresponding implementation and acceptance pass. Keep throughput, durability, playback speed, execution latency, user results, and recovery measurements separate.

Engineering checks marked above use generated input. Public replay completed in the browser, and standalone/private replay is covered by API/subprocess tests. Simultaneous actual live ingestion and recorded experiments, an actual saved IEX interval, and fresh-live browser regression remain unchecked.

Manual acceptance procedure: confirm account storage rights, start the documented local dependency-ordered stack, wait for actual fresh subscribed quotes, capture a short interval, stop it, and verify a ready checksum. Stop ingestion only after noting its prior state, replay the saved interval offline, inspect original quote-linked fills, and restore prior services. Never remove streams, leases, recordings, or database rows to make a check pass. For physical sleep/network testing, coordinate with the user during fresh quotes, record existing fill IDs, suspend/resume the laptop and separately interrupt/restore networking, then verify fresh quotes, renewed ownership and a subsequent simulated fill without duplicates. Record failures; do not claim automatic startup or physical recovery from subprocess tests.

Participant task: with the user recruiting a consenting participant, use a generated saved interval, compare 0 ms versus 100 ms latency, and ask them to identify the first responsible quote and explain the outcome without terminal access. Record completion, elapsed time, help, explanation accuracy, confusion and a proposed reuse reason. No participant is contacted by this implementation.

### Build order and completion rules

1. A1 readiness fix and repeated tests → A2 commit/recovery gaps → A3 operational acceptance when the user/feed are available.
2. B1 contracts and privacy/bounds → B2 generated offline service replay → B3 UI and actual recording acceptance.
3. C1 deterministic comparison tests → C2 browser workflow → C3 observed user task → Phase 7 presentation.

The original default was one numbered build batch at a time; the user's October 5 request explicitly authorized building the complete extension together. Test each layer and retain exact results, limitations, and a suggested commit message. Do not run Git commands. A planning/documentation commit is not an implementation or acceptance result.

No new cloud deployment, multi-machine scaling, historical downloader, full-depth model, shared-liquidity matching, reconciliation ledger, portfolio, arbitrary parameter sweep, authentication system, or public raw-data publication belongs to this extension. Missing rights, live feed, physical-suspension participation, or user-validation access must remain explicit blockers to the relevant acceptance, not substituted generated passes. Existing bounded recovery assumptions and known timing/normalization limitations remain visible until evidence resolves them.

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
