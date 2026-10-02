# Pre-Phase 7 Completion: A / B / C

## Scope and boundaries

Build the missing live execution path, meaningful generated experiments, and honest continuous-load validation before presenting the project. This is not post-trade reconciliation, real-money trading, or an exchange matching engine. Existing public Fly/Vercel services remain generated-only and idle-safe. Private live operation uses separate local/private infrastructure, explicit operator credentials, and ongoing resource usage. No production deployment or provider disruption is part of this implementation.

Independent orders are separate top-of-book experiments; they do not compete for shared liquidity. Retained private session streams are bounded, and reaching capacity requires a new session rather than silently deleting recovery history. Private endpoints must never be exposed without a private network/access boundary.

Current direction as of October 2, 2026: local live execution is the primary project workflow. The new Local A/B/C checklist in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md#local-first-completion-plan--october-2-2026) tracks restart recovery, session lifecycle/retention, and final evidence. The older cloud acceptance items below remain historical deferred work, not local-first completion gates.

## A — End-to-end private live execution

### Build

- [x] Register an operator-owned live session with supported symbols and connection/freshness status.
- [x] Continuously ingest Alpaca quotes/trades into stable session partitions; preserve timestamps and deduplication identifiers.
- [x] Authenticate and subscribe explicitly; reconnect transient disconnections with bounded backoff, report disconnects, reject fatal authentication errors, and skip malformed quotes safely.
- [x] Support subscription changes from the UI without environment-file edits; limit the watchlist to ten validated symbols.
- [x] Add independently runnable continuous engine and persistence workers with exclusive partition ownership, retained-stream reconstruction, pending recovery, and idempotent writes.
- [x] Process multiple independent simulated orders against subsequent events, publish intermediate snapshots, and persist progress without completing the live session.
- [x] Reject live orders when the feed/quote is stale, the symbol is unsubscribed, or retained stream/backlog/order capacity is exhausted.
- [x] Add a private live UI with real quote age, symbol controls, order ticket, continuous snapshots, and fill-event explanations.
- [x] Provide an isolated private local launch configuration; keep public Fly settings unchanged.

### Verify

- [x] Unit tests cover authentication, reconnect/cancellation, malformed messages, freshness, and public rejection.
- [x] Local Redis/PostgreSQL tests cover quote → command → subsequent quote → fill → durable result → WebSocket snapshot.
- [x] Worker replacement reconstructs the same fill IDs without duplicating durable fills.
- [x] Competing partition owners cannot both process; stopped consumers leave recoverable work.
- [x] An actual Alpaca market-window session produces a simulated fill tied to a real normalized quote. Verified locally October 2, 2026; see the real-feed evidence below. This does not certify automatic restart recovery or sustained capacity.

## B — Meaningful replay experiments

### Build

- [x] Retain small regression fixtures, and add versioned public datasets with hundreds of deterministic quotes/trades, changing spread, size, volatility, replenishment, and gaps.
- [x] Bound public fixture size and keep temporary-key TTL, monthly admission, and idle shutdown behavior intact.
- [x] Add backend event-time pacing and controlled-rate replay to the CLI/local path; public demand mode remains fast finite replay to avoid long HTTP requests.
- [x] Dispatch local engine work only after publication completes; never run an incomplete replay as if it were final.
- [x] Clearly separate client trace playback from producer delivery speed in UI/docs.
- [x] Preserve zero spread, zero latency impact, and zero-duration metrics through serialization and PostgreSQL.
- [x] Compare size/latency changes on identical events; keep independent top-of-book assumptions explicit.

### Verify

- [x] Longer datasets are ordered, generated-only, deterministic, and remain within the public event cap.
- [x] Quantity and artificial-latency changes produce explainable differences.
- [x] Paced and maximum-speed publication produce identical fills; timing tests use an injected clock, not slow sleeps.
- [x] Replayed duplicates, stale events, malformed messages, replacement workers, and zero-valued metrics are covered.
- [x] Frontend type/production build and browser workflow pass.

## C — Honest capacity evidence and operational acceptance

### Build

- [x] Replace drain-then-repeat certification with an independent rate-controlled producer and concurrent continuous consumers.
- [x] Sample source/result backlog while production is active, record sample timestamps, final backlog, growth slope, producer achieved rate, and drain timeout.
- [x] Measure per-event publish-to-durable-write latency percentiles; distinguish these from batch processing times and artificial execution latency.
- [x] Fail certification for insufficient offered rate, growing backlog, worker exceptions, missed/duplicate persisted events, or drain timeout.
- [x] Record platform/CPU, Python version, input configuration, concurrency implementation, dataset version, duration, and optional operator revision identifier without invoking Git.
- [x] Treat million-event in-memory dedupe and full distributed-pipeline correctness as separate claims.
- [x] Correct README, architecture, pipeline, testing, and resume drafts so implemented behavior is distinguished from deferred features.
- [x] Document reproducible commands and a manual provider-counter acceptance record.

### Verify

- [x] Short local continuous-load tests validate the harness, including insufficient rate/backlog/failure cases.
- [x] Save fresh local evidence with explicit methodology; do not relabel old thread benchmarks as distributed-machine scaling.
- [ ] Measure an actual private live peak, then certify twice that offered rate for at least 600 seconds. External market-window acceptance.
- [ ] Deferred cloud acceptance: redeploy explicitly, complete a public browser smoke test and five concurrent submissions, and verify exact worker counts return to idle.
- [ ] Deferred cloud acceptance: observe 24 hours without dashboard traffic and record Fly machine state, Upstash command/bytes, and Supabase size/egress deltas. No scheduled app probes.

## Exit gate

All local build/test items and the current Local A/B/C completion gate must pass. The real-feed smoke test is recorded below; sustained-rate certification remains pending. Deferred cloud checks stay unchecked and must not be represented as free-tier idle guarantees or deployed capacity.

## Execution order

1. A contracts, workers, adapter, APIs, UI → targeted tests and private pipeline integration.
2. B longer fixtures, pacing, metric fixes → regression/integration tests and frontend build.
3. C continuous validation and accurate docs → short local evidence, full suite, browser smoke, and explicit external-check handoff.

## Local evidence and remaining acceptance

- Final full Python suite: 93 passed against isolated local Redis/PostgreSQL, including replacement recovery, competing ownership, stale/public rejection, late snapshot regression, PostgreSQL zero-metric preservation, bounded retained-stream admission, and paced/unpaced durable-result equality. A two-second integration check verifies harness data flow; deterministic unit tests cover acceptance thresholds, while capacity acceptance uses the separate longer run.
- Next.js production build passed. The public browser replay completed 300 events and a 1,000-share order with partial fills and a latency baseline. The private browser test used generated local quotes, not Alpaca, completed a simulated order with its persisted triggering quote, and disabled order submission when the feed stopped. No browser console errors were recorded during the private check.
- `benchmarks/pre-phase-7-local.json`: 3,000 events, 198.83/sec offered, zero final backlog or duplicate events, 3.82 ms durable-write p95; 15-second local-thread evidence only.
- Actual Alpaca quote-linked fills were verified October 2; a measured nonzero live peak and the 600-second check at twice that rate remain pending.
- Production deployment, five concurrent deployed submissions, worker idle return, and 24-hour provider counter deltas are deferred under the local-first direction. The local test did not redeploy or test hosted services.

## October 2, 2026 Real-Feed Local Acceptance

Environment: laptop-hosted Docker project `tickertrace-local`, frontend `http://localhost:3030`, API `http://localhost:8030`, isolated Redis/PostgreSQL, Alpaca IEX subscriptions AAPL/MSFT. The browser and database were checked between approximately 11:18 and 11:22 AM Pacific. No real brokerage orders were placed.

Session: `31c2fccb-0da8-4ee0-a26e-e7fcf3edba43`.

| Check | Recorded outcome |
| --- | --- |
| Fresh actual quotes | AAPL/MSFT appeared in the browser after ingestion and both workers were restarted into the new session. |
| First simulated order | `67b8af48-d4ed-4f16-aee3-4c6997396e67`: buy 5 AAPL, filled 5 at 332.890000, remaining 0. |
| First triggering quote | `alpaca:q:AAPL:2026-10-02T18:19:02.901718744Z:332.86:332.89:40:80`; stored ask 332.89, normalized ask size 8,000 under the MVP lot assumption. |
| Immediate execution-worker restart | Failed with `live partition already has an owner`. The old lease survived process termination. |
| Manual recovery | Confirmed the old lease had expired, then restarted the execution worker again. Fresh quotes and order admission resumed. |
| Post-recovery simulated order | `63b934d9-0f28-4330-ab24-a032c220ffc9`: buy 5 AAPL, filled 5 at 332.940000, remaining 0. |
| Post-recovery triggering quote | `alpaca:q:AAPL:2026-10-02T18:21:20.8826782Z:332.9:332.94:40:120`; browser showed bid 332.90, ask 332.94 and normalized ask size 12,000. |
| Durable correctness | PostgreSQL joins verified both fill prices equal their triggering quote asks, quote times were after order submission, and fill sizes did not exceed normalized displayed sizes. Each order had exactly one fill row totaling 5 shares. |

The three live processes were also found stopped at the start of this test with ownership-loss errors from the previous overnight session. The logs establish lost leases, not whether laptop/Docker suspension caused them.

Conclusion: actual provider-to-UI simulated execution and manually assisted replacement passed. Immediate automatic restart recovery failed and remains the first Local A task. No mid-fill crash, multi-day stability, ten-minute capacity, or production scaling is certified by this smoke test. The 93-test suite/build result above predates this operational test; no application code was changed during it.
