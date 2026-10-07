# Pre-Phase 7 Completion: A / B / C

October 7 presentation update: Phase 7 documentation is in README, ARCHITECTURE, BENCHMARKS, DEMO, RESUME_BULLETS, and INTERVIEW. The generated 300-event NVDA comparison completed in a separate local preview; screenshots contain no vendor data. Phase 7 packages the existing implementation/evidence; it does not close physical sleep/network recovery, owner usability, reliable unattended startup, or post-extension fresh-live browser acceptance. Hosting and independent demand remain deferred/unverified.

## October 5 recorded-market extension

The new [Extension A/B/C section in IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md#extension-a--b--c--recorded-market-execution-comparisons) is authoritative for recovery hardening, private recorded replay, and first-divergence comparisons. Earlier A/B/C work below is historical shipped functionality, not evidence of this extension's manual acceptance. Actual vendor recording/storage permission, actual saved-input offline acceptance, physical sleep/wake/network interruption, and a participant usability task remain distinct from generated automated/browser tests.

October 6 update: under user-reported personal recording permission, [actual capture/offline comparison acceptance passed](../benchmarks/extension-live-recording-acceptance-2026-10-06.md). The saved 680-event interval reproduced canonical results at different publication pacing with SQL-verified unique quote-linked fills and unchanged live registry/subscriptions. Owner usability, fresh-live browser regression and physical laptop/network recovery remain unverified; independent user demand is not claimed. Private recordings are ignored/excluded and ordinary local services were stopped afterward.

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
- [x] Measure an actual private live peak, then certify twice that offered rate for at least 600 seconds. October 5: measured 141/sec AAPL/MSFT IEX peak; 169,200 generated events at a 282/sec target over 600 seconds, zero lost/duplicate events, drained backlog, and no worker errors. See [local acceptance evidence](../benchmarks/local-c-live-acceptance-2026-10-05.md).
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

Historical conclusion before Local A: actual provider-to-UI simulated execution and manually assisted replacement passed, but immediate automatic restart recovery failed. No mid-fill crash, multi-day stability, ten-minute capacity, or production scaling was certified by this smoke test. The 93-test suite/build result above predates it; no application code was changed during that initial test.

## October 2, 2026 Local A Recovery Acceptance

The same local session remained active during this test, approximately 12:45–12:52 PM Pacific. Generated integration tests used separate local test containers; neither cloud deployment nor hosted data was changed. Only simulated orders were submitted.

- Full Python suite: **106 passed**, with one existing Starlette/httpx deprecation warning. Next.js production build and private Compose configuration validation passed.
- Replacing the old engine/persistence images left their old leases intact. Both new workers logged waiting, acquired after expiry, and became ready without a second restart or manual lease deletion. Engine acquisition took about 29 seconds; retained-history reconstruction added about 10 seconds.
- A subsequent immediate graceful engine restart released its owned leases and acquired without waiting. Started at `19:47:46.599Z`, ready at `19:47:57.554Z`: approximately 11 seconds, including history reconstruction. The session ID did not change.
- Independent persistence restart became ready at `19:48:25.529Z`, immediately after acquisition, without changing the session.

| Actual-feed order | Verified durable outcome |
| --- | --- |
| Before immediate engine restart: `70e8df52-e7cc-408b-a437-4abed9dd2206` | Buy 5 MSFT; one fill at 516.300000; quote `alpaca:q:MSFT:2026-10-02T19:47:19.834416571Z:516.22:516.3:40:120`. |
| After engine recovery: `e47e74d0-46bf-46ed-85a1-17f65778b98e` | Buy 5 MSFT; one fill at 516.060000; quote `alpaca:q:MSFT:2026-10-02T19:48:11.560953366Z:516.01:516.06:40:40`. |
| After persistence recovery: `d374b787-bc68-45e0-9651-0cefa7c1e5b4` | Buy 5 MSFT; one fill at 517.340000; remaining zero. |

SQL joins verified all five orders in the session, including the original two AAPL orders: exactly one fill totaling five shares each; fill prices equal stored triggering asks; quote times follow submission; sizes fit normalized visible liquidity. Generated subprocess tests independently replace engine and persistence after SIGTERM and SIGKILL and assert unchanged original fill IDs. These tests shorten isolated crash-test leases to one second; real worker leases remain 30 seconds. Slow reconstruction also retained ownership beyond a two-second test lease.

Intentional Docker stop/kill is an administrative stop, not a restart-policy crash test. The intentionally stopped local engine was restored. Sending SIGKILL to namespace PID 1 from inside the container had no effect; no Docker-level automatic crash restart is certified by that attempt. The process-level SIGKILL replacement tests and actual old-lease/graceful replacement checks are separate evidence. The active local ingestion container was not replaced, to avoid an uncoordinated session change; its updated image and SIGTERM cleanup were tested separately with a generated/stubbed feed.

Local A is complete. Leases are cooperative ownership checks, not database fencing: an already-running I/O operation cannot be cancelled retroactively when ownership expires. Idempotent durable fill identifiers remain the duplication safeguard. Ingestion/session coordination after restart or suspension, active-session rollover, and bounded historical retention remain Local B. Multi-day operation, a real-feed mid-fill crash, and 600-second capacity certification remain unverified.

After the accepted fills, at approximately 12:52 PM Pacific, the AAPL source partition reached the existing 100,000-message cap. Ingestion exited with `live session capacity reached; start a new session`; retained data stayed intact and the dashboard disabled orders as stale. At that point no cap was raised, active history deleted, or replacement session started; engine/persistence remained running against that session. This was the Local B rollover limitation addressed below.

## October 2, 2026 Local B Lifecycle Acceptance

- Full isolated Redis/PostgreSQL suite: **120 passed**, with one existing Starlette/httpx deprecation warning. Next.js production build, local images, and private Compose configuration validation passed. Additive migration `0005_private_session_lifecycle` was applied locally.
- Generated service-process tests verify ingestion restart with persisted subscriptions, three rollovers with unchanged worker PIDs, and retained quote-linked fills. Time/message/order limits close partial orders as cancelled with reason `session ended`, preserving existing fills. Five injected interruption stages retry with exactly one successor. Cleanup tests cover bounded SQL batches, closed Redis expiry, active-history protection, and retained triggering quotes.
- The actual capped session `31c2fccb-0da8-4ee0-a26e-e7fcf3edba43` drained and completed, activating `2b3fe528-ee18-4b53-9ad0-30dcbb039fa8`. Both workers followed the registry. An ordinary ingestion-container restart retained that successor and AAPL/MSFT subscriptions. SQL checks preserved the five earlier orders with exactly one five-share fill each matching its triggering ask.
- Subsequent timed rollover activated `410a6005-492e-4e63-85d7-31e8d1548b4a`. On resuming work, ingestion/engine/persistence were stopped after three ownership-loss retries. macOS power logs record repeated sleep/dark-wake intervals from 13:25 through 13:55 Pacific, coinciding with the failures. This supports suspension-related lease expiry, but does not prove unattended recovery succeeds: the bounded retry budget was exhausted.
- Explicitly starting the three existing containers restored processing without deleting leases, streams, or database rows. The expired-duration session closed and activated `c6e930ae-2b05-438d-8b78-e98c130714cd`; both workers became ready against that run. The old migration container was also discovered to predate migration 0005 and was rebuilt for future startup.

Defaults are 30-minute sessions, 90,000 messages per partition, 240 orders, 15-minute closed Redis recovery grace, one-hour closed raw-event retention, and 24-hour closed order/fill explanations. Expired private data is intentionally removed; export evidence before its deadline. Closed Redis history has expired under this policy, while retained fill explanations remain in PostgreSQL. Restore deleted history only from a prior export/backup. Active reconstruction history is not trimmed.

Actual post-update fresh quotes and execution were not verified after hours. Unattended physical sleep/wake recovery remains an open operational limitation requiring a deliberate drill and retry-policy decision. Multi-day stability and a 600-second run at twice a measured nonzero live peak remain Local C, not certified capacity.

## Local C Market-Window Completion Runbook

This is the remaining external acceptance, not an automatic scheduled task. Use the existing `tickertrace-local` instance and isolated local benchmark dependencies. Do not change the public deployment or use hosted free-tier storage for load tests.

1. During an active quote window, open `http://localhost:3030`, confirm AAPL/MSFT show Fresh, submit a five-share simulated market order, and save its run/order IDs. Verify the durable fill and triggering quote via `/api/v1/orders/<order_id>` and `/api/v1/orders/<order_id>/events`. Quote time must follow submission and fill price must equal the triggering side's price. No real brokerage orders are authorized.
2. Restart engine, then persistence separately. Confirm registry identity, fresh quotes, a subsequent simulated fill, and unchanged original fill IDs. Exercise a small limit order and artificial latency separately; an unfilled limit is not a failed test if price never crosses it. Verify stale quotes disable submission, then wait for actual fresh quotes to enable it again.
3. To avoid opening a competing provider connection, stop normal ingestion while measuring the peak with its existing credentials and AAPL/MSFT settings:

```bash
POSTGRES_PASSWORD=tickertrace-local-only docker compose --project-name tickertrace-local --env-file .env -f infra/docker-compose.private.yml stop ingestion
POSTGRES_PASSWORD=tickertrace-local-only docker compose --project-name tickertrace-local --env-file .env -f infra/docker-compose.private.yml run --rm --no-deps ingestion market-execution-measure-live-peak --duration-seconds 60
POSTGRES_PASSWORD=tickertrace-local-only docker compose --project-name tickertrace-local --env-file .env -f infra/docker-compose.private.yml start ingestion
```

Record the date/time, subscription set, total events, duration, and nonzero one-second peak. The CLI measures normalized quote/trade events, not all-exchange market volume. Restart normal ingestion even if measurement fails. A zero peak is not an acceptance value; repeat during a genuinely active window without changing providers or claiming fabricated data is live.

4. Start the dedicated test Redis/PostgreSQL, then substitute twice the recorded peak for `TARGET` below. Use a separate JSON path so the synthetic endurance evidence is not overwritten:

```bash
docker start tickertrace-pre7-redis tickertrace-pre7-postgres
caffeinate -i .venv/bin/python -m market_execution_lab.validation_service --redis-url redis://127.0.0.1:56379/2 --database-url postgresql+psycopg://tickertrace:pre7-local-test@127.0.0.1:55432/tickertrace --partitions 4 --workers 2 --duration-seconds 600 --target-events-per-second TARGET --revision operator-recorded-source-hash --environment-note 'Record hardware, Docker limits, live peak measurement and workload here' --output benchmarks/local-c-live-rate.json
```

Accept only an exit-zero result with offered rate at least 95% of target, steady backlog slope within the documented tolerance, fully drained backlog, zero worker errors, zero duplicate/lost durable events, and complete sample orders. Record load failures too; do not raise limits or replace the target to manufacture a pass. This is a local-thread generated-load test sized from a live peak, not multi-machine or browser capacity.

5. Repeat a deliberate physical sleep/wake drill during fresh quotes, with data retained and the laptop returned to normal operation; verify recovery, freshness, and duplicate-free execution. Container/process pauses are narrower evidence and do not replace this check. Capture generated replay visuals for public demos until account-specific vendor display/redistribution permission is established.

Vendor review on October 2: [Alpaca's real-time stock documentation](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data) documents the IEX stream and subscription-scoped access, not public screenshot redistribution permission. [IEX's market-data documents](https://www.iex.io/resources/trading/market-data) require applicable agreements. These sources do not establish this account's public-display permission; no raw live-data publication was performed.

## October 2, 2026 Local C Engineering Evidence

Local work is complete; the active-market and deliberate physical laptop/network checks above remain open. No cloud services were changed, no real brokerage orders were placed, and no public raw-data screenshot was published.

### Recovery and shutdown

- Final isolated Redis/PostgreSQL suite: **125 passed**, with one existing Starlette/httpx deprecation warning; Next.js production build and private Compose configuration passed.
- Added typed ownership-loss recovery at the live entry points. Each attempt releases only owned leases, waits one second, acquires a new owner token, and resumes/reconstructs against the registry. This preserves exclusivity instead of silently extending an expired lease. Unrelated failures and healthy competing ownership still fail through bounded paths. Fatal authentication does not restart; mixed authentication/unrelated exception groups are no longer silently treated as authentication-only.
- A container pause initially exposed an additional Redis read-timeout path. Recovery now treats that timeout as lease loss only after Redis independently confirms the old ingestion token is no longer current. If that confirmation fails or an unrelated error is present, the error remains fatal to that attempt.
- Generated service tests perform three rollovers, then four SIGSTOP/SIGCONT cycles with isolated leases shortened to one second. The first pause lasts 11 seconds, beyond the service Redis socket timeout. All process IDs stay unchanged; tests wait for a new cached symbol quote, not just a fresh ingestion heartbeat, and verify a subsequent simulated order plus unchanged original fills.
- Actual Docker pause drill: all three running live containers paused at approximately `22:32:54Z`, unpaused after their normal 30-second leases expired, and logged acquisition/recovery without Docker restarts. Ingestion/engine/persistence host PIDs remained 4801/3819/3879, with restart counts zero. The run stayed `c6e930ae-2b05-438d-8b78-e98c130714cd`, subscriptions stayed AAPL/MSFT, and readiness returned. This is container suspension evidence, not a physical laptop/network test or fresh market execution.
- Full-stack Compose `stop` shut down ingestion/engine/persistence with exit code zero and stopped frontend/API/Redis/PostgreSQL. Compose `start` restored services and migration 0005 completed successfully. SQL after restart verified five retained orders, five fills totaling 25 shares, all matching their subsequent triggering asks. Browser close alone does not stop services.
- Application update also gracefully recreated dependencies during this run. Redis restored 131 keys from an RDB saved in its inherited anonymous `/data` volume; current source history and registry survived. This observed success is not a durable backup guarantee. Future application-only image updates should use `--no-deps` after migrations succeed.
- Browser reload after recovery showed a connected session with no fresh quotes and a disabled simulation button, as expected after hours. Screenshot `/private/tmp/ticker-trace-local-c-stale.png` contains no raw quote values. No post-update provider-fill claim follows from this stale-feed check.

### Ten-minute synthetic workload

`benchmarks/local-c-endurance.json`: 600 seconds requested, 600.11 seconds total elapsed, 120,000 offered and persisted events, 199.99 events/sec offered against an arbitrary 200/sec target. Enqueue-to-market-event-commit p50/p95/p99: 1.68/8.06/32.79 ms. Maximum sampled engine/persistence backlog: 20/27; steady slope -0.00176 messages/sec; final backlog, duplicate durable observations, and worker errors all zero. The independent in-memory million-event check detected 10,000 duplicate IDs and produced zero duplicate fills. These are separate workloads.

Environment: Apple M3, 16 GiB host, macOS 27.0.1, Python 3.13.1, shared Docker 29.8.1 VM with eight CPUs and 8,215,117,824 bytes RAM; Redis 7.4.11 and PostgreSQL 16.15. Dataset `continuous_quotes_v1`, four partitions, two threads per consumer role in one Python process. Regression tests, container builds, and the live stack shared the host. Benchmark dependencies were dedicated local containers, not hosted free-tier services. Idle sleep was inhibited only while the benchmark ran.

The benchmark started before the Part C retry edits; loaded-source SHA-256 provenance:

| Source | SHA-256 |
| --- | --- |
| `validation_service.py` | `7d10c1e5d33efa8b6e2ab94922fa21571dce18e0d8a031e35ff0c0d481e7859f` |
| `live_pipeline.py` | `3a5a09b0949514115f998c8326eae91c53486cb96e73c93ec3a236e7c4c68015` |
| `storage.py` | `4b6ef22ff2076568d47107f46a551c460dbcb3c0a15fd7a483ffbfbe2f19a3dc` |

Final recovery sources were separately tested; no Git commands were used:

| Source | SHA-256 |
| --- | --- |
| `live_ownership.py` | `8565effd62c48cd7bda36e71f146aeadcea2368f004210b4593e6e44c140b4fa` |
| `live_pipeline.py` | `470c2dc850e923d57cd78081b011c400ba345c60e2c735281a1ca4f15d7f364c` |
| `ingestion_service.py` | `16171427f67332bcb8d97857a6015b66760ede8add2c6287b89ea8353e4bb4d7` |
| `live_sessions.py` | `0f7423f8a436ba12452022e33e44be0799e4c7eae7996323f3ccbd2fb7874d50` |

This run does not establish twice-live-peak capacity, multi-machine scaling, fill/browser latency, indefinite uptime, complete physical suspension recovery, or public redistribution rights. The remaining checklist must stay open until those scoped acceptance steps are actually verified.
