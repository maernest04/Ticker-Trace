# Testing Strategy

## Recorded-market extension verification

`tests/test_recorded_replay.py` covers versioned manifest/content validation, checksum/count/source-boundary corruption, equal-time entry exclusion, recorded source order, stale/duplicate diagnostics, capture caps/rollover/history loss/restart, offline subprocess replay, deterministic repeated execution, paged source/fill/trace evidence, and public REST/WebSocket denial. Generated inputs are explicitly labelled and are not actual IEX acceptance.

`tests/test_commit_boundaries.py` interrupts engine result publication, source commits, and fill commits both before and after their effects, then reconstructs partial orders and verifies stable original fills, completion quantities, and duplicate suppression. Finite source commit recovery covers more than 100 pending messages. Existing ownership, malformed/dead-letter, missing-history, rollback, replacement, and stale-snapshot tests remain the baseline.

The lifecycle pause test now waits for all engine/persistence leases, a fresh successor-symbol quote, and a persisted probe before suspension. Repetitions must have a dedicated PostgreSQL database, not merely a separate Redis DB: retention tests intentionally prune globally eligible closed sessions and can delete another concurrent test's evidence. Keep the generated workload, lifecycle repetitions, browser preview, and regression suite isolated. Record failures and subsequent corrections separately; never interpret container pause as physical laptop sleep/wake.

Full isolated regression run on October 5: **168 passed**, with the existing Starlette/httpx deprecation warning; the final validation-message change subsequently passed all **43 focused extension tests**. Five quiet and five isolated contended lifecycle repetitions passed. Browser verification uses a generated 300-event recording, never a vendor recording, and includes comparison completion, source paging, restored results, and a 390-pixel responsive overflow check. Public replay also completed in the browser. See [extension evidence](../benchmarks/extension-abc-acceptance-2026-10-05.md) for failures, exact repetitions, and remaining manual gates.

## Pre-Phase 7 implementation coverage

The older unchecked strategy lists below describe desired scope, not proof of completion. New tests cover non-finalizing live snapshots, zero-valued metric round trips, deterministic longer datasets, size/latency differences, CLI pacing, public live-endpoint rejection, symbol bounds, fatal authentication, transient reconnect/malformed-message handling, subscription-task cleanup, quote freshness, ongoing live order execution, partition-owner rejection, pending reconstruction, and continuous backlog/durable-latency measurement.

The private integration path uses normalized **generated Alpaca-format messages**, not an actual market connection. Local Redis/PostgreSQL demonstrate submitted → subsequent quote → filled, private WebSocket output, one durable fill after replacement, and public denial of that private data. Actual provider-fill acceptance remains separate.

Run with dedicated local dependencies, not the hosted free tiers:

```bash
RUN_STREAMING_INTEGRATION=1 APP_MODE=public_replay REDIS_URL=redis://127.0.0.1:56379/0 DATABASE_URL=<dedicated-local-postgres-url> .venv/bin/python -m pytest -q
```

The continuous acceptance test offers generated input independently of consumers and samples backlog during production. It is not the former drain-after-each-batch test. External market, deployed concurrency, rollback, and 24-hour provider-counter checks remain operator acceptance tasks in `PRE_PHASE_7.md` and `DEPLOYMENT.md`.

## Current Local-First Acceptance

Follow the Local A/B/C checklist in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md#local-first-completion-plan--october-2-2026). Local A/B and available Local C engineering/evidence work are implemented; actual-market, measured-live-rate, and physical laptop/network checks remain.

- [x] Local A: immediate restart waits safely for an unexpired old lease and resumes without manual retry; a competing healthy owner is never displaced.
- [x] Local A: partial ownership acquisition, graceful/abrupt termination, slow reconstruction, and lost ownership preserve exclusivity and idempotent fills.
- [x] Local A: execution and persistence replacements recover independently; repeat the actual provider-linked browser test after the fix.
- [x] Local B: generated-feed ingestion restart restores the same session/subscriptions, fresh quotes, and execution; actual local container restart retains the successor registry/run.
- [x] Local B: forced time/message/order rollover closes partial orders explicitly, drains source/results, and activates exactly one replacement session.
- [x] Local B: retries at five injected rollover interruptions are safe; bounded cleanup preserves active work, retained triggering quotes, and unrelated benchmark records.
- [x] Local C: typed lease-expiry recovery; four generated-process pause/resume cycles retain PIDs/fills, including a pause beyond the Redis timeout, followed by another unique quote-linked simulated fill.
- [x] Local C: actual Docker pause beyond 30-second leases resumes all three processes without replacement or Docker restarts; explicit stack stop/start preserves the five original actual-feed fills.
- [x] Local C: final full suite **125 passed**, frontend build passed, Compose configuration passed. One existing Starlette/httpx deprecation warning remains.
- [x] Local C: 600-second synthetic endurance at an arbitrary 200/sec target persists all 120,000 events; 8.06 ms p95 enqueue-to-event-commit latency, zero duplicates/loss, errors or final backlog.
- [x] Local C: workload/environment/source hashes, vendor review and generated-only public visual policy documented; public raw-data permission is unresolved.
- [ ] Deliberate physical laptop/network suspension and fresh actual-feed post-update simulation remain unverified. Process/container pause recovery is narrower evidence.
- [ ] Measure a nonzero active-market peak, then offer twice that measured rate for 600 seconds. The arbitrary-rate synthetic run does not satisfy this gate.

October 2 manual actual-feed smoke: two five-share AAPL orders filled at 332.89 and 332.94; SQL verified subsequent-quote linkage and one fill row per order. Immediate engine restart failed because the old lease still existed. Retrying after expiry restored fresh quotes and subsequent execution. This is manually assisted recovery, not a passed immediate automatic recovery test or a mid-fill crash test. Full identifiers and scope are in [PRE_PHASE_7.md](PRE_PHASE_7.md#october-2-2026-real-feed-local-acceptance).

After Local A: 106 Python tests passed against isolated Redis/PostgreSQL, and the Next.js production build passed. New coverage includes deterministic retry deadlines/cancellation, compare-and-delete ownership, real lease expiry/loss, reconstruction lasting longer than its two-second test lease, ingestion SIGTERM cleanup without touching a replacement owner/session, and independent worker SIGTERM/SIGKILL replacements preserving fill IDs. The subprocess crash tests shorten only isolated test leases to one second; production leases remain 30 seconds. Actual-feed graceful replacement and surviving-old-lease upgrade were verified separately, with quote-linked MSFT fills. This does not certify multi-day operation, laptop suspension recovery, or crash-fencing of an already-running database operation. Cloud concurrency, rollback, and 24-hour usage checks remain deferred.

## Testing Goals

Local B result: **120 passed** against isolated Redis/PostgreSQL, Next.js production build passed, and local container images built. `tests/test_live_sessions.py` covers limits/headroom, all three rollover triggers with partial fills, cancellation regression protection, durable intent/boundary/completion/initialization/activation interruptions, retained subscriptions, missing-history fail-closed behavior, per-batch SQL cleanup and Redis expiry, admission/closure races, and worker registry following. Actual service subprocesses with a generated feed complete three rollovers with unchanged worker PIDs and one retained quote-linked fill. These are exception/process tests, not a physical laptop suspension or measured capacity test. The actual capped local session also closed and activated a successor while preserving five existing fills; after-hours quotes were unavailable, so no fresh provider-fill claim is made for this update.

- Prove domain-state correctness under concurrent processing.
- Verify behavior when events are duplicated, delayed, or reordered.
- Confirm worker and dependency failures are recoverable.
- Measure throughput and latency using reproducible inputs.
- Verify the complete deployed user workflow.

## Unit Tests

Cover deterministic domain behavior:

- Event validation and normalization
- Ordering and deduplication rules
- State transitions
- Cache-key and expiration behavior
- Market and limit order activation
- Buy and sell fill eligibility
- Partial-fill and remaining-quantity calculations
- Weighted average fill price, fill rate, spread cost, time to fill, and latency impact
- Deterministic event and fill identifiers
- Error classification

## Integration Tests

Run against real test instances of the selected infrastructure:

- Producer to stream to consumer flow
- Concurrent worker coordination
- Cache updates and invalidation
- Database persistence and queries
- REST and WebSocket behavior
- Worker restart and retry handling
- Public/private mode enforcement
- PostgreSQL transaction and Redis acknowledgement ordering

## Deterministic Replay Tests

- [ ] Use immutable generated fixture versions.
- [ ] Produce identical final state across repeated runs.
- [ ] Produce identical results at different replay speeds.
- [ ] Verify behavior with duplicated events.
- [ ] Verify behavior with delayed or reordered events.
- [ ] Verify recovery from a mid-stream worker restart.
- [ ] Verify live and replay adapters produce the same normalized event contracts.

Required fixture scenarios:

- Stable spread with a complete market-order fill
- Visible size smaller than order quantity, producing partial fills
- Volatile price swing across the configured activation window
- Limit price never reached
- Limit price crossed after configured latency
- Price gap between submission and activation
- Duplicate and stale quote events
- Malformed event routed to dead letter

## Fault Matrix

| Fault | Deterministic outcome | Coverage |
| --- | --- | --- |
| Delayed activation | The first event at or after the latency window can activate the order. | `limit_reached_after_latency` |
| Duplicate event | The event is counted once and cannot create an additional fill. | `test_duplicate_quote_cannot_create_a_second_fill` |
| Stale event | Older event time/sequence cannot replace current market state. | `test_stale_quote_does_not_replace_newer_market_state` |
| Malformed or missing fields | The invalid source message is dead-lettered and valid replay messages complete. | `test_invalid_message_is_dead_lettered_without_stopping_the_replay` |
| Engine worker loss | A replacement consumer claims pending messages and reaches the pure-simulation result. | `test_replacement_engine_recovers_pending_messages_after_a_worker_dies` |
| Temporary database failure | The result remains pending until a replacement persistence worker completes it. | `test_persistence_recovers_after_a_temporary_database_failure` |
| Killed consumer plus database connection refusal | Replacement workers preserve one fill and recreated APIs read the completed run. | `test_killed_consumer_replacement_and_api_restart_preserve_one_fill` |
| Worker dependency outage | Safe correlated 503; capacity released; next healthy job succeeds. | `test_worker_dependency_failures_are_safe_correlated_and_recoverable` |
| API dependency outage | Safe correlated HTTP 503 or WebSocket 1013; liveness is unaffected. | `test_api_dependency_outage_returns_safe_error_and_recovers`, `test_websocket_database_failure_closes_with_retryable_code` |
| Completed persistence job retried | Return success without reprocessing or accessing Redis. | `test_completed_persistence_retry_returns_success_without_reprocessing` |
| Cross-machine timestamps | Throughput uses comparable Unix timestamps; duration samples remain available. | `test_metrics_timestamps_are_comparable_between_processes` |

Phase 6D verification: 70 tests passed with local Redis/PostgreSQL and the Next.js production build passed. Provider services were not disrupted. The operational runbook, manual health/metrics checks, and rollback boundaries are in `DEPLOYMENT.md`. Production rollback execution and the 24-hour idle usage observation are not certified by these local tests.

## Load Tests

Measure at increasing event rates:

- Accepted and processed events per second
- End-to-end p50, p95, and p99 latency
- Queue depth and consumer lag
- Cache hit rate
- Database write throughput
- CPU and memory use
- WebSocket delivery delay

Record the dataset, duration, hardware, configuration, and first observed bottleneck for every published benchmark.

Initial acceptance targets are goals, not resume claims:

- Process one million generated events with zero duplicate fills.
- Sustain twice the observed peak rate of the configured private-live symbol set for ten minutes without continuously increasing lag.
- Demonstrate improved replay completion time when adding workers up to the fixed partition count.
- Recover a killed engine worker and produce the same final result as a failure-free run.

## Failure Tests

- External feed disconnects and reconnects
- Worker termination during processing
- Redis interruption
- Database slowdown or temporary failure
- Poison or malformed event
- Consumer backlog
- WebSocket client disconnect and reconnect
- Attempt to enable the live adapter in public mode
- Attempt to submit an order against stale market state

## End-to-End Tests

- Open the deployed application.
- Complete the primary user workflow.
- Confirm live updates without refreshing.
- Confirm historical or replay results.
- Verify disconnected and error states.
- Confirm the UI does not expose credentials or internal errors.
- Trace every displayed fill to its triggering normalized event.
- Confirm public mode only exposes approved replay fixtures.

## Completion Criteria

- [ ] Critical domain behavior has unit coverage.
- [ ] The main pipeline has integration coverage.
- [ ] Replay results are deterministic.
- [ ] Failure tests demonstrate documented recovery behavior.
- [ ] Performance claims are reproducible.
- [ ] The deployed primary workflow passes end to end.
- [ ] Public/private data boundaries are enforced by automated tests.
- [ ] One million generated events complete with zero duplicate fills.

## Benchmark Record

Every benchmark result must record:

- Application revision identifier
- Dataset name and immutable version
- Event count and event-type distribution
- Worker and partition counts
- Replay speed
- Machine CPU and memory
- Redis and PostgreSQL configuration
- Test duration
- Throughput, latency percentiles, lag, recovery, and correctness results

Only results captured with this information may replace placeholders in `RESUME_BULLETS.md`.
