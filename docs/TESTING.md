# Testing Strategy

## Pre-Phase 7 implementation coverage

The older unchecked strategy lists below describe desired scope, not proof of completion. New tests cover non-finalizing live snapshots, zero-valued metric round trips, deterministic longer datasets, size/latency differences, CLI pacing, public live-endpoint rejection, symbol bounds, fatal authentication, transient reconnect/malformed-message handling, subscription-task cleanup, quote freshness, ongoing live order execution, partition-owner rejection, pending reconstruction, and continuous backlog/durable-latency measurement.

The private integration path uses normalized **generated Alpaca-format messages**, not an actual market connection. Local Redis/PostgreSQL demonstrate submitted → subsequent quote → filled, private WebSocket output, one durable fill after replacement, and public denial of that private data. Actual provider-fill acceptance remains separate.

Run with dedicated local dependencies, not the hosted free tiers:

```bash
RUN_STREAMING_INTEGRATION=1 APP_MODE=public_replay REDIS_URL=redis://127.0.0.1:56379/0 DATABASE_URL=<dedicated-local-postgres-url> .venv/bin/python -m pytest -q
```

The continuous acceptance test offers generated input independently of consumers and samples backlog during production. It is not the former drain-after-each-batch test. External market, deployed concurrency, rollback, and 24-hour provider-counter checks remain operator acceptance tasks in `PRE_PHASE_7.md` and `DEPLOYMENT.md`.

## Testing Goals

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
