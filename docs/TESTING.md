# Testing Strategy

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
- Limit price never reached
- Limit price crossed after configured latency
- Price gap between submission and activation
- Duplicate and stale quote events
- Malformed event routed to dead letter

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
