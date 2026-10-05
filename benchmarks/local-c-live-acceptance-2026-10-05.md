# October 5, 2026 local live acceptance

Only local Docker resources and simulated orders were used. No real brokerage orders, cloud changes, Git commands, application-code changes, or public raw-market-data screenshots were performed.

## Actual-feed checks

- Started Docker Desktop and restored the existing `tickertrace-local` stack using existing images and additive migrations. Docker startup attempted restart-policy workers before Redis was available, producing connection errors; explicit ordered stack startup restored services. This is not evidence of reliable unattended cold boot.
- Session `29c979dd-5eff-40bc-8eb8-fca3f37aea4f` reported connected/fresh with actual Alpaca IEX AAPL/MSFT quotes.
- Submitted a five-share AAPL market order, restarted engine, submitted a five-share MSFT market order with 100 ms artificial latency, restarted persistence separately, then submitted a five-share marketable AAPL limit order with 100 ms artificial latency.
- Both workers acquired ownership and became ready against the unchanged session without manual lease deletion or a second restart.
- Submitted another five-share AAPL market order through the Next.js dashboard. The UI showed a complete fill and its triggering quote explanation.
- PostgreSQL joins verified all four orders: exactly one five-share fill per order; fill price equals the stored triggering ask; triggering event time is at or after submission plus configured artificial latency. The first fill ID remained unchanged after both restarts.
- A second deliberate ingestion stop changed the browser to `Stale — orders disabled` with a disabled submission button. After ingestion resumed, the same page returned to Fresh and re-enabled submission.
- A separate API stop changed the browser to `Private session disconnected`; starting the same API container restored `Private session connected`, fresh quotes, and enabled submission without reloading the page.
- Running container source hashes match the documented final Local C recovery sources. No image rebuild was needed.

| Order ID | Check |
| --- | --- |
| `dc977fbd-6ec7-4c1d-a7d1-ebae84d59e37` | Initial market fill and unchanged original fill after recovery |
| `d04fd259-b1d7-4bde-8706-097e24d0c22a` | Market fill after engine restart, 100 ms artificial latency |
| `956479ea-ae89-4771-8745-a2b7e06c1254` | Marketable limit fill after persistence restart, 100 ms artificial latency |
| `c3003bb5-c044-4c82-b87c-d49f7dcb3150` | Browser-submitted market fill and displayed explanation |

## Live-rate measurement

Normal ingestion was stopped during measurement to avoid competing Alpaca connections, then restored. While stopped, the API rejected a simulated order with HTTP 409 and `live quote is stale or unavailable`.

The existing live-peak CLI recorded 1,483 normalized AAPL/MSFT quote/trade events over 60.238643986 seconds. Peak one-second bucket: 141 events/sec. This is a short subscription-specific IEX sample, not an all-day or all-exchange maximum. The generated-load acceptance target is twice this measurement: 282 events/sec for 600 seconds.

## Benchmark methodology

Dedicated local containers `tickertrace-pre7-redis` and `tickertrace-pre7-postgres`, loopback ports 56379/55432, isolate generated load from the live stack. Four partitions and two threads per consumer role run in one Python benchmark process. This is not multi-machine scaling. Latency is enqueue-to-PostgreSQL-market-event commit, not order-fill or browser latency.

Hardware: Apple M3, 16 GiB host memory; Docker 29.8.1 VM with eight CPUs and 8,215,117,824 bytes RAM. The live stack shares the host. A regression run also overlapped part of the benchmark and used another Redis database but the same dedicated PostgreSQL container/database; results therefore include that contention.

The benchmark inhibits idle sleep only during execution. Output: `local-c-live-rate-2026-10-05.json`. Exit code zero; acceptance passed.

| Measurement | Result |
| --- | --- |
| Duration | 600.1554 seconds |
| Target / achieved offered rate | 282 / 281.9718 events/sec |
| Offered / persisted events | 169,200 / 169,200 |
| Enqueue-to-commit p50 / p95 / p99 | 2.21 / 37.57 / 123.26 ms |
| Maximum engine / persistence backlog | 28 / 33 messages |
| Steady backlog growth | -0.00772 messages/sec |
| Final backlog / duplicate durable observations / worker errors | 0 / 0 / 0 |
| Backlog samples | 5,511 |

The separate engine-only million-event dedupe check observed 10,000 duplicate IDs and zero duplicate fills; it is not a million-event durable pipeline test. The higher p95 than the earlier 200/sec endurance run is retained rather than combining the best latency from one run with the throughput from another.

## Regression status and remaining checks

The concurrent regression run returned 124 passed, one failed, and one existing Starlette/httpx deprecation warning. Failure: `test_ingestion_restart_and_three_rollovers_with_real_service_processes` expected 33 ownership keys after rollover but observed 17. The test waits for fresh engine-cached quotes before pausing workers, which does not prove persistence has acquired all successor leases. This suggests a readiness timing race. After the benchmark finished, the failing test passed individually (36.66 seconds), then the full suite passed: 125 tests in 77.04 seconds with the same existing warning. These passing reruns do not erase the timing-sensitive failure or establish that it is fixed; no test/application code was changed.

Physical laptop sleep/wake and network-recovery acceptance remain unverified and require user participation. No multi-day uptime, public display permission, production scaling, or reliable cold-start restart-policy claim follows from these checks.
