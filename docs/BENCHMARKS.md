# Benchmark Methodology

## Scope

The Phase 5B harness measures generated replay runs through Redis Streams, the execution engine, and PostgreSQL persistence. It compares fixed symbol partitions with one or more concurrent benchmark workers.

The benchmark does not use the always-on job-worker streams. It directly dispatches each generated replay so a local development worker cannot consume the same work during measurement.

## Run

Stop the always-on engine and persistence workers, start Redis and PostgreSQL, apply migrations, then run:

```bash
market-execution-benchmark --partitions 8 --events-per-partition 1000 --workers 1 2 4 --output benchmarks/phase-5b.json
```

The JSON record contains the worker count, stable partition set, total events, runtime, throughput, observed p95 engine and persistence processing time, peak source-stream depth, and final engine/persistence lag.

## Interpretation

This measures concurrent replay-run processing with fixed partition ownership, not an exchange feed or full-depth execution benchmark. A zero final lag confirms the measured run drained; it does not establish a production capacity claim. Record the first worker count where throughput stops improving in the benchmark result and use that as the first observed bottleneck.

## Local Baseline — 2026-09-30

Configuration: 8 fixed partitions, 1,000 generated events per partition, local Redis and PostgreSQL containers.

| Workers | Throughput (events/s) | p95 engine (ms) | p95 persistence (ms) | Final lag |
| --- | ---: | ---: | ---: | --- |
| 1 | 1,143 | 161 | 620 | 0 |
| 2 | 1,882 | 171 | 807 | 0 |
| 4 | 2,152 | 310 | 1,360 | 0 |
| 8 | 2,010 | 547 | 3,015 | 0 |

The first observed bottleneck is local PostgreSQL persistence contention: throughput stops improving from four to eight workers while p95 persistence time more than doubles. This result applies only to the documented local configuration. The complete machine-readable record is [`benchmarks/phase-5b.json`](../benchmarks/phase-5b.json).

## Phase 5C Validation

The former drain-then-repeat sustained harness has been replaced by independent paced production with continuous consumers. Older finite replay evidence remains valid only for its documented workload. The following CLI now runs the new continuous pipeline; the example target is illustrative and must be replaced with a measured value.

The certification command combines an in-memory duplicate-fill validation with a sustained Redis Streams, engine, and PostgreSQL pipeline run. It requires an explicit target rate because the public fixture mode has no observed private-live peak.

Measure that peak from the private Alpaca IEX feed before starting the certification:

```bash
APP_MODE=private_live market-execution-measure-live-peak --duration-seconds 60
```

Stop the always-on engine and persistence workers, then run this with twice the measured private-live peak rate:

```bash
market-execution-validate --target-events-per-second 4000 --output benchmarks/phase-5c.json
```

The default command validates 1,000,000 generated events with injected duplicate identifiers and runs the pipeline for 600 seconds. The resulting JSON is the only source for the final one-million-event and sustained-throughput claims. Do not add those claims to the resume or README until this command passes using a documented target rate.

The million-event check is **in-memory engine dedupe only** and does not send those million events through Redis/PostgreSQL. The continuous stage independently counts offered and durable events and checks completed execution results. Never combine these into a one-million-event distributed-pipeline claim.

To run only the one-million-event duplicate-fill check while a live-rate measurement is unavailable:

```bash
market-execution-validate --skip-sustained-pipeline --output benchmarks/phase-5c-engine.json
```

## Pre-Phase 7 Continuous Harness

- Independent rate-controlled producer; separate continuous engine and persistence consumers. Worker count is threads per role in one local process, capped by partition count, not distributed-machine scaling.
- Source lag plus pending and result lag plus pending are sampled every approximately 100 ms while production is active. Maxima include active processing; final zero backlog alone cannot pass certification.
- Backlog growth is least-squares slope over the last two-thirds of production samples. The explicit tolerance is at most `max(1 message/sec, 1% target rate)`; small positive jitter is not treated as unlimited capacity.
- Offered rate must reach at least 95% of the requested rate. Final backlog must drain within 30 seconds; no consumer exceptions, duplicate persisted observations, missing durable events, or incomplete sample orders may occur.
- p50/p95/p99 latency is per-event timestamp just before enqueue to the successful PostgreSQL market-event batch commit. It includes queue, normalization/publication overhead, and database time; it does not include final fill-result persistence or browser delivery. Single-machine wall-clock sampling avoids a cross-host clock claim.
- Producer runs for the requested duration; the reported total includes drain/shutdown time. Certification exits nonzero on failed acceptance.
- JSON records platform, Python/CPU count, supplied revision/environment note, actual Redis/PostgreSQL versions, dataset version, configured duration/partitions/workers, observed offered rate, samples, and latency.
- Use dedicated local Redis/PostgreSQL for high-volume certification. This command can create substantial retained benchmark data; do not run it against hosted free-tier dependencies. Source/result benchmark keys receive a 15-minute TTL afterward; durable benchmark rows are not silently deleted.

Example smoke check, not market certification:

```bash
market-execution-validate --duplicate-events 10000 --partitions 4 --workers 2 --duration-seconds 15 --target-events-per-second 200 --revision <operator-recorded-revision> --environment-note '<hardware and container limits>' --output benchmarks/local-smoke.json
```

Run the 600-second check only after a nonzero active-window `market-execution-measure-live-peak` result, supplying twice that measured peak and a documented environment. A zero after-hours peak is not certification.

`benchmarks/pre-phase-7-local.json` is exploratory local evidence: one-million-event in-memory dedupe plus a 15-second continuous generated pipeline. It does not certify real-feed execution, ten-minute capacity, multi-machine scaling, browser concurrency, or idle provider quotas.

The refreshed local run persisted all 3,000 generated events: target 200/sec, offered 198.83/sec, final backlog 0, duplicate events 0, and durable-write p50/p95/p99 of 1.33/3.82/33.88 ms. It used four partitions and two local threads per consumer role, Redis 7.4.11 and PostgreSQL 16.15 in a shared eight-CPU Docker VM. The JSON contains environment metadata and active-production backlog samples; these figures are not deployed-system capacity claims.
