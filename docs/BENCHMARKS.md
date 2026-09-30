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

To run only the one-million-event duplicate-fill check while a live-rate measurement is unavailable:

```bash
market-execution-validate --skip-sustained-pipeline --output benchmarks/phase-5c-engine.json
```
