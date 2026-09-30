import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from math import ceil
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import sqlalchemy as sa
from redis import Redis

from market_execution_lab.fixtures import ScenarioFixture
from market_execution_lab.models import OrderCommand, OrderSide, OrderType, QuoteEvent
from market_execution_lab.observability import pipeline_metrics
from market_execution_lab.pipeline import PARTITION_COUNT, partition_for_symbol, publish_replay, run_engine, run_persistence
from market_execution_lab.storage import DatabaseStore
from market_execution_lab.streaming import partition_stream_name


@dataclass(frozen=True)
class BenchmarkResult:
    workers: int
    partitions: list[int]
    total_events: int
    runtime_seconds: float
    throughput_events_per_second: float
    p95_engine_processing_ms: float
    p95_persistence_processing_ms: float
    peak_source_depth: int
    final_engine_lag: int
    final_persistence_lag: int


def run_benchmarks(
    redis_url: str,
    database_url: str,
    partition_count: int,
    events_per_partition: int,
    worker_counts: list[int],
) -> list[BenchmarkResult]:
    if not 1 <= partition_count <= PARTITION_COUNT:
        raise ValueError(f"partition_count must be between 1 and {PARTITION_COUNT}")
    if events_per_partition < 1:
        raise ValueError("events_per_partition must be positive")
    if not worker_counts or any(worker_count < 1 for worker_count in worker_counts):
        raise ValueError("worker counts must be positive")

    redis = Redis.from_url(redis_url, decode_responses=True)
    store = DatabaseStore(sa.create_engine(database_url))
    symbols = _symbols_for_partitions(partition_count)
    results = []
    for workers in worker_counts:
        started_at = perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as executor:
            records = list(executor.map(lambda item: _run_partition(redis, store, item, events_per_partition), symbols))
        runtime_seconds = perf_counter() - started_at
        total_events = partition_count * events_per_partition
        results.append(
            BenchmarkResult(
                workers=workers,
                partitions=[partition_for_symbol(symbol) for symbol in symbols],
                total_events=total_events,
                runtime_seconds=runtime_seconds,
                throughput_events_per_second=total_events / runtime_seconds if runtime_seconds else 0,
                p95_engine_processing_ms=_percentile([record[0] for record in records], 0.95),
                p95_persistence_processing_ms=_percentile([record[1] for record in records], 0.95),
                peak_source_depth=max(record[2] for record in records),
                final_engine_lag=max(record[3] for record in records),
                final_persistence_lag=max(record[4] for record in records),
            )
        )
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--database-url", default="postgresql+psycopg://tickertrace:tickertrace@localhost:5432/tickertrace")
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--events-per-partition", type=int, default=1_000)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    results = run_benchmarks(
        arguments.redis_url,
        arguments.database_url,
        arguments.partitions,
        arguments.events_per_partition,
        arguments.workers,
    )
    payload = {
        "methodology": "Each worker count processes the same fixed symbol partitions through Redis Streams, the execution engine, and PostgreSQL persistence. Benchmark dispatch bypasses always-on job workers to prevent duplicate consumers during measurement.",
        "events_per_partition": arguments.events_per_partition,
        "results": [asdict(result) for result in results],
    }
    serialized = json.dumps(payload, indent=2)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(f"{serialized}\n")
    print(serialized)


def _symbols_for_partitions(partition_count: int) -> list[str]:
    symbols_by_partition: dict[int, str] = {}
    candidate = 0
    while len(symbols_by_partition) < partition_count:
        symbol = f"BM{candidate:04d}"
        partition = partition_for_symbol(symbol)
        symbols_by_partition.setdefault(partition, symbol)
        candidate += 1
    return [symbols_by_partition[partition] for partition in sorted(symbols_by_partition)[:partition_count]]


def _run_partition(redis: Redis, store: DatabaseStore, symbol: str, event_count: int) -> tuple[float, float, int, int, int]:
    run_id = uuid4()
    partition = partition_for_symbol(symbol)
    submitted_at = datetime(2026, 9, 30, 13, 30, tzinfo=UTC)
    scenario = ScenarioFixture(
        name="benchmark",
        order=OrderCommand(
            order_id=uuid4(),
            run_id=run_id,
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10,
            submitted_at=submitted_at,
        ),
        events=tuple(
            QuoteEvent(
                event_id=f"{run_id}-{sequence}",
                run_id=run_id,
                symbol=symbol,
                event_time=submitted_at + timedelta(milliseconds=sequence),
                ingested_at=submitted_at + timedelta(milliseconds=sequence + 1),
                sequence=sequence,
                partition=partition,
                bid_price=Decimal("99.99"),
                bid_size=100,
                ask_price=Decimal("100.00"),
                ask_size=100,
            )
            for sequence in range(1, event_count + 1)
        ),
    )
    publish_replay(redis, scenario, dispatch=False)
    stream = partition_stream_name(str(run_id), partition)
    peak_source_depth = redis.xlen(stream)
    run_engine(redis, run_id, partition, f"benchmark-engine-{run_id}")
    run_persistence(redis, store, run_id, partition, f"benchmark-persistence-{run_id}")
    metrics = pipeline_metrics(redis, stream, str(run_id), partition)
    processing_ms = float(redis.hget(f"pipeline.metrics:{run_id}:{partition}", "engine_processing_ms") or 0)
    persistence_ms = float(redis.hget(f"pipeline.metrics:{run_id}:{partition}", "persistence_processing_ms") or 0)
    return processing_ms, persistence_ms, peak_source_depth, metrics.engine_lag, metrics.persistence_lag


def _percentile(values: list[float], percentile: float) -> float:
    return sorted(values)[max(ceil(len(values) * percentile) - 1, 0)]


if __name__ == "__main__":
    main()
