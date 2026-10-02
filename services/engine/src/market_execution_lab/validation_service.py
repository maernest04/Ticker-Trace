import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from time import sleep
import os
import platform
import sys
import sqlalchemy as sa
from redis import Redis

from market_execution_lab.benchmark_service import _symbols_for_partitions, _percentile
from market_execution_lab.live_pipeline import initialize_session, run_live_worker
from market_execution_lab.pipeline import partition_for_symbol, result_stream_name
from market_execution_lab.storage import DatabaseStore, sqlalchemy_url
from market_execution_lab.streaming import partition_stream_name
from market_execution_lab.engine import ExecutionEngine
from market_execution_lab.models import OrderCommand, OrderSide, OrderType, QuoteEvent


@dataclass(frozen=True)
class DuplicateFillValidation:
    received_events: int
    duplicate_events: int
    fills: int
    duplicate_fills: int
    passed: bool


@dataclass(frozen=True)
class SustainedPipelineValidation:
    duration_seconds: float
    total_events: int
    throughput_events_per_second: float
    target_events_per_second: float
    max_engine_lag: int
    max_persistence_lag: int
    offered_events_per_second: float
    final_backlog: int
    backlog_growth_per_second: float
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    persisted_events: int
    duplicate_events: int
    samples: list[dict[str, float | int]]
    errors: list[str]
    infrastructure: dict[str, str]
    passed: bool


def validate_duplicate_fills(event_count: int, duplicate_every: int) -> DuplicateFillValidation:
    if event_count < 1:
        raise ValueError("event_count must be positive")
    if duplicate_every < 2:
        raise ValueError("duplicate_every must be at least 2")
    run_id = uuid4()
    submitted_at = datetime(2026, 9, 30, 13, 30, tzinfo=UTC)
    engine = ExecutionEngine(
        OrderCommand(
            order_id=uuid4(),
            run_id=run_id,
            symbol="VALID",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=1_000,
            submitted_at=submitted_at,
        )
    )
    for sequence in range(1, event_count + 1):
        source_sequence = sequence - 1 if sequence % duplicate_every == 0 else sequence
        engine.process(
            QuoteEvent(
                event_id=f"validation-{source_sequence}",
                run_id=run_id,
                symbol="VALID",
                event_time=submitted_at + timedelta(milliseconds=sequence),
                ingested_at=submitted_at + timedelta(milliseconds=sequence + 1),
                sequence=sequence,
                partition=0,
                bid_price=Decimal("99.99"),
                bid_size=10,
                ask_price=Decimal("100.00"),
                ask_size=10,
            )
        )
    result = engine.finalize()
    duplicate_fills = len(result.fills) - len({fill.fill_id for fill in result.fills})
    return DuplicateFillValidation(
        received_events=event_count,
        duplicate_events=result.duplicate_events,
        fills=len(result.fills),
        duplicate_fills=duplicate_fills,
        passed=duplicate_fills == 0 and result.duplicate_events > 0,
    )


def validate_sustained_pipeline(
    redis_url: str,
    database_url: str,
    partition_count: int,
    workers: int,
    duration_seconds: float,
    target_events_per_second: float,
) -> SustainedPipelineValidation:
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")
    if target_events_per_second <= 0:
        raise ValueError("target_events_per_second must be positive")
    if not 1 <= partition_count <= 16 or workers < 1:
        raise ValueError("invalid partition, worker, or batch configuration")
    redis = Redis.from_url(redis_url, decode_responses=True, socket_timeout=10)
    database = sa.create_engine(sqlalchemy_url(database_url))
    store = DatabaseStore(database)
    with database.connect() as connection:
        postgres_version = connection.scalar(sa.text("SELECT version()"))
    infrastructure = {"postgresql": postgres_version, "redis": redis.info("server")["redis_version"]}
    symbols = _symbols_for_partitions(partition_count)
    partitions = [partition_for_symbol(symbol) for symbol in symbols]
    run_id = uuid4()
    initialize_session(redis, run_id, symbols, register=False)
    commands = []
    for symbol, partition in zip(symbols, partitions):
        order = OrderCommand(order_id=uuid4(), run_id=run_id, symbol=symbol, side=OrderSide.BUY,
                             order_type=OrderType.MARKET, quantity=10, submitted_at=datetime.now(UTC))
        commands.append(order)
        redis.xadd(partition_stream_name(str(run_id), partition), {"message_type": "order.command.v1", "payload": order.model_dump_json()})
    stop, lock = Event(), Lock()
    latencies, seen = [], set()
    duplicates = 0

    def observe(records):
        nonlocal duplicates
        with lock:
            for event_id, latency in records:
                duplicates += int(event_id in seen)
                seen.add(event_id)
                latencies.append(latency)

    def backlog():
        engine_lag = persistence_lag = result_lag = 0
        for partition in partitions:
            for group in redis.xinfo_groups(partition_stream_name(str(run_id), partition)):
                lag = int(group.get("lag") or 0) + int(group["pending"])
                if group["name"] == "engine":
                    engine_lag += lag
                else:
                    persistence_lag += lag
            for group in redis.xinfo_groups(result_stream_name(str(run_id), partition)):
                result_lag += int(group.get("lag") or 0) + int(group["pending"])
        return engine_lag, persistence_lag, result_lag

    total_events = 0
    errors, samples = [], []
    assignments = [partitions[index::min(workers, partition_count)] for index in range(min(workers, partition_count))]
    with ThreadPoolExecutor(max_workers=len(assignments) * 2 + 1) as pool:
        futures = [pool.submit(run_live_worker, redis, store if role == "persistence" else None, run_id, assignment, role, stop, observe if role == "persistence" else None)
                   for role in ("engine", "persistence") for assignment in assignments]
        started_at = perf_counter()

        def produce():
            nonlocal total_events
            for sequence in range(max(1, int(target_events_per_second * duration_seconds))):
                if stop.is_set() or perf_counter() - started_at >= duration_seconds:
                    return
                wait = started_at + sequence / target_events_per_second - perf_counter()
                if wait > 0:
                    stop.wait(wait)
                if stop.is_set():
                    return
                index = sequence % len(symbols)
                now = datetime.now(UTC)
                event = QuoteEvent(event_id=f"{run_id}:{sequence}", run_id=run_id, symbol=symbols[index],
                                   event_time=now, ingested_at=now, sequence=sequence, partition=partitions[index],
                                   bid_price=Decimal("99.99"), bid_size=100, ask_price=Decimal("100"), ask_size=100)
                redis.xadd(partition_stream_name(str(run_id), partitions[index]), {"message_type": "market.event.v1", "payload": event.model_dump_json()})
                total_events += 1

        producer = pool.submit(produce)
        try:
            while perf_counter() - started_at < duration_seconds or not producer.done():
                engine_lag, persistence_lag, result_lag = backlog()
                samples.append({"seconds": perf_counter() - started_at, "engine": engine_lag, "persistence": persistence_lag, "results": result_lag})
                if any(future.done() for future in futures):
                    raise RuntimeError("continuous consumer exited before completion")
                sleep(0.1)
            producer.result()
            offered_duration = perf_counter() - started_at
            drain_deadline = perf_counter() + 30
            while sum(backlog()) and perf_counter() < drain_deadline:
                if any(future.done() for future in futures):
                    raise RuntimeError("continuous consumer failed while draining")
                sleep(0.1)
        except Exception as error:
            errors.append(type(error).__name__)
            offered_duration = perf_counter() - started_at
        finally:
            stop.set()
            for future in [producer, *futures]:
                try:
                    future.result(timeout=15)
                except Exception as error:
                    errors.append(type(error).__name__)
    elapsed = perf_counter() - started_at
    final_backlog = sum(backlog())
    persisted = store.counts_for_run(run_id)["events"]
    order_results = [store.order_for_id(order.order_id) for order in commands]
    if any(not order or order["final_state"] != "filled" for order in order_results):
        errors.append("IncompleteExecution")
    growth = backlog_growth(samples)
    offered_rate = total_events / offered_duration if offered_duration else 0
    passed = certification_passed(target_events_per_second, offered_rate, final_backlog, growth, total_events, persisted, duplicates, errors)
    for partition in range(16):
        redis.expire(partition_stream_name(str(run_id), partition), 900)
        redis.expire(result_stream_name(str(run_id), partition), 900)
    redis.close()
    return SustainedPipelineValidation(
        duration_seconds=elapsed,
        total_events=total_events,
        throughput_events_per_second=persisted / elapsed if elapsed else 0,
        target_events_per_second=target_events_per_second,
        max_engine_lag=max((int(sample["engine"]) for sample in samples), default=0),
        max_persistence_lag=max((int(sample["persistence"]) for sample in samples), default=0),
        offered_events_per_second=offered_rate, final_backlog=final_backlog, backlog_growth_per_second=growth,
        latency_p50_ms=_percentile(latencies, 0.5) if latencies else None,
        latency_p95_ms=_percentile(latencies, 0.95) if latencies else None,
        latency_p99_ms=_percentile(latencies, 0.99) if latencies else None,
        persisted_events=persisted, duplicate_events=duplicates, samples=samples, errors=errors, infrastructure=infrastructure, passed=passed,
    )


def backlog_growth(samples: list[dict]) -> float:
    steady = samples[len(samples) // 3:]
    if len(steady) < 2:
        return float("inf")
    times = [float(sample["seconds"]) for sample in steady]
    values = [int(sample["engine"]) + int(sample["persistence"]) + int(sample["results"]) for sample in steady]
    mean_time, mean_value = sum(times) / len(times), sum(values) / len(values)
    variance = sum((value - mean_time) ** 2 for value in times)
    return sum((x - mean_time) * (y - mean_value) for x, y in zip(times, values)) / variance if variance else float("inf")


def certification_passed(target, offered, final_backlog, growth, received, persisted, duplicates, errors) -> bool:
    return offered >= target * 0.95 and final_backlog == 0 and growth <= max(1, target * 0.01) and received == persisted and duplicates == 0 and not errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--database-url", default="postgresql+psycopg://tickertrace:tickertrace@localhost:5432/tickertrace")
    parser.add_argument("--duplicate-events", type=int, default=1_000_000)
    parser.add_argument("--duplicate-every", type=int, default=100)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--duration-seconds", type=float, default=600)
    parser.add_argument("--target-events-per-second", type=float)
    parser.add_argument("--skip-sustained-pipeline", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--revision", default="unrecorded")
    parser.add_argument("--environment-note", default="Resource limits not recorded; exploratory local evidence only")
    arguments = parser.parse_args()
    duplicate_fills = validate_duplicate_fills(arguments.duplicate_events, arguments.duplicate_every)
    if not arguments.skip_sustained_pipeline and arguments.target_events_per_second is None:
        parser.error("--target-events-per-second is required unless --skip-sustained-pipeline is set")
    sustained_pipeline = None if arguments.skip_sustained_pipeline else validate_sustained_pipeline(
        arguments.redis_url,
        arguments.database_url,
        arguments.partitions,
        arguments.workers,
        arguments.duration_seconds,
        arguments.target_events_per_second,
    )
    payload = {
        "methodology": {"platform": platform.platform(), "processor": platform.processor(), "cpu_count": os.cpu_count(),
                        "python": sys.version.split()[0], "revision": arguments.revision, "dataset": "continuous_quotes_v1",
                        "environment_note": arguments.environment_note,
                        "workers_per_role": min(arguments.workers, arguments.partitions), "partitions": arguments.partitions,
                        "concurrency": "local threads, separate continuous engine and persistence consumers",
                        "requested_duration_seconds": arguments.duration_seconds,
                        "latency_basis": "event enqueue timestamp to committed PostgreSQL batch; includes queue and database time",
                        "acceptance": "offered rate >=95% target; steady backlog slope <=max(1 message/s,1% target); no final backlog, duplicates, losses, or worker errors",
                        "scope": "engine-only dedupe and local continuous pipeline are separate checks; not production-machine scaling"},
        "duplicate_fill_validation": asdict(duplicate_fills),
        "sustained_pipeline_validation": asdict(sustained_pipeline) if sustained_pipeline else None,
        "passed": duplicate_fills.passed and (sustained_pipeline.passed if sustained_pipeline else True),
    }
    serialized = json.dumps(payload, indent=2)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(f"{serialized}\n")
    print(serialized)
    if not payload["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
