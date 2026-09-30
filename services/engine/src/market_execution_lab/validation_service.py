import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from market_execution_lab.benchmark_service import run_benchmarks
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
            quantity=10,
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
                bid_size=100,
                ask_price=Decimal("100.00"),
                ask_size=100,
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
    events_per_partition: int,
    workers: int,
    duration_seconds: float,
    target_events_per_second: float,
) -> SustainedPipelineValidation:
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")
    if target_events_per_second <= 0:
        raise ValueError("target_events_per_second must be positive")
    started_at = perf_counter()
    deadline = started_at + duration_seconds
    total_events = 0
    max_engine_lag = 0
    max_persistence_lag = 0
    while perf_counter() < deadline:
        result = run_benchmarks(
            redis_url,
            database_url,
            partition_count,
            events_per_partition,
            [workers],
        )[0]
        total_events += result.total_events
        max_engine_lag = max(max_engine_lag, result.final_engine_lag)
        max_persistence_lag = max(max_persistence_lag, result.final_persistence_lag)
    elapsed = perf_counter() - started_at
    throughput = total_events / elapsed if elapsed else 0
    return SustainedPipelineValidation(
        duration_seconds=elapsed,
        total_events=total_events,
        throughput_events_per_second=throughput,
        target_events_per_second=target_events_per_second,
        max_engine_lag=max_engine_lag,
        max_persistence_lag=max_persistence_lag,
        passed=throughput >= target_events_per_second and max_engine_lag == 0 and max_persistence_lag == 0,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--database-url", default="postgresql+psycopg://tickertrace:tickertrace@localhost:5432/tickertrace")
    parser.add_argument("--duplicate-events", type=int, default=1_000_000)
    parser.add_argument("--duplicate-every", type=int, default=100)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--events-per-partition", type=int, default=1_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--duration-seconds", type=float, default=600)
    parser.add_argument("--target-events-per-second", type=float, required=True)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    duplicate_fills = validate_duplicate_fills(arguments.duplicate_events, arguments.duplicate_every)
    sustained_pipeline = validate_sustained_pipeline(
        arguments.redis_url,
        arguments.database_url,
        arguments.partitions,
        arguments.events_per_partition,
        arguments.workers,
        arguments.duration_seconds,
        arguments.target_events_per_second,
    )
    payload = {
        "duplicate_fill_validation": asdict(duplicate_fills),
        "sustained_pipeline_validation": asdict(sustained_pipeline),
        "passed": duplicate_fills.passed and sustained_pipeline.passed,
    }
    serialized = json.dumps(payload, indent=2)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(f"{serialized}\n")
    print(serialized)


if __name__ == "__main__":
    main()
