import argparse
import asyncio
import json
import os
from dataclasses import asdict, dataclass
from time import perf_counter
from uuid import uuid4

from market_execution_lab.alpaca import AlpacaSettings, stream_alpaca
from market_execution_lab.pipeline import partition_for_symbol


@dataclass(frozen=True)
class LivePeakMeasurement:
    duration_seconds: float
    total_events: int
    peak_events_per_second: int
    symbols: list[str]


async def measure_live_peak(settings: AlpacaSettings, duration_seconds: float) -> LivePeakMeasurement:
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")
    started_at = perf_counter()
    buckets: dict[int, int] = {}

    async def observe(_event) -> None:
        second = int(perf_counter() - started_at)
        buckets[second] = buckets.get(second, 0) + 1

    try:
        await asyncio.wait_for(
            stream_alpaca(settings, uuid4(), partition_for_symbol, observe),
            timeout=duration_seconds,
        )
    except TimeoutError:
        pass
    elapsed = perf_counter() - started_at
    return LivePeakMeasurement(
        duration_seconds=elapsed,
        total_events=sum(buckets.values()),
        peak_events_per_second=max(buckets.values(), default=0),
        symbols=list(settings.symbols),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=float, default=60)
    arguments = parser.parse_args()
    result = asyncio.run(measure_live_peak(AlpacaSettings.from_environment(), arguments.duration_seconds))
    print(json.dumps(asdict(result), indent=2))


if __name__ == "__main__":
    main()
