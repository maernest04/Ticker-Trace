import argparse
import json
import os
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import sqlalchemy as sa
from redis import Redis

from market_execution_lab.fixtures import ScenarioFixture, generated_scenarios
from market_execution_lab.storage import DatabaseStore
from market_execution_lab.streaming import RedisReplayRunner


DEFAULT_DATABASE_URL = "postgresql+psycopg://tickertrace:tickertrace@localhost:5432/tickertrace"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"


def main() -> None:
    scenarios = {scenario.name: scenario for scenario in generated_scenarios()}
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=sorted(scenarios))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", DEFAULT_REDIS_URL))
    arguments = parser.parse_args()

    scenario = _new_run(scenarios[arguments.scenario])
    database = sa.create_engine(arguments.database_url)
    redis = Redis.from_url(arguments.redis_url, decode_responses=True)
    result = RedisReplayRunner(redis, DatabaseStore(database)).run(scenario.name, scenario.order, scenario.events)

    print(
        json.dumps(
            {
                "run_id": str(scenario.order.run_id),
                "scenario": scenario.name,
                "state": result.state.value,
                "remaining_quantity": result.remaining_quantity,
                "fill_count": len(result.fills),
                "average_fill_price": str(result.metrics.average_fill_price)
                if result.metrics.average_fill_price is not None
                else None,
                "fill_rate": str(result.metrics.fill_rate),
                "spread_cost": str(result.metrics.spread_cost) if result.metrics.spread_cost is not None else None,
                "time_to_first_fill_ms": _milliseconds(result.metrics.time_to_first_fill),
                "time_to_completion_ms": _milliseconds(result.metrics.time_to_completion),
                "latency_impact": str(result.metrics.latency_impact)
                if result.metrics.latency_impact is not None
                else None,
                "processed_events": result.processed_events,
                "duplicate_events": result.duplicate_events,
                "stale_events": result.stale_events,
            },
            indent=2,
        )
    )


def _new_run(scenario: ScenarioFixture) -> ScenarioFixture:
    run_id = uuid4()
    order = scenario.order.model_copy(update={"run_id": run_id, "order_id": uuid4()})
    events = tuple(event.model_copy(update={"run_id": run_id}) for event in scenario.events)
    return replace(scenario, order=order, events=events)


def _milliseconds(value: timedelta | None) -> int | None:
    if value is None:
        return None
    return int(value.total_seconds() * 1000)


if __name__ == "__main__":
    main()
