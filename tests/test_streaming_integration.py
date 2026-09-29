import os
import json
import subprocess
import sys
from dataclasses import replace
from uuid import uuid4

import pytest
import sqlalchemy as sa
from redis import Redis

from market_execution_lab.engine import simulate
from market_execution_lab.fixtures import ScenarioFixture, generated_scenarios
from market_execution_lab.pipeline import ENGINE_GROUP, PERSISTENCE_GROUP, partition_for_symbol, result_stream_name
from market_execution_lab.storage import DatabaseStore
from market_execution_lab.streaming import RedisReplayRunner, market_state_key, order_state_key, partition_stream_name


pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.getenv("RUN_STREAMING_INTEGRATION") != "1",
    reason="set RUN_STREAMING_INTEGRATION=1 with local Redis and PostgreSQL",
)
def test_streamed_replay_matches_pure_execution_three_times() -> None:
    template = next(scenario for scenario in generated_scenarios() if scenario.name == "price_gap_before_activation")
    database = sa.create_engine(os.environ["DATABASE_URL"])
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    store = DatabaseStore(database)
    runner = RedisReplayRunner(redis, store)

    for _ in range(3):
        scenario = _new_run(template)
        expected = simulate(scenario.order, scenario.events)
        actual = runner.run(scenario.name, scenario.order, scenario.events)

        assert actual == expected
        assert store.counts_for_run(scenario.order.run_id) == {
            "events": len(scenario.events),
            "orders": 1,
            "fills": len(actual.fills),
            "transitions": len(actual.transitions),
        }
        assert redis.hgetall(market_state_key(str(scenario.order.run_id), scenario.order.symbol))["ask_price"] == "181.00"
        assert redis.hgetall(order_state_key(str(scenario.order.run_id), str(scenario.order.order_id))) == {
            "state": "filled",
            "remaining_quantity": "0",
            "fill_count": "1",
        }


@pytest.mark.skipif(
    os.getenv("RUN_STREAMING_INTEGRATION") != "1",
    reason="set RUN_STREAMING_INTEGRATION=1 with local Redis and PostgreSQL",
)
def test_replay_engine_and_persistence_run_as_separate_processes() -> None:
    template = next(scenario for scenario in generated_scenarios() if scenario.name == "price_gap_before_activation")
    run_id = uuid4()
    order_id = uuid4()
    partition = partition_for_symbol(template.order.symbol)
    scenario = _new_run(template, run_id, order_id, partition)
    environment = os.environ | {
        "DATABASE_URL": os.environ["DATABASE_URL"],
        "REDIS_URL": os.environ["REDIS_URL"],
    }

    replay = _run_service(
        environment,
        "market_execution_lab.replay_service",
        "--scenario",
        template.name,
        "--run-id",
        str(run_id),
        "--order-id",
        str(order_id),
    )
    assert json.loads(replay.stdout)["partition"] == partition
    engine = _run_service(
        environment,
        "market_execution_lab.engine_service",
        "--run-id",
        str(run_id),
        "--partition",
        str(partition),
    )
    _run_service(
        environment,
        "market_execution_lab.persistence_service",
        "--run-id",
        str(run_id),
        "--partition",
        str(partition),
    )

    expected = simulate(scenario.order, scenario.events)
    database = sa.create_engine(environment["DATABASE_URL"])
    redis = Redis.from_url(environment["REDIS_URL"], decode_responses=True)
    store = DatabaseStore(database)

    assert json.loads(engine.stdout) == {"state": expected.state.value, "fills": len(expected.fills)}
    assert store.counts_for_run(run_id) == {
        "events": len(scenario.events),
        "orders": 1,
        "fills": len(expected.fills),
        "transitions": len(expected.transitions),
    }
    assert {group["name"] for group in redis.xinfo_groups(partition_stream_name(str(run_id), partition))} == {
        ENGINE_GROUP,
        PERSISTENCE_GROUP,
    }
    assert {group["name"] for group in redis.xinfo_groups(result_stream_name(str(run_id), partition))} == {
        PERSISTENCE_GROUP
    }


def _run_service(environment: dict[str, str], module: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", module, *arguments],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )


def _new_run(
    scenario: ScenarioFixture,
    run_id=None,
    order_id=None,
    partition: int | None = None,
) -> ScenarioFixture:
    run_id = run_id or uuid4()
    return replace(
        scenario,
        order=scenario.order.model_copy(update={"run_id": run_id, "order_id": order_id or uuid4()}),
        events=tuple(
            event.model_copy(update={"run_id": run_id, **({"partition": partition} if partition is not None else {})})
            for event in scenario.events
        ),
    )
