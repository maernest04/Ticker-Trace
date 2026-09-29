import os
from dataclasses import replace
from uuid import uuid4

import pytest
import sqlalchemy as sa
from redis import Redis

from market_execution_lab.engine import simulate
from market_execution_lab.fixtures import ScenarioFixture, generated_scenarios
from market_execution_lab.storage import DatabaseStore
from market_execution_lab.streaming import RedisReplayRunner, market_state_key, order_state_key


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


def _new_run(scenario: ScenarioFixture) -> ScenarioFixture:
    run_id = uuid4()
    return replace(
        scenario,
        order=scenario.order.model_copy(update={"run_id": run_id, "order_id": uuid4()}),
        events=tuple(event.model_copy(update={"run_id": run_id}) for event in scenario.events),
    )
