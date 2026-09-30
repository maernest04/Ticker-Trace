import os
import json
import subprocess
import sys
from dataclasses import replace
from decimal import Decimal
from time import time
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient
from redis import Redis

from market_execution_lab.engine import simulate
from market_execution_lab.fixtures import ScenarioFixture, generated_scenarios
from market_execution_lab.api_service import create_app as create_api_app
from market_execution_lab.operations_service import create_app
from market_execution_lab.pipeline import (
    BackpressureError,
    ENGINE_GROUP,
    PERSISTENCE_GROUP,
    dead_letter_stream_name,
    partition_for_symbol,
    publish_replay,
    result_stream_name,
    run_engine,
    run_persistence,
)
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


@pytest.mark.skipif(
    os.getenv("RUN_STREAMING_INTEGRATION") != "1",
    reason="set RUN_STREAMING_INTEGRATION=1 with local Redis and PostgreSQL",
)
def test_replacement_engine_recovers_pending_messages_after_a_worker_dies() -> None:
    template = next(scenario for scenario in generated_scenarios() if scenario.name == "price_gap_before_activation")
    scenario = _new_run(template)
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    store = DatabaseStore(sa.create_engine(os.environ["DATABASE_URL"]))
    partition = publish_replay(redis, scenario)
    stream = partition_stream_name(str(scenario.order.run_id), partition)
    redis.xgroup_create(stream, ENGINE_GROUP, id="0-0")
    assert redis.xreadgroup(ENGINE_GROUP, "crashed-engine", {stream: ">"}, count=2)

    actual = run_engine(redis, scenario.order.run_id, partition, "replacement-engine", recovery_idle_ms=0)
    run_persistence(redis, store, scenario.order.run_id, partition, "persistence-1", recovery_idle_ms=0)

    assert actual == simulate(scenario.order, scenario.events)
    assert redis.xpending(stream, ENGINE_GROUP)["pending"] == 0
    assert store.counts_for_run(scenario.order.run_id)["fills"] == len(actual.fills)


@pytest.mark.skipif(
    os.getenv("RUN_STREAMING_INTEGRATION") != "1",
    reason="set RUN_STREAMING_INTEGRATION=1 with local Redis and PostgreSQL",
)
@pytest.mark.parametrize("invalid_payload", ["not-json", '{"event_type":"market.quote.v1"}'])
def test_invalid_message_is_dead_lettered_without_stopping_the_replay(invalid_payload: str) -> None:
    template = next(scenario for scenario in generated_scenarios() if scenario.name == "complete_market_fill")
    scenario = _new_run(template)
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    store = DatabaseStore(sa.create_engine(os.environ["DATABASE_URL"]))
    partition = publish_replay(redis, scenario)
    stream = partition_stream_name(str(scenario.order.run_id), partition)
    redis.xadd(stream, {"message_type": "market.event.v1", "payload": invalid_payload})

    actual = run_engine(redis, scenario.order.run_id, partition, "engine-1")
    run_persistence(redis, store, scenario.order.run_id, partition, "persistence-1")

    dead_letters = redis.xrange(dead_letter_stream_name(str(scenario.order.run_id), partition))
    assert actual == simulate(scenario.order, scenario.events)
    assert {entry["group"] for _, entry in dead_letters} == {ENGINE_GROUP, PERSISTENCE_GROUP}
    assert store.counts_for_run(scenario.order.run_id)["events"] == len(scenario.events)


@pytest.mark.skipif(
    os.getenv("RUN_STREAMING_INTEGRATION") != "1",
    reason="set RUN_STREAMING_INTEGRATION=1 with local Redis and PostgreSQL",
)
def test_persistence_recovers_after_a_temporary_database_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    scenario = _new_run(generated_scenarios()[0])
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    store = DatabaseStore(sa.create_engine(os.environ["DATABASE_URL"]))
    partition = publish_replay(redis, scenario)
    run_engine(redis, scenario.order.run_id, partition, "engine-1")
    original_complete_run = store.complete_run

    def unavailable(*_args, **_kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(store, "complete_run", unavailable)
    with pytest.raises(RuntimeError, match="database unavailable"):
        run_persistence(redis, store, scenario.order.run_id, partition, "persistence-1")

    results = result_stream_name(str(scenario.order.run_id), partition)
    assert redis.xpending(results, PERSISTENCE_GROUP)["pending"] == 1

    monkeypatch.setattr(store, "complete_run", original_complete_run)
    run_persistence(redis, store, scenario.order.run_id, partition, "replacement-persistence", recovery_idle_ms=0)

    assert redis.xpending(results, PERSISTENCE_GROUP)["pending"] == 0
    assert store.counts_for_run(scenario.order.run_id)["fills"] == 1


@pytest.mark.skipif(
    os.getenv("RUN_STREAMING_INTEGRATION") != "1",
    reason="set RUN_STREAMING_INTEGRATION=1 with local Redis and PostgreSQL",
)
def test_queue_limit_rejects_a_replay_before_publishing() -> None:
    scenario = _new_run(generated_scenarios()[0])
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    partition = partition_for_symbol(scenario.order.symbol)
    stream = partition_stream_name(str(scenario.order.run_id), partition)

    with pytest.raises(BackpressureError):
        publish_replay(redis, scenario, max_queue_depth=1)

    assert redis.xlen(stream) == 0


@pytest.mark.skipif(
    os.getenv("RUN_STREAMING_INTEGRATION") != "1",
    reason="set RUN_STREAMING_INTEGRATION=1 with local Redis and PostgreSQL",
)
def test_delivery_limit_dead_letters_an_abandoned_message() -> None:
    run_id = uuid4()
    partition = 0
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    stream = partition_stream_name(str(run_id), partition)
    redis.xadd(stream, {"message_type": "invalid.v1", "payload": "{}"})
    redis.xgroup_create(stream, ENGINE_GROUP, id="0-0")
    assert redis.xreadgroup(ENGINE_GROUP, "crashed-engine", {stream: ">"})

    with pytest.raises(RuntimeError):
        run_engine(redis, run_id, partition, "replacement-engine", recovery_idle_ms=0, max_deliveries=1)

    dead_letters = redis.xrange(dead_letter_stream_name(str(run_id), partition))
    assert dead_letters[0][1]["reason"] == "delivery limit reached"
    assert redis.xpending(stream, ENGINE_GROUP)["pending"] == 0


@pytest.mark.skipif(
    os.getenv("RUN_STREAMING_INTEGRATION") != "1",
    reason="set RUN_STREAMING_INTEGRATION=1 with local Redis and PostgreSQL",
)
def test_operational_endpoints_report_readiness_and_pipeline_metrics() -> None:
    scenario = _new_run(generated_scenarios()[0])
    redis_url = os.environ["REDIS_URL"]
    redis = Redis.from_url(redis_url, decode_responses=True)
    store = DatabaseStore(sa.create_engine(os.environ["DATABASE_URL"]))
    partition = publish_replay(redis, scenario)
    run_engine(redis, scenario.order.run_id, partition, "engine-1")
    run_persistence(redis, store, scenario.order.run_id, partition, "persistence-1")
    client = TestClient(create_app(redis_url))

    health = client.get("/health", headers={"x-request-id": "request-123"})
    ready = client.get("/ready")
    metrics = client.get(f"/metrics?run_id={scenario.order.run_id}&partition={partition}")

    assert health.json() == {"status": "ok"}
    assert health.headers["x-request-id"] == "request-123"
    assert ready.json() == {"status": "ready"}
    assert "market_execution_throughput_events_per_second" in metrics.text
    assert "market_execution_processing_latency_ms" in metrics.text


@pytest.mark.skipif(
    os.getenv("RUN_STREAMING_INTEGRATION") != "1",
    reason="set RUN_STREAMING_INTEGRATION=1 with local Redis and PostgreSQL",
)
def test_read_api_returns_replay_state_without_direct_dependency_access() -> None:
    template = next(scenario for scenario in generated_scenarios() if scenario.name == "price_gap_before_activation")
    scenario = _new_run(template)
    redis_url = os.environ["REDIS_URL"]
    database_url = os.environ["DATABASE_URL"]
    redis = Redis.from_url(redis_url, decode_responses=True)
    store = DatabaseStore(sa.create_engine(database_url))
    partition = publish_replay(redis, scenario)
    run_engine(redis, scenario.order.run_id, partition, "engine-1")
    run_persistence(redis, store, scenario.order.run_id, partition, "persistence-1")
    client = TestClient(create_api_app(database_url, redis_url))

    symbols = client.get("/api/v1/symbols")
    configuration = client.get("/api/v1/configuration")
    scenarios = client.get("/api/v1/scenarios")
    market = client.get(f"/api/v1/market/{scenario.order.symbol}?run_id={scenario.order.run_id}")
    order = client.get(f"/api/v1/orders/{scenario.order.order_id}")
    replay = client.get(f"/api/v1/replays/{scenario.order.run_id}")
    events = client.get(f"/api/v1/replays/{scenario.order.run_id}/events")
    pipeline_health = client.get(f"/api/v1/replays/{scenario.order.run_id}/health?partition={partition}")
    watchlists = client.get("/api/v1/watchlists")

    assert {item["symbol"] for item in symbols.json()} >= {scenario.order.symbol}
    assert configuration.json() == {"mode": "public_replay"}
    assert any(item["name"] == "price_gap_before_activation" for item in scenarios.json())
    assert next(item for item in scenarios.json() if item["name"] == "price_gap_before_activation")["market"]["ask_price"] == "181.00"
    assert next(item for item in scenarios.json() if item["name"] == "price_gap_before_activation")["replay_change_percent"] == "0.56"
    assert market.json()["ask_price"] == "181.00"
    assert order.json()["final_state"] == "filled"
    assert Decimal(order.json()["metrics"]["fill_rate"]) == Decimal("1")
    assert Decimal(order.json()["fills"][0]["price"]) == Decimal("181.00")
    assert replay.json()["counts"]["events"] == len(scenario.events)
    assert [event["event_id"] for event in events.json()] == [event.event_id for event in scenario.events]
    assert pipeline_health.json()["queue_depth"] == 0
    assert pipeline_health.json()["throughput_events_per_second"] > 0
    assert watchlists.status_code == 200


@pytest.mark.skipif(
    os.getenv("RUN_STREAMING_INTEGRATION") != "1",
    reason="set RUN_STREAMING_INTEGRATION=1 with local Redis and PostgreSQL",
)
def test_command_api_queues_orders_and_watchlists_without_direct_engine_access() -> None:
    redis_url = os.environ["REDIS_URL"]
    database_url = os.environ["DATABASE_URL"]
    redis = Redis.from_url(redis_url, decode_responses=True)
    store = DatabaseStore(sa.create_engine(database_url))
    client = TestClient(create_api_app(database_url, redis_url))

    replay = client.post("/api/v1/replays", json={"scenario_name": "complete_market_fill"})
    queued = client.post(
        "/api/v1/orders",
        json={
            "scenario_name": "price_gap_before_activation",
            "symbol": "AMZN",
            "side": "buy",
            "order_type": "market",
            "quantity": 10,
            "latency_ms": 100,
        },
    )

    assert replay.status_code == 202
    assert queued.status_code == 202
    command = queued.json()
    run_id = command["run_id"]
    order_id = command["order_id"]
    partition = command["partition"]
    stream = partition_stream_name(run_id, partition)
    assert [message["message_type"] for _, message in redis.xrange(stream)] == [
        "replay.started.v1",
        "order.command.v1",
        "market.event.v1",
        "market.event.v1",
        "market.event.v1",
        "replay.completed.v1",
    ]

    run_engine(redis, UUID(run_id), partition, "engine-1")
    run_persistence(redis, store, UUID(run_id), partition, "persistence-1")
    order = client.get(f"/api/v1/orders/{order_id}")

    assert order.json()["final_state"] == "filled"
    assert Decimal(order.json()["fills"][0]["price"]) == Decimal("181.00")
    with client.websocket_connect(f"/ws/v1/sessions/{run_id}") as websocket:
        snapshot = websocket.receive_json()
    assert snapshot["type"] == "session.snapshot"
    assert snapshot["market"]["AMZN"]["ask_price"] == "181.00"
    assert snapshot["orders"][0]["fills"][0]["triggering_event_id"] == "price-gap-quote-2"
    assert snapshot["events"][-1]["event_id"] == "price-gap-quote-2"

    created_watchlist = client.post("/api/v1/watchlists", json={"name": "Tech", "symbols": ["AAPL", "MSFT"]})
    updated_watchlist = client.put(
        f"/api/v1/watchlists/{created_watchlist.json()['watchlist_id']}",
        json={"name": "Tech", "symbols": ["MSFT", "NVDA"]},
    )
    updates = redis.xrange("ingestion.control")

    assert created_watchlist.status_code == 201
    assert updated_watchlist.json()["symbols"] == ["MSFT", "NVDA"]
    assert json.loads(updates[-1][1]["symbols"]) == ["MSFT", "NVDA"]


@pytest.mark.skipif(
    os.getenv("RUN_STREAMING_INTEGRATION") != "1",
    reason="set RUN_STREAMING_INTEGRATION=1 with local Redis and PostgreSQL",
)
def test_public_api_enforces_request_ids_errors_and_rate_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_MODE", "public_replay")
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    redis_url = os.environ["REDIS_URL"]
    database_url = os.environ["DATABASE_URL"]
    redis = Redis.from_url(redis_url, decode_responses=True)
    redis.delete(f"rate_limit:testclient:{int(time() // 60)}")
    client = TestClient(create_api_app(database_url, redis_url, public_request_limit=10))

    missing = client.get(f"/api/v1/orders/{uuid4()}", headers={"x-request-id": "missing-123"})
    invalid = client.post("/api/v1/orders", json={"scenario_name": "complete_market_fill"})

    assert missing.status_code == 404
    assert missing.headers["x-request-id"] == "missing-123"
    assert missing.json()["error"] == {
        "code": "http_error",
        "message": "order not found",
        "request_id": "missing-123",
    }
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "validation_error"

    redis.delete(f"rate_limit:testclient:{int(time() // 60)}")
    limited_client = TestClient(create_api_app(database_url, redis_url, public_request_limit=2))
    assert limited_client.get("/api/v1/watchlists").status_code == 200
    assert limited_client.get("/api/v1/watchlists").status_code == 200
    limited = limited_client.get("/api/v1/watchlists", headers={"x-request-id": "limited-123"})

    assert limited.status_code == 429
    assert limited.headers["x-request-id"] == "limited-123"
    assert limited.json()["error"]["code"] == "rate_limited"


def test_public_mode_rejects_live_alpaca_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_MODE", "public_replay")
    monkeypatch.setenv("ALPACA_API_KEY", "key")
    monkeypatch.setenv("ALPACA_API_SECRET", "secret")

    with pytest.raises(ValueError, match="cannot enable Alpaca credentials"):
        create_api_app("postgresql+psycopg://unused", "redis://localhost:6379/0")


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
