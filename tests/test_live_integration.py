import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from threading import Event
from time import sleep, time, monotonic
from uuid import uuid4

import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient
from redis import Redis

from market_execution_lab.api_service import create_app
from market_execution_lab.alpaca import normalize_alpaca_message
from market_execution_lab.live_pipeline import initialize_session, LIVE_SESSION_KEY, LiveEngine, persist_live_batch, append_live, run_live_worker
from market_execution_lab.pipeline import partition_for_symbol, _result_payload
from market_execution_lab.storage import DatabaseStore
from market_execution_lab.streaming import partition_stream_name, market_state_key
from market_execution_lab.validation_service import validate_sustained_pipeline
from market_execution_lab.live_ownership import LiveOwnership


pytestmark = [pytest.mark.integration, pytest.mark.skipif(os.getenv("RUN_STREAMING_INTEGRATION") != "1", reason="requires isolated Redis/PostgreSQL")]


def test_real_leases_wait_for_expiry_and_preserve_replacement_owner():
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    keys = [f"test:ownership:{uuid4()}" for _ in range(2)]
    redis.set(keys[1], "previous", ex=1)
    ownership = LiveOwnership(redis, keys, Event())
    try:
        assert ownership.acquire(4)
        assert all(redis.get(key) == ownership.owner for key in keys)
        redis.set(keys[1], "replacement", ex=30)
        with pytest.raises(RuntimeError, match="ownership lost"):
            ownership.check(force=True)
        ownership.release()
        assert redis.get(keys[0]) is None
        assert redis.get(keys[1]) == "replacement"
    finally:
        ownership.release()
        redis.delete(*keys)


def test_slow_reconstruction_renews_real_leases(monkeypatch):
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    run_id = uuid4()
    initialize_session(redis, run_id, ["AAPL"], register=False)
    stream = partition_stream_name(str(run_id), 0)
    for _ in range(4):
        redis.xadd(stream, {"message_type": "replay.started.v1"})
    redis.xreadgroup("engine", "previous", {stream: ">"}, count=100)
    key = f"live:owner:{run_id}:engine:0"
    ownership = LiveOwnership(redis, [key], Event(), ttl_seconds=2)
    original = LiveEngine.process

    def slow_process(engine, message):
        sleep(0.8)
        return original(engine, message)

    monkeypatch.setattr(LiveEngine, "process", slow_process)
    try:
        assert ownership.acquire()
        LiveEngine(redis, run_id, 0, ownership.check)
        assert redis.get(key) == ownership.owner
        with pytest.raises(RuntimeError, match="already has an owner"):
            LiveOwnership(redis, [key], Event()).acquire()
    finally:
        ownership.release()


@pytest.mark.parametrize("replace_owner", [False, True])
def test_ingestion_sigterm_releases_only_its_own_lease(replace_owner):
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    redis.delete(LIVE_SESSION_KEY)
    run_id = uuid4()
    script = "import asyncio; from market_execution_lab import ingestion_service as service\nasync def stream(*args):\n    await asyncio.Future()\nservice.stream_alpaca = stream\nservice.main()"
    environment = {**os.environ, "APP_MODE": "private_live", "ALPACA_API_KEY": "test-key", "ALPACA_API_SECRET": "test-secret", "ALPACA_SYMBOLS": "AAPL"}
    process = subprocess.Popen([sys.executable, "-c", script, "--run-id", str(run_id)], env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        deadline = monotonic() + 10
        while redis.hget(LIVE_SESSION_KEY, "run_id") != str(run_id):
            assert process.poll() is None
            assert monotonic() < deadline
            sleep(0.05)
        assert redis.exists("live:ingestion-owner")
        if replace_owner:
            redis.set("live:ingestion-owner", "replacement", ex=30)
            redis.hset(LIVE_SESSION_KEY, mapping={"run_id": "replacement-session", "status": "connected"})
        process.terminate()
        assert process.wait(timeout=10) == 0, process.communicate()[0]
        if replace_owner:
            assert redis.get("live:ingestion-owner") == "replacement"
            assert redis.hget(LIVE_SESSION_KEY, "status") == "connected"
        else:
            assert redis.get("live:ingestion-owner") is None
            assert redis.hget(LIVE_SESSION_KEY, "status") == "stopped"
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=10)
        redis.delete("live:ingestion-owner")
        redis.delete(LIVE_SESSION_KEY)


@pytest.mark.parametrize("role", ["engine", "persistence"])
def test_worker_process_recovers_after_graceful_and_abrupt_restart(role):
    from market_execution_lab.models import OrderCommand
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    store = DatabaseStore(sa.create_engine(os.environ["DATABASE_URL"]))
    run_id = uuid4()
    initialize_session(redis, run_id, ["AAPL"], register=False)
    partition = partition_for_symbol("AAPL")
    stream = partition_stream_name(str(run_id), partition)
    lease = f"live:owner:{run_id}:{role}:{partition}"
    command = [sys.executable, "-m", "market_execution_lab.live_pipeline", "--run-id", str(run_id), "--role", role, "--partitions", str(partition), "--startup-timeout-seconds", "4"]
    environment = {**os.environ, "APP_MODE": "private_live"}
    process = None

    def wait_for(predicate):
        deadline = monotonic() + 10
        while monotonic() < deadline:
            if process.poll() is not None:
                pytest.fail(process.communicate()[0])
            if predicate():
                return
            sleep(0.05)
        pytest.fail("worker recovery did not complete")

    def drain():
        if role == "engine":
            wait_for(lambda: redis.xpending(stream, "engine")["pending"] == 0 and next(group for group in redis.xinfo_groups(stream) if group["name"] == "engine")["lag"] == 0)
            persist_live_batch(redis, store, run_id, partition, "test-persistence")
        else:
            LiveEngine(redis, run_id, partition).step("test-engine")
            wait_for(lambda: store.counts_for_run(run_id)["fills"] == len(orders))

    orders = []

    def publish_order(sequence):
        now = datetime.now(UTC)
        order = OrderCommand(order_id=uuid4(), run_id=run_id, symbol="AAPL", side="buy", order_type="market", quantity=5, submitted_at=now)
        quote = normalize_alpaca_message({"T": "q", "S": "AAPL", "bp": 199.98, "bs": 10, "ap": 200, "as": 10, "t": (now + timedelta(milliseconds=1)).isoformat()}, run_id, partition, sequence, now)
        append_live(redis, stream, {"message_type": "order.command.v1", "payload": order.model_dump_json()})
        append_live(redis, stream, {"message_type": "market.event.v1", "payload": quote.model_dump_json()})
        orders.append(order)
        drain()

    try:
        process = subprocess.Popen(command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        wait_for(lambda: redis.exists(lease))
        publish_order(1)
        original = store.order_for_id(orders[0].order_id)["fills"]
        process.terminate()
        assert process.wait(timeout=10) == 0
        assert redis.get(lease) is None
        process = subprocess.Popen(command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        wait_for(lambda: redis.exists(lease))
        publish_order(2)
        previous_owner = redis.get(lease)
        process.kill()
        process.wait(timeout=10)
        assert redis.get(lease) == previous_owner
        redis.expire(lease, 1)
        process = subprocess.Popen(command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        wait_for(lambda: redis.get(lease) not in (None, previous_owner))
        publish_order(3)
        assert store.order_for_id(orders[0].order_id)["fills"] == original
        assert store.counts_for_run(run_id)["fills"] == 3
        assert all(len(store.order_for_id(order.order_id)["fills"]) == 1 for order in orders)
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            process.wait(timeout=10)
        redis.delete(lease)


def test_private_live_order_uses_subsequent_alpaca_quote_and_survives_replacement(monkeypatch):
    monkeypatch.setenv("APP_MODE", "private_live")
    monkeypatch.delenv("FLY_WORKER_LIFECYCLE", raising=False)
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    store = DatabaseStore(sa.create_engine(os.environ["DATABASE_URL"]))
    run_id = uuid4()
    initialize_session(redis, run_id, ["AAPL"])
    partition = partition_for_symbol("AAPL")
    stream = partition_stream_name(str(run_id), partition)
    engine = LiveEngine(redis, run_id, partition)

    def publish(sequence, price):
        now = datetime.now(UTC)
        quote = normalize_alpaca_message({"T": "q", "S": "AAPL", "bp": price - 0.02, "bs": 100, "ap": price, "as": 100, "t": now.isoformat()}, run_id, partition, sequence, now)
        append_live(redis, stream, {"message_type": "market.event.v1", "payload": quote.model_dump_json()})
        redis.hset(LIVE_SESSION_KEY, mapping={"status": "connected", "received_at": str(time())})
        return quote

    publish(1, 200)
    engine.step("engine")
    persist_live_batch(redis, store, run_id, partition, "persistence")
    with TestClient(create_app(os.environ["DATABASE_URL"], os.environ["REDIS_URL"])) as client:
        status = client.get("/api/v1/live/session").json()
        assert status["fresh"]
        response = client.post("/api/v1/orders", json={"run_id": str(run_id), "symbol": "AAPL", "side": "buy", "order_type": "market", "quantity": 50})
        assert response.status_code == 202
        order_id = response.json()["order_id"]
        engine.step("engine")
        persist_live_batch(redis, store, run_id, partition, "persistence")
        assert client.get(f"/api/v1/orders/{order_id}").json()["final_state"] == "submitted"
        triggering_quote = publish(2, 201)
        replacement = LiveEngine(redis, run_id, partition)
        replacement.step("replacement")
        persist_live_batch(redis, store, run_id, partition, "persistence")
        result = client.get(f"/api/v1/orders/{order_id}").json()
        assert result["final_state"] == "filled"
        assert result["fills"][0]["triggering_event_id"] == triggering_quote.event_id
        assert result["fills"][0]["price"] == "201.000000"
        assert client.get(f"/api/v1/orders/{order_id}/events").json()[0]["event_id"] == triggering_quote.event_id
        assert store.replay_for_run(run_id)["status"] == "running"
        with client.websocket_connect(f"/ws/v1/sessions/{run_id}") as socket:
            assert socket.receive_json()["orders"][0]["final_state"] == "filled"
        again = LiveEngine(redis, run_id, partition)
        publish(3, 202)
        again.step("replacement-2")
        persist_live_batch(redis, store, run_id, partition, "persistence")
        assert store.counts_for_run(run_id)["fills"] == 1
        state_key = market_state_key(str(run_id), "AAPL")
        old = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
        redis.hset(state_key, "quote_event_time", old)
        assert client.post("/api/v1/orders", json={"run_id": str(run_id), "symbol": "AAPL", "side": "buy", "order_type": "market", "quantity": 50}).status_code == 409
        assert client.post("/api/v1/orders", json={"run_id": str(run_id), "symbol": "MSFT", "side": "buy", "order_type": "market", "quantity": 50}).status_code == 422
        assert client.put("/api/v1/live/symbols", json={"name": "Live", "symbols": ["AAPL", "NVDA"]}).status_code == 200
        assert json.loads(redis.xrange(f"ingestion.control:{run_id}")[-1][1]["symbols"]) == ["AAPL", "NVDA"]
    monkeypatch.setenv("APP_MODE", "public_replay")
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    app = create_app(os.environ["DATABASE_URL"], os.environ["REDIS_URL"])
    app.state.public_request_limit = None
    with TestClient(app) as public:
        assert public.get(f"/api/v1/orders/{order_id}").status_code == 404
        assert public.get(f"/api/v1/replays/{run_id}/events").status_code == 404
        assert public.get(f"/api/v1/orders/{order_id}/events").status_code == 404
        with public.websocket_connect(f"/ws/v1/sessions/{run_id}") as socket:
            assert socket.receive()["code"] == 1008


def test_continuous_load_samples_active_backlog_and_per_event_durable_latency():
    result = validate_sustained_pipeline(os.environ["REDIS_URL"], os.environ["DATABASE_URL"], 2, 2, 2, 40)
    assert not result.errors, result
    assert result.duplicate_events == 0
    assert result.total_events == result.persisted_events == 80
    assert len(result.samples) > 5
    assert result.latency_p99_ms >= result.latency_p50_ms >= 0
    assert result.final_backlog == 0


def test_live_partition_ownership_rejects_competing_worker():
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    run_id = uuid4()
    key = f"live:owner:{run_id}:engine:0"
    redis.set(key, "existing-owner", ex=30)
    try:
        with pytest.raises(RuntimeError, match="already has an owner"):
            run_live_worker(redis, None, run_id, [0], "engine")
        assert redis.get(key) == "existing-owner"
    finally:
        redis.delete(key)


def test_live_pending_reconstruction_emits_one_idempotent_fill():
    from decimal import Decimal
    from market_execution_lab.models import OrderCommand, QuoteEvent
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    store = DatabaseStore(sa.create_engine(os.environ["DATABASE_URL"]))
    run_id = uuid4()
    initialize_session(redis, run_id, ["AAPL"], register=False)
    partition = partition_for_symbol("AAPL")
    stream = partition_stream_name(str(run_id), partition)
    now = datetime.now(UTC)
    order = OrderCommand(order_id=uuid4(), run_id=run_id, symbol="AAPL", side="buy", order_type="market", quantity=50, submitted_at=now)
    quote = QuoteEvent(event_id="pending-quote", run_id=run_id, symbol="AAPL", event_time=now + timedelta(milliseconds=1), ingested_at=now, sequence=1, partition=partition, bid_price=Decimal("199.98"), bid_size=100, ask_price=Decimal("200"), ask_size=100)
    redis.xadd(stream, {"message_type": "order.command.v1", "payload": order.model_dump_json()})
    redis.xadd(stream, {"message_type": "market.event.v1", "payload": quote.model_dump_json()})
    pending = redis.xreadgroup("engine", "crashed", {stream: ">"}, count=100)[0][1]
    redis.xclaim(stream, "engine", "crashed", 0, [entry_id for entry_id, _ in pending], idle=2000)
    replacement = LiveEngine(redis, run_id, partition)
    replacement.step("replacement")
    persist_live_batch(redis, store, run_id, partition, "persistence")
    assert redis.xpending(stream, "engine")["pending"] == 0
    assert store.counts_for_run(run_id)["fills"] == 1
    assert store.order_for_id(order.order_id)["fills"][0]["triggering_event_id"] == "pending-quote"
    from market_execution_lab.engine import ExecutionEngine
    store.complete_run(order, ExecutionEngine(order).snapshot(), now, final=False)
    assert store.order_for_id(order.order_id)["final_state"] == "filled"
    assert store.order_for_id(order.order_id)["remaining_quantity"] == 0


def test_paced_and_unpaced_long_replay_have_identical_durable_results(monkeypatch):
    from dataclasses import replace
    from market_execution_lab import pipeline
    from market_execution_lab.fixtures import replay_scenarios
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    store = DatabaseStore(sa.create_engine(os.environ["DATABASE_URL"]))
    clock = [0.0]
    monkeypatch.setattr(pipeline, "monotonic", lambda: clock[0])
    monkeypatch.setattr(pipeline, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))
    fixture = replay_scenarios()[-2]
    outputs = []
    for pacing in ({}, {"playback_speed": 2}, {"events_per_second": 100}):
        run_id = uuid4()
        order = fixture.order.model_copy(update={"run_id": run_id, "order_id": uuid4()})
        scenario = replace(fixture, order=order, events=tuple(event.model_copy(update={"run_id": run_id}) for event in fixture.events))
        partition = pipeline.publish_replay(redis, scenario, dispatch=False, **pacing)
        pipeline.run_engine(redis, run_id, partition, "paced-engine")
        pipeline.run_persistence(redis, store, run_id, partition, "paced-persistence")
        result = store.order_for_id(order.order_id)
        outputs.append((result["final_state"], result["average_fill_price"], result["remaining_quantity"], [(fill["triggering_event_id"], fill["price"], fill["quantity"]) for fill in result["fills"]]))
    assert outputs[0] == outputs[1] == outputs[2]
    assert clock[0] > 0


def test_zero_execution_metrics_remain_zero_in_postgresql():
    from dataclasses import replace
    from decimal import Decimal
    from market_execution_lab.engine import simulate
    from market_execution_lab.fixtures import generated_scenarios
    store = DatabaseStore(sa.create_engine(os.environ["DATABASE_URL"]))
    fixture = generated_scenarios()[0]
    run_id = uuid4()
    order = fixture.order.model_copy(update={"run_id": run_id, "order_id": uuid4()})
    events = tuple(event.model_copy(update={"run_id": run_id}) for event in fixture.events)
    result = simulate(order, events)
    result = replace(result, metrics=replace(result.metrics, spread_cost=Decimal(0), latency_impact=Decimal(0), time_to_first_fill=timedelta(0), time_to_completion=timedelta(0)))
    store.create_run(run_id, fixture.name, order.submitted_at)
    store.record_order(order)
    store.record_events(events)
    store.complete_run(order, result, datetime.now(UTC))
    saved = store.order_for_id(order.order_id)
    assert Decimal(saved["spread_cost"]) == Decimal(saved["latency_impact"]) == 0
    assert saved["time_to_first_fill_ms"] == saved["time_to_completion_ms"] == 0


def test_live_stream_capacity_does_not_discard_recovery_history(monkeypatch):
    from market_execution_lab import live_pipeline
    monkeypatch.setattr(live_pipeline, "LIVE_MAX_MESSAGES", 2)
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    stream = partition_stream_name(str(uuid4()), 0)
    message = {"message_type": "market.event.v1", "payload": "{}"}
    first = append_live(redis, stream, message)
    assert first
    assert append_live(redis, stream, message)
    assert not append_live(redis, stream, message)
    assert redis.xlen(stream) == 2
    assert redis.xrange(stream)[0][0] == first
    redis.expire(stream, 900)
