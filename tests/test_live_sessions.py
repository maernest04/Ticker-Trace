import os
import signal
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from threading import Event, Thread
from time import monotonic, sleep, time
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient
from redis import Redis

from market_execution_lab.api_service import create_app
from market_execution_lab import live_sessions
from market_execution_lab.engine import ExecutionEngine
from market_execution_lab.live_pipeline import LIVE_SESSION_KEY, LiveEngine, append_live, persist_live_batch, run_live_worker
from market_execution_lab.live_sessions import LiveLimits, SessionCoordinator
from market_execution_lab.models import OrderCommand, QuoteEvent
from market_execution_lab.pipeline import PARTITION_COUNT, partition_for_symbol
from market_execution_lab.storage import DatabaseStore
from market_execution_lab.streaming import partition_stream_name


def test_lifecycle_limits_require_headroom_and_retention_order(monkeypatch):
    for changes in ({"messages": 100000}, {"orders": 256}, {"session_seconds": 0}, {"history_seconds": 1}):
        with pytest.raises(ValueError):
            LiveLimits(**changes)
    monkeypatch.setenv("LIVE_SESSION_SECONDS", "60")
    assert LiveLimits.from_environment().session_seconds == 60


@pytest.fixture
def lifecycle():
    if os.getenv("RUN_STREAMING_INTEGRATION") != "1":
        pytest.skip("requires isolated Redis/PostgreSQL")
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    database = sa.create_engine(os.environ["DATABASE_URL"])
    store = DatabaseStore(database)
    redis.delete(LIVE_SESSION_KEY)
    owner = str(uuid4())
    assert redis.set("live:ingestion-owner", owner, nx=True, ex=30)
    coordinator = SessionCoordinator(redis, store, owner, LiveLimits(session_seconds=600, messages=20, orders=2, recovery_seconds=1, raw_seconds=2, history_seconds=4))
    run_id = uuid4()
    coordinator.resume(["AAPL"], run_id)
    try:
        yield redis, store, coordinator, run_id
    finally:
        redis.delete("live:ingestion-owner", LIVE_SESSION_KEY)
        database.dispose()


def publish_partial(redis, run_id):
    now = datetime.now(UTC)
    partition = partition_for_symbol("AAPL")
    order = OrderCommand(order_id=uuid4(), run_id=run_id, symbol="AAPL", side="buy", order_type="market", quantity=150, submitted_at=now)
    quote = QuoteEvent(event_id=f"partial:{uuid4()}", run_id=run_id, symbol="AAPL", event_time=now + timedelta(milliseconds=1), ingested_at=now, sequence=1, partition=partition, bid_price=Decimal("199.98"), bid_size=100, ask_price=Decimal("200"), ask_size=100)
    stream = partition_stream_name(str(run_id), partition)
    append_live(redis, stream, {"message_type": "order.command.v1", "payload": order.model_dump_json()})
    append_live(redis, stream, {"message_type": "market.event.v1", "payload": quote.model_dump_json()})
    return order, quote


def drain_session(redis, store, run_id):
    for partition in range(PARTITION_COUNT):
        stream = partition_stream_name(str(run_id), partition)
        group = next(group for group in redis.xinfo_groups(stream) if group["name"] == "engine")
        if group["lag"] or group["pending"]:
            LiveEngine(redis, run_id, partition).step("lifecycle-test", block_ms=1)
        persist_live_batch(redis, store, run_id, partition, "lifecycle-test", block_ms=1)


@pytest.mark.integration
@pytest.mark.parametrize("trigger", ["time", "messages", "orders"])
def test_rollover_closes_partial_order_and_blocks_new_submissions(lifecycle, monkeypatch, trigger):
    redis, store, coordinator, run_id = lifecycle
    order, quote = publish_partial(redis, run_id)
    drain_session(redis, store, run_id)
    partial = store.order_for_id(order.order_id)
    assert partial["remaining_quantity"] == 50
    if trigger == "time":
        redis.hset(LIVE_SESSION_KEY, "started_at", str(time() - 601))
    elif trigger == "messages":
        coordinator.limits = LiveLimits(messages=3)
    else:
        redis.set(f"live:orders:{run_id}", 2)
    assert coordinator.rollover_reason(run_id)
    coordinator.begin_close(run_id)
    boundaries = redis.hgetall(f"live:closing:{run_id}")
    coordinator.begin_close(run_id)
    assert redis.hgetall(f"live:closing:{run_id}") == boundaries
    assert coordinator.finish_close(run_id) is None
    monkeypatch.setenv("APP_MODE", "private_live")
    monkeypatch.delenv("FLY_WORKER_LIFECYCLE", raising=False)
    with TestClient(create_app(os.environ["DATABASE_URL"], os.environ["REDIS_URL"])) as client:
        assert not client.get("/api/v1/live/session").json()["fresh"]
        assert client.post("/api/v1/orders", json={"run_id": str(run_id), "symbol": "AAPL", "side": "buy", "order_type": "market", "quantity": 5}).status_code == 409
        assert client.put("/api/v1/live/symbols", json={"name": "Live", "symbols": ["MSFT"]}).status_code == 409
    drain_session(redis, store, run_id)
    successor = coordinator.finish_close(run_id)
    assert successor != run_id
    assert redis.hget(LIVE_SESSION_KEY, "run_id") == str(successor)
    closed = store.order_for_id(order.order_id)
    assert closed["final_state"] == "cancelled"
    assert closed["remaining_quantity"] == 50
    assert closed["fills"] == partial["fills"]
    assert closed["transitions"][-1]["reason"] == "session ended"
    assert store.events_for_order(order.order_id)[0]["event_id"] == quote.event_id
    engine = ExecutionEngine(order)
    engine.process(quote)
    store.complete_run(order, engine.snapshot(), datetime.now(UTC), final=False)
    assert store.order_for_id(order.order_id)["final_state"] == "cancelled"
    assert store.replay_for_run(run_id)["status"] == "completed"
    assert store.counts_for_run(successor)["orders"] == 0


@pytest.mark.integration
@pytest.mark.parametrize("stage", ["intent", "boundary", "completion", "initialization", "activation"])
def test_interrupted_rollover_resumes_one_successor(lifecycle, monkeypatch, stage):
    redis, store, coordinator, run_id = lifecycle
    order, _ = publish_partial(redis, run_id)
    drain_session(redis, store, run_id)

    def fail_once(function):
        failed = False
        def wrapped(*args, **kwargs):
            nonlocal failed
            result = function(*args, **kwargs)
            if not failed:
                failed = True
                raise RuntimeError("injected interruption")
            return result
        return wrapped

    if stage == "intent":
        monkeypatch.setattr(store, "begin_private_close", fail_once(store.begin_private_close))
    elif stage == "boundary":
        original_eval = redis.eval
        failing_eval = fail_once(original_eval)
        monkeypatch.setattr(redis, "eval", lambda script, *args: failing_eval(script, *args) if "boundary_ready" in script else original_eval(script, *args))
    if stage in {"intent", "boundary"}:
        with pytest.raises(RuntimeError, match="injected"):
            coordinator.begin_close(run_id)
        coordinator.resume(["MSFT"])
    coordinator.begin_close(run_id)
    successor = store.private_session(run_id)["next_run_id"]
    drain_session(redis, store, run_id)
    if stage == "completion":
        monkeypatch.setattr(store, "finish_private_close", fail_once(store.finish_private_close))
    elif stage == "initialization":
        monkeypatch.setattr(live_sessions, "initialize_session", fail_once(live_sessions.initialize_session))
    elif stage == "activation":
        monkeypatch.setattr(coordinator, "expire_closed", fail_once(coordinator.expire_closed))
    if stage in {"completion", "initialization", "activation"}:
        with pytest.raises(RuntimeError, match="injected"):
            coordinator.finish_close(run_id)
    assert coordinator.finish_close(run_id) == successor
    recovered = SessionCoordinator(redis, store, coordinator.owner, coordinator.limits)
    resumed = recovered.resume(["MSFT"])
    assert UUID(resumed["run_id"]) == successor
    assert resumed["symbols"] == '["AAPL"]'
    assert store.order_for_id(order.order_id)["final_state"] == "cancelled"
    assert store.counts_for_run(run_id)["fills"] == 1
    assert redis.xlen(partition_stream_name(str(successor), 0)) == 1


@pytest.mark.integration
def test_resume_restores_subscriptions_and_rejects_missing_history(lifecycle):
    redis, store, coordinator, run_id = lifecycle
    store.create_watchlist(run_id, "Live", ["AAPL", "MSFT"], datetime.now(UTC))
    store.record_replay_settings(run_id, "private_live", ["AAPL", "MSFT"])
    resumed = coordinator.resume(["NVDA"])
    assert resumed["run_id"] == str(run_id)
    assert resumed["symbols"] == '["AAPL", "MSFT"]'
    drain_session(redis, store, run_id)
    assert store.replay_for_run(run_id)["settings"]["symbols"] == ["AAPL", "MSFT"]
    missing = partition_stream_name(str(run_id), 0)
    redis.delete(missing)
    with pytest.raises(RuntimeError, match="history is missing"):
        coordinator.resume(["NVDA"])
    assert redis.hget(LIVE_SESSION_KEY, "run_id") == str(run_id)


@pytest.mark.integration
def test_retention_is_batched_and_preserves_active_quotes_and_unrelated_runs(lifecycle):
    redis, store, coordinator, run_id = lifecycle
    order, quote = publish_partial(redis, run_id)
    drain_session(redis, store, run_id)
    coordinator.begin_close(run_id)
    drain_session(redis, store, run_id)
    successor = coordinator.finish_close(run_id)
    store.record_event(quote.model_copy(update={"event_id": f"unused:{uuid4()}"}))
    active_order, _ = publish_partial(redis, successor)
    drain_session(redis, store, successor)
    unrelated = uuid4()
    store.create_run(unrelated, "continuous_benchmark_v1", datetime.now(UTC))
    store.record_replay_settings(unrelated, "private_live", ["AAPL"])
    unrelated_event = quote.model_copy(update={"run_id": unrelated})
    store.record_event(unrelated_event)
    closed_at = store.private_session(run_id)["closed_at"]
    removed = store.prune_private_history(2, 4, batch_size=1, now=closed_at + timedelta(seconds=3))
    assert removed["events"] <= 1
    assert store.events_for_order(order.order_id)[0]["event_id"] == quote.event_id
    assert store.order_for_id(active_order.order_id)["remaining_quantity"] == 50
    assert store.counts_for_run(unrelated)["events"] == 1
    assert redis.ttl(partition_stream_name(str(successor), 0)) == -1
    assert redis.ttl(partition_stream_name(str(run_id), 0)) >= 0
    coordinator.expire_closed(successor)
    coordinator.expire_closed(unrelated)
    assert redis.ttl(partition_stream_name(str(successor), 0)) == -1
    for _ in range(200):
        removed = store.prune_private_history(2, 4, batch_size=1, now=closed_at + timedelta(seconds=5))
        assert max(removed.values()) <= 1
        if store.private_session(run_id) is None:
            break
    assert store.replay_for_run(run_id) is None
    assert store.replay_for_run(successor)["status"] == "running"
    assert store.counts_for_run(unrelated)["events"] == 1


@pytest.mark.integration
def test_workers_follow_multiple_sessions_without_restarts(lifecycle):
    redis, store, coordinator, run_id = lifecycle
    stop, errors = Event(), []
    def worker(role):
        try:
            current = run_id
            while not stop.is_set():
                run_live_worker(redis, store if role == "persistence" else None, current, list(range(PARTITION_COUNT)), role, stop=stop, follow_session=True)
                current = UUID(redis.hget(LIVE_SESSION_KEY, "run_id"))
        except Exception as error:
            errors.append(error)
    workers = [Thread(target=worker, args=(role,)) for role in ("engine", "persistence")]
    for thread in workers:
        thread.start()
    completed = []
    try:
        for _ in range(3):
            order, _ = publish_partial(redis, run_id)
            coordinator.begin_close(run_id)
            deadline = monotonic() + 15
            while (successor := coordinator.finish_close(run_id)) is None:
                assert not errors, errors
                assert monotonic() < deadline
                sleep(0.1)
            completed.append(run_id)
            assert store.order_for_id(order.order_id)["final_state"] == "cancelled"
            assert store.counts_for_run(run_id)["fills"] == 1
            run_id = successor
        assert len(set(completed + [run_id])) == 4
        sleep(1.1)
        assert all(not redis.exists(partition_stream_name(str(old), 0)) for old in completed)
        assert redis.exists(partition_stream_name(str(run_id), 0))
        assert not errors
    finally:
        stop.set()
        for thread in workers:
            thread.join(timeout=10)
            assert not thread.is_alive()


@pytest.mark.integration
def test_order_admission_rechecks_session_atomically(lifecycle, monkeypatch):
    redis, store, coordinator, run_id = lifecycle
    publish_partial(redis, run_id)
    drain_session(redis, store, run_id)
    redis.hset(LIVE_SESSION_KEY, mapping={"status": "connected", "received_at": str(time())})
    monkeypatch.setenv("APP_MODE", "private_live")
    monkeypatch.delenv("FLY_WORKER_LIFECYCLE", raising=False)
    app = create_app(os.environ["DATABASE_URL"], os.environ["REDIS_URL"])
    original_eval = app.state.redis.eval
    def race(script, *args):
        if "order.command.v1" in script:
            redis.hset(LIVE_SESSION_KEY, "phase", "closing")
        return original_eval(script, *args)
    monkeypatch.setattr(app.state.redis, "eval", race)
    stream = partition_stream_name(str(run_id), partition_for_symbol("AAPL"))
    before = redis.xlen(stream)
    with TestClient(app) as client:
        result = client.post("/api/v1/orders", json={"run_id": str(run_id), "symbol": "AAPL", "side": "buy", "order_type": "market", "quantity": 5})
        assert result.status_code == 409
    assert redis.xlen(stream) == before
    assert redis.get(f"live:orders:{run_id}") is None


@pytest.mark.integration
def test_ingestion_restart_and_three_rollovers_with_real_service_processes(lifecycle, monkeypatch):
    redis, store, coordinator, run_id = lifecycle
    redis.delete("live:ingestion-owner")
    script = """import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from market_execution_lab import ingestion_service as service
from market_execution_lab.models import QuoteEvent
async def stream(settings, run_id, partition, publish, updates, status):
    sequence = 0
    while True:
        for symbol in settings.symbols:
            sequence += 1
            now = datetime.now(UTC)
            await publish(QuoteEvent(event_id=f'generated:{run_id}:{sequence}', run_id=run_id, symbol=symbol, event_time=now, ingested_at=now, sequence=sequence, partition=partition(symbol), bid_price=Decimal('199.98'), bid_size=100, ask_price=Decimal('200'), ask_size=100))
        await asyncio.sleep(0.05)
service.stream_alpaca = stream
service.main()
"""
    environment = {**os.environ, "APP_MODE": "private_live", "ALPACA_API_KEY": "test-key", "ALPACA_API_SECRET": "test-secret", "ALPACA_SYMBOLS": "AAPL", "LIVE_SESSION_SECONDS": "600", "LIVE_MESSAGES": "1000", "LIVE_RECOVERY_SECONDS": "1", "LIVE_RAW_SECONDS": "60", "LIVE_HISTORY_SECONDS": "120"}
    ingestion_command = [sys.executable, "-c", script]
    processes = [subprocess.Popen(ingestion_command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)]
    processes.extend(subprocess.Popen([sys.executable, "-m", "market_execution_lab.live_pipeline", "--role", role], env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) for role in ("engine", "persistence"))
    worker_pids = [process.pid for process in processes[1:]]
    def wait_for(predicate):
        deadline = monotonic() + 20
        while monotonic() < deadline:
            for process in processes:
                assert process.poll() is None, process.communicate()[0]
            if predicate():
                return
            sleep(0.05)
        pytest.fail("service lifecycle did not complete")
    monkeypatch.setenv("APP_MODE", "private_live")
    monkeypatch.delenv("FLY_WORKER_LIFECYCLE", raising=False)
    try:
        wait_for(lambda: bool(redis.hgetall(f"state:replay:{run_id}:AAPL")))
        with TestClient(create_app(os.environ["DATABASE_URL"], os.environ["REDIS_URL"])) as client:
            assert client.put("/api/v1/live/symbols", json={"name": "Live", "symbols": ["MSFT"]}).status_code == 200
            processes[0].terminate()
            assert processes[0].wait(timeout=10) == 0
            processes[0] = subprocess.Popen(ingestion_command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            wait_for(lambda: bool(redis.hgetall(f"state:replay:{run_id}:MSFT")))
            assert redis.hget(LIVE_SESSION_KEY, "run_id") == str(run_id)
            wait_for(lambda: client.get("/api/v1/live/session").json()["fresh"])
            response = client.post("/api/v1/orders", json={"run_id": str(run_id), "symbol": "MSFT", "side": "buy", "order_type": "market", "quantity": 5})
            assert response.status_code == 202, response.json()
            order_id = UUID(response.json()["order_id"])
            wait_for(lambda: (store.order_for_id(order_id) or {}).get("final_state") == "filled")
            completed = []
            for _ in range(3):
                previous = run_id
                redis.hset(LIVE_SESSION_KEY, "started_at", str(time() - 601))
                wait_for(lambda: redis.hget(LIVE_SESSION_KEY, "run_id") != str(previous))
                run_id = UUID(redis.hget(LIVE_SESSION_KEY, "run_id"))
                wait_for(lambda: client.get("/api/v1/live/session").json()["fresh"])
                assert store.private_session(previous)["status"] == "completed"
                completed.append(previous)
                assert client.get("/api/v1/live/session").json()["symbols"] == ["MSFT"]
            assert [process.pid for process in processes[1:]] == worker_pids
            assert len(set(completed + [run_id])) == 4
            assert len(store.order_for_id(order_id)["fills"]) == 1
            assert len(store.events_for_order(order_id)) == 1
            original_fills = store.order_for_id(order_id)["fills"]
            process_ids = [process.pid for process in processes]
            for cycle in range(4):
                wait_for(lambda: len(list(redis.scan_iter(match=f"live:owner:{run_id}:*"))) == 32 and
                         (quote_time := redis.hget(f"state:replay:{run_id}:MSFT", "quote_ingested_at")) and
                         time() - datetime.fromisoformat(quote_time).timestamp() < 5 and
                         client.get("/api/v1/live/session").json()["fresh"])
                probe = client.post("/api/v1/orders", json={"run_id": str(run_id), "symbol": "MSFT", "side": "buy", "order_type": "market", "quantity": 1})
                assert probe.status_code == 202, probe.json()
                probe_id = UUID(probe.json()["order_id"])
                wait_for(lambda: (store.order_for_id(probe_id) or {}).get("final_state") == "filled")
                for process in processes:
                    process.send_signal(signal.SIGSTOP)
                try:
                    keys = ["live:ingestion-owner", *redis.scan_iter(match=f"live:owner:{run_id}:*")]
                    assert len(keys) == 33
                    for key in keys:
                        assert redis.expire(key, 1)
                    sleep(11 if cycle == 0 else 1.2)
                    assert all(not redis.exists(key) for key in keys)
                finally:
                    resumed_at = time()
                    for process in processes:
                        process.send_signal(signal.SIGCONT)
                wait_for(lambda: float(redis.hget(LIVE_SESSION_KEY, "received_at") or 0) > resumed_at and (quote_time := redis.hget(f"state:replay:{run_id}:MSFT", "quote_ingested_at")) and datetime.fromisoformat(quote_time).timestamp() > resumed_at and client.get("/api/v1/live/session").json()["fresh"] and len(list(redis.scan_iter(match=f"live:owner:{run_id}:*"))) == 32)
                assert [process.pid for process in processes] == process_ids
                assert redis.hget(LIVE_SESSION_KEY, "run_id") == str(run_id)
                assert store.order_for_id(order_id)["fills"] == original_fills
            response = client.post("/api/v1/orders", json={"run_id": str(run_id), "symbol": "MSFT", "side": "buy", "order_type": "market", "quantity": 5})
            assert response.status_code == 202, response.json()
            recovered_order_id = UUID(response.json()["order_id"])
            wait_for(lambda: (store.order_for_id(recovered_order_id) or {}).get("final_state") == "filled")
            assert len(store.order_for_id(recovered_order_id)["fills"]) == 1
            assert len(store.events_for_order(recovered_order_id)) == 1
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=10)
