import os
from dataclasses import replace
from datetime import timedelta
from time import sleep
from uuid import uuid4

import pytest
import sqlalchemy as sa
from redis import Redis

from market_execution_lab.fixtures import generated_scenarios
from market_execution_lab.live_pipeline import LiveEngine, initialize_session, persist_live_batch
from market_execution_lab.pipeline import partition_for_symbol, result_stream_name
from market_execution_lab.pipeline import publish_replay, run_engine, run_persistence
from market_execution_lab.storage import DatabaseStore
from market_execution_lab.streaming import partition_stream_name


pytestmark = [pytest.mark.integration, pytest.mark.skipif(os.getenv("RUN_STREAMING_INTEGRATION") != "1", reason="requires isolated Redis/PostgreSQL")]


@pytest.mark.parametrize("role", ["engine", "source", "result"])
@pytest.mark.parametrize("after_effect", [False, True])
def test_live_retry_commit_boundaries_preserve_partial_fills(monkeypatch, role, after_effect):
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    database = sa.create_engine(os.environ["DATABASE_URL"])
    store = DatabaseStore(database)
    fixture = generated_scenarios()[1]
    run_id = uuid4()
    order = fixture.order.model_copy(update={"run_id": run_id, "order_id": uuid4()})
    partition = partition_for_symbol(order.symbol)
    quote = fixture.events[0].model_copy(update={"run_id": run_id, "partition": partition})
    initialize_session(redis, run_id, [order.symbol], register=False)
    stream = partition_stream_name(str(run_id), partition)
    redis.xadd(stream, {"message_type": "order.command.v1", "payload": order.model_dump_json()})
    redis.xadd(stream, {"message_type": "market.event.v1", "payload": quote.model_dump_json()})
    failed = False

    def interrupted(function):
        def wrapped(*args, **kwargs):
            nonlocal failed
            if failed:
                return function(*args, **kwargs)
            failed = True
            if after_effect:
                function(*args, **kwargs)
            raise RuntimeError("injected commit interruption")
        return wrapped

    try:
        if role == "engine":
            original = redis.xadd
            failing = interrupted(original)
            monkeypatch.setattr(redis, "xadd", lambda target, *args, **kwargs: failing(target, *args, **kwargs) if target == result_stream_name(str(run_id), partition) else original(target, *args, **kwargs))
            with pytest.raises(RuntimeError, match="injected"):
                LiveEngine(redis, run_id, partition).step("first", block_ms=1)
            monkeypatch.setattr(redis, "xadd", original)
            sleep(1.05)
            LiveEngine(redis, run_id, partition).step("replacement", block_ms=1)
        else:
            LiveEngine(redis, run_id, partition).step("engine", block_ms=1)
            method = "record_events" if role == "source" else "complete_run"
            original = getattr(store, method)
            monkeypatch.setattr(store, method, interrupted(original))
            with pytest.raises(RuntimeError, match="injected"):
                persist_live_batch(redis, store, run_id, partition, "first", block_ms=1)
            monkeypatch.setattr(store, method, original)
            sleep(1.05)
        persist_live_batch(redis, store, run_id, partition, "replacement", block_ms=1)
        partial = store.order_for_id(order.order_id)
        assert partial["remaining_quantity"] == 50
        assert len(partial["fills"]) == 1
        first_fill = partial["fills"][0]
        replenished = quote.model_copy(update={"event_id": f"next:{uuid4()}", "event_time": quote.event_time + timedelta(milliseconds=1), "sequence": quote.sequence + 1})
        redis.xadd(stream, {"message_type": "market.event.v1", "payload": replenished.model_dump_json()})
        redis.xadd(stream, {"message_type": "market.event.v1", "payload": replenished.model_dump_json()})
        LiveEngine(redis, run_id, partition).step("second-replacement", block_ms=1)
        persist_live_batch(redis, store, run_id, partition, "second-replacement", block_ms=1)
        completed = store.order_for_id(order.order_id)
        assert completed["final_state"] == "filled"
        assert completed["remaining_quantity"] == 0
        assert len(completed["fills"]) == 2
        assert completed["fills"][0] == first_fill
        assert sum(fill["quantity"] for fill in completed["fills"]) == 150
        assert store.counts_for_run(run_id)["events"] == 2
    finally:
        database.dispose()


def test_finite_source_commit_recovery_drains_more_than_one_pending_page(monkeypatch):
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    database = sa.create_engine(os.environ["DATABASE_URL"])
    store = DatabaseStore(database)
    fixture = generated_scenarios()[0]
    run_id = uuid4()
    order = fixture.order.model_copy(update={"run_id": run_id, "order_id": uuid4()})
    events = tuple(fixture.events[0].model_copy(update={"run_id": run_id, "event_id": f"generated-recover:{index}", "sequence": index + 1, "event_time": fixture.events[0].event_time + timedelta(milliseconds=index)}) for index in range(205))
    scenario = replace(fixture, order=order, events=events)
    partition = publish_replay(redis, scenario, dispatch=False)
    run_engine(redis, run_id, partition, "engine")
    original = store.record_event

    def interrupted(event):
        original(event)
        raise RuntimeError("commit before ack")

    try:
        monkeypatch.setattr(store, "record_event", interrupted)
        with pytest.raises(RuntimeError): run_persistence(redis, store, run_id, partition, "first")
        assert redis.xpending(partition_stream_name(str(run_id), partition), "persistence")["pending"] > 100
        monkeypatch.setattr(store, "record_event", original)
        run_persistence(redis, store, run_id, partition, "replacement", recovery_idle_ms=0)
        assert store.counts_for_run(run_id)["events"] == 205
        assert store.counts_for_run(run_id)["fills"] == 1
        assert redis.xpending(partition_stream_name(str(run_id), partition), "persistence")["pending"] == 0
    finally:
        database.dispose()


def test_unfillable_live_order_stays_unfilled_after_reconstruction():
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    database = sa.create_engine(os.environ["DATABASE_URL"])
    store = DatabaseStore(database)
    fixture = generated_scenarios()[3]
    run_id = uuid4()
    order = fixture.order.model_copy(update={"run_id": run_id, "order_id": uuid4()})
    partition = partition_for_symbol(order.symbol)
    initialize_session(redis, run_id, [order.symbol], register=False)
    stream = partition_stream_name(str(run_id), partition)
    try:
        redis.xadd(stream, {"message_type": "order.command.v1", "payload": order.model_dump_json()})
        for event in fixture.events:
            redis.xadd(stream, {"message_type": "market.event.v1", "payload": event.model_copy(update={"run_id": run_id, "partition": partition}).model_dump_json()})
        for consumer in ("original", "replacement"):
            LiveEngine(redis, run_id, partition).step(consumer, block_ms=1)
            persist_live_batch(redis, store, run_id, partition, consumer, block_ms=1)
        result = store.order_for_id(order.order_id)
        assert result["remaining_quantity"] == order.quantity
        assert not result["fills"]
        assert store.counts_for_run(run_id)["fills"] == 0
    finally:
        database.dispose()
