import asyncio
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from market_execution_lab import api_service, pipeline
from market_execution_lab.engine import ExecutionEngine, simulate
from market_execution_lab.fixtures import generated_scenarios, replay_scenarios
from market_execution_lab.pipeline import _result_payload, _result_from_payload, publish_replay
from market_execution_lab.storage import _milliseconds
from market_execution_lab.validation_service import backlog_growth, certification_passed


def test_live_snapshot_does_not_finalize_or_invent_open_transition():
    fixture = generated_scenarios()[1]
    engine = ExecutionEngine(fixture.order)
    before = engine.snapshot()
    assert before.state == "submitted"
    engine.process(fixture.events[0])
    assert engine.snapshot().state == "partially_filled"
    next_quote = fixture.events[0].model_copy(update={"event_id": "new", "sequence": 2, "event_time": fixture.events[0].event_time + timedelta(milliseconds=1)})
    engine.process(next_quote)
    assert engine.snapshot().state == "filled"
    assert engine.finalize() == engine.snapshot()


def test_zero_metrics_round_trip_without_becoming_missing():
    result = simulate(generated_scenarios()[0].order, generated_scenarios()[0].events)
    result = replace(result, metrics=replace(result.metrics, spread_cost=Decimal("0"), latency_impact=Decimal("0"), time_to_first_fill=timedelta(0), time_to_completion=timedelta(0)))
    payload = _result_payload(result)
    assert payload["metrics"]["spread_cost"] == "0"
    assert payload["metrics"]["latency_impact"] == "0"
    assert payload["metrics"]["time_to_completion_ms"] == 0
    assert _result_from_payload(payload) == result
    assert _milliseconds(timedelta(0)) == 0


def test_stale_event_does_not_rewind_cached_freshness():
    from market_execution_lab.streaming import cache_market_state
    fixture = generated_scenarios()[0]
    engine = ExecutionEngine(fixture.order)
    engine.process(fixture.events[0])
    stale = fixture.events[0].model_copy(update={"event_id": "stale", "event_time": fixture.order.submitted_at, "sequence": 0})
    engine.process(stale)
    redis = Mock()
    cache_market_state(redis, "state", engine, stale)
    redis.hset.assert_not_called()


def test_long_replays_are_versioned_bounded_ordered_and_deterministic():
    experiments = replay_scenarios()[6:]
    assert len(experiments) == 3
    assert all(item.name.endswith("_v1") and len(item.events) == 300 for item in experiments)
    assert replay_scenarios() == replay_scenarios()
    for fixture in experiments:
        assert list(fixture.events) == sorted(fixture.events, key=lambda event: (event.event_time, event.sequence))
        assert len({event.event_id for event in fixture.events}) == len(fixture.events)
        assert simulate(fixture.order, fixture.events) == simulate(fixture.order, fixture.events)


def test_long_replay_size_and_latency_change_explainable_results():
    fixture = replay_scenarios()[7]
    small = simulate(fixture.order.model_copy(update={"quantity": 10, "latency_ms": 0}), fixture.events)
    delayed = simulate(fixture.order.model_copy(update={"quantity": 10, "latency_ms": 250}), fixture.events)
    large = simulate(fixture.order.model_copy(update={"quantity": 100_000}), fixture.events)
    assert small.metrics.average_fill_price != delayed.metrics.average_fill_price
    assert large.remaining_quantity > 0
    assert large.metrics.fill_rate < small.metrics.fill_rate


def test_paced_publish_waits_before_delivery_and_dispatches_only_when_complete(monkeypatch):
    fixture = generated_scenarios()[4]
    redis = Mock()
    redis.xlen.return_value = 0
    clock = [0.0]
    monkeypatch.setattr(pipeline, "monotonic", lambda: clock[0])
    waits = []

    def wait(seconds):
        waits.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(pipeline, "sleep", wait)
    publish_replay(redis, fixture, playback_speed=1)
    assert waits == [pytest.approx(0.075)]
    assert redis.xadd.call_args_list[-1].args[0] == pipeline.ENGINE_JOB_STREAM
    messages = [call.args[1].get("message_type") for call in redis.xadd.call_args_list]
    assert messages.index("replay.completed.v1") < len(messages) - 1


@pytest.mark.parametrize("options", [{"playback_speed": 0}, {"events_per_second": -1}, {"playback_speed": 1, "events_per_second": 10}])
def test_invalid_pacing_is_rejected_before_publishing(options):
    redis = Mock()
    with pytest.raises(ValueError):
        publish_replay(redis, generated_scenarios()[0], **options)
    assert not redis.mock_calls


def test_continuous_certification_rejects_growth_losses_duplicates_and_low_offered_rate():
    assert certification_passed(100, 100, 0, 0, 1000, 1000, 0, [])
    assert not certification_passed(100, 80, 0, 0, 1000, 1000, 0, [])
    assert not certification_passed(100, 100, 0, 20, 1000, 1000, 0, [])
    assert not certification_passed(100, 100, 1, 0, 1000, 1000, 0, [])
    assert not certification_passed(100, 100, 0, 0, 1000, 999, 0, [])
    assert not certification_passed(100, 100, 0, 0, 1000, 1000, 1, [])
    assert not certification_passed(100, 100, 0, 0, 1000, 1000, 0, ["RedisError"])
    samples = [{"seconds": index, "engine": index * 10, "persistence": 0, "results": 0} for index in range(20)]
    assert backlog_growth(samples) == pytest.approx(10)


def test_public_live_endpoints_fail_before_touching_dependencies(monkeypatch):
    monkeypatch.setenv("APP_MODE", "public_replay")
    monkeypatch.delenv("FLY_WORKER_LIFECYCLE", raising=False)
    app = api_service.create_app("postgresql+psycopg://unused", "redis://unused")
    app.state.public_request_limit = None
    redis = Mock()
    app.state.redis.hgetall = redis
    with TestClient(app) as client:
        assert client.get("/api/v1/live/session").status_code == 404
        assert client.put("/api/v1/live/symbols", json={"name": "Private", "symbols": ["AAPL"]}).status_code == 404
        assert client.post("/api/v1/orders", json={"run_id": str(uuid4()), "symbol": "AAPL", "side": "buy", "order_type": "market", "quantity": 1}).status_code == 422
    redis.assert_not_called()


@pytest.mark.parametrize("symbols", [["AAPL"] * 11, ["BAD SYMBOL"]])
def test_live_watchlist_validation_rejects_oversized_or_invalid_symbols(symbols):
    with pytest.raises(ValueError):
        api_service.WatchlistCommandRequest(name="Private", symbols=symbols)


def test_private_mode_cannot_use_public_fly_dispatch(monkeypatch):
    monkeypatch.setenv("APP_MODE", "private_live")
    monkeypatch.setenv("FLY_WORKER_LIFECYCLE", "on")
    monkeypatch.setattr(api_service, "FlyWorkers", Mock())
    with pytest.raises(ValueError, match="private live services"):
        api_service.create_app("postgresql+psycopg://unused", "redis://unused")


def test_alpaca_authentication_rejection_does_not_subscribe_or_publish(monkeypatch):
    from market_execution_lab.alpaca import AlpacaSettings, stream_alpaca
    import websockets.asyncio.client

    class Socket:
        def __init__(self):
            self.sent = []

        async def send(self, payload):
            self.sent.append(payload)

        async def recv(self):
            return '[{"T":"error","code":402,"msg":"secret"}]'

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    socket = Socket()
    monkeypatch.setattr(websockets.asyncio.client, "connect", lambda *args, **kwargs: socket)

    async def publish(event):
        raise AssertionError("must not publish unauthenticated events")

    with pytest.raises(PermissionError, match="authentication rejected"):
        asyncio.run(stream_alpaca(AlpacaSettings("key", "secret", ("AAPL",)), uuid4(), lambda symbol: 0, publish))
    assert len(socket.sent) == 1


def test_alpaca_reconnect_skips_malformed_quotes_and_cleans_up_subscription_task(monkeypatch):
    import json
    from market_execution_lab.alpaca import AlpacaSettings, stream_alpaca
    import websockets.asyncio.client

    fixtures = generated_scenarios()
    quote = fixtures[0].events[0]
    messages = [
        '[{"T":"success","msg":"authenticated"}]',
        json.dumps([{"T": "q", "S": "AAPL", "bp": 200, "ap": 199, "bs": 1, "as": 1, "t": quote.event_time.isoformat()},
                    {"T": "q", "S": "AAPL", "bp": 199.98, "ap": 200, "bs": 2, "as": 1, "t": quote.event_time.isoformat()}]),
    ]
    statuses, received, waits = [], [], []
    closed = []

    class Socket:
        def __init__(self, disconnect):
            self.disconnect = disconnect

        async def send(self, payload):
            pass

        async def recv(self):
            if self.disconnect:
                self.disconnect = False
                raise OSError("connection lost")
            return messages.pop(0)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    sockets = iter([Socket(True), Socket(False)])
    monkeypatch.setattr(websockets.asyncio.client, "connect", lambda *args, **kwargs: next(sockets))

    async def backoff(seconds):
        waits.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", backoff)

    async def updates():
        try:
            await asyncio.Event().wait()
            yield ("AAPL",)
        finally:
            closed.append(True)

    async def status(value):
        statuses.append(value)

    async def publish(event):
        received.append(event)
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(stream_alpaca(AlpacaSettings("key", "secret", ("AAPL",)), uuid4(), lambda symbol: 0, publish, updates(), status))
    assert statuses == ["connecting", "disconnected", "connecting", "connected"]
    assert waits == [1]
    assert len(received) == 1 and received[0].ask_size == 100
    assert closed == [True]
