import json
from dataclasses import replace
from datetime import datetime
from typing import Iterable

from redis import Redis

from market_execution_lab.engine import ExecutionEngine, ExecutionResult, simulate
from market_execution_lab.models import MarketEvent, OrderCommand, QuoteEvent, TradeEvent
from market_execution_lab.storage import DatabaseStore


def partition_stream_name(run_id: str, partition: int) -> str:
    return f"market.events:{run_id}:{partition}"


def market_state_key(run_id: str, symbol: str) -> str:
    return f"state:replay:{run_id}:{symbol}"


def order_state_key(run_id: str, order_id: str) -> str:
    return f"order:replay:{run_id}:{order_id}"


class RedisReplayRunner:
    def __init__(self, redis: Redis, store: DatabaseStore) -> None:
        self._redis = redis
        self._store = store

    def run(self, scenario_name: str, order: OrderCommand, events: Iterable[MarketEvent]) -> ExecutionResult:
        replay_events = tuple(events)
        if not replay_events:
            raise ValueError("replay requires at least one event")

        partitions = {event.partition for event in replay_events}
        if len(partitions) != 1:
            raise ValueError("a single replay runner handles one partition")

        partition = next(iter(partitions))
        stream = partition_stream_name(str(order.run_id), partition)
        market_key = market_state_key(str(order.run_id), order.symbol)
        order_key = order_state_key(str(order.run_id), str(order.order_id))
        self._redis.delete(stream, market_key, order_key)
        self._store.create_run(order.run_id, scenario_name, order.submitted_at)
        self._store.record_order(order)

        for event in replay_events:
            self._redis.xadd(stream, {"event": json.dumps(event.model_dump(mode="json"))})

        engine = ExecutionEngine(order)
        next_id = "0-0"
        processed_entries = 0
        while processed_entries < len(replay_events):
            response = self._redis.xread({stream: next_id}, count=len(replay_events) - processed_entries)
            if not response:
                raise RuntimeError("stream ended before all replay events were read")
            for _, entries in response:
                for entry_id, payload in entries:
                    event = _event_from_json(payload["event"])
                    engine.process(event)
                    self._cache_market_state(market_key, engine, event)
                    self._store.record_event(event)
                    next_id = entry_id
                    processed_entries += 1

        streamed_result = engine.finalize()
        expected_result = simulate(order, replay_events)
        if (
            streamed_result.state != expected_result.state
            or streamed_result.remaining_quantity != expected_result.remaining_quantity
            or streamed_result.fills != expected_result.fills
            or streamed_result.transitions != expected_result.transitions
            or streamed_result.processed_events != expected_result.processed_events
            or streamed_result.duplicate_events != expected_result.duplicate_events
            or streamed_result.stale_events != expected_result.stale_events
        ):
            raise AssertionError("streamed execution did not match pure execution")

        result = replace(streamed_result, metrics=expected_result.metrics)
        self._cache_order_state(order_key, result)
        self._store.complete_run(order, result, datetime.now(order.submitted_at.tzinfo))
        return result

    def _cache_market_state(self, key: str, engine: ExecutionEngine, event: MarketEvent) -> None:
        state = engine.market_state
        mapping = {
            "event_id": event.event_id,
            "event_time": event.event_time.isoformat(),
            "sequence": str(event.sequence),
            "last_trade_price": str(state.last_trade_price) if state.last_trade_price is not None else "",
            "bid_price": str(state.bid_price) if state.bid_price is not None else "",
            "bid_size": str(state.bid_size) if state.bid_size is not None else "",
            "ask_price": str(state.ask_price) if state.ask_price is not None else "",
            "ask_size": str(state.ask_size) if state.ask_size is not None else "",
        }
        self._redis.hset(key, mapping=mapping)

    def _cache_order_state(self, key: str, result: ExecutionResult) -> None:
        self._redis.hset(
            key,
            mapping={
                "state": result.state.value,
                "remaining_quantity": str(result.remaining_quantity),
                "fill_count": str(len(result.fills)),
            },
        )


def _event_from_json(raw_event: str) -> MarketEvent:
    payload = json.loads(raw_event)
    if payload["event_type"] == "market.quote.v1":
        return QuoteEvent.model_validate(payload)
    if payload["event_type"] == "market.trade.v1":
        return TradeEvent.model_validate(payload)
    raise ValueError("unsupported market event type")
