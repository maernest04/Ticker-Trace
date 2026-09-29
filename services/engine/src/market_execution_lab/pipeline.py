import json
import zlib
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from redis import Redis
from redis.exceptions import ResponseError

from market_execution_lab.engine import ExecutionEngine, ExecutionMetrics, ExecutionResult, simulate
from market_execution_lab.fixtures import ScenarioFixture
from market_execution_lab.models import Fill, MarketEvent, OrderCommand, OrderState, OrderStateChange
from market_execution_lab.storage import DatabaseStore
from market_execution_lab.streaming import (
    cache_market_state,
    cache_order_state,
    event_from_json,
    market_state_key,
    order_state_key,
    partition_stream_name,
)


PARTITION_COUNT = 16
ENGINE_GROUP = "engine"
PERSISTENCE_GROUP = "persistence"


def partition_for_symbol(symbol: str) -> int:
    return zlib.crc32(symbol.encode()) % PARTITION_COUNT


def result_stream_name(run_id: str, partition: int) -> str:
    return f"execution.results:{run_id}:{partition}"


def publish_replay(redis: Redis, scenario: ScenarioFixture) -> int:
    partition = partition_for_symbol(scenario.order.symbol)
    stream = partition_stream_name(str(scenario.order.run_id), partition)
    events = tuple(event.model_copy(update={"partition": partition}) for event in scenario.events)
    redis.delete(stream, result_stream_name(str(scenario.order.run_id), partition))
    redis.xadd(
        stream,
        {
            "message_type": "replay.started.v1",
            "payload": json.dumps({"scenario_name": scenario.name, "started_at": scenario.order.submitted_at.isoformat()}),
        },
    )
    redis.xadd(
        stream,
        {"message_type": "order.command.v1", "payload": json.dumps(scenario.order.model_dump(mode="json"))},
    )
    for event in events:
        redis.xadd(stream, {"message_type": "market.event.v1", "payload": json.dumps(event.model_dump(mode="json"))})
    redis.xadd(stream, {"message_type": "replay.completed.v1", "payload": "{}"})
    return partition


def run_engine(redis: Redis, run_id: UUID, partition: int, consumer: str) -> ExecutionResult:
    stream = partition_stream_name(str(run_id), partition)
    _ensure_group(redis, stream, ENGINE_GROUP)
    engine: ExecutionEngine | None = None
    events: list[MarketEvent] = []
    for entry_id, message in _read_group(redis, stream, ENGINE_GROUP, consumer):
        message_type = message["message_type"]
        if message_type == "order.command.v1":
            engine = ExecutionEngine(OrderCommand.model_validate_json(message["payload"]))
        elif message_type == "market.event.v1":
            if engine is None:
                raise ValueError("market event arrived before its order command")
            event = event_from_json(message["payload"])
            engine.process(event)
            events.append(event)
            cache_market_state(redis, market_state_key(str(run_id), event.symbol), engine, event)
        elif message_type == "replay.completed.v1":
            if engine is None:
                raise ValueError("replay completed without an order command")
            streamed_result = engine.finalize()
            expected_result = simulate(engine.order, events)
            if streamed_result != expected_result and replace(streamed_result, metrics=expected_result.metrics) != expected_result:
                raise AssertionError("streamed execution did not match pure execution")
            result = replace(streamed_result, metrics=expected_result.metrics)
            cache_order_state(redis, order_state_key(str(run_id), str(engine.order.order_id)), result)
            redis.xadd(
                result_stream_name(str(run_id), partition),
                {
                    "message_type": "execution.result.v1",
                    "order": json.dumps(engine.order.model_dump(mode="json")),
                    "result": json.dumps(_result_payload(result)),
                },
            )
            redis.xack(stream, ENGINE_GROUP, entry_id)
            return result
        redis.xack(stream, ENGINE_GROUP, entry_id)
    raise RuntimeError("replay stream did not include a completion message")


def run_persistence(redis: Redis, store: DatabaseStore, run_id: UUID, partition: int, consumer: str) -> None:
    stream = partition_stream_name(str(run_id), partition)
    _ensure_group(redis, stream, PERSISTENCE_GROUP)
    for entry_id, message in _read_group(redis, stream, PERSISTENCE_GROUP, consumer):
        message_type = message["message_type"]
        if message_type == "replay.started.v1":
            payload = json.loads(message["payload"])
            store.create_run(run_id, payload["scenario_name"], datetime.fromisoformat(payload["started_at"]))
        elif message_type == "order.command.v1":
            store.record_order(OrderCommand.model_validate_json(message["payload"]))
        elif message_type == "market.event.v1":
            store.record_event(event_from_json(message["payload"]))
        redis.xack(stream, PERSISTENCE_GROUP, entry_id)

    results = result_stream_name(str(run_id), partition)
    _ensure_group(redis, results, PERSISTENCE_GROUP)
    for entry_id, message in _read_group(redis, results, PERSISTENCE_GROUP, consumer):
        if message["message_type"] == "execution.result.v1":
            order = OrderCommand.model_validate_json(message["order"])
            store.complete_run(order, _result_from_payload(json.loads(message["result"])), datetime.now(UTC))
            redis.xack(results, PERSISTENCE_GROUP, entry_id)
            return
        redis.xack(results, PERSISTENCE_GROUP, entry_id)
    raise RuntimeError("result stream did not include an execution result")


def _ensure_group(redis: Redis, stream: str, group: str) -> None:
    try:
        redis.xgroup_create(stream, group, id="0-0", mkstream=True)
    except ResponseError as error:
        if "BUSYGROUP" not in str(error):
            raise


def _read_group(redis: Redis, stream: str, group: str, consumer: str):
    while response := redis.xreadgroup(group, consumer, {stream: ">"}, count=100):
        for _, entries in response:
            yield from entries


def _result_payload(result: ExecutionResult) -> dict[str, object]:
    return {
        "state": result.state.value,
        "remaining_quantity": result.remaining_quantity,
        "fills": [fill.model_dump(mode="json") for fill in result.fills],
        "transitions": [transition.model_dump(mode="json") for transition in result.transitions],
        "metrics": {
            "average_fill_price": str(result.metrics.average_fill_price) if result.metrics.average_fill_price else None,
            "fill_rate": str(result.metrics.fill_rate),
            "spread_cost": str(result.metrics.spread_cost) if result.metrics.spread_cost else None,
            "time_to_first_fill_ms": _milliseconds(result.metrics.time_to_first_fill),
            "time_to_completion_ms": _milliseconds(result.metrics.time_to_completion),
            "latency_impact": str(result.metrics.latency_impact) if result.metrics.latency_impact else None,
        },
        "processed_events": result.processed_events,
        "duplicate_events": result.duplicate_events,
        "stale_events": result.stale_events,
    }


def _result_from_payload(payload: dict[str, object]) -> ExecutionResult:
    metrics = payload["metrics"]
    return ExecutionResult(
        state=OrderState(payload["state"]),
        remaining_quantity=payload["remaining_quantity"],
        fills=tuple(Fill.model_validate(fill) for fill in payload["fills"]),
        transitions=tuple(OrderStateChange.model_validate(transition) for transition in payload["transitions"]),
        metrics=ExecutionMetrics(
            average_fill_price=Decimal(metrics["average_fill_price"]) if metrics["average_fill_price"] else None,
            fill_rate=Decimal(metrics["fill_rate"]),
            spread_cost=Decimal(metrics["spread_cost"]) if metrics["spread_cost"] else None,
            time_to_first_fill=_timedelta_from_milliseconds(metrics["time_to_first_fill_ms"]),
            time_to_completion=_timedelta_from_milliseconds(metrics["time_to_completion_ms"]),
            latency_impact=Decimal(metrics["latency_impact"]) if metrics["latency_impact"] else None,
        ),
        processed_events=payload["processed_events"],
        duplicate_events=payload["duplicate_events"],
        stale_events=payload["stale_events"],
    )


def _milliseconds(value):
    return int(value.total_seconds() * 1000) if value else None


def _timedelta_from_milliseconds(value):
    if value is None:
        return None
    from datetime import timedelta

    return timedelta(milliseconds=value)
