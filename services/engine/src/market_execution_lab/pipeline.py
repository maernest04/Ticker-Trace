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
from market_execution_lab.observability import log_event, now_seconds, record_metrics
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
RECOVERY_IDLE_MS = 1_000
MAX_DELIVERIES = 3
MAX_QUEUE_DEPTH = 10_000


class BackpressureError(RuntimeError):
    pass


def partition_for_symbol(symbol: str) -> int:
    return zlib.crc32(symbol.encode()) % PARTITION_COUNT


def result_stream_name(run_id: str, partition: int) -> str:
    return f"execution.results:{run_id}:{partition}"


def dead_letter_stream_name(run_id: str, partition: int) -> str:
    return f"pipeline.dead-letter:{run_id}:{partition}"


def publish_replay(redis: Redis, scenario: ScenarioFixture, max_queue_depth: int = MAX_QUEUE_DEPTH) -> int:
    partition = partition_for_symbol(scenario.order.symbol)
    stream = partition_stream_name(str(scenario.order.run_id), partition)
    events = tuple(event.model_copy(update={"partition": partition}) for event in scenario.events)
    if redis.xlen(stream) + len(events) + 3 > max_queue_depth:
        raise BackpressureError(f"replay would exceed the queue depth limit of {max_queue_depth}")
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
    record_metrics(
        redis,
        str(scenario.order.run_id),
        partition,
        published_at=now_seconds(),
        published_messages=len(events) + 3,
    )
    log_event("replay_published", run_id=str(scenario.order.run_id), partition=partition, event_count=len(events))
    return partition


def run_engine(
    redis: Redis,
    run_id: UUID,
    partition: int,
    consumer: str,
    recovery_idle_ms: int = RECOVERY_IDLE_MS,
    max_deliveries: int = MAX_DELIVERIES,
) -> ExecutionResult:
    started_at = now_seconds()
    stream = partition_stream_name(str(run_id), partition)
    _ensure_group(redis, stream, ENGINE_GROUP)
    claimed, exhausted = _claim_pending(redis, stream, ENGINE_GROUP, consumer, recovery_idle_ms, max_deliveries)
    for entry_id, message in exhausted:
        _dead_letter(redis, run_id, partition, stream, ENGINE_GROUP, entry_id, message, "delivery limit reached")
        redis.xack(stream, ENGINE_GROUP, entry_id)

    delivered = claimed + _read_group(redis, stream, ENGINE_GROUP, consumer)
    engine: ExecutionEngine | None = None
    events: list[MarketEvent] = []
    result: ExecutionResult | None = None
    exhausted_ids = {entry_id for entry_id, _ in exhausted}
    for entry_id, message in redis.xrange(stream):
        if entry_id in exhausted_ids:
            continue
        try:
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
                result = replace(streamed_result, metrics=expected_result.metrics)
                if result != expected_result:
                    raise AssertionError("streamed execution did not match pure execution")
            elif message_type != "replay.started.v1":
                raise ValueError("unsupported source message type")
        except (KeyError, RuntimeError, TypeError, ValueError) as error:
            _dead_letter(redis, run_id, partition, stream, ENGINE_GROUP, entry_id, message, str(error))

    if engine is None or result is None:
        raise RuntimeError("replay stream did not produce an execution result")

    cache_order_state(redis, order_state_key(str(run_id), str(engine.order.order_id)), result)
    redis.xadd(
        result_stream_name(str(run_id), partition),
        {
            "message_type": "execution.result.v1",
            "order": json.dumps(engine.order.model_dump(mode="json")),
            "result": json.dumps(_result_payload(result)),
        },
    )
    if delivered:
        redis.xack(stream, ENGINE_GROUP, *(entry_id for entry_id, _ in delivered))
    completed_at = now_seconds()
    record_metrics(
        redis,
        str(run_id),
        partition,
        engine_completed_at=completed_at,
        engine_processing_ms=(completed_at - started_at) * 1_000,
        processed_events=result.processed_events,
    )
    log_event(
        "engine_completed",
        run_id=str(run_id),
        partition=partition,
        processed_events=result.processed_events,
        state=result.state.value,
    )
    return result


def run_persistence(
    redis: Redis,
    store: DatabaseStore,
    run_id: UUID,
    partition: int,
    consumer: str,
    recovery_idle_ms: int = RECOVERY_IDLE_MS,
    max_deliveries: int = MAX_DELIVERIES,
) -> None:
    started_at = now_seconds()
    stream = partition_stream_name(str(run_id), partition)
    _ensure_group(redis, stream, PERSISTENCE_GROUP)
    _persist_source_messages(
        redis,
        store,
        run_id,
        partition,
        stream,
        _claim_pending(redis, stream, PERSISTENCE_GROUP, consumer, recovery_idle_ms, max_deliveries),
        _read_group(redis, stream, PERSISTENCE_GROUP, consumer),
    )

    results = result_stream_name(str(run_id), partition)
    _ensure_group(redis, results, PERSISTENCE_GROUP)
    claimed, exhausted = _claim_pending(redis, results, PERSISTENCE_GROUP, consumer, recovery_idle_ms, max_deliveries)
    for entry_id, message in exhausted:
        _dead_letter(redis, run_id, partition, results, PERSISTENCE_GROUP, entry_id, message, "delivery limit reached")
        redis.xack(results, PERSISTENCE_GROUP, entry_id)
    for entry_id, message in claimed + _read_group(redis, results, PERSISTENCE_GROUP, consumer):
        try:
            if message["message_type"] != "execution.result.v1":
                raise ValueError("unsupported result message type")
            order = OrderCommand.model_validate_json(message["order"])
            store.complete_run(order, _result_from_payload(json.loads(message["result"])), datetime.now(UTC))
            redis.xack(results, PERSISTENCE_GROUP, entry_id)
            completed_at = now_seconds()
            record_metrics(
                redis,
                str(run_id),
                partition,
                persistence_completed_at=completed_at,
                persistence_processing_ms=(completed_at - started_at) * 1_000,
            )
            log_event("persistence_completed", run_id=str(run_id), partition=partition)
            return
        except (KeyError, TypeError, ValueError) as error:
            _dead_letter(redis, run_id, partition, results, PERSISTENCE_GROUP, entry_id, message, str(error))
            redis.xack(results, PERSISTENCE_GROUP, entry_id)
    raise RuntimeError("result stream did not include an execution result")


def _ensure_group(redis: Redis, stream: str, group: str) -> None:
    try:
        redis.xgroup_create(stream, group, id="0-0", mkstream=True)
    except ResponseError as error:
        if "BUSYGROUP" not in str(error):
            raise


def _read_group(redis: Redis, stream: str, group: str, consumer: str) -> list[tuple[str, dict[str, str]]]:
    entries: list[tuple[str, dict[str, str]]] = []
    while response := redis.xreadgroup(group, consumer, {stream: ">"}, count=100):
        for _, batch in response:
            entries.extend(batch)
    return entries


def _claim_pending(
    redis: Redis,
    stream: str,
    group: str,
    consumer: str,
    recovery_idle_ms: int,
    max_deliveries: int,
) -> tuple[list[tuple[str, dict[str, str]]], list[tuple[str, dict[str, str]]]]:
    claimed: list[tuple[str, dict[str, str]]] = []
    exhausted: list[tuple[str, dict[str, str]]] = []
    for pending in redis.xpending_range(stream, group, "-", "+", 100):
        if pending["time_since_delivered"] < recovery_idle_ms:
            continue
        messages = redis.xclaim(stream, group, consumer, recovery_idle_ms, [pending["message_id"]])
        if pending["times_delivered"] >= max_deliveries:
            exhausted.extend(messages)
        else:
            claimed.extend(messages)
    return claimed, exhausted


def _persist_source_messages(
    redis: Redis,
    store: DatabaseStore,
    run_id: UUID,
    partition: int,
    stream: str,
    pending: tuple[list[tuple[str, dict[str, str]]], list[tuple[str, dict[str, str]]]],
    fresh: list[tuple[str, dict[str, str]]],
) -> None:
    claimed, exhausted = pending
    for entry_id, message in exhausted:
        _dead_letter(redis, run_id, partition, stream, PERSISTENCE_GROUP, entry_id, message, "delivery limit reached")
        redis.xack(stream, PERSISTENCE_GROUP, entry_id)
    for entry_id, message in claimed + fresh:
        try:
            message_type = message["message_type"]
            if message_type == "replay.started.v1":
                payload = json.loads(message["payload"])
                store.create_run(run_id, payload["scenario_name"], datetime.fromisoformat(payload["started_at"]))
            elif message_type == "order.command.v1":
                store.record_order(OrderCommand.model_validate_json(message["payload"]))
            elif message_type == "market.event.v1":
                store.record_event(event_from_json(message["payload"]))
            elif message_type not in {"replay.completed.v1"}:
                raise ValueError("unsupported source message type")
        except (KeyError, TypeError, ValueError) as error:
            _dead_letter(redis, run_id, partition, stream, PERSISTENCE_GROUP, entry_id, message, str(error))
        redis.xack(stream, PERSISTENCE_GROUP, entry_id)


def _dead_letter(
    redis: Redis,
    run_id: UUID,
    partition: int,
    source_stream: str,
    group: str,
    entry_id: str,
    message: dict[str, str],
    reason: str,
) -> None:
    redis.xadd(
        dead_letter_stream_name(str(run_id), partition),
        {
            "source_stream": source_stream,
            "group": group,
            "entry_id": entry_id,
            "reason": reason,
            "message": json.dumps(message),
        },
    )


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
