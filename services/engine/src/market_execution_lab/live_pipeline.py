import argparse
import json
import os
import signal
from threading import Event
from datetime import UTC, datetime
from time import sleep, time
from uuid import UUID, uuid4

import sqlalchemy as sa
from redis import Redis

from market_execution_lab.alpaca import require_private_live_mode
from market_execution_lab.engine import ExecutionEngine, MarketState, _apply_event_to_market_state
from market_execution_lab.models import OrderCommand, OrderState
from market_execution_lab.pipeline import (
    ENGINE_GROUP, PERSISTENCE_GROUP, PARTITION_COUNT, _ensure_group, _claim_pending,
    _dead_letter, _result_payload, _result_from_payload, result_stream_name,
)
from market_execution_lab.storage import DatabaseStore, sqlalchemy_url
from market_execution_lab.streaming import cache_market_state, cache_order_state, event_from_json, market_state_key, order_state_key, partition_stream_name
from market_execution_lab.observability import configure_logging, log_event
from market_execution_lab.live_ownership import LiveOwnership


LIVE_SESSION_KEY = "live:session"
LIVE_MAX_MESSAGES = 100_000
LIVE_MAX_BACKLOG = 10_000
LIVE_STALE_SECONDS = 15
LIVE_MAX_ORDERS = 256


def initialize_session(redis: Redis, run_id: UUID, symbols: list[str], register: bool = True, scenario_name: str | None = None) -> None:
    started_at = datetime.now(UTC).isoformat()
    for partition in range(PARTITION_COUNT):
        stream = partition_stream_name(str(run_id), partition)
        _ensure_group(redis, stream, ENGINE_GROUP)
        _ensure_group(redis, stream, PERSISTENCE_GROUP)
        _ensure_group(redis, result_stream_name(str(run_id), partition), PERSISTENCE_GROUP)
        if redis.xlen(stream) == 0:
            redis.xadd(stream, {"message_type": "replay.started.v1", "payload": json.dumps({
                "scenario_name": scenario_name or ("private_live" if register else "continuous_benchmark_v1"), "started_at": started_at,
                "mode": "private_live", "symbols": symbols,
            })})
    if register:
        redis.hset(LIVE_SESSION_KEY, mapping={"run_id": str(run_id), "symbols": json.dumps(symbols), "status": "connecting", "phase": "running", "started_at": str(time()), "received_at": "0"})


def live_backlog(redis: Redis, stream: str) -> int:
    return max((int(group.get("lag") or 0) + int(group["pending"]) for group in redis.xinfo_groups(stream)), default=0)


def append_live(redis, stream: str, message: dict[str, str]):
    return redis.eval(
        "if redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[1]) then return false end; "
        "return redis.call('XADD', KEYS[1], '*', 'message_type', ARGV[2], 'payload', ARGV[3])",
        1, stream, LIVE_MAX_MESSAGES, message["message_type"], message["payload"],
    )


class LiveEngine:
    def __init__(self, redis: Redis, run_id: UUID, partition: int, check_owner=None):
        self.redis, self.run_id, self.partition = redis, run_id, partition
        self.check_owner = check_owner
        self.stream = partition_stream_name(str(run_id), partition)
        self.engines: dict[UUID, ExecutionEngine] = {}
        self.markets: dict[str, MarketState] = {}
        _ensure_group(redis, self.stream, ENGINE_GROUP)
        group = next(group for group in redis.xinfo_groups(self.stream) if group["name"] == ENGINE_GROUP)
        cursor = "-"
        while True:
            if self.check_owner:
                self.check_owner(force=True)
            entries = redis.xrange(self.stream, min=cursor, max=group["last-delivered-id"], count=100)
            if not entries:
                break
            for entry_id, message in entries:
                try:
                    self.process(message)
                except (KeyError, TypeError, ValueError):
                    pass
            cursor = f"({entries[-1][0]}"

    def process(self, message: dict[str, str]) -> set[UUID]:
        if self.check_owner:
            self.check_owner()
        changed = set()
        if message["message_type"] == "order.command.v1":
            order = OrderCommand.model_validate_json(message["payload"])
            if order.run_id != self.run_id:
                raise ValueError("order belongs to another live session")
            if order.order_id not in self.engines:
                self.engines[order.order_id] = ExecutionEngine(order)
                changed.add(order.order_id)
        elif message["message_type"] == "market.event.v1":
            event = event_from_json(message["payload"])
            if event.run_id != self.run_id or event.partition != self.partition:
                raise ValueError("event belongs to another live session or partition")
            state = self.markets.setdefault(event.symbol, MarketState())
            if state.last_event_time is None or (event.event_time, event.sequence) > (state.last_event_time, state.last_event_sequence):
                _apply_event_to_market_state(state, event)
                cache_market_state(self.redis, market_state_key(str(self.run_id), event.symbol), state, event)
            for order_id, engine in self.engines.items():
                if engine.order.symbol != event.symbol or engine.state in {OrderState.FILLED, OrderState.CANCELLED}:
                    continue
                previous = (engine.state, engine.remaining_quantity)
                engine.process(event)
                if previous != (engine.state, engine.remaining_quantity):
                    changed.add(order_id)
        elif message["message_type"] == "session.closed.v1":
            payload = json.loads(message["payload"])
            if UUID(payload["run_id"]) != self.run_id:
                raise ValueError("closure belongs to another live session")
            for order_id, engine in self.engines.items():
                if engine.state not in {OrderState.FILLED, OrderState.CANCELLED}:
                    engine.cancel(datetime.fromisoformat(payload["closed_at"]), payload["reason"])
                    changed.add(order_id)
        elif message["message_type"] != "replay.started.v1":
            raise ValueError("unsupported live message type")
        return changed

    def step(self, consumer: str, block_ms: int = 100) -> int:
        if self.check_owner:
            self.check_owner(force=True)
        claimed, exhausted = _claim_pending(self.redis, self.stream, ENGINE_GROUP, consumer, 1000, 3)
        for entry_id, message in exhausted:
            _dead_letter(self.redis, self.run_id, self.partition, self.stream, ENGINE_GROUP, entry_id, message, "delivery limit reached")
            self.redis.xack(self.stream, ENGINE_GROUP, entry_id)
        response = self.redis.xreadgroup(ENGINE_GROUP, consumer, {self.stream: ">"}, count=100, block=block_ms)
        entries = claimed + [entry for _, batch in response for entry in batch]
        changed = set(self.engines) if claimed else set()
        for entry_id, message in entries:
            try:
                changed.update(self.process(message))
            except (KeyError, TypeError, ValueError) as error:
                _dead_letter(self.redis, self.run_id, self.partition, self.stream, ENGINE_GROUP, entry_id, message, str(error))
        for order_id in changed:
            if self.check_owner:
                self.check_owner()
            engine = self.engines[order_id]
            result = engine.snapshot()
            cache_order_state(self.redis, order_state_key(str(self.run_id), str(order_id)), result)
            self.redis.xadd(result_stream_name(str(self.run_id), self.partition), {
                "message_type": "execution.result.v1", "order": engine.order.model_dump_json(),
                "result": json.dumps(_result_payload(result)),
            })
        if entries:
            if self.check_owner:
                self.check_owner(force=True)
            self.redis.xack(self.stream, ENGINE_GROUP, *(entry_id for entry_id, _ in entries))
        return len(entries)


def persist_live_batch(redis: Redis, store: DatabaseStore, run_id: UUID, partition: int, consumer: str, block_ms: int = 100, check_owner=None) -> list[tuple[str, float]]:
    if check_owner:
        check_owner(force=True)
    stream = partition_stream_name(str(run_id), partition)
    _ensure_group(redis, stream, PERSISTENCE_GROUP)
    claimed, exhausted = _claim_pending(redis, stream, PERSISTENCE_GROUP, consumer, 1000, 3)
    response = redis.xreadgroup(PERSISTENCE_GROUP, consumer, {stream: ">"}, count=100, block=block_ms)
    entries = claimed + [entry for _, batch in response for entry in batch]
    events = []
    for entry_id, message in exhausted:
        _dead_letter(redis, run_id, partition, stream, PERSISTENCE_GROUP, entry_id, message, "delivery limit reached")
        redis.xack(stream, PERSISTENCE_GROUP, entry_id)
    for entry_id, message in entries:
        if check_owner:
            check_owner()
        try:
            if message["message_type"] == "replay.started.v1":
                payload = json.loads(message["payload"])
                store.create_run(run_id, payload["scenario_name"], datetime.fromisoformat(payload["started_at"]))
                store.record_replay_settings(run_id, "private_live", payload["symbols"], overwrite=False)
            elif message["message_type"] == "order.command.v1":
                store.record_order(OrderCommand.model_validate_json(message["payload"]))
            elif message["message_type"] == "market.event.v1":
                events.append(event_from_json(message["payload"]))
            elif message["message_type"] != "session.closed.v1":
                raise ValueError("unsupported live source message")
        except (KeyError, TypeError, ValueError) as error:
            _dead_letter(redis, run_id, partition, stream, PERSISTENCE_GROUP, entry_id, message, str(error))
    if check_owner:
        check_owner(force=True)
    store.record_events(events)
    persisted_at = time()
    if entries:
        if check_owner:
            check_owner(force=True)
        redis.xack(stream, PERSISTENCE_GROUP, *(entry_id for entry_id, _ in entries))
    results = result_stream_name(str(run_id), partition)
    _ensure_group(redis, results, PERSISTENCE_GROUP)
    pending, exhausted_results = _claim_pending(redis, results, PERSISTENCE_GROUP, consumer, 1000, 3)
    fresh = redis.xreadgroup(PERSISTENCE_GROUP, consumer, {results: ">"}, count=100)
    for entry_id, message in exhausted_results:
        _dead_letter(redis, run_id, partition, results, PERSISTENCE_GROUP, entry_id, message, "delivery limit reached")
        redis.xack(results, PERSISTENCE_GROUP, entry_id)
    for entry_id, message in pending + [entry for _, batch in fresh for entry in batch]:
        if check_owner:
            check_owner(force=True)
        try:
            order = OrderCommand.model_validate_json(message["order"])
            result = _result_from_payload(json.loads(message["result"]))
            if order.run_id != run_id:
                raise ValueError("result belongs to another session")
        except (KeyError, TypeError, ValueError) as error:
            _dead_letter(redis, run_id, partition, results, PERSISTENCE_GROUP, entry_id, message, str(error))
            redis.xack(results, PERSISTENCE_GROUP, entry_id)
            continue
        store.record_order(order)
        store.complete_run(order, result, datetime.now(UTC), final=False)
        if check_owner:
            check_owner(force=True)
        redis.xack(results, PERSISTENCE_GROUP, entry_id)
    return [(event.event_id, (persisted_at - event.ingested_at.timestamp()) * 1000) for event in events]


class SessionChanged(Exception):
    pass


def run_live_worker(redis: Redis, store: DatabaseStore | None, run_id: UUID, partitions: list[int], role: str, stop=None, on_persist=None, startup_timeout_seconds=0, follow_session=False) -> None:
    stop = stop if stop is not None else Event()
    leases = [f"live:owner:{run_id}:{role}:{partition}" for partition in partitions]
    ownership = LiveOwnership(redis, leases, stop)
    def check_owner(force=False):
        ownership.check(force)
        if follow_session and redis.hget(LIVE_SESSION_KEY, "run_id") != str(run_id):
            raise SessionChanged()
    try:
        if not ownership.acquire(startup_timeout_seconds):
            return
        check_owner(force=True)
        engines = {partition: LiveEngine(redis, run_id, partition, check_owner) for partition in partitions} if role == "engine" else {}
        log_event("live_worker_ready", role=role, run_id=str(run_id))
        while not stop.is_set():
            for partition in partitions:
                check_owner(force=True)
                if role == "engine":
                    engines[partition].step(ownership.owner)
                else:
                    persisted = persist_live_batch(redis, store, run_id, partition, ownership.owner, check_owner=check_owner)
                    if on_persist:
                        on_persist(persisted)
    except SessionChanged:
        log_event("live_worker_session_changed", role=role, run_id=str(run_id))
    except InterruptedError:
        if not stop.is_set():
            raise
    finally:
        ownership.release()
        log_event("live_worker_stopped", role=role, run_id=str(run_id))


def main() -> None:
    require_private_live_mode()
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", choices=["engine", "persistence"], required=True)
    parser.add_argument("--run-id", type=UUID)
    parser.add_argument("--startup-timeout-seconds", type=float, default=45)
    parser.add_argument("--partitions", type=int, nargs="+", default=list(range(PARTITION_COUNT)))
    args = parser.parse_args()
    if args.startup_timeout_seconds < 0:
        parser.error("startup timeout must be nonnegative")
    if not args.partitions or any(partition not in range(PARTITION_COUNT) for partition in args.partitions) or len(set(args.partitions)) != len(args.partitions):
        parser.error("partitions must be unique values between 0 and 15")
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True, socket_timeout=10)
    session_id = redis.hget(LIVE_SESSION_KEY, "run_id")
    deadline = time() + 30
    while not args.run_id and not session_id and time() < deadline:
        sleep(1)
        session_id = redis.hget(LIVE_SESSION_KEY, "run_id")
    if not args.run_id and not session_id:
        raise RuntimeError("start private ingestion before live workers")
    run_id = args.run_id or UUID(session_id)
    store = DatabaseStore(sa.create_engine(sqlalchemy_url(os.environ["DATABASE_URL"]))) if args.role == "persistence" else None
    log_event("live_worker_started", role=args.role, run_id=str(run_id), partitions=args.partitions)
    stop = Event()
    previous_handlers = {sig: signal.signal(sig, lambda *_: stop.set()) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        while not stop.is_set():
            run_live_worker(redis, store, run_id, args.partitions, args.role, stop=stop, startup_timeout_seconds=args.startup_timeout_seconds, follow_session=not args.run_id)
            if args.run_id:
                break
            session_id = redis.hget(LIVE_SESSION_KEY, "run_id")
            if not session_id:
                raise RuntimeError("active live registry disappeared; restore ingestion before workers")
            run_id = UUID(session_id)
    finally:
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
        redis.close()


if __name__ == "__main__":
    main()
