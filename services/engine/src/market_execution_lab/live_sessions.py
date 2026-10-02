import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from time import time
from uuid import UUID, uuid4

from market_execution_lab.live_pipeline import LIVE_SESSION_KEY, LIVE_MAX_MESSAGES, LIVE_MAX_ORDERS, initialize_session, live_backlog
from market_execution_lab.observability import log_event
from market_execution_lab.pipeline import PARTITION_COUNT, result_stream_name
from market_execution_lab.streaming import partition_stream_name


@dataclass(frozen=True)
class LiveLimits:
    session_seconds: int = 1800
    messages: int = 90000
    orders: int = 240
    recovery_seconds: int = 900
    raw_seconds: int = 3600
    history_seconds: int = 86400

    def __post_init__(self):
        if min(self.session_seconds, self.messages, self.orders, self.recovery_seconds, self.raw_seconds, self.history_seconds) <= 0:
            raise ValueError("live lifecycle limits must be positive")
        if self.messages >= LIVE_MAX_MESSAGES or self.orders >= LIVE_MAX_ORDERS:
            raise ValueError("rollover thresholds must leave room below hard session caps")
        if self.history_seconds < max(self.raw_seconds, self.recovery_seconds):
            raise ValueError("history retention must cover raw retention and Redis recovery grace")

    @classmethod
    def from_environment(cls):
        return cls(**{field: int(os.getenv(f"LIVE_{field.upper()}", str(default.default))) for field, default in cls.__dataclass_fields__.items()})


class SessionCoordinator:
    def __init__(self, redis, store, owner: str, limits: LiveLimits):
        self.redis, self.store, self.owner, self.limits = redis, store, owner, limits

    def check_owner(self):
        if not self.redis.eval("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('EXPIRE', KEYS[1], 30) end; return 0", 1, "live:ingestion-owner", self.owner):
            raise RuntimeError("ingestion ownership lost")

    def resume(self, symbols: list[str], requested_run_id: UUID | None = None) -> dict:
        self.check_owner()
        registry = self.redis.hgetall(LIVE_SESSION_KEY)
        current = UUID(registry["run_id"]) if registry else None
        if requested_run_id and current and requested_run_id != current:
            raise ValueError("cannot replace an active session with --run-id; use coordinated rollover")
        durable = self.store.private_session(current) if current else (None if requested_run_id else self.store.private_session())
        run_id = current or (durable["run_id"] if durable else requested_run_id or uuid4())
        control = self.redis.xrevrange(f"ingestion.control:{run_id}", count=1)
        replay = self.store.replay_for_run(run_id)
        if replay and replay["settings"] and replay["settings"]["mode"] != "private_live":
            raise ValueError("cannot resume a public replay as private live")
        watchlist = self.store.watchlist_for_id(run_id)
        symbols = watchlist["symbols"] if watchlist else replay["settings"]["symbols"] if replay and replay["settings"] else symbols
        started_at = replay["started_at"] if replay else datetime.now(UTC)
        if replay and (not durable or durable["status"] not in {"starting", "completed"}):
            if any(not self.redis.exists(partition_stream_name(str(run_id), partition)) for partition in range(PARTITION_COUNT)):
                raise RuntimeError("retained source history is missing; restore Redis before resuming this session")
        self.store.create_run(run_id, "private_live", started_at)
        self.store.record_replay_settings(run_id, "private_live", symbols, overwrite=False)
        self.store.register_private_session(run_id, started_at)
        initialize_session(self.redis, run_id, symbols, register=False, scenario_name="private_live")
        self.store.activate_private_session(run_id)
        durable = self.store.private_session(run_id)
        phase = "closing" if durable["status"] != "running" else "running"
        self.check_owner()
        self.redis.hset(LIVE_SESSION_KEY, mapping={"run_id": str(run_id), "symbols": json.dumps(symbols), "status": "closing" if phase == "closing" else "connecting", "phase": phase,
                                                 "started_at": str(started_at.timestamp()), "received_at": "0", "control_id": control[0][0] if control else "0-0"})
        return self.redis.hgetall(LIVE_SESSION_KEY)

    def rollover_reason(self, run_id: UUID) -> str | None:
        session = self.redis.hgetall(LIVE_SESSION_KEY)
        if session.get("phase") == "closing":
            return "recovering closure"
        if time() - float(session["started_at"]) >= self.limits.session_seconds:
            return "session duration"
        if int(self.redis.get(f"live:orders:{run_id}") or 0) >= self.limits.orders:
            return "order threshold"
        if any(self.redis.xlen(partition_stream_name(str(run_id), partition)) >= self.limits.messages for partition in range(PARTITION_COUNT)):
            return "message threshold"
        return None

    def begin_close(self, run_id: UUID) -> dict:
        self.check_owner()
        closing = self.store.begin_private_close(run_id, uuid4(), datetime.now(UTC))
        if closing["status"] == "completed":
            return closing
        streams = [partition_stream_name(str(run_id), partition) for partition in range(PARTITION_COUNT)]
        payload = json.dumps({"run_id": str(run_id), "closed_at": closing["closing_at"].isoformat(), "reason": "session ended"})
        result = self.redis.eval(
            "if redis.call('GET', KEYS[1]) ~= ARGV[1] or redis.call('HGET', KEYS[2], 'run_id') ~= ARGV[2] then return 0 end; "
            "redis.call('HSET', KEYS[2], 'phase', 'closing', 'status', 'closing'); "
            "if redis.call('HGET', KEYS[3], 'boundary_ready') == '1' then return 1 end; "
            "for i=4,#KEYS do local id=redis.call('XADD', KEYS[i], '*', 'message_type', 'session.closed.v1', 'payload', ARGV[3]); redis.call('HSET', KEYS[3], tostring(i-4), id) end; "
            "redis.call('HSET', KEYS[3], 'boundary_ready', '1'); return 1",
            3 + len(streams), "live:ingestion-owner", LIVE_SESSION_KEY, f"live:closing:{run_id}", *streams, self.owner, str(run_id), payload)
        if not result:
            raise RuntimeError("session changed during closure")
        log_event("live_session_closing", run_id=str(run_id), next_run_id=str(closing["next_run_id"]))
        return closing

    def finish_close(self, run_id: UUID) -> UUID | None:
        self.check_owner()
        closing = self.store.private_session(run_id)
        if not closing or closing["status"] not in {"closing", "completed"}:
            raise RuntimeError("session closure has not started")
        if closing["status"] != "completed":
            if self.redis.hget(f"live:closing:{run_id}", "boundary_ready") != "1":
                return None
            if any(live_backlog(self.redis, partition_stream_name(str(run_id), partition)) or live_backlog(self.redis, result_stream_name(str(run_id), partition)) for partition in range(PARTITION_COUNT)):
                return None
            self.store.finish_private_close(run_id, datetime.now(UTC))
        next_run_id = closing["next_run_id"]
        watchlist = self.store.watchlist_for_id(run_id)
        symbols = watchlist["symbols"] if watchlist else self.store.replay_for_run(run_id)["settings"]["symbols"]
        started_at = datetime.now(UTC)
        self.store.create_run(next_run_id, "private_live", started_at)
        self.store.record_replay_settings(next_run_id, "private_live", symbols, overwrite=False)
        self.store.register_private_session(next_run_id, started_at)
        initialize_session(self.redis, next_run_id, symbols, register=False, scenario_name="private_live")
        self.store.activate_private_session(next_run_id)
        result = self.redis.eval(
            "if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end; "
            "local current=redis.call('HGET', KEYS[2], 'run_id'); if current==ARGV[3] then return 1 end; if current~=ARGV[2] then return 0 end; "
            "redis.call('HSET', KEYS[2], 'run_id', ARGV[3], 'symbols', ARGV[4], 'phase', 'running', 'status', 'connecting', 'started_at', ARGV[5], 'received_at', '0', 'control_id', '0-0'); return 1",
            2, "live:ingestion-owner", LIVE_SESSION_KEY, self.owner, str(run_id), str(next_run_id), json.dumps(symbols), str(self.store.private_session(next_run_id)["started_at"].timestamp()))
        if not result:
            raise RuntimeError("session changed during activation")
        self.expire_closed(run_id)
        log_event("live_session_activated", run_id=str(next_run_id), previous_run_id=str(run_id))
        return next_run_id

    def expire_closed(self, run_id: UUID):
        self.check_owner()
        session = self.store.private_session(run_id)
        if not session or session["status"] != "completed" or self.redis.hget(LIVE_SESSION_KEY, "run_id") == str(run_id):
            return
        for pattern in (f"market.events:{run_id}:*", f"execution.results:{run_id}:*", f"state:replay:{run_id}:*", f"order:replay:{run_id}:*", f"pipeline.dead-letter:{run_id}:*", f"pipeline.metrics:{run_id}:*"):
            for key in self.redis.scan_iter(match=pattern, count=100):
                self.redis.expire(key, self.limits.recovery_seconds, nx=True)
        for key in (f"live:closing:{run_id}", f"live:orders:{run_id}", f"ingestion.control:{run_id}"):
            self.redis.expire(key, self.limits.recovery_seconds, nx=True)
        self.store.mark_private_redis_expired(run_id)

    def cleanup(self):
        self.check_owner()
        for session in self.store.closed_private_sessions():
            self.expire_closed(session["run_id"])
        self.check_owner()
        return self.store.prune_private_history(self.limits.raw_seconds, self.limits.history_seconds)
