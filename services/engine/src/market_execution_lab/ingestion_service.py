import argparse
import asyncio
import json
import os
import signal
from dataclasses import replace
from time import time
from uuid import UUID, uuid4

from redis import asyncio as aioredis
from redis import Redis
from redis.exceptions import TimeoutError as RedisTimeoutError
import sqlalchemy as sa

from market_execution_lab.alpaca import (
    INGESTION_CONTROL_STREAM,
    AlpacaSettings,
    require_private_live_mode,
    stream_alpaca,
)
from market_execution_lab.observability import configure_logging, log_event
from market_execution_lab.pipeline import partition_for_symbol
from market_execution_lab.streaming import partition_stream_name
from market_execution_lab.live_pipeline import LIVE_SESSION_KEY, LIVE_MAX_BACKLOG
from market_execution_lab.live_sessions import LiveLimits, SessionCoordinator
from market_execution_lab.live_ownership import OwnershipLost
from market_execution_lab.storage import DatabaseStore, sqlalchemy_url


class SessionRollover(Exception):
    pass


async def run(run_id: UUID | None, redis_url: str) -> None:
    require_private_live_mode()
    settings = AlpacaSettings.from_environment()
    limits = LiveLimits.from_environment()
    redis = aioredis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=10)
    sync_redis = Redis.from_url(redis_url, decode_responses=True, socket_timeout=10)
    database = sa.create_engine(sqlalchemy_url(os.environ["DATABASE_URL"]))
    store = DatabaseStore(database)
    owner = str(uuid4())
    coordinator = SessionCoordinator(sync_redis, store, owner, limits)
    current_run_id = None

    async def coordinated_call(function, *args):
        task = asyncio.create_task(asyncio.to_thread(function, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def heartbeat():
        while True:
            await coordinated_call(coordinator.check_owner)
            await asyncio.sleep(5)

    async def status(value):
        await redis.eval("if redis.call('GET', KEYS[1]) == ARGV[1] and redis.call('HGET', KEYS[2], 'run_id') == ARGV[2] and redis.call('HGET', KEYS[2], 'phase') == 'running' then return redis.call('HSET', KEYS[2], 'status', ARGV[3]) end; return 0", 2, "live:ingestion-owner", LIVE_SESSION_KEY, owner, str(current_run_id), value)

    async def publish(event) -> None:
        stream = partition_stream_name(str(current_run_id), event.partition)
        while True:
            groups = await redis.xinfo_groups(stream)
            backlog = max((int(group.get("lag") or 0) + int(group["pending"]) for group in groups), default=0)
            if backlog < LIVE_MAX_BACKLOG:
                break
            await status("backpressure")
            await asyncio.sleep(0.1)
        result = await redis.eval(
            "if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end; "
            "if redis.call('HGET', KEYS[2], 'run_id') ~= ARGV[2] or redis.call('HGET', KEYS[2], 'phase') ~= 'running' or redis.call('XLEN', KEYS[3]) >= tonumber(ARGV[3]) then return 0 end; "
            "redis.call('XADD', KEYS[3], '*', 'message_type', 'market.event.v1', 'payload', ARGV[4]); redis.call('HSET', KEYS[2], 'status', 'connected', 'received_at', ARGV[5]); return 1",
            3, "live:ingestion-owner", LIVE_SESSION_KEY, stream, owner, str(current_run_id), limits.messages, event.model_dump_json(), str(time()))
        if result == -1:
            raise OwnershipLost("ingestion ownership lost")
        if not result:
            raise SessionRollover()

    async def subscription_updates():
        control_stream = f"{INGESTION_CONTROL_STREAM}:{current_run_id}"
        last_id = await redis.hget(LIVE_SESSION_KEY, "control_id") or "0-0"
        while True:
            response = await redis.xread({control_stream: last_id}, block=1_000)
            for _, entries in response:
                for entry_id, message in entries:
                    last_id = entry_id
                    if message["message_type"] == "watchlist.updated.v1" and message["watchlist_id"] == str(current_run_id):
                        symbols = tuple(json.loads(message["symbols"]))
                        await redis.hset(LIVE_SESSION_KEY, mapping={"symbols": json.dumps(symbols), "control_id": entry_id})
                        yield symbols

    async def consume():
        try:
            symbols = tuple(json.loads(await redis.hget(LIVE_SESSION_KEY, "symbols")))
            await stream_alpaca(replace(settings, symbols=symbols), current_run_id, partition_for_symbol, publish, subscription_updates(), status)
            raise RuntimeError("live stream stopped unexpectedly")
        except SessionRollover:
            return

    async def monitor():
        cleanup_at = 0
        while True:
            reason = await coordinated_call(coordinator.rollover_reason, current_run_id)
            if reason:
                log_event("live_session_rollover_requested", run_id=str(current_run_id), reason=reason)
                return
            if time() >= cleanup_at:
                await coordinated_call(coordinator.cleanup)
                cleanup_at = time() + 60
            await asyncio.sleep(1)

    async def sessions():
        nonlocal current_run_id
        session = await coordinated_call(coordinator.resume, list(settings.symbols), run_id)
        current_run_id = UUID(session["run_id"])
        while True:
            if await redis.hget(LIVE_SESSION_KEY, "phase") != "closing":
                feed, watcher = asyncio.create_task(consume()), asyncio.create_task(monitor())
                try:
                    done, _ = await asyncio.wait({feed, watcher}, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
                    await coordinated_call(coordinator.begin_close, current_run_id)
                finally:
                    feed.cancel()
                    watcher.cancel()
                    await asyncio.gather(feed, watcher, return_exceptions=True)
            else:
                await coordinated_call(coordinator.begin_close, current_run_id)
            while True:
                next_run_id = await coordinated_call(coordinator.finish_close, current_run_id)
                if next_run_id:
                    current_run_id = next_run_id
                    break
                await asyncio.sleep(0.2)

    try:
        deadline = time() + 45
        while not await redis.set("live:ingestion-owner", owner, nx=True, ex=30):
            if time() >= deadline:
                raise RuntimeError("a private ingestion session is already running; startup wait timed out")
            await asyncio.sleep(1)
        log_event("alpaca_ingestion_started", run_id=str(run_id), symbol_count=len(settings.symbols))
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(heartbeat())
                tasks.create_task(sessions())
        except ExceptionGroup as failure:
            _, remaining = failure.split((OwnershipLost, RedisTimeoutError))
            if remaining is not None or failure.subgroup(RedisTimeoutError) is None:
                raise
            if await redis.get("live:ingestion-owner") == owner:
                raise
            raise ExceptionGroup("ingestion ownership expired during Redis timeout", [OwnershipLost("ingestion ownership lost")]) from failure
    finally:
        try:
            await redis.eval("if redis.call('GET', KEYS[1]) == ARGV[1] then if redis.call('HGET', KEYS[2], 'run_id') == ARGV[2] and redis.call('HGET', KEYS[2], 'phase') ~= 'closing' then redis.call('HSET', KEYS[2], 'status', 'stopped') end; return redis.call('DEL', KEYS[1]) end; return 0", 2, "live:ingestion-owner", LIVE_SESSION_KEY, owner, str(current_run_id))
        finally:
            await redis.aclose()
            sync_redis.close()
            database.dispose()


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=UUID)
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    arguments = parser.parse_args()
    async def serve():
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        try:
            while True:
                try:
                    await run(arguments.run_id, arguments.redis_url)
                    return
                except ExceptionGroup as failure:
                    _, remaining = failure.split(OwnershipLost)
                    if remaining is not None:
                        raise
                    log_event("alpaca_ingestion_recovering_ownership", run_id=str(arguments.run_id))
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            log_event("alpaca_ingestion_stopped", run_id=str(arguments.run_id))
        except ExceptionGroup as failure:
            _, remaining = failure.split(PermissionError)
            if remaining is not None:
                raise
            log_event("alpaca_authentication_fatal", retry=False)
        finally:
            loop.remove_signal_handler(signal.SIGTERM)

    asyncio.run(serve())


if __name__ == "__main__":
    main()
