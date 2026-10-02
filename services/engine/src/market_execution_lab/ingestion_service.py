import argparse
import asyncio
import json
import os
import signal
from time import time
from uuid import UUID, uuid4

from redis import asyncio as aioredis
from redis import Redis

from market_execution_lab.alpaca import (
    INGESTION_CONTROL_STREAM,
    AlpacaSettings,
    require_private_live_mode,
    stream_alpaca,
)
from market_execution_lab.observability import configure_logging, log_event
from market_execution_lab.pipeline import partition_for_symbol
from market_execution_lab.streaming import partition_stream_name
from market_execution_lab.live_pipeline import LIVE_SESSION_KEY, LIVE_MAX_BACKLOG, append_live, initialize_session


async def run(run_id: UUID, redis_url: str) -> None:
    require_private_live_mode()
    settings = AlpacaSettings.from_environment()
    redis = aioredis.Redis.from_url(redis_url, decode_responses=True)
    sync_redis = Redis.from_url(redis_url, decode_responses=True)
    owner = str(uuid4())
    if not await redis.set("live:ingestion-owner", owner, nx=True, ex=30):
        await redis.aclose()
        sync_redis.close()
        raise RuntimeError("a private ingestion session is already running")

    async def heartbeat():
        while True:
            if not await redis.eval("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('EXPIRE', KEYS[1], 30) end; return 0", 1, "live:ingestion-owner", owner):
                raise RuntimeError("ingestion ownership lost")
            await asyncio.sleep(5)

    async def status(value):
        await redis.hset(LIVE_SESSION_KEY, "status", value)

    async def publish(event) -> None:
        stream = partition_stream_name(str(run_id), event.partition)
        while True:
            groups = await redis.xinfo_groups(stream)
            backlog = max((int(group.get("lag") or 0) + int(group["pending"]) for group in groups), default=0)
            if backlog < LIVE_MAX_BACKLOG:
                break
            await status("backpressure")
            await asyncio.sleep(0.1)
        if not await append_live(redis, stream, {"message_type": "market.event.v1", "payload": event.model_dump_json()}):
            raise RuntimeError("live session capacity reached; start a new session")
        await redis.hset(LIVE_SESSION_KEY, mapping={"status": "connected", "received_at": str(time())})

    async def subscription_updates():
        latest = await redis.xrevrange(INGESTION_CONTROL_STREAM, count=1)
        last_id = latest[0][0] if latest else "0-0"
        while True:
            response = await redis.xread({INGESTION_CONTROL_STREAM: last_id}, block=1_000)
            for _, entries in response:
                for entry_id, message in entries:
                    last_id = entry_id
                    if message["message_type"] == "watchlist.updated.v1":
                        symbols = tuple(json.loads(message["symbols"]))
                        await redis.hset(LIVE_SESSION_KEY, "symbols", json.dumps(symbols))
                        yield symbols

    log_event("alpaca_ingestion_started", run_id=str(run_id), symbol_count=len(settings.symbols))
    try:
        initialization = asyncio.create_task(asyncio.to_thread(initialize_session, sync_redis, run_id, list(settings.symbols)))
        try:
            await asyncio.shield(initialization)
        except asyncio.CancelledError:
            await initialization
            raise
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(heartbeat())
            tasks.create_task(stream_alpaca(settings, run_id, partition_for_symbol, publish, subscription_updates(), status))
    finally:
        try:
            await redis.eval("if redis.call('GET', KEYS[1]) == ARGV[1] then if redis.call('HGET', KEYS[2], 'run_id') == ARGV[2] then redis.call('HSET', KEYS[2], 'status', 'stopped') end; return redis.call('DEL', KEYS[1]) end; return 0", 2, "live:ingestion-owner", LIVE_SESSION_KEY, owner, str(run_id))
        finally:
            await redis.aclose()
            sync_redis.close()


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=UUID, default=uuid4())
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    arguments = parser.parse_args()
    async def serve():
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        try:
            await run(arguments.run_id, arguments.redis_url)
        except asyncio.CancelledError:
            log_event("alpaca_ingestion_stopped", run_id=str(arguments.run_id))
        finally:
            loop.remove_signal_handler(signal.SIGTERM)

    asyncio.run(serve())


if __name__ == "__main__":
    main()
