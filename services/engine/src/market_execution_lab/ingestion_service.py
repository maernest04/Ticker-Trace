import argparse
import asyncio
import json
import os
from uuid import UUID, uuid4

from redis import asyncio as aioredis

from market_execution_lab.alpaca import AlpacaSettings, require_private_live_mode, stream_alpaca
from market_execution_lab.observability import configure_logging, log_event
from market_execution_lab.pipeline import partition_for_symbol
from market_execution_lab.streaming import partition_stream_name


async def run(run_id: UUID, redis_url: str) -> None:
    require_private_live_mode()
    settings = AlpacaSettings.from_environment()
    redis = aioredis.Redis.from_url(redis_url, decode_responses=True)

    async def publish(event) -> None:
        await redis.xadd(
            partition_stream_name(str(run_id), event.partition),
            {"message_type": "market.event.v1", "payload": json.dumps(event.model_dump(mode="json"))},
        )

    log_event("alpaca_ingestion_started", run_id=str(run_id), symbol_count=len(settings.symbols))
    try:
        await stream_alpaca(settings, run_id, partition_for_symbol, publish)
    finally:
        await redis.aclose()


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=UUID, default=uuid4())
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    arguments = parser.parse_args()
    asyncio.run(run(arguments.run_id, arguments.redis_url))


if __name__ == "__main__":
    main()
