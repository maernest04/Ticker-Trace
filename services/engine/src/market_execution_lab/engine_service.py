import argparse
import json
import os
from uuid import UUID

from redis import Redis

from market_execution_lab.pipeline import MAX_DELIVERIES, RECOVERY_IDLE_MS, run_engine, run_engine_worker
from market_execution_lab.observability import configure_logging


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=UUID)
    parser.add_argument("--partition", type=int)
    parser.add_argument("--forever", action="store_true")
    parser.add_argument("--consumer", default="engine-1")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--recovery-idle-ms", type=int, default=RECOVERY_IDLE_MS)
    parser.add_argument("--max-deliveries", type=int, default=MAX_DELIVERIES)
    arguments = parser.parse_args()
    redis = Redis.from_url(arguments.redis_url, decode_responses=True)

    if arguments.forever:
        run_engine_worker(redis, arguments.consumer)
        return
    if arguments.run_id is None or arguments.partition is None:
        parser.error("--run-id and --partition are required without --forever")

    result = run_engine(
        redis,
        arguments.run_id,
        arguments.partition,
        arguments.consumer,
        arguments.recovery_idle_ms,
        arguments.max_deliveries,
    )
    print(json.dumps({"state": result.state.value, "fills": len(result.fills)}))


if __name__ == "__main__":
    main()
