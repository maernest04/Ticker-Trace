import argparse
import json
import os
from uuid import UUID

from redis import Redis

from market_execution_lab.pipeline import run_engine


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--partition", type=int, required=True)
    parser.add_argument("--consumer", default="engine-1")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    arguments = parser.parse_args()

    result = run_engine(
        Redis.from_url(arguments.redis_url, decode_responses=True),
        arguments.run_id,
        arguments.partition,
        arguments.consumer,
    )
    print(json.dumps({"state": result.state.value, "fills": len(result.fills)}))


if __name__ == "__main__":
    main()
