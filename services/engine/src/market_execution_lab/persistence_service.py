import argparse
import os
from uuid import UUID

import sqlalchemy as sa
from redis import Redis

from market_execution_lab.pipeline import MAX_DELIVERIES, RECOVERY_IDLE_MS, run_persistence
from market_execution_lab.storage import DatabaseStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--partition", type=int, required=True)
    parser.add_argument("--consumer", default="persistence-1")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--recovery-idle-ms", type=int, default=RECOVERY_IDLE_MS)
    parser.add_argument("--max-deliveries", type=int, default=MAX_DELIVERIES)
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "postgresql+psycopg://tickertrace:tickertrace@localhost:5432/tickertrace"),
    )
    arguments = parser.parse_args()

    run_persistence(
        Redis.from_url(arguments.redis_url, decode_responses=True),
        DatabaseStore(sa.create_engine(arguments.database_url)),
        arguments.run_id,
        arguments.partition,
        arguments.consumer,
        arguments.recovery_idle_ms,
        arguments.max_deliveries,
    )


if __name__ == "__main__":
    main()
