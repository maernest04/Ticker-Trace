import argparse
import json
import os
from dataclasses import replace
from uuid import UUID, uuid4

from redis import Redis

from market_execution_lab.fixtures import replay_scenarios
from market_execution_lab.observability import configure_logging
from market_execution_lab.pipeline import MAX_QUEUE_DEPTH, publish_replay


def main() -> None:
    configure_logging()
    scenarios = {scenario.name: scenario for scenario in replay_scenarios()}
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=sorted(scenarios), required=True)
    parser.add_argument("--run-id", type=UUID, default=uuid4())
    parser.add_argument("--order-id", type=UUID, default=uuid4())
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--max-queue-depth", type=int, default=MAX_QUEUE_DEPTH)
    pacing = parser.add_mutually_exclusive_group()
    pacing.add_argument("--playback-speed", type=float)
    pacing.add_argument("--events-per-second", type=float)
    arguments = parser.parse_args()

    template = scenarios[arguments.scenario]
    scenario = replace(
        template,
        order=template.order.model_copy(update={"run_id": arguments.run_id, "order_id": arguments.order_id}),
        events=tuple(event.model_copy(update={"run_id": arguments.run_id}) for event in template.events),
    )
    partition = publish_replay(
        Redis.from_url(arguments.redis_url, decode_responses=True),
        scenario,
        arguments.max_queue_depth,
        playback_speed=arguments.playback_speed,
        events_per_second=arguments.events_per_second,
    )
    print(json.dumps({"run_id": str(arguments.run_id), "order_id": str(arguments.order_id), "partition": partition}))


if __name__ == "__main__":
    main()
