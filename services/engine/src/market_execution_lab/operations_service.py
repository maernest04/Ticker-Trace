import os
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from redis import Redis
from redis.exceptions import RedisError
import uvicorn

from market_execution_lab.observability import (
    configure_logging,
    log_event,
    pipeline_metrics,
    request_id_context,
)
from market_execution_lab.streaming import partition_stream_name


def create_app(redis_url: str) -> FastAPI:
    redis = Redis.from_url(redis_url, decode_responses=True)
    app = FastAPI()
    app.state.redis = redis

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = request.headers.get("x-request-id", str(uuid4()))
        token = request_id_context.set(request_id)
        try:
            response = await call_next(request)
        finally:
            request_id_context.reset(token)
        response.headers["x-request-id"] = request_id
        return response

    @app.get("/health")
    def health() -> dict[str, str]:
        log_event("health_checked")
        return {"status": "ok"}

    @app.get("/ready")
    def ready() -> dict[str, str]:
        try:
            redis.ping()
        except RedisError as error:
            raise HTTPException(status_code=503, detail="redis unavailable") from error
        return {"status": "ready"}

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics(run_id: UUID, partition: int) -> str:
        result = pipeline_metrics(redis, partition_stream_name(str(run_id), partition), str(run_id), partition)
        return "\n".join(
            [
                f"market_execution_queue_depth {result.queue_depth}",
                f"market_execution_engine_lag {result.engine_lag}",
                f"market_execution_engine_pending {result.engine_pending}",
                f"market_execution_persistence_lag {result.persistence_lag}",
                f"market_execution_persistence_pending {result.persistence_pending}",
                f"market_execution_throughput_events_per_second {result.throughput_events_per_second}",
                f"market_execution_processing_latency_ms {result.processing_latency_ms or 0}",
            ]
        )

    return app


def main() -> None:
    configure_logging()
    uvicorn.run(create_app(os.getenv("REDIS_URL", "redis://localhost:6379/0")), host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
