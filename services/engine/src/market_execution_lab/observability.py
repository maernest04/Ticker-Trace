import json
import logging
from contextvars import ContextVar
from dataclasses import dataclass
from time import perf_counter

from redis import Redis


request_id_context: ContextVar[str] = ContextVar("request_id", default="-")
run_id_context: ContextVar[str] = ContextVar("run_id", default="-")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "event": record.getMessage(),
            "request_id": request_id_context.get(),
            "run_id": run_id_context.get(),
        }
        payload.update(getattr(record, "fields", {}))
        return json.dumps(payload, default=str)


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("market_execution_lab")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False


def log_event(event: str, **fields: object) -> None:
    logging.getLogger("market_execution_lab").info(event, extra={"fields": fields})


def metrics_key(run_id: str, partition: int) -> str:
    return f"pipeline.metrics:{run_id}:{partition}"


def record_metrics(redis: Redis, run_id: str, partition: int, **values: int | float) -> None:
    redis.hset(metrics_key(run_id, partition), mapping={key: str(value) for key, value in values.items()})


@dataclass(frozen=True)
class PipelineMetrics:
    queue_depth: int
    engine_lag: int
    engine_pending: int
    persistence_lag: int
    persistence_pending: int
    throughput_events_per_second: float
    processing_latency_ms: float | None


def pipeline_metrics(redis: Redis, source_stream: str, run_id: str, partition: int) -> PipelineMetrics:
    groups = {group["name"]: group for group in redis.xinfo_groups(source_stream)}
    engine = groups.get("engine", {})
    persistence = groups.get("persistence", {})
    values = redis.hgetall(metrics_key(run_id, partition))
    published_at = float(values.get("published_at", "0"))
    completed_at = float(values.get("engine_completed_at", "0"))
    processed_events = int(values.get("processed_events", "0"))
    duration = completed_at - published_at
    return PipelineMetrics(
        queue_depth=max(int(engine.get("lag", 0)), int(persistence.get("lag", 0))),
        engine_lag=int(engine.get("lag", 0)),
        engine_pending=int(engine.get("pending", 0)),
        persistence_lag=int(persistence.get("lag", 0)),
        persistence_pending=int(persistence.get("pending", 0)),
        throughput_events_per_second=processed_events / duration if duration > 0 else 0,
        processing_latency_ms=float(values["engine_processing_ms"]) if "engine_processing_ms" in values else None,
    )


def now_seconds() -> float:
    return perf_counter()
