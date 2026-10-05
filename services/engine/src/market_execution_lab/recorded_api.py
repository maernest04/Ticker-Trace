import json
import os
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock
from uuid import UUID

from fastapi import HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from market_execution_lab.recorded_replay import ExperimentRequest, ExperimentService
from market_execution_lab.recordings import CaptureService, RecordingStore


class CaptureRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    symbols: list[str] = Field(min_length=1, max_length=2)
    seconds: int = Field(default=60, ge=1, le=600)
    permissions_confirmed: bool = False


def install_recorded_routes(app, redis, store, redis_url, database_url):
    services = None
    lock = Lock()

    def initialized():
        nonlocal services
        with lock:
            if services is None:
                files = RecordingStore(Path(os.getenv("RECORDING_DIRECTORY", "recordings")))
                capture = CaptureService(redis, files)
                experiments = ExperimentService(files, redis, store, redis_url, database_url)
                services = files, capture, experiments
            return services

    def execute(function):
        try:
            return function(*initialized())
        except FileNotFoundError as error:
            raise HTTPException(404, "private recording or experiment not found") from error
        except (ValueError, RuntimeError) as error:
            raise HTTPException(409, str(error)) from error
        except OSError as error:
            raise HTTPException(503, "private recording storage unavailable") from error

    previous_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        async with previous_lifespan(application):
            try:
                yield
            finally:
                if services:
                    await asyncio.to_thread(services[2].close)
                    await asyncio.to_thread(services[1].close)

    app.router.lifespan_context = lifespan

    @app.get("/api/v1/recordings")
    def recordings():
        return execute(lambda files, *_: files.list())

    @app.post("/api/v1/recordings", status_code=202)
    def capture(request: CaptureRequest):
        return execute(lambda files, service, _: service.start(request.symbols, request.seconds, request.permissions_confirmed))

    @app.post("/api/v1/recordings/{recording_id}/stop", status_code=202)
    def stop(recording_id: UUID):
        return execute(lambda files, service, _: service.request_stop(recording_id))

    @app.get("/api/v1/recordings/{recording_id}")
    def recording(recording_id: UUID):
        return execute(lambda files, *_: files.manifest(recording_id))

    @app.get("/api/v1/recordings/{recording_id}/events")
    def events(recording_id: UUID, symbol: str, offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200)):
        def page(files, *_):
            manifest, entries = files.load(recording_id)
            selected = [entry for entry in entries if entry.event["symbol"] == symbol]
            return {"total": len(selected), "offset": offset, "checksum": manifest.checksum,
                "events": [{"index": index, **entry.model_dump()} for index, entry in enumerate(selected[offset:offset + limit], start=offset)]}
        return execute(page)

    @app.post("/api/v1/experiments", status_code=202)
    def experiment(request: ExperimentRequest):
        return execute(lambda files, capture, service: service.submit(request))

    @app.get("/api/v1/experiments/{experiment_id}")
    def result(experiment_id: UUID):
        def summary(files, capture, service):
            job = service.get(experiment_id)
            for outcome in job.get("result", {}).get("outcomes", []):
                outcome["fill_count"] = len(outcome["fills"])
                outcome["fills"] = outcome["fills"][:20]
                outcome["transitions"] = outcome["transitions"][:20]
            return job
        return execute(summary)

    @app.get("/api/v1/experiments/{experiment_id}/fills")
    def fills(experiment_id: UUID, outcome_index: int = Query(0, ge=0, le=1), offset: int = Query(0, ge=0), limit: int = Query(20, ge=1, le=100)):
        def page(files, capture, service):
            job = service.get(experiment_id)
            outcomes = job.get("result", {}).get("outcomes", [])
            if job["status"] != "completed" or outcome_index >= len(outcomes):
                raise ValueError("completed outcome is unavailable")
            _, entries = files.load(UUID(job["request"]["recording_id"]))
            selected = [entry for entry in entries if entry.event["symbol"] == job["request"]["symbol"]]
            sources = {}
            for index, entry in enumerate(selected):
                sources.setdefault(entry.event["event_id"], {"index": index, **entry.model_dump()})
            rows = outcomes[outcome_index]["fills"]
            return {"total": len(rows), "fills": [{**fill, "source": sources[fill["triggering_event_id"]]} for fill in rows[offset:offset + limit]]}
        return execute(page)

    @app.get("/api/v1/experiments/{experiment_id}/trace")
    def trace(experiment_id: UUID, offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200)):
        def page(files, capture, service):
            job = service.get(experiment_id)
            if job["status"] != "completed":
                raise ValueError("experiment is not complete")
            rows = json.loads(files.path(experiment_id, "trace.json").read_bytes())["trace"]
            return {"total": len(rows), "offset": offset, "trace": rows[offset:offset + limit]}
        return execute(page)
