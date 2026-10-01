import asyncio
import os
import threading
from contextlib import asynccontextmanager
from time import monotonic
from uuid import UUID

import sqlalchemy as sa
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from redis import Redis

from market_execution_lab.pipeline import run_engine, run_persistence
from market_execution_lab.storage import DatabaseStore, sqlalchemy_url


class ReplayJob(BaseModel):
    run_id: UUID
    partition: int = Field(ge=0, lt=16)


def create_worker_app(role: str, redis: Redis, store: DatabaseStore | None, on_idle, idle_seconds: int = 60) -> FastAPI:
    lock = threading.Lock()
    active = 0
    active_runs = set()
    last_activity = monotonic()
    closing = False

    @asynccontextmanager
    async def lifespan(app):
        async def shutdown_when_idle():
            nonlocal closing
            while True:
                await asyncio.sleep(1)
                with lock:
                    if active == 0 and monotonic() - last_activity >= idle_seconds:
                        closing = True
                        on_idle()
                        return

        task = asyncio.create_task(shutdown_when_idle())
        yield
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    app = FastAPI(lifespan=lifespan)

    @app.get("/health")
    def health():
        if closing:
            raise HTTPException(503, "worker stopping")
        return {"role": role}

    @app.post("/jobs")
    async def job(request: ReplayJob):
        nonlocal active, last_activity
        with lock:
            if closing or active >= 4 or request.run_id in active_runs:
                raise HTTPException(503, "worker unavailable")
            active += 1
            active_runs.add(request.run_id)
        try:
            if role == "engine":
                await asyncio.to_thread(run_engine, redis, request.run_id, request.partition, str(request.run_id))
            else:
                await asyncio.to_thread(run_persistence, redis, store, request.run_id, request.partition, str(request.run_id))
            return {"status": "completed"}
        finally:
            with lock:
                active -= 1
                active_runs.remove(request.run_id)
                last_activity = monotonic()

    return app


def main() -> None:
    role = os.environ["WORKER_ROLE"]
    if role not in {"engine", "persistence"}:
        raise ValueError("WORKER_ROLE must be engine or persistence")
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True, socket_timeout=10, socket_connect_timeout=5)
    store = DatabaseStore(sa.create_engine(sqlalchemy_url(os.environ["DATABASE_URL"]), connect_args={"connect_timeout": 10})) if role == "persistence" else None
    server = None

    def stop():
        server.should_exit = True

    app = create_worker_app(role, redis, store, stop)
    server = uvicorn.Server(uvicorn.Config(app, host="::", port=8000))
    server.run()
