import io
import json
import threading
from unittest.mock import Mock
from urllib.error import HTTPError, URLError
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from market_execution_lab import api_service, fly_workers, worker_service
from market_execution_lab.fly_workers import FlyWorkers
from market_execution_lab.worker_service import create_worker_app


def test_idle_worker_health_does_not_access_dependencies():
    redis, store, stop = Mock(), Mock(), Mock()
    with TestClient(create_worker_app("persistence", redis, store, stop)) as client:
        assert client.get("/health").json() == {"role": "persistence"}
    assert not redis.mock_calls
    assert not store.mock_calls
    stop.assert_not_called()


@pytest.mark.parametrize("role", ["engine", "persistence"])
def test_worker_dispatches_once_and_releases_capacity_after_failure(monkeypatch, role):
    execute = Mock(side_effect=[RuntimeError("dependency unavailable"), None])
    monkeypatch.setattr(worker_service, f"run_{role}", execute)
    run_id = uuid4()
    redis, store = Mock(), Mock()
    with TestClient(create_worker_app(role, redis, store, Mock()), raise_server_exceptions=False) as client:
        payload = {"run_id": str(run_id), "partition": 0}
        assert client.post("/jobs", json=payload).status_code == 503
        assert client.post("/jobs", json=payload).json() == {"status": "completed"}
        assert client.post("/jobs", json={**payload, "partition": 16}).status_code == 422
    assert execute.call_count == 2
    assert execute.call_args.args[-1] == str(run_id)


def test_idle_worker_stops_and_rejects_new_jobs():
    stopped = threading.Event()
    with TestClient(create_worker_app("engine", Mock(), None, stopped.set, idle_seconds=0)) as client:
        assert stopped.wait(3)
        assert client.get("/health").status_code == 503
        assert client.post("/jobs", json={"run_id": str(uuid4()), "partition": 0}).status_code == 503


def test_active_job_prevents_idle_shutdown_and_duplicate_dispatch(monkeypatch):
    entered, release, stopped = threading.Event(), threading.Event(), threading.Event()

    def execute(*args):
        entered.set()
        assert release.wait(5)

    monkeypatch.setattr(worker_service, "run_engine", execute)
    with TestClient(create_worker_app("engine", Mock(), None, stopped.set, idle_seconds=0)) as client:
        payload = {"run_id": str(uuid4()), "partition": 0}
        responses = []
        thread = threading.Thread(target=lambda: responses.append(client.post("/jobs", json=payload)))
        thread.start()
        try:
            assert entered.wait(2)
            assert not stopped.wait(1.2)
            assert client.post("/jobs", json=payload).status_code == 503
        finally:
            release.set()
            thread.join(3)
        assert responses[0].status_code == 200
        assert stopped.wait(3)


def test_fly_wake_handles_concurrent_start_and_validates_worker_readiness(monkeypatch):
    monkeypatch.setenv("FLY_APP_NAME", "test-app")
    monkeypatch.setenv("FLY_WORKER_TOKEN", "test-token")
    workers = FlyWorkers()
    machines = [{"id": role, "state": "stopped", "config": {"metadata": {"fly_process_group": role}}} for role in ("engine", "persistence")]

    def request(path, method="GET"):
        if not path:
            return machines
        raise HTTPError("https://example.invalid", 409, "already starting", {}, None)

    workers.request = Mock(side_effect=request)
    monkeypatch.setattr(fly_workers, "urlopen", lambda url, timeout: io.BytesIO(json.dumps({"role": "engine" if "engine.vm" in url else "persistence"}).encode()))
    hosts = workers.wake()
    assert hosts == {role: f"http://{role}.vm.test-app.internal:8000" for role in ("engine", "persistence")}
    assert workers.request.call_count == 3


def test_fly_startup_timeout_is_bounded(monkeypatch):
    monkeypatch.setenv("FLY_APP_NAME", "test-app")
    monkeypatch.setenv("FLY_WORKER_TOKEN", "test-token")
    workers = FlyWorkers()
    machines = [{"id": role, "state": "started", "config": {"metadata": {"fly_process_group": role}}} for role in ("engine", "persistence")]
    workers.request = Mock(side_effect=[machines, machines[0]])
    monkeypatch.setattr(fly_workers, "monotonic", Mock(side_effect=[0, 1, 46]))
    monkeypatch.setattr(fly_workers, "sleep", Mock())
    monkeypatch.setattr(fly_workers, "urlopen", Mock(side_effect=URLError("unavailable")))
    with pytest.raises(RuntimeError, match="startup timed out"):
        workers.wake()


def test_fly_dispatches_engine_before_persistence(monkeypatch):
    monkeypatch.setenv("FLY_APP_NAME", "test-app")
    monkeypatch.setenv("FLY_WORKER_TOKEN", "test-token")
    requests = []

    def respond(request, timeout):
        requests.append(request)
        assert timeout == 60
        return io.BytesIO(b'{"status":"completed"}')

    monkeypatch.setattr(fly_workers, "urlopen", respond)
    run_id = uuid4()
    FlyWorkers().execute({"engine": "http://engine", "persistence": "http://persistence"}, run_id, 0)
    assert [request.full_url for request in requests] == ["http://engine/jobs", "http://persistence/jobs"]
    assert json.loads(requests[0].data) == {"run_id": str(run_id), "partition": 0}


@pytest.mark.parametrize("failure", ["budget", "redis", "startup"])
def test_public_replay_failure_does_not_publish_unowned_work(monkeypatch, failure):
    from redis.exceptions import ConnectionError

    monkeypatch.setenv("APP_MODE", "public_replay")
    monkeypatch.setenv("FLY_WORKER_LIFECYCLE", "on")
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    workers = Mock()
    if failure == "startup":
        workers.wake.side_effect = RuntimeError("startup failed")
    monkeypatch.setattr(api_service, "FlyWorkers", lambda: workers)
    publish = Mock()
    monkeypatch.setattr(api_service, "publish_replay", publish)
    app = api_service.create_app("postgresql+psycopg://unused:unused@localhost/unused", "redis://localhost", public_request_limit=0)
    app.state.public_request_limit = None
    app.state.redis.eval = Mock(return_value=0 if failure == "budget" else 1)
    if failure == "redis":
        app.state.redis.eval.side_effect = ConnectionError("offline")
    with TestClient(app) as client:
        assert client.get("/api/v1/configuration").status_code == 200
        assert client.get("/api/v1/scenarios").status_code == 200
        response = client.post("/api/v1/replays", json={"scenario_name": "complete_market_fill"})
    assert response.status_code == (429 if failure == "budget" else 503)
    publish.assert_not_called()
    workers.execute.assert_not_called()
    if failure != "startup":
        workers.wake.assert_not_called()
