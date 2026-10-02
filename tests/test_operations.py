import json
import logging
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy.exc import OperationalError

from market_execution_lab import api_service, observability, worker_service
from market_execution_lab.observability import JsonFormatter, request_id_context
from market_execution_lab.worker_service import create_worker_app


@pytest.mark.parametrize("failure", [RedisConnectionError("secret-token"), OperationalError("SELECT secret", {}, Exception("secret-password"))])
def test_worker_dependency_failures_are_safe_correlated_and_recoverable(monkeypatch, caplog, failure):
    caplog.set_level(logging.INFO, logger="market_execution_lab")
    monkeypatch.setattr(caplog.handler, "formatter", JsonFormatter())
    execute = Mock(side_effect=[failure, None])
    monkeypatch.setattr(worker_service, "run_persistence", execute)
    payload = {"run_id": str(uuid4()), "partition": 0}
    with TestClient(create_worker_app("persistence", Mock(), Mock(), Mock())) as client:
        response = client.post("/jobs", json=payload, headers={"x-request-id": "recovery-123"})
        assert response.status_code == 503
        assert "secret" not in response.text
        assert client.get("/health").status_code == 200
        assert client.post("/jobs", json=payload).status_code == 200
    failures = [record for record in caplog.records if record.getMessage() == "worker_job_failed"]
    assert len(failures) == 1
    assert failures[0].fields == {"role": "persistence", "run_id": payload["run_id"], "partition": 0, "error_type": type(failure).__name__}
    assert "secret" not in JsonFormatter().format(failures[0])
    captured = [json.loads(line) for line in caplog.text.splitlines()]
    assert next(record for record in captured if record["event"] == "worker_job_failed")["request_id"] == "recovery-123"
    assert next(record for record in captured if record["event"] == "worker_job_completed")["request_id"] == "-"


@pytest.mark.parametrize("failure", [RedisConnectionError("secret-token"), OperationalError("SELECT secret", {}, Exception("secret-password"))])
def test_api_dependency_outage_returns_safe_error_and_recovers(monkeypatch, failure):
    monkeypatch.setenv("APP_MODE", "public_replay")
    monkeypatch.delenv("FLY_WORKER_LIFECYCLE", raising=False)
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    store = Mock()
    store.list_watchlists.side_effect = [failure, []]
    monkeypatch.setattr(api_service, "DatabaseStore", lambda engine: store)
    app = api_service.create_app("postgresql+psycopg://unused", "redis://unused")
    app.state.public_request_limit = None
    with TestClient(app) as client:
        response = client.get("/api/v1/watchlists", headers={"x-request-id": "outage-123"})
        assert response.status_code == 503
        assert response.json()["error"]["request_id"] == "outage-123"
        assert response.headers["x-request-id"] == "outage-123"
        assert "secret" not in response.text
        assert client.get("/health").status_code == 200
        assert client.get("/api/v1/watchlists").json() == []


def test_websocket_database_failure_closes_with_retryable_code(monkeypatch):
    monkeypatch.setenv("APP_MODE", "public_replay")
    monkeypatch.delenv("FLY_WORKER_LIFECYCLE", raising=False)
    store = Mock()
    store.replay_for_run.side_effect = OperationalError("SELECT secret", {}, Exception("secret-password"))
    monkeypatch.setattr(api_service, "DatabaseStore", lambda engine: store)
    app = api_service.create_app("postgresql+psycopg://unused", "redis://unused")
    with TestClient(app) as client:
        with client.websocket_connect(f"/ws/v1/sessions/{uuid4()}") as socket:
            assert socket.receive() == {"type": "websocket.close", "code": 1013, "reason": ""}


def test_ready_recovers_without_restarting_api(monkeypatch):
    monkeypatch.delenv("FLY_WORKER_LIFECYCLE", raising=False)
    app = api_service.create_app("postgresql+psycopg://unused", "redis://unused")
    app.state.redis.ping = Mock(side_effect=[RedisConnectionError("secret"), True])
    with TestClient(app) as client:
        assert client.get("/ready").status_code == 503
        assert client.get("/health").status_code == 200
        assert client.get("/ready").json() == {"status": "ready"}


def test_metrics_timestamps_are_comparable_between_processes(monkeypatch):
    monkeypatch.setattr(observability, "time", lambda: 1_790_000_000.0)
    assert observability.now_seconds() == 1_790_000_000.0
    redis = Mock()
    redis.xinfo_groups.return_value = []
    redis.hgetall.return_value = {"published_at": "1790000000", "engine_completed_at": "1790000002", "processed_events": "10", "engine_processing_ms": "20"}
    metrics = observability.pipeline_metrics(redis, "stream", "run", 0)
    assert metrics.throughput_events_per_second == 5
    assert metrics.processing_latency_ms == 20


def test_worker_dispatch_propagates_request_id(monkeypatch):
    import io
    from market_execution_lab import fly_workers

    monkeypatch.setenv("FLY_APP_NAME", "test-app")
    monkeypatch.setenv("FLY_WORKER_TOKEN", "test-token")
    requests = []

    def respond(request, timeout):
        requests.append(request)
        return io.BytesIO(json.dumps({"status": "completed"}).encode())

    monkeypatch.setattr(fly_workers, "urlopen", respond)
    token = request_id_context.set("dispatch-123")
    try:
        fly_workers.FlyWorkers().execute({"engine": "http://engine", "persistence": "http://persistence"}, uuid4(), 0)
    finally:
        request_id_context.reset(token)
    assert [request.get_header("X-request-id") for request in requests] == ["dispatch-123", "dispatch-123"]


def test_completed_persistence_retry_returns_success_without_reprocessing(monkeypatch):
    execute = Mock()
    monkeypatch.setattr(worker_service, "run_persistence", execute)
    store, redis = Mock(), Mock()
    store.replay_for_run.return_value = {"status": "completed"}
    with TestClient(create_worker_app("persistence", redis, store, Mock())) as client:
        assert client.post("/jobs", json={"run_id": str(uuid4()), "partition": 0}).json() == {"status": "completed"}
    execute.assert_not_called()
    assert not redis.mock_calls
