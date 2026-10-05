import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from time import monotonic, sleep, time
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient
from redis import Redis

from market_execution_lab.api_service import create_app
from market_execution_lab.engine import simulate
from market_execution_lab.models import TradeEvent
from market_execution_lab.fixtures import generated_scenarios
from market_execution_lab.live_pipeline import LIVE_SESSION_KEY
from market_execution_lab.pipeline import partition_for_symbol, publish_replay, run_engine, run_persistence
from market_execution_lab.recorded_replay import ExperimentRequest, ExperimentService, RecordedOrder, explain_comparison, experiment_scenarios
from market_execution_lab.recordings import CaptureService, RecordedEntry, RecordingManifest, RecordingStore
from market_execution_lab.storage import DatabaseStore
from market_execution_lab.streaming import partition_stream_name


def generated_recording(directory, scenario=None):
    scenario = scenario or generated_scenarios()[2]
    files = RecordingStore(directory)
    identity = uuid4()
    partition = partition_for_symbol(scenario.order.symbol)
    entries = tuple(RecordedEntry(partition=partition, cursor=f"{index + 1}-0", event=event.model_copy(update={"partition": partition}).model_dump(mode="json")) for index, event in enumerate(scenario.events))
    raw = b"".join(entry.model_dump_json().encode() + b"\n" for entry in entries)
    files.path(identity, "events.jsonl").write_bytes(raw)
    manifest = RecordingManifest(recording_id=identity, source_run_id=scenario.order.run_id, feed="generated", symbols=[scenario.order.symbol], status="ready", started_at=datetime.now(UTC), ended_at=datetime.now(UTC), reason="generated test fixture", boundaries={str(partition): {"start": "0-0", "end": f"{len(entries)}-0"}}, event_count=len(entries), byte_count=len(raw), checksum=sha256(raw).hexdigest(), quality=["Generated test input, not a vendor capture"])
    files.save_manifest(manifest)
    return files, manifest


def request_for(manifest, **changes):
    return ExperimentRequest(recording_id=manifest.recording_id, symbol=manifest.symbols[0], entry_index=0, baseline=RecordedOrder(), **changes)


def wait_for(predicate, seconds=20):
    deadline = monotonic() + seconds
    while monotonic() < deadline:
        value = predicate()
        if value:
            return value
        sleep(0.05)
    pytest.fail("bounded recorded test deadline expired")


def test_recording_contract_and_checksum(tmp_path):
    files, manifest = generated_recording(tmp_path)
    assert files.load(manifest.recording_id)[0] == manifest
    path = files.path(manifest.recording_id, "events.jsonl")
    path.write_bytes(path.read_bytes()[:-1])
    with pytest.raises(ValueError, match="byte count"):
        files.load(manifest.recording_id)


@pytest.mark.parametrize("change", [{"schema_version": "v2"}, {"boundaries": {}}, {"boundaries": {"0": {"start": "2-0", "end": "1-0"}}}, {"units": "lots"}, {"symbols": ["aapl"]}, {"checksum": "bad"}, {"started_at": "2026-01-01T00:00:00"}, {"ended_at": None}, {"event_count": 0}])
def test_manifest_rejects_ambiguous_metadata(tmp_path, change):
    _, manifest = generated_recording(tmp_path)
    with pytest.raises(ValueError):
        RecordingManifest.model_validate({**manifest.model_dump(mode="json"), **change})


@pytest.mark.parametrize("mutation", ["checksum", "ordering", "symbol", "run", "truncated", "version"])
def test_event_artifact_validation(tmp_path, mutation):
    files, manifest = generated_recording(tmp_path)
    path = files.path(manifest.recording_id, "events.jsonl")
    lines = [json.loads(line) for line in path.read_bytes().splitlines()]
    if mutation == "ordering": lines.reverse()
    if mutation == "symbol": lines[0]["event"]["symbol"] = "AAPL"
    if mutation == "run": lines[0]["event"]["run_id"] = str(uuid4())
    if mutation == "version": lines[0]["event"]["schema_version"] = "v2"
    raw = b"".join(json.dumps(line).encode() + b"\n" for line in lines)
    if mutation == "truncated": raw = raw[:-3]
    path.write_bytes(raw)
    update = {"byte_count": len(raw)}
    if mutation != "checksum": update["checksum"] = sha256(raw).hexdigest()
    files.save_manifest(manifest.model_copy(update=update))
    with pytest.raises(ValueError): files.load(manifest.recording_id)


def test_entry_boundary_excludes_equal_time_pre_entry_fill(tmp_path):
    base = generated_scenarios()[0]
    first = base.events[0]
    second = first.model_copy(update={"event_id": "second", "sequence": 2, "ask_price": first.ask_price + 1})
    files, manifest = generated_recording(tmp_path, replace(base, events=(first, second)))
    _, entries = files.load(manifest.recording_id)
    request = request_for(manifest).model_copy(update={"entry_index": 1})
    scenario = experiment_scenarios(manifest, entries, request)[0]
    result = simulate(scenario.order, scenario.events, scenario.entry_index)
    assert len(result.fills) == 1
    assert result.fills[0].triggering_event_id == "second"
    assert result.fills[0].price == second.ask_price


@pytest.mark.parametrize("variant", [{"latency_ms": 100}, {"quantity": 150}, {"order_type": "limit", "limit_price": "499"}])
def test_first_divergence_matches_engine_and_swapped_deltas(tmp_path, variant):
    files, manifest = generated_recording(tmp_path)
    _, entries = files.load(manifest.recording_id)
    request = request_for(manifest, variant=RecordedOrder(**variant))
    scenarios = experiment_scenarios(manifest, entries, request)
    comparison = explain_comparison(scenarios)
    assert comparison["explanation"] is not None
    assert comparison["first_state_divergence"]["index"] == 0
    reversed_result = explain_comparison(tuple(reversed(scenarios)))
    assert reversed_result["explanation"]["source_event"]["event_id"] == comparison["explanation"]["source_event"]["event_id"]
    for key, delta in comparison["deltas"].items():
        if delta is not None:
            from decimal import Decimal
            assert Decimal(reversed_result["deltas"][key]) == -Decimal(delta)
    for index, scenario in enumerate(scenarios):
        expected = simulate(scenario.order, scenario.events, scenario.entry_index)
        assert comparison["outcomes"][index]["remaining_quantity"] == expected.remaining_quantity


def test_identical_control_and_multi_parameter_rejection(tmp_path):
    files, manifest = generated_recording(tmp_path)
    _, entries = files.load(manifest.recording_id)
    single = experiment_scenarios(manifest, entries, request_for(manifest))[0]
    assert explain_comparison((single, single))["explanation"] is None
    assert not explain_comparison((single, single))["outcome_differs"]
    with pytest.raises(ValueError): request_for(manifest, variant=RecordedOrder())
    with pytest.raises(ValueError): request_for(manifest, variant=RecordedOrder(quantity=100, latency_ms=5))


def test_state_divergence_can_have_identical_final_fills(tmp_path):
    base = generated_scenarios()[0]
    quote = base.events[0]
    early = TradeEvent(**quote.model_dump(exclude={"event_type", "bid_price", "bid_size", "ask_price", "ask_size"}), price="200", size=1)
    later = quote.model_copy(update={"event_id": "later", "sequence": 2, "event_time": quote.event_time + timedelta(milliseconds=200)})
    files, manifest = generated_recording(tmp_path, replace(base, events=(early, later)))
    _, entries = files.load(manifest.recording_id)
    scenarios = experiment_scenarios(manifest, entries, request_for(manifest, variant=RecordedOrder(latency_ms=100)))
    result = explain_comparison(scenarios)
    assert result["first_state_divergence"]["index"] == 0
    assert result["first_fill_divergence"] is None
    assert not result["outcome_differs"]
    assert result["deltas"]["entry_to_first_fill_ms"] == "0.0"
    changed = replace(scenarios[1], entry_index=1)
    with pytest.raises(ValueError, match="share source"):
        explain_comparison((scenarios[0], changed))


def test_unfilled_comparison_does_not_invent_metrics(tmp_path):
    files, manifest = generated_recording(tmp_path)
    _, entries = files.load(manifest.recording_id)
    request = ExperimentRequest(recording_id=manifest.recording_id, symbol="META", entry_index=0,
        baseline=RecordedOrder(order_type="limit", limit_price="400"), variant=RecordedOrder(order_type="limit", limit_price="401"))
    result = explain_comparison(experiment_scenarios(manifest, entries, request))
    assert result["explanation"] is None
    assert result["deltas"]["average_fill_price"] is None
    assert all(outcome["metrics"]["entry_to_completion_ms"] is None for outcome in result["outcomes"])


def test_limit_price_change_has_a_rule_linked_divergence(tmp_path):
    files, manifest = generated_recording(tmp_path)
    _, entries = files.load(manifest.recording_id)
    request = ExperimentRequest(recording_id=manifest.recording_id, symbol="META", entry_index=0,
        baseline=RecordedOrder(order_type="limit", limit_price="499"), variant=RecordedOrder(order_type="limit", limit_price="500"))
    result = explain_comparison(experiment_scenarios(manifest, entries, request))
    assert result["explanation"]["cause"] == "limit eligibility"
    assert result["first_fill_divergence"]["index"] == 0


def test_stale_duplicate_input_is_preserved_not_sorted(tmp_path):
    base = generated_scenarios()[2]
    events = (base.events[1], base.events[0], base.events[1], base.events[2])
    files, manifest = generated_recording(tmp_path, replace(base, events=events))
    _, entries = files.load(manifest.recording_id)
    scenario = experiment_scenarios(manifest, entries, request_for(manifest))[0]
    assert [event.event_id for event in scenario.events] == [event.event_id for event in events]
    result = simulate(scenario.order, scenario.events)
    assert result.stale_events == 1
    assert result.duplicate_events == 1


def test_limits_unknown_identity_and_truncated_manifest(tmp_path, monkeypatch):
    from market_execution_lab import recordings
    files, manifest = generated_recording(tmp_path)
    with pytest.raises(FileNotFoundError): files.load(uuid4())
    monkeypatch.setattr(recordings, "MAX_BYTES", 10)
    with pytest.raises(ValueError, match="byte count"): files.load(manifest.recording_id)
    with pytest.raises(ValueError, match="storage limit"): files.save_json(uuid4(), "experiment.json", {"oversized": "x" * 20})


@pytest.fixture
def isolated():
    if os.getenv("RUN_STREAMING_INTEGRATION") != "1": pytest.skip("requires isolated local Redis/PostgreSQL")
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    database = sa.create_engine(os.environ["DATABASE_URL"])
    store = DatabaseStore(database)
    try: yield redis, store
    finally: database.dispose()


def test_captured_prefix_finalizes_and_restart_is_incomplete(tmp_path, isolated):
    redis, _ = isolated
    files = RecordingStore(tmp_path)
    scenario = generated_scenarios()[0]
    run_id = uuid4()
    partition = partition_for_symbol("AAPL")
    stream = partition_stream_name(str(run_id), partition)
    redis.xadd(stream, {"message_type": "replay.started.v1", "payload": "{}"})
    redis.hset(LIVE_SESSION_KEY, mapping={"run_id": str(run_id), "symbols": '["AAPL"]', "status": "connected", "received_at": str(time())})
    service = CaptureService(redis, files)
    try:
        with pytest.raises(ValueError, match="confirm"): service.start(["AAPL"], 1, False)
        recording = service.start(["AAPL"], 1, True)
        with pytest.raises(ValueError, match="already"): service.start(["AAPL"], 1, True)
        for index in range(3):
            event = scenario.events[0].model_copy(update={"run_id": run_id, "partition": partition, "event_id": f"generated:{index}", "sequence": index})
            redis.xadd(stream, {"message_type": "market.event.v1", "payload": event.model_dump_json()})
        service.request_stop(recording.recording_id)
        wait_for(lambda: files.manifest(recording.recording_id).status == "ready")
        assert files.load(recording.recording_id)[0].event_count == 3
        before = redis.xlen(stream)
        files.load(recording.recording_id)
        assert redis.xlen(stream) == before
    finally:
        service.close()
        redis.delete(LIVE_SESSION_KEY)
    files.save_manifest(recording.model_copy(update={"status": "capturing"}))
    recovered = CaptureService(redis, files)
    try: assert files.manifest(recording.recording_id).status == "incomplete"
    finally: recovered.close()


@pytest.mark.parametrize("ending", ["cap", "rollover", "trim"])
def test_capture_bounds_and_interruption_fail_closed(tmp_path, isolated, monkeypatch, ending):
    from market_execution_lab import recordings
    redis, _ = isolated
    files = RecordingStore(tmp_path)
    run_id, partition = uuid4(), partition_for_symbol("AAPL")
    stream = partition_stream_name(str(run_id), partition)
    cursor = redis.xadd(stream, {"message_type": "replay.started.v1", "payload": "{}"})
    redis.hset(LIVE_SESSION_KEY, mapping={"run_id": str(run_id), "symbols": '["AAPL"]', "status": "connected", "phase": "running", "received_at": str(time())})
    monkeypatch.setattr(recordings, "MAX_EVENTS", 2)
    service = CaptureService(redis, files)
    try:
        manifest = service.start(["AAPL"], 3, True)
        if ending == "rollover": redis.hset(LIVE_SESSION_KEY, "run_id", str(uuid4()))
        if ending == "trim": redis.xdel(stream, cursor)
        for index in range(4):
            event = generated_scenarios()[0].events[0].model_copy(update={"run_id": run_id, "partition": partition, "event_id": f"generated-cap:{index}", "sequence": index})
            redis.xadd(stream, {"message_type": "market.event.v1", "payload": event.model_dump_json()})
        value = wait_for(lambda: (current if (current := files.manifest(manifest.recording_id)).status in {"ready", "incomplete", "failed"} else None))
        if ending == "cap":
            assert value.status == "ready"
            assert files.load(manifest.recording_id)[0].event_count == 2
        else:
            assert value.status != "ready"
            with pytest.raises(ValueError, match="not ready"): files.load(manifest.recording_id)
    finally:
        service.close()
        redis.delete(LIVE_SESSION_KEY)


@pytest.mark.parametrize("speed", [None, 5, 20])
def test_recorded_pipeline_replacement_preserves_fills(tmp_path, isolated, speed):
    redis, store = isolated
    files, manifest = generated_recording(tmp_path)
    _, entries = files.load(manifest.recording_id)
    request = request_for(manifest).model_copy(update={"entry_index": 1})
    scenario = experiment_scenarios(manifest, entries, request)[0]
    partition = publish_replay(redis, scenario, mode="private_recorded", dispatch=False, playback_speed=speed)
    expected = run_engine(redis, scenario.order.run_id, partition, "first")
    run_persistence(redis, store, scenario.order.run_id, partition, "first")
    original = store.order_for_id(scenario.order.order_id)["fills"]
    assert run_engine(redis, scenario.order.run_id, partition, "replacement") == expected
    run_persistence(redis, store, scenario.order.run_id, partition, "replacement", recovery_idle_ms=0)
    assert store.order_for_id(scenario.order.order_id)["fills"] == original
    assert store.counts_for_run(scenario.order.run_id)["fills"] == len(expected.fills)


def test_private_api_offline_comparison_and_public_isolation(tmp_path, isolated, monkeypatch):
    redis, store = isolated
    files, manifest = generated_recording(tmp_path)
    monkeypatch.setenv("RECORDING_DIRECTORY", str(tmp_path))
    monkeypatch.setenv("APP_MODE", "private_live")
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    with TestClient(create_app(os.environ["DATABASE_URL"], os.environ["REDIS_URL"])) as client:
        assert client.get("/api/v1/recordings").status_code == 200
        request = request_for(manifest, variant=RecordedOrder(latency_ms=100))
        invalid = request.model_dump(mode="json")
        invalid["variant"] = invalid["baseline"]
        rejected = client.post("/api/v1/experiments", json=invalid)
        assert rejected.status_code == 422
        assert "exactly one parameter" in rejected.json()["error"]["message"]
        response = client.post("/api/v1/experiments", json=request.model_dump(mode="json"))
        assert response.status_code == 202, response.json()
        identity = response.json()["experiment_id"]
        result = wait_for(lambda: (value if (value := client.get(f"/api/v1/experiments/{identity}").json())["status"] in {"completed", "failed"} else None))
        assert result["status"] == "completed", result
        assert result["result"]["explanation"]["cause"] == "activation delay"
        assert client.get(f"/api/v1/experiments/{identity}/trace?limit=1").json()["trace"][0]["index"] == 0
        evidence = client.get(f"/api/v1/experiments/{identity}/fills?limit=1").json()
        assert evidence["fills"][0]["source"]["event"]["event_id"] == evidence["fills"][0]["triggering_event_id"]
        assert evidence["fills"][0]["source"]["event"]["run_id"] == str(manifest.source_run_id)
        assert client.get(f"/api/v1/recordings/{manifest.recording_id}/events?symbol=META&limit=201").status_code == 422
        run = UUID(result["runs"][0]["run_id"])
        assert store.replay_for_run(run)["settings"]["mode"] == "private_recorded"
    monkeypatch.setenv("APP_MODE", "public_replay")
    with TestClient(create_app(os.environ["DATABASE_URL"], os.environ["REDIS_URL"])) as public:
        for endpoint in ("recordings", f"recordings/{manifest.recording_id}", f"experiments/{identity}", f"replays/{run}", f"orders/{result['runs'][0]['order_id']}"):
            assert public.get(f"/api/v1/{endpoint}").status_code == 404
        with public.websocket_connect(f"/ws/v1/sessions/{run}") as socket:
            from starlette.websockets import WebSocketDisconnect
            with pytest.raises(WebSocketDisconnect): socket.receive_json()


def test_failed_worker_does_not_yield_successful_comparison(tmp_path, isolated, monkeypatch):
    from market_execution_lab import recorded_replay
    from unittest.mock import Mock
    redis, store = isolated
    files, manifest = generated_recording(tmp_path)
    worker = Mock(returncode=1)
    worker.poll.return_value = 1
    monkeypatch.setattr(recorded_replay.subprocess, "Popen", lambda *args, **kwargs: worker)
    service = ExperimentService(files, redis, store, os.environ["REDIS_URL"], os.environ["DATABASE_URL"])
    try:
        job = service.submit(request_for(manifest, variant=RecordedOrder(latency_ms=100)))
        identity = UUID(job["experiment_id"])
        failed = wait_for(lambda: (value if (value := service.get(identity))["status"] == "failed" else None))
        assert "engine process failed" in failed["error"]
        assert "result" not in failed
        assert not files.path(identity, "trace.json").exists()
    finally:
        service.close()
    failed["status"] = "running"
    files.save_json(identity, "experiment.json", failed)
    restarted = ExperimentService(files, redis, store, os.environ["REDIS_URL"], os.environ["DATABASE_URL"])
    try:
        assert restarted.get(identity)["status"] == "failed"
        assert "application restart" in restarted.get(identity)["error"]
    finally:
        restarted.close()
