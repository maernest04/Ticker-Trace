import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from threading import Event, Thread
from time import monotonic
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from market_execution_lab.engine import ExecutionEngine, _is_fill_eligible, simulate
from market_execution_lab.fixtures import ScenarioFixture
from market_execution_lab.models import OrderCommand, OrderSide, OrderType, QuoteEvent
from market_execution_lab.pipeline import _result_payload, publish_replay
from market_execution_lab.recordings import MAX_EVENTS, MODEL_VERSION, RecordingStore
from market_execution_lab.streaming import event_from_json


class RecordedOrder(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_type: OrderType = OrderType.MARKET
    quantity: int = Field(default=50, ge=1, le=10_000)
    latency_ms: int = Field(default=0, ge=0, le=60_000)
    limit_price: Decimal | None = Field(default=None, gt=0, max_digits=18, decimal_places=6)

    @model_validator(mode="after")
    def valid_price(self):
        if (self.order_type == OrderType.LIMIT) != (self.limit_price is not None):
            raise ValueError("only limit orders require a limit price")
        return self


class ExperimentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    recording_id: UUID
    symbol: str = Field(min_length=1, max_length=10)
    entry_index: int = Field(ge=0)
    side: OrderSide = OrderSide.BUY
    baseline: RecordedOrder
    variant: RecordedOrder | None = None
    publication_speed: Literal["1", "5", "20", "max"] = "max"

    @model_validator(mode="after")
    def one_difference(self):
        if self.variant is None:
            return self
        first, second = self.baseline.model_dump(), self.variant.model_dump()
        changed = {key for key in first if first[key] != second[key]}
        if first["order_type"] != second["order_type"]:
            changed.discard("limit_price")
        if len(changed) != 1:
            raise ValueError("comparison must vary exactly one parameter")
        return self


def experiment_scenarios(manifest, entries, request: ExperimentRequest):
    events = tuple(event_from_json(json.dumps(entry.event)) for entry in entries if entry.event["symbol"] == request.symbol)
    if not events or request.entry_index >= len(events):
        raise ValueError("select a symbol and entry boundary inside this recording")
    anchor = events[request.entry_index].event_time
    scenarios = []
    for config in (request.baseline, request.variant):
        if config is None:
            continue
        run_id = uuid4()
        order = OrderCommand(order_id=uuid4(), run_id=run_id, symbol=request.symbol, side=request.side, submitted_at=anchor, **config.model_dump())
        scenarios.append(ScenarioFixture(name=f"recorded:{manifest.recording_id}", order=order,
            events=tuple(event.model_copy(update={"run_id": run_id, "ingested_at": datetime.now(UTC)}) for event in events), entry_index=request.entry_index))
    return tuple(scenarios)


def outcome(scenario):
    result = simulate(scenario.order, scenario.events, scenario.entry_index)
    payload = _result_payload(result)
    for fill in payload["fills"]:
        fill.pop("fill_id")
        fill.pop("order_id")
    for transition in payload["transitions"]:
        transition.pop("order_id")
    payload["metrics"]["entry_to_first_fill_ms"] = (result.fills[0].filled_at - scenario.order.submitted_at).total_seconds() * 1000 if result.fills else None
    payload["metrics"]["entry_to_completion_ms"] = (result.fills[-1].filled_at - scenario.order.submitted_at).total_seconds() * 1000 if result.remaining_quantity == 0 else None
    return payload


def explain_comparison(scenarios):
    if not 1 <= len(scenarios) <= 2:
        raise ValueError("one or two experiments are required")
    first = scenarios[0]
    identity = [(event.event_id, event.model_dump(mode="json", exclude={"run_id", "ingested_at"})) for event in first.events]
    for scenario in scenarios[1:]:
        if scenario.entry_index != first.entry_index or scenario.order.submitted_at != first.order.submitted_at or scenario.order.side != first.order.side or scenario.order.symbol != first.order.symbol or [(event.event_id, event.model_dump(mode="json", exclude={"run_id", "ingested_at"})) for event in scenario.events] != identity:
            raise ValueError("controlled experiments must share source input, side, and entry")
    engines = [ExecutionEngine(scenario.order) for scenario in scenarios]
    trace, first_state, first_fill = [], None, None
    for index in range(len(scenarios[0].events)):
        decisions = []
        for engine, scenario in zip(engines, scenarios):
            event = scenario.events[index]
            before = engine.remaining_quantity
            accepted = engine.process(event, execute=index >= scenario.entry_index)
            active = engine.state.value != "submitted"
            eligible = accepted and index >= scenario.entry_index and active and before > 0 and isinstance(event, QuoteEvent) and _is_fill_eligible(engine.order, engine.market_state)
            delta = before - engine.remaining_quantity
            decisions.append({"state": engine.state.value, "eligible": eligible,
                "filled_quantity": engine.order.quantity - engine.remaining_quantity, "event_fill_quantity": delta,
                "event_fill_price": str(event.ask_price if engine.order.side == OrderSide.BUY else event.bid_price) if delta else None})
        if index < scenarios[0].entry_index:
            continue
        row = {"index": index, "event_id": scenarios[0].events[index].event_id, "decisions": decisions}
        trace.append(row)
        if len(decisions) == 2:
            if first_state is None and any(decisions[0][key] != decisions[1][key] for key in ("state", "eligible", "filled_quantity")):
                first_state = row
            if first_fill is None and any(decisions[0][key] != decisions[1][key] for key in ("event_fill_quantity", "event_fill_price")):
                first_fill = row
    results = [outcome(scenario) for scenario in scenarios]
    explanation = None
    if first_state or first_fill:
        point = first_state or first_fill
        event = scenarios[0].events[point["index"]]
        a, b = (scenario.order for scenario in scenarios)
        if a.latency_ms != b.latency_ms:
            cause = "activation delay"
        elif a.order_type != b.order_type or a.limit_price != b.limit_price:
            cause = "limit eligibility"
        else:
            cause = "order quantity versus displayed liquidity"
        explanation = {"cause": cause, "source_index": point["index"], "source_event": event.model_dump(mode="json"), "decisions": point["decisions"],
            "configurations": [scenario.order.model_dump(mode="json") for scenario in scenarios],
            "rule": "Only eligible post-entry quotes can fill at the displayed side price, up to visible size and remaining quantity."}
    deltas = {}
    if len(results) == 2:
        for key, first in results[0]["metrics"].items():
            second = results[1]["metrics"].get(key)
            deltas[key] = str(Decimal(str(second)) - Decimal(str(first))) if first is not None and second is not None else None
    final_signatures = [(result["state"], result["remaining_quantity"], result["fills"]) for result in results]
    return {"outcomes": results, "trace": trace, "first_state_divergence": first_state,
        "first_fill_divergence": first_fill, "explanation": explanation, "deltas": deltas,
        "outcome_differs": len(results) == 2 and final_signatures[0] != final_signatures[1]}


class ExperimentService:
    def __init__(self, files: RecordingStore, redis, store, redis_url: str, database_url: str):
        self.files, self.redis, self.store = files, redis, store
        self.redis_url, self.database_url = redis_url, database_url
        self.thread = None
        self.stop = Event()
        for path in files.directory.glob("*.experiment.json"):
            value = json.loads(path.read_bytes())
            if value["status"] in {"queued", "running"}:
                value.update(status="failed", error="application restart interrupted experiment; create a new isolated run")
                files.save_json(UUID(value["experiment_id"]), "experiment.json", value)

    def submit(self, request: ExperimentRequest):
        manifest, entries = self.files.load(request.recording_id)
        scenarios = experiment_scenarios(manifest, entries, request)
        with self.files.locked():
            if self.thread and self.thread.is_alive():
                raise ValueError("another recorded experiment is running")
            if any(value.status in {"capturing", "finalizing"} for value in self.files.list()):
                raise ValueError("finish the active recording before starting an experiment")
            if len(list(self.files.directory.glob("*.experiment.json"))) >= 10:
                raise ValueError("ten saved experiments reached; archive local results before further runs")
            if self.files.used_bytes() + 64 * 1024 * 1024 > 256 * 1024 * 1024:
                raise ValueError("insufficient experiment storage headroom")
            identity = uuid4()
            job = {"experiment_id": str(identity), "status": "queued", "request": request.model_dump(mode="json"),
                "checksum": manifest.checksum, "model_version": MODEL_VERSION, "entry_market_time": scenarios[0].order.submitted_at.isoformat(),
                "runs": [{"run_id": str(scenario.order.run_id), "order_id": str(scenario.order.order_id)} for scenario in scenarios], "error": None}
            self.files.save_json(identity, "experiment.json", job)
            self.stop.clear()
            self.thread = Thread(target=self._run, args=(identity, job, scenarios, request), daemon=True)
            self.thread.start()
            return job

    def get(self, identity: UUID):
        return json.loads(self.files.path(identity, "experiment.json").read_bytes())

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=15)

    def _run(self, identity, job, scenarios, request):
        try:
            job["status"] = "running"
            self.files.save_json(identity, "experiment.json", job)
            environment = {key: value for key, value in os.environ.items() if not key.startswith("ALPACA_")}
            environment.update(REDIS_URL=self.redis_url, DATABASE_URL=self.database_url)
            for scenario in scenarios:
                if self.stop.is_set():
                    raise RuntimeError("application is stopping")
                partition = publish_replay(self.redis, scenario, max_queue_depth=MAX_EVENTS + 3, mode="private_recorded", dispatch=False,
                    playback_speed=None if request.publication_speed == "max" else float(request.publication_speed), stop=self.stop)
                for role in ("engine", "persistence"):
                    arguments = [sys.executable, "-m", f"market_execution_lab.{role}_service", "--run-id", str(scenario.order.run_id), "--partition", str(partition), "--recovery-idle-ms", "0"]
                    for attempt in range(2):
                        process = subprocess.Popen(arguments, env=environment, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        deadline = monotonic() + 900
                        while process.poll() is None and not self.stop.wait(0.1) and monotonic() < deadline:
                            pass
                        if process.poll() is None:
                            process.terminate()
                            try:
                                process.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait(timeout=5)
                            raise RuntimeError("worker interrupted or timed out")
                        if process.returncode == 0:
                            break
                    else:
                        raise RuntimeError(f"{role} process failed after bounded retry")
                durable = self.store.order_for_id(scenario.order.order_id)
                expected = outcome(scenario)
                if self.store.counts_for_run(scenario.order.run_id)["events"] != len({event.event_id for event in scenario.events}):
                    raise RuntimeError("durable replay source is incomplete")
                if not durable or durable["final_state"] != expected["state"] or durable["remaining_quantity"] != expected["remaining_quantity"] or [(fill["triggering_event_id"], str(Decimal(str(fill["price"])).normalize()), fill["quantity"]) for fill in durable["fills"]] != [(fill["triggering_event_id"], str(Decimal(fill["price"]).normalize()), fill["quantity"]) for fill in expected["fills"]]:
                    raise RuntimeError("durable execution does not match canonical replay")
            comparison = explain_comparison(scenarios)
            explanation = comparison["explanation"]
            if explanation:
                _, source_entries = self.files.load(request.recording_id)
                original = [entry for entry in source_entries if entry.event["symbol"] == request.symbol][explanation["source_index"]]
                explanation["source_event"] = original.event
                explanation["source_cursor"] = original.cursor
            self.files.save_json(identity, "trace.json", {"trace": comparison.pop("trace")})
            job.update(status="completed", result=comparison)
        except Exception as error:
            reason = str(error) if isinstance(error, RuntimeError) else type(error).__name__
            job.update(status="failed", error=f"recorded experiment failed: {reason}")
        self.files.save_json(identity, "experiment.json", job)
