import os
import json
import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
import sqlalchemy as sa
import uvicorn

from market_execution_lab.alpaca import INGESTION_CONTROL_STREAM
from market_execution_lab.fixtures import ScenarioFixture, generated_scenarios
from market_execution_lab.fly_workers import FlyWorkers
from market_execution_lab.models import OrderCommand, OrderSide, OrderType, QuoteEvent, TradeEvent
from market_execution_lab.observability import configure_logging, log_event, pipeline_metrics, request_id_context
from market_execution_lab.operations_service import create_app as create_operations_app
from market_execution_lab.pipeline import publish_replay
from market_execution_lab.storage import DatabaseStore, sqlalchemy_url
from market_execution_lab.streaming import market_state_key, partition_stream_name
from redis.exceptions import RedisError


class SymbolResponse(BaseModel):
    symbol: str


class ConfigurationResponse(BaseModel):
    mode: str


class ScenarioMarketResponse(BaseModel):
    event_time: datetime
    bid_price: Decimal | None
    bid_size: int | None
    ask_price: Decimal | None
    ask_size: int | None
    last_trade_price: Decimal | None


class ScenarioResponse(BaseModel):
    name: str
    symbol: str
    default_side: OrderSide
    default_order_type: OrderType
    default_quantity: int
    default_limit_price: Decimal | None
    default_latency_ms: int
    replay_change_percent: Decimal | None
    market: ScenarioMarketResponse


class MarketStateResponse(BaseModel):
    run_id: UUID
    symbol: str
    event_id: str
    event_time: datetime
    sequence: int
    last_trade_price: Decimal | None
    bid_price: Decimal | None
    bid_size: int | None
    ask_price: Decimal | None
    ask_size: int | None


class FillResponse(BaseModel):
    fill_id: str
    triggering_event_id: str
    quantity: int
    price: Decimal
    filled_at: datetime


class TransitionResponse(BaseModel):
    state: str
    changed_at: datetime
    triggering_event_id: str | None


class ExecutionMetricsResponse(BaseModel):
    average_fill_price: Decimal | None
    fill_rate: Decimal
    spread_cost: Decimal | None
    time_to_first_fill_ms: int | None
    time_to_completion_ms: int | None
    latency_impact: Decimal | None


class OrderResponse(BaseModel):
    order_id: UUID
    run_id: UUID
    symbol: str
    side: str
    order_type: str
    quantity: int
    limit_price: Decimal | None
    submitted_at: datetime
    latency_ms: int
    final_state: str | None
    remaining_quantity: int | None
    metrics: ExecutionMetricsResponse | None
    fills: list[FillResponse]
    transitions: list[TransitionResponse]


class ReplayCountsResponse(BaseModel):
    events: int
    orders: int
    fills: int
    transitions: int


class ReplaySettingsResponse(BaseModel):
    mode: str
    symbols: list[str]


class ReplayResponse(BaseModel):
    run_id: UUID
    scenario_name: str
    status: str
    started_at: datetime
    completed_at: datetime | None
    counts: ReplayCountsResponse
    settings: ReplaySettingsResponse | None


class MarketEventResponse(BaseModel):
    event_id: str
    event_type: str
    symbol: str
    event_time: datetime
    sequence: int
    bid_price: Decimal | None
    bid_size: int | None
    ask_price: Decimal | None
    ask_size: int | None
    price: Decimal | None
    size: int | None


class PipelineHealthResponse(BaseModel):
    queue_depth: int
    engine_lag: int
    engine_pending: int
    persistence_lag: int
    persistence_pending: int
    throughput_events_per_second: float
    processing_latency_ms: float | None


class WatchlistResponse(BaseModel):
    watchlist_id: UUID
    name: str
    created_at: datetime
    symbols: list[str]


class ReplayCommandRequest(BaseModel):
    scenario_name: str


class OrderCommandRequest(BaseModel):
    scenario_name: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: int = Field(gt=0)
    limit_price: Decimal | None = Field(default=None, gt=0)
    latency_ms: int = Field(default=0, ge=0)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @model_validator(mode="after")
    def validate_limit_price(self) -> "OrderCommandRequest":
        if self.order_type is OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit orders require limit_price")
        if self.order_type is OrderType.MARKET and self.limit_price is not None:
            raise ValueError("market orders cannot include limit_price")
        return self


class QueuedReplayResponse(BaseModel):
    run_id: UUID
    order_id: UUID
    partition: int
    status: str


class WatchlistCommandRequest(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    symbols: list[str] = Field(min_length=1)

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, values: list[str]) -> list[str]:
        symbols = sorted({value.strip().upper() for value in values if value.strip()})
        if not symbols:
            raise ValueError("watchlists require at least one symbol")
        return symbols


def create_app(database_url: str, redis_url: str, public_request_limit: int | None = None) -> FastAPI:
    mode = _application_mode()
    request_limit = public_request_limit if mode == "public_replay" else None
    if request_limit is None and mode == "public_replay":
        request_limit = int(os.getenv("PUBLIC_REQUEST_LIMIT", "60"))
    app = create_operations_app(redis_url, request_limit)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[origin.strip() for origin in os.getenv("PUBLIC_ALLOWED_ORIGINS", "http://localhost:3000").split(",") if origin.strip()],
        allow_methods=["GET", "POST", "PUT"],
        allow_headers=["content-type", "x-request-id"],
    )
    store = DatabaseStore(sa.create_engine(sqlalchemy_url(database_url), connect_args={"connect_timeout": 10}))
    redis = app.state.redis
    scenarios = {scenario.name: scenario for scenario in generated_scenarios()}
    workers = FlyWorkers() if os.getenv("FLY_WORKER_LIFECYCLE") == "on" else None

    def submit(queued: ScenarioFixture) -> int:
        try:
            if workers:
                bucket = datetime.now(UTC).strftime("%Y-%m")
                admitted = redis.eval(
                    "local n = tonumber(redis.call('GET', KEYS[1]) or '0'); "
                    "if n >= tonumber(ARGV[1]) then return 0 end; "
                    "redis.call('INCR', KEYS[1]); redis.call('EXPIRE', KEYS[1], 2764800); return 1",
                    1, f"public-replay-budget:{bucket}", 1000,
                )
                if not admitted:
                    raise HTTPException(429, "monthly demo replay allowance reached")
            hosts = workers.wake() if workers else None
            partition = publish_replay(redis, queued, mode=mode, dispatch=workers is None)
            if workers:
                workers.execute(hosts, queued.order.run_id, partition)
                try:
                    store.prune_public_replays(tuple(scenarios))
                except sa.exc.SQLAlchemyError:
                    log_event("public_replay_cleanup_failed", run_id=str(queued.order.run_id))
            return partition
        except (RuntimeError, OSError, RedisError) as error:
            raise HTTPException(503, "execution workers unavailable; try again shortly") from error

    @app.exception_handler(HTTPException)
    async def http_error(_: Request, error: HTTPException) -> JSONResponse:
        request_id = request_id_context.get()
        return JSONResponse(
            status_code=error.status_code,
            content={"error": {"code": "http_error", "message": str(error.detail), "request_id": request_id}},
            headers={"x-request-id": request_id},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, error: RequestValidationError) -> JSONResponse:
        request_id = request_id_context.get()
        return JSONResponse(
            status_code=422,
            content={"error": {"code": "validation_error", "message": "request validation failed", "request_id": request_id}},
            headers={"x-request-id": request_id},
        )

    @app.get("/api/v1/symbols", response_model=list[SymbolResponse])
    def symbols() -> list[SymbolResponse]:
        return [SymbolResponse(symbol=symbol) for symbol in store.list_symbols()]

    @app.get("/api/v1/configuration", response_model=ConfigurationResponse)
    def configuration() -> ConfigurationResponse:
        return ConfigurationResponse(mode=mode)

    @app.get("/api/v1/scenarios", response_model=list[ScenarioResponse])
    def scenario_list() -> list[ScenarioResponse]:
        return [_scenario_response(scenario) for scenario in scenarios.values()]

    @app.get("/api/v1/market/{symbol}", response_model=MarketStateResponse)
    def market(symbol: str, run_id: UUID) -> MarketStateResponse:
        state = redis.hgetall(market_state_key(str(run_id), symbol.upper()))
        if not state:
            raise HTTPException(status_code=404, detail="market state not found")
        return MarketStateResponse(
            run_id=run_id,
            symbol=symbol.upper(),
            event_id=state["event_id"],
            event_time=datetime.fromisoformat(state["event_time"]),
            sequence=int(state["sequence"]),
            last_trade_price=Decimal(state["last_trade_price"]) if state["last_trade_price"] else None,
            bid_price=Decimal(state["bid_price"]) if state["bid_price"] else None,
            bid_size=int(state["bid_size"]) if state["bid_size"] else None,
            ask_price=Decimal(state["ask_price"]) if state["ask_price"] else None,
            ask_size=int(state["ask_size"]) if state["ask_size"] else None,
        )

    @app.get("/api/v1/orders/{order_id}", response_model=OrderResponse)
    def order(order_id: UUID) -> OrderResponse:
        result = store.order_for_id(order_id)
        if result is None:
            raise HTTPException(status_code=404, detail="order not found")
        return OrderResponse.model_validate(result)

    @app.get("/api/v1/replays/{run_id}", response_model=ReplayResponse)
    def replay(run_id: UUID) -> ReplayResponse:
        return _replay_response(store, run_id)

    @app.get("/api/v1/replays/{run_id}/events", response_model=list[MarketEventResponse])
    def replay_events(run_id: UUID, limit: int = 200) -> list[MarketEventResponse]:
        _replay_response(store, run_id)
        return [_event_response(event) for event in store.events_for_run(run_id, min(limit, 500))]

    @app.get("/api/v1/replays/{run_id}/health", response_model=PipelineHealthResponse)
    def replay_health(run_id: UUID, partition: int) -> PipelineHealthResponse:
        _replay_response(store, run_id)
        result = pipeline_metrics(redis, partition_stream_name(str(run_id), partition), str(run_id), partition)
        return PipelineHealthResponse(
            queue_depth=result.queue_depth,
            engine_lag=result.engine_lag,
            engine_pending=result.engine_pending,
            persistence_lag=result.persistence_lag,
            persistence_pending=result.persistence_pending,
            throughput_events_per_second=result.throughput_events_per_second,
            processing_latency_ms=result.processing_latency_ms,
        )

    @app.get("/api/v1/watchlists", response_model=list[WatchlistResponse])
    def watchlists() -> list[WatchlistResponse]:
        return [WatchlistResponse.model_validate(watchlist) for watchlist in store.list_watchlists()]

    @app.post("/api/v1/replays", response_model=QueuedReplayResponse, status_code=202)
    def create_replay(request: ReplayCommandRequest) -> QueuedReplayResponse:
        scenario = _scenario_for_name(scenarios, request.scenario_name)
        order = scenario.order.model_copy(update={"run_id": uuid4(), "order_id": uuid4()})
        queued = _new_run(scenario, order)
        partition = submit(queued)
        return QueuedReplayResponse(
            run_id=queued.order.run_id,
            order_id=queued.order.order_id,
            partition=partition,
            status="queued",
        )

    @app.post("/api/v1/orders", response_model=QueuedReplayResponse, status_code=202)
    def create_order(request: OrderCommandRequest) -> QueuedReplayResponse:
        scenario = _scenario_for_name(scenarios, request.scenario_name)
        if request.symbol != scenario.order.symbol:
            raise HTTPException(status_code=422, detail="symbol does not match the selected replay scenario")
        order = OrderCommand(
            order_id=uuid4(),
            run_id=uuid4(),
            symbol=request.symbol,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            limit_price=request.limit_price,
            submitted_at=scenario.order.submitted_at,
            latency_ms=request.latency_ms,
        )
        queued = _new_run(scenario, order)
        partition = submit(queued)
        return QueuedReplayResponse(
            run_id=queued.order.run_id,
            order_id=queued.order.order_id,
            partition=partition,
            status="queued",
        )

    @app.post("/api/v1/watchlists", response_model=WatchlistResponse, status_code=201)
    def create_watchlist(request: WatchlistCommandRequest) -> WatchlistResponse:
        watchlist_id = uuid4()
        created_at = datetime.now(UTC)
        store.create_watchlist(watchlist_id, request.name, request.symbols, created_at)
        _publish_watchlist_update(redis, watchlist_id, request.symbols)
        return WatchlistResponse(watchlist_id=watchlist_id, name=request.name, created_at=created_at, symbols=request.symbols)

    @app.put("/api/v1/watchlists/{watchlist_id}", response_model=WatchlistResponse)
    def update_watchlist(watchlist_id: UUID, request: WatchlistCommandRequest) -> WatchlistResponse:
        if not store.update_watchlist(watchlist_id, request.name, request.symbols):
            raise HTTPException(status_code=404, detail="watchlist not found")
        _publish_watchlist_update(redis, watchlist_id, request.symbols)
        watchlist = store.watchlist_for_id(watchlist_id)
        if watchlist is None:
            raise HTTPException(status_code=404, detail="watchlist not found")
        return WatchlistResponse.model_validate(watchlist)

    @app.websocket("/ws/v1/sessions/{run_id}")
    async def session_updates(websocket: WebSocket, run_id: UUID) -> None:
        await websocket.accept()
        replay = store.replay_for_run(run_id)
        if replay is None:
            await websocket.close(code=1008)
            return
        last_payload = None
        for _ in range(60):
            latest_replay = ReplayResponse.model_validate(store.replay_for_run(run_id))
            latest_orders = [OrderResponse.model_validate(order) for order in store.orders_for_run(run_id)]
            events = [_event_response(event) for event in store.events_for_run(run_id, 200)]
            market = {
                symbol: redis.hgetall(market_state_key(str(run_id), symbol))
                for symbol in (latest_replay.settings.symbols if latest_replay.settings else [])
            }
            payload = {
                "type": "session.snapshot",
                "replay": latest_replay.model_dump(mode="json"),
                "orders": [order.model_dump(mode="json") for order in latest_orders],
                "events": [event.model_dump(mode="json") for event in events],
                "market": market,
                "health": {"status": "ready"},
            }
            if payload != last_payload:
                await websocket.send_json(payload)
                last_payload = payload
            if latest_replay.status == "completed":
                await websocket.close(code=1000)
                return
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=1)
            except TimeoutError:
                pass
            except WebSocketDisconnect:
                return
        await websocket.close(code=1013)

    return app


def main() -> None:
    configure_logging()
    uvicorn.run(
        create_app(
            os.getenv("DATABASE_URL", "postgresql+psycopg://tickertrace:tickertrace@localhost:5432/tickertrace"),
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        ),
        host="0.0.0.0",
        port=8000,
    )


def _scenario_for_name(scenarios: dict[str, ScenarioFixture], scenario_name: str) -> ScenarioFixture:
    scenario = scenarios.get(scenario_name)
    if scenario is None:
        raise HTTPException(status_code=404, detail="replay scenario not found")
    return scenario


def _scenario_response(scenario: ScenarioFixture) -> ScenarioResponse:
    bid_price = bid_size = ask_price = ask_size = last_trade_price = None
    first_price = last_price = None
    for event in scenario.events:
        if isinstance(event, QuoteEvent):
            bid_price = event.bid_price
            bid_size = event.bid_size
            ask_price = event.ask_price
            ask_size = event.ask_size
            price = (event.bid_price + event.ask_price) / Decimal(2)
        elif isinstance(event, TradeEvent):
            last_trade_price = event.price
            price = event.price
        if first_price is None:
            first_price = price
        last_price = price
    order = scenario.order
    return ScenarioResponse(
        name=scenario.name,
        symbol=order.symbol,
        default_side=order.side,
        default_order_type=order.order_type,
        default_quantity=order.quantity,
        default_limit_price=order.limit_price,
        default_latency_ms=order.latency_ms,
        replay_change_percent=(((last_price - first_price) / first_price) * Decimal(100)).quantize(Decimal("0.01")) if first_price and last_price else None,
        market=ScenarioMarketResponse(
            event_time=scenario.events[-1].event_time,
            bid_price=bid_price,
            bid_size=bid_size,
            ask_price=ask_price,
            ask_size=ask_size,
            last_trade_price=last_trade_price,
        ),
    )


def _application_mode() -> str:
    mode = os.getenv("APP_MODE", "public_replay")
    if mode not in {"public_replay", "private_live"}:
        raise ValueError("APP_MODE must be public_replay or private_live")
    if mode == "public_replay" and (os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_API_SECRET")):
        raise ValueError("public_replay mode cannot enable Alpaca credentials")
    return mode


def _replay_response(store: DatabaseStore, run_id: UUID) -> ReplayResponse:
    result = store.replay_for_run(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="replay not found")
    return ReplayResponse.model_validate(result)


def _new_run(scenario: ScenarioFixture, order: OrderCommand) -> ScenarioFixture:
    return replace(
        scenario,
        order=order,
        events=tuple(event.model_copy(update={"run_id": order.run_id}) for event in scenario.events),
    )


def _event_response(event: dict[str, object]) -> MarketEventResponse:
    payload = event["payload"]
    return MarketEventResponse(
        event_id=event["event_id"],
        event_type=event["event_type"],
        symbol=event["symbol"],
        event_time=event["event_time"],
        sequence=event["sequence"],
        bid_price=payload.get("bid_price"),
        bid_size=payload.get("bid_size"),
        ask_price=payload.get("ask_price"),
        ask_size=payload.get("ask_size"),
        price=payload.get("price"),
        size=payload.get("size"),
    )


def _publish_watchlist_update(redis, watchlist_id: UUID, symbols: list[str]) -> None:
    redis.xadd(
        INGESTION_CONTROL_STREAM,
        {
            "message_type": "watchlist.updated.v1",
            "watchlist_id": str(watchlist_id),
            "symbols": json.dumps(symbols),
        },
    )


if __name__ == "__main__":
    main()
