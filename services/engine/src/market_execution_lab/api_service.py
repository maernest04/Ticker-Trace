import os
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlalchemy as sa
import uvicorn

from market_execution_lab.observability import configure_logging
from market_execution_lab.operations_service import create_app as create_operations_app
from market_execution_lab.storage import DatabaseStore
from market_execution_lab.streaming import market_state_key


class SymbolResponse(BaseModel):
    symbol: str


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


class WatchlistResponse(BaseModel):
    watchlist_id: UUID
    name: str
    created_at: datetime
    symbols: list[str]


def create_app(database_url: str, redis_url: str) -> FastAPI:
    app = create_operations_app(redis_url)
    store = DatabaseStore(sa.create_engine(database_url))
    redis = app.state.redis

    @app.get("/api/v1/symbols", response_model=list[SymbolResponse])
    def symbols() -> list[SymbolResponse]:
        return [SymbolResponse(symbol=symbol) for symbol in store.list_symbols()]

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
        result = store.replay_for_run(run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="replay not found")
        return ReplayResponse.model_validate(result)

    @app.get("/api/v1/watchlists", response_model=list[WatchlistResponse])
    def watchlists() -> list[WatchlistResponse]:
        return [WatchlistResponse.model_validate(watchlist) for watchlist in store.list_watchlists()]

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


if __name__ == "__main__":
    main()
