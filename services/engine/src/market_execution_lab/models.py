from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


Price = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=6)]
Quantity = Annotated[int, Field(gt=0)]
NonNegativeMilliseconds = Annotated[int, Field(ge=0)]


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class OrderState(StrEnum):
    SUBMITTED = "submitted"
    ACTIVE = "active"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    OPEN = "open"
    CANCELLED = "cancelled"


class EventEnvelope(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: str = Field(min_length=1, max_length=128)
    schema_version: Literal["v1"] = "v1"
    run_id: UUID
    symbol: str = Field(min_length=1, max_length=10)
    event_time: datetime
    ingested_at: datetime
    sequence: int = Field(ge=0)
    partition: int = Field(ge=0)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized.replace(".", "").replace("-", "").isalnum():
            raise ValueError("symbol must contain only letters, numbers, periods, or hyphens")
        return normalized

    @field_validator("event_time", "ingested_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include a timezone")
        return value


class QuoteEvent(EventEnvelope):
    event_type: Literal["market.quote.v1"] = "market.quote.v1"
    bid_price: Price
    bid_size: Quantity
    ask_price: Price
    ask_size: Quantity

    @model_validator(mode="after")
    def require_valid_spread(self) -> "QuoteEvent":
        if self.bid_price >= self.ask_price:
            raise ValueError("bid price must be lower than ask price")
        return self


class TradeEvent(EventEnvelope):
    event_type: Literal["market.trade.v1"] = "market.trade.v1"
    price: Price
    size: Quantity


MarketEvent = QuoteEvent | TradeEvent


class OrderCommand(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: UUID
    run_id: UUID
    symbol: str = Field(min_length=1, max_length=10)
    side: OrderSide
    order_type: OrderType
    quantity: Quantity
    limit_price: Price | None = None
    submitted_at: datetime
    latency_ms: NonNegativeMilliseconds = 0

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized.replace(".", "").replace("-", "").isalnum():
            raise ValueError("symbol must contain only letters, numbers, periods, or hyphens")
        return normalized

    @field_validator("submitted_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def require_limit_price_for_limit_order(self) -> "OrderCommand":
        if self.order_type is OrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit orders require a limit price")
        if self.order_type is OrderType.MARKET and self.limit_price is not None:
            raise ValueError("market orders cannot include a limit price")
        return self


class Fill(BaseModel):
    model_config = ConfigDict(frozen=True)

    fill_id: str = Field(min_length=1, max_length=128)
    order_id: UUID
    triggering_event_id: str = Field(min_length=1, max_length=128)
    quantity: Quantity
    price: Price
    filled_at: datetime

    @field_validator("filled_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include a timezone")
        return value


class OrderStateChange(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: UUID
    state: OrderState
    changed_at: datetime
    triggering_event_id: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("changed_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include a timezone")
        return value
