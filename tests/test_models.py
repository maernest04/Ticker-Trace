from datetime import datetime
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from market_execution_lab.models import OrderCommand, OrderSide, OrderType, QuoteEvent


RUN_ID = UUID("11111111-1111-1111-1111-111111111111")
EVENT_TIME = datetime.fromisoformat("2026-09-28T13:30:00+00:00")


def test_quote_normalizes_symbol_and_accepts_timezone_aware_timestamps() -> None:
    quote = QuoteEvent(
        event_id="quote-1",
        run_id=RUN_ID,
        symbol=" aapl ",
        event_time=EVENT_TIME,
        ingested_at=EVENT_TIME,
        sequence=0,
        partition=0,
        bid_price=Decimal("199.99"),
        bid_size=100,
        ask_price=Decimal("200.00"),
        ask_size=100,
    )

    assert quote.symbol == "AAPL"


def test_quote_rejects_crossed_market() -> None:
    with pytest.raises(ValidationError, match="bid price must be lower than ask price"):
        QuoteEvent(
            event_id="quote-1",
            run_id=RUN_ID,
            symbol="AAPL",
            event_time=EVENT_TIME,
            ingested_at=EVENT_TIME,
            sequence=0,
            partition=0,
            bid_price=Decimal("200.00"),
            bid_size=100,
            ask_price=Decimal("200.00"),
            ask_size=100,
        )


def test_quote_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="timestamps must include a timezone"):
        QuoteEvent(
            event_id="quote-1",
            run_id=RUN_ID,
            symbol="AAPL",
            event_time=datetime(2026, 9, 28, 13, 30),
            ingested_at=EVENT_TIME,
            sequence=0,
            partition=0,
            bid_price=Decimal("199.99"),
            bid_size=100,
            ask_price=Decimal("200.00"),
            ask_size=100,
        )


def test_limit_order_requires_limit_price() -> None:
    with pytest.raises(ValidationError, match="limit orders require a limit price"):
        OrderCommand(
            order_id=UUID("00000000-0000-0000-0000-000000000001"),
            run_id=RUN_ID,
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=10,
            submitted_at=EVENT_TIME,
        )


def test_market_order_rejects_limit_price() -> None:
    with pytest.raises(ValidationError, match="market orders cannot include a limit price"):
        OrderCommand(
            order_id=UUID("00000000-0000-0000-0000-000000000001"),
            run_id=RUN_ID,
            symbol="AAPL",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=10,
            limit_price=Decimal("200.00"),
            submitted_at=EVENT_TIME,
        )
