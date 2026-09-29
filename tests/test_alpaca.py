from datetime import UTC, datetime
from uuid import UUID

import pytest

from market_execution_lab.alpaca import AlpacaSettings, normalize_alpaca_message, require_private_live_mode
from market_execution_lab.models import QuoteEvent, TradeEvent


RUN_ID = UUID("22222222-2222-2222-2222-222222222222")
INGESTED_AT = datetime(2026, 9, 29, 13, 30, tzinfo=UTC)


def test_normalizes_alpaca_quote() -> None:
    event = normalize_alpaca_message(
        {"T": "q", "S": "AAPL", "bp": 199.98, "bs": 2, "ap": 200.00, "as": 1, "t": "2026-09-29T13:30:00Z"},
        RUN_ID,
        3,
        7,
        INGESTED_AT,
    )

    assert isinstance(event, QuoteEvent)
    assert event.event_id == "alpaca:q:AAPL:2026-09-29T13:30:00Z:199.98:200.0"
    assert event.partition == 3
    assert event.sequence == 7


def test_normalizes_alpaca_trade() -> None:
    event = normalize_alpaca_message(
        {"T": "t", "S": "AAPL", "i": 99, "p": 200.01, "s": 4, "t": "2026-09-29T13:30:01Z"},
        RUN_ID,
        3,
        8,
        INGESTED_AT,
    )

    assert isinstance(event, TradeEvent)
    assert event.event_id == "alpaca:t:AAPL:99"
    assert event.sequence == 8


def test_ignores_non_market_alpaca_messages() -> None:
    assert normalize_alpaca_message({"T": "subscription"}, RUN_ID, 0, 1, INGESTED_AT) is None


def test_private_live_mode_requires_explicit_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("APP_MODE", raising=False)

    with pytest.raises(ValueError, match="APP_MODE=private_live"):
        require_private_live_mode()


def test_alpaca_settings_require_credentials_and_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPACA_API_KEY", "key")
    monkeypatch.setenv("ALPACA_API_SECRET", "secret")
    monkeypatch.setenv("ALPACA_SYMBOLS", "AAPL, MSFT")

    assert AlpacaSettings.from_environment().symbols == ("AAPL", "MSFT")
