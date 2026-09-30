from datetime import timedelta
from decimal import Decimal

import pytest

from market_execution_lab.engine import simulate
from market_execution_lab.fixtures import BASE_TIME, generated_scenarios
from market_execution_lab.models import OrderSide, OrderState, OrderType


def _scenario(name: str):
    return next(scenario for scenario in generated_scenarios() if scenario.name == name)


def test_market_order_fills_at_current_ask() -> None:
    scenario = _scenario("complete_market_fill")

    result = simulate(scenario.order, scenario.events)

    assert result.state is OrderState.FILLED
    assert result.remaining_quantity == 0
    assert [(fill.quantity, fill.price) for fill in result.fills] == [(50, Decimal("200.00"))]
    assert result.metrics.average_fill_price == Decimal("200.00")
    assert result.metrics.fill_rate == Decimal("1")
    assert result.metrics.spread_cost == Decimal("0.01")


def test_market_order_remains_partially_filled_when_visible_liquidity_is_insufficient() -> None:
    scenario = _scenario("partial_visible_liquidity")

    result = simulate(scenario.order, scenario.events)

    assert result.state is OrderState.PARTIALLY_FILLED
    assert result.remaining_quantity == 50
    assert [(fill.quantity, fill.price) for fill in result.fills] == [(100, Decimal("125.50"))]
    assert result.metrics.fill_rate == Decimal(100) / Decimal(150)


def test_limit_order_stays_open_when_ask_never_reaches_limit() -> None:
    scenario = _scenario("limit_never_reached")

    result = simulate(scenario.order, scenario.events)

    assert result.state is OrderState.OPEN
    assert result.remaining_quantity == 25
    assert result.fills == ()
    assert result.metrics.fill_rate == Decimal("0")


def test_limit_order_activates_after_latency_and_uses_later_eligible_quote() -> None:
    scenario = _scenario("limit_reached_after_latency")

    result = simulate(scenario.order, scenario.events)

    assert result.state is OrderState.FILLED
    assert [(fill.quantity, fill.price) for fill in result.fills] == [(30, Decimal("249.89"))]
    assert result.metrics.time_to_first_fill == timedelta(milliseconds=25)
    assert result.metrics.latency_impact == Decimal("-0.01")


def test_market_order_reports_worse_execution_after_price_gap() -> None:
    scenario = _scenario("price_gap_before_activation")

    result = simulate(scenario.order, scenario.events)

    assert result.state is OrderState.FILLED
    assert result.fills[0].price == Decimal("181.00")
    assert result.metrics.latency_impact == Decimal("1.00")


def test_volatile_fixture_reports_the_price_change_after_activation() -> None:
    scenario = _scenario("volatile_price_swing")

    result = simulate(scenario.order, scenario.events)

    assert result.state is OrderState.FILLED
    assert result.fills[0].price == Decimal("502.00")
    assert result.metrics.latency_impact == Decimal("2.00")


def test_duplicate_quote_cannot_create_a_second_fill() -> None:
    scenario = _scenario("partial_visible_liquidity")

    result = simulate(scenario.order, scenario.events + scenario.events)

    assert result.state is OrderState.PARTIALLY_FILLED
    assert result.remaining_quantity == 50
    assert len(result.fills) == 1
    assert result.duplicate_events == 1


def test_stale_quote_does_not_replace_newer_market_state() -> None:
    scenario = _scenario("complete_market_fill")
    quote = scenario.events[0]
    stale_quote = quote.model_copy(
        update={
            "event_id": "stale-quote-1",
            "event_time": BASE_TIME,
            "ingested_at": BASE_TIME,
            "sequence": 0,
            "bid_price": Decimal("100.00"),
            "ask_price": Decimal("100.01"),
        }
    )

    result = simulate(scenario.order, scenario.events + (stale_quote,))

    assert result.fills[0].price == Decimal("200.00")
    assert result.stale_events == 1


def test_engine_is_deterministic_for_identical_inputs() -> None:
    scenario = _scenario("price_gap_before_activation")

    results = [simulate(scenario.order, scenario.events) for _ in range(3)]

    assert results[0] == results[1] == results[2]


def test_sell_limit_order_fills_at_bid() -> None:
    scenario = _scenario("complete_market_fill")
    order = scenario.order.model_copy(
        update={
            "side": OrderSide.SELL,
            "order_type": OrderType.LIMIT,
            "limit_price": Decimal("199.98"),
            "quantity": 25,
        }
    )

    result = simulate(order, scenario.events)

    assert result.state is OrderState.FILLED
    assert [(fill.quantity, fill.price) for fill in result.fills] == [(25, Decimal("199.98"))]


def test_engine_rejects_events_for_another_symbol() -> None:
    scenario = _scenario("complete_market_fill")
    wrong_symbol_event = scenario.events[0].model_copy(update={"symbol": "MSFT"})

    with pytest.raises(ValueError, match="event symbol does not match order symbol"):
        simulate(scenario.order, (wrong_symbol_event,))
