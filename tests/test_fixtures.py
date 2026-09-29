from market_execution_lab.fixtures import generated_scenarios
from market_execution_lab.models import QuoteEvent


def test_generated_scenarios_cover_phase_one_execution_cases() -> None:
    scenarios = generated_scenarios()

    assert [scenario.name for scenario in scenarios] == [
        "complete_market_fill",
        "partial_visible_liquidity",
        "limit_never_reached",
        "limit_reached_after_latency",
        "price_gap_before_activation",
    ]


def test_generated_scenarios_are_time_ordered_and_partitioned_by_symbol() -> None:
    for scenario in generated_scenarios():
        assert list(scenario.events) == sorted(scenario.events, key=lambda event: event.event_time)
        assert {event.symbol for event in scenario.events} == {scenario.order.symbol}
        assert len({event.partition for event in scenario.events}) == 1


def test_generated_scenarios_contain_valid_quotes() -> None:
    quotes = [
        event
        for scenario in generated_scenarios()
        for event in scenario.events
        if isinstance(event, QuoteEvent)
    ]

    assert all(quote.bid_price < quote.ask_price for quote in quotes)
