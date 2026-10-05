from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID
from math import sin

from market_execution_lab.models import MarketEvent, OrderCommand, OrderSide, OrderType, QuoteEvent, TradeEvent


RUN_ID = UUID("11111111-1111-1111-1111-111111111111")
BASE_TIME = datetime(2026, 9, 28, 13, 30, tzinfo=UTC)


@dataclass(frozen=True)
class ScenarioFixture:
    name: str
    order: OrderCommand
    events: tuple[MarketEvent, ...]
    entry_index: int = 0


def generated_scenarios() -> tuple[ScenarioFixture, ...]:
    return (
        ScenarioFixture(
            name="complete_market_fill",
            order=OrderCommand(
                order_id=UUID("00000000-0000-0000-0000-000000000001"),
                run_id=RUN_ID,
                symbol="AAPL",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=50,
                submitted_at=BASE_TIME,
            ),
            events=(
                QuoteEvent(
                    event_id="complete-market-quote-1",
                    run_id=RUN_ID,
                    symbol="AAPL",
                    event_time=BASE_TIME + timedelta(milliseconds=1),
                    ingested_at=BASE_TIME + timedelta(milliseconds=2),
                    sequence=1,
                    partition=0,
                    bid_price=Decimal("199.98"),
                    bid_size=100,
                    ask_price=Decimal("200.00"),
                    ask_size=100,
                ),
            ),
        ),
        ScenarioFixture(
            name="partial_visible_liquidity",
            order=OrderCommand(
                order_id=UUID("00000000-0000-0000-0000-000000000002"),
                run_id=RUN_ID,
                symbol="NVDA",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=150,
                submitted_at=BASE_TIME,
            ),
            events=(
                QuoteEvent(
                    event_id="partial-liquidity-quote-1",
                    run_id=RUN_ID,
                    symbol="NVDA",
                    event_time=BASE_TIME + timedelta(milliseconds=1),
                    ingested_at=BASE_TIME + timedelta(milliseconds=2),
                    sequence=1,
                    partition=1,
                    bid_price=Decimal("125.49"),
                    bid_size=100,
                    ask_price=Decimal("125.50"),
                    ask_size=100,
                ),
            ),
        ),
        ScenarioFixture(
            name="volatile_price_swing",
            order=OrderCommand(
                order_id=UUID("00000000-0000-0000-0000-000000000006"),
                run_id=RUN_ID,
                symbol="META",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=40,
                submitted_at=BASE_TIME,
                latency_ms=100,
            ),
            events=(
                QuoteEvent(
                    event_id="volatile-quote-1",
                    run_id=RUN_ID,
                    symbol="META",
                    event_time=BASE_TIME + timedelta(milliseconds=25),
                    ingested_at=BASE_TIME + timedelta(milliseconds=26),
                    sequence=1,
                    partition=5,
                    bid_price=Decimal("499.98"),
                    bid_size=100,
                    ask_price=Decimal("500.00"),
                    ask_size=100,
                ),
                QuoteEvent(
                    event_id="volatile-quote-2",
                    run_id=RUN_ID,
                    symbol="META",
                    event_time=BASE_TIME + timedelta(milliseconds=75),
                    ingested_at=BASE_TIME + timedelta(milliseconds=76),
                    sequence=2,
                    partition=5,
                    bid_price=Decimal("503.98"),
                    bid_size=100,
                    ask_price=Decimal("504.00"),
                    ask_size=100,
                ),
                QuoteEvent(
                    event_id="volatile-quote-3",
                    run_id=RUN_ID,
                    symbol="META",
                    event_time=BASE_TIME + timedelta(milliseconds=125),
                    ingested_at=BASE_TIME + timedelta(milliseconds=126),
                    sequence=3,
                    partition=5,
                    bid_price=Decimal("501.98"),
                    bid_size=100,
                    ask_price=Decimal("502.00"),
                    ask_size=100,
                ),
            ),
        ),
        ScenarioFixture(
            name="limit_never_reached",
            order=OrderCommand(
                order_id=UUID("00000000-0000-0000-0000-000000000003"),
                run_id=RUN_ID,
                symbol="MSFT",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=25,
                limit_price=Decimal("399.50"),
                submitted_at=BASE_TIME,
            ),
            events=(
                QuoteEvent(
                    event_id="limit-never-quote-1",
                    run_id=RUN_ID,
                    symbol="MSFT",
                    event_time=BASE_TIME + timedelta(milliseconds=1),
                    ingested_at=BASE_TIME + timedelta(milliseconds=2),
                    sequence=1,
                    partition=2,
                    bid_price=Decimal("399.98"),
                    bid_size=100,
                    ask_price=Decimal("400.00"),
                    ask_size=100,
                ),
            ),
        ),
        ScenarioFixture(
            name="limit_reached_after_latency",
            order=OrderCommand(
                order_id=UUID("00000000-0000-0000-0000-000000000004"),
                run_id=RUN_ID,
                symbol="TSLA",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=30,
                limit_price=Decimal("249.90"),
                submitted_at=BASE_TIME,
                latency_ms=100,
            ),
            events=(
                QuoteEvent(
                    event_id="limit-latency-quote-1",
                    run_id=RUN_ID,
                    symbol="TSLA",
                    event_time=BASE_TIME + timedelta(milliseconds=50),
                    ingested_at=BASE_TIME + timedelta(milliseconds=51),
                    sequence=1,
                    partition=3,
                    bid_price=Decimal("249.88"),
                    bid_size=100,
                    ask_price=Decimal("249.90"),
                    ask_size=100,
                ),
                QuoteEvent(
                    event_id="limit-latency-quote-2",
                    run_id=RUN_ID,
                    symbol="TSLA",
                    event_time=BASE_TIME + timedelta(milliseconds=125),
                    ingested_at=BASE_TIME + timedelta(milliseconds=126),
                    sequence=2,
                    partition=3,
                    bid_price=Decimal("249.87"),
                    bid_size=100,
                    ask_price=Decimal("249.89"),
                    ask_size=100,
                ),
            ),
        ),
        ScenarioFixture(
            name="price_gap_before_activation",
            order=OrderCommand(
                order_id=UUID("00000000-0000-0000-0000-000000000005"),
                run_id=RUN_ID,
                symbol="AMZN",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=10,
                submitted_at=BASE_TIME,
                latency_ms=100,
            ),
            events=(
                QuoteEvent(
                    event_id="price-gap-quote-1",
                    run_id=RUN_ID,
                    symbol="AMZN",
                    event_time=BASE_TIME + timedelta(milliseconds=25),
                    ingested_at=BASE_TIME + timedelta(milliseconds=26),
                    sequence=1,
                    partition=4,
                    bid_price=Decimal("179.98"),
                    bid_size=100,
                    ask_price=Decimal("180.00"),
                    ask_size=100,
                ),
                TradeEvent(
                    event_id="price-gap-trade-1",
                    run_id=RUN_ID,
                    symbol="AMZN",
                    event_time=BASE_TIME + timedelta(milliseconds=50),
                    ingested_at=BASE_TIME + timedelta(milliseconds=51),
                    sequence=2,
                    partition=4,
                    price=Decimal("180.01"),
                    size=50,
                ),
                QuoteEvent(
                    event_id="price-gap-quote-2",
                    run_id=RUN_ID,
                    symbol="AMZN",
                    event_time=BASE_TIME + timedelta(milliseconds=125),
                    ingested_at=BASE_TIME + timedelta(milliseconds=126),
                    sequence=3,
                    partition=4,
                    bid_price=Decimal("180.98"),
                    bid_size=100,
                    ask_price=Decimal("181.00"),
                    ask_size=100,
                ),
            ),
        ),
    )


def replay_scenarios() -> tuple[ScenarioFixture, ...]:
    experiments = []
    for index, (name, symbol, base_price) in enumerate((
        ("liquidity_replenishment_v1", "NVDA", Decimal("125")),
        ("volatile_spread_v1", "META", Decimal("500")),
        ("gap_and_recovery_v1", "AMZN", Decimal("180")),
    )):
        events = []
        sequence = 0
        for tick in range(240):
            oscillation = Decimal(str(round(sin(tick / 8) * (0.8 if index == 1 else 0.12), 2)))
            gap = Decimal("2") if index == 2 and 40 <= tick < 120 else Decimal("0")
            midpoint = base_price + oscillation + gap + Decimal(tick) / Decimal("1000")
            half_spread = Decimal("0.01") * (1 + (tick // 20) % (5 if index == 1 else 2))
            sequence += 1
            event_time = BASE_TIME + timedelta(milliseconds=1 + tick * 25)
            events.append(QuoteEvent(
                event_id=f"{name}:quote:{tick}", run_id=RUN_ID, symbol=symbol,
                event_time=event_time, ingested_at=event_time + timedelta(milliseconds=1),
                sequence=sequence, partition=index,
                bid_price=midpoint - half_spread, bid_size=5 + (tick * 7) % 40,
                ask_price=midpoint + half_spread, ask_size=5 + (tick * 11) % 40,
            ))
            if tick % 4 == 0:
                sequence += 1
                events.append(TradeEvent(
                    event_id=f"{name}:trade:{tick}", run_id=RUN_ID, symbol=symbol,
                    event_time=event_time + timedelta(milliseconds=1), ingested_at=event_time + timedelta(milliseconds=2),
                    sequence=sequence, partition=index, price=midpoint, size=10 + tick % 30,
                ))
        experiments.append(ScenarioFixture(name=name, order=OrderCommand(
            order_id=UUID(int=100 + index), run_id=RUN_ID, symbol=symbol, side=OrderSide.BUY,
            order_type=OrderType.MARKET, quantity=1000, submitted_at=BASE_TIME, latency_ms=250,
        ), events=tuple(events)))
    return generated_scenarios() + tuple(experiments)
