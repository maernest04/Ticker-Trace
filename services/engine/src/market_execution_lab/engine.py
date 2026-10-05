from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Iterable

from market_execution_lab.models import (
    Fill,
    MarketEvent,
    OrderCommand,
    OrderSide,
    OrderState,
    OrderStateChange,
    OrderType,
    QuoteEvent,
    TradeEvent,
)


@dataclass
class MarketState:
    quote_event_time: datetime | None = None
    quote_ingested_at: datetime | None = None
    bid_price: Decimal | None = None
    bid_size: int | None = None
    ask_price: Decimal | None = None
    ask_size: int | None = None
    last_trade_price: Decimal | None = None
    last_event_time: datetime | None = None
    last_event_sequence: int = -1


@dataclass(frozen=True)
class ExecutionMetrics:
    average_fill_price: Decimal | None
    fill_rate: Decimal
    spread_cost: Decimal | None
    time_to_first_fill: timedelta | None
    time_to_completion: timedelta | None
    latency_impact: Decimal | None


@dataclass(frozen=True)
class ExecutionResult:
    state: OrderState
    remaining_quantity: int
    fills: tuple[Fill, ...]
    transitions: tuple[OrderStateChange, ...]
    metrics: ExecutionMetrics
    processed_events: int
    duplicate_events: int
    stale_events: int


class ExecutionEngine:
    def __init__(self, order: OrderCommand) -> None:
        self.order = order
        self.market_state = MarketState()
        self._fills: list[Fill] = []
        self._transitions = [
            OrderStateChange(
                order_id=order.order_id,
                state=OrderState.SUBMITTED,
                changed_at=order.submitted_at,
            )
        ]
        self._seen_event_ids: set[str] = set()
        self._remaining_quantity = order.quantity
        self._state = OrderState.SUBMITTED
        self._active = False
        self._arrival_midpoint: Decimal | None = None
        self._first_fill_time: datetime | None = None
        self._completion_time: datetime | None = None
        self._processed_events = 0
        self._duplicate_events = 0
        self._stale_events = 0
        self._final_result: ExecutionResult | None = None

    @property
    def state(self) -> OrderState:
        return self._state

    @property
    def remaining_quantity(self) -> int:
        return self._remaining_quantity

    def process(self, event: MarketEvent, *, execute: bool = True) -> bool:
        if self._final_result is not None:
            raise RuntimeError("cannot process events after finalization")

        _validate_event_for_order(self.order, event)

        if event.event_id in self._seen_event_ids:
            self._duplicate_events += 1
            return False

        self._seen_event_ids.add(event.event_id)
        event_key = (event.event_time, event.sequence)
        if self.market_state.last_event_time is not None:
            last_event_key = (self.market_state.last_event_time, self.market_state.last_event_sequence)
            if event_key <= last_event_key:
                self._stale_events += 1
                return False

        _apply_event_to_market_state(self.market_state, event)
        self._processed_events += 1
        if not execute:
            return True

        activation_time = self.order.submitted_at + timedelta(milliseconds=self.order.latency_ms)
        if not self._active and event.event_time >= activation_time:
            self._active = True
            self._state = OrderState.ACTIVE
            self._arrival_midpoint = _midpoint(self.market_state)
            self._transitions.append(
                OrderStateChange(
                    order_id=self.order.order_id,
                    state=OrderState.ACTIVE,
                    changed_at=event.event_time,
                    triggering_event_id=event.event_id,
                )
            )

        if not self._active or self._remaining_quantity == 0 or not isinstance(event, QuoteEvent):
            return True

        if not _is_fill_eligible(self.order, self.market_state):
            return True

        available_quantity, fill_price = _available_quantity_and_price(self.order, self.market_state)
        fill_quantity = min(self._remaining_quantity, available_quantity)
        if fill_quantity == 0:
            return True

        self._fills.append(
            Fill(
                fill_id=_fill_id(self.order, event),
                order_id=self.order.order_id,
                triggering_event_id=event.event_id,
                quantity=fill_quantity,
                price=fill_price,
                filled_at=event.event_time,
            )
        )
        self._remaining_quantity -= fill_quantity
        self._first_fill_time = self._first_fill_time or event.event_time

        if self._remaining_quantity == 0:
            self._state = OrderState.FILLED
            self._completion_time = event.event_time
        else:
            self._state = OrderState.PARTIALLY_FILLED

        self._transitions.append(
            OrderStateChange(
                order_id=self.order.order_id,
                state=self._state,
                changed_at=event.event_time,
                triggering_event_id=event.event_id,
            )
        )
        return True

    def cancel(self, changed_at: datetime, reason: str) -> ExecutionResult:
        if self._state not in {OrderState.FILLED, OrderState.CANCELLED}:
            self._state = OrderState.CANCELLED
            self._transitions.append(OrderStateChange(order_id=self.order.order_id, state=self._state,
                                                      changed_at=max(changed_at, self._transitions[-1].changed_at), reason=reason))
            self._final_result = self.snapshot()
        return self.snapshot()

    def finalize(self) -> ExecutionResult:
        if self._final_result is not None:
            return self._final_result

        activation_time = self.order.submitted_at + timedelta(milliseconds=self.order.latency_ms)
        if self._active and self._state is OrderState.ACTIVE:
            self._state = OrderState.OPEN
            self._transitions.append(
                OrderStateChange(
                    order_id=self.order.order_id,
                    state=OrderState.OPEN,
                    changed_at=self.market_state.last_event_time or activation_time,
                )
            )

        self._final_result = self.snapshot()
        return self._final_result

    def snapshot(self) -> ExecutionResult:
        activation_time = self.order.submitted_at + timedelta(milliseconds=self.order.latency_ms)
        average_fill_price = _average_fill_price(self._fills)
        fill_rate = Decimal(self.order.quantity - self._remaining_quantity) / Decimal(self.order.quantity)
        spread_cost = None
        if average_fill_price is not None and self._arrival_midpoint is not None:
            if self.order.side is OrderSide.BUY:
                spread_cost = average_fill_price - self._arrival_midpoint
            else:
                spread_cost = self._arrival_midpoint - average_fill_price

        return ExecutionResult(
            state=self._state,
            remaining_quantity=self._remaining_quantity,
            fills=tuple(self._fills),
            transitions=tuple(self._transitions),
            metrics=ExecutionMetrics(
                average_fill_price=average_fill_price,
                fill_rate=fill_rate,
                spread_cost=spread_cost,
                time_to_first_fill=(self._first_fill_time - activation_time) if self._first_fill_time else None,
                time_to_completion=(self._completion_time - activation_time) if self._completion_time else None,
                latency_impact=None,
            ),
            processed_events=self._processed_events,
            duplicate_events=self._duplicate_events,
            stale_events=self._stale_events,
        )


def simulate(order: OrderCommand, events: Iterable[MarketEvent], entry_index: int = 0) -> ExecutionResult:
    ordered_events = tuple(events)
    result = _simulate_once(order, ordered_events, entry_index)

    if order.latency_ms == 0:
        return result

    baseline_order = order.model_copy(update={"latency_ms": 0})
    baseline = _simulate_once(baseline_order, ordered_events, entry_index)
    if result.metrics.average_fill_price is None or baseline.metrics.average_fill_price is None:
        latency_impact = None
    elif order.side is OrderSide.BUY:
        latency_impact = result.metrics.average_fill_price - baseline.metrics.average_fill_price
    else:
        latency_impact = baseline.metrics.average_fill_price - result.metrics.average_fill_price

    return replace(result, metrics=replace(result.metrics, latency_impact=latency_impact))


def _simulate_once(order: OrderCommand, events: tuple[MarketEvent, ...], entry_index: int = 0) -> ExecutionResult:
    engine = ExecutionEngine(order)
    for index, event in enumerate(events):
        engine.process(event, execute=index >= entry_index)
    return engine.finalize()


def _validate_event_for_order(order: OrderCommand, event: MarketEvent) -> None:
    if event.run_id != order.run_id:
        raise ValueError("event run does not match order run")
    if event.symbol != order.symbol:
        raise ValueError("event symbol does not match order symbol")


def _apply_event_to_market_state(state: MarketState, event: MarketEvent) -> None:
    state.last_event_time = event.event_time
    state.last_event_sequence = event.sequence

    if isinstance(event, QuoteEvent):
        state.quote_event_time = event.event_time
        state.quote_ingested_at = event.ingested_at
        state.bid_price = event.bid_price
        state.bid_size = event.bid_size
        state.ask_price = event.ask_price
        state.ask_size = event.ask_size
    elif isinstance(event, TradeEvent):
        state.last_trade_price = event.price


def _midpoint(state: MarketState) -> Decimal | None:
    if state.bid_price is None or state.ask_price is None:
        return None
    return (state.bid_price + state.ask_price) / Decimal("2")


def _is_fill_eligible(order: OrderCommand, state: MarketState) -> bool:
    if order.order_type is OrderType.MARKET:
        return True
    if order.side is OrderSide.BUY:
        return state.ask_price is not None and state.ask_price <= order.limit_price
    return state.bid_price is not None and state.bid_price >= order.limit_price


def _available_quantity_and_price(order: OrderCommand, state: MarketState) -> tuple[int, Decimal]:
    if order.side is OrderSide.BUY:
        if state.ask_size is None or state.ask_price is None:
            raise ValueError("buy orders require an ask quote")
        return state.ask_size, state.ask_price

    if state.bid_size is None or state.bid_price is None:
        raise ValueError("sell orders require a bid quote")
    return state.bid_size, state.bid_price


def _fill_id(order: OrderCommand, event: QuoteEvent) -> str:
    source = f"{order.order_id}:{event.event_id}"
    return sha256(source.encode()).hexdigest()


def _average_fill_price(fills: list[Fill]) -> Decimal | None:
    if not fills:
        return None
    total_quantity = sum(fill.quantity for fill in fills)
    total_value = sum((fill.price * fill.quantity for fill in fills), Decimal("0"))
    return total_value / Decimal(total_quantity)
