from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from market_execution_lab.engine import ExecutionResult
from market_execution_lab.models import MarketEvent, OrderCommand


metadata = sa.MetaData()

replay_runs = sa.Table(
    "replay_runs",
    metadata,
    sa.Column("run_id", sa.Uuid(as_uuid=True), primary_key=True),
    sa.Column("scenario_name", sa.String(128), nullable=False),
    sa.Column("status", sa.String(32), nullable=False),
    sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("completed_at", sa.DateTime(timezone=True)),
)

market_events = sa.Table(
    "market_events",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=False, index=True),
    sa.Column("event_id", sa.String(128), nullable=False),
    sa.Column("event_type", sa.String(64), nullable=False),
    sa.Column("symbol", sa.String(10), nullable=False, index=True),
    sa.Column("event_time", sa.DateTime(timezone=True), nullable=False, index=True),
    sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("sequence", sa.Integer, nullable=False),
    sa.Column("partition", sa.Integer, nullable=False),
    sa.Column("payload", sa.JSON, nullable=False),
    sa.UniqueConstraint("run_id", "event_id", name="uq_market_events_run_event"),
)

orders = sa.Table(
    "orders",
    metadata,
    sa.Column("order_id", sa.Uuid(as_uuid=True), primary_key=True),
    sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=False, index=True),
    sa.Column("symbol", sa.String(10), nullable=False),
    sa.Column("side", sa.String(8), nullable=False),
    sa.Column("order_type", sa.String(8), nullable=False),
    sa.Column("quantity", sa.Integer, nullable=False),
    sa.Column("limit_price", sa.Numeric(18, 6)),
    sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("latency_ms", sa.Integer, nullable=False),
    sa.Column("final_state", sa.String(32)),
    sa.Column("remaining_quantity", sa.Integer),
)

fills = sa.Table(
    "fills",
    metadata,
    sa.Column("fill_id", sa.String(128), primary_key=True),
    sa.Column("order_id", sa.Uuid(as_uuid=True), nullable=False, index=True),
    sa.Column("triggering_event_id", sa.String(128), nullable=False),
    sa.Column("quantity", sa.Integer, nullable=False),
    sa.Column("price", sa.Numeric(18, 6), nullable=False),
    sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
)

order_state_transitions = sa.Table(
    "order_state_transitions",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("order_id", sa.Uuid(as_uuid=True), nullable=False, index=True),
    sa.Column("state", sa.String(32), nullable=False),
    sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("triggering_event_id", sa.String(128)),
)


class DatabaseStore:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def create_run(self, run_id: UUID, scenario_name: str, started_at: datetime) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                pg_insert(replay_runs).values(
                    run_id=run_id,
                    scenario_name=scenario_name,
                    status="running",
                    started_at=started_at,
                ).on_conflict_do_nothing(index_elements=[replay_runs.c.run_id])
            )

    def record_order(self, order: OrderCommand) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                pg_insert(orders).values(
                    order_id=order.order_id,
                    run_id=order.run_id,
                    symbol=order.symbol,
                    side=order.side.value,
                    order_type=order.order_type.value,
                    quantity=order.quantity,
                    limit_price=order.limit_price,
                    submitted_at=order.submitted_at,
                    latency_ms=order.latency_ms,
                ).on_conflict_do_nothing(index_elements=[orders.c.order_id])
            )

    def record_event(self, event: MarketEvent) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                pg_insert(market_events).values(
                    run_id=event.run_id,
                    event_id=event.event_id,
                    event_type=event.event_type,
                    symbol=event.symbol,
                    event_time=event.event_time,
                    ingested_at=event.ingested_at,
                    sequence=event.sequence,
                    partition=event.partition,
                    payload=event.model_dump(mode="json"),
                ).on_conflict_do_nothing(constraint="uq_market_events_run_event")
            )

    def complete_run(self, order: OrderCommand, result: ExecutionResult, completed_at: datetime) -> None:
        with self._engine.begin() as connection:
            if result.fills:
                connection.execute(
                    pg_insert(fills).values(
                        [
                            {
                                "fill_id": fill.fill_id,
                                "order_id": fill.order_id,
                                "triggering_event_id": fill.triggering_event_id,
                                "quantity": fill.quantity,
                                "price": fill.price,
                                "filled_at": fill.filled_at,
                            }
                            for fill in result.fills
                        ]
                    ).on_conflict_do_nothing(index_elements=[fills.c.fill_id])
                )
            connection.execute(
                sa.delete(order_state_transitions).where(order_state_transitions.c.order_id == order.order_id)
            )
            connection.execute(
                sa.insert(order_state_transitions),
                [
                    {
                        "order_id": transition.order_id,
                        "state": transition.state.value,
                        "changed_at": transition.changed_at,
                        "triggering_event_id": transition.triggering_event_id,
                    }
                    for transition in result.transitions
                ],
            )
            connection.execute(
                sa.update(orders)
                .where(orders.c.order_id == order.order_id)
                .values(final_state=result.state.value, remaining_quantity=result.remaining_quantity)
            )
            connection.execute(
                sa.update(replay_runs)
                .where(replay_runs.c.run_id == order.run_id)
                .values(status="completed", completed_at=completed_at)
            )

    def counts_for_run(self, run_id: UUID) -> dict[str, int]:
        with self._engine.connect() as connection:
            return {
                "events": connection.scalar(
                    sa.select(sa.func.count()).select_from(market_events).where(market_events.c.run_id == run_id)
                )
                or 0,
                "orders": connection.scalar(
                    sa.select(sa.func.count()).select_from(orders).where(orders.c.run_id == run_id)
                )
                or 0,
                "fills": connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(fills.join(orders, fills.c.order_id == orders.c.order_id))
                    .where(orders.c.run_id == run_id)
                )
                or 0,
                "transitions": connection.scalar(
                    sa.select(sa.func.count())
                    .select_from(order_state_transitions.join(orders, order_state_transitions.c.order_id == orders.c.order_id))
                    .where(orders.c.run_id == run_id)
                )
                or 0,
            }
