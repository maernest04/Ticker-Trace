from datetime import UTC, datetime, timedelta
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
    sa.Column("average_fill_price", sa.Numeric(18, 6)),
    sa.Column("fill_rate", sa.Numeric(18, 6)),
    sa.Column("spread_cost", sa.Numeric(18, 6)),
    sa.Column("time_to_first_fill_ms", sa.Integer),
    sa.Column("time_to_completion_ms", sa.Integer),
    sa.Column("latency_impact", sa.Numeric(18, 6)),
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
    sa.Column("reason", sa.String(128)),
)

watchlists = sa.Table(
    "watchlists",
    metadata,
    sa.Column("watchlist_id", sa.Uuid(as_uuid=True), primary_key=True),
    sa.Column("name", sa.String(128), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
)

watchlist_symbols = sa.Table(
    "watchlist_symbols",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("watchlist_id", sa.Uuid(as_uuid=True), sa.ForeignKey("watchlists.watchlist_id"), nullable=False),
    sa.Column("symbol", sa.String(10), nullable=False),
    sa.UniqueConstraint("watchlist_id", "symbol", name="uq_watchlist_symbols_watchlist_symbol"),
)

replay_session_settings = sa.Table(
    "replay_session_settings",
    metadata,
    sa.Column("run_id", sa.Uuid(as_uuid=True), sa.ForeignKey("replay_runs.run_id"), primary_key=True),
    sa.Column("mode", sa.String(32), nullable=False),
    sa.Column("symbols", sa.JSON, nullable=False),
)

private_live_sessions = sa.Table(
    "private_live_sessions", metadata,
    sa.Column("run_id", sa.Uuid(as_uuid=True), sa.ForeignKey("replay_runs.run_id"), primary_key=True),
    sa.Column("status", sa.String(32), nullable=False),
    sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("closing_at", sa.DateTime(timezone=True)),
    sa.Column("closed_at", sa.DateTime(timezone=True), index=True),
    sa.Column("next_run_id", sa.Uuid(as_uuid=True)),
    sa.Column("redis_expired", sa.Boolean, nullable=False, server_default=sa.false()),
)


def sqlalchemy_url(url: str) -> str:
    if url.startswith("postgres://"):
        return f"postgresql+psycopg://{url.removeprefix('postgres://')}"
    if url.startswith("postgresql://"):
        return f"postgresql+psycopg://{url.removeprefix('postgresql://')}"
    return url


class DatabaseStore:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def prune_public_replays(self, scenario_names: tuple[str, ...]) -> int:
        with self._engine.begin() as connection:
            eligible = sa.select(replay_runs.c.run_id).join(replay_session_settings).where(
                replay_session_settings.c.mode == "public_replay",
                replay_runs.c.scenario_name.in_(scenario_names),
                replay_runs.c.status == "completed",
            )
            recent = eligible.order_by(replay_runs.c.completed_at.desc()).limit(1000)
            expired = list(connection.scalars(eligible.where(sa.or_(
                replay_runs.c.completed_at < datetime.now(UTC) - timedelta(days=7),
                replay_runs.c.run_id.not_in(recent),
            )).limit(100)))
            if not expired:
                return 0
            order_ids = sa.select(orders.c.order_id).where(orders.c.run_id.in_(expired))
            connection.execute(sa.delete(fills).where(fills.c.order_id.in_(order_ids)))
            connection.execute(sa.delete(order_state_transitions).where(order_state_transitions.c.order_id.in_(order_ids)))
            connection.execute(sa.delete(orders).where(orders.c.run_id.in_(expired)))
            connection.execute(sa.delete(market_events).where(market_events.c.run_id.in_(expired)))
            connection.execute(sa.delete(replay_session_settings).where(replay_session_settings.c.run_id.in_(expired)))
            connection.execute(sa.delete(replay_runs).where(replay_runs.c.run_id.in_(expired)))
            return len(expired)

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
        self.record_events([event])

    def record_events(self, events: list[MarketEvent]) -> None:
        if not events:
            return
        with self._engine.begin() as connection:
            connection.execute(
                pg_insert(market_events).values([
                    dict(run_id=event.run_id, event_id=event.event_id, event_type=event.event_type,
                         symbol=event.symbol, event_time=event.event_time, ingested_at=event.ingested_at,
                         sequence=event.sequence, partition=event.partition, payload=event.model_dump(mode="json"))
                    for event in events
                ]).on_conflict_do_nothing(constraint="uq_market_events_run_event")
            )

    def record_replay_settings(self, run_id: UUID, mode: str, symbols: list[str], overwrite: bool = True) -> None:
        with self._engine.begin() as connection:
            statement = pg_insert(replay_session_settings).values(run_id=run_id, mode=mode, symbols=symbols)
            statement = statement.on_conflict_do_update(index_elements=[replay_session_settings.c.run_id], set_={"mode": mode, "symbols": symbols}) if overwrite else statement.on_conflict_do_nothing(index_elements=[replay_session_settings.c.run_id])
            connection.execute(statement)

    def complete_run(self, order: OrderCommand, result: ExecutionResult, completed_at: datetime, final: bool = True) -> None:
        with self._engine.begin() as connection:
            if not final:
                current_order = connection.execute(sa.select(orders.c.remaining_quantity, orders.c.final_state).where(orders.c.order_id == order.order_id).with_for_update()).mappings().one_or_none()
                if current_order and current_order["final_state"] == "cancelled" and result.state.value != "cancelled":
                    return
                current = current_order["remaining_quantity"] if current_order else None
                latest_transition = connection.scalar(sa.select(sa.func.max(order_state_transitions.c.changed_at)).where(order_state_transitions.c.order_id == order.order_id))
                if current is not None and (result.remaining_quantity > current or (
                    result.remaining_quantity == current and latest_transition is not None
                    and max(transition.changed_at for transition in result.transitions) < latest_transition
                )):
                    return
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
                        "reason": transition.reason,
                    }
                    for transition in result.transitions
                ],
            )
            connection.execute(
                sa.update(orders)
                .where(orders.c.order_id == order.order_id)
                .values(
                    final_state=result.state.value,
                    remaining_quantity=result.remaining_quantity,
                    average_fill_price=result.metrics.average_fill_price,
                    fill_rate=result.metrics.fill_rate,
                    spread_cost=result.metrics.spread_cost,
                    time_to_first_fill_ms=_milliseconds(result.metrics.time_to_first_fill),
                    time_to_completion_ms=_milliseconds(result.metrics.time_to_completion),
                    latency_impact=result.metrics.latency_impact,
                )
            )
            if final:
                connection.execute(
                    sa.update(replay_runs)
                    .where(replay_runs.c.run_id == order.run_id)
                    .values(status="completed", completed_at=completed_at)
                )

    def register_private_session(self, run_id: UUID, started_at: datetime) -> None:
        with self._engine.begin() as connection:
            connection.execute(pg_insert(private_live_sessions).values(run_id=run_id, status="starting", started_at=started_at).on_conflict_do_nothing())

    def activate_private_session(self, run_id: UUID) -> None:
        with self._engine.begin() as connection:
            connection.execute(sa.update(private_live_sessions).where(private_live_sessions.c.run_id == run_id, private_live_sessions.c.status == "starting").values(status="running"))

    def private_session(self, run_id: UUID | None = None) -> dict | None:
        with self._engine.connect() as connection:
            query = sa.select(private_live_sessions)
            query = query.where(private_live_sessions.c.run_id == run_id) if run_id else query.where(private_live_sessions.c.status != "completed").order_by(private_live_sessions.c.started_at.desc()).limit(1)
            row = connection.execute(query).mappings().one_or_none()
            return dict(row) if row else None

    def begin_private_close(self, run_id: UUID, next_run_id: UUID, closing_at: datetime) -> dict:
        with self._engine.begin() as connection:
            connection.execute(sa.update(private_live_sessions).where(private_live_sessions.c.run_id == run_id, private_live_sessions.c.status == "running").values(status="closing", next_run_id=next_run_id, closing_at=closing_at))
        return self.private_session(run_id)

    def finish_private_close(self, run_id: UUID, closed_at: datetime) -> None:
        with self._engine.begin() as connection:
            status = connection.scalar(sa.select(private_live_sessions.c.status).where(private_live_sessions.c.run_id == run_id).with_for_update())
            if status == "completed":
                return
            if status != "closing":
                raise RuntimeError("session closure has not started")
            unfinished = connection.scalar(sa.select(sa.func.count()).select_from(orders).where(orders.c.run_id == run_id, sa.or_(orders.c.final_state.is_(None), orders.c.final_state.not_in(["filled", "cancelled"]))))
            if unfinished:
                raise RuntimeError("private session still has unfinished orders")
            connection.execute(sa.update(private_live_sessions).where(private_live_sessions.c.run_id == run_id, private_live_sessions.c.status == "closing").values(status="completed", closed_at=closed_at))
            connection.execute(sa.update(replay_runs).where(replay_runs.c.run_id == run_id).values(status="completed", completed_at=closed_at))

    def closed_private_sessions(self, limit: int = 100) -> list[dict]:
        with self._engine.connect() as connection:
            return [dict(row) for row in connection.execute(sa.select(private_live_sessions).where(private_live_sessions.c.status == "completed", private_live_sessions.c.redis_expired.is_(False)).order_by(private_live_sessions.c.closed_at).limit(limit)).mappings()]

    def mark_private_redis_expired(self, run_id: UUID) -> None:
        with self._engine.begin() as connection:
            connection.execute(sa.update(private_live_sessions).where(private_live_sessions.c.run_id == run_id, private_live_sessions.c.status == "completed").values(redis_expired=True))

    def prune_private_history(self, raw_seconds: int, history_seconds: int, batch_size: int = 1000, now: datetime | None = None) -> dict[str, int]:
        now = now or datetime.now(UTC)
        with self._engine.begin() as connection:
            closed = sa.select(private_live_sessions.c.run_id).where(private_live_sessions.c.status == "completed", private_live_sessions.c.closed_at < now - timedelta(seconds=raw_seconds))
            expired = sa.select(private_live_sessions.c.run_id).where(private_live_sessions.c.status == "completed", private_live_sessions.c.redis_expired.is_(True), private_live_sessions.c.closed_at < now - timedelta(seconds=history_seconds))
            retained_fill = sa.exists(sa.select(fills.c.fill_id).join(orders, orders.c.order_id == fills.c.order_id).where(orders.c.run_id == market_events.c.run_id, fills.c.triggering_event_id == market_events.c.event_id))
            event_ids = sa.select(market_events.c.id).where(market_events.c.run_id.in_(closed), sa.or_(market_events.c.run_id.in_(expired), ~retained_fill)).order_by(market_events.c.id).limit(batch_size)
            events_removed = connection.execute(sa.delete(market_events).where(market_events.c.id.in_(event_ids))).rowcount
            order_ids = sa.select(orders.c.order_id).where(orders.c.run_id.in_(expired))
            fill_ids = sa.select(fills.c.fill_id).where(fills.c.order_id.in_(order_ids)).limit(batch_size)
            fills_removed = connection.execute(sa.delete(fills).where(fills.c.fill_id.in_(fill_ids))).rowcount
            transition_ids = sa.select(order_state_transitions.c.id).where(order_state_transitions.c.order_id.in_(order_ids)).limit(batch_size)
            transitions_removed = connection.execute(sa.delete(order_state_transitions).where(order_state_transitions.c.id.in_(transition_ids))).rowcount
            removable = sa.select(private_live_sessions.c.run_id).where(private_live_sessions.c.run_id.in_(expired),
                ~sa.exists(sa.select(market_events.c.id).where(market_events.c.run_id == private_live_sessions.c.run_id)),
                ~sa.exists(sa.select(fills.c.fill_id).join(orders, orders.c.order_id == fills.c.order_id).where(orders.c.run_id == private_live_sessions.c.run_id)),
                ~sa.exists(sa.select(order_state_transitions.c.id).join(orders, orders.c.order_id == order_state_transitions.c.order_id).where(orders.c.run_id == private_live_sessions.c.run_id))).limit(1)
            expired_run = connection.scalar(removable)
            if expired_run:
                connection.execute(sa.delete(orders).where(orders.c.run_id == expired_run))
                connection.execute(sa.delete(watchlist_symbols).where(watchlist_symbols.c.watchlist_id == expired_run))
                connection.execute(sa.delete(watchlists).where(watchlists.c.watchlist_id == expired_run))
                connection.execute(sa.delete(replay_session_settings).where(replay_session_settings.c.run_id == expired_run))
                connection.execute(sa.delete(private_live_sessions).where(private_live_sessions.c.run_id == expired_run))
                connection.execute(sa.delete(replay_runs).where(replay_runs.c.run_id == expired_run))
            return {"events": events_removed, "fills": fills_removed, "transitions": transitions_removed, "sessions": int(expired_run is not None)}

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

    def list_symbols(self) -> list[str]:
        with self._engine.connect() as connection:
            return list(connection.scalars(sa.select(market_events.c.symbol).distinct().order_by(market_events.c.symbol)))

    def replay_for_run(self, run_id: UUID) -> dict[str, object] | None:
        with self._engine.connect() as connection:
            replay = connection.execute(
                sa.select(replay_runs).where(replay_runs.c.run_id == run_id)
            ).mappings().one_or_none()
            if replay is None:
                return None
            settings = connection.execute(
                sa.select(replay_session_settings).where(replay_session_settings.c.run_id == run_id)
            ).mappings().one_or_none()
            return {
                **dict(replay),
                "counts": self.counts_for_run(run_id),
                "settings": dict(settings) if settings is not None else None,
            }

    def order_for_id(self, order_id: UUID) -> dict[str, object] | None:
        with self._engine.connect() as connection:
            order = connection.execute(sa.select(orders).where(orders.c.order_id == order_id)).mappings().one_or_none()
            if order is None:
                return None
            return {
                **dict(order),
                "metrics": _order_metrics(order),
                "fills": [
                    dict(fill)
                    for fill in connection.execute(
                        sa.select(fills).where(fills.c.order_id == order_id).order_by(fills.c.filled_at)
                    ).mappings()
                ],
                "transitions": [
                    dict(transition)
                    for transition in connection.execute(
                        sa.select(order_state_transitions)
                        .where(order_state_transitions.c.order_id == order_id)
                        .order_by(order_state_transitions.c.changed_at)
                    ).mappings()
                ],
            }

    def orders_for_run(self, run_id: UUID) -> list[dict[str, object]]:
        with self._engine.connect() as connection:
            order_ids = list(connection.scalars(sa.select(orders.c.order_id).where(orders.c.run_id == run_id)))
        return [order for order_id in order_ids if (order := self.order_for_id(order_id)) is not None]

    def events_for_run(self, run_id: UUID, limit: int) -> list[dict[str, object]]:
        with self._engine.connect() as connection:
            rows = list(
                connection.execute(
                    sa.select(market_events)
                    .where(market_events.c.run_id == run_id)
                    .order_by(market_events.c.event_time.desc(), market_events.c.sequence.desc())
                    .limit(limit)
                ).mappings()
            )
        return [dict(row) for row in reversed(rows)]

    def events_for_order(self, order_id: UUID) -> list[dict[str, object]]:
        with self._engine.connect() as connection:
            rows = connection.execute(
                sa.select(market_events)
                .join(fills, fills.c.triggering_event_id == market_events.c.event_id)
                .join(orders, orders.c.order_id == fills.c.order_id)
                .where(orders.c.order_id == order_id, market_events.c.run_id == orders.c.run_id)
                .order_by(market_events.c.event_time, market_events.c.sequence)
            ).mappings()
            return [dict(row) for row in rows]

    def list_watchlists(self) -> list[dict[str, object]]:
        with self._engine.connect() as connection:
            return [
                {
                    **dict(watchlist),
                    "symbols": list(
                        connection.scalars(
                            sa.select(watchlist_symbols.c.symbol)
                            .where(watchlist_symbols.c.watchlist_id == watchlist["watchlist_id"])
                            .order_by(watchlist_symbols.c.symbol)
                        )
                    ),
                }
                for watchlist in connection.execute(sa.select(watchlists).order_by(watchlists.c.created_at)).mappings()
            ]

    def create_watchlist(self, watchlist_id: UUID, name: str, symbols: list[str], created_at: datetime) -> None:
        with self._engine.begin() as connection:
            connection.execute(sa.insert(watchlists).values(watchlist_id=watchlist_id, name=name, created_at=created_at))
            if symbols:
                connection.execute(
                    sa.insert(watchlist_symbols),
                    [{"watchlist_id": watchlist_id, "symbol": symbol} for symbol in symbols],
                )

    def update_watchlist(self, watchlist_id: UUID, name: str, symbols: list[str]) -> bool:
        with self._engine.begin() as connection:
            updated = connection.execute(
                sa.update(watchlists).where(watchlists.c.watchlist_id == watchlist_id).values(name=name)
            )
            if updated.rowcount == 0:
                return False
            connection.execute(sa.delete(watchlist_symbols).where(watchlist_symbols.c.watchlist_id == watchlist_id))
            if symbols:
                connection.execute(
                    sa.insert(watchlist_symbols),
                    [{"watchlist_id": watchlist_id, "symbol": symbol} for symbol in symbols],
                )
            return True

    def watchlist_for_id(self, watchlist_id: UUID) -> dict[str, object] | None:
        with self._engine.connect() as connection:
            watchlist = connection.execute(
                sa.select(watchlists).where(watchlists.c.watchlist_id == watchlist_id)
            ).mappings().one_or_none()
            if watchlist is None:
                return None
            return {
                **dict(watchlist),
                "symbols": list(
                    connection.scalars(
                        sa.select(watchlist_symbols.c.symbol)
                        .where(watchlist_symbols.c.watchlist_id == watchlist_id)
                        .order_by(watchlist_symbols.c.symbol)
                    )
                ),
            }


def _milliseconds(value) -> int | None:
    return int(value.total_seconds() * 1_000) if value is not None else None


def _order_metrics(order) -> dict[str, object] | None:
    if order["final_state"] is None:
        return None
    return {
        "average_fill_price": order["average_fill_price"],
        "fill_rate": order["fill_rate"],
        "spread_cost": order["spread_cost"],
        "time_to_first_fill_ms": order["time_to_first_fill_ms"],
        "time_to_completion_ms": order["time_to_completion_ms"],
        "latency_impact": order["latency_impact"],
    }
