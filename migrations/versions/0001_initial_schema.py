from alembic import op
import sqlalchemy as sa


revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "replay_runs",
        sa.Column("run_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("scenario_name", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
    )
    op.create_table(
        "market_events",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(128), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(10), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sequence", sa.Integer, nullable=False),
        sa.Column("partition", sa.Integer, nullable=False),
        sa.Column("payload", sa.JSON, nullable=False),
    )
    op.create_index("ix_market_events_run_id", "market_events", ["run_id"])
    op.create_index("ix_market_events_symbol", "market_events", ["symbol"])
    op.create_index("ix_market_events_event_time", "market_events", ["event_time"])
    op.create_table(
        "orders",
        sa.Column("order_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("run_id", sa.Uuid(as_uuid=True), nullable=False),
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
    op.create_index("ix_orders_run_id", "orders", ["run_id"])
    op.create_table(
        "fills",
        sa.Column("fill_id", sa.String(128), primary_key=True),
        sa.Column("order_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("triggering_event_id", sa.String(128), nullable=False),
        sa.Column("quantity", sa.Integer, nullable=False),
        sa.Column("price", sa.Numeric(18, 6), nullable=False),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_fills_order_id", "fills", ["order_id"])
    op.create_table(
        "order_state_transitions",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("order_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("triggering_event_id", sa.String(128)),
    )
    op.create_index("ix_order_state_transitions_order_id", "order_state_transitions", ["order_id"])


def downgrade() -> None:
    op.drop_index("ix_order_state_transitions_order_id", table_name="order_state_transitions")
    op.drop_table("order_state_transitions")
    op.drop_index("ix_fills_order_id", table_name="fills")
    op.drop_table("fills")
    op.drop_index("ix_orders_run_id", table_name="orders")
    op.drop_table("orders")
    op.drop_index("ix_market_events_event_time", table_name="market_events")
    op.drop_index("ix_market_events_symbol", table_name="market_events")
    op.drop_index("ix_market_events_run_id", table_name="market_events")
    op.drop_table("market_events")
    op.drop_table("replay_runs")
