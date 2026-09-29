from alembic import op
import sqlalchemy as sa


revision = "0003_session_configuration"
down_revision = "0002_event_idempotency"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "watchlists",
        sa.Column("watchlist_id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "watchlist_symbols",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("watchlist_id", sa.Uuid(as_uuid=True), sa.ForeignKey("watchlists.watchlist_id"), nullable=False),
        sa.Column("symbol", sa.String(10), nullable=False),
        sa.UniqueConstraint("watchlist_id", "symbol", name="uq_watchlist_symbols_watchlist_symbol"),
    )
    op.create_table(
        "replay_session_settings",
        sa.Column("run_id", sa.Uuid(as_uuid=True), sa.ForeignKey("replay_runs.run_id"), primary_key=True),
        sa.Column("mode", sa.String(32), nullable=False),
        sa.Column("symbols", sa.JSON, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("replay_session_settings")
    op.drop_table("watchlist_symbols")
    op.drop_table("watchlists")
