from alembic import op
import sqlalchemy as sa


revision = "0005_private_session_lifecycle"
down_revision = "0004_execution_metrics"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("order_state_transitions", sa.Column("reason", sa.String(128)))
    op.create_table("private_live_sessions",
        sa.Column("run_id", sa.Uuid(as_uuid=True), sa.ForeignKey("replay_runs.run_id"), primary_key=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closing_at", sa.DateTime(timezone=True)),
        sa.Column("closed_at", sa.DateTime(timezone=True)),
        sa.Column("next_run_id", sa.Uuid(as_uuid=True)),
        sa.Column("redis_expired", sa.Boolean, nullable=False, server_default=sa.false()))
    op.create_index("ix_private_live_sessions_closed_at", "private_live_sessions", ["closed_at"])


def downgrade() -> None:
    op.drop_table("private_live_sessions")
    op.drop_column("order_state_transitions", "reason")
