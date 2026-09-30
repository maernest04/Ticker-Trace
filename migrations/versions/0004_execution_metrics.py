from alembic import op
import sqlalchemy as sa


revision = "0004_execution_metrics"
down_revision = "0003_session_configuration"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("orders", sa.Column("average_fill_price", sa.Numeric(18, 6)))
    op.add_column("orders", sa.Column("fill_rate", sa.Numeric(18, 6)))
    op.add_column("orders", sa.Column("spread_cost", sa.Numeric(18, 6)))
    op.add_column("orders", sa.Column("time_to_first_fill_ms", sa.Integer()))
    op.add_column("orders", sa.Column("time_to_completion_ms", sa.Integer()))
    op.add_column("orders", sa.Column("latency_impact", sa.Numeric(18, 6)))


def downgrade() -> None:
    op.drop_column("orders", "latency_impact")
    op.drop_column("orders", "time_to_completion_ms")
    op.drop_column("orders", "time_to_first_fill_ms")
    op.drop_column("orders", "spread_cost")
    op.drop_column("orders", "fill_rate")
    op.drop_column("orders", "average_fill_price")
