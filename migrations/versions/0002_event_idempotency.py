from alembic import op


revision = "0002_event_idempotency"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint("uq_market_events_run_event", "market_events", ["run_id", "event_id"])


def downgrade() -> None:
    op.drop_constraint("uq_market_events_run_event", "market_events", type_="unique")
