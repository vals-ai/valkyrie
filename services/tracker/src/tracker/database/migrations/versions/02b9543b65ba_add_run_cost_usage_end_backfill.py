"""Save which run costs got their usage_end from Daytona, so the backfill can be undone.

Revision ID: 02b9543b65ba
Revises: c90685b18a8a
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "02b9543b65ba"
down_revision: str | Sequence[str] | None = "c90685b18a8a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "run_cost_usage_end_backfill",
        sa.Column("run_cost_id", sa.Uuid(), sa.ForeignKey("run_cost.id"), primary_key=True),
        sa.Column("backfilled_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("run_cost_usage_end_backfill")
