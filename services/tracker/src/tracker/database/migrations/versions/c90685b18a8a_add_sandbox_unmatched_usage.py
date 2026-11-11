"""Keep, per Daytona account hour, the usage from sandboxes no run recorded.

Revision ID: c90685b18a8a
Revises: e3a7c1d9b5f2
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c90685b18a8a"
down_revision: str | Sequence[str] | None = "e3a7c1d9b5f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sandbox_unmatched_usage",
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("secret_name", sa.String(), nullable=False),
        sa.Column("hour", sa.DateTime(timezone=True), nullable=False),
        sa.Column("amount_usd", sa.Float(), nullable=False),
        sa.Column("sandboxes", sa.Integer(), nullable=False),
        sa.Column("unpriced_sandboxes", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"]),
        sa.PrimaryKeyConstraint("org_id", "secret_name", "hour"),
    )


def downgrade() -> None:
    op.drop_table("sandbox_unmatched_usage")
