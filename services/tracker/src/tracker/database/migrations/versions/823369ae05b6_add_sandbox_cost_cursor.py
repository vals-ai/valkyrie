"""Keep, per Daytona account, the oldest clock hour of sandbox usage the cost sweep has not yet read.

Revision ID: 823369ae05b6
Revises: 3222f9e0a6ce
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "823369ae05b6"
down_revision: str | Sequence[str] | None = "4c9d2e7f1a3b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sandbox_cost_cursor",
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("secret_name", sa.String(), nullable=False),
        sa.Column("next_hour", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"]),
        sa.PrimaryKeyConstraint("org_id", "secret_name"),
    )


def downgrade() -> None:
    op.drop_table("sandbox_cost_cursor")
