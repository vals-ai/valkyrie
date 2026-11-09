"""Record when each run cost's usage ended, so its spend is placed by that date.

Revision ID: 265bacf0f12a
Revises: a6e7d8f9b0c1
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "265bacf0f12a"
down_revision: str | Sequence[str] | None = "a6e7d8f9b0c1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("run_cost", sa.Column("usage_end", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("run_cost", "usage_end")
