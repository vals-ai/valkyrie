"""Keep the last time the Tracker or its cost sweep saw each linked sandbox alive.

Revision ID: d967e427b9ad
Revises: d4e8a1c6b9f2
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d967e427b9ad"
down_revision: str | Sequence[str] | None = "d4e8a1c6b9f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("sandbox", sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("sandbox", "last_seen_at")
