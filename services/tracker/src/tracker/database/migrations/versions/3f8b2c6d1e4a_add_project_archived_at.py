"""Record when a project was archived, so new runs can't use it while its history stays.

Revision ID: 3f8b2c6d1e4a
Revises: 265bacf0f12a
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "3f8b2c6d1e4a"
down_revision: str | Sequence[str] | None = "265bacf0f12a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("project", sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("project", "archived_at")
