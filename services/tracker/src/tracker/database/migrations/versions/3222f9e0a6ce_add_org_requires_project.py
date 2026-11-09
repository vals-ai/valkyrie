"""Let an organization require a project on every run start; every organization starts with it off.

Revision ID: 3222f9e0a6ce
Revises: 3f8b2c6d1e4a
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "3222f9e0a6ce"
down_revision: str | Sequence[str] | None = "3f8b2c6d1e4a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("org", sa.Column("requires_project", sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade() -> None:
    op.drop_column("org", "requires_project")
