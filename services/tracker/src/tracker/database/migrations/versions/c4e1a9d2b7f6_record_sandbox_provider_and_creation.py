"""Record the sandbox provider on each error and when each sandbox was first linked.

Revision ID: c4e1a9d2b7f6
Revises: e2b7c4a9d1f3
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c4e1a9d2b7f6"
down_revision: str | Sequence[str] | None = "e2b7c4a9d1f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("errorresult", sa.Column("sandbox_provider", sa.String(), nullable=True))
    op.add_column("sandbox", sa.Column("created_at", sa.DateTime(timezone=True), nullable=True))
    op.alter_column("sandbox", "created_at", server_default=sa.func.now())


def downgrade() -> None:
    op.drop_column("sandbox", "created_at")
    op.drop_column("errorresult", "sandbox_provider")
