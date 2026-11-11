"""Record how often each executor dispatch's runner was relaunched and why the last one stopped.

Revision ID: 96defe9ab703
Revises: 265bacf0f12a
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "96defe9ab703"
down_revision: str | Sequence[str] | None = "265bacf0f12a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("executordispatch", sa.Column("relaunch_count", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("executordispatch", sa.Column("last_relaunch_reason", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("executordispatch", "last_relaunch_reason")
    op.drop_column("executordispatch", "relaunch_count")
