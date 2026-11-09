"""Record each project's Gateway request and issued key identifiers.

Revision ID: a6e7d8f9b0c1
Revises: 5d2f8a3c7e19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a6e7d8f9b0c1"
down_revision: str | Sequence[str] | None = "5d2f8a3c7e19"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("project", sa.Column("gateway_request_id", sa.Uuid(), nullable=True))
    op.add_column("project", sa.Column("gateway_key_id", sa.Uuid(), nullable=True))


def downgrade() -> None:
    op.drop_column("project", "gateway_key_id")
    op.drop_column("project", "gateway_request_id")
