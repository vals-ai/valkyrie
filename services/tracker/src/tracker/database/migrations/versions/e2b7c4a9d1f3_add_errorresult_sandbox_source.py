"""Add the sandbox source to error results

Revision ID: e2b7c4a9d1f3
Revises: a83d67c12e49
Create Date: 2026-11-13 19:30:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "e2b7c4a9d1f3"
down_revision: Union[str, Sequence[str], None] = "a83d67c12e49"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("errorresult", sa.Column("sandbox_source", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("errorresult", "sandbox_source")
