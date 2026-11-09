"""Add benchmarkservicewake

Revision ID: b7e4c2a9d1f3
Revises: 3222f9e0a6ce
Create Date: 2026-10-29 21:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
import sqlmodel
from alembic import op

revision: str = "b7e4c2a9d1f3"
down_revision: Union[str, Sequence[str], None] = "3222f9e0a6ce"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "benchmarkservicewake",
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("requested_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("name"),
    )


def downgrade() -> None:
    op.drop_table("benchmarkservicewake")
