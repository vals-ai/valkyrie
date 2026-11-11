"""Record the dataset.toml a version was published with and each task's name in it.

Revision ID: b4d8f2a6c1e3
Revises: 9c3e7a1b5d20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b4d8f2a6c1e3"
down_revision: str | Sequence[str] | None = "9c3e7a1b5d20"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("dataset_version", sa.Column("manifest", sa.JSON(), nullable=True))
    op.add_column("dataset_version_task", sa.Column("source_name", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("dataset_version_task", "source_name")
    op.drop_column("dataset_version", "manifest")
