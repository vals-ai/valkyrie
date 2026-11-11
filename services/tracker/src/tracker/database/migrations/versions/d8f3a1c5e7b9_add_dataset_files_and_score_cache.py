"""Record the files that belong to a dataset version and the scores computed from them.

Revision ID: d8f3a1c5e7b9
Revises: c7a2e9d4b6f1
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d8f3a1c5e7b9"
down_revision: str | Sequence[str] | None = "c7a2e9d4b6f1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "dataset_version_file",
        sa.Column("version_id", sa.Uuid(), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("digest", sa.String(), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["version_id"], ["dataset_version.id"]),
        sa.PrimaryKeyConstraint("version_id", "path"),
    )
    op.create_table(
        "dataset_score_cache",
        sa.Column("script_digest", sa.String(), nullable=False),
        sa.Column("input_digest", sa.String(), nullable=False),
        sa.Column("metrics", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("script_digest", "input_digest"),
    )


def downgrade() -> None:
    op.drop_table("dataset_score_cache")
    op.drop_table("dataset_version_file")
