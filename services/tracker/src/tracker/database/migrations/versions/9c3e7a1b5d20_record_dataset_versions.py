"""Record each dataset version, its tasks and its tags.

Revision ID: 9c3e7a1b5d20
Revises: a6e7d8f9b0c1
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "9c3e7a1b5d20"
down_revision: str | Sequence[str] | None = "a6e7d8f9b0c1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "dataset_version",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("dataset_id", sa.Uuid(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("digest", sa.String(), nullable=False),
        sa.Column("task_count", sa.Integer(), nullable=False),
        sa.Column("created_by_email", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dataset_id"], ["dataset.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dataset_id", "digest", name="uq_dataset_version_digest"),
        sa.UniqueConstraint("dataset_id", "revision", name="uq_dataset_version_revision"),
    )
    op.create_table(
        "dataset_tag",
        sa.Column("dataset_id", sa.Uuid(), nullable=False),
        sa.Column("tag", sa.String(), nullable=False),
        sa.Column("version_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(["dataset_id"], ["dataset.id"]),
        sa.ForeignKeyConstraint(["version_id"], ["dataset_version.id"]),
        sa.PrimaryKeyConstraint("dataset_id", "tag"),
    )
    op.create_index(op.f("ix_dataset_tag_version_id"), "dataset_tag", ["version_id"], unique=False)
    op.create_table(
        "dataset_version_task",
        sa.Column("version_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.String(), nullable=False),
        sa.Column("digest", sa.String(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(["version_id"], ["dataset_version.id"]),
        sa.PrimaryKeyConstraint("version_id", "task_id"),
    )


def downgrade() -> None:
    op.drop_table("dataset_version_task")
    op.drop_index(op.f("ix_dataset_tag_version_id"), table_name="dataset_tag")
    op.drop_table("dataset_tag")
    op.drop_table("dataset_version")
