"""Store the metric script digest on the dataset version and drop the file and score cache tables.

Revision ID: f3c1d9a7b2e4
Revises: 125d7d985c7c
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f3c1d9a7b2e4"
down_revision: str | Sequence[str] | None = "125d7d985c7c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("dataset_version", sa.Column("metric_digest", sa.String(), nullable=True))
    op.execute(
        "UPDATE dataset_version SET metric_digest = dataset_version_file.digest FROM dataset_version_file "
        "WHERE dataset_version_file.version_id = dataset_version.id AND dataset_version_file.path = 'metric.py'"
    )
    op.drop_table("dataset_version_file")
    op.drop_table("dataset_score_cache")


def downgrade() -> None:
    op.create_table(
        "dataset_score_cache",
        sa.Column("script_digest", sa.String(), nullable=False),
        sa.Column("input_digest", sa.String(), nullable=False),
        sa.Column("metrics", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("script_digest", "input_digest"),
    )
    op.create_table(
        "dataset_version_file",
        sa.Column("version_id", sa.Uuid(), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("digest", sa.String(), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["version_id"], ["dataset_version.id"]),
        sa.PrimaryKeyConstraint("version_id", "path"),
    )
    op.execute(
        "INSERT INTO dataset_version_file (version_id, path, digest, size_bytes) "
        "SELECT id, 'metric.py', metric_digest, 0 FROM dataset_version WHERE metric_digest IS NOT NULL"
    )
    op.drop_column("dataset_version", "metric_digest")
