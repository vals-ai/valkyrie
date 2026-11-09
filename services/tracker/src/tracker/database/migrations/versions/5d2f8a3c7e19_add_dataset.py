"""Record the benchmarks an organization owns and the datasets that belong to them.

Revision ID: 5d2f8a3c7e19
Revises: a4c90b4c4b43
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "5d2f8a3c7e19"
down_revision: str | Sequence[str] | None = "a4c90b4c4b43"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "benchmark_definition",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("description", sa.String(), nullable=False),
        sa.Column("created_by_email", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("uq_benchmark_definition_org_name", "benchmark_definition", ["org_id", "name"], unique=True)
    op.create_table(
        "dataset",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("benchmark_definition_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("description", sa.String(), nullable=False),
        sa.Column("created_by_email", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["benchmark_definition_id"], ["benchmark_definition.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("uq_dataset_benchmark_name", "dataset", ["benchmark_definition_id", "name"], unique=True)


def downgrade() -> None:
    op.drop_index("uq_dataset_benchmark_name", table_name="dataset")
    op.drop_table("dataset")
    op.drop_index("uq_benchmark_definition_org_name", table_name="benchmark_definition")
    op.drop_table("benchmark_definition")
