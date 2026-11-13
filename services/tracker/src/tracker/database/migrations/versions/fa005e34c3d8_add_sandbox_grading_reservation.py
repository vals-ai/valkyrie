"""Add per-attempt grading sandbox reservations.

Revision ID: fa005e34c3d8
Revises: d967e427b9ad
"""

import sqlalchemy as sa
from alembic import op

revision: str = "fa005e34c3d8"
down_revision: str = "d967e427b9ad"
branch_labels = None
depends_on = None

_TABLE = "sandboxgradingreservation"
_POOL_INDEX = "ix_sandboxgradingreservation_pool"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("task_row_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_started_at", sa.DateTime(), nullable=False),
        sa.Column("released_at", sa.DateTime(), nullable=True),
        sa.Column("pool_id", sa.String(), nullable=False),
        sa.Column("target_id", sa.String(), nullable=False),
        sa.Column("sandbox_class", sa.String(), nullable=False),
        sa.Column("requested_vcpu", sa.Float(), nullable=False),
        sa.Column("requested_memory", sa.Float(), nullable=False),
        sa.Column("requested_disk", sa.Float(), nullable=False),
        sa.Column("requested_gpu", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("task_row_id", "attempt_started_at"),
        sa.ForeignKeyConstraint(["task_row_id"], ["task.id"]),
    )
    op.create_index(_POOL_INDEX, _TABLE, ["pool_id"])


def downgrade() -> None:
    op.drop_index(_POOL_INDEX, table_name=_TABLE)
    op.drop_table(_TABLE)
