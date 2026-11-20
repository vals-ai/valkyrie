"""Record each run cost's attempt and usage.

Revision ID: c3f7a2d9e1b4
Revises: e8b2d4f6a1c3
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c3f7a2d9e1b4"
down_revision: str | Sequence[str] | None = "e8b2d4f6a1c3"
branch_labels = None
depends_on = None

_SANDBOX_ROWS = "cost_type = 'sandbox'"
_SANDBOX_KEY = "uq_run_cost_sandbox_provider_source_id"


def upgrade() -> None:
    """Add nullable attempt, resource and usage columns, and key sandbox rows by resource too."""
    for table in ("run_cost", "sandbox"):
        op.add_column(table, sa.Column("attempt_id", sa.String(), nullable=True))
    for column, column_type in (("resource", sa.String()), ("quantity", sa.Float()), ("unit", sa.String())):
        op.add_column("run_cost", sa.Column(column, column_type, nullable=True))
    op.drop_index(_SANDBOX_KEY, table_name="run_cost")
    op.create_index(
        _SANDBOX_KEY,
        "run_cost",
        ["provider", "source_id", "resource"],
        unique=True,
        postgresql_where=sa.text(_SANDBOX_ROWS),
        postgresql_nulls_not_distinct=True,
    )


def downgrade() -> None:
    """Keep every row, moving per-resource sandbox rows to `sandbox_<resource>` types before dropping the columns."""
    op.execute(
        "UPDATE run_cost SET cost_type = 'sandbox_' || resource WHERE cost_type = 'sandbox' AND resource IS NOT NULL"
    )
    op.drop_index(_SANDBOX_KEY, table_name="run_cost")
    op.create_index(
        _SANDBOX_KEY, "run_cost", ["provider", "source_id"], unique=True, postgresql_where=sa.text(_SANDBOX_ROWS)
    )
    for column in ("unit", "quantity", "resource"):
        op.drop_column("run_cost", column)
    for table in ("run_cost", "sandbox"):
        op.drop_column(table, "attempt_id")
