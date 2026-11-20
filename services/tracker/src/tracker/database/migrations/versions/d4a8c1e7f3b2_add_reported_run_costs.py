"""Record who reported a run cost and the tokens that let a run's benchmark service report them.

Revision ID: d4a8c1e7f3b2
Revises: c3f7a2d9e1b4
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d4a8c1e7f3b2"
down_revision: str | Sequence[str] | None = "c3f7a2d9e1b4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add `run_cost.reporter`, its unique index and the `run_cost_report_token` table."""
    op.add_column("run_cost", sa.Column("reporter", sa.String(), nullable=True))
    op.create_index(
        "uq_run_cost_reported_source_id",
        "run_cost",
        ["benchmark_id", "attempt_id", "provider", "source_id", "cost_type"],
        unique=True,
        postgresql_where=sa.text("reporter IS NOT NULL"),
    )
    op.create_table(
        "run_cost_report_token",
        sa.Column("token_hash", sa.String(), primary_key=True),
        sa.Column("benchmark_id", sa.Uuid(), sa.ForeignKey("benchmark.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_run_cost_report_token_benchmark_id", "run_cost_report_token", ["benchmark_id"])


def downgrade() -> None:
    """Drop the token table, the index and the reporter column and keep every run cost row."""
    op.drop_index("ix_run_cost_report_token_benchmark_id", table_name="run_cost_report_token")
    op.drop_table("run_cost_report_token")
    op.drop_index("uq_run_cost_reported_source_id", table_name="run_cost")
    op.drop_column("run_cost", "reporter")
