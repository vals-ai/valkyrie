from alembic import op
import sqlalchemy as sa

revision = "e8b2d4f6a1c3"
down_revision = "f1d3b5a7c9e2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "uq_run_cost_search_benchmark_provider_source_id",
        "run_cost",
        ["benchmark_id", "provider", "source_id"],
        unique=True,
        postgresql_where=sa.text("cost_type = 'search'"),
    )


def downgrade() -> None:
    op.drop_index("uq_run_cost_search_benchmark_provider_source_id", table_name="run_cost")
