"""Add per-attempt egress proxy policies.

Revision ID: 9a8b7c6d5e4f
Revises: 6c7d8e9f0a1b
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "9a8b7c6d5e4f"
down_revision: str | Sequence[str] | None = "6c7d8e9f0a1b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "egress_proxy_policy",
        sa.Column("key_id", sa.String(), nullable=False),
        sa.Column("benchmark_id", sa.Uuid(), nullable=False),
        sa.Column("token_sha256", sa.String(), nullable=False),
        sa.Column("methods", sa.JSON(), nullable=False),
        sa.Column("domains", sa.JSON(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["benchmark_id"], ["benchmark.id"]),
        sa.PrimaryKeyConstraint("key_id"),
    )
    op.create_index("ix_egress_proxy_policy_benchmark_id", "egress_proxy_policy", ["benchmark_id"])
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'egress_proxy_reader') THEN
                EXECUTE 'CREATE ROLE egress_proxy_reader LOGIN';
            END IF;
            EXECUTE 'GRANT USAGE ON SCHEMA public TO egress_proxy_reader';
            EXECUTE 'GRANT SELECT ON egress_proxy_policy TO egress_proxy_reader';
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_iam') THEN
                EXECUTE 'GRANT rds_iam TO egress_proxy_reader';
            END IF;
        END
        $$;
        """
    )


def downgrade() -> None:
    op.drop_index("ix_egress_proxy_policy_benchmark_id", table_name="egress_proxy_policy")
    op.drop_table("egress_proxy_policy")
