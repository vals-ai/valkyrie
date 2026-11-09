"""Add inspection domains and the run's gateway URL to egress proxy policies.

Revision ID: f1e2d3c4b5a6
Revises: 9a8b7c6d5e4f
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f1e2d3c4b5a6"
down_revision: str | Sequence[str] | None = "9a8b7c6d5e4f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "egress_proxy_policy",
        sa.Column("inspect_domains", sa.JSON(), server_default=sa.text("'[]'"), nullable=False),
    )
    op.add_column("egress_proxy_policy", sa.Column("gateway_url", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("egress_proxy_policy", "gateway_url")
    op.drop_column("egress_proxy_policy", "inspect_domains")
