from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | Sequence[str] | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "project_limit",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("limit_dollars", sa.Numeric(), nullable=False),
        sa.Column("gateway_confirmed_limit", sa.Numeric(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("reason", sa.String(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("project_id"),
    )


def downgrade() -> None:
    op.drop_table("project_limit")
