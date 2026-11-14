from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b6d8e1c3f902"
down_revision: str | Sequence[str] | None = "a4c70e9d133f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "project_backfill",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("source_hash", sa.String(length=64), nullable=False),
        sa.Column("confirmed", sa.Boolean(), nullable=False),
        sa.Column("copied_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("project_id"),
    )


def downgrade() -> None:
    op.drop_table("project_backfill")
