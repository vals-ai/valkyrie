from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c9e2f4a6b803"
down_revision: str | Sequence[str] | None = "b6d8e1c3f902"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "project_cutover",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("project_cutover")
