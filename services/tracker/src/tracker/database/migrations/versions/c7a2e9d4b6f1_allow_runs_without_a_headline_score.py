"""Let a final evaluation have no headline score.

Revision ID: c7a2e9d4b6f1
Revises: b4d8f2a6c1e3
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c7a2e9d4b6f1"
down_revision: str | Sequence[str] | None = "b4d8f2a6c1e3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '1s'")
    op.alter_column("finalevaluation", "final_score", existing_type=sa.Float(), nullable=True)


def downgrade() -> None:
    op.execute("UPDATE finalevaluation SET final_score = 0 WHERE final_score IS NULL")
    op.alter_column("finalevaluation", "final_score", existing_type=sa.Float(), nullable=False)
