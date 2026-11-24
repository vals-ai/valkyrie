from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "8158c0d1e46f"
down_revision: str | Sequence[str] | None = "403df27aa5c9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '1s'")
    op.add_column("errorresult", sa.Column("attempt_started_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '1s'")
    op.drop_column("errorresult", "attempt_started_at")
