from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "403df27aa5c9"
down_revision: str | Sequence[str] | None = "f16c2a718b94"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '1s'")
    op.add_column("task", sa.Column("last_log_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '1s'")
    op.drop_column("task", "last_log_at")
