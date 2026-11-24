import sqlalchemy as sa
from alembic import op

revision: str = "e5b9d2f8a4c3"
down_revision: str | None = "c4e1a9d2b7f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(sa.text("UPDATE org SET requires_project = true WHERE name = 'vals.ai'"))


def downgrade() -> None:
    op.execute(sa.text("UPDATE org SET requires_project = false WHERE name = 'vals.ai'"))
