import sqlalchemy as sa
from alembic import op

revision = "e2d4c6b8a0f1"
down_revision = ("c1e2f3a4b5d6", "6c7d8e9f0a1b")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("executordispatch", sa.Column("claim_deadline_at", sa.DateTime(), nullable=True))
    op.execute(
        "UPDATE executordispatch SET claim_deadline_at = created_at + INTERVAL '120 seconds' WHERE status = 'QUEUED'"
    )


def downgrade() -> None:
    op.drop_column("executordispatch", "claim_deadline_at")
