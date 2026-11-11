from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "8f2a6b9c1d30"
down_revision = "e2d4c6b8a0f1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE executordispatchstatus ADD VALUE IF NOT EXISTS 'SUPERSEDED'")
    op.add_column("executordispatch", sa.Column("superseded_by_dispatch_id", postgresql.UUID(as_uuid=True)))
    op.create_foreign_key(
        "fk_executordispatch_superseded_by",
        "executordispatch",
        "executordispatch",
        ["superseded_by_dispatch_id"],
        ["id"],
    )
    op.create_unique_constraint("uq_executordispatch_superseded_by", "executordispatch", ["superseded_by_dispatch_id"])
    op.add_column("executordispatch", sa.Column("successor_launch_claimed_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    op.drop_column("executordispatch", "successor_launch_claimed_at")
    op.drop_constraint("uq_executordispatch_superseded_by", "executordispatch", type_="unique")
    op.drop_constraint("fk_executordispatch_superseded_by", "executordispatch", type_="foreignkey")
    op.drop_column("executordispatch", "superseded_by_dispatch_id")
