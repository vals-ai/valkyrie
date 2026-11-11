"""Drop the migration epoch tables and the epoch, park and successor columns that planned migrations used.

Revision ID: d4e8a1c6b9f2
Revises: a7d2c4e9f1b3
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d4e8a1c6b9f2"
down_revision: str | Sequence[str] | None = "a7d2c4e9f1b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
requires_maintenance = True
_DISPATCH_COLUMNS = (
    "frozen_epoch_id",
    "frozen_remaining_seconds",
    "queued_park_invocation_digest",
    "superseded_by_dispatch_id",
    "successor_launch_claimed_at",
)


def upgrade() -> None:
    op.drop_constraint("fk_executoradmission_active_epoch", "executoradmission", type_="foreignkey")
    op.drop_constraint("fk_executordispatch_frozen_epoch", "executordispatch", type_="foreignkey")
    op.drop_table("migrationparticipant")
    op.drop_table("migrationepoch")
    op.drop_column("executoradmission", "active_epoch_id")
    for column in _DISPATCH_COLUMNS:
        op.drop_column("executordispatch", column)


def downgrade() -> None:
    op.add_column("executoradmission", sa.Column("active_epoch_id", postgresql.UUID(as_uuid=True)))
    op.add_column("executordispatch", sa.Column("frozen_epoch_id", postgresql.UUID(as_uuid=True)))
    op.add_column("executordispatch", sa.Column("frozen_remaining_seconds", sa.Float()))
    op.add_column("executordispatch", sa.Column("queued_park_invocation_digest", sa.LargeBinary()))
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
    op.create_table(
        "migrationepoch",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("phase", sa.String(), nullable=False),
        sa.Column("permit_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_revision", sa.String(), nullable=False),
        sa.Column("destination_revision", sa.String(), nullable=False),
        sa.Column("schema_revision", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "migrationparticipant",
        sa.Column("epoch_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("migrationepoch.id"), primary_key=True),
        sa.Column("dispatch_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("executordispatch.id"), primary_key=True),
        sa.Column("benchmark_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("benchmark.id"), nullable=False),
        sa.Column("invocation_key_digest", sa.LargeBinary(), nullable=False),
        sa.Column("executor_release_id", sa.String(), sa.ForeignKey("executorrelease.id"), nullable=False),
        sa.Column("permit_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("parked_at", sa.DateTime(timezone=True)),
        sa.Column("rejoined_at", sa.DateTime(timezone=True)),
        sa.Column("returned_at", sa.DateTime(timezone=True)),
        sa.Column("attempt_phase_snapshot", sa.JSON()),
        sa.Column("rejoin_receipt", postgresql.UUID(as_uuid=True)),
        sa.Column("ordinary_lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("disposition", sa.String()),
    )
    op.create_foreign_key(
        "fk_executordispatch_frozen_epoch", "executordispatch", "migrationepoch", ["frozen_epoch_id"], ["id"]
    )
    op.create_foreign_key(
        "fk_executoradmission_active_epoch", "executoradmission", "migrationepoch", ["active_epoch_id"], ["id"]
    )
