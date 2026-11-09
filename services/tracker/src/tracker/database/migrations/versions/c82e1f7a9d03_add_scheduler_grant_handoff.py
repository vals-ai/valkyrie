"""Add the coordinator-to-executor reservation handoff.

Revision ID: c82e1f7a9d03
Revises: 5a6b7c8d9e0f
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c82e1f7a9d03"
down_revision: str | Sequence[str] | None = "5a6b7c8d9e0f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("sandboxbuildreservation", sa.Column("executor_dispatch_id", sa.Uuid(), nullable=True))
    op.add_column("sandboxbuildreservation", sa.Column("accepted_at", sa.DateTime(), nullable=True))
    op.create_foreign_key(
        "fk_sandboxbuildreservation_dispatch",
        "sandboxbuildreservation",
        "executordispatch",
        ["executor_dispatch_id"],
        ["id"],
    )
    # Existing reservations were created by the executor, not offered by a coordinator.
    op.execute("UPDATE sandboxbuildreservation SET accepted_at = reserved_at")
    op.add_column("sandboxadmissioncandidate", sa.Column("readiness_checked_at", sa.DateTime(), nullable=True))
    op.add_column("sandboxadmissioncandidate", sa.Column("readiness_requested_at", sa.DateTime(), nullable=True))
    op.execute("UPDATE sandboxadmissioncandidate SET readiness_checked_at = clock_timestamp()")
    op.alter_column("sandboxadmissioncandidate", "readiness_checked_at", existing_type=sa.DateTime(), nullable=False)


def downgrade() -> None:
    op.drop_column("sandboxadmissioncandidate", "readiness_requested_at")
    op.drop_column("sandboxadmissioncandidate", "readiness_checked_at")
    op.drop_constraint(
        "fk_sandboxbuildreservation_dispatch",
        "sandboxbuildreservation",
        type_="foreignkey",
    )
    op.drop_column("sandboxbuildreservation", "accepted_at")
    op.drop_column("sandboxbuildreservation", "executor_dispatch_id")
