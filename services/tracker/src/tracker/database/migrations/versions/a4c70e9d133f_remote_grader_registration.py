"""Persist trusted remote grader grants, registrations and Gateway child custody.

Revision ID: a4c70e9d133f
Revises: e4b8a1c7d2f9
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a4c70e9d133f"
down_revision: str | Sequence[str] | None = "e4b8a1c7d2f9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "remotegradergrant",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("token_digest", sa.String(), nullable=False, unique=True),
        sa.Column("token_ciphertext", sa.LargeBinary(), nullable=True),
        sa.Column("org_id", sa.Uuid(), sa.ForeignKey("org.id"), nullable=False),
        sa.Column("task_row_id", sa.Uuid(), sa.ForeignKey("task.id"), nullable=False),
        sa.Column("attempt_id", sa.String(), nullable=False),
        sa.Column("worker_service", sa.String(), nullable=False),
        sa.Column("gateway_secret_ref", sa.String(), nullable=False),
        sa.Column("identity", sa.JSON(), nullable=False),
        sa.Column("closed_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint(
            "task_row_id", "attempt_id", "worker_service", name="uq_remote_grader_grant_attempt_service"
        ),
    )
    op.create_table(
        "remotegraderregistration",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("grant_id", sa.Uuid(), sa.ForeignKey("remotegradergrant.id"), nullable=False),
        sa.Column("worker_service", sa.String(), nullable=False),
        sa.Column("job_id", sa.String(), nullable=False),
        sa.Column("task_id", sa.String(), nullable=False),
        sa.Column("attempt_id", sa.String(), nullable=False),
        sa.Column("allowed_models", sa.JSON(), nullable=False),
        sa.Column("first_pickup_deadline", sa.Float(), nullable=False),
        sa.Column("gateway_url", sa.String(), nullable=True),
        sa.Column("gateway_lease_id", sa.String(), nullable=True),
        sa.Column("token_ciphertext", sa.LargeBinary(), nullable=True),
        sa.Column("expires_at", sa.Float(), nullable=True),
        sa.Column("stopped_at", sa.DateTime(), nullable=True),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint(
            "worker_service", "job_id", "task_id", "attempt_id", name="uq_remote_grader_registration_remote_attempt"
        ),
    )


def downgrade() -> None:
    op.drop_table("remotegraderregistration")
    op.drop_table("remotegradergrant")
