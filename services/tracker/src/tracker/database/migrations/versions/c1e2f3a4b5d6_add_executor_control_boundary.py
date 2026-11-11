from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "c1e2f3a4b5d6"
down_revision = "b7e4c1a9d2f0"
branch_labels = None
depends_on = None
deployment_policy = "initial-adoption"


def upgrade() -> None:
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
    op.add_column("executoradmission", sa.Column("active_epoch_id", postgresql.UUID(as_uuid=True)))
    op.create_foreign_key(
        "fk_executoradmission_active_epoch", "executoradmission", "migrationepoch", ["active_epoch_id"], ["id"]
    )
    op.add_column("executordispatch", sa.Column("invocation_key_digest", sa.LargeBinary()))
    op.add_column("executordispatch", sa.Column("claim_receipt_at", sa.DateTime(timezone=True)))
    op.add_column("executordispatch", sa.Column("frozen_epoch_id", postgresql.UUID(as_uuid=True)))
    op.add_column("executordispatch", sa.Column("frozen_remaining_seconds", sa.Float()))
    op.add_column("executordispatch", sa.Column("queued_park_invocation_digest", sa.LargeBinary()))
    op.create_foreign_key(
        "fk_executordispatch_frozen_epoch", "executordispatch", "migrationepoch", ["frozen_epoch_id"], ["id"]
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
    for column in ("build_phase_dispatch_id", "evaluation_phase_dispatch_id"):
        op.add_column("task", sa.Column(column, postgresql.UUID(as_uuid=True)))
        op.create_foreign_key(f"fk_task_{column}", "task", "executordispatch", [column], ["id"])
    for column in ("build_phase_token", "evaluation_phase_token"):
        op.add_column("task", sa.Column(column, postgresql.UUID(as_uuid=True)))
    op.add_column("sandboxbuildreservation", sa.Column("phase_dispatch_id", postgresql.UUID(as_uuid=True)))
    op.create_foreign_key(
        "fk_sandboxbuildreservation_phase_dispatch",
        "sandboxbuildreservation",
        "executordispatch",
        ["phase_dispatch_id"],
        ["id"],
    )
    op.add_column("sandboxbuildreservation", sa.Column("phase_token", postgresql.UUID(as_uuid=True)))
    op.add_column("benchmark", sa.Column("publication_phase_dispatch_id", postgresql.UUID(as_uuid=True)))
    op.create_foreign_key(
        "fk_benchmark_publication_dispatch", "benchmark", "executordispatch", ["publication_phase_dispatch_id"], ["id"]
    )
    op.add_column("benchmark", sa.Column("publication_phase_token", postgresql.UUID(as_uuid=True)))
    op.add_column("benchmark", sa.Column("publication_phase_fingerprint", sa.String()))
    op.add_column("benchmark", sa.Column("publication_status", sa.String()))


def downgrade() -> None:
    for column in ("publication_status", "publication_phase_fingerprint", "publication_phase_token"):
        op.drop_column("benchmark", column)
    op.drop_constraint("fk_benchmark_publication_dispatch", "benchmark", type_="foreignkey")
    op.drop_column("benchmark", "publication_phase_dispatch_id")
    op.drop_column("sandboxbuildreservation", "phase_token")
    op.drop_constraint("fk_sandboxbuildreservation_phase_dispatch", "sandboxbuildreservation", type_="foreignkey")
    op.drop_column("sandboxbuildreservation", "phase_dispatch_id")
    for column in ("evaluation_phase_token", "build_phase_token"):
        op.drop_column("task", column)
    for column in ("evaluation_phase_dispatch_id", "build_phase_dispatch_id"):
        op.drop_constraint(f"fk_task_{column}", "task", type_="foreignkey")
        op.drop_column("task", column)
    op.drop_table("migrationparticipant")
    op.drop_constraint("fk_executordispatch_frozen_epoch", "executordispatch", type_="foreignkey")
    for column in (
        "queued_park_invocation_digest",
        "frozen_remaining_seconds",
        "frozen_epoch_id",
        "claim_receipt_at",
        "invocation_key_digest",
    ):
        op.drop_column("executordispatch", column)
    op.drop_constraint("fk_executoradmission_active_epoch", "executoradmission", type_="foreignkey")
    op.drop_column("executoradmission", "active_epoch_id")
    op.drop_table("migrationepoch")
