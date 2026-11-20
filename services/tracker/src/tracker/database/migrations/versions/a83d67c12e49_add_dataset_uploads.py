from alembic import op
import sqlalchemy as sa

revision = "a83d67c12e49"
down_revision = "d4a8c1e7f3b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dataset_upload",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("dataset_id", sa.Uuid(), nullable=False),
        sa.Column("owner", sa.String(), nullable=False),
        sa.Column("created_by_email", sa.String(), nullable=True),
        sa.Column("archive_sha256", sa.String(), nullable=False),
        sa.Column("archive_size", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(length=14), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("lease_id", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_deadline", sa.DateTime(timezone=True), nullable=True),
        sa.Column("review", sa.JSON(), nullable=True),
        sa.Column("review_digest", sa.String(), nullable=True),
        sa.Column("task_count", sa.Integer(), nullable=True),
        sa.Column("total_task_bytes", sa.BigInteger(), nullable=True),
        sa.Column("metric_digest", sa.String(), nullable=True),
        sa.Column("tags", sa.JSON(), nullable=False),
        sa.Column("warnings", sa.JSON(), nullable=False),
        sa.Column("error", sa.String(), nullable=True),
        sa.Column("published_version_id", sa.Uuid(), nullable=True),
        sa.Column("archive_cleanup_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("archive_size > 0", name="ck_dataset_upload_positive_size"),
        sa.ForeignKeyConstraint(["dataset_id"], ["dataset.id"]),
        sa.ForeignKeyConstraint(["published_version_id"], ["dataset_version.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_dataset_upload_dataset_created", "dataset_upload", ["dataset_id", "created_at"])
    op.create_index("ix_dataset_upload_status_lease", "dataset_upload", ["status", "lease_expires_at"])
    op.create_index("ix_dataset_upload_expires", "dataset_upload", ["expires_at"])
    op.create_index("ix_dataset_upload_cleanup", "dataset_upload", ["archive_cleanup_at"])


def downgrade() -> None:
    op.drop_index("ix_dataset_upload_cleanup", table_name="dataset_upload")
    op.drop_index("ix_dataset_upload_expires", table_name="dataset_upload")
    op.drop_index("ix_dataset_upload_status_lease", table_name="dataset_upload")
    op.drop_index("ix_dataset_upload_dataset_created", table_name="dataset_upload")
    op.drop_table("dataset_upload")
