from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "project",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("handle", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("spend_type", sa.String(), nullable=False),
        sa.Column("subcategory", sa.String(), nullable=True),
        sa.Column("customer", sa.String(), nullable=True),
        sa.Column("extra_tags", sa.JSON(), nullable=False),
        sa.Column("gateway_request_id", sa.Uuid(), nullable=True),
        sa.Column("gateway_key_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by_email", sa.String(), nullable=True),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "handle", name="unique_project_handle_per_org"),
    )
    op.create_index("uq_project_org_name", "project", ["org_id", sa.text("lower(trim(name))")], unique=True)


def downgrade() -> None:
    op.drop_index("uq_project_org_name", table_name="project")
    op.drop_table("project")
