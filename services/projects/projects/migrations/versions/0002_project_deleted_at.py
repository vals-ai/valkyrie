from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LIVE = sa.text("deleted_at IS NULL")


def upgrade() -> None:
    op.drop_index("uq_project_org_name", table_name="project")
    with op.batch_alter_table("project") as batch:
        batch.add_column(sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True))
        batch.drop_constraint("unique_project_handle_per_org", type_="unique")
    op.create_index(
        "unique_project_handle_per_org",
        "project",
        ["org_id", "handle"],
        unique=True,
        postgresql_where=_LIVE,
        sqlite_where=_LIVE,
    )
    op.create_index(
        "uq_project_org_name",
        "project",
        ["org_id", sa.text("lower(trim(name))")],
        unique=True,
        postgresql_where=_LIVE,
        sqlite_where=_LIVE,
    )


def downgrade() -> None:
    op.drop_index("uq_project_org_name", table_name="project")
    op.drop_index("unique_project_handle_per_org", table_name="project")
    op.execute("DELETE FROM project WHERE deleted_at IS NOT NULL")
    with op.batch_alter_table("project") as batch:
        batch.drop_column("deleted_at")
        batch.create_unique_constraint("unique_project_handle_per_org", ["org_id", "handle"])
    op.create_index("uq_project_org_name", "project", ["org_id", sa.text("lower(trim(name))")], unique=True)
