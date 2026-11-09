"""Make project names unique per organization, ignoring case and outer spaces; archived projects keep their names.

Revision ID: 4c9d2e7f1a3b
Revises: 3222f9e0a6ce
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from alembic.util import CommandError

revision: str = "4c9d2e7f1a3b"
down_revision: str | Sequence[str] | None = "3222f9e0a6ce"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_DUPLICATE_NAMES = sa.text(
    "SELECT org.name, lower(btrim(project.name)), string_agg(project.handle, ', ' ORDER BY project.handle) "
    "FROM project JOIN org ON org.id = project.org_id "
    "GROUP BY org.name, project.org_id, lower(btrim(project.name)) HAVING count(*) > 1 "
    "ORDER BY org.name, lower(btrim(project.name))"
)


def upgrade() -> None:
    duplicates = op.get_bind().execute(_DUPLICATE_NAMES).all()
    if duplicates:
        listing = "; ".join(f"org '{org}' name '{name}' held by {handles}" for org, name, handles in duplicates)
        raise CommandError(f"Rename these projects so each name is used once per organization, then rerun: {listing}")
    op.create_index("uq_project_org_name", "project", ["org_id", sa.text("lower(btrim(name))")], unique=True)


def downgrade() -> None:
    op.drop_index("uq_project_org_name", table_name="project")
