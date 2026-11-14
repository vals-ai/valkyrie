"""Link runs to project IDs without a foreign key to Tracker's project table.

Revision ID: e4b8a1c7d2f9
Revises: 83a507dd5eaf
"""

from collections.abc import Sequence

from alembic import op

revision: str = "e4b8a1c7d2f9"
down_revision: str | Sequence[str] | None = "83a507dd5eaf"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint("run_project_project_id_fkey", "run_project", type_="foreignkey")


def downgrade() -> None:
    op.create_foreign_key(
        "run_project_project_id_fkey", "run_project", "project", ["project_id"], ["id"], postgresql_not_valid=True
    )
