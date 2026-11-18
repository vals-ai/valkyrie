"""Drop Tracker's project tables now that the project service owns every project row.

Revision ID: f1d3b5a7c9e2
Revises: d3e4f5a6b7c8
"""

import logging
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f1d3b5a7c9e2"
down_revision: str | Sequence[str] | None = "d3e4f5a6b7c8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ENUM_NAME = "spendtype"
_COUNTS = sa.text(
    "SELECT (SELECT count(*) FROM project), (SELECT count(*) FROM project_backfill), "
    "(SELECT count(*) FROM project_backfill WHERE confirmed), (SELECT count(*) FROM project_cutover)"
)


def upgrade() -> None:
    projects, baselines, confirmed, cutovers = op.get_bind().execute(_COUNTS).one()
    logging.getLogger("alembic.runtime.migration").info(
        "Dropping project tables: project=%s project_backfill=%s confirmed_baselines=%s project_cutover=%s",
        projects,
        baselines,
        confirmed,
        cutovers,
    )
    op.drop_table("project_cutover")
    op.drop_table("project_backfill")
    op.drop_table("project")
    sa.Enum(name=_ENUM_NAME).drop(op.get_bind())


def downgrade() -> None:
    op.create_table(
        "project",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("handle", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("spend_type", sa.Enum("DEVELOPMENT", "INTERNAL", "CUSTOMER", name=_ENUM_NAME), nullable=False),
        sa.Column("subcategory", sa.String(), nullable=True),
        sa.Column("customer", sa.String(), nullable=True),
        sa.Column("extra_tags", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by_email", sa.String(), nullable=True),
        sa.Column("gateway_request_id", sa.Uuid(), nullable=True),
        sa.Column("gateway_key_id", sa.Uuid(), nullable=True),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "handle", name="unique_project_handle_per_org"),
    )
    op.create_index("uq_project_org_name", "project", ["org_id", sa.text("lower(btrim(name))")], unique=True)
    op.create_table(
        "project_backfill",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("source_hash", sa.String(length=64), nullable=False),
        sa.Column("confirmed", sa.Boolean(), nullable=False),
        sa.Column("copied_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("project_id"),
    )
    op.create_table(
        "project_cutover",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
