from alembic import op
import sqlalchemy as sa

revision = "d3e4f5a6b7c8"
down_revision = "c9e2f4a6b803"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "descopemembership",
        sa.Column("project_id", sa.String(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("tenant_roles", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cooldown_until", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("project_id", "user_id"),
    )


def downgrade() -> None:
    op.drop_table("descopemembership")
