"""Merge beta dataset version and dev benchmark service wake migration heads.

Revision ID: 5b9d2e7f4a18
Revises: 3f8c2a6e9d14, e3a7c1d9b5f2
"""

revision: str = "5b9d2e7f4a18"
down_revision: tuple[str, str] = ("3f8c2a6e9d14", "e3a7c1d9b5f2")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join migration histories without schema changes."""
    pass


def downgrade() -> None:
    """Split migration histories without schema changes."""
    pass
