"""Merge run argument column and grading sandbox reservation migration heads.

Revision ID: 83a507dd5eaf
Revises: e5f1a2b3c4d6, fa005e34c3d8
"""

revision: str = "83a507dd5eaf"
down_revision: tuple[str, str] = ("e5f1a2b3c4d6", "fa005e34c3d8")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join migration histories without schema changes."""
    pass


def downgrade() -> None:
    """Split migration histories without schema changes."""
    pass
