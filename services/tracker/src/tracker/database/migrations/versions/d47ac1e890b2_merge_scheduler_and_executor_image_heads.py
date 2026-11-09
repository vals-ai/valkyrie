"""Merge shared scheduler and executor image migration heads.

Revision ID: d47ac1e890b2
Revises: c82e1f7a9d03, 6c7d8e9f0a1b
"""

revision: str = "d47ac1e890b2"
down_revision: tuple[str, str] = ("c82e1f7a9d03", "6c7d8e9f0a1b")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join migration histories without schema changes."""
    pass


def downgrade() -> None:
    """Split migration histories without schema changes."""
    pass
