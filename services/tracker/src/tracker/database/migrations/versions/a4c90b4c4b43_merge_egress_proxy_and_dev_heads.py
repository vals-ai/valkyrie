"""Merge egress proxy and shared dev migration heads.

Revision ID: a4c90b4c4b43
Revises: d47ac1e890b2, f1e2d3c4b5a6
"""

revision: str = "a4c90b4c4b43"
down_revision: tuple[str, str] = ("d47ac1e890b2", "f1e2d3c4b5a6")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join migration histories without schema changes."""
    pass


def downgrade() -> None:
    """Split migration histories without schema changes."""
    pass
