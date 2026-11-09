"""Merge benchmark service wake and sandbox cost cursor migration heads.

Revision ID: e3a7c1d9b5f2
Revises: 823369ae05b6, b7e4c2a9d1f3
"""

revision: str = "e3a7c1d9b5f2"
down_revision: tuple[str, str] = ("823369ae05b6", "b7e4c2a9d1f3")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join migration histories without schema changes."""
    pass


def downgrade() -> None:
    """Split migration histories without schema changes."""
    pass
