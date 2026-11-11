"""Merge beta dataset and dev usage_end backfill migration heads.

Revision ID: 125d7d985c7c
Revises: 5b9d2e7f4a18, 02b9543b65ba
"""

revision: str = "125d7d985c7c"
down_revision: tuple[str, str] = ("5b9d2e7f4a18", "02b9543b65ba")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join migration histories without schema changes."""
    pass


def downgrade() -> None:
    """Split migration histories without schema changes."""
    pass
