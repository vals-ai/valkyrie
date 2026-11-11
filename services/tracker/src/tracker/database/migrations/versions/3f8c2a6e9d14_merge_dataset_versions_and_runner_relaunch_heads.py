"""Merge dataset versions and runner relaunch migration heads.

Revision ID: 3f8c2a6e9d14
Revises: d8f3a1c5e7b9, 96defe9ab703
"""

from typing import Sequence, Union

revision: str = "3f8c2a6e9d14"
down_revision: Union[str, Sequence[str], None] = ("d8f3a1c5e7b9", "96defe9ab703")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
