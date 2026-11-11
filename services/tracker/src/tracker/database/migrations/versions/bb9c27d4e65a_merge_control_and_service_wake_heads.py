from collections.abc import Sequence

revision: str = "bb9c27d4e65a"
down_revision: str | Sequence[str] | None = ("9c4f0e2a7b61", "e3a7c1d9b5f2")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
