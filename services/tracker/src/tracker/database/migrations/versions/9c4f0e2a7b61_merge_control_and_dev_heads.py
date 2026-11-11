from collections.abc import Sequence

revision: str = "9c4f0e2a7b61"
down_revision: str | Sequence[str] | None = ("3222f9e0a6ce", "8f2a6b9c1d30")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
