from collections.abc import Sequence

revision: str = "a7d2c4e9f1b3"
down_revision: str | Sequence[str] | None = ("cd95348d4f10", "f3c1d9a7b2e4")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
