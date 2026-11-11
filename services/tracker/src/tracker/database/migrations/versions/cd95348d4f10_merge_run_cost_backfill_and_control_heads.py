from collections.abc import Sequence

revision: str = "cd95348d4f10"
down_revision: str | Sequence[str] | None = ("02b9543b65ba", "bb9c27d4e65a")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
