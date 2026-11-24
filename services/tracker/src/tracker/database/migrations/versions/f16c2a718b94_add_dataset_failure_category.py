from collections.abc import Sequence

from alembic import op

revision: str = "f16c2a718b94"
down_revision: str | Sequence[str] | None = "e5b9d2f8a4c3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TYPE failurecategory ADD VALUE 'DATASET'")


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '1s'")
    op.execute("UPDATE errorresult SET category = 'BENCHMARK_SERVICE' WHERE category = 'DATASET'")
    op.execute("ALTER TYPE failurecategory RENAME TO failurecategory_with_dataset")
    op.execute(
        "CREATE TYPE failurecategory AS ENUM ('INFRASTRUCTURE', 'BENCHMARK_SERVICE', 'AGENT', 'CANCELLED', 'UNKNOWN')"
    )
    op.execute(
        "ALTER TABLE errorresult ALTER COLUMN category TYPE failurecategory USING category::text::failurecategory"
    )
    op.execute("DROP TYPE failurecategory_with_dataset")
