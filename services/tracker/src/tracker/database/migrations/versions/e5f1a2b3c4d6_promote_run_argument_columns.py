"""Store run argument fields used for filtering and pool lookups as generated columns.

Revision ID: e5f1a2b3c4d6
Revises: d4e8a1c6b9f2
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e5f1a2b3c4d6"
down_revision: str | Sequence[str] | None = "d4e8a1c6b9f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = {
    "queue_pool_id": "arguments ->> 'queue_pool_id'",
    "agent_name": "(arguments -> 'contract') ->> 'name'",
    "agent_model": "(arguments -> 'contract') ->> 'model'",
    "dataset": "arguments ->> 'dataset'",
}
_INDEX_NAME = "ix_benchmark_queue_pool_id"


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '1s'")
    additions = ", ".join(
        f"ADD COLUMN IF NOT EXISTS {name} VARCHAR GENERATED ALWAYS AS ({expression}) STORED"
        for name, expression in _COLUMNS.items()
    )
    op.execute(sa.text(f"ALTER TABLE benchmark {additions}"))
    with op.get_context().autocommit_block():
        op.execute(sa.text(f'DROP INDEX CONCURRENTLY IF EXISTS "{_INDEX_NAME}"'))
        op.create_index(
            _INDEX_NAME,
            "benchmark",
            ["queue_pool_id"],
            unique=False,
            postgresql_where=sa.text("queue_pool_id IS NOT NULL"),
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f'DROP INDEX CONCURRENTLY IF EXISTS "{_INDEX_NAME}"'))
    op.execute(sa.text("SET LOCAL lock_timeout = '1s'"))
    op.execute(sa.text("ALTER TABLE benchmark " + ", ".join(f"DROP COLUMN IF EXISTS {name}" for name in _COLUMNS)))
