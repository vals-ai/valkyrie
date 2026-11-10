import asyncio
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from tracker.database.models import FailureCategory, Org, Task, TaskStatus
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.runtime.artifacts import ATTEMPT_RECORD, run_attempts_prefix
from tracker.runtime.storage import ObjectStore
from tracker.services.tasks.execute import task_attempt_id
from sqlmodel import Session

_ATTEMPT_RECORDS = 64


@dataclass(frozen=True)
class PriorAttempt:
    task_id: str
    attempt_id: str
    status: TaskStatus
    started_at: datetime
    finished_at: datetime | None
    error: str | None
    category: FailureCategory | None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def prior_attempts(session: Session, org: Org, rows: Sequence[Task]) -> tuple[PriorAttempt, ...]:
    started = {row.id: _utc(row.started_at) for row in rows}
    errors: dict[UUID, tuple[datetime, str | None, FailureCategory | None]] = {}
    for task_row_id, created_at, message, category in BenchmarkRepository(session).final_errors(org.id, list(started)):
        created_at = _utc(created_at)
        latest = errors.get(task_row_id)
        if created_at >= started[task_row_id] and (latest is None or created_at >= latest[0]):
            errors[task_row_id] = (created_at, message, category)
    return tuple(
        PriorAttempt(
            task_id=row.task_id,
            attempt_id=task_attempt_id(row.id, row.started_at),
            status=row.status,
            started_at=_utc(row.started_at),
            finished_at=_utc(row.finished_at) if row.finished_at is not None else None,
            error=errors[row.id][1] if row.id in errors else None,
            category=errors[row.id][2] if row.id in errors else None,
        )
        for row in rows
    )


async def record_prior_attempts(store: ObjectStore, benchmark_id: str, attempts: Sequence[PriorAttempt]) -> list[str]:
    replaced_at = datetime.now(UTC).isoformat()
    slots = asyncio.Semaphore(_ATTEMPT_RECORDS)

    async def record(attempt: PriorAttempt) -> str | None:
        content = {
            "task_id": attempt.task_id,
            "attempt_id": attempt.attempt_id,
            "status": attempt.status.value,
            "started_at": attempt.started_at.isoformat(),
            "finished_at": attempt.finished_at.isoformat() if attempt.finished_at is not None else None,
            "replaced_at": replaced_at,
            "error": attempt.error,
            "failure_category": attempt.category.value if attempt.category is not None else None,
        }
        key = f"{run_attempts_prefix(benchmark_id)}{attempt.task_id}/{attempt.attempt_id}/{ATTEMPT_RECORD}"
        async with slots:
            if await store.exists(key):
                return None
            await store.put_bytes(key, json.dumps(content, indent=2).encode())
            return key

    return [key for key in await asyncio.gather(*(record(attempt) for attempt in attempts)) if key is not None]
