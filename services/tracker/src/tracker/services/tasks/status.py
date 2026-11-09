import time
from datetime import datetime
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo
from sqlmodel import Session
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.database.models import (
    ErrorResult,
    FailureCategory,
    FailurePhase,
    Org,
    Task,
    TaskStatus,
)
from tracker.exceptions import (
    ExecutionAuthorityRevoked,
    TrackerServiceError,
)
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.observability import elapsed_ms
from tracker.observability.tracing import observability_span
from tracker.services.benchmarks import fetch_task_row


def normalized_attempt_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def exception_message(exc: BaseException) -> str:

    def describe(error: BaseException) -> str:
        detail = str(error).strip()
        name = type(error).__name__
        return f"{name}: {detail}" if detail else name

    message = describe(exc)
    seen = {id(exc)}
    current = exc
    while True:
        if isinstance(current, TrackerServiceError) and not current.include_cause_in_task_error:
            break
        if current.__cause__ is not None:
            next_error = current.__cause__
            relation = "caused by"
        elif not current.__suppress_context__:
            next_error = current.__context__
            relation = "during handling of"
        else:
            break
        if next_error is None or id(next_error) in seen:
            break
        seen.add(id(next_error))
        message = f"{message} ({relation} {describe(next_error)})"
        current = next_error
    return message


def handle_early_exit(task_row: Task, task_session: Session, authority: ExecutionAuthority) -> bool:
    return _commit_task_status(
        task_row,
        task_session,
        TaskStatus.STOPPED,
        authority=authority,
        expected_started_at=task_row.started_at,
    )


def _commit_task_status(
    task: Task,
    session: Session,
    to_status: TaskStatus,
    *,
    authority: ExecutionAuthority,
    error_message: str | None = None,
    extra: dict[str, Any] | None = None,
    expected_started_at: datetime | None = None,
    expected_status: TaskStatus | tuple[TaskStatus, ...] | None = None,
) -> bool:
    from_status = task.status
    span_attributes = {
        "benchmark_id": str(task.benchmark),
        "task_id": task.task_id,
        "from_status": from_status.value,
        "to_status": to_status.value,
        **(extra or {}),
    }
    if error_message is not None:
        span_attributes["has_error_message"] = True

    with observability_span("task.status_transition", **span_attributes):
        try:
            lock_execution_authority(session, authority)
        except ExecutionAuthorityRevoked:
            session.rollback()
            return False
        values: dict[str, TaskStatus | datetime] = {"status": to_status}
        if to_status in [TaskStatus.FINISHED, TaskStatus.ERROR]:
            values["finished_at"] = datetime.now(ZoneInfo("UTC"))

        if not BenchmarkRepository(session).update_task(
            task.id, task.org_id, values, expected_started_at=expected_started_at, expected_status=expected_status
        ):
            session.rollback()
            return False

        session.commit()
        return True


def commit_task_status_transition(
    task_row_id: UUID,
    session: Session,
    org: Org,
    to_status: TaskStatus,
    *,
    authority: ExecutionAuthority,
    expected_started_at: datetime | None = None,
    expected_status: TaskStatus | tuple[TaskStatus, ...] | None = None,
) -> bool:
    fetch_start = time.monotonic()
    task = fetch_task_row(task_row_id, session, org)
    return _commit_task_status(
        task,
        session,
        to_status,
        extra={"fetch_duration_ms": elapsed_ms(fetch_start)},
        expected_started_at=expected_started_at,
        expected_status=expected_status,
        authority=authority,
    )


def commit_task_error(
    task_row: Task,
    session: Session,
    error_message: str,
    *,
    producer: str,
    operation: str,
    error_type: str,
    category: FailureCategory,
    authority: ExecutionAuthority,
    cause_code: str | None = None,
    phase: FailurePhase | None = None,
    transient: bool | None = None,
    expected_started_at: datetime | None = None,
    expected_status: TaskStatus | None = None,
) -> bool:
    BenchmarkRepository(session).save(
        ErrorResult(
            org_id=task_row.org_id,
            task=task_row.id,
            error_message=error_message,
            producer=producer,
            operation=operation,
            error_type=error_type,
            cause_code=cause_code,
            category=category,
            phase=phase,
            transient=transient,
        )
    )
    return _commit_task_status(
        task_row,
        session,
        TaskStatus.ERROR,
        error_message=error_message,
        expected_started_at=expected_started_at,
        expected_status=expected_status,
        authority=authority,
    )
