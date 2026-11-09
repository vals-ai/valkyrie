import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID
from sqlmodel import Session
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    DocentReadingStatus,
    FailureCategory,
    Org,
    TaskStatus,
)
from tracker.database.session import engine
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.executor.dispatch_control import record_dispatch_failure, terminalize_active_dispatches
from tracker.exceptions import ExecutionAuthorityRevoked, TrackerServiceError
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.observability.tracing import observability_span
from tracker.services.benchmarks import (
    fetch_benchmark_row,
)
from tracker.services.tasks.error_summary import summarize_task_errors


RUNNABLE_TASK_STATUSES = [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]


TERMINAL_BENCHMARK_STATUSES = (BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED)


TaskFingerprint = tuple[tuple[UUID, datetime, TaskStatus, UUID | None], ...]


def _record_run_finalized(benchmark_id: UUID, status: BenchmarkStatus, authority: ExecutionAuthority) -> None:
    with observability_span(
        "run.finalized",
        benchmark_id=str(benchmark_id),
        status=status.value,
        executor_dispatch_id=str(authority.dispatch_id),
    ):
        pass


def set_benchmark_final_status(
    benchmark_row: Benchmark,
    session: Session,
    org: Org,
    *,
    authority: ExecutionAuthority,
) -> None:

    lock_execution_authority(session, authority)

    repository = BenchmarkRepository(session)
    tasks_not_finished = repository.count_tasks(org.id, benchmark_row.id, RUNNABLE_TASK_STATUSES)

    if tasks_not_finished:
        raise TrackerServiceError(
            f"Cannot set final status for run {benchmark_row.id} because tasks are still runnable."
        )

    tasks_stopped = repository.count_tasks(org.id, benchmark_row.id, [TaskStatus.STOPPED])

    benchmark_status = BenchmarkStatus.FINISHED
    if tasks_stopped:
        benchmark_status = BenchmarkStatus.STOPPED

    terminalize_active_dispatches(
        session,
        benchmark_row.id,
        except_dispatch_id=authority.dispatch_id if benchmark_status == BenchmarkStatus.FINISHED else None,
    )
    benchmark_row.status = benchmark_status
    benchmark_row.finished_at = datetime.now(UTC)
    benchmark_row.error_message = None
    repository.save(benchmark_row)
    session.commit()
    _record_run_finalized(benchmark_row.id, benchmark_status, authority)


def fetch_final_score_state(
    session: Session,
    benchmark_row: Benchmark,
    org: Org,
    *,
    for_update: bool = False,
) -> tuple[dict[str, dict[str, Any] | None], TaskFingerprint]:
    repository = BenchmarkRepository(session)
    task_rows = repository.task_attempts(org.id, benchmark_row.id, for_update=for_update)
    latest_results: dict[UUID, tuple[UUID, dict[str, Any]]] = {}
    for task_row_id, result_id, result in repository.latest_evaluation_results(org.id, benchmark_row.id):
        latest_results.setdefault(task_row_id, (result_id, result))

    inputs = {
        task_id: latest_results[task_row_id][1]
        if status == TaskStatus.FINISHED and task_row_id in latest_results
        else None
        for task_row_id, task_id, _started_at, status in task_rows
    }
    fingerprint = tuple(
        (
            task_row_id,
            started_at,
            status,
            latest_results[task_row_id][0] if task_row_id in latest_results else None,
        )
        for task_row_id, _task_id, started_at, status in task_rows
    )
    return inputs, fingerprint


def fetch_final_score_inputs(session: Session, benchmark_row: Benchmark, org: Org) -> dict[str, dict[str, Any] | None]:
    return fetch_final_score_state(session, benchmark_row, org)[0]


async def finalize_all_error_run(
    benchmark_id: UUID,
    org: Org,
    *,
    authority: ExecutionAuthority,
) -> bool:
    with Session(bind=engine) as session:
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        if benchmark_row.status in TERMINAL_BENCHMARK_STATUSES:
            return True
        lock_execution_authority(session, authority)
        if BenchmarkRepository(session).has_tasks(org.id, benchmark_row.id, RUNNABLE_TASK_STATUSES):
            return True
        terminalize_active_dispatches(
            session,
            benchmark_id,
            except_dispatch_id=authority.dispatch_id,
        )
        if BenchmarkRepository(session).has_tasks(org.id, benchmark_row.id, [TaskStatus.STOPPED]):
            set_benchmark_final_status(benchmark_row, session, org, authority=authority)
            return False
        _evaluation_results, task_fingerprint = fetch_final_score_state(
            session,
            benchmark_row,
            org,
            for_update=True,
        )
        task_errors = {
            task_id: message
            for task_id, (message, _category) in BenchmarkRepository(session).task_failures(benchmark_row).items()
        }
        session.commit()

    error_message = await asyncio.to_thread(summarize_task_errors, task_errors)

    with Session(bind=engine) as session:
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
        if benchmark_row.status in TERMINAL_BENCHMARK_STATUSES:
            return True
        if BenchmarkRepository(session).has_tasks(org.id, benchmark_row.id, RUNNABLE_TASK_STATUSES):
            return True
        if BenchmarkRepository(session).has_tasks(org.id, benchmark_row.id, [TaskStatus.STOPPED]):
            set_benchmark_final_status(benchmark_row, session, org, authority=authority)
            return False
        _evaluation_results, current_fingerprint = fetch_final_score_state(
            session,
            benchmark_row,
            org,
            for_update=True,
        )
        if current_fingerprint != task_fingerprint:
            return True

        commit_benchmark_error(
            benchmark_row,
            session,
            error_message,
            producer="tracker",
            operation="summarize_task_errors",
            error_type="AllTasksFailed",
            category=FailureCategory.UNKNOWN,
            authority=authority,
        )
        return False


def commit_benchmark_error(
    benchmark_row: Benchmark,
    session: Session,
    error_message: str,
    *,
    producer: str,
    operation: str,
    error_type: str,
    category: FailureCategory,
    authority: ExecutionAuthority,
    cause_code: str | None = None,
    task_ids: list[str] | None = None,
) -> bool:
    try:
        benchmark_row = lock_execution_authority(session, authority)
    except ExecutionAuthorityRevoked:
        session.rollback()
        return False
    committed = record_dispatch_failure(
        session,
        benchmark_id=benchmark_row.id,
        dispatch_id=authority.dispatch_id,
        task_ids=task_ids or [],
        error_message=error_message,
        producer=producer,
        operation=operation,
        error_type=error_type,
        cause_code=cause_code,
        category=category,
    )
    session.commit()
    if committed and benchmark_row.status == BenchmarkStatus.ERROR:
        _record_run_finalized(benchmark_row.id, BenchmarkStatus.ERROR, authority)
    return committed


def catch_errors_during_cleanup(
    benchmark_id: UUID,
    session: Session,
    org: Org,
    *,
    authority: ExecutionAuthority,
    task_ids: list[str] | None = None,
) -> None:
    terminal_statuses = [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED]

    benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
    try:
        benchmark_row = lock_execution_authority(
            session,
            authority,
            require_in_progress=False,
        )
    except ExecutionAuthorityRevoked:
        session.rollback()
        return
    if benchmark_row.status in terminal_statuses:
        return

    task_terminal_statuses = [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED]
    repository = BenchmarkRepository(session)
    undetected_exit_tasks = repository.list_tasks(
        org.id,
        benchmark_id,
        task_ids=task_ids,
        statuses=[status for status in TaskStatus if status not in task_terminal_statuses],
    )

    if benchmark_row.docent_reading_status == DocentReadingStatus.RUNNING:
        benchmark_row.docent_reading_status = DocentReadingStatus.ERROR
        repository.save(benchmark_row)

    error_message = f"Run {benchmark_id} exited without finishing"
    committed = record_dispatch_failure(
        session,
        benchmark_id=benchmark_row.id,
        dispatch_id=authority.dispatch_id,
        task_ids=[task.task_id for task in undetected_exit_tasks],
        error_message=error_message,
        producer="tracker",
        operation="cleanup",
        error_type="UndetectedExecutorExit",
        category=FailureCategory.INFRASTRUCTURE,
    )
    session.commit()
    if committed and benchmark_row.status == BenchmarkStatus.ERROR:
        _record_run_finalized(benchmark_id, BenchmarkStatus.ERROR, authority)
