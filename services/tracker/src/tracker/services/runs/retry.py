from dataclasses import dataclass, field
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from benchmark_service.client import BenchmarkServiceError
from sqlmodel import Session
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    Org,
    RetryMode,
    Task,
    TaskStatus,
)
from tracker.exceptions import TrackerServiceError
from tracker.services.runs.attempts import PriorAttempt, prior_attempts
from tracker.services.benchmarks import fetch_benchmark_row


@dataclass(frozen=True)
class RetryState:
    task_ids: tuple[str, ...]
    version: str
    attempts: tuple[PriorAttempt, ...] = field(default=(), compare=False)


def _retry_candidates(
    benchmark_row: Benchmark,
    session: Session,
    retry: bool,
    rerun_task_ids: list[str],
    org: Org,
    *,
    for_update: bool = False,
) -> tuple[list[Task], list[str]]:
    existing_rows = BenchmarkRepository(session).retry_candidates(
        benchmark_row, retry, rerun_task_ids, for_update=for_update
    )
    if benchmark_row.arguments.regraded_from_run_id is not None and not rerun_task_ids:
        existing_rows = [task for task in existing_rows if task.eval_resume_state is not None]
    existing_ids = {task.task_id for task in existing_rows}
    new_task_ids = [task_id for task_id in rerun_task_ids if task_id not in existing_ids]
    if benchmark_row.status == BenchmarkStatus.IN_PROGRESS and new_task_ids:
        raise TrackerServiceError(
            f"{', '.join(new_task_ids)} cannot be retried while run {benchmark_row.id} is in progress because they are not in ERROR or STOPPED status"
        )
    return existing_rows, new_task_ids


def regrade_rerun_error(
    benchmark_row: Benchmark,
    session: Session,
    retry: bool,
    rerun_task_ids: list[str],
    org: Org,
    retry_mode: RetryMode,
) -> str | None:
    if benchmark_row.arguments.regraded_from_run_id is None:
        return None
    if retry_mode == RetryMode.FROM_SCRATCH:
        return f"Run {benchmark_row.id} is a regrade run; retry_mode from_scratch would rerun the agent."
    rows, new_task_ids = _retry_candidates(benchmark_row, session, retry, rerun_task_ids, org)
    stateless = sorted([*new_task_ids, *(row.task_id for row in rows if row.eval_resume_state is None)])
    if stateless:
        return (
            f"Run {benchmark_row.id} is a regrade run with no saved evaluation state for {', '.join(stateless)}; "
            "only its regraded tasks can be retried."
        )
    return None


def prepare_retry_state(
    benchmark_row: Benchmark,
    session: Session,
    retry: bool,
    rerun_task_ids: list[str],
    org: Org,
    *,
    queued_recovery: bool = False,
    for_update: bool = False,
) -> RetryState:
    if queued_recovery:
        rows = BenchmarkRepository(session).list_tasks_by_start(
            org.id,
            benchmark_row.id,
            [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING],
            for_update=for_update,
        )
        new_task_ids: list[str] = []
        attempts: tuple[PriorAttempt, ...] = ()
    else:
        rows, new_task_ids = _retry_candidates(
            benchmark_row, session, retry, rerun_task_ids, org, for_update=for_update
        )
        attempts = prior_attempts(session, org, rows) if retry else ()
    version = json.dumps(
        {
            "dispatch": ExecutorDispatchRepository(session).latest_id(benchmark_row.id),
            "status": benchmark_row.status,
            "started_at": benchmark_row.started_at,
            "finished_at": benchmark_row.finished_at,
            "release": benchmark_row.current_execution_release_id,
            "name": benchmark_row.name,
            "destination": benchmark_row.custom_benchmark_service,
            "dataset": benchmark_row.arguments.dataset,
            "dataset_version": benchmark_row.arguments.dataset_version.model_dump()
            if benchmark_row.arguments.dataset_version
            else None,
            "queue_pool_id": benchmark_row.arguments.queue_pool_id,
            "properties": benchmark_row.arguments.model_dump(mode="json")["properties"],
            "tasks": [(row.id, row.task_id, row.status, row.started_at, row.eval_resume_state) for row in rows],
        },
        default=str,
        sort_keys=True,
    )
    return RetryState(tuple([row.task_id for row in rows] + new_task_ids), version, attempts)


def reset_to_in_progress_status(
    benchmark_row: Benchmark,
    session: Session,
    retry: bool,
    retry_mode: RetryMode,
    rerun_task_ids: list[str],
    org: Org,
    verified_task_ids: list[str],
) -> list[str]:
    try:
        repository = BenchmarkRepository(session)
        benchmark_row = fetch_benchmark_row(benchmark_row.id, session, org, for_update=True)
        existing_rows, new_task_ids = _retry_candidates(
            benchmark_row, session, retry, rerun_task_ids, org, for_update=True
        )
        if existing_rows or new_task_ids or benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
            benchmark_row.publication_phase_dispatch_id = None
            benchmark_row.publication_phase_token = None
            benchmark_row.publication_phase_fingerprint = None
            benchmark_row.publication_status = None

        if not existing_rows and not new_task_ids:
            if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
                repository.delete_final_evaluation(benchmark_row)
                benchmark_row.status = BenchmarkStatus.IN_PROGRESS
                benchmark_row.finished_at = None
                repository.save(benchmark_row)
            return []

        repository.delete_final_evaluation(benchmark_row)

        if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
            benchmark_row.status = BenchmarkStatus.IN_PROGRESS
            benchmark_row.error_message = None
        benchmark_row.finished_at = None
        repository.save(benchmark_row)

        for task in existing_rows:
            task.status = (
                TaskStatus.EVALUATING
                if retry_mode == RetryMode.AUTO and task.eval_resume_state is not None
                else TaskStatus.PENDING
            )
            if task.status == TaskStatus.PENDING:
                task.credited_wall_deadline_at = None
            retry_started_at = datetime.now(ZoneInfo("UTC"))
            comparable_retry_started_at = retry_started_at
            if task.started_at.tzinfo is None and retry_started_at.tzinfo is not None:
                comparable_retry_started_at = retry_started_at.replace(tzinfo=None)
            if comparable_retry_started_at <= task.started_at:
                retry_started_at = task.started_at + timedelta(microseconds=1)
            task.started_at = retry_started_at
            task.finished_at = None
            task.build_phase_dispatch_id = None
            task.build_phase_token = None
            task.evaluation_phase_dispatch_id = None
            task.evaluation_phase_token = None
            if retry_mode == RetryMode.FROM_SCRATCH:
                task.eval_resume_state = None
            repository.save(task)

        repository.add_tasks(org.id, benchmark_row.id, new_task_ids)

        return verified_task_ids
    except (TrackerServiceError, BenchmarkServiceError):
        raise
    except Exception as e:
        raise TrackerServiceError(f"Unexpected error resuming run {benchmark_row.id}: {str(e)}") from e
