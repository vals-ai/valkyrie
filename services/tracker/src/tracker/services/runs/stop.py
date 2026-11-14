import asyncio
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
from botocore.config import Config
from sqlmodel import Session

from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.egress_policy import EgressPolicyRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.repositories.remote_grader import RemoteGraderRepository
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    Org,
    TaskStatus,
)
from tracker.executor.dispatch_control import (
    stop_unclaimed_dispatches,
    terminalize_active_dispatches,
)
from tracker.database.session import database_call
from tracker.exceptions import TrackerServiceError
from tracker.logging import get_logger
from tracker.services.benchmarks import fetch_benchmark_row
from tracker.services.tasks.execute import task_attempt_id

logger = get_logger(__name__)


def apply_stop_benchmark(
    benchmark_row: Benchmark,
    session: Session,
    force: bool,
    org: Org,
    task_ids: list[str] | None = None,
) -> list[str]:
    fetch_benchmark_row(benchmark_row.id, session, org, for_update=True)
    stopped_unclaimed, task_arns = (
        stop_unclaimed_dispatches(session, benchmark_row) if task_ids is None else (False, [])
    )

    stoppable_statuses = [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.EVALUATING]
    if force:
        stoppable_statuses.append(TaskStatus.IN_PROGRESS)

    repository = BenchmarkRepository(session)
    stopped = repository.stop_tasks(org.id, benchmark_row.id, stoppable_statuses, task_ids)
    RemoteGraderRepository(session).close_stopped_tasks(benchmark_row.id)

    if force:
        stopped_attempts = None
        if task_ids:
            stopped_attempts = [
                task_attempt_id(task.id, task.started_at)
                for task in repository.list_tasks(org.id, benchmark_row.id, task_ids=task_ids)
            ]
        EgressPolicyRepository(session).revoke_attempts(benchmark_row.id, stopped_attempts)
        active_tasks = repository.count_tasks(
            org.id,
            benchmark_row.id,
            [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING],
        )
        if active_tasks == 0:
            benchmark_row.status = BenchmarkStatus.STOPPED
            benchmark_row.finished_at = datetime.now(ZoneInfo("UTC"))
            task_arns.extend(terminalize_active_dispatches(session, benchmark_row.id))
            repository.save(benchmark_row)
    elif task_ids is None and (
        stopped > 0
        or stopped_unclaimed
        or (
            benchmark_row.status == BenchmarkStatus.IN_PROGRESS
            and repository.count_tasks(org.id, benchmark_row.id, [TaskStatus.IN_PROGRESS]) > 0
        )
    ):
        if stopped_unclaimed and not ExecutorDispatchRepository(session).has_active(benchmark_row.id):
            benchmark_row.status = BenchmarkStatus.STOPPED
            benchmark_row.finished_at = datetime.now(ZoneInfo("UTC"))
        else:
            benchmark_row.status = BenchmarkStatus.STOPPING
        repository.save(benchmark_row)
    return task_arns


def apply_shed_benchmark(benchmark_row: Benchmark, concurrency: int, session: Session, org: Org) -> list[str]:
    repository = BenchmarkRepository(session)
    active_count = repository.count_tasks(
        org.id, benchmark_row.id, [TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]
    )
    shed_tasks = repository.lock_newest_tasks(
        org.id, benchmark_row.id, [TaskStatus.BUILDING, TaskStatus.IN_PROGRESS], max(active_count - concurrency, 0)
    )

    for task in shed_tasks:
        task.status = TaskStatus.STOPPED
        task.active_native_output = None
        RemoteGraderRepository(session).close_attempt(task.id, task_attempt_id(task.id, task.started_at))
    EgressPolicyRepository(session).revoke_attempts(
        benchmark_row.id, [task_attempt_id(task.id, task.started_at) for task in shed_tasks]
    )
    benchmark_row.arguments = benchmark_row.arguments.model_copy(update={"concurrency": concurrency})
    repository.save(benchmark_row)
    return [task.task_id for task in shed_tasks]


def commit_stop_benchmark(
    benchmark_row: Benchmark,
    session: Session,
    force: bool,
    org: Org,
    task_ids: list[str] | None = None,
) -> list[str]:
    try:
        task_arns = apply_stop_benchmark(benchmark_row, session, force, org, task_ids)
        session.commit()
        return task_arns
    except Exception as e:
        raise TrackerServiceError(f"Unexpected error stopping run {benchmark_row.id}: {str(e)}") from e


async def stop_executor_tasks(task_arns: list[str]) -> None:
    if task_arns:
        ecs = boto3.client("ecs", config=Config(connect_timeout=3, read_timeout=8, retries={"max_attempts": 1}))
        for task_arn in task_arns:
            try:
                await asyncio.to_thread(
                    ecs.stop_task,
                    cluster=os.environ["EXECUTOR_RUNNER_CLUSTER"],
                    task=task_arn,
                    reason="Benchmark stopped before runner claim",
                )
            except Exception:
                logger.exception("Failed to stop unclaimed executor task %s", task_arn)


async def initiate_stop_benchmark(
    benchmark_row: Benchmark,
    session: Session,
    force: bool,
    org: Org,
    task_ids: list[str] | None = None,
) -> None:
    task_arns = await database_call(lambda: commit_stop_benchmark(benchmark_row, session, force, org, task_ids))
    await stop_executor_tasks(task_arns)
