from dataclasses import dataclass
from datetime import UTC, datetime

from sqlmodel import Session

from tracker.database.models import (
    BenchmarkStatus,
    ExecutorDispatchStatus,
    TaskStatus,
)
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.executor.release_control import ReleaseControlError, get_executor_admission

_ACTIVE_BENCHMARK_STATUSES = (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)
_ACTIVE_TASK_STATUSES = (
    TaskStatus.PENDING,
    TaskStatus.BUILDING,
    TaskStatus.IN_PROGRESS,
    TaskStatus.EVALUATING,
)


class MaintenanceOwnershipError(ReleaseControlError):
    pass


@dataclass(frozen=True)
class MaintenanceStopSummary:
    benchmarks: int
    tasks: int
    dispatches: int


def begin_maintenance(
    session: Session,
    *,
    target_sha: str,
) -> MaintenanceStopSummary:
    if not target_sha:
        raise ValueError("Maintenance target SHA is required")

    admission = get_executor_admission(session, for_update=True)
    if admission.maintenance_target_sha not in (None, target_sha):
        raise MaintenanceOwnershipError("Executor maintenance is owned by another deployment")

    admission.maintenance_target_sha = target_sha
    admission.updated_at = datetime.now(UTC)

    benchmark_repository = BenchmarkRepository(session)
    finished_at = datetime.now(UTC)
    benchmarks = benchmark_repository.lock_by_status(_ACTIVE_BENCHMARK_STATUSES)
    for benchmark in benchmarks:
        benchmark.status = BenchmarkStatus.STOPPED
        benchmark.finished_at = finished_at

    tasks = benchmark_repository.lock_all_tasks(_ACTIVE_TASK_STATUSES)
    for task in tasks:
        task.status = TaskStatus.STOPPED

    dispatches = ExecutorDispatchRepository(session).lock_active()
    for dispatch in dispatches:
        dispatch.status = ExecutorDispatchStatus.FAILED
        dispatch.finished_at = finished_at

    session.flush()
    return MaintenanceStopSummary(
        benchmarks=len(benchmarks),
        tasks=len(tasks),
        dispatches=len(dispatches),
    )


def finish_maintenance(
    session: Session,
    *,
    target_sha: str,
) -> None:
    admission = get_executor_admission(session, for_update=True)
    if admission.maintenance_target_sha is None:
        return
    if admission.maintenance_target_sha != target_sha:
        raise MaintenanceOwnershipError("Deployment does not own executor maintenance")

    admission.maintenance_target_sha = None
    admission.updated_at = datetime.now(UTC)
    session.flush()
