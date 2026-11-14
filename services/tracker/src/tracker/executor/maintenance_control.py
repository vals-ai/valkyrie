from dataclasses import dataclass
from datetime import UTC, datetime

from sqlmodel import Session

from executor_protocol import ExecutorDispatchStatus
from tracker.domain import BenchmarkStatus, TaskStatus
from tracker.executor.release_control import ReleaseControlError, get_executor_admission
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.remote_grader import attempt_id_for
from tracker.repositories.remote_grader import RemoteGraderRepository
from tracker.repositories.executor_release import ExecutorReleaseRepository


class MaintenanceOwnershipError(ReleaseControlError):
    pass


@dataclass(frozen=True)
class MaintenanceStopSummary:
    benchmarks: int
    tasks: int
    dispatches: int


def begin_maintenance(session: Session, *, target_sha: str) -> MaintenanceStopSummary:
    if not target_sha:
        raise ValueError("Maintenance target SHA is required")
    admission = get_executor_admission(session, for_update=True)
    if admission.maintenance_target_sha not in (None, target_sha):
        raise MaintenanceOwnershipError("Another executor maintenance owner holds admission")
    admission.maintenance_target_sha = target_sha
    admission.updated_at = _database_now(session)
    ExecutorReleaseRepository(session).save(admission)
    benchmark_repository = BenchmarkRepository(session)
    finished_at = _database_now(session)
    benchmarks = benchmark_repository.lock_by_status((BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING))
    for benchmark in benchmarks:
        benchmark.status = BenchmarkStatus.STOPPED
        benchmark.finished_at = finished_at
        benchmark_repository.save(benchmark)

    tasks = benchmark_repository.lock_all_tasks(
        (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
    )
    for task in tasks:
        task.status = TaskStatus.STOPPED
        RemoteGraderRepository(session).close_attempt(task.id, attempt_id_for(task))
        benchmark_repository.save(task)
    dispatches = ExecutorDispatchRepository(session).lock_active()
    for dispatch in dispatches:
        dispatch.status = ExecutorDispatchStatus.FAILED
        dispatch.finished_at = finished_at
        ExecutorDispatchRepository(session).save(dispatch)
    session.flush()
    return MaintenanceStopSummary(benchmarks=len(benchmarks), tasks=len(tasks), dispatches=len(dispatches))


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


def _database_now(session: Session) -> datetime:
    return ExecutorDispatchRepository(session).clock_timestamp()
