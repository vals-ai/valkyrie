from uuid import UUID

from sqlmodel import Session

from tracker.database.models import ActiveNativeOutput, TaskStatus
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority


def publish_native_output(
    session: Session,
    task_id: UUID,
    descriptor: ActiveNativeOutput,
    authority: ExecutionAuthority,
) -> None:
    benchmark = lock_execution_authority(session, authority)
    task = BenchmarkRepository(session).lock_task(
        benchmark.org_id,
        authority.benchmark_id,
        task_id,
        started_at=descriptor.attempt_started_at,
        statuses=(TaskStatus.BUILDING, TaskStatus.IN_PROGRESS),
    )
    if task is None:
        raise ExecutionAuthorityRevoked("Task attempt no longer owns native output")
    task.active_native_output = descriptor
    session.commit()


def clear_native_output(session: Session, task_id: UUID, descriptor: ActiveNativeOutput) -> None:
    BenchmarkRepository(session).clear_native_output(
        task_id, descriptor.sandbox_id, descriptor.model_dump(mode="json")["attempt_started_at"]
    )
    session.commit()
