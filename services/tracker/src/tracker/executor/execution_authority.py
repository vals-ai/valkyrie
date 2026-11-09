from dataclasses import dataclass
from uuid import UUID

from sqlmodel import Session

from tracker.database.models import Benchmark, BenchmarkStatus, ExecutorDispatchStatus
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository


@dataclass(frozen=True)
class ExecutionAuthority:
    benchmark_id: UUID
    dispatch_id: UUID


def lock_execution_authority(
    session: Session,
    authority: ExecutionAuthority,
    *,
    require_in_progress: bool = True,
) -> Benchmark:
    benchmark = BenchmarkRepository(session).get_by_id(authority.benchmark_id, for_update=True)
    if (
        benchmark is None
        or benchmark.status == BenchmarkStatus.STOPPED
        or (require_in_progress and benchmark.status not in (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING))
    ):
        raise ExecutionAuthorityRevoked("Executor benchmark authority was revoked")

    dispatch = ExecutorDispatchRepository(session).lock(
        authority.dispatch_id, benchmark_id=authority.benchmark_id, status=ExecutorDispatchStatus.RUNNING
    )
    if dispatch is None:
        raise ExecutionAuthorityRevoked("Executor dispatch authority was revoked")
    return benchmark
