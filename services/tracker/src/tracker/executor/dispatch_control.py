from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlmodel import Session

from executor_protocol import (
    DATASET_VERSION_PROTOCOL_VERSION,
    EXACT_AGENT_PROTOCOL_VERSION,
    DEFAULT_EXECUTOR_DISPATCH_CLAIM_TIMEOUT_SECONDS,
    ExecutorDispatchStatus,
    MANAGED_EXECUTION_PROTOCOL_VERSION,
    RESERVED_QUEUE_PROTOCOL_VERSION,
    SHARED_POOL_QUEUE_PROTOCOL_VERSION,
)
from tracker.database.models import (
    Benchmark,
    ErrorResult,
    ExecutorDispatch,
    ExecutorRelease,
    Task,
)
from tracker.domain import BenchmarkStatus, ExecutorDispatchKind, FailureCategory, TaskStatus
from tracker.exceptions import TrackerServiceError
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.executor.release_control import (
    ReleaseControlError,
    create_executor_dispatch,
    lock_executor_admission,
    pin_benchmark_to_release,
    resolve_current_execution_release,
    select_active_release,
)
from tracker.executor.control_queue import is_reserved_queue_pool_id, reserved_queue_pool_id

DISPATCH_CLAIM_WINDOW = timedelta(seconds=DEFAULT_EXECUTOR_DISPATCH_CLAIM_TIMEOUT_SECONDS)
_ACTIVE_DISPATCH_STATUSES = (
    ExecutorDispatchStatus.QUEUED,
    ExecutorDispatchStatus.RUNNING,
)


class EnqueueFailureResolution(str, Enum):
    FAILED = "FAILED"
    DELIVERED = "DELIVERED"
    SUPERSEDED = "SUPERSEDED"


def _require_managed_execution_release(release: ExecutorRelease) -> None:
    if release.protocol_version not in {
        MANAGED_EXECUTION_PROTOCOL_VERSION,
        DATASET_VERSION_PROTOCOL_VERSION,
        RESERVED_QUEUE_PROTOCOL_VERSION,
        SHARED_POOL_QUEUE_PROTOCOL_VERSION,
        EXACT_AGENT_PROTOCOL_VERSION,
    }:
        raise ReleaseControlError("Activate an executor release that supports managed runs")


def _require_shared_pool_release(release: ExecutorRelease) -> None:
    if release.protocol_version not in {SHARED_POOL_QUEUE_PROTOCOL_VERSION, EXACT_AGENT_PROTOCOL_VERSION}:
        raise ReleaseControlError("Activate an executor release that supports shared pool admission")


def _require_compatible_release(benchmark: Benchmark, release: ExecutorRelease) -> None:
    if (
        benchmark.arguments.contract.agent_release is not None
        and release.protocol_version != EXACT_AGENT_PROTOCOL_VERSION
    ):
        raise ReleaseControlError("Activate an executor release that supports exact agent selections")
    if benchmark.arguments.dataset_version is not None and release.protocol_version not in {
        DATASET_VERSION_PROTOCOL_VERSION,
        RESERVED_QUEUE_PROTOCOL_VERSION,
        SHARED_POOL_QUEUE_PROTOCOL_VERSION,
        EXACT_AGENT_PROTOCOL_VERSION,
    }:
        raise ReleaseControlError("Activate an executor release that supports pinned dataset versions")
    if benchmark.arguments.environment == "aws":
        _require_managed_execution_release(release)
    queue_pool_id = benchmark.arguments.queue_pool_id
    if queue_pool_id is not None:
        _require_shared_pool_release(release)


def validate_managed_execution_release(session: Session) -> None:
    _require_managed_execution_release(select_active_release(session))


def admit_start_dispatch(
    session: Session,
    *,
    benchmark: Benchmark,
    dispatch_id: UUID,
    task_ids: list[str] | None = None,
) -> ExecutorDispatch:
    with session.no_autoflush:
        release = select_active_release(session, for_update=True)
    _require_compatible_release(benchmark, release)
    queue_pool_id = benchmark.arguments.queue_pool_id
    if queue_pool_id is not None:
        benchmark.arguments = benchmark.arguments.model_copy(
            update={"queue_pool_id": reserved_queue_pool_id(queue_pool_id)}
        )
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(
        benchmark.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=dispatch_id,
        task_ids=task_ids,
    )
    ExecutorDispatchRepository(session).save(benchmark, dispatch)
    session.flush()
    return dispatch


def admit_recovery_dispatch(
    session: Session,
    *,
    benchmark: Benchmark,
    pre_action_status: BenchmarkStatus,
    dispatch_id: UUID,
    kind: ExecutorDispatchKind,
    task_ids: list[str] | None = None,
) -> ExecutorDispatch:
    with session.no_autoflush:
        lock_executor_admission(session)
        if pre_action_status == BenchmarkStatus.IN_PROGRESS:
            release = resolve_current_execution_release(session, benchmark, for_update=True)
        else:
            release = select_active_release(session, for_update=True)
    _require_compatible_release(benchmark, release)
    stored_pool_id = benchmark.arguments.queue_pool_id
    if stored_pool_id is not None and not is_reserved_queue_pool_id(stored_pool_id):
        raise ReleaseControlError("Legacy queued runs cannot use shared pool admission; start a new queued run")
    if pre_action_status != BenchmarkStatus.IN_PROGRESS:
        benchmark.current_execution_release_id = release.id
        benchmark.finished_at = None
        terminalize_active_dispatches(session, benchmark.id)

    dispatch = create_executor_dispatch(
        benchmark.id,
        release,
        kind,
        dispatch_id=dispatch_id,
        task_ids=task_ids,
    )
    ExecutorDispatchRepository(session).save(benchmark, dispatch)
    session.flush()
    return dispatch


def terminalize_active_dispatches(
    session: Session,
    benchmark_id: UUID,
    *,
    except_dispatch_id: UUID | None = None,
) -> list[str]:
    return ExecutorDispatchRepository(session).terminalize_active(
        benchmark_id, datetime.now(ZoneInfo("UTC")), except_dispatch_id=except_dispatch_id
    )


def dispatch_now(session: Session) -> datetime:
    return ExecutorDispatchRepository(session).now()


@dataclass(frozen=True)
class _DispatchError:
    error_message: str
    producer: str
    operation: str
    error_type: str
    category: FailureCategory
    cause_code: str | None = None


def stop_unclaimed_dispatches(session: Session, benchmark: Benchmark) -> tuple[bool, list[str]]:
    repository = ExecutorDispatchRepository(session)
    dispatches = repository.lock_queued(benchmark.id)
    arns = []
    for dispatch in dispatches:
        dispatch.status = ExecutorDispatchStatus.FAILED
        dispatch.finished_at = datetime.now(ZoneInfo("UTC"))
        dispatch.failure_reason = "STOPPED_BEFORE_CLAIM"
        repository.delete_payload(dispatch.id)
        if dispatch.ecs_task_arn is not None:
            arns.append(dispatch.ecs_task_arn)
    return bool(dispatches), arns


def _as_utc_naive(value: datetime) -> datetime:
    return value if value.tzinfo is None else value.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def _lists(dispatch: ExecutorDispatch, task: Task) -> bool:
    return dispatch.assigned_task_ids is None or task.task_id in dispatch.assigned_task_ids


def _runs_attempt(dispatch: ExecutorDispatch, task: Task) -> bool:
    if dispatch.assigned_task_ids is None:
        return True
    return task.task_id in dispatch.assigned_task_ids and _as_utc_naive(dispatch.created_at) >= _as_utc_naive(
        task.started_at
    )


def unowned_pending_task_ids(session: Session, benchmark: Benchmark) -> list[str]:
    if benchmark.status != BenchmarkStatus.IN_PROGRESS:
        return []
    pending = BenchmarkRepository(session).pending_tasks(benchmark)
    if not pending:
        return []
    dispatches = ExecutorDispatchRepository(session).list_for_benchmark(benchmark.id)
    if not dispatches:
        return []
    unowned: list[str] = []
    for task in pending:
        runners = [dispatch for dispatch in dispatches if _runs_attempt(dispatch, task)]
        owners = runners or [dispatch for dispatch in dispatches if _lists(dispatch, task)]
        if not any(owner.status in _ACTIVE_DISPATCH_STATUSES for owner in owners):
            unowned.append(task.task_id)
    return unowned


def _terminalize_dispatch_tasks(
    session: Session,
    *,
    benchmark: Benchmark,
    dispatch: ExecutorDispatch,
    task_ids: list[str],
    error: _DispatchError,
    finished_at: datetime,
) -> bool:
    siblings = ExecutorDispatchRepository(session).list_active_siblings(benchmark.id, dispatch.id)
    created_at = _as_utc_naive(dispatch.created_at)
    repository = BenchmarkRepository(session)
    for task in repository.lock_runnable_tasks(benchmark, task_ids, dispatch.created_at):
        owned_evaluation = task.status == TaskStatus.EVALUATING and _as_utc_naive(task.started_at) == created_at
        if not owned_evaluation and any(_runs_attempt(sibling, task) for sibling in siblings):
            continue
        repository.save(ErrorResult(org_id=task.org_id, task=task.id, **asdict(error)))
        task.status = TaskStatus.ERROR
        task.finished_at = finished_at
    return bool(siblings)


def record_dispatch_failure(
    session: Session,
    *,
    benchmark_id: UUID,
    dispatch_id: UUID,
    task_ids: list[str],
    error_message: str,
    producer: str,
    operation: str,
    error_type: str,
    category: FailureCategory,
    cause_code: str | None = None,
    failure_reason: str | None = None,
    dispatch_status: ExecutorDispatchStatus = ExecutorDispatchStatus.RUNNING,
    expired_before: datetime | None = None,
    ecs_task_arn: str | None = None,
    claim_expired_before: datetime | None = None,
) -> bool:
    benchmark = BenchmarkRepository(session).lock_by_id(benchmark_id)
    dispatch = ExecutorDispatchRepository(session).lock(
        dispatch_id,
        benchmark_id=benchmark_id,
        status=dispatch_status,
        expired_before=expired_before,
        claim_expired_before=claim_expired_before,
        ecs_task_arn=ecs_task_arn,
    )
    if dispatch is None:
        return False

    now = datetime.now(ZoneInfo("UTC"))
    sibling_active = _terminalize_dispatch_tasks(
        session,
        benchmark=benchmark,
        dispatch=dispatch,
        task_ids=task_ids,
        error=_DispatchError(error_message, producer, operation, error_type, category, cause_code),
        finished_at=now,
    )
    dispatch.status = ExecutorDispatchStatus.FAILED
    dispatch.finished_at = now
    if failure_reason is not None:
        dispatch.failure_reason = failure_reason
    if not sibling_active:
        if benchmark.status == BenchmarkStatus.IN_PROGRESS:
            benchmark.status = BenchmarkStatus.ERROR
            benchmark.finished_at = now
            benchmark.error_message = error_message
        elif benchmark.status == BenchmarkStatus.STOPPING:
            benchmark.status = BenchmarkStatus.STOPPED
            benchmark.finished_at = now
    return True


def resolve_enqueue_failure(
    session: Session,
    *,
    benchmark_id: UUID,
    dispatch_id: UUID,
    task_ids: list[str],
) -> EnqueueFailureResolution:
    benchmark = BenchmarkRepository(session).lock_by_id(benchmark_id)
    dispatch = ExecutorDispatchRepository(session).lock(dispatch_id, benchmark_id=benchmark_id)
    if dispatch is None:
        raise TrackerServiceError(f"Executor dispatch {dispatch_id} not found")

    if dispatch.status != ExecutorDispatchStatus.QUEUED:
        session.rollback()
        if dispatch.started_at is not None or dispatch.status == ExecutorDispatchStatus.FINISHED:
            return EnqueueFailureResolution.DELIVERED
        return EnqueueFailureResolution.SUPERSEDED

    now = datetime.now(ZoneInfo("UTC"))
    error = _DispatchError(
        "Executor dispatch enqueue failed",
        "executor_dispatch",
        "enqueue",
        "ExecutorDispatchEnqueueError",
        FailureCategory.INFRASTRUCTURE,
    )
    dispatch.status = ExecutorDispatchStatus.FAILED
    dispatch.finished_at = now
    sibling_active = _terminalize_dispatch_tasks(
        session, benchmark=benchmark, dispatch=dispatch, task_ids=task_ids, error=error, finished_at=now
    )
    if benchmark.status == BenchmarkStatus.IN_PROGRESS and not sibling_active:
        benchmark.status = BenchmarkStatus.ERROR
        benchmark.finished_at = now
        benchmark.error_message = error.error_message
    session.commit()
    return EnqueueFailureResolution.FAILED
