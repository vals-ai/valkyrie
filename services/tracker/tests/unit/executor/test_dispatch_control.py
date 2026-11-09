import base64
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from cryptography.exceptions import InvalidTag
from tracker.executor import entrypoint
from benchmark_service.schemas import DatasetVersion
from sqlmodel import Session, col, select

from executor_protocol import (
    DATASET_VERSION_PROTOCOL_VERSION,
    MANAGED_EXECUTION_PROTOCOL_VERSION,
    RESERVED_QUEUE_PROTOCOL_VERSION,
    SHARED_POOL_QUEUE_PROTOCOL_VERSION,
)
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchPayload,
    ExecutorDispatchStatus,
    ExecutorRelease,
    FailureCategory,
    Task,
    TaskStatus,
)
from tracker.executor.dispatch_control import (
    unowned_pending_task_ids,
    ClaimedDispatch,
    claim_dispatch,
    finish_dispatch,
    EnqueueFailureResolution,
    admit_recovery_dispatch,
    admit_start_dispatch,
    record_dispatch_failure,
    resolve_enqueue_failure,
    terminalize_active_dispatches,
)
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.dispatch_payload import generate_payload_key, seal_payload
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.executor.release_control import (
    ReleaseControlError,
    create_executor_dispatch,
    pin_benchmark_to_release,
    promote_release,
    register_release,
)
from tracker.scheduler.store import queue_pool_id, reserved_queue_pool_id

_LEGACY_QUEUE_POOL_ID = queue_pool_id("daytona:org-1")


def _fail_stopped_task(session: Session, dispatch: ExecutorDispatch) -> bool:
    before_claim = dispatch.status == ExecutorDispatchStatus.QUEUED
    return record_dispatch_failure(
        session,
        benchmark_id=dispatch.benchmark_id,
        dispatch_id=dispatch.id,
        task_ids=dispatch.assigned_task_ids or [],
        error_message="Executor task stopped",
        producer="executor_dispatch",
        operation="dispatch_reconciliation",
        error_type="ExecutorTaskStoppedBeforeClaim" if before_claim else "ExecutorTaskStopped",
        category=FailureCategory.INFRASTRUCTURE,
        cause_code="ECS_TASK_STOPPED",
        failure_reason="ECS_TASK_STOPPED",
        dispatch_status=dispatch.status,
    )


def _release(release_id: str, *, protocol_version: str = MANAGED_EXECUTION_PROTOCOL_VERSION) -> ExecutorRelease:
    return ExecutorRelease(
        id=release_id,
        artifact_uri=f"source:///artifacts/{release_id}.pex",
        artifact_digest="a" * 64,
        protocol_version=protocol_version,
        created_at=datetime.now(UTC),
    )


@pytest.mark.description("Start admission picks the active executor release.")
def test_start_admission_selects_active_release(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    register_release(database_session, _release("active"))
    promote_release(database_session, "active")
    database_session.commit()
    dispatch_id = uuid4()

    dispatch = admit_start_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        dispatch_id=dispatch_id,
    )
    database_session.commit()

    assert example_benchmark_object.current_execution_release_id == "active"
    assert dispatch.id == dispatch_id
    assert dispatch.executor_release_id == "active"


@pytest.mark.description("A managed start needs a compatible active release.")
@pytest.mark.parametrize("protocol_version", ["1", "2"])
def test_managed_start_requires_a_compatible_active_release(
    protocol_version: str,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    register_release(database_session, _release("legacy", protocol_version=protocol_version))
    promote_release(database_session, "legacy")
    database_session.commit()
    with pytest.raises(ReleaseControlError, match="supports managed runs"):
        admit_start_dispatch(
            database_session,
            benchmark=example_benchmark_object,
            dispatch_id=uuid4(),
        )


@pytest.mark.description("A managed start outside the queue accepts compatible protocols.")
@pytest.mark.parametrize(
    "protocol_version",
    [
        MANAGED_EXECUTION_PROTOCOL_VERSION,
        DATASET_VERSION_PROTOCOL_VERSION,
        RESERVED_QUEUE_PROTOCOL_VERSION,
        SHARED_POOL_QUEUE_PROTOCOL_VERSION,
    ],
)
def test_nonqueued_managed_start_accepts_compatible_protocols(
    protocol_version: str,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("managed", protocol_version=protocol_version)
    register_release(database_session, release)
    promote_release(database_session, release.id)
    database_session.commit()
    dispatch = admit_start_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        dispatch_id=uuid4(),
    )

    assert dispatch.executor_release_id == release.id


@pytest.mark.description("A pinned dataset start outside the queue accepts compatible protocols.")
@pytest.mark.parametrize(
    "protocol_version",
    [DATASET_VERSION_PROTOCOL_VERSION, RESERVED_QUEUE_PROTOCOL_VERSION, SHARED_POOL_QUEUE_PROTOCOL_VERSION],
)
def test_nonqueued_pinned_dataset_start_accepts_compatible_protocols(
    protocol_version: str,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("dataset", protocol_version=protocol_version)
    register_release(database_session, release)
    promote_release(database_session, release.id)
    database_session.commit()
    example_benchmark_object.arguments = example_benchmark_object.arguments.model_copy(
        update={"dataset_version": DatasetVersion(id="dataset-v1", label="snapshot")}
    )

    dispatch = admit_start_dispatch(database_session, benchmark=example_benchmark_object, dispatch_id=uuid4())

    assert example_benchmark_object.arguments.queue_pool_id is None
    assert dispatch.executor_release_id == release.id


@pytest.mark.description("A pinned dataset start rejects protocols that lack dataset versions.")
@pytest.mark.parametrize("protocol_version", ["1", "2", MANAGED_EXECUTION_PROTOCOL_VERSION])
def test_pinned_dataset_start_rejects_protocols_without_dataset_version_support(
    protocol_version: str,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("legacy", protocol_version=protocol_version)
    register_release(database_session, release)
    promote_release(database_session, release.id)
    database_session.commit()
    example_benchmark_object.arguments = example_benchmark_object.arguments.model_copy(
        update={"dataset_version": DatasetVersion(id="dataset-v1", label="snapshot")}
    )

    with pytest.raises(ReleaseControlError, match="supports pinned dataset versions"):
        admit_start_dispatch(database_session, benchmark=example_benchmark_object, dispatch_id=uuid4())


@pytest.mark.description("A queued start rejects old protocols and leaves admission untouched.")
@pytest.mark.parametrize(
    "protocol_version",
    [MANAGED_EXECUTION_PROTOCOL_VERSION, DATASET_VERSION_PROTOCOL_VERSION, RESERVED_QUEUE_PROTOCOL_VERSION],
)
def test_queued_start_rejects_old_protocols_without_mutating_admission(
    protocol_version: str,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("active", protocol_version=protocol_version)
    register_release(database_session, release)
    promote_release(database_session, release.id)
    example_benchmark_object.arguments = example_benchmark_object.arguments.model_copy(
        update={"queue_pool_id": _LEGACY_QUEUE_POOL_ID}
    )
    database_session.add(example_benchmark_object)
    database_session.commit()
    dispatch_id = uuid4()

    with pytest.raises(ReleaseControlError, match="supports shared pool admission"):
        admit_start_dispatch(database_session, benchmark=example_benchmark_object, dispatch_id=dispatch_id)

    assert example_benchmark_object.arguments.queue_pool_id == _LEGACY_QUEUE_POOL_ID
    assert example_benchmark_object.executor_release_id is None
    assert example_benchmark_object.current_execution_release_id is None
    assert example_benchmark_object.executor_artifact_uri is None
    assert example_benchmark_object.executor_artifact_digest is None
    assert example_benchmark_object.executor_protocol_version is None
    assert database_session.get(ExecutorDispatch, dispatch_id) is None
    database_session.rollback()
    database_session.refresh(example_benchmark_object)

    assert example_benchmark_object.arguments.queue_pool_id == _LEGACY_QUEUE_POOL_ID
    assert example_benchmark_object.executor_release_id is None
    assert example_benchmark_object.current_execution_release_id is None
    assert example_benchmark_object.executor_artifact_uri is None
    assert example_benchmark_object.executor_artifact_digest is None
    assert example_benchmark_object.executor_protocol_version is None
    assert database_session.get(ExecutorDispatch, dispatch_id) is None


@pytest.mark.description("A start on protocol 6 saves a reserved queue ID.")
def test_start_persists_a_reserved_queue_id_with_protocol_six(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("active", protocol_version=SHARED_POOL_QUEUE_PROTOCOL_VERSION)
    register_release(database_session, release)
    promote_release(database_session, release.id)
    database_session.commit()
    example_benchmark_object.arguments = example_benchmark_object.arguments.model_copy(
        update={"queue_pool_id": _LEGACY_QUEUE_POOL_ID}
    )

    dispatch = admit_start_dispatch(database_session, benchmark=example_benchmark_object, dispatch_id=uuid4())
    database_session.commit()
    database_session.expire_all()

    persisted = database_session.get(Benchmark, example_benchmark_object.id)
    assert persisted is not None
    assert persisted.arguments.queue_pool_id == reserved_queue_pool_id(_LEGACY_QUEUE_POOL_ID)
    assert persisted.current_execution_release_id == release.id
    assert dispatch.executor_release_id == release.id


@pytest.mark.description("Reserved queue recovery rejects old protocols and leaves admission untouched.")
@pytest.mark.parametrize(
    "protocol_version",
    [MANAGED_EXECUTION_PROTOCOL_VERSION, DATASET_VERSION_PROTOCOL_VERSION, RESERVED_QUEUE_PROTOCOL_VERSION],
)
@pytest.mark.parametrize("pre_action_status", [BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPED])
def test_reserved_queue_recovery_rejects_old_protocols_without_mutating_admission(
    protocol_version: str,
    pre_action_status: BenchmarkStatus,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    previous = _release("previous", protocol_version=protocol_version)
    register_release(database_session, previous)
    promote_release(database_session, previous.id)
    pin_benchmark_to_release(example_benchmark_object, previous)
    reserved_id = reserved_queue_pool_id(_LEGACY_QUEUE_POOL_ID)
    example_benchmark_object.status = pre_action_status
    example_benchmark_object.arguments = example_benchmark_object.arguments.model_copy(
        update={"queue_pool_id": reserved_id}
    )
    database_session.add(example_benchmark_object)
    register_release(database_session, _release("active", protocol_version=protocol_version))
    promote_release(database_session, "active")
    database_session.commit()
    dispatch_id = uuid4()

    with pytest.raises(ReleaseControlError, match="supports shared pool admission"):
        admit_recovery_dispatch(
            database_session,
            benchmark=example_benchmark_object,
            pre_action_status=pre_action_status,
            dispatch_id=dispatch_id,
            kind=ExecutorDispatchKind.RETRY,
        )

    assert example_benchmark_object.arguments.queue_pool_id == reserved_id
    assert example_benchmark_object.current_execution_release_id == previous.id
    assert database_session.get(ExecutorDispatch, dispatch_id) is None
    database_session.rollback()
    database_session.refresh(example_benchmark_object)

    assert example_benchmark_object.arguments.queue_pool_id == reserved_id
    assert example_benchmark_object.current_execution_release_id == previous.id
    assert database_session.get(ExecutorDispatch, dispatch_id) is None


@pytest.mark.description("Reserved queue recovery on protocol 6 keeps its queue ID.")
@pytest.mark.parametrize("pre_action_status", [BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPED])
def test_reserved_queue_recovery_keeps_its_id_with_protocol_six(
    pre_action_status: BenchmarkStatus,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    previous_protocol = (
        SHARED_POOL_QUEUE_PROTOCOL_VERSION
        if pre_action_status == BenchmarkStatus.IN_PROGRESS
        else RESERVED_QUEUE_PROTOCOL_VERSION
    )
    previous = _release("previous", protocol_version=previous_protocol)
    register_release(database_session, previous)
    promote_release(database_session, previous.id)
    pin_benchmark_to_release(example_benchmark_object, previous)
    reserved_id = reserved_queue_pool_id(_LEGACY_QUEUE_POOL_ID)
    example_benchmark_object.status = pre_action_status
    example_benchmark_object.arguments = example_benchmark_object.arguments.model_copy(
        update={"queue_pool_id": reserved_id}
    )
    database_session.add(example_benchmark_object)
    register_release(database_session, _release("active", protocol_version=SHARED_POOL_QUEUE_PROTOCOL_VERSION))
    promote_release(database_session, "active")
    database_session.commit()

    dispatch = admit_recovery_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        pre_action_status=pre_action_status,
        dispatch_id=uuid4(),
        kind=ExecutorDispatchKind.RETRY,
    )
    database_session.commit()
    database_session.expire_all()

    persisted = database_session.get(Benchmark, example_benchmark_object.id)
    assert persisted is not None
    release_id = "previous" if pre_action_status == BenchmarkStatus.IN_PROGRESS else "active"
    assert persisted.arguments.queue_pool_id == reserved_id
    assert persisted.current_execution_release_id == release_id
    assert dispatch.executor_release_id == release_id


@pytest.mark.description("Legacy queue recovery asks for a new run instead of dispatching work nothing can serve.")
@pytest.mark.parametrize(
    "protocol_version",
    [MANAGED_EXECUTION_PROTOCOL_VERSION, DATASET_VERSION_PROTOCOL_VERSION, RESERVED_QUEUE_PROTOCOL_VERSION],
)
def test_legacy_queue_recovery_requires_a_new_run_instead_of_dispatching_unserviceable_work(
    protocol_version: str,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    legacy = _release("legacy", protocol_version=protocol_version)
    register_release(database_session, legacy)
    pin_benchmark_to_release(example_benchmark_object, legacy)
    example_benchmark_object.arguments = example_benchmark_object.arguments.model_copy(
        update={"queue_pool_id": _LEGACY_QUEUE_POOL_ID}
    )
    database_session.add(example_benchmark_object)
    register_release(database_session, _release("active", protocol_version=SHARED_POOL_QUEUE_PROTOCOL_VERSION))
    promote_release(database_session, "active")
    database_session.commit()
    dispatch_id = uuid4()

    with pytest.raises(ReleaseControlError, match="start a new queued run"):
        admit_recovery_dispatch(
            database_session,
            benchmark=example_benchmark_object,
            pre_action_status=BenchmarkStatus.STOPPED,
            dispatch_id=dispatch_id,
            kind=ExecutorDispatchKind.RETRY,
        )
    database_session.rollback()
    database_session.refresh(example_benchmark_object)
    assert example_benchmark_object.arguments.queue_pool_id == _LEGACY_QUEUE_POOL_ID
    assert example_benchmark_object.current_execution_release_id == legacy.id
    assert database_session.get(ExecutorDispatch, dispatch_id) is None


@pytest.mark.description("Managed recovery of an in progress run refuses a pinned legacy protocol.")
def test_in_progress_managed_recovery_refuses_a_pinned_legacy_protocol(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    legacy = _release("legacy", protocol_version="2")
    register_release(database_session, legacy)
    promote_release(database_session, legacy.id)
    pin_benchmark_to_release(example_benchmark_object, legacy)
    database_session.add(example_benchmark_object)
    register_release(database_session, _release("managed", protocol_version=MANAGED_EXECUTION_PROTOCOL_VERSION))
    promote_release(database_session, "managed")
    database_session.commit()

    with pytest.raises(ReleaseControlError, match="supports managed runs"):
        admit_recovery_dispatch(
            database_session,
            benchmark=example_benchmark_object,
            pre_action_status=BenchmarkStatus.IN_PROGRESS,
            dispatch_id=uuid4(),
            kind=ExecutorDispatchKind.RETRY,
        )

    database_session.rollback()

    dispatch = admit_recovery_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        pre_action_status=BenchmarkStatus.STOPPED,
        dispatch_id=uuid4(),
        kind=ExecutorDispatchKind.RETRY,
    )

    assert dispatch.executor_release_id == "managed"


@pytest.mark.description("A retry of an in progress run keeps its release and original dispatch active.")
def test_in_progress_retry_keeps_release_and_original_dispatch_active(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("current")
    register_release(database_session, release)
    promote_release(database_session, release.id)
    pin_benchmark_to_release(example_benchmark_object, release)
    database_session.add(example_benchmark_object)
    old_dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    old_dispatch.status = ExecutorDispatchStatus.RUNNING
    old_dispatch.started_at = datetime.now(UTC)
    database_session.add(old_dispatch)
    database_session.commit()

    dispatch = admit_recovery_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        pre_action_status=BenchmarkStatus.IN_PROGRESS,
        dispatch_id=uuid4(),
        kind=ExecutorDispatchKind.RETRY,
    )
    database_session.commit()
    database_session.refresh(old_dispatch)

    assert example_benchmark_object.current_execution_release_id == release.id
    assert dispatch.executor_release_id == release.id
    assert old_dispatch.status == ExecutorDispatchStatus.RUNNING
    assert old_dispatch.finished_at is None


@pytest.mark.description("An enqueue failure leaves the start retryable.")
def test_enqueue_failure_makes_start_retryable(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    register_release(database_session, _release("active"))
    promote_release(database_session, "active")
    dispatch = admit_start_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        dispatch_id=uuid4(),
    )
    tasks = [
        Task(
            org_id=example_benchmark_object.org_id,
            benchmark=example_benchmark_object.id,
            task_id=f"task-{status.value.lower()}",
            status=status,
            started_at=dispatch.created_at - timedelta(seconds=1),
        )
        for status in (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
    ]
    database_session.add_all(tasks)
    database_session.commit()

    resolution = resolve_enqueue_failure(
        database_session,
        benchmark_id=example_benchmark_object.id,
        dispatch_id=dispatch.id,
        task_ids=[task.task_id for task in tasks],
    )
    database_session.refresh(example_benchmark_object)
    database_session.refresh(dispatch)
    for task in tasks:
        database_session.refresh(task)

    assert resolution == EnqueueFailureResolution.FAILED
    assert example_benchmark_object.status == BenchmarkStatus.ERROR
    assert dispatch.status == ExecutorDispatchStatus.FAILED
    assert {task.status for task in tasks} == {TaskStatus.ERROR}

    error_results = database_session.exec(
        select(ErrorResult).where(col(ErrorResult.task).in_([task.id for task in tasks]))
    ).all()
    assert len(error_results) == len(tasks)
    for error_result in error_results:
        assert error_result.error_message == "Executor dispatch enqueue failed"
        assert error_result.producer == "executor_dispatch"
        assert error_result.operation == "enqueue"
        assert error_result.error_type == "ExecutorDispatchEnqueueError"
        assert error_result.category == FailureCategory.INFRASTRUCTURE
        assert error_result.cause_code is None
        assert error_result.retry_scheduled is False
        assert error_result.failed_attempt_number is None


@pytest.mark.description("A payload decryption failure is recorded as an infrastructure failure.")
def test_payload_decryption_failure_records_infrastructure_category(
    database_session: Session,
    example_benchmark_object: Benchmark,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register_release(database_session, _release("active"))
    promote_release(database_session, "active")
    dispatch = admit_start_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        dispatch_id=uuid4(),
        task_ids=["preclaim"],
    )
    task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="preclaim",
        status=TaskStatus.PENDING,
        started_at=dispatch.created_at - timedelta(seconds=1),
    )
    database_session.add(task)
    database_session.commit()
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setattr(entrypoint, "engine", database_session.get_bind())
    key = generate_payload_key(dispatch.id)
    sealed = seal_payload(dispatch.id, {"benchmark_id_str": str(example_benchmark_object.id)}, key)
    database_session.add(
        ExecutorDispatchPayload(
            dispatch_id=dispatch.id,
            ciphertext=sealed.ciphertext + b"corrupt",
            encrypted_data_key=sealed.encrypted_data_key,
            nonce=sealed.nonce,
        )
    )
    database_session.commit()

    with pytest.raises(InvalidTag):
        entrypoint._claim(dispatch.id)

    database_session.expire_all()
    error = database_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).one()
    assert error.error_message == "Executor payload decrypt failed: InvalidTag"
    assert error.error_type == "InvalidTag"
    assert error.cause_code == "PAYLOAD_DECRYPT_FAILED"
    assert error.category == FailureCategory.INFRASTRUCTURE


@pytest.mark.description("An enqueue failure on an additive retry keeps the original execution active.")
def test_additive_retry_enqueue_failure_keeps_original_execution_active(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("active")
    register_release(database_session, release)
    promote_release(database_session, release.id)
    pin_benchmark_to_release(example_benchmark_object, release)
    original_dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    original_dispatch.status = ExecutorDispatchStatus.RUNNING
    original_dispatch.started_at = datetime.now(UTC)
    dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.RETRY,
        dispatch_id=uuid4(),
    )
    shared_tasks = [
        Task(
            org_id=example_benchmark_object.org_id,
            benchmark=example_benchmark_object.id,
            task_id=f"shared-{status.value.lower()}",
            status=status,
            started_at=dispatch.created_at - timedelta(seconds=1),
        )
        for status in (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS)
    ]
    owned_evaluation = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="owned-evaluation",
        status=TaskStatus.EVALUATING,
        started_at=dispatch.created_at,
    )
    sibling_evaluation = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="sibling-evaluation",
        status=TaskStatus.EVALUATING,
        started_at=dispatch.created_at - timedelta(seconds=1),
    )
    stopped_task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="stopped-task",
        status=TaskStatus.STOPPED,
    )
    newer_retry_task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="newer-retry-task",
        status=TaskStatus.PENDING,
        started_at=dispatch.created_at + timedelta(seconds=1),
    )
    database_session.add(example_benchmark_object)
    database_session.add_all(
        [
            *shared_tasks,
            owned_evaluation,
            sibling_evaluation,
            stopped_task,
            newer_retry_task,
            original_dispatch,
            dispatch,
        ]
    )
    database_session.commit()

    resolution = resolve_enqueue_failure(
        database_session,
        benchmark_id=example_benchmark_object.id,
        dispatch_id=dispatch.id,
        task_ids=[
            *(task.task_id for task in shared_tasks),
            owned_evaluation.task_id,
            sibling_evaluation.task_id,
            stopped_task.task_id,
            newer_retry_task.task_id,
        ],
    )
    database_session.refresh(example_benchmark_object)
    for task in [*shared_tasks, owned_evaluation, sibling_evaluation, stopped_task, newer_retry_task]:
        database_session.refresh(task)

    assert resolution == EnqueueFailureResolution.FAILED
    assert example_benchmark_object.status == BenchmarkStatus.IN_PROGRESS
    assert [task.status for task in shared_tasks] == [
        TaskStatus.PENDING,
        TaskStatus.BUILDING,
        TaskStatus.IN_PROGRESS,
    ]
    assert owned_evaluation.status == TaskStatus.ERROR
    assert sibling_evaluation.status == TaskStatus.EVALUATING
    assert stopped_task.status == TaskStatus.STOPPED
    assert newer_retry_task.status == TaskStatus.PENDING

    error_results = database_session.exec(
        select(ErrorResult).where(
            col(ErrorResult.task).in_([owned_evaluation.id, stopped_task.id, newer_retry_task.id])
        )
    ).all()
    assert len(error_results) == 1
    assert error_results[0].task == owned_evaluation.id
    assert error_results[0].retry_scheduled is False


@pytest.mark.description("An enqueue failure does not override a delivery that was already claimed.")
def test_enqueue_failure_does_not_override_claimed_delivery(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    register_release(database_session, _release("active"))
    promote_release(database_session, "active")
    dispatch = admit_start_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        dispatch_id=uuid4(),
    )
    dispatch.status = ExecutorDispatchStatus.RUNNING
    dispatch.started_at = datetime.now(UTC)
    database_session.add(dispatch)
    database_session.commit()

    resolution = resolve_enqueue_failure(
        database_session,
        benchmark_id=example_benchmark_object.id,
        dispatch_id=dispatch.id,
        task_ids=[],
    )
    database_session.refresh(example_benchmark_object)

    assert resolution == EnqueueFailureResolution.DELIVERED
    assert example_benchmark_object.status == BenchmarkStatus.IN_PROGRESS


@pytest.mark.description("A failed running dispatch leaves its active sibling alone.")
def test_running_dispatch_failure_preserves_active_sibling(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("active")
    register_release(database_session, release)
    pin_benchmark_to_release(example_benchmark_object, release)
    failing_dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.RETRY,
        dispatch_id=uuid4(),
    )
    sibling_dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    for dispatch in (failing_dispatch, sibling_dispatch):
        dispatch.status = ExecutorDispatchStatus.RUNNING
        dispatch.started_at = datetime.now(UTC)
        database_session.add(dispatch)
    shared_tasks = [
        Task(
            org_id=example_benchmark_object.org_id,
            benchmark=example_benchmark_object.id,
            task_id=f"shared-{status.value.lower()}",
            status=status,
            started_at=failing_dispatch.created_at - timedelta(seconds=1),
        )
        for status in (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS)
    ]
    newer_retry_task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="newer-retry-task",
        status=TaskStatus.PENDING,
        started_at=failing_dispatch.created_at + timedelta(seconds=1),
    )
    owned_evaluation = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="owned-evaluation",
        status=TaskStatus.EVALUATING,
        started_at=failing_dispatch.created_at,
    )
    sibling_evaluation = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="sibling-evaluation",
        status=TaskStatus.EVALUATING,
        started_at=failing_dispatch.created_at - timedelta(seconds=1),
    )
    database_session.add(example_benchmark_object)
    database_session.add_all([*shared_tasks, newer_retry_task, owned_evaluation, sibling_evaluation])
    database_session.commit()

    assert record_dispatch_failure(
        database_session,
        benchmark_id=example_benchmark_object.id,
        dispatch_id=failing_dispatch.id,
        task_ids=[
            *(task.task_id for task in shared_tasks),
            newer_retry_task.task_id,
            owned_evaluation.task_id,
            sibling_evaluation.task_id,
        ],
        error_message="retry failed",
        producer="tracker",
        operation="process_benchmark",
        error_type="RuntimeError",
        category=FailureCategory.UNKNOWN,
    )
    database_session.commit()
    database_session.refresh(example_benchmark_object)
    database_session.refresh(failing_dispatch)
    database_session.refresh(sibling_dispatch)
    for task in [*shared_tasks, newer_retry_task, owned_evaluation, sibling_evaluation]:
        database_session.refresh(task)

    assert example_benchmark_object.status == BenchmarkStatus.IN_PROGRESS
    assert failing_dispatch.status == ExecutorDispatchStatus.FAILED
    assert sibling_dispatch.status == ExecutorDispatchStatus.RUNNING
    assert [task.status for task in shared_tasks] == [
        TaskStatus.PENDING,
        TaskStatus.BUILDING,
        TaskStatus.IN_PROGRESS,
    ]
    assert newer_retry_task.status == TaskStatus.PENDING
    assert owned_evaluation.status == TaskStatus.ERROR
    assert sibling_evaluation.status == TaskStatus.EVALUATING

    error_results = database_session.exec(
        select(ErrorResult).where(col(ErrorResult.task).in_([owned_evaluation.id, newer_retry_task.id]))
    ).all()
    assert len(error_results) == 1
    error_result = error_results[0]
    assert error_result.task == owned_evaluation.id
    assert error_result.error_message == "retry failed"
    assert error_result.producer == "tracker"
    assert error_result.operation == "process_benchmark"
    assert error_result.error_type == "RuntimeError"
    assert error_result.category == FailureCategory.UNKNOWN
    assert error_result.retry_scheduled is False
    assert error_result.failed_attempt_number is None


@pytest.mark.description("When the last dispatch fails, a graceful stop completes.")
@pytest.mark.parametrize(
    ("dispatch_status", "task_status"),
    [
        (ExecutorDispatchStatus.RUNNING, TaskStatus.IN_PROGRESS),
        (ExecutorDispatchStatus.QUEUED, TaskStatus.PENDING),
    ],
)
def test_last_dispatch_failure_completes_graceful_stop(
    dispatch_status: ExecutorDispatchStatus,
    task_status: TaskStatus,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("active")
    register_release(database_session, release)
    pin_benchmark_to_release(example_benchmark_object, release)
    example_benchmark_object.status = BenchmarkStatus.STOPPING
    dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
        task_ids=["task-1"],
    )
    dispatch.status = dispatch_status
    if dispatch_status == ExecutorDispatchStatus.RUNNING:
        dispatch.started_at = datetime.now(UTC) - timedelta(minutes=6)
    task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="task-1",
        status=task_status,
        started_at=dispatch.created_at - timedelta(seconds=1),
    )
    database_session.add_all([example_benchmark_object, dispatch, task])
    database_session.commit()

    assert _fail_stopped_task(database_session, dispatch)
    database_session.commit()
    database_session.refresh(example_benchmark_object)
    database_session.refresh(dispatch)
    database_session.refresh(task)

    assert example_benchmark_object.status == BenchmarkStatus.STOPPED
    assert example_benchmark_object.finished_at is not None
    assert dispatch.status == ExecutorDispatchStatus.FAILED
    assert dispatch.failure_reason == "ECS_TASK_STOPPED"
    assert task.status == TaskStatus.ERROR


@pytest.mark.description("A failed queued dispatch errors the run.")
def test_queued_dispatch_failure_errors_the_benchmark(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("active")
    register_release(database_session, release)
    pin_benchmark_to_release(example_benchmark_object, release)
    dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.RETRY,
        dispatch_id=uuid4(),
        task_ids=["task-1"],
    )
    task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="task-1",
        status=TaskStatus.PENDING,
        started_at=dispatch.created_at - timedelta(seconds=1),
    )
    database_session.add_all([example_benchmark_object, dispatch, task])
    database_session.commit()

    assert _fail_stopped_task(database_session, dispatch)
    database_session.commit()
    database_session.refresh(example_benchmark_object)
    database_session.refresh(dispatch)
    database_session.refresh(task)

    assert example_benchmark_object.status == BenchmarkStatus.ERROR
    assert dispatch.status == ExecutorDispatchStatus.FAILED
    assert dispatch.failure_reason == "ECS_TASK_STOPPED"
    assert task.status == TaskStatus.ERROR

    error_result = database_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).one()
    assert error_result.error_type == "ExecutorTaskStoppedBeforeClaim"
    assert error_result.category == FailureCategory.INFRASTRUCTURE


@pytest.mark.description("Terminal recovery finishes the active dispatches.")
def test_terminal_recovery_terminalizes_active_dispatches(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    old_release = _release("old")
    active_release = _release("active")
    register_release(database_session, old_release)
    register_release(database_session, active_release)
    promote_release(database_session, old_release.id)
    pin_benchmark_to_release(example_benchmark_object, old_release)
    database_session.add(example_benchmark_object)
    old_dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        old_release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    database_session.add(old_dispatch)
    promote_release(database_session, active_release.id)
    database_session.commit()

    example_benchmark_object.status = BenchmarkStatus.IN_PROGRESS
    example_benchmark_object.finished_at = None
    new_dispatch = admit_recovery_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        pre_action_status=BenchmarkStatus.ERROR,
        dispatch_id=uuid4(),
        kind=ExecutorDispatchKind.RESUME,
    )
    database_session.commit()
    database_session.refresh(old_dispatch)

    assert example_benchmark_object.current_execution_release_id == active_release.id
    assert new_dispatch.executor_release_id == active_release.id
    assert old_dispatch.status == ExecutorDispatchStatus.FAILED
    assert old_dispatch.finished_at is not None


@pytest.mark.description("Finishing active dispatches keeps the chosen finalizer.")
def test_terminalize_active_dispatches_preserves_selected_finalizer(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("active")
    register_release(database_session, release)
    pin_benchmark_to_release(example_benchmark_object, release)
    database_session.add(example_benchmark_object)
    finalizer = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    sibling = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.RETRY,
        dispatch_id=uuid4(),
    )
    sibling.created_at = finalizer.created_at + timedelta(seconds=1)
    finalizer.status = ExecutorDispatchStatus.RUNNING
    finalizer.started_at = datetime.now(UTC)
    sibling.status = ExecutorDispatchStatus.RUNNING
    sibling.started_at = datetime.now(UTC)
    database_session.add(finalizer)
    database_session.add(sibling)
    database_session.commit()

    terminalize_active_dispatches(
        database_session,
        example_benchmark_object.id,
        except_dispatch_id=finalizer.id,
    )
    database_session.commit()
    database_session.refresh(finalizer)
    database_session.refresh(sibling)

    assert finalizer.status == ExecutorDispatchStatus.RUNNING
    assert sibling.status == ExecutorDispatchStatus.FAILED
    assert sibling.finished_at is not None


@pytest.mark.description("Locking execution authority for a stopped run raises `ExecutionAuthorityRevoked`.")
@pytest.mark.parametrize(
    ("status", "allowed"),
    [
        (BenchmarkStatus.IN_PROGRESS, True),
        (BenchmarkStatus.FINISHED, True),
        (BenchmarkStatus.ERROR, True),
        (BenchmarkStatus.STOPPING, True),
        (BenchmarkStatus.STOPPED, False),
    ],
)
def test_terminal_authority_rejects_stopped_state(
    status: BenchmarkStatus,
    allowed: bool,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("authority")
    register_release(database_session, release)
    promote_release(database_session, release.id)
    pin_benchmark_to_release(example_benchmark_object, release)
    example_benchmark_object.status = status
    if status in (BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR):
        example_benchmark_object.finished_at = datetime.now(UTC)
    dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    dispatch.status = ExecutorDispatchStatus.RUNNING
    dispatch.started_at = datetime.now(UTC)
    database_session.add(example_benchmark_object)
    database_session.add(dispatch)
    database_session.commit()
    authority = ExecutionAuthority(
        benchmark_id=example_benchmark_object.id,
        dispatch_id=dispatch.id,
    )

    if allowed:
        assert (
            lock_execution_authority(database_session, authority, require_in_progress=False).id
            == example_benchmark_object.id
        )
        database_session.rollback()
    else:
        with pytest.raises(ExecutionAuthorityRevoked):
            lock_execution_authority(database_session, authority, require_in_progress=False)
        database_session.rollback()


@pytest.mark.description(
    (
        "Failure recording respects sibling assignments and newer attempts.\n"
        "\n"
        "Test cases:\n"
        "- An unrelated sibling does not prevent cleanup of an expired assignment.\n"
        "- Overlapping or unknown assignments preserve potentially shared tasks.\n"
        "- A newer task attempt and the sibling's own task remain untouched."
    )
)
@pytest.mark.parametrize("task_status", [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS])
@pytest.mark.parametrize("assignment", [None, ["owned-task", "sibling-task"], ["sibling-task"]])
def test_dispatch_failure_only_preserves_tasks_an_active_sibling_can_run(
    database_session: Session,
    example_benchmark_object: Benchmark,
    task_status: TaskStatus,
    assignment: list[str] | None,
) -> None:
    release = _release("active")
    register_release(database_session, release)
    pin_benchmark_to_release(example_benchmark_object, release)
    dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
        task_ids=["owned-task", "newer-task"],
    )
    sibling = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.RESUME,
        dispatch_id=uuid4(),
        task_ids=assignment,
    )
    sibling.created_at = dispatch.created_at + timedelta(seconds=1)
    task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="owned-task",
        status=task_status,
        started_at=dispatch.created_at - timedelta(seconds=1),
    )
    newer_task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="newer-task",
        started_at=dispatch.created_at + timedelta(seconds=1),
    )
    sibling_task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="sibling-task",
        status=TaskStatus.IN_PROGRESS,
    )
    database_session.add_all([example_benchmark_object, dispatch, sibling, task, newer_task, sibling_task])
    database_session.commit()

    assert _fail_stopped_task(database_session, dispatch)
    database_session.commit()
    for row in [example_benchmark_object, dispatch, sibling, task, newer_task, sibling_task]:
        database_session.refresh(row)

    expected_status = TaskStatus.ERROR if assignment == ["sibling-task"] else task_status
    assert task.status == expected_status
    assert sibling_task.status == TaskStatus.IN_PROGRESS
    assert newer_task.status == TaskStatus.PENDING
    assert sibling.status == ExecutorDispatchStatus.QUEUED
    assert dispatch.status == ExecutorDispatchStatus.FAILED
    assert example_benchmark_object.status == BenchmarkStatus.IN_PROGRESS
    errors = database_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).all()
    assert len(errors) == (1 if expected_status == TaskStatus.ERROR else 0)
    if errors:
        assert errors[0].cause_code == "ECS_TASK_STOPPED"


def _retried_run(
    session: Session, benchmark: Benchmark, *, retry_status: ExecutorDispatchStatus
) -> tuple[ExecutorDispatch, ExecutorDispatch, Task, Task]:
    release = _release("active")
    register_release(session, release)
    pin_benchmark_to_release(benchmark, release)
    start = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["task-a", "task-b"]
    )
    start.status = ExecutorDispatchStatus.RUNNING
    start.started_at = start.created_at
    retry = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.RETRY, dispatch_id=uuid4(), task_ids=["task-a"]
    )
    retry.created_at = start.created_at + timedelta(minutes=30)
    retry.status = retry_status
    retried = Task(
        org_id=benchmark.org_id,
        benchmark=benchmark.id,
        task_id="task-a",
        status=TaskStatus.PENDING,
        started_at=retry.created_at - timedelta(microseconds=1),
    )
    running = Task(
        org_id=benchmark.org_id,
        benchmark=benchmark.id,
        task_id="task-b",
        status=TaskStatus.IN_PROGRESS,
        started_at=start.created_at + timedelta(seconds=1),
    )
    session.add_all([benchmark, start, retry, retried, running])
    session.commit()
    return start, retry, retried, running


@pytest.mark.description(
    "A retry runner that never starts must not leave its task PENDING behind the start dispatch's assignment."
)
def test_failed_retry_dispatch_errors_its_task_even_though_the_start_dispatch_listed_it(
    database_session: Session, example_benchmark_object: Benchmark
) -> None:
    start, retry, retried, running = _retried_run(
        database_session, example_benchmark_object, retry_status=ExecutorDispatchStatus.QUEUED
    )

    assert _fail_stopped_task(database_session, retry)
    database_session.commit()
    for row in (example_benchmark_object, start, retry, retried, running):
        database_session.refresh(row)

    assert retried.status == TaskStatus.ERROR
    assert running.status == TaskStatus.IN_PROGRESS
    assert retry.status == ExecutorDispatchStatus.FAILED
    assert retry.failure_reason == "ECS_TASK_STOPPED"
    assert start.status == ExecutorDispatchStatus.RUNNING
    assert example_benchmark_object.status == BenchmarkStatus.IN_PROGRESS
    error = database_session.exec(select(ErrorResult).where(ErrorResult.task == retried.id)).one()
    assert error.cause_code == "ECS_TASK_STOPPED"
    assert unowned_pending_task_ids(database_session, example_benchmark_object) == []


@pytest.mark.description(
    (
        "A PENDING task is owned while a dispatch admitted for its attempt is active and stranded once it failed.\n"
        "\n"
        "An attempt the scheduler restarted in place (newer than every dispatch) still belongs to the start dispatch."
    )
)
def test_unowned_pending_tasks_are_those_no_active_dispatch_runs(
    database_session: Session, example_benchmark_object: Benchmark
) -> None:
    start, retry, retried, _running = _retried_run(
        database_session, example_benchmark_object, retry_status=ExecutorDispatchStatus.QUEUED
    )
    assert unowned_pending_task_ids(database_session, example_benchmark_object) == []

    retry.status = ExecutorDispatchStatus.FAILED
    database_session.add(retry)
    database_session.commit()
    assert unowned_pending_task_ids(database_session, example_benchmark_object) == ["task-a"]

    retried.started_at = retry.created_at + timedelta(minutes=1)
    database_session.add(retried)
    database_session.commit()
    assert unowned_pending_task_ids(database_session, example_benchmark_object) == []

    start.status = ExecutorDispatchStatus.FINISHED
    database_session.add(start)
    database_session.commit()
    assert unowned_pending_task_ids(database_session, example_benchmark_object) == ["task-a"]

    example_benchmark_object.status = BenchmarkStatus.ERROR
    database_session.add(example_benchmark_object)
    database_session.commit()
    assert unowned_pending_task_ids(database_session, example_benchmark_object) == []


@pytest.mark.description("Pending tasks of a run with no dispatch rows have no known owner.")
def test_unowned_pending_tasks_are_unknown_for_a_run_without_dispatch_rows(
    database_session: Session, example_benchmark_object: Benchmark
) -> None:
    pending = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="task-a",
        status=TaskStatus.PENDING,
    )
    database_session.add_all([example_benchmark_object, pending])
    database_session.commit()

    assert unowned_pending_task_ids(database_session, example_benchmark_object) == []


@pytest.mark.description(
    "Queued running recovery lists runnable tasks without resetting started_at. The start dispatch keeps them."
)
def test_failed_resume_of_untouched_attempts_keeps_tasks_the_start_dispatch_still_runs(
    database_session: Session, example_benchmark_object: Benchmark
) -> None:
    release = _release("active")
    register_release(database_session, release)
    pin_benchmark_to_release(example_benchmark_object, release)
    start = create_executor_dispatch(
        example_benchmark_object.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["a", "b"]
    )
    start.status = ExecutorDispatchStatus.RUNNING
    start.started_at = start.created_at
    resume = create_executor_dispatch(
        example_benchmark_object.id, release, ExecutorDispatchKind.RESUME, dispatch_id=uuid4(), task_ids=["a", "b"]
    )
    resume.created_at = start.created_at + timedelta(minutes=30)
    building = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="a",
        status=TaskStatus.BUILDING,
        started_at=start.created_at - timedelta(seconds=1),
    )
    pending = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="b",
        status=TaskStatus.PENDING,
        started_at=start.created_at - timedelta(seconds=1),
    )
    database_session.add_all([example_benchmark_object, start, resume, building, pending])
    database_session.commit()

    assert _fail_stopped_task(database_session, resume)
    database_session.commit()
    for row in (building, pending, resume, start):
        database_session.refresh(row)

    assert building.status == TaskStatus.BUILDING
    assert pending.status == TaskStatus.PENDING
    assert resume.status == ExecutorDispatchStatus.FAILED
    assert start.status == ExecutorDispatchStatus.RUNNING
    assert unowned_pending_task_ids(database_session, example_benchmark_object) == []


@pytest.fixture
def local_payload_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"k" * 32).decode())
    monkeypatch.delenv("EXECUTOR_PAYLOAD_KMS_KEY_ID", raising=False)


def _queue_claimable_dispatch(session: Session, benchmark: Benchmark) -> tuple[ExecutorRelease, ExecutorDispatch]:
    release = _release("claimable")
    register_release(session, release)
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["task-1"]
    )
    sealed = seal_payload(dispatch.id, {"benchmark_id_str": str(benchmark.id)}, generate_payload_key(dispatch.id))
    payload = ExecutorDispatchPayload(
        dispatch_id=dispatch.id,
        ciphertext=sealed.ciphertext,
        encrypted_data_key=sealed.encrypted_data_key,
        nonce=sealed.nonce,
    )
    session.add_all([benchmark, dispatch, payload])
    session.commit()
    return release, dispatch


@pytest.mark.usefixtures("local_payload_key")
def test_claim_consumes_the_payload_once(database_session: Session, example_benchmark_object: Benchmark) -> None:
    release, dispatch = _queue_claimable_dispatch(database_session, example_benchmark_object)

    claimed = claim_dispatch(database_session, dispatch.id, task_arn=None)
    database_session.commit()

    assert claimed == ClaimedDispatch(
        dispatch_id=dispatch.id,
        benchmark_id=example_benchmark_object.id,
        release_id=release.id,
        assigned_task_ids=["task-1"],
        payload={"benchmark_id_str": str(example_benchmark_object.id)},
    )
    database_session.refresh(dispatch)
    assert dispatch.status == ExecutorDispatchStatus.RUNNING
    assert dispatch.started_at is not None
    assert database_session.get(ExecutorDispatchPayload, dispatch.id) is None
    assert claim_dispatch(database_session, dispatch.id, task_arn=None) is None


@pytest.mark.usefixtures("local_payload_key")
@pytest.mark.parametrize("revocation", ["benchmark_stopping", "benchmark_repinned"])
def test_claim_refuses_a_dispatch_the_benchmark_no_longer_owns(
    database_session: Session, example_benchmark_object: Benchmark, revocation: str
) -> None:
    _, dispatch = _queue_claimable_dispatch(database_session, example_benchmark_object)
    if revocation == "benchmark_stopping":
        example_benchmark_object.status = BenchmarkStatus.STOPPING
    else:
        newer = _release("newer")
        register_release(database_session, newer)
        example_benchmark_object.current_execution_release_id = newer.id
    database_session.add(example_benchmark_object)
    database_session.commit()

    assert claim_dispatch(database_session, dispatch.id, task_arn=None) is None
    database_session.refresh(dispatch)
    assert dispatch.status == ExecutorDispatchStatus.QUEUED
    assert database_session.get(ExecutorDispatchPayload, dispatch.id) is not None


@pytest.mark.usefixtures("local_payload_key")
@pytest.mark.parametrize(
    ("benchmark_status", "with_sibling", "expected_status"),
    [
        (BenchmarkStatus.IN_PROGRESS, False, BenchmarkStatus.ERROR),
        (BenchmarkStatus.IN_PROGRESS, True, BenchmarkStatus.IN_PROGRESS),
        (BenchmarkStatus.STOPPING, False, BenchmarkStatus.STOPPED),
        (BenchmarkStatus.FINISHED, False, BenchmarkStatus.FINISHED),
    ],
)
def test_finish_finalizes_only_a_benchmark_no_dispatch_still_owns(
    database_session: Session,
    example_benchmark_object: Benchmark,
    benchmark_status: BenchmarkStatus,
    with_sibling: bool,
    expected_status: BenchmarkStatus,
) -> None:
    release, dispatch = _queue_claimable_dispatch(database_session, example_benchmark_object)
    claimed = claim_dispatch(database_session, dispatch.id, task_arn=None)
    assert claimed is not None
    if with_sibling:
        sibling = create_executor_dispatch(
            example_benchmark_object.id, release, ExecutorDispatchKind.RETRY, dispatch_id=uuid4(), task_ids=["task-2"]
        )
        sibling.created_at = dispatch.created_at + timedelta(seconds=1)
        database_session.add(sibling)
    example_benchmark_object.status = benchmark_status
    database_session.add(example_benchmark_object)
    database_session.commit()

    finish_dispatch(database_session, claimed)
    database_session.commit()

    database_session.refresh(dispatch)
    database_session.refresh(example_benchmark_object)
    assert dispatch.status == ExecutorDispatchStatus.FINISHED
    assert dispatch.finished_at is not None
    assert example_benchmark_object.status == expected_status


@pytest.mark.usefixtures("local_payload_key")
def test_finish_does_not_revive_a_dispatch_recovery_already_failed(
    database_session: Session, example_benchmark_object: Benchmark
) -> None:
    _, dispatch = _queue_claimable_dispatch(database_session, example_benchmark_object)
    claimed = claim_dispatch(database_session, dispatch.id, task_arn=None)
    assert claimed is not None
    database_session.commit()
    database_session.refresh(dispatch)
    assert _fail_stopped_task(database_session, dispatch)
    database_session.commit()

    finish_dispatch(database_session, claimed)
    database_session.commit()

    database_session.refresh(dispatch)
    database_session.refresh(example_benchmark_object)
    assert dispatch.status == ExecutorDispatchStatus.FAILED
    assert example_benchmark_object.status == BenchmarkStatus.ERROR
