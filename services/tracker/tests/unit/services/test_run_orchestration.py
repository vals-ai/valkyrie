import asyncio
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import anyio
import pytest
from benchmark_service import SandboxProvider
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.client import BenchmarkServiceClient
from sqlmodel import Session, col, select

from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import (
    Benchmark,
    ErrorResult,
    EvaluationResult,
    ExecutorDispatch,
    Org,
    SandboxBuildReservation,
    Task,
)
from tracker.domain import BenchmarkStatus, FailureCategory, TaskStatus
from tracker.executor.control_queue import queue_pool_id, reserved_queue_pool_id
from tracker.executor.control_tasks import TaskState
from tracker.aws.runtime import AWSResources
from tracker.types import AgentContractRequest, ManagedExecutionContext, RunExecutionRequest, StartBenchmarkRequest
from tracker.logging.context import sandbox_provider_var
from tracker.services.runs.start import process_benchmark
from tracker.services.benchmarks import start_benchmark_request_to_benchmark


_RESOURCES = AWSResources(
    region="us-east-1",
    s3_bucket="test-bucket",
    log_group="test-log-group",
    log_retention_days=30,
)


def _persist_benchmark(
    request: StartBenchmarkRequest,
    session: Session,
    *,
    queued_pool_id: str | None = None,
) -> Benchmark:
    org = session.get(Org, TEST_ORG_ID)
    assert org is not None
    benchmark = start_benchmark_request_to_benchmark(
        request,
        RequestIdentity(org=org),
        queue_pool_id=queued_pool_id,
    )
    session.add(benchmark)
    session.commit()
    return benchmark


def _queued_run(
    contract: AgentContractRequest,
    session: Session,
    task_ids: list[str],
    *,
    pool_id: str = "coordinator-pool",
    concurrency: int = 5,
) -> tuple[StartBenchmarkRequest, Benchmark]:
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        concurrency=concurrency,
        priority=3,
        task_ids=task_ids,
        sandbox_provider="daytona",
        sandbox_provider_secret_name="provider-secret",
        properties=_RESOURCES,
    )
    return request, _persist_benchmark(request, session, queued_pool_id=queue_pool_id(pool_id))


def _context(request: StartBenchmarkRequest, benchmark: Benchmark, task_ids: list[str]) -> dict[str, Any]:
    return ManagedExecutionContext(
        version=3,
        benchmark_id=benchmark.id,
        verified_task_ids=task_ids,
        start_benchmark_request=RunExecutionRequest.model_validate(request.model_dump(mode="python")),
    ).model_dump(mode="json")


def _add_tasks(session: Session, benchmark: Benchmark, statuses: dict[str, TaskStatus]) -> None:
    session.add_all(
        [
            Task(
                org_id=TEST_ORG_ID,
                task_id=task_id,
                benchmark=benchmark.id,
                status=status,
                eval_resume_state={"cursor": task_id} if status == TaskStatus.EVALUATING else None,
            )
            for task_id, status in statuses.items()
        ]
    )
    session.commit()


def _finish_task(session: Session, task_row: TaskState) -> dict[str, Any]:
    result = {"status": "success", "score": 1.0}
    with Session(bind=session.bind) as worker_session:
        task = worker_session.get(Task, task_row.id)
        assert task is not None
        task.status = TaskStatus.FINISHED
        worker_session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, instance_id=None, result=result))
        worker_session.commit()
    return result


@pytest.mark.description("Legacy queued runs retry orphan reservation recovery until pending work can finish.")
@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_coordinator_retries_abandoned_reservation_recovery(
    contract: AgentContractRequest,
    control_postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Any,
) -> None:
    request, benchmark = _queued_run(contract, control_postgres_session, ["recover"], pool_id="retry-recovery")
    _add_tasks(control_postgres_session, benchmark, {"recover": TaskStatus.PENDING})
    task = control_postgres_session.exec(select(Task).where(Task.benchmark == benchmark.id)).one()
    control_postgres_session.add(
        SandboxBuildReservation(
            task_row_id=task.id,
            attempt_started_at=task.started_at,
            pool_id=queue_pool_id("retry-recovery"),
            target_id="region-a",
            sandbox_class="sysbox",
            requested_vcpu=1,
            requested_memory=1,
            requested_disk=1,
            requested_gpu=0,
        )
    )
    control_postgres_session.commit()
    recoveries = 0

    async def recover(_context: object) -> None:
        nonlocal recoveries
        recoveries += 1
        if recoveries == 2:
            with Session(control_postgres_session.bind) as session:
                reservation = session.get(SandboxBuildReservation, (task.id, task.started_at))
                assert reservation is not None
                session.delete(reservation)
                session.commit()

    async def process(task_row: TaskState, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
        return {task_row.task_id: _finish_task(control_postgres_session, task_row)}

    provider = Mock(spec=SandboxProvider, admission_pool_id="retry-recovery")
    monkeypatch.setattr("tracker.services.runs.start._POOL_RECOVERY_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr("tracker.services.runs.start.recover_queued_pool", recover)
    monkeypatch.setattr("tracker.services.runs.start.process_task", process)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=provider))
    authority_kwargs = bound_controls(benchmark, session=control_postgres_session)

    await asyncio.wait_for(
        process_benchmark(execution_context_json=_context(request, benchmark, ["recover"]), **authority_kwargs),
        timeout=4,
    )

    control_postgres_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.FINISHED
    assert recoveries >= 2


@pytest.mark.usefixtures("process_benchmark_env")
@pytest.mark.parametrize("reserved_oldest", [False, True])
async def test_queued_runner_limits_evaluations_but_prepares_the_backlog(
    contract: AgentContractRequest,
    control_postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Any,
    reserved_oldest: bool,
) -> None:
    task_ids = ["evaluation_0", "evaluation_1", "evaluation_2", "pending_0", "pending_1"]
    provider_pool_id = "coordinator-pool"
    request, benchmark = _queued_run(contract, control_postgres_session, task_ids, concurrency=3)
    _add_tasks(
        control_postgres_session,
        benchmark,
        {
            **dict.fromkeys(task_ids[:3], TaskStatus.EVALUATING),
            **dict.fromkeys(task_ids[3:], TaskStatus.PENDING),
            "existing_active": TaskStatus.IN_PROGRESS,
        },
    )
    oldest_pending = control_postgres_session.exec(
        select(Task).where(col(Task.task_id).in_(task_ids[3:])).order_by(col(Task.started_at), col(Task.id))
    ).first()
    assert oldest_pending is not None
    if reserved_oldest:
        control_postgres_session.add(
            SandboxBuildReservation(
                task_row_id=oldest_pending.id,
                attempt_started_at=oldest_pending.started_at - timedelta(seconds=1),
                pool_id=queue_pool_id(provider_pool_id),
                reserved_at=oldest_pending.started_at - timedelta(seconds=1),
                accepted_at=oldest_pending.started_at - timedelta(seconds=1),
                target_id="region-a",
                sandbox_class="sysbox",
                requested_vcpu=1,
                requested_memory=1,
                requested_disk=1,
                requested_gpu=0,
            )
        )
        control_postgres_session.commit()
    evaluation_started: list[str] = []
    evaluation_finished: asyncio.Queue[str] = asyncio.Queue()
    release_evaluation = {task_id: asyncio.Event() for task_id in task_ids[:3]}
    two_evaluations_started = asyncio.Event()
    third_evaluation_started = asyncio.Event()
    pending_started: list[str] = []
    pending_contender_started = asyncio.Event()
    both_pending_contenders_started = asyncio.Event()
    hold_pending = asyncio.Event()
    coordinator_polls: asyncio.Queue[None] = asyncio.Queue()
    continue_coordinator: asyncio.Queue[None] = asyncio.Queue()
    original_sleep = asyncio.sleep

    def set_concurrency(value: int) -> None:
        with Session(bind=control_postgres_session.bind) as session:
            persisted_benchmark = session.get(Benchmark, benchmark.id)
            assert persisted_benchmark is not None
            persisted_benchmark.arguments = persisted_benchmark.arguments.model_copy(update={"concurrency": value})
            session.commit()

    async def process_task(task_row: TaskState, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, Any] | None]:
        if task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is not None:
            evaluation_started.append(task_row.task_id)
            if len(evaluation_started) == 2:
                two_evaluations_started.set()
            elif len(evaluation_started) == 3:
                third_evaluation_started.set()
            await release_evaluation[task_row.task_id].wait()
            result = _finish_task(control_postgres_session, task_row)
            evaluation_finished.put_nowait(task_row.task_id)
            return {task_row.task_id: result}

        assert task_row.status == TaskStatus.PENDING
        pending_started.append(task_row.task_id)
        pending_contender_started.set()
        if len(pending_started) == 2:
            both_pending_contenders_started.set()
        await hold_pending.wait()

        return {task_row.task_id: None}

    async def controlled_sleep(seconds: float) -> None:
        if seconds != 1.0:
            await original_sleep(0)

            return
        coordinator_polls.put_nowait(None)
        await continue_coordinator.get()

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    monkeypatch.setattr("tracker.services.runs.start.process_task", process_task)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=sandbox_provider))
    monkeypatch.setattr("tracker.services.runs.start.asyncio.sleep", controlled_sleep)
    authority_kwargs = bound_controls(benchmark, session=control_postgres_session)
    run = asyncio.create_task(
        process_benchmark(execution_context_json=_context(request, benchmark, task_ids), **authority_kwargs)
    )
    try:
        await coordinator_polls.get()
        await asyncio.wait_for(two_evaluations_started.wait(), timeout=2)

        assert len(evaluation_started) == 2
        assert pending_started == []

        await anyio.to_thread.run_sync(set_concurrency, 2)
        first_evaluation = evaluation_started[0]
        release_evaluation[first_evaluation].set()
        await evaluation_finished.get()
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()

        assert len(evaluation_started) == 2
        assert pending_started == []

        await anyio.to_thread.run_sync(set_concurrency, 3)
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()
        await asyncio.wait_for(third_evaluation_started.wait(), timeout=2)

        assert len(evaluation_started) == 3
        assert pending_started == []

        for task_id in evaluation_started[1:]:
            release_evaluation[task_id].set()
            await evaluation_finished.get()
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()
        await asyncio.wait_for(pending_contender_started.wait(), timeout=2)
        if not reserved_oldest:
            await asyncio.wait_for(both_pending_contenders_started.wait(), timeout=2)

        assert len(pending_started) == (1 if reserved_oldest else 2)
        if not reserved_oldest:
            assert pending_started[0] == oldest_pending.task_id
            assert set(pending_started) == set(task_ids[3:])
        if reserved_oldest:
            assert oldest_pending.task_id not in pending_started
            assert (
                control_postgres_session.get(
                    SandboxBuildReservation, (oldest_pending.id, oldest_pending.started_at - timedelta(seconds=1))
                )
                is not None
            )

        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()

        assert len(pending_started) == (1 if reserved_oldest else 2)
    finally:
        run.cancel()
        with suppress(asyncio.CancelledError):
            await run


@pytest.mark.description("Reserved queues prepare the full backlog while legacy queues bound pending contenders.")
@pytest.mark.usefixtures("process_benchmark_env")
@pytest.mark.parametrize("reserved", [False, True], ids=["legacy-bounded-contenders", "reserved-full-backlog"])
async def test_queued_coordinator_prepares_backlog_only_for_reserved_queues(
    contract: AgentContractRequest,
    control_postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Any,
    reserved: bool,
) -> None:
    task_ids = [f"pending_{index}" for index in range(15)]
    request, benchmark = _queued_run(contract, control_postgres_session, task_ids, concurrency=20)
    if reserved:
        benchmark.arguments = benchmark.arguments.model_copy(
            update={"queue_pool_id": reserved_queue_pool_id(queue_pool_id("coordinator-pool"))}
        )
        control_postgres_session.add(benchmark)
        control_postgres_session.commit()
    _add_tasks(control_postgres_session, benchmark, dict.fromkeys(task_ids, TaskStatus.PENDING))
    pending_started: list[str] = []
    started_count = {10: asyncio.Event(), 13: asyncio.Event(), 15: asyncio.Event()}
    coordinator_polls: asyncio.Queue[None] = asyncio.Queue()
    continue_coordinator: asyncio.Queue[None] = asyncio.Queue()
    original_sleep = asyncio.sleep

    async def process_task(task_row: TaskState, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, Any] | None]:
        pending_started.append(task_row.task_id)
        if (reached := started_count.get(len(pending_started))) is not None:
            reached.set()
        await asyncio.Event().wait()
        return {task_row.task_id: None}

    async def controlled_sleep(seconds: float) -> None:
        if seconds != 1.0:
            await original_sleep(0)
            return
        coordinator_polls.put_nowait(None)
        await continue_coordinator.get()

    def set_status(task_ids_to_update: list[str], status: TaskStatus) -> None:
        with Session(bind=control_postgres_session.bind) as session:
            for task in session.exec(select(Task).where(col(Task.task_id).in_(task_ids_to_update))).all():
                task.status = status
            session.commit()

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id="coordinator-pool")
    monkeypatch.setattr("tracker.services.runs.start.process_task", process_task)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=sandbox_provider))
    monkeypatch.setattr("tracker.services.runs.start.asyncio.sleep", controlled_sleep)
    monkeypatch.setattr("tracker.services.runs.start._SANDBOX_CREATION_CAP", 10)
    authority_kwargs = bound_controls(benchmark, session=control_postgres_session)
    run = asyncio.create_task(
        process_benchmark(execution_context_json=_context(request, benchmark, task_ids), **authority_kwargs)
    )
    try:
        await asyncio.wait_for(coordinator_polls.get(), timeout=30 if reserved else 2)
        if reserved:
            await asyncio.wait_for(started_count[15].wait(), timeout=10)
            assert pending_started == task_ids
            return
        await asyncio.wait_for(started_count[10].wait(), timeout=2)
        await original_sleep(0)
        assert pending_started == task_ids[:10]

        continue_coordinator.put_nowait(None)
        await asyncio.wait_for(coordinator_polls.get(), timeout=2)
        await original_sleep(0)
        assert pending_started == task_ids[:10]

        set_status(task_ids[:3], TaskStatus.BUILDING)
        continue_coordinator.put_nowait(None)
        await asyncio.wait_for(coordinator_polls.get(), timeout=2)
        await asyncio.wait_for(started_count[13].wait(), timeout=2)
        await original_sleep(0)
        assert pending_started == task_ids[:13]
    finally:
        run.cancel()
        with suppress(asyncio.CancelledError):
            await run


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_runner_prepares_backlog_independently_of_run_slots(
    contract: AgentContractRequest,
    control_postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Any,
) -> None:
    task_ids = [f"pending_{index}" for index in range(15)]
    request, benchmark = _queued_run(contract, control_postgres_session, task_ids, concurrency=1)
    benchmark.arguments = benchmark.arguments.model_copy(
        update={"queue_pool_id": reserved_queue_pool_id(queue_pool_id("coordinator-pool"))}
    )
    control_postgres_session.add(benchmark)
    control_postgres_session.commit()
    _add_tasks(control_postgres_session, benchmark, dict.fromkeys(task_ids, TaskStatus.PENDING))
    pending_started: list[str] = []
    all_preparing = asyncio.Event()

    async def process_task(task_row: TaskState, *_args: object, **_kwargs: object) -> dict[str, None]:
        pending_started.append(task_row.task_id)
        if len(pending_started) == len(task_ids):
            all_preparing.set()
        await asyncio.Event().wait()
        return {task_row.task_id: None}

    provider = Mock(spec=SandboxProvider, admission_pool_id="coordinator-pool")
    monkeypatch.setattr("tracker.services.runs.start.process_task", process_task)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=provider))
    authority_kwargs = bound_controls(benchmark, session=control_postgres_session)
    run = asyncio.create_task(
        process_benchmark(execution_context_json=_context(request, benchmark, task_ids), **authority_kwargs)
    )
    try:
        await asyncio.wait_for(all_preparing.wait(), timeout=2)
        assert pending_started == task_ids
    finally:
        run.cancel()
        with suppress(asyncio.CancelledError):
            await run


@pytest.mark.usefixtures("process_benchmark_env")
@pytest.mark.description("Tasks launched by a run log with the run's sandbox provider type as `sandbox_provider`.")
async def test_process_benchmark_binds_sandbox_provider_for_tasks(
    contract: AgentContractRequest,
    control_postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Any,
) -> None:
    task_ids = ["pending_0"]
    request, benchmark = _queued_run(contract, control_postgres_session, task_ids, concurrency=1)
    _add_tasks(control_postgres_session, benchmark, dict.fromkeys(task_ids, TaskStatus.PENDING))
    task_providers: list[str] = []
    task_started = asyncio.Event()

    async def process_task(task_row: Task, *_args: object, **_kwargs: object) -> dict[str, None]:
        task_providers.append(sandbox_provider_var.get())
        task_started.set()
        await asyncio.Event().wait()
        return {task_row.task_id: None}

    provider = Mock(spec=SandboxProvider, admission_pool_id="coordinator-pool")
    monkeypatch.setattr("tracker.services.runs.start.process_task", process_task)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=provider))
    authority_kwargs = bound_controls(benchmark, session=control_postgres_session)
    run = asyncio.create_task(
        process_benchmark(execution_context_json=_context(request, benchmark, task_ids), **authority_kwargs)
    )
    try:
        await asyncio.wait_for(task_started.wait(), timeout=2)
        assert task_providers == ["daytona"]
    finally:
        run.cancel()
        with suppress(asyncio.CancelledError):
            await run
    assert sandbox_provider_var.get() == ""


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_process_benchmark_observes_recovered_work_before_finalizing(
    contract: AgentContractRequest,
    control_postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Any,
) -> None:
    task_ids = ["resume_eval", "recover_build"]
    provider_pool_id = "evaluation-pool"
    request, benchmark = _queued_run(
        contract, control_postgres_session, task_ids, pool_id=provider_pool_id, concurrency=2
    )
    _add_tasks(
        control_postgres_session,
        benchmark,
        {
            "resume_eval": TaskStatus.EVALUATING,
            "recover_build": TaskStatus.BUILDING,
        },
    )
    initial_statuses: dict[str, TaskStatus] = {}
    original_resume_started_at = control_postgres_session.exec(
        select(Task.started_at).where(Task.benchmark == benchmark.id).where(Task.task_id == "resume_eval")
    ).one()
    resumed_at: datetime | None = None
    evaluation_started = asyncio.Event()

    async def recover_in_scheduler() -> None:
        await evaluation_started.wait()
        with Session(bind=control_postgres_session.bind) as session:
            task = session.exec(
                select(Task).where(Task.benchmark == benchmark.id).where(Task.task_id == "recover_build")
            ).one()
            if task.status == TaskStatus.BUILDING:
                task.status = TaskStatus.PENDING
                session.commit()

    async def process_task(task_row: TaskState, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
        nonlocal resumed_at
        initial_statuses[task_row.task_id] = task_row.status
        if task_row.task_id == "resume_eval":
            resumed_at = task_row.started_at
            evaluation_started.set()
        result = _finish_task(control_postgres_session, task_row)
        return {task_row.task_id: result}

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    monkeypatch.setattr("tracker.services.runs.start.process_task", process_task)
    monkeypatch.setattr(
        DaytonaProviderConfig,
        "create_provider",
        Mock(return_value=sandbox_provider),
    )

    authority_kwargs = bound_controls(benchmark, session=control_postgres_session)
    dispatch = control_postgres_session.get(ExecutorDispatch, UUID(str(authority_kwargs["executor_dispatch_id"])))
    assert dispatch is not None
    resumed_task = control_postgres_session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.task_id == "resume_eval")
    ).one()
    resumed_task.started_at = dispatch.created_at
    control_postgres_session.add(resumed_task)
    control_postgres_session.commit()
    recovery = asyncio.create_task(recover_in_scheduler())
    try:
        await asyncio.wait_for(
            process_benchmark(execution_context_json=_context(request, benchmark, task_ids), **authority_kwargs),
            timeout=5,
        )
    finally:
        recovery.cancel()
        await asyncio.gather(recovery, return_exceptions=True)

    control_postgres_session.expire_all()
    control_postgres_session.refresh(benchmark)
    tasks = control_postgres_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
    assert initial_statuses == {
        "resume_eval": TaskStatus.EVALUATING,
        "recover_build": TaskStatus.PENDING,
    }
    assert {task.task_id: task.status for task in tasks} == {
        "resume_eval": TaskStatus.FINISHED,
        "recover_build": TaskStatus.FINISHED,
    }
    assert resumed_at is not None
    assert resumed_at == dispatch.created_at
    assert resumed_at > original_resume_started_at
    assert benchmark.status == BenchmarkStatus.FINISHED


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_coordinator_retires_stale_evaluation_runner(
    contract: AgentContractRequest,
    control_postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Any,
) -> None:
    request, benchmark = _queued_run(
        contract,
        control_postgres_session,
        ["resume_eval"],
        pool_id="stale-evaluation-pool",
        concurrency=1,
    )
    _add_tasks(control_postgres_session, benchmark, {"resume_eval": TaskStatus.EVALUATING})
    calls = 0

    async def process_task(_task_row: TaskState, *_args: Any, **_kwargs: Any) -> dict[str, None]:
        nonlocal calls
        calls += 1
        return {"resume_eval": None}

    provider = Mock(spec=SandboxProvider, admission_pool_id="stale-evaluation-pool")
    monkeypatch.setattr("tracker.services.runs.start.process_task", process_task)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=provider))
    authority_kwargs = bound_controls(benchmark, session=control_postgres_session)
    dispatch = control_postgres_session.get(ExecutorDispatch, UUID(str(authority_kwargs["executor_dispatch_id"])))
    assert dispatch is not None
    task = control_postgres_session.exec(select(Task).where(Task.benchmark == benchmark.id)).one()
    task.started_at = dispatch.created_at
    control_postgres_session.add(task)
    control_postgres_session.commit()

    await asyncio.wait_for(
        process_benchmark(execution_context_json=_context(request, benchmark, ["resume_eval"]), **authority_kwargs),
        timeout=2,
    )

    control_postgres_session.expire_all()
    persisted_task = control_postgres_session.get(Task, task.id)
    assert persisted_task is not None
    assert persisted_task.status == TaskStatus.EVALUATING
    assert calls == 1


@pytest.mark.usefixtures("process_benchmark_env")
@pytest.mark.parametrize("close_fails", [False, True])
async def test_direct_provider_setup_failure_closes_client(
    contract: AgentContractRequest,
    control_postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Any,
    close_fails: bool,
) -> None:
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        task_ids=["task_0"],
        sandbox_provider="daytona",
        sandbox_provider_secret_name="provider-secret",
        properties=_RESOURCES,
    )
    benchmark = _persist_benchmark(request, control_postgres_session)
    close_client = AsyncMock(side_effect=RuntimeError("client close failed") if close_fails else None)
    monkeypatch.setattr(
        DaytonaProviderConfig, "create_provider", Mock(side_effect=RuntimeError("provider setup failed"))
    )
    monkeypatch.setattr(BenchmarkServiceClient, "close", close_client)
    authority_kwargs = bound_controls(benchmark, session=control_postgres_session)

    if close_fails:
        with pytest.raises(RuntimeError, match="client close failed"):
            await process_benchmark(execution_context_json=_context(request, benchmark, ["task_0"]), **authority_kwargs)
    else:
        await process_benchmark(execution_context_json=_context(request, benchmark, ["task_0"]), **authority_kwargs)

    control_postgres_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert benchmark.error_message is not None
    assert "provider setup failed" in benchmark.error_message
    close_client.assert_awaited_once_with()


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_cancellation_errors_owned_work_and_preserves_unassigned_pending_work(
    contract: AgentContractRequest,
    control_postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Any,
) -> None:
    task_ids = ["active", "pending"]
    request, benchmark = _queued_run(contract, control_postgres_session, task_ids, concurrency=1)
    _add_tasks(control_postgres_session, benchmark, {"pending": TaskStatus.PENDING})
    task_started = asyncio.Event()
    started_task_id: str | None = None

    async def process_task(task_row: TaskState, *_args: Any, **_kwargs: Any) -> dict[str, None]:
        nonlocal started_task_id
        with Session(bind=control_postgres_session.bind) as session:
            task = session.get(Task, task_row.id)
            assert task is not None
            task.status = TaskStatus.IN_PROGRESS
            session.commit()
        started_task_id = task_row.task_id
        task_started.set()
        await asyncio.Event().wait()
        return {task_row.task_id: None}

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id="coordinator-pool")
    monkeypatch.setattr("tracker.services.runs.start.process_task", process_task)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=sandbox_provider))

    authority_kwargs = bound_controls(benchmark, session=control_postgres_session)
    run = asyncio.create_task(
        process_benchmark(execution_context_json=_context(request, benchmark, ["active"]), **authority_kwargs)
    )
    await asyncio.wait_for(task_started.wait(), timeout=2)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    control_postgres_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert benchmark.error_message is None
    task_statuses = {
        task.task_id: task.status
        for task in control_postgres_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
    }
    assert started_task_id is not None
    assert task_statuses[started_task_id] == TaskStatus.ERROR
    assert list(task_statuses.values()).count(TaskStatus.PENDING) == 1
    error_rows = control_postgres_session.exec(
        select(ErrorResult).join(Task, col(Task.id) == ErrorResult.task).where(Task.benchmark == benchmark.id)
    ).all()
    assert [(row.error_message, row.category) for row in error_rows] == [
        ("Run was interrupted", FailureCategory.CANCELLED)
    ]


@pytest.mark.description("Repeated caller cancellation does not interrupt provider shutdown.")
@pytest.mark.usefixtures("process_benchmark_env")
async def test_cancelled_shutdown_drains_provider(
    contract: AgentContractRequest,
    control_postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Any,
) -> None:
    started, release = asyncio.Event(), asyncio.Event()
    finished = asyncio.Event()

    async def close() -> None:
        started.set()
        await release.wait()
        finished.set()

    request, benchmark = _queued_run(contract, control_postgres_session, ["task_0"], pool_id="expected-pool")
    provider = Mock(spec=SandboxProvider, admission_pool_id="different-pool", close=AsyncMock(side_effect=close))
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=provider))

    authority_kwargs = bound_controls(benchmark, session=control_postgres_session)
    closing = asyncio.create_task(
        process_benchmark(execution_context_json=_context(request, benchmark, ["task_0"]), **authority_kwargs)
    )
    await asyncio.wait_for(started.wait(), timeout=2)
    for _ in range(2):
        closing.cancel()
        await asyncio.sleep(0)
        assert not closing.done()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert finished.is_set()
    provider.close.assert_awaited_once()


@pytest.mark.usefixtures("process_benchmark_env")
@pytest.mark.parametrize("provider_pool_id", [None, "different-pool"])
async def test_queued_process_benchmark_reports_provider_configuration_drift(
    contract: AgentContractRequest,
    control_postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    provider_pool_id: str | None,
    bound_controls: Any,
) -> None:
    task_ids = ["task_0", "task_1"]
    request, benchmark = _queued_run(contract, control_postgres_session, task_ids, pool_id="expected-pool")
    _add_tasks(
        control_postgres_session,
        benchmark,
        {
            "task_0": TaskStatus.BUILDING,
            "task_1": TaskStatus.PENDING,
            "task_unrelated": TaskStatus.PENDING,
        },
    )
    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=sandbox_provider))

    authority_kwargs = bound_controls(benchmark, session=control_postgres_session)
    await process_benchmark(execution_context_json=_context(request, benchmark, task_ids), **authority_kwargs)

    control_postgres_session.refresh(benchmark)
    tasks = control_postgres_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
    task_statuses = {task.task_id: task.status for task in tasks}
    assert benchmark.status == BenchmarkStatus.ERROR
    expected_error = (
        "Sandbox provider configuration is unavailable"
        if provider_pool_id is None
        else "Configured sandbox provider does not match the run's queued provider pool"
    )
    assert benchmark.error_message is not None and expected_error in benchmark.error_message
    assert task_statuses == {
        "task_unrelated": TaskStatus.PENDING,
        "task_0": TaskStatus.ERROR,
        "task_1": TaskStatus.ERROR,
    }
