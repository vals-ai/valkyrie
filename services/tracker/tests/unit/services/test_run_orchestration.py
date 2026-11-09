import asyncio
from contextlib import suppress
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from benchmark_service import SandboxProvider
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.client import BenchmarkServiceClient
from sqlmodel import Session, col, select

from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    ExecutorDispatch,
    FailureCategory,
    Org,
    SandboxBuildReservation,
    Task,
    TaskStatus,
)
from tracker.scheduler.admission import SandboxQueueContext
from tracker.scheduler.store import queue_pool_id, reserved_queue_pool_id
from tracker.aws.runtime import AWSResources
from tracker.types import ManagedExecutionContext, RunExecutionRequest, StartBenchmarkRequest
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
    org = Org(id=TEST_ORG_ID, name="default")
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
    return request, _persist_benchmark(request, session, queued_pool_id=reserved_queue_pool_id(queue_pool_id(pool_id)))


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


def _finish_task(session: Session, task_row: Task) -> dict[str, Any]:
    result = {"status": "success", "score": 1.0}
    with Session(bind=session.bind) as worker_session:
        task = worker_session.get(Task, task_row.id)
        assert task is not None
        task.status = TaskStatus.FINISHED
        worker_session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, instance_id=None, result=result))
        worker_session.commit()
    return result


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_runner_wakes_from_shared_grant_in_run_state_refresh(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    request, benchmark = _queued_run(contract, database_session, ["granted"], pool_id="shared-grant")
    _add_tasks(database_session, benchmark, {"granted": TaskStatus.PENDING})
    task = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).one()
    waiting = asyncio.Event()
    observed_grant = False

    async def grant() -> None:
        await waiting.wait()
        with Session(database_session.bind) as session:
            row = session.get(Task, task.id)
            assert row is not None
            row.status = TaskStatus.BUILDING
            session.commit()

    async def process(
        task_row: Task, *_args: object, queue_context: SandboxQueueContext, **_kwargs: object
    ) -> dict[str, dict[str, object]]:
        nonlocal observed_grant
        waiting.set()
        observed_grant = (
            await queue_context.admission_signals.wait(task_row.id, task_row.started_at, ready=True) is True
        )
        return {task_row.task_id: _finish_task(database_session, task_row)}

    provider = Mock(spec=SandboxProvider, admission_pool_id="shared-grant")
    monkeypatch.setattr("tracker.services.runs.start.process_task", process)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=provider))
    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
    grant_task = asyncio.create_task(grant())
    try:
        await asyncio.wait_for(
            process_benchmark(execution_context_json=_context(request, benchmark, ["granted"]), **authority_kwargs),
            timeout=4,
        )
    finally:
        grant_task.cancel()
        await asyncio.gather(grant_task, return_exceptions=True)
    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.FINISHED
    assert observed_grant


@pytest.mark.usefixtures("process_benchmark_env")
@pytest.mark.parametrize("reserved_oldest", [False, True])
async def test_queued_runner_limits_evaluations_but_prepares_the_backlog(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
    reserved_oldest: bool,
) -> None:
    task_ids = ["evaluation_0", "evaluation_1", "evaluation_2", "pending_0", "pending_1"]
    provider_pool_id = "coordinator-pool"
    request, benchmark = _queued_run(contract, database_session, task_ids, concurrency=3)
    _add_tasks(
        database_session,
        benchmark,
        {
            **dict.fromkeys(task_ids[:3], TaskStatus.EVALUATING),
            **dict.fromkeys(task_ids[3:], TaskStatus.PENDING),
            "existing_active": TaskStatus.IN_PROGRESS,
        },
    )
    oldest_pending = database_session.exec(
        select(Task).where(col(Task.task_id).in_(task_ids[3:])).order_by(col(Task.started_at), col(Task.id))
    ).first()
    assert oldest_pending is not None
    if reserved_oldest:
        database_session.add(
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
        database_session.commit()
    evaluation_started: list[str] = []
    evaluation_finished: asyncio.Queue[str] = asyncio.Queue()
    release_evaluation = {task_id: asyncio.Event() for task_id in task_ids[:3]}
    two_evaluations_started = asyncio.Event()
    third_evaluation_started = asyncio.Event()
    pending_started: list[str] = []
    expected_pending = 1 if reserved_oldest else 2
    pending_contender_started = asyncio.Event()
    hold_pending = asyncio.Event()
    coordinator_polls: asyncio.Queue[None] = asyncio.Queue()
    continue_coordinator: asyncio.Queue[None] = asyncio.Queue()
    original_sleep = asyncio.sleep

    def set_concurrency(value: int) -> None:
        with Session(bind=database_session.bind) as session:
            persisted_benchmark = session.get(Benchmark, benchmark.id)
            assert persisted_benchmark is not None
            persisted_benchmark.arguments = persisted_benchmark.arguments.model_copy(update={"concurrency": value})
            session.commit()

    async def process_task(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, Any] | None]:
        if task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is not None:
            evaluation_started.append(task_row.task_id)
            if len(evaluation_started) == 2:
                two_evaluations_started.set()
            elif len(evaluation_started) == 3:
                third_evaluation_started.set()
            await release_evaluation[task_row.task_id].wait()
            result = _finish_task(database_session, task_row)
            evaluation_finished.put_nowait(task_row.task_id)
            return {task_row.task_id: result}

        assert task_row.status == TaskStatus.PENDING
        pending_started.append(task_row.task_id)
        if len(pending_started) == expected_pending:
            pending_contender_started.set()
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
    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
    run = asyncio.create_task(
        process_benchmark(execution_context_json=_context(request, benchmark, task_ids), **authority_kwargs)
    )
    try:
        await coordinator_polls.get()
        await asyncio.wait_for(two_evaluations_started.wait(), timeout=2)
        await asyncio.wait_for(pending_contender_started.wait(), timeout=2)

        assert len(evaluation_started) == 2
        assert len(pending_started) == expected_pending

        set_concurrency(2)
        first_evaluation = evaluation_started[0]
        release_evaluation[first_evaluation].set()
        await evaluation_finished.get()
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()

        assert len(evaluation_started) == 2
        assert len(pending_started) == expected_pending

        set_concurrency(3)
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()
        await asyncio.wait_for(third_evaluation_started.wait(), timeout=2)

        assert len(evaluation_started) == 3
        assert len(pending_started) == expected_pending

        for task_id in evaluation_started[1:]:
            release_evaluation[task_id].set()
            await evaluation_finished.get()
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()
        await asyncio.wait_for(pending_contender_started.wait(), timeout=2)

        assert len(pending_started) == (1 if reserved_oldest else 2)
        if reserved_oldest:
            assert oldest_pending.task_id not in pending_started
            assert (
                database_session.get(
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


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_runner_prepares_backlog_independently_of_run_slots(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    task_ids = [f"pending_{index}" for index in range(15)]
    request, benchmark = _queued_run(contract, database_session, task_ids, concurrency=1)
    _add_tasks(database_session, benchmark, dict.fromkeys(task_ids, TaskStatus.PENDING))
    pending_started: list[str] = []
    all_preparing = asyncio.Event()

    async def process_task(task_row: Task, *_args: object, **_kwargs: object) -> dict[str, None]:
        pending_started.append(task_row.task_id)
        if len(pending_started) == len(task_ids):
            all_preparing.set()
        await asyncio.Event().wait()
        return {task_row.task_id: None}

    provider = Mock(spec=SandboxProvider, admission_pool_id="coordinator-pool")
    monkeypatch.setattr("tracker.services.runs.start.process_task", process_task)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=provider))
    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
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
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    task_ids = ["pending_0"]
    request, benchmark = _queued_run(contract, database_session, task_ids, concurrency=1)
    _add_tasks(database_session, benchmark, dict.fromkeys(task_ids, TaskStatus.PENDING))
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
    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
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
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    task_ids = ["resume_eval", "recover_build"]
    provider_pool_id = "evaluation-pool"
    request, benchmark = _queued_run(contract, database_session, task_ids, pool_id=provider_pool_id, concurrency=2)
    _add_tasks(
        database_session,
        benchmark,
        {
            "resume_eval": TaskStatus.EVALUATING,
            "recover_build": TaskStatus.BUILDING,
        },
    )
    initial_statuses: dict[str, TaskStatus] = {}
    original_resume_started_at = database_session.exec(
        select(Task.started_at).where(Task.benchmark == benchmark.id).where(Task.task_id == "resume_eval")
    ).one()
    resumed_at: datetime | None = None
    evaluation_started = asyncio.Event()

    async def recover_in_scheduler() -> None:
        await evaluation_started.wait()
        with Session(bind=database_session.bind) as session:
            task = session.exec(
                select(Task).where(Task.benchmark == benchmark.id).where(Task.task_id == "recover_build")
            ).one()
            if task.status == TaskStatus.BUILDING:
                task.status = TaskStatus.PENDING
                session.commit()

    async def process_task(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
        nonlocal resumed_at
        initial_statuses[task_row.task_id] = task_row.status
        if task_row.task_id == "resume_eval":
            resumed_at = task_row.started_at
            evaluation_started.set()
        result = _finish_task(database_session, task_row)
        return {task_row.task_id: result}

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    monkeypatch.setattr("tracker.services.runs.start.process_task", process_task)
    monkeypatch.setattr(
        DaytonaProviderConfig,
        "create_provider",
        Mock(return_value=sandbox_provider),
    )

    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
    dispatch = database_session.get(ExecutorDispatch, UUID(str(authority_kwargs["executor_dispatch_id"])))
    assert dispatch is not None
    resumed_task = database_session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.task_id == "resume_eval")
    ).one()
    resumed_task.started_at = dispatch.created_at
    database_session.add(resumed_task)
    database_session.commit()
    recovery = asyncio.create_task(recover_in_scheduler())
    try:
        await asyncio.wait_for(
            process_benchmark(execution_context_json=_context(request, benchmark, task_ids), **authority_kwargs),
            timeout=5,
        )
    finally:
        recovery.cancel()
        await asyncio.gather(recovery, return_exceptions=True)

    database_session.expire_all()
    database_session.refresh(benchmark)
    tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
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
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    request, benchmark = _queued_run(
        contract,
        database_session,
        ["resume_eval"],
        pool_id="stale-evaluation-pool",
        concurrency=1,
    )
    _add_tasks(database_session, benchmark, {"resume_eval": TaskStatus.EVALUATING})
    calls = 0

    async def process_task(_task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, None]:
        nonlocal calls
        calls += 1
        return {"resume_eval": None}

    provider = Mock(spec=SandboxProvider, admission_pool_id="stale-evaluation-pool")
    monkeypatch.setattr("tracker.services.runs.start.process_task", process_task)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=provider))
    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
    dispatch = database_session.get(ExecutorDispatch, UUID(str(authority_kwargs["executor_dispatch_id"])))
    assert dispatch is not None
    task = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).one()
    task.started_at = dispatch.created_at
    database_session.add(task)
    database_session.commit()

    await asyncio.wait_for(
        process_benchmark(execution_context_json=_context(request, benchmark, ["resume_eval"]), **authority_kwargs),
        timeout=2,
    )

    database_session.expire_all()
    persisted_task = database_session.get(Task, task.id)
    assert persisted_task is not None
    assert persisted_task.status == TaskStatus.EVALUATING
    assert calls == 1


@pytest.mark.usefixtures("process_benchmark_env")
@pytest.mark.parametrize("close_fails", [False, True])
async def test_direct_provider_setup_failure_closes_client(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
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
    benchmark = _persist_benchmark(request, database_session)
    close_client = AsyncMock(side_effect=RuntimeError("client close failed") if close_fails else None)
    monkeypatch.setattr(
        DaytonaProviderConfig, "create_provider", Mock(side_effect=RuntimeError("provider setup failed"))
    )
    monkeypatch.setattr(BenchmarkServiceClient, "close", close_client)
    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)

    if close_fails:
        with pytest.raises(RuntimeError, match="client close failed"):
            await process_benchmark(execution_context_json=_context(request, benchmark, ["task_0"]), **authority_kwargs)
    else:
        await process_benchmark(execution_context_json=_context(request, benchmark, ["task_0"]), **authority_kwargs)

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert benchmark.error_message is not None
    assert "provider setup failed" in benchmark.error_message
    close_client.assert_awaited_once_with()


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_cancellation_errors_owned_work_and_preserves_unassigned_pending_work(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    task_ids = ["active", "pending"]
    request, benchmark = _queued_run(contract, database_session, task_ids, concurrency=1)
    _add_tasks(database_session, benchmark, dict.fromkeys(task_ids, TaskStatus.PENDING))
    task_started = asyncio.Event()
    started_task_id: str | None = None

    async def process_task(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, None]:
        nonlocal started_task_id
        with Session(bind=database_session.bind) as session:
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

    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
    run = asyncio.create_task(
        process_benchmark(execution_context_json=_context(request, benchmark, ["active"]), **authority_kwargs)
    )
    await asyncio.wait_for(task_started.wait(), timeout=2)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert benchmark.error_message is None
    task_statuses = {
        task.task_id: task.status
        for task in database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
    }
    assert started_task_id is not None
    assert task_statuses[started_task_id] == TaskStatus.ERROR
    assert list(task_statuses.values()).count(TaskStatus.PENDING) == 1
    error_rows = database_session.exec(
        select(ErrorResult).join(Task, col(Task.id) == ErrorResult.task).where(Task.benchmark == benchmark.id)
    ).all()
    assert [(row.error_message, row.category) for row in error_rows] == [
        ("Run was interrupted", FailureCategory.CANCELLED)
    ]


@pytest.mark.description("Repeated caller cancellation does not interrupt provider shutdown.")
@pytest.mark.usefixtures("process_benchmark_env")
async def test_cancelled_shutdown_drains_provider(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    started, release = asyncio.Event(), asyncio.Event()
    finished = asyncio.Event()

    async def close() -> None:
        started.set()
        await release.wait()
        finished.set()

    request, benchmark = _queued_run(contract, database_session, ["task_0"], pool_id="expected-pool")
    provider = Mock(spec=SandboxProvider, admission_pool_id="different-pool", close=AsyncMock(side_effect=close))
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=provider))

    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
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
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    provider_pool_id: str | None,
    executor_authority_kwargs: Any,
) -> None:
    task_ids = ["task_0", "task_1"]
    request, benchmark = _queued_run(contract, database_session, task_ids, pool_id="expected-pool")
    _add_tasks(
        database_session,
        benchmark,
        {
            "task_0": TaskStatus.BUILDING,
            "task_1": TaskStatus.PENDING,
            "task_unrelated": TaskStatus.PENDING,
        },
    )
    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", Mock(return_value=sandbox_provider))

    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
    await process_benchmark(execution_context_json=_context(request, benchmark, task_ids), **authority_kwargs)

    database_session.refresh(benchmark)
    tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
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
