import base64
import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import types
import threading
from asyncio import Semaphore
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

from alembic.config import Config
from alembic.script import ScriptDirectory
import boto3
import httpx
from benchmark_service import (
    DaytonaProviderConfig,
    ImageSource,
    ResourceCapacity,
    Resources,
    Sandbox,
    SandboxCapacity,
    SandboxCapacityDomain,
    SandboxReservationDemand,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxProviderConfig,
    SandboxSource,
    SnapshotSource,
    TargetedSnapshotSource,
)
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.base import BenchmarkService
from benchmark_service.grading_credentials import current_grading_credentials
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.schemas import (
    EvaluateResponseRequest,
    FinalScoreResult,
    RetrieveTaskResponse,
    SetupTaskResponse,
    StreamChunk,
    StreamResultChunk,
    VerifyTaskIdsResponse,
    VersionResponse,
)
from fastapi import HTTPException, Request
import pytest
from sqlalchemy import event, text, update
from sqlalchemy.engine import Connection, Engine, ExecutionContext
from sqlalchemy.exc import DBAPIError
from sqlmodel import Session, col, func, select
from testcontainers.core.container import DockerContainer
import uvicorn

from tests.factories import make_task
from tests.unit.services.task_execution_support import finalize_agent_generation
from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tracker.repositories import advisory_lock
from tracker.repositories.scheduler import SchedulerRepository
from tracker.secret_access import tenant_secret_prefix
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.sandbox_build_reservation import SandboxBuildReservationRepository
from tracker.auth import RequestIdentity
from tracker.aws.runtime import AWSRuntime
from tracker.aws.services import CloudRuntimeFactory
from tracker.runtime.services import RuntimeServices
from tracker.database.models import (
    Benchmark,
    ErrorResult,
    FailurePhase,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorRelease,
    EvaluationResult,
    Org,
    OrgProviderSecret,
    Project,
    RunProject,
    SandboxAdmissionCandidate,
    SandboxBuildReservation,
    SpendType,
    Task,
)
from executor_protocol import ExecutorDispatchStatus
from tracker.types import AgentContractRequest, AWSBenchmarkArguments
from tracker.domain import BenchmarkStatus, ExecutorDispatchKind, RetryMode, TaskStatus
from tracker.exceptions import ExecutionAuthorityRevoked, SandboxSetupError
import tracker.executor.dispatch_control as dispatch_control
from tracker.executor.control_protocol import AttemptRef, PhaseRef
from tracker.executor.control_runs import RunControl
from tracker.executor.control_tasks import SandboxLink, TaskControl, TaskState
from tracker.executor.control_queue import (
    QueueControl,
    QueueBuildResponse,
    is_reserved_queue_pool_id,
    queue_pool_id,
    reserved_queue_pool_id,
    queue_pool_lock_id,
)
from tracker.executor.release_control import promote_release
import tracker.runtime.model_gateway as model_gateway
import tracker.scheduler.admission as admission
import tracker.scheduler.coordinator as coordinator
import tracker.scheduler.store as store
from tracker.sandbox import SandboxCreationState, create_sandbox
from tracker.types import OrgContext, StartBenchmarkRequest
import tracker.services.tasks.execute as task_execution
import tracker.services.runs.stop as lifecycle_control
import tracker.services.runs.retry as retry_control
from tracker.services.benchmarks import update_benchmark_resume_arguments
import main as tracker_main

_ATTEMPT = datetime(2026, 7, 27, 12)
_SOURCE = ImageSource(image="scheduler-test-image")
_RESOURCES = Resources(vcpu=1, memory=2, disk=3)
_DEMAND = SandboxReservationDemand(target_id="region-a", sandbox_class="sysbox", vcpu=1, memory=2, disk=3, gpu=0)
_CAPACITY = SandboxCapacity(
    cpu=ResourceCapacity(total=8, used=0),
    memory=ResourceCapacity(total=16, used=0),
    disk=ResourceCapacity(total=24, used=0),
)


async def _version(*_args: Any, **_kwargs: Any) -> VersionResponse:
    return VersionResponse(framework_version="test")


async def _run_agent_success(
    *_args: Any,
    deadline_controller: Any,
    on_external_service_finalized: Callable[..., Awaitable[None]],
    **_kwargs: Any,
) -> tuple[None, float]:
    await finalize_agent_generation(
        deadline_controller=deadline_controller,
        on_external_service_finalized=on_external_service_finalized,
    )
    return None, 0.0


def _use_managed_runtime(monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime) -> None:
    monkeypatch.setattr(
        tracker_main,
        "resolve_run_aws_runtime",
        Mock(return_value=aws_runtime),
    )
    monkeypatch.setattr(tracker_main, "resolve_start_aws_runtime", Mock(return_value=aws_runtime))


def _use_real_sandbox_recovery(service: AsyncMock) -> None:
    async def run(**kwargs: Any) -> Any:
        return await BenchmarkServiceClient.run_with_sandbox_recovery(
            cast(BenchmarkServiceClient, service),
            **kwargs,
        )

    service.run_with_sandbox_recovery.side_effect = run


class MockProvider:
    def __init__(
        self,
        pool_id: str,
        events: list[str],
        *,
        capacity: Sequence[bool] = (True,),
        on_check: Callable[[], Awaitable[None]] | None = None,
        snapshot: SandboxCapacity | Exception = _CAPACITY,
        demand: SandboxReservationDemand | Exception | None = None,
        domains: list[SandboxCapacityDomain] | None = None,
    ) -> None:
        self.admission_pool_id = pool_id
        self.events = events
        self.capacity = iter(capacity)
        self.on_check = on_check
        self.snapshot = snapshot
        self.demand = demand
        self.domains = domains
        self.sources: list[SandboxSource] = []

    async def reservable_demand(self, _source: SandboxSource, resources: Resources) -> SandboxReservationDemand:
        if isinstance(self.demand, Exception):
            raise self.demand
        if self.demand is not None:
            return self.demand

        return SandboxReservationDemand(
            target_id="region-a",
            sandbox_class="sysbox",
            vcpu=resources.vcpu,
            memory=resources.memory,
            disk=resources.disk,
            gpu=0,
        )

    async def check_admission(self, source: SandboxSource, _resources: Resources) -> bool:
        self.events.append("capacity")
        self.sources.append(source)
        if self.on_check:
            await self.on_check()
        return next(self.capacity, True)

    async def get_capacity(self) -> SandboxCapacity:
        self.events.append("read_capacity")
        if isinstance(self.snapshot, Exception):
            raise self.snapshot
        return self.snapshot

    async def get_capacity_domains(self) -> list[SandboxCapacityDomain]:
        if self.domains is not None:
            self.events.append("read_capacity")

            return self.domains

        return [SandboxCapacityDomain(target_id="region-a", sandbox_class="sysbox", capacity=await self.get_capacity())]

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        self.events.append("create")
        return cast(Sandbox, Mock(id="scheduler-sandbox", name=request.name, labels=request.labels))

    async def delete_sandbox(self, _sandbox_id: str) -> None:
        self.events.append("cleanup")

    async def get_sandbox(self, _sandbox_id: str) -> Sandbox:
        raise SandboxNotFoundError("No abandoned sandbox")

    async def close(self) -> None:
        pass


_RELEASE_ID = "scheduler-test-release"


def _run(
    session: Session,
    pool_id: str,
    tasks: Sequence[tuple[str, TaskStatus, datetime]],
    *,
    priority: int = 3,
    concurrency: int = 1,
    executor: ExecutorDispatchStatus = ExecutorDispatchStatus.RUNNING,
) -> tuple[Org, Benchmark, list[Task]]:
    org = Org(id=uuid4(), name=f"scheduler-{uuid4()}")
    release = session.get(ExecutorRelease, _RELEASE_ID)
    if release is None:
        release = ExecutorRelease(
            id=_RELEASE_ID,
            artifact_uri=f"source:///artifacts/{_RELEASE_ID}/src",
            artifact_digest="a" * 64,
            protocol_version=SUPPORTED_PROTOCOL_VERSION,
        )
        session.add(release)
        session.flush()
    benchmark = Benchmark(
        org_id=org.id,
        name=f"run-{uuid4()}",
        arguments=AWSBenchmarkArguments(
            contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
            concurrency=concurrency,
            priority=priority,
            queue_pool_id=pool_id,
            sandbox_provider_secret_name=f"{tenant_secret_prefix(org.id)}provider",
        ),
        executor_release_id=release.id,
        current_execution_release_id=release.id,
        executor_artifact_uri=release.artifact_uri,
        executor_artifact_digest=release.artifact_digest,
        executor_protocol_version=release.protocol_version,
    )
    rows = [
        make_task(benchmark, task_id, status=status, started_at=started_at) for task_id, status, started_at in tasks
    ]
    session.add(org)
    session.flush()
    session.add(benchmark)
    session.add_all(rows)
    session.flush()
    session.add(
        ExecutorDispatch(
            benchmark_id=benchmark.id,
            kind=ExecutorDispatchKind.START,
            status=executor,
            executor_release_id=release.id,
            executor_artifact_uri=release.artifact_uri,
            executor_artifact_digest=release.artifact_digest,
            executor_protocol_version=release.protocol_version,
        )
    )
    session.commit()
    return org, benchmark, rows


def _claim_executor(engine: Engine, benchmark: Benchmark) -> None:
    with Session(engine) as session:
        dispatch = session.exec(select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark.id)).one()
        dispatch.status = ExecutorDispatchStatus.RUNNING
        session.commit()


def _update_task(engine: Engine, task: Task, status: TaskStatus, started_at: datetime) -> None:
    with Session(engine) as session:
        row = session.get(Task, task.id)
        assert row
        row.status = status
        row.started_at = started_at
        session.commit()


def _task(engine: Engine, task: Task) -> Task:
    with Session(engine) as session:
        row = session.get(Task, task.id)
        assert row
        return row


def _reservation(engine: Engine, task: Task, started_at: datetime) -> SandboxBuildReservation:
    with Session(engine) as session:
        row = session.get(SandboxBuildReservation, (task.id, started_at))
        assert row is not None
        return row


def _set_concurrency(engine: Engine, benchmark: Benchmark, concurrency: int) -> None:
    with Session(engine) as session:
        row = session.get(Benchmark, benchmark.id)
        assert row
        row.arguments = row.arguments.model_copy(update={"concurrency": concurrency})
        session.commit()


def _revoke_dispatch(engine: Engine, dispatch_id: UUID) -> None:
    with Session(engine) as session:
        dispatch = session.get(ExecutorDispatch, dispatch_id)
        assert dispatch is not None
        dispatch.status = ExecutorDispatchStatus.FAILED
        session.commit()


def _sandbox(
    events: list[str],
    *,
    creation_state: SandboxCreationState,
    name: str = "sandbox",
    on_create: Callable[[], Awaitable[None]] | None = None,
    on_cleanup: Callable[[], Awaitable[None]] | None = None,
) -> Callable[[], AbstractAsyncContextManager[Sandbox]]:
    @asynccontextmanager
    async def context() -> AsyncGenerator[Sandbox]:
        creation_state.cleanup_pending = True
        events.append("create")
        if on_create:
            await on_create()
        sandbox = Mock(
            id=f"sandbox-{len(events)}",
            clear_egress_rules=AsyncMock(),
            modify_egress_rules=AsyncMock(),
            block_all_egress=AsyncMock(),
        )
        sandbox.name = name
        try:
            yield cast(Sandbox, sandbox)
        finally:
            if on_cleanup:
                await on_cleanup()
            events.append("cleanup")
            creation_state.cleanup_pending = False

    return context


def _context(
    control: QueueControl,
    provider_pool_id: str,
    events: list[str],
    *,
    capacity: Sequence[bool] = (True,),
    on_check: Callable[[], Awaitable[None]] | None = None,
    snapshot: SandboxCapacity | Exception = _CAPACITY,
    reserved: bool = False,
    demand: SandboxReservationDemand | Exception | None = None,
) -> admission.SandboxQueueContext:
    provider = MockProvider(
        provider_pool_id, events, capacity=capacity, on_check=on_check, snapshot=snapshot, demand=demand
    )
    pool_id = queue_pool_id(provider_pool_id)
    if reserved:
        pool_id = reserved_queue_pool_id(pool_id)
    return admission.SandboxQueueContext(
        provider=cast(SandboxProvider, provider),
        pool_id=pool_id,
        control=control,
        poll_interval_seconds=0,
    )


def _reservation_count(engine: Engine, pool_id: str) -> int:
    with Session(engine) as session:
        return SchedulerRepository(session).active_reservations(pool_id).count


async def _build_lock_is_free(engine: Engine, task: Task) -> bool:
    lock = advisory_lock.task_build_lock(engine, task.id)
    if not await lock.acquire():
        return False
    await lock.release()
    return True


@pytest.fixture
def executed_sql(postgres_engine: Engine) -> Generator[list[str], None, None]:
    statements: list[str] = []

    def record(
        _connection: Connection,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: ExecutionContext,
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    event.listen(postgres_engine, "before_cursor_execute", record)
    try:
        yield statements
    finally:
        event.remove(postgres_engine, "before_cursor_execute", record)


@pytest.fixture
def scheduler_schema_revision(postgres_engine: Engine) -> Generator[None, None, None]:
    config = Config(str(Path(__file__).resolve().parents[4] / "alembic.ini"))
    revision = ScriptDirectory.from_config(config).get_current_head()
    with postgres_engine.begin() as connection:
        connection.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)"))
        connection.execute(text("INSERT INTO alembic_version (version_num) VALUES (:revision)"), {"revision": revision})
    yield
    with postgres_engine.begin() as connection:
        connection.execute(text("DROP TABLE alembic_version"))


@pytest.mark.description("A queue context needs a managed provider.")
def test_queue_context_requires_managed_provider() -> None:
    provider = Mock(admission_pool_id=None)

    with pytest.raises(ValueError, match="does not support queued admission"):
        admission.create_queue_context(
            control=Mock(),
            provider=cast(SandboxProvider, provider),
        )


@pytest.mark.description(
    "A run start request waits on a held Postgres row lock without blocking the event loop, then enqueues once."
)
@pytest.mark.parametrize("operation", ["retry", "start"])
async def test_http_admission_waits_for_real_postgres_row_lock_without_blocking_loop(
    operation: str,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    org, benchmark, _ = _run(
        postgres_session,
        reserved_queue_pool_id(queue_pool_id(f"daytona:{uuid4()}")),
        [("locked-admission", TaskStatus.STOPPED, _ATTEMPT)],
    )
    bound_controls(benchmark, session=postgres_session)
    assert benchmark.current_execution_release_id is not None
    benchmark.status = BenchmarkStatus.STOPPED
    postgres_session.add(benchmark)
    promote_release(postgres_session, benchmark.current_execution_release_id)
    postgres_session.commit()
    identity = RequestIdentity(org=org)

    def request_session() -> Generator[Session, None, None]:
        with Session(postgres_engine) as session:
            yield session

    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_session, request_session)
    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_current_org, lambda: org)
    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_current_runner, lambda: identity)
    monkeypatch.setattr(tracker_main, "check_database_connection", lambda: True)
    monkeypatch.setattr(tracker_main, "SANDBOX_QUEUE_ENABLED", False)
    _use_managed_runtime(monkeypatch, aws_runtime)

    async def health_check(*_args: Any, **_kwargs: Any) -> object:
        return object()

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["new-task"])

    async def close(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def copy_agent(*_args: Any, **_kwargs: Any) -> None:
        return None

    enqueued: list[UUID] = []

    async def enqueue(dispatch: ExecutorDispatch, **_kwargs: Any) -> None:
        enqueued.append(dispatch.id)

    monkeypatch.setattr(BenchmarkServiceClient, "health_check", health_check)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr(BenchmarkServiceClient, "version", _version)
    monkeypatch.setattr(BenchmarkServiceClient, "close", close)
    monkeypatch.setattr(tracker_main, "copy_agent_to_benchmark", copy_agent)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", AsyncMock(return_value=True))
    monkeypatch.setattr(tracker_main, "_launch_executor_dispatch", enqueue)
    lock_entered = threading.Event()
    if operation == "retry":
        original_lock = tracker_main.get_executor_admission

        def observed_recovery_lock(session: Session, *, for_update: bool) -> ExecutorAdmission:
            assert for_update
            session.connection().execute(text("SET LOCAL lock_timeout = '5s'"))
            lock_entered.set()
            return original_lock(session, for_update=for_update)

        monkeypatch.setattr(tracker_main, "get_executor_admission", observed_recovery_lock)
        url = f"/retry-or-resume-benchmark/{benchmark.id}"
        body: dict[str, Any] = {}
    else:
        original_select_active_release = dispatch_control.select_active_release

        def observed_start_lock(session: Session, *, for_update: bool = False) -> ExecutorRelease:
            if for_update:
                session.connection().execute(text("SET LOCAL lock_timeout = '5s'"))
                lock_entered.set()
            return original_select_active_release(session, for_update=for_update)

        monkeypatch.setattr(dispatch_control, "select_active_release", observed_start_lock)
        url = "/start-benchmark"
        body = StartBenchmarkRequest(
            benchmark_name=f"postgres-lock-{uuid4()}",
            contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
            task_ids=["new-task"],
            properties=aws_runtime.resources,
            sandbox_provider="daytona",
        ).model_dump(mode="json")
        postgres_session.add(OrgProviderSecret(org_id=org.id, provider="daytona", secret_name="provider"))
        postgres_session.commit()

    request: asyncio.Task[httpx.Response] | None = None
    with Session(postgres_engine) as blocker:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=tracker_main.app), base_url="http://test"
        ) as client:
            try:
                blocker.exec(select(ExecutorAdmission).with_for_update()).one()
                request = asyncio.create_task(client.post(url, json=body))
                assert await asyncio.to_thread(lock_entered.wait, 5)
                assert not request.done()
                health = await asyncio.wait_for(client.get("/health"), timeout=2)
                assert health.status_code == 200
                blocker.rollback()
                response = await asyncio.wait_for(request, timeout=5)
            finally:
                blocker.rollback()
                if request is not None:
                    await asyncio.wait_for(
                        asyncio.gather(request, return_exceptions=True),
                        timeout=5,
                    )

    assert response.status_code == 200, response.text
    assert len(enqueued) == 1


@pytest.mark.description(
    "Manual recovery gives only the selected task a fresh wall clock deadline about 166 hours out."
)
@pytest.mark.parametrize(
    ("case", "retry", "retry_mode", "selected_status", "saved_evaluation", "explicit_ids", "reject"),
    [
        ("retry-scratch", True, RetryMode.FROM_SCRATCH, TaskStatus.ERROR, False, False, False),
        ("retry-scratch-saved", True, RetryMode.FROM_SCRATCH, TaskStatus.ERROR, True, False, False),
        ("retry-auto", True, RetryMode.AUTO, TaskStatus.ERROR, False, False, False),
        ("resume-scratch", False, RetryMode.FROM_SCRATCH, TaskStatus.STOPPED, False, False, False),
        ("resume-scratch-saved", False, RetryMode.FROM_SCRATCH, TaskStatus.STOPPED, True, False, False),
        ("resume-auto", False, RetryMode.AUTO, TaskStatus.STOPPED, False, False, False),
        ("resume-explicit-finished", False, RetryMode.AUTO, TaskStatus.FINISHED, False, True, False),
        ("retry-saved-evaluation", True, RetryMode.AUTO, TaskStatus.ERROR, True, False, False),
        ("resume-saved-evaluation", False, RetryMode.AUTO, TaskStatus.STOPPED, True, False, False),
        ("rejected-admission", True, RetryMode.FROM_SCRATCH, TaskStatus.ERROR, False, False, True),
    ],
)
async def test_manual_recovery_restarts_only_selected_generation_wall(
    case: str,
    retry: bool,
    retry_mode: RetryMode,
    selected_status: TaskStatus,
    saved_evaluation: bool,
    explicit_ids: bool,
    reject: bool,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    runtime_services: RuntimeServices,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (selected, unselected) = _run(
        postgres_session,
        reserved_queue_pool_id(queue_pool_id(provider_pool_id)),
        [("selected", selected_status, _ATTEMPT), ("unselected", TaskStatus.FINISHED, _ATTEMPT)],
    )
    old_deadline = datetime.now(UTC) - timedelta(days=1)
    selected.credited_wall_deadline_at = old_deadline
    unselected.credited_wall_deadline_at = old_deadline
    if saved_evaluation:
        selected.eval_resume_state = {"job_id": "saved-job"}
    dispatch = postgres_session.exec(
        select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark.id)
    ).one()
    bound_controls(benchmark, dispatch_id=dispatch.id, session=postgres_session)
    assert benchmark.current_execution_release_id is not None
    benchmark.status = BenchmarkStatus.STOPPED
    postgres_session.add_all([benchmark, selected, unselected])
    promote_release(postgres_session, benchmark.current_execution_release_id)
    postgres_session.commit()

    def request_session() -> Generator[Session, None, None]:
        with Session(postgres_engine) as session:
            yield session

    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_session, request_session)
    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_current_org, lambda: org)
    monkeypatch.setitem(
        tracker_main.app.dependency_overrides, tracker_main.get_current_runner, lambda: RequestIdentity(org=org)
    )
    monkeypatch.setattr(tracker_main, "check_database_connection", lambda: True)
    monkeypatch.setattr(tracker_main, "SANDBOX_QUEUE_ENABLED", False)
    _use_managed_runtime(monkeypatch, aws_runtime)
    monkeypatch.setattr(BenchmarkServiceClient, "health_check", AsyncMock(return_value=object()))
    monkeypatch.setattr(
        BenchmarkServiceClient,
        "verify_task_ids",
        AsyncMock(return_value=VerifyTaskIdsResponse(task_ids=[selected.task_id])),
    )
    monkeypatch.setattr(BenchmarkServiceClient, "close", AsyncMock())
    monkeypatch.setattr(tracker_main, "copy_agent_to_benchmark", AsyncMock())
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", AsyncMock(return_value=True))
    enqueue = AsyncMock()
    monkeypatch.setattr(tracker_main, "_launch_executor_dispatch", enqueue)
    if reject:
        monkeypatch.setattr(
            tracker_main, "admit_recovery_dispatch", Mock(side_effect=RuntimeError("admission rejected"))
        )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=tracker_main.app), base_url="http://test") as client:
        request = client.post(
            f"/retry-or-resume-benchmark/{benchmark.id}",
            params={"retry": retry, "retry_mode": retry_mode.value},
            json={"task_ids": [selected.task_id] if explicit_ids else []},
        )
        if reject:
            with pytest.raises(RuntimeError, match="admission rejected"):
                await request
        else:
            response = await request
            assert response.status_code == 200, response.text

    with Session(postgres_engine) as session:
        persisted_selected = session.get(Task, selected.id)
        persisted_unselected = session.get(Task, unselected.id)
        assert persisted_selected is not None
        assert persisted_unselected is not None
        assert persisted_selected.task_id == selected.task_id
        assert persisted_unselected.status == TaskStatus.FINISHED
        assert persisted_unselected.credited_wall_deadline_at == old_deadline
        if reject:
            assert persisted_selected.status == selected_status
            assert persisted_selected.credited_wall_deadline_at == old_deadline
            persisted_benchmark = session.get(Benchmark, benchmark.id)
            assert persisted_benchmark is not None and persisted_benchmark.status == BenchmarkStatus.STOPPED
            assert (
                session.exec(
                    select(func.count())
                    .select_from(ExecutorDispatch)
                    .where(ExecutorDispatch.benchmark_id == benchmark.id)
                ).one()
                == 1
            )
            enqueue.assert_not_awaited()
            return
        continues_evaluation = saved_evaluation and retry_mode == RetryMode.AUTO
        assert persisted_selected.status == (TaskStatus.EVALUATING if continues_evaluation else TaskStatus.PENDING)
        assert persisted_selected.credited_wall_deadline_at == (old_deadline if continues_evaluation else None)
        selected_started_at = persisted_selected.started_at
        assert persisted_selected.eval_resume_state == ({"job_id": "saved-job"} if continues_evaluation else None)
        dispatch = session.exec(
            select(ExecutorDispatch)
            .where(ExecutorDispatch.benchmark_id == benchmark.id)
            .where(ExecutorDispatch.status == ExecutorDispatchStatus.QUEUED)
        ).one()
        assert dispatch.assigned_task_ids == [selected.task_id]
        dispatch_id = dispatch.id
    enqueue.assert_awaited_once()

    if case == "retry-scratch":
        controls = bound_controls(benchmark, session=postgres_session, dispatch_id=dispatch_id)
        task_control = cast(TaskControl, controls["task_control"])
        events: list[str] = []
        context = _context(cast(QueueControl, controls["queue_control"]), provider_pool_id, events, reserved=True)
        service = AsyncMock(spec=BenchmarkServiceClient)
        service.retrieve_task.return_value = RetrieveTaskResponse(
            source=_SOURCE, problem_path="/tmp/problem.txt", cwd="/testbed", resources=_RESOURCES
        )
        service.evaluate_instance.return_value = {"status": "success", "score": 1.0}
        _use_real_sandbox_recovery(service)

        monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
        monkeypatch.setattr(
            task_execution,
            "create_sandbox",
            lambda *_args, **kwargs: _sandbox(
                events, creation_state=cast(SandboxCreationState, kwargs["creation_state"])
            )(),
        )
        monkeypatch.setattr(task_execution, "upload_agent_artifacts", AsyncMock())
        monkeypatch.setattr(task_execution, "install_agent_dependencies", AsyncMock())
        monkeypatch.setattr(task_execution, "run_agent", AsyncMock(side_effect=_run_agent_success))
        admitted_task = (
            await task_control.read(
                AttemptRef(
                    org_id=selected.org_id,
                    task_row_id=selected.id,
                    task_id=selected.task_id,
                    started_at=selected_started_at,
                )
            )
        ).task
        async with _drive_queue(postgres_engine, context):
            await task_execution.process_task(
                admitted_task,
                benchmark.managed_start_benchmark_request(),
                cast(BenchmarkServiceClient, service),
                benchmark.id,
                selected.task_id,
                runtime_services,
                OrgContext(id=org.id, name=org.name, storage_key=org.storage_key),
                sandbox_provider_config=DaytonaProviderConfig(
                    DAYTONA_API_KEY="test-api-key", DAYTONA_API_URL="http://localhost:8001", DAYTONA_TARGET="us"
                ),
                sandbox_provider=context.provider,
                creation_semaphore=Semaphore(1),
                control=task_control,
                started_by_email=benchmark.started_by_email,
                queue_context=context,
            )
        assert "create" in events
        new_deadline = _task(postgres_engine, selected).credited_wall_deadline_at
        assert new_deadline is not None
        assert timedelta(hours=165) < new_deadline - datetime.now(UTC) <= timedelta(hours=166)


@pytest.mark.description("The dispatch payload key is generated while the admission lock is still free.")
@pytest.mark.parametrize("operation", ["retry", "start"])
async def test_http_admission_generates_payload_key_without_holding_admission_lock(
    operation: str,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    org, benchmark, _ = _run(
        postgres_session,
        queue_pool_id(f"daytona:{uuid4()}"),
        [("payload-key", TaskStatus.STOPPED, _ATTEMPT)],
    )
    benchmark.arguments = benchmark.arguments.model_copy(update={"queue_pool_id": None, "priority": None})
    bound_controls(benchmark, session=postgres_session)
    assert benchmark.current_execution_release_id is not None
    benchmark.status = BenchmarkStatus.STOPPED
    postgres_session.add(benchmark)
    promote_release(postgres_session, benchmark.current_execution_release_id)
    postgres_session.commit()
    identity = RequestIdentity(org=org)

    def request_session() -> Generator[Session, None, None]:
        with Session(postgres_engine) as session:
            yield session

    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_session, request_session)
    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_current_org, lambda: org)
    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_current_runner, lambda: identity)
    monkeypatch.setattr(tracker_main, "check_database_connection", lambda: True)
    monkeypatch.setattr(tracker_main, "SANDBOX_QUEUE_ENABLED", False)
    _use_managed_runtime(monkeypatch, aws_runtime)

    async def health_check(*_args: Any, **_kwargs: Any) -> object:
        return object()

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["new-task"])

    async def close(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def copy_agent(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def enqueue(_dispatch: ExecutorDispatch, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(BenchmarkServiceClient, "health_check", health_check)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr(BenchmarkServiceClient, "version", _version)
    monkeypatch.setattr(BenchmarkServiceClient, "close", close)
    monkeypatch.setattr(tracker_main, "copy_agent_to_benchmark", copy_agent)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", AsyncMock(return_value=True))
    monkeypatch.setattr(tracker_main, "_launch_executor_dispatch", enqueue)
    original_generate_payload_key = tracker_main.generate_payload_key
    admission_lockable_during_key_generation: list[bool] = []

    def generate_payload_key(dispatch_id: UUID) -> Any:
        with Session(postgres_engine) as other_replica:
            other_replica.exec(select(ExecutorAdmission).with_for_update(nowait=True)).one()
            admission_lockable_during_key_generation.append(True)
        return original_generate_payload_key(dispatch_id)

    monkeypatch.setattr(tracker_main, "generate_payload_key", generate_payload_key)
    if operation == "retry":
        url = f"/retry-or-resume-benchmark/{benchmark.id}"
        body: dict[str, Any] = {}
    else:
        url = "/start-benchmark"
        body = StartBenchmarkRequest(
            benchmark_name=f"payload-key-{uuid4()}",
            contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
            task_ids=["new-task"],
            properties=aws_runtime.resources,
            sandbox_provider="daytona",
        ).model_dump(mode="json")
        postgres_session.add(OrgProviderSecret(org_id=org.id, provider="daytona", secret_name="daytona"))
        postgres_session.commit()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=tracker_main.app), base_url="http://test") as client:
        response = await asyncio.wait_for(client.post(url, json=body), timeout=10)

    assert response.status_code == 200, response.text
    assert admission_lockable_during_key_generation == [True]


@pytest.mark.description("A targeted snapshot source is passed to the provider unchanged, after the capacity reads.")
async def test_targeted_snapshot_reaches_admission_and_creation_unchanged(
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    events: list[str] = []
    source = TargetedSnapshotSource(snapshot="snapshot", target="us-west-3")
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session, queue_pool_id(provider_pool_id), [("targeted", TaskStatus.PENDING, _ATTEMPT)]
    )
    dispatch = postgres_session.exec(
        select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark.id)
    ).one()
    bound = bound_controls(benchmark, session=postgres_session, dispatch_id=dispatch.id)
    control = cast(QueueControl, bound["queue_control"])
    provider = MockProvider(provider_pool_id, events)
    context = admission.create_queue_context(
        control=control, provider=cast(SandboxProvider, provider), poll_interval_seconds=0
    )

    creation_state = SandboxCreationState(cleanup_pending=False)
    attempt = AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
    phases: list[object] = []
    async with AsyncExitStack() as stack, AsyncExitStack() as preparation_stack:
        sandbox = await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            attempt=attempt,
            source=source,
            resources=_RESOURCES,
            creation_state=creation_state,
            create=_sandbox(events, creation_state=creation_state),
            preparation_stack=preparation_stack,
            on_phase_claimed=phases.append,
            on_phase_released=phases.append,
            on_phase_abandon=phases.append,
        )

        assert sandbox is not None
        assert provider.sources == [source]
        assert provider.sources[0] is source
        assert events == ["capacity", "create"]
        assert len(phases) == 2 and phases[1] is True


@asynccontextmanager
async def _drive_queue(engine: Engine, context: admission.SandboxQueueContext) -> AsyncGenerator[None, None]:
    if not is_reserved_queue_pool_id(context.pool_id):
        yield
        return

    async def provider_factory(_reference: coordinator.PoolReference) -> SandboxProvider:
        return context.provider

    scheduler = coordinator.SharedPoolScheduler(
        engine, provider_factory=provider_factory, interval_seconds=0.01, rate_per_minute=0
    )
    scheduler.start()
    try:
        async with asyncio.timeout(10):
            yield
    finally:
        await scheduler.stop()


async def _enter(
    stack: AsyncExitStack,
    context: admission.SandboxQueueContext,
    task: Task,
    events: list[str],
    engine: Engine,
    *,
    source: SandboxSource = _SOURCE,
    resources: Resources = _RESOURCES,
    on_create: Callable[[], Awaitable[None]] | None = None,
    on_cleanup: Callable[[], Awaitable[None]] | None = None,
) -> Sandbox | None:
    creation_state = SandboxCreationState(cleanup_pending=False)
    attempt = AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
    async with _drive_queue(engine, context), AsyncExitStack() as preparation_stack:
        await task_execution._acquire_queue_preparation(preparation_stack, context)
        return await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            attempt=attempt,
            source=source,
            resources=resources,
            creation_state=creation_state,
            create=_sandbox(events, creation_state=creation_state, on_create=on_create, on_cleanup=on_cleanup),
            preparation_stack=preparation_stack,
            on_phase_claimed=lambda _phase: None,
            on_phase_released=lambda _started: None,
            on_phase_abandon=lambda _phase: None,
        )


async def _enter_provider_sandbox(
    stack: AsyncExitStack,
    context: admission.SandboxQueueContext,
    task: Task,
    creation_state: SandboxCreationState,
    creation_semaphore: Semaphore,
    engine: Engine,
) -> Sandbox | None:
    def create() -> AbstractAsyncContextManager[Sandbox]:
        return create_sandbox(
            provider=context.provider,
            sandbox_name="scheduler-sandbox",
            source=_SOURCE,
            resources=_RESOURCES,
            creation_semaphore=creation_semaphore,
            creation_state=creation_state,
            sandbox_link=SandboxLink(benchmark_id=context.control.ref.benchmark_id, provider="daytona"),
            record_sandbox=TaskControl(context.control.control, context.control.ref).record_sandbox,
            unique_name=False,
        )

    attempt = AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
    async with _drive_queue(engine, context), AsyncExitStack() as preparation_stack:
        await task_execution._acquire_queue_preparation(preparation_stack, context)
        return await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            attempt=attempt,
            source=_SOURCE,
            resources=_RESOURCES,
            creation_state=creation_state,
            create=create,
            preparation_stack=preparation_stack,
            on_phase_claimed=lambda _phase: None,
            on_phase_released=lambda _started: None,
            on_phase_abandon=lambda _phase: None,
        )


def _assert_reserved_demand(engine: Engine, pool_id: str, count: int) -> None:
    with Session(engine) as session:
        assert SchedulerRepository(session).active_reservations(pool_id) == store.ActiveReservations(
            count=count,
            vcpu=count,
            memory=count * 2,
            disk=count * 3,
            gpu=0,
        )


@pytest.mark.description("Queued pool recovery resets an abandoned build.")
async def test_recover_queued_pool_resets_abandoned_build(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = queue_pool_id(provider_pool_id)
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("abandoned", TaskStatus.PENDING, _ATTEMPT)])
    first = bound_controls(benchmark, session=postgres_session)
    first_control = cast(QueueControl, first["queue_control"])
    attempt = AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
    claimed = await first_control.claim(pool_id, attempt)
    assert claimed.phase is not None
    building = _task(postgres_engine, task)
    assert building.status == TaskStatus.BUILDING
    assert building.build_phase_dispatch_id == first_control.ref.dispatch_id
    assert building.build_phase_token == claimed.phase.token
    assert building.started_at == attempt.started_at

    with Session(postgres_engine) as session:
        owner = session.get(ExecutorDispatch, first_control.ref.dispatch_id)
        assert owner is not None
        owner.status = ExecutorDispatchStatus.FAILED
        session.commit()
    replacement = bound_controls(benchmark, session=postgres_session, dispatch_id=uuid4())
    context = _context(cast(QueueControl, replacement["queue_control"]), provider_pool_id, [])
    await admission.recover_queued_pool(context)

    assert _task(postgres_engine, task).status == TaskStatus.PENDING


@pytest.mark.description("A completed run finalizes while another run's sandbox creation remains in progress.")
async def test_completed_run_finalizes_while_another_run_creates_a_sandbox(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = queue_pool_id(provider_pool_id)
    org, completing_run, (finished,) = _run(postgres_session, pool_id, [("finished", TaskStatus.FINISHED, _ATTEMPT)])
    postgres_session.add(
        EvaluationResult(
            org_id=org.id,
            task=finished.id,
            instance_id=f"completed-{finished.id}",
            result={"score": 1.0},
        )
    )
    postgres_session.commit()
    _, building_run, (building,) = _run(postgres_session, pool_id, [("building", TaskStatus.PENDING, _ATTEMPT)])
    run_control = cast(RunControl, bound_controls(completing_run, session=postgres_session)["run_control"])
    queue_control = cast(QueueControl, bound_controls(building_run, session=postgres_session)["queue_control"])
    events: list[str] = []
    context = _context(queue_control, provider_pool_id, events)
    creation_started = asyncio.Event()
    release_creation = asyncio.Event()

    async def hold_creation() -> None:
        creation_started.set()
        await release_creation.wait()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter(
                stack,
                context,
                building,
                events,
                postgres_engine,
                source=SnapshotSource(snapshot="snapshot"),
                on_create=hold_creation,
            )
        )
        try:
            await asyncio.wait_for(creation_started.wait(), timeout=2)
            decision = await asyncio.wait_for(run_control.prepare_finalization(), timeout=2)
            assert decision.kind == "score"
            assert decision.inputs is not None
            claim = await asyncio.wait_for(
                run_control.commit_final_score(
                    decision.inputs.phase,
                    decision.inputs.fingerprint,
                    1.0,
                    {},
                ),
                timeout=2,
            )
            assert claim.committed
            assert claim.status == BenchmarkStatus.FINISHED
            assert _task(postgres_engine, building).status == TaskStatus.BUILDING
            assert not entering.done()
        finally:
            release_creation.set()
            await asyncio.wait_for(entering, timeout=2)

    assert _task(postgres_engine, building).status == TaskStatus.IN_PROGRESS


@pytest.mark.description("Admission returns no sandbox and calls no provider for a task that has left the queue.")
async def test_admission_ignores_task_that_is_no_longer_waiting(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    events: list[str] = []
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session,
        queue_pool_id(provider_pool_id),
        [("stopped", TaskStatus.STOPPED, _ATTEMPT)],
    )
    control = cast(QueueControl, bound_controls(benchmark, session=postgres_session)["queue_control"])
    context = _context(control, provider_pool_id, events)

    async with AsyncExitStack() as stack:
        sandbox = await _enter(stack, context, task, events, postgres_engine)

    assert sandbox is None
    assert events == []


@pytest.mark.description(
    "Revoked authority before the claim returns no sandbox and leaves the task `PENDING` after the capacity check."
)
async def test_revocation_before_claim_creates_no_sandbox(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session,
        queue_pool_id(provider_pool_id),
        [("revoked-before-claim", TaskStatus.PENDING, _ATTEMPT)],
    )
    control = cast(QueueControl, bound_controls(benchmark, session=postgres_session)["queue_control"])
    dispatch_id = control.ref.dispatch_id
    events: list[str] = []

    async def revoke() -> None:
        _revoke_dispatch(postgres_engine, dispatch_id)

    async with AsyncExitStack() as stack:
        with pytest.raises(ExecutionAuthorityRevoked):
            await _enter(
                stack, _context(control, provider_pool_id, events, on_check=revoke), task, events, postgres_engine
            )
    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert events == ["capacity"]


@pytest.mark.description(
    "Revocation during creation cleans up the sandbox, and recovery returns the task to `PENDING` with a new start time."
)
async def test_revocation_during_creation_cleans_up_without_starting(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session,
        queue_pool_id(provider_pool_id),
        [("revoked-during-creation", TaskStatus.PENDING, _ATTEMPT)],
    )
    control = cast(QueueControl, bound_controls(benchmark, session=postgres_session)["queue_control"])
    dispatch_id = control.ref.dispatch_id
    events: list[str] = []

    async def revoke() -> None:
        _revoke_dispatch(postgres_engine, dispatch_id)

    async with AsyncExitStack() as stack:
        with pytest.raises(ExecutionAuthorityRevoked):
            await _enter(
                stack, _context(control, provider_pool_id, events), task, events, postgres_engine, on_create=revoke
            )
    assert _task(postgres_engine, task).status == TaskStatus.BUILDING
    assert events == ["capacity", "create", "cleanup"]


@pytest.mark.description("An unclaimed higher-priority executor cannot hold the queue head.")
def test_unclaimed_executor_does_not_block_the_pool_head(postgres_session: Session, postgres_engine: Engine) -> None:
    pool_id = f"pool_{uuid4().hex[:24]}"
    _, booting, (booting_task,) = _run(
        postgres_session,
        pool_id,
        [("booting", TaskStatus.PENDING, _ATTEMPT)],
        priority=0,
        executor=ExecutorDispatchStatus.QUEUED,
    )
    _, _, (claimed_task,) = _run(postgres_session, pool_id, [("claimed", TaskStatus.PENDING, _ATTEMPT)], priority=3)

    repository = SchedulerRepository(postgres_session)
    assert not repository.eligible_task_is(pool_id, booting_task.id, booting_task.started_at, 1)
    assert repository.eligible_task_is(pool_id, claimed_task.id, claimed_task.started_at, 1)

    _claim_executor(postgres_engine, booting)
    postgres_session.expire_all()

    assert repository.eligible_task_is(pool_id, booting_task.id, booting_task.started_at, 1)
    assert not repository.eligible_task_is(pool_id, claimed_task.id, claimed_task.started_at, 1)


@pytest.mark.description("Priority and FIFO admission stay within a pool, and a claim requires the exact attempt.")
def test_priority_fifo_pool_isolation_and_claim(
    postgres_session: Session,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = queue_pool_id(provider_pool_id)
    other_pool_id = queue_pool_id(f"daytona:{uuid4()}")
    _, _, (low_priority,) = _run(
        postgres_session,
        pool_id,
        [("low", TaskStatus.PENDING, _ATTEMPT - timedelta(minutes=1))],
        priority=4,
    )
    _, _, (head,) = _run(
        postgres_session,
        pool_id,
        [("head", TaskStatus.PENDING, _ATTEMPT)],
        priority=0,
    )
    _, _, (next_fifo,) = _run(
        postgres_session,
        pool_id,
        [("next", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1))],
        priority=0,
    )
    _, _, (other_pool,) = _run(
        postgres_session,
        other_pool_id,
        [("other", TaskStatus.PENDING, _ATTEMPT)],
    )

    repository = SchedulerRepository(postgres_session)
    assert not repository.eligible_task_is(pool_id, low_priority.id, low_priority.started_at, 1)
    assert repository.eligible_task_is(pool_id, head.id, head.started_at, 1)
    assert not repository.claim_eligible_task(pool_id, next_fifo.id, next_fifo.started_at)
    assert repository.eligible_task_is(other_pool_id, other_pool.id, other_pool.started_at, 1)
    assert not repository.claim_eligible_task(pool_id, head.id, head.started_at + timedelta(microseconds=1))
    assert repository.claim_eligible_task(pool_id, head.id, head.started_at)
    postgres_session.commit()


@pytest.mark.description("Admission rechecks concurrent work and capacity before creating the waiting sandbox.")
async def test_admission_rechecks_dynamic_concurrency_and_capacity(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = queue_pool_id(provider_pool_id)
    _, benchmark, (active, waiting) = _run(
        postgres_session,
        pool_id,
        [
            ("active", TaskStatus.IN_PROGRESS, _ATTEMPT),
            ("waiting", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
    )
    events: list[str] = []
    observed_states: list[TaskStatus] = []

    async def observe_admission() -> None:
        observed_states.append(_task(postgres_engine, waiting).status)

    async def poll(_seconds: float) -> None:
        _set_concurrency(postgres_engine, benchmark, 2)

    sleep = AsyncMock(side_effect=poll)
    monkeypatch.setattr("tracker.scheduler.admission.asyncio.sleep", sleep)
    context = _context(
        cast(QueueControl, bound_controls(benchmark, session=postgres_session)["queue_control"]),
        provider_pool_id,
        events,
        capacity=(False, True),
        on_check=observe_admission,
    )
    async with AsyncExitStack() as stack:
        sandbox = await _enter(stack, context, waiting, events, postgres_engine, on_create=observe_admission)
        assert sandbox
        assert _task(postgres_engine, waiting).status == TaskStatus.IN_PROGRESS

    assert _task(postgres_engine, active).status == TaskStatus.IN_PROGRESS
    assert sleep.await_count >= 1
    assert observed_states[0] == TaskStatus.PENDING
    assert observed_states[-1] == TaskStatus.BUILDING
    assert all(state == TaskStatus.PENDING for state in observed_states[:-1])
    assert events.count("capacity") >= 2
    assert events[-2:] == ["create", "cleanup"]


@pytest.mark.description("Cancellation during the capacity check leaves the queued task available for admission.")
async def test_cancellation_during_capacity_check_leaves_queue_admittable(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = queue_pool_id(provider_pool_id)
    _, benchmark, (task,) = _run(
        postgres_session,
        pool_id,
        [("cancelled", TaskStatus.PENDING, _ATTEMPT)],
    )
    capacity_started = asyncio.Event()
    hold_capacity = asyncio.Event()

    async def block_capacity() -> None:
        capacity_started.set()
        await hold_capacity.wait()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter(
                stack,
                _context(
                    cast(QueueControl, bound_controls(benchmark, session=postgres_session)["queue_control"]),
                    provider_pool_id,
                    [],
                    on_check=block_capacity,
                ),
                task,
                [],
                postgres_engine,
            )
        )
        await capacity_started.wait()
        entering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await entering

    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert _reservation_count(postgres_engine, pool_id) == 0


@pytest.mark.description("Cancelling during a stale creation waits for cleanup and leaves the task `PENDING`.")
async def test_stale_creation_is_cleaned_before_cancellation(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (capacity_task, creation_task) = _run(
        postgres_session,
        pool_id,
        [
            ("stale-capacity", TaskStatus.PENDING, _ATTEMPT),
            ("stale-creation", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    control = cast(QueueControl, bound_controls(benchmark, session=postgres_session)["queue_control"])
    capacity_events: list[str] = []

    async def supersede_during_capacity() -> None:
        _update_task(
            postgres_engine,
            capacity_task,
            TaskStatus.STOPPED,
            capacity_task.started_at + timedelta(microseconds=1),
        )

    async with AsyncExitStack() as stack:
        assert (
            await _enter(
                stack,
                _context(control, provider_pool_id, capacity_events, on_check=supersede_during_capacity, reserved=True),
                capacity_task,
                capacity_events,
                postgres_engine,
            )
            is None
        )

    events: list[str] = []
    cleanup_started = asyncio.Event()
    finish_cleanup = asyncio.Event()

    async def supersede_attempt() -> None:
        _update_task(
            postgres_engine,
            creation_task,
            TaskStatus.PENDING,
            creation_task.started_at + timedelta(microseconds=1),
        )

    async def block_cleanup() -> None:
        cleanup_started.set()
        await finish_cleanup.wait()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter(
                stack,
                _context(control, provider_pool_id, events, reserved=True),
                creation_task,
                events,
                postgres_engine,
                on_create=supersede_attempt,
                on_cleanup=block_cleanup,
            )
        )
        await cleanup_started.wait()
        entering.cancel()
        await asyncio.sleep(0)
        assert not entering.done()
        finish_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await entering

    assert capacity_events == ["capacity"]
    assert _task(postgres_engine, creation_task).status == TaskStatus.PENDING

    assert events[:2] == ["capacity", "read_capacity"]
    assert [event for event in events if event != "read_capacity"] == ["capacity", "create", "cleanup"]


@pytest.mark.description("An active evaluation phase rejects recovery without mutating the task.")
async def test_active_evaluation_phase_rejects_recovery_without_mutation(
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (task,) = _run(
        postgres_session,
        queue_pool_id(provider_pool_id),
        [("owned-evaluation", TaskStatus.EVALUATING, _ATTEMPT)],
    )
    task.eval_resume_state = {"job_id": "active-job"}
    postgres_session.add(task)
    postgres_session.commit()
    control = cast(QueueControl, bound_controls(benchmark, session=postgres_session)["queue_control"])
    dispatch = postgres_session.get(ExecutorDispatch, control.ref.dispatch_id)
    assert dispatch is not None
    task.started_at = dispatch.created_at
    task.evaluation_phase_dispatch_id = dispatch.id
    task.evaluation_phase_token = uuid4()
    postgres_session.add(task)
    postgres_session.commit()
    original_started_at = task.started_at
    dispatch_count = postgres_session.exec(
        select(func.count()).select_from(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark.id)
    ).one()
    enqueue = AsyncMock()
    monkeypatch.setattr(tracker_main, "_launch_executor_dispatch", enqueue)
    _use_managed_runtime(monkeypatch, aws_runtime)

    with pytest.raises(HTTPException) as error:
        await tracker_main.retry_or_resume_benchmark(
            benchmark.id,
            Request({"type": "http", "headers": []}),
            retry=False,
            retry_mode=RetryMode.AUTO,
            concurrency=None,
            task_ids=[],
            service_headers={},
            secrets={},
            benchmark_url=None,
            session=postgres_session,
            org=org,
            issued_keys=[],
        )

    assert error.value.status_code == 409
    assert error.value.detail == "Run has an evaluation that is already owned by an active executor"
    enqueue.assert_not_awaited()
    postgres_session.expire_all()
    persisted_task = postgres_session.get(Task, task.id)
    assert persisted_task is not None
    assert persisted_task.status == TaskStatus.EVALUATING
    assert persisted_task.started_at == original_started_at
    assert (
        postgres_session.exec(
            select(func.count()).select_from(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark.id)
        ).one()
        == dispatch_count
    )


@pytest.mark.description(
    "After two recovery handoffs, only the newest dispatch resumes the evaluation and the task finishes once."
)
async def test_two_recovery_handoffs_leave_one_evaluation_owner(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    runtime_services: RuntimeServices,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (task,) = _run(
        postgres_session,
        reserved_queue_pool_id(queue_pool_id(provider_pool_id)),
        [("raced-evaluation", TaskStatus.EVALUATING, _ATTEMPT)],
    )
    task.eval_resume_state = {"job_id": "raced-job"}
    postgres_session.add(task)
    postgres_session.commit()
    bound_controls(benchmark, session=postgres_session)
    assert benchmark.current_execution_release_id is not None
    promote_release(postgres_session, benchmark.current_execution_release_id)
    postgres_session.commit()
    first_enqueued = asyncio.Event()
    release_first = asyncio.Event()
    enqueued_dispatch_ids: list[UUID] = []

    async def enqueue(dispatch: ExecutorDispatch, *, session: Session, **_kwargs: object) -> None:
        persisted_dispatch = session.get(ExecutorDispatch, dispatch.id)
        assert persisted_dispatch is not None
        persisted_dispatch.status = ExecutorDispatchStatus.RUNNING
        session.commit()
        enqueued_dispatch_ids.append(dispatch.id)
        if len(enqueued_dispatch_ids) == 1:
            first_enqueued.set()
            await release_first.wait()

    monkeypatch.setattr(tracker_main, "_launch_executor_dispatch", enqueue)
    _use_managed_runtime(monkeypatch, aws_runtime)

    async def recover() -> None:
        with Session(postgres_engine) as recovery_session:
            response = await tracker_main.retry_or_resume_benchmark(
                benchmark.id,
                Request({"type": "http", "headers": []}),
                retry=False,
                retry_mode=RetryMode.AUTO,
                concurrency=None,
                task_ids=[],
                service_headers={},
                secrets={},
                benchmark_url=None,
                session=recovery_session,
                org=org,
                issued_keys=[],
            )
            assert response.status == "success"

    first_recovery = asyncio.create_task(recover())
    await asyncio.wait_for(first_enqueued.wait(), timeout=5)
    await asyncio.wait_for(recover(), timeout=5)
    release_first.set()
    await asyncio.wait_for(first_recovery, timeout=5)

    assert len(enqueued_dispatch_ids) == 2
    first_dispatch = postgres_session.get(ExecutorDispatch, enqueued_dispatch_ids[0])
    winning_dispatch = postgres_session.get(ExecutorDispatch, enqueued_dispatch_ids[1])
    assert first_dispatch is not None
    assert winning_dispatch is not None
    postgres_session.expire_all()
    winning_task = postgres_session.get(Task, task.id)
    assert winning_task is not None
    assert winning_task.started_at == winning_dispatch.created_at

    first_control = cast(
        TaskControl,
        bound_controls(
            benchmark,
            session=postgres_session,
            dispatch_id=first_dispatch.id,
        )["task_control"],
    )
    winner_control = cast(
        TaskControl,
        bound_controls(
            benchmark,
            session=postgres_session,
            dispatch_id=winning_dispatch.id,
        )["task_control"],
    )
    winning_state = (
        await winner_control.read(
            AttemptRef(
                org_id=task.org_id,
                task_row_id=task.id,
                task_id=task.task_id,
                started_at=winning_task.started_at,
            )
        )
    ).task
    stale_state = winning_state.model_copy(update={"started_at": first_dispatch.created_at})
    service = AsyncMock(spec=BenchmarkServiceClient)
    service.resume_evaluation.return_value = {"score": 1.0}
    _use_real_sandbox_recovery(service)

    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
    request = benchmark.managed_start_benchmark_request()

    async def run(task_row: TaskState, control: TaskControl) -> dict[str, dict[str, Any] | None]:
        return await task_execution.process_task(
            task_row,
            request,
            cast(BenchmarkServiceClient, service),
            benchmark.id,
            task.task_id,
            runtime_services,
            OrgContext(id=org.id, name=org.name, storage_key=org.storage_key),
            sandbox_provider_config=cast(SandboxProviderConfig, object()),
            sandbox_provider=cast(SandboxProvider, object()),
            creation_semaphore=Semaphore(1),
            control=control,
            started_by_email=benchmark.started_by_email,
        )

    stale_result = await run(stale_state, first_control)
    winner_result = await run(winning_state, winner_control)

    assert stale_result == {task.task_id: None}
    assert winner_result == {task.task_id: {"score": 1.0}}
    service.resume_evaluation.assert_awaited_once()
    assert _task(postgres_engine, task).status == TaskStatus.FINISHED


@pytest.mark.description(
    "A resumed evaluation gets a child grading token and stores the updated `eval_resume_state` when the task finishes."
)
@pytest.mark.parametrize("grading_path", ["nonproject", "project"])
async def test_resumed_evaluation_persists_callback_before_result(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    runtime_services: RuntimeServices,
    bound_controls: Callable[..., dict[str, object]],
    grading_path: str,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (task,) = _run(
        postgres_session,
        reserved_queue_pool_id(queue_pool_id(provider_pool_id)),
        [("resume-on-one-connection", TaskStatus.EVALUATING, _ATTEMPT)],
    )
    if grading_path == "project":
        org.name = "vals.ai"
        project = Project(org_id=org.id, handle="selected", name="Selected", spend_type=SpendType.INTERNAL)
        project.gateway_key_id = uuid4()
        postgres_session.add(project)
        postgres_session.flush()
        postgres_session.add(RunProject(benchmark_id=benchmark.id, project_id=project.id))
        reference = f"{tenant_secret_prefix(org.id)}model-gateway-{project.gateway_key_id}"
        benchmark.arguments = benchmark.arguments.model_copy(
            update={
                "contract": benchmark.arguments.contract.model_copy(
                    update={
                        "secrets": {
                            "MODEL_GATEWAY_URL": reference,
                            "MODEL_GATEWAY_API_KEY": reference,
                            "UNRELATED_KEY": "deleted-agent-secret",
                        }
                    }
                ),
                "selected_project_gateway_secret_ref": reference,
            }
        )
        postgres_session.add_all([org, benchmark])
    task.eval_resume_state = {"job_id": "initial-job"}
    postgres_session.add(task)
    postgres_session.commit()
    control = cast(TaskControl, bound_controls(benchmark, session=postgres_session)["task_control"])
    dispatch = postgres_session.get(ExecutorDispatch, control.ref.dispatch_id)
    assert dispatch is not None
    task.started_at = dispatch.created_at
    postgres_session.add(task)
    postgres_session.commit()

    service = AsyncMock(spec=BenchmarkServiceClient)
    if grading_path == "project":
        service.grading_requirements.return_value = ["openai/gpt-5"]
        monkeypatch.setattr(
            model_gateway,
            "_client",
            httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(
                        200,
                        json={"token": "grader-child", "lease_id": "grader-lease", "expires_at": 1_800_000_000}
                        if request.url.path == "/service-auth"
                        else {"revoked": True},
                    )
                )
            ),
        )

    async def resume_evaluation(*_args: object, on_eval_resume_state: Any, **_kwargs: object) -> dict[str, float]:
        await on_eval_resume_state({"job_id": "updated-job"})
        return {"score": 1.0}

    service.resume_evaluation.side_effect = resume_evaluation
    _use_real_sandbox_recovery(service)

    if grading_path == "project":

        async def resolve(names: dict[str, str], _store: Any) -> dict[str, str]:
            assert set(names) == {"MODEL_GATEWAY_URL", "MODEL_GATEWAY_API_KEY"}
            return {"MODEL_GATEWAY_URL": "https://gateway.test", "MODEL_GATEWAY_API_KEY": "saved-project-root"}
    else:
        resolve = AsyncMock(side_effect=AssertionError("nonproject resume must not load grading secrets"))
    monkeypatch.setattr(task_execution, "resolve_secrets", resolve)
    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
    task_control = cast(
        TaskControl,
        bound_controls(
            benchmark,
            session=postgres_session,
            dispatch_id=dispatch.id,
        )["task_control"],
    )
    task_state = (
        await task_control.read(
            AttemptRef(
                org_id=task.org_id,
                task_row_id=task.id,
                task_id=task.task_id,
                started_at=task.started_at,
            )
        )
    ).task
    result = await task_execution.process_task(
        task_state,
        benchmark.managed_start_benchmark_request(),
        cast(BenchmarkServiceClient, service),
        benchmark.id,
        task.task_id,
        runtime_services,
        OrgContext(id=org.id, name=org.name, storage_key=org.storage_key),
        sandbox_provider_config=cast(SandboxProviderConfig, object()),
        sandbox_provider=cast(SandboxProvider, object()),
        creation_semaphore=Semaphore(1),
        control=task_control,
        started_by_email=benchmark.started_by_email,
    )

    assert result == {task.task_id: {"score": 1.0}}
    service.resume_evaluation.assert_awaited_once()
    if grading_path == "project":
        assert service.resume_evaluation.await_args.kwargs["grading_token"] == "grader-child"
    else:
        resolve.assert_not_awaited()
    persisted_task = _task(postgres_engine, task)
    assert persisted_task.status == TaskStatus.FINISHED
    assert persisted_task.eval_resume_state == {"job_id": "updated-job"}


@pytest.mark.description("A setup retry reenters FIFO ahead of a competing queued task.")
async def test_setup_retry_reenters_fifo_before_competitor(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    runtime_services: RuntimeServices,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = queue_pool_id(provider_pool_id)
    org, benchmark, (retrying, competitor) = _run(
        postgres_session,
        pool_id,
        [
            ("retrying", TaskStatus.PENDING, _ATTEMPT),
            ("competitor", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
    )
    events: list[str] = []
    admission_states: list[tuple[TaskStatus, TaskStatus]] = []

    async def observe_admission() -> None:
        admission_states.append(
            (
                _task(postgres_engine, retrying).status,
                _task(postgres_engine, competitor).status,
            )
        )

    names: list[str] = []

    def create_sandbox(*_args: object, **kwargs: object) -> AbstractAsyncContextManager[Sandbox]:
        names.append(cast(str, kwargs["sandbox_name"]))
        return _sandbox(events, creation_state=cast(SandboxCreationState, kwargs["creation_state"]), name=names[-1])()

    service = AsyncMock(spec=BenchmarkServiceClient)
    service.retrieve_task.return_value = RetrieveTaskResponse(
        source=_SOURCE,
        problem_path="/tmp/problem.txt",
        cwd="/testbed",
        resources=_RESOURCES,
    )
    service.evaluate_instance.return_value = {"status": "success", "score": 1.0}
    _use_real_sandbox_recovery(service)
    monkeypatch.setattr(task_execution, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
    monkeypatch.setattr(task_execution, "create_sandbox", create_sandbox)
    monkeypatch.setattr(
        task_execution,
        "upload_agent_artifacts",
        AsyncMock(side_effect=[SandboxSetupError("retry"), None]),
    )
    monkeypatch.setattr(task_execution, "run_agent", AsyncMock(side_effect=_run_agent_success))
    controls = bound_controls(benchmark, session=postgres_session)
    task_control = cast(TaskControl, controls["task_control"])
    context = _context(
        cast(QueueControl, controls["queue_control"]), provider_pool_id, events, on_check=observe_admission
    )
    retry_state = (
        await task_control.read(
            AttemptRef(
                org_id=retrying.org_id,
                task_row_id=retrying.id,
                task_id=retrying.task_id,
                started_at=retrying.started_at,
            )
        )
    ).task

    await task_execution.process_task(
        retry_state,
        benchmark.managed_start_benchmark_request(),
        cast(BenchmarkServiceClient, service),
        benchmark.id,
        retrying.task_id,
        runtime_services,
        OrgContext(id=org.id, name=org.name, storage_key=org.storage_key),
        sandbox_provider_config=DaytonaProviderConfig(
            DAYTONA_API_KEY="test-api-key",
            DAYTONA_API_URL="http://localhost:8001",
            DAYTONA_TARGET="us",
        ),
        sandbox_provider=context.provider,
        creation_semaphore=Semaphore(1),
        control=task_control,
        queue_context=context,
        started_by_email=benchmark.started_by_email,
    )

    assert admission_states == [
        (TaskStatus.PENDING, TaskStatus.PENDING),
        (TaskStatus.PENDING, TaskStatus.PENDING),
    ]
    assert _task(postgres_engine, competitor).status == TaskStatus.PENDING
    assert len(names) == 2
    assert names[0] == names[1]
    assert retrying.id.hex in names[0]


@pytest.mark.description(
    "A setup retry schedules an error row, runs setup a second time on a fresh grant, and evaluates once."
)
async def test_queued_worker_setup_retry_waits_for_retention_then_finishes_with_a_fresh_grant(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
    runtime_services: RuntimeServices,
) -> None:
    events: list[str] = []
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    org, benchmark, (task,) = _run(postgres_session, pool_id, [("setup-retry", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    task_control = cast(TaskControl, bound["task_control"])
    context = _context(cast(QueueControl, bound["queue_control"]), provider_pool_id, events, reserved=True)
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    grants: list[SandboxBuildReservation] = []
    sandboxes: list[Sandbox] = []
    deleted: list[str] = []

    async def create(request: SandboxCreateRequest) -> Sandbox:
        with Session(postgres_engine) as session:
            grant = session.get(SandboxBuildReservation, (task.id, task.started_at))
            assert grant is not None
            assert grant.accepted_at is not None
            assert grant.released_at is None
            assert grant.executor_dispatch_id == context.control.ref.dispatch_id
            assert _task(postgres_engine, task).status == TaskStatus.BUILDING
            grants.append(grant)
        sandbox = Mock(
            id=f"retry-sandbox-{len(grants)}",
            state="started",
            labels=request.labels,
            clear_egress_rules=AsyncMock(),
            modify_egress_rules=AsyncMock(),
            block_all_egress=AsyncMock(),
        )
        sandbox.name = request.name
        sandboxes.append(cast(Sandbox, sandbox))
        return cast(Sandbox, sandbox)

    async def delete(sandbox_id: str) -> None:
        if sandbox_id == sandboxes[0].id:
            cleanup_started.set()
            await allow_cleanup.wait()
        deleted.append(sandbox_id)

    service = AsyncMock(spec=BenchmarkServiceClient)
    _use_real_sandbox_recovery(service)
    service.retrieve_task.return_value = RetrieveTaskResponse(
        source=_SOURCE, resources=_RESOURCES, problem_path="/tmp/problem.txt", cwd="/testbed"
    )
    service.setup_task.side_effect = [
        BenchmarkServiceError("Command failed: docker compose"),
        SetupTaskResponse(status="ok"),
    ]
    service.evaluate_instance.return_value = {"score": 1.0}
    monkeypatch.setattr(context.provider, "create_sandbox", create)
    monkeypatch.setattr(context.provider, "delete_sandbox", delete)
    monkeypatch.setattr(task_execution, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
    monkeypatch.setattr(coordinator, "_RECOVERY_SECONDS", 0.01)
    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
    monkeypatch.setattr(task_execution, "upload_agent_artifacts", AsyncMock())
    monkeypatch.setattr(task_execution, "install_agent_dependencies", AsyncMock())
    monkeypatch.setattr(task_execution, "run_agent", _run_agent_success)
    state = (
        await task_control.read(
            AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
        )
    ).task
    async with _drive_queue(postgres_engine, context):
        worker = asyncio.create_task(
            task_execution.process_task(
                state,
                benchmark.managed_start_benchmark_request(),
                cast(BenchmarkServiceClient, service),
                benchmark.id,
                task.task_id,
                runtime_services,
                OrgContext(id=org.id, name=org.name, storage_key=org.storage_key),
                sandbox_provider_config=DaytonaProviderConfig(
                    DAYTONA_API_KEY="test-api-key", DAYTONA_API_URL="http://localhost:8001", DAYTONA_TARGET="us"
                ),
                sandbox_provider=context.provider,
                creation_semaphore=Semaphore(1),
                control=task_control,
                queue_context=context,
                started_by_email=benchmark.started_by_email,
            )
        )
        try:
            async with asyncio.timeout(10):
                await cleanup_started.wait()
                assert _task(postgres_engine, task).status == TaskStatus.IN_PROGRESS
                assert len(grants) == 1
                assert deleted == []
                allow_cleanup.set()
                while True:
                    with Session(postgres_engine) as session:
                        candidate = session.get(SandboxAdmissionCandidate, (task.id, task.started_at))
                    if candidate is not None:
                        break
                    assert _task(postgres_engine, task).started_at == task.started_at
                    assert not worker.done()
                    await asyncio.sleep(0.01)
                assert candidate.executor_dispatch_id == context.control.ref.dispatch_id
                assert candidate.admission_ready
                with Session(postgres_engine) as session:
                    error = session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).one()
                    assert error.error_type == "SandboxSetupError"
                    assert "Command failed: docker compose" in error.error_message
                    assert error.phase == FailurePhase.SETUP
                    assert error.retry_scheduled
                    assert error.failed_attempt_number == 1
                    retained = session.get(SandboxBuildReservation, (task.id, task.started_at))
                assert retained is not None
                assert retained.released_at is not None
                assert deleted == [sandboxes[0].id]
                assert retained.reserved_at == grants[0].reserved_at
                assert retained.accepted_at == grants[0].accepted_at
                assert _reservation_count(postgres_engine, context.pool_id) == 0
                assert _task(postgres_engine, task).status == TaskStatus.PENDING
                assert _candidate_count(postgres_engine, context.pool_id) == 1
                assert len(grants) == 1
                assert not worker.done()
                with Session(postgres_engine) as session:
                    session.exec(
                        update(SandboxBuildReservation)
                        .where(col(SandboxBuildReservation.task_row_id) == task.id)
                        .where(col(SandboxBuildReservation.attempt_started_at) == task.started_at)
                        .values(
                            released_at=SchedulerRepository(session).database_now()
                            - coordinator._RELEASED_RESERVATION_RETENTION
                            - timedelta(seconds=1)
                        )
                    )
                    session.commit()
                await worker
        finally:
            allow_cleanup.set()
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    assert len(grants) == 2
    assert grants[1].reserved_at > grants[0].reserved_at
    assert grants[1].accepted_at is not None
    assert grants[0].accepted_at is not None
    assert grants[1].accepted_at > grants[0].accepted_at
    assert grants[1].attempt_started_at == grants[0].attempt_started_at == task.started_at
    assert deleted == [sandbox.id for sandbox in sandboxes]
    assert _task(postgres_engine, task).status == TaskStatus.FINISHED
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert _candidate_count(postgres_engine, context.pool_id) == 0
    with Session(postgres_engine) as session:
        result = session.exec(select(EvaluationResult).where(EvaluationResult.task == task.id)).one()
        assert result.instance_id == sandboxes[1].id
        assert result.result == {"score": 1.0}
        assert session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).one().retry_scheduled
    assert service.setup_task.await_count == 2
    service.evaluate_instance.assert_awaited_once()


def _stop_run(engine: Engine, benchmark: Benchmark) -> None:
    with Session(engine) as session:
        row = session.get(Benchmark, benchmark.id)
        assert row
        row.status = BenchmarkStatus.STOPPING
        session.commit()


def _prepare(
    session: Session,
    pool_id: str,
    task: Task,
    demand: SandboxReservationDemand = _DEMAND,
    *,
    admission_ready: bool = True,
    dispatch_id: UUID | None = None,
) -> UUID:
    if dispatch_id is None:
        dispatch = session.exec(
            select(ExecutorDispatch).where(col(ExecutorDispatch.benchmark_id) == task.benchmark)
        ).one()
        dispatch_id = dispatch.id
    SchedulerRepository(session).prepare_admission_candidate(
        pool_id,
        task.id,
        task.started_at,
        dispatch_id,
        demand,
        admission_ready=admission_ready,
    )
    session.commit()

    return dispatch_id


def _candidate_count(engine: Engine, pool_id: str) -> int:
    with Session(engine) as session:
        return len(
            session.exec(
                select(SandboxAdmissionCandidate).where(
                    col(SandboxAdmissionCandidate.pool_id) == queue_pool_lock_id(pool_id)
                )
            ).all()
        )


def _reserve(session: Session, pool_id: str, task: Task) -> None:
    task.status = TaskStatus.BUILDING
    session.add(task)
    session.add(
        SandboxBuildReservation(
            task_row_id=task.id,
            attempt_started_at=task.started_at,
            pool_id=queue_pool_lock_id(pool_id),
            target_id="region-a",
            sandbox_class="sysbox",
            requested_vcpu=1,
            requested_memory=2,
            requested_disk=3,
            requested_gpu=0,
            accepted_at=SchedulerRepository(session).database_now(),
        )
    )
    session.commit()


@pytest.mark.description(
    "Two reserved image builds create sandboxes concurrently and release their reservations afterward."
)
async def test_reserved_image_builds_provision_concurrently(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (first, second) = _run(
        postgres_session,
        pool_id,
        [
            ("first", TaskStatus.PENDING, _ATTEMPT),
            ("second", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    context = _context(
        cast(QueueControl, bound_controls(benchmark, session=postgres_session)["queue_control"]),
        provider_pool_id,
        [],
        reserved=True,
    )
    creating = 0
    both_creating = asyncio.Event()
    finish_creating = asyncio.Event()

    async def hold_create() -> None:
        nonlocal creating
        creating += 1
        if creating == 2:
            both_creating.set()
        await finish_creating.wait()

    events: list[str] = []
    async with AsyncExitStack() as first_stack, AsyncExitStack() as second_stack:
        entering = asyncio.gather(
            _enter(first_stack, context, first, events, postgres_engine, on_create=hold_create),
            _enter(second_stack, context, second, events, postgres_engine, on_create=hold_create),
        )
        try:
            await asyncio.wait_for(both_creating.wait(), timeout=10)

            assert _reservation_count(postgres_engine, context.pool_id) == 2
            assert {_task(postgres_engine, task).status for task in (first, second)} == {TaskStatus.BUILDING}
            assert all(
                _reservation(postgres_engine, task, task.started_at).phase_token is not None for task in (first, second)
            )
            assert not entering.done()
        finally:
            finish_creating.set()
        sandboxes = await asyncio.wait_for(entering, timeout=10)

        assert all(sandboxes)
        assert {_task(postgres_engine, task).status for task in (first, second)} == {TaskStatus.IN_PROGRESS}
        assert _reservation_count(postgres_engine, context.pool_id) == 0

    assert events.count("create") == 2
    assert events.index("cleanup") > max(index for index, event in enumerate(events) if event == "create")


@pytest.mark.description("Candidate cleanup needs the exact attempt and dispatch.")
def test_candidate_cleanup_requires_the_exact_attempt_and_dispatch(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    pool_id = reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, (task,) = _run(postgres_session, pool_id, [("owned", TaskStatus.PENDING, _ATTEMPT)])
    dispatch_id = _prepare(postgres_session, pool_id, task)

    SchedulerRepository(postgres_session).clear_admission_candidate(
        task.id, task.started_at + timedelta(seconds=1), dispatch_id
    )
    SchedulerRepository(postgres_session).clear_admission_candidate(task.id, task.started_at, uuid4())
    postgres_session.commit()
    assert _candidate_count(postgres_engine, pool_id) == 1

    SchedulerRepository(postgres_session).clear_admission_candidate(task.id, task.started_at, dispatch_id)
    postgres_session.commit()

    assert _candidate_count(postgres_engine, pool_id) == 0
    assert _task(postgres_engine, task).status == TaskStatus.PENDING


@pytest.mark.description("Recovery deletes queue candidates and reservations whose dispatch no longer has authority.")
@pytest.mark.parametrize("interruption", ["superseded", "revoked", "stopped"])
async def test_recovery_removes_candidates_without_current_execution_authority(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
    interruption: str,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    org, benchmark, (task,) = _run(postgres_session, pool_id, [("stale", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    control = cast(QueueControl, bound["queue_control"])
    attempt = AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
    await control.prepare(pool_id, attempt, admission.QueueDemand(**_DEMAND.model_dump()), True)
    assert _candidate_count(postgres_engine, pool_id) == 1
    if interruption == "superseded":
        with Session(postgres_engine) as session:
            row = session.get(Benchmark, benchmark.id)
            owner = session.get(Org, org.id)
            assert row is not None and owner is not None
            lifecycle_control.apply_stop_benchmark(row, session, force=True, org=owner, task_ids=[task.task_id])
            retry_control.reset_to_in_progress_status(
                row,
                session,
                retry=False,
                retry_mode=RetryMode.FROM_SCRATCH,
                rerun_task_ids=[task.task_id],
                org=owner,
                verified_task_ids=[task.task_id],
            )
            session.commit()
    elif interruption == "revoked":
        dispatch_id = UUID(cast(str, bound["executor_dispatch_id"]))
        with Session(postgres_engine) as session:
            dispatch = session.get(ExecutorDispatch, dispatch_id)
            assert dispatch is not None
            dispatch.status = ExecutorDispatchStatus.FAILED
            session.commit()
    else:
        _stop_run(postgres_engine, benchmark)

    recovery = bound_controls(benchmark, session=postgres_session)
    context = _context(cast(QueueControl, recovery["queue_control"]), provider_pool_id, [], reserved=True)
    await admission.recover_queued_pool(context)

    assert _candidate_count(postgres_engine, pool_id) == 0
    assert _reservation_count(postgres_engine, pool_id) == 0


@pytest.mark.description("A capacity snapshot with headroom grants only one of two parallel builds.")
@pytest.mark.parametrize(
    "snapshot",
    [
        _CAPACITY.model_copy(update={"cpu": ResourceCapacity(total=_RESOURCES.vcpu * 2, used=0)}),
        _CAPACITY.model_copy(update={"memory": ResourceCapacity(total=_RESOURCES.memory * 2, used=0)}),
        _CAPACITY.model_copy(update={"disk": ResourceCapacity(total=_RESOURCES.disk * 2, used=0)}),
    ],
    ids=["cpu", "memory", "disk"],
)
async def test_reservations_bound_parallel_builds(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
    snapshot: SandboxCapacity,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    provider_events: list[str] = []
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (first, second) = _run(
        postgres_session,
        pool_id,
        [
            ("first", TaskStatus.PENDING, _ATTEMPT),
            ("second", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    control = cast(QueueControl, bound_controls(benchmark, session=postgres_session)["queue_control"])
    context = _context(control, provider_pool_id, provider_events, reserved=True, snapshot=snapshot)
    first_creating = asyncio.Event()
    finish_first = asyncio.Event()
    polled_states: list[tuple[TaskStatus, TaskStatus, int]] = []

    async def hold_first_create() -> None:
        first_creating.set()
        await finish_first.wait()

    async def poll(_seconds: float) -> None:
        if not first_creating.is_set():
            await asyncio.sleep(0)
            return
        polled_states.append(
            (
                _task(postgres_engine, first).status,
                _task(postgres_engine, second).status,
                _reservation_count(postgres_engine, context.pool_id),
            )
        )
        finish_first.set()

    monkeypatch.setattr(admission, "_poll_sleep", poll)
    first_events: list[str] = []
    second_events: list[str] = []
    async with AsyncExitStack() as first_stack, AsyncExitStack() as second_stack:
        first_entering = asyncio.create_task(
            _enter(first_stack, context, first, first_events, postgres_engine, on_create=hold_first_create)
        )
        try:
            await asyncio.wait_for(first_creating.wait(), timeout=10)
            second_sandbox = await asyncio.wait_for(
                _enter(second_stack, context, second, second_events, postgres_engine), timeout=10
            )
        finally:
            finish_first.set()
            first_sandbox = await asyncio.wait_for(first_entering, timeout=10)

        assert first_sandbox is not None
        assert second_sandbox is not None

    assert polled_states
    assert polled_states[0] == (TaskStatus.BUILDING, TaskStatus.PENDING, 1)
    assert {_task(postgres_engine, task).status for task in (first, second)} == {TaskStatus.IN_PROGRESS}
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert first_events == second_events == ["create", "cleanup"]
    assert provider_events.index("capacity") < provider_events.index("read_capacity")


@pytest.mark.description(
    "After a failed reserved create is cleaned up, its released phase survives queue-exit and the task can start again."
)
async def test_failed_reserved_create_recovers_after_provider_cleanup(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("failing", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(cast(QueueControl, bound["queue_control"]), provider_pool_id, events, reserved=True)

    async def fail_create() -> None:
        raise SandboxSetupError("provider rejected the create")

    async with AsyncExitStack() as stack:
        with pytest.raises(SandboxSetupError):
            await _enter(stack, context, task, events, postgres_engine, on_create=fail_create)

    assert events[:2] == ["capacity", "read_capacity"]
    assert events[-1] == "create"
    assert all(event == "read_capacity" for event in events[2:-1])
    assert _reservation_count(postgres_engine, context.pool_id) == 1
    reservation = _reservation(postgres_engine, task, task.started_at)
    assert reservation.phase_dispatch_id == UUID(cast(str, bound["executor_dispatch_id"]))
    assert reservation.phase_token is not None
    await context.control.abandon(
        pool_id,
        AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at),
        PhaseRef(token=reservation.phase_token),
    )
    relinquished = _reservation(postgres_engine, task, task.started_at)
    assert relinquished.phase_dispatch_id is None
    assert relinquished.phase_token is None
    assert _reservation_count(postgres_engine, pool_id) == 1
    assert not (await context.control.control.call("queue-exit", context.control.ref, QueueBuildResponse)).started
    assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id is None
    assert _reservation(postgres_engine, task, task.started_at).phase_token is None
    assert _reservation_count(postgres_engine, pool_id) == 1
    await _recover_once(context, postgres_engine)
    recovery = bound_controls(benchmark, session=postgres_session)
    context = _context(cast(QueueControl, recovery["queue_control"]), provider_pool_id, events, reserved=True)

    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    recovered = _task(postgres_engine, task)

    async with AsyncExitStack() as stack:
        assert await _enter(stack, context, recovered, events, postgres_engine) is not None
        assert _task(postgres_engine, task).status == TaskStatus.IN_PROGRESS

    assert _reservation_count(postgres_engine, context.pool_id) == 0


async def _recovery_orphan_control(
    session: Session,
    source_benchmark: Benchmark,
    recovery_benchmark: Benchmark,
    task: Task,
    pool_id: str,
    provider_pool_id: str,
    bound_controls: Callable[..., dict[str, object]],
) -> tuple[QueueControl, QueueControl]:
    source = bound_controls(source_benchmark, session=session)
    context = _context(cast(QueueControl, source["queue_control"]), provider_pool_id, [], reserved=True)
    assert context.pool_id == pool_id

    async def lose_provider_reply() -> None:
        raise SandboxError("creation outcome unknown")

    async with AsyncExitStack() as stack:
        with pytest.raises(SandboxError, match="creation outcome unknown"):
            await _enter(stack, context, task, [], cast(Engine, session.bind), on_create=lose_provider_reply)
    session.refresh(task)
    assert task.status == TaskStatus.BUILDING
    reservation = session.get(SandboxBuildReservation, (task.id, task.started_at))
    assert reservation is not None
    assert reservation.phase_dispatch_id == UUID(cast(str, source["executor_dispatch_id"]))
    assert reservation.phase_token is not None
    dispatch = session.get(ExecutorDispatch, reservation.phase_dispatch_id)
    assert dispatch is not None
    dispatch.status = ExecutorDispatchStatus.FAILED
    session.commit()
    original_control = cast(QueueControl, source["queue_control"])
    assert not (await original_control.control.call("queue-exit", original_control.ref, QueueBuildResponse)).started
    session.refresh(reservation)
    assert reservation.phase_dispatch_id is None
    assert reservation.phase_token is None
    assert reservation.released_at is None
    recovery_control = cast(QueueControl, bound_controls(recovery_benchmark, session=session)["queue_control"])
    return recovery_control, original_control


async def _recover_once(context: admission.SandboxQueueContext, engine: Engine) -> None:
    pool_context = coordinator._PoolContext(cast(SandboxProvider, context.provider), context.pool_id, engine)
    reference = next(
        reference for reference in coordinator.read_pool_references(engine) if reference.pool_id == context.pool_id
    )
    await coordinator._reconcile_abandoned_reservations(pool_context)
    with engine.connect() as connection:
        coordinator.PoolCoordinator(engine, reference)._maintain(connection, pool_context, request_readiness=False)


@pytest.mark.description(
    "A confirmed original dispatch exit hands off its accepted phase, but provider cleanup must be confirmed before admission resumes."
)
@pytest.mark.parametrize("provider_outcome", ["deleted", "missing", "lookup_failed", "delete_failed", "wrong_name"])
async def test_abandoned_reservation_requires_confirmed_provider_cleanup(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
    provider_outcome: str,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("abandoned", TaskStatus.PENDING, _ATTEMPT)])
    control, _ = await _recovery_orphan_control(
        postgres_session, benchmark, benchmark, task, pool_id, provider_pool_id, bound_controls
    )
    context = _context(control, provider_pool_id, [], reserved=True)
    expected_name = f"queued-{task.id.hex}-{int(_ATTEMPT.replace(tzinfo=UTC).timestamp() * 1_000_000):x}"
    original = _reservation(postgres_engine, task, task.started_at)
    assert original.phase_dispatch_id is None and original.phase_token is None

    async def lookup(name: str) -> Sandbox:
        assert name == expected_name
        assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id == original.phase_dispatch_id
        assert _reservation(postgres_engine, task, task.started_at).phase_token == original.phase_token
        assert _reservation_count(postgres_engine, context.pool_id) == 1
        if provider_outcome == "missing":
            raise SandboxNotFoundError("Absent")

        if provider_outcome == "lookup_failed":
            raise SandboxError("Lookup failed")

        sandbox = Mock(id="orphan", labels={})
        sandbox.name = "wrong" if provider_outcome == "wrong_name" else name
        return cast(Sandbox, sandbox)

    async def remove(sandbox_id: str) -> None:
        assert sandbox_id == "orphan"
        assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id == original.phase_dispatch_id
        assert _reservation_count(postgres_engine, context.pool_id) == 1
        if provider_outcome == "delete_failed":
            raise SandboxError("Deletion failed")

    deletion = AsyncMock(side_effect=remove)
    monkeypatch.setattr(context.provider, "get_sandbox", AsyncMock(side_effect=lookup))
    monkeypatch.setattr(context.provider, "delete_sandbox", deletion)

    await _recover_once(context, postgres_engine)

    expected_reservations = 0 if provider_outcome in {"deleted", "missing"} else 1
    assert _reservation_count(postgres_engine, context.pool_id) == expected_reservations
    recovered = _task(postgres_engine, task)
    assert recovered.status == TaskStatus.PENDING
    persisted = _reservation(postgres_engine, task, task.started_at)
    assert persisted.phase_dispatch_id == original.phase_dispatch_id
    assert persisted.phase_token == original.phase_token
    assert (persisted.released_at is None) is bool(expected_reservations)
    if provider_outcome not in {"deleted", "delete_failed"}:
        deletion.assert_not_awaited()

    if expected_reservations:
        monkeypatch.setattr(context.provider, "get_sandbox", AsyncMock(side_effect=SandboxNotFoundError("Absent")))
        await _recover_once(context, postgres_engine)
        assert _reservation_count(postgres_engine, context.pool_id) == 0


@pytest.mark.description(
    "Recovery releases an abandoned reservation whose cleanup keeps failing, frees its build lock, and returns the task to `PENDING`."
)
@pytest.mark.parametrize("provider_outcome", ["lookup_failed", "delete_failed", "wrong_name"])
async def test_abandoned_reservation_with_failing_cleanup_is_released_after_grace(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
    provider_outcome: str,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("leaked", TaskStatus.PENDING, _ATTEMPT)])
    control, _ = await _recovery_orphan_control(
        postgres_session, benchmark, benchmark, task, pool_id, provider_pool_id, bound_controls
    )
    context = _context(control, provider_pool_id, [], reserved=True)
    postgres_session.exec(
        update(SandboxBuildReservation)
        .where(col(SandboxBuildReservation.task_row_id) == task.id)
        .values(reserved_at=datetime.now(UTC) - coordinator._ORPHAN_CLEANUP_GRACE - timedelta(seconds=1))
    )
    postgres_session.commit()

    async def lookup(name: str) -> Sandbox:
        if provider_outcome == "lookup_failed":
            raise SandboxError("Lookup failed")

        sandbox = Mock(id="orphan", labels={})
        sandbox.name = "wrong" if provider_outcome == "wrong_name" else name
        return cast(Sandbox, sandbox)

    monkeypatch.setattr(context.provider, "get_sandbox", AsyncMock(side_effect=lookup))
    monkeypatch.setattr(context.provider, "delete_sandbox", AsyncMock(side_effect=SandboxError("Deletion failed")))

    await _recover_once(context, postgres_engine)

    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert _reservation(postgres_engine, task, task.started_at).released_at is not None


@pytest.mark.description(
    "A cancelled unconfirmed create retains its accepted reservation until the original executor exits, then recovery cleans it."
)
async def test_cancellation_during_unconfirmed_create_requires_recovery(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    events: list[str] = []
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("cancelled", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(cast(QueueControl, bound["queue_control"]), provider_pool_id, events, reserved=True)

    async def cancel_create() -> None:
        raise asyncio.CancelledError

    async with AsyncExitStack() as stack:
        with pytest.raises(asyncio.CancelledError):
            await _enter(stack, context, task, events, postgres_engine, on_create=cancel_create)

    assert events[:2] == ["capacity", "read_capacity"]
    assert [event for event in events if event != "read_capacity"] == ["capacity", "create"]
    cancelled = _task(postgres_engine, task)
    assert cancelled.status == TaskStatus.BUILDING
    assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id == UUID(
        cast(str, bound["executor_dispatch_id"])
    )
    assert _reservation(postgres_engine, task, task.started_at).phase_token is not None
    assert _reservation_count(postgres_engine, pool_id) == 1

    dispatch_id = UUID(cast(str, bound["executor_dispatch_id"]))
    with Session(postgres_engine) as session:
        dispatch = session.get(ExecutorDispatch, dispatch_id)
        assert dispatch is not None
        dispatch.status = ExecutorDispatchStatus.FAILED
        session.commit()
    recovery = bound_controls(benchmark, session=postgres_session)
    context = _context(cast(QueueControl, recovery["queue_control"]), provider_pool_id, events, reserved=True)
    await admission.recover_queued_pool(context)
    retained = _reservation(postgres_engine, task, task.started_at)
    assert retained.phase_dispatch_id == dispatch_id
    assert retained.phase_token is not None
    assert retained.released_at is None
    assert _task(postgres_engine, task).status == TaskStatus.BUILDING
    assert _reservation_count(postgres_engine, pool_id) == 1
    original_control = cast(QueueControl, bound["queue_control"])
    assert not (await original_control.control.call("queue-exit", original_control.ref, QueueBuildResponse)).started
    await admission.recover_queued_pool(context)

    fresh_task = _task(postgres_engine, task)
    assert fresh_task.status == TaskStatus.PENDING
    assert fresh_task.started_at > task.started_at
    assert _reservation_count(postgres_engine, pool_id) == 0

    async with AsyncExitStack() as stack:
        assert await _enter(stack, context, fresh_task, events, postgres_engine) is not None
        assert _task(postgres_engine, fresh_task).status == TaskStatus.IN_PROGRESS

    assert [event for event in events if event != "read_capacity"] == [
        "capacity",
        "create",
        "capacity",
        "create",
        "cleanup",
    ]
    assert _reservation_count(postgres_engine, pool_id) == 0


@pytest.mark.description("A reserved cancellation releases only acknowledged cleanup.")
@pytest.mark.parametrize(
    "outcome",
    [
        "acknowledged",
        "already_gone",
        "unknown_create",
        "provider_delete_error",
        "unexpected_delete_error",
        "second_cancel_create",
        "second_cancel_delete",
    ],
)
async def test_reserved_cancellation_releases_only_acknowledged_cleanup(
    outcome: str,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    events: list[str] = []
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    org, benchmark, (task,) = _run(postgres_session, pool_id, [("cancelled", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(cast(QueueControl, bound["queue_control"]), provider_pool_id, events, reserved=True)
    creation_state = SandboxCreationState(cleanup_pending=False)
    creating = asyncio.Event()
    finish_creation = asyncio.Event()
    deleting = asyncio.Event()
    finish_deletion = asyncio.Event()
    acknowledged = outcome in ("acknowledged", "already_gone", "second_cancel_create", "second_cancel_delete")
    expected_count = 0 if acknowledged else 1
    create_error = SandboxError("creation unknown")
    delete_error = SandboxError("deletion rejected")
    phase_owner = UUID(cast(str, bound["executor_dispatch_id"]))

    provider = cast(MockProvider, context.provider)
    provider_create = provider.create_sandbox
    provider_delete = provider.delete_sandbox

    async def hold_create(request: SandboxCreateRequest) -> Sandbox:
        assert creation_state.cleanup_pending
        sandbox = await provider_create(request)
        creating.set()
        await finish_creation.wait()
        if outcome == "unknown_create":
            raise create_error
        return sandbox

    async def hold_delete(sandbox_id: str) -> None:
        deleting.set()
        await finish_deletion.wait()
        if outcome == "already_gone":
            raise SandboxNotFoundError("gone")
        if outcome == "provider_delete_error":
            raise delete_error
        if outcome == "unexpected_delete_error":
            raise RuntimeError("deletion unknown")
        await provider_delete(sandbox_id)

    monkeypatch.setattr(provider, "create_sandbox", hold_create)
    monkeypatch.setattr(provider, "delete_sandbox", hold_delete)
    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter_provider_sandbox(stack, context, task, creation_state, Semaphore(1), postgres_engine)
        )
        try:
            await asyncio.wait_for(creating.wait(), timeout=10)
            _assert_reserved_demand(postgres_engine, context.pool_id, 1)
            reservation = _reservation(postgres_engine, task, task.started_at)
            assert reservation.phase_dispatch_id == phase_owner
            assert reservation.phase_token is not None
            _stop_run(postgres_engine, benchmark)
            entering.cancel()
            delivered = asyncio.Event()
            asyncio.get_running_loop().call_soon(delivered.set)
            await delivered.wait()
            if outcome == "second_cancel_create":
                entering.cancel()
                assert not entering.done()
                _assert_reserved_demand(postgres_engine, context.pool_id, 1)
            finish_creation.set()
            if outcome != "unknown_create":
                await asyncio.wait_for(deleting.wait(), timeout=10)
                assert creation_state.cleanup_pending
                _assert_reserved_demand(postgres_engine, context.pool_id, 1)
                assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id == phase_owner
                assert not entering.done()
                if outcome == "second_cancel_delete":
                    entering.cancel()
                    assert not entering.done()
                finish_deletion.set()
            error_type = (
                SandboxError if outcome in ("unknown_create", "provider_delete_error") else asyncio.CancelledError
            )
            with pytest.raises(error_type) as exc_info:
                await asyncio.wait_for(entering, timeout=10)
            if outcome == "unknown_create":
                assert exc_info.value is create_error
            elif outcome == "provider_delete_error":
                assert exc_info.value is delete_error
        finally:
            finish_creation.set()
            finish_deletion.set()
            if not entering.done():
                entering.cancel()
                await asyncio.gather(entering, return_exceptions=True)

    assert creation_state.cleanup_pending is (not acknowledged)
    _assert_reserved_demand(postgres_engine, context.pool_id, expected_count)
    reservation = _reservation(postgres_engine, task, task.started_at)
    assert reservation.phase_dispatch_id == (None if acknowledged else phase_owner)
    assert (reservation.phase_token is None) is acknowledged
    with Session(postgres_engine) as session:
        reservations = session.exec(
            select(SandboxBuildReservation).where(col(SandboxBuildReservation.released_at).is_(None))
        ).all()
        assert len(reservations) == expected_count
        if reservations:
            assert reservations[0].task_row_id == task.id
            assert reservations[0].attempt_started_at == task.started_at

    if acknowledged:
        with Session(postgres_engine) as session:
            run = session.get(Benchmark, benchmark.id)
            owner = session.get(Org, org.id)
            assert run is not None and owner is not None
            lifecycle_control.apply_stop_benchmark(run, session, force=True, org=owner, task_ids=[task.task_id])
            retry_control.reset_to_in_progress_status(
                run,
                session,
                retry=False,
                retry_mode=RetryMode.FROM_SCRATCH,
                rerun_task_ids=[task.task_id],
                org=owner,
                verified_task_ids=[task.task_id],
            )
            session.commit()
        new_attempt = _task(postgres_engine, task).started_at
        assert new_attempt > task.started_at
        recovery = bound_controls(benchmark, session=postgres_session)
        control = cast(QueueControl, recovery["queue_control"])
        stale = AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
        assert (await control.state(pool_id, stale)).status is None
        assert (
            await control.state(pool_id, stale.model_copy(update={"started_at": new_attempt}))
        ).status == TaskStatus.PENDING.value


@pytest.mark.description(
    "A failed reserved build commits disposition before cancellation and repeats a lost handoff without clearing a successor."
)
@pytest.mark.parametrize("reply", ["received", "lost-ownerless", "lost-successor"])
async def test_cancelled_failed_build_disposition_commits_before_phase_handoff(
    reply: str,
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
    runtime_services: RuntimeServices,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    org, benchmark, (task,) = _run(postgres_session, pool_id, [("handoff", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    task_control = cast(TaskControl, bound["task_control"])
    context = _context(cast(QueueControl, bound["queue_control"]), provider_pool_id, [], reserved=True)
    expected_owner = UUID(cast(str, bound["executor_dispatch_id"]))
    attempt = AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
    create_entered = asyncio.Event()
    release_create = asyncio.Event()
    delete_entered = asyncio.Event()
    release_delete = asyncio.Event()
    disposition_entered = threading.Event()
    disposition_release = threading.Event()
    handoff_entered = threading.Event()
    handoff_release = threading.Event()
    created: list[Sandbox] = []
    delete_calls: list[str] = []
    original_create = context.provider.create_sandbox
    original_benchmark_save = BenchmarkRepository.save
    original_reservation_save = SandboxBuildReservationRepository.save
    handoff_requests: list[bytes] = []
    successors: list[tuple[QueueControl, PhaseRef]] = []
    original_transport = context.control.control._http._transport

    async def lose_handoff_reply(request: httpx.Request) -> httpx.Response:
        response = await original_transport.handle_async_request(request)
        if not request.url.path.endswith("/queue-abandon"):
            return response
        handoff_requests.append(request.content)
        if len(handoff_requests) != 1 or reply == "received":
            return response
        assert response.status_code == 200
        assert state() == (TaskStatus.ERROR, None, None, False)
        if reply == "lost-successor":
            successor = bound_controls(benchmark, session=postgres_session)
            successor_control = cast(QueueControl, successor["queue_control"])
            (orphan,) = (await successor_control.orphans(pool_id)).orphans
            assert orphan.attempt == attempt
            successors.append((successor_control, orphan.phase))
        await response.aclose()
        raise httpx.ReadError("Controlled queue handoff reply loss", request=request)

    async def create(request: SandboxCreateRequest) -> Sandbox:
        sandbox = await original_create(request)
        created.append(sandbox)
        create_entered.set()
        await release_create.wait()

        return sandbox

    async def delete(sandbox_id: str) -> None:
        delete_calls.append(sandbox_id)
        delete_entered.set()
        await release_delete.wait()
        raise SandboxError("controlled deletion failure")

    def save_task(repository: BenchmarkRepository, *rows: Any) -> None:
        original_benchmark_save(repository, *rows)
        if any(isinstance(row, Task) and row.id == task.id and row.status == TaskStatus.ERROR for row in rows):
            disposition_entered.set()
            assert disposition_release.wait(timeout=20)

    def save_reservation(repository: SandboxBuildReservationRepository, reservation: SandboxBuildReservation) -> None:
        original_reservation_save(repository, reservation)
        if reservation.task_row_id == task.id and reservation.phase_dispatch_id is None:
            handoff_entered.set()
            assert handoff_release.wait(timeout=20)

    def state() -> tuple[TaskStatus, UUID | None, UUID | None, bool]:
        with Session(postgres_engine) as session:
            current_task = session.get(Task, task.id)
            reservation = session.get(SandboxBuildReservation, (task.id, task.started_at))
            assert current_task is not None and reservation is not None

            return (
                current_task.status,
                reservation.phase_dispatch_id,
                reservation.phase_token,
                reservation.released_at is not None,
            )

    monkeypatch.setattr(context.control.control._http, "_transport", httpx.MockTransport(lose_handoff_reply))

    monkeypatch.setattr(context.provider, "create_sandbox", create)
    monkeypatch.setattr(context.provider, "delete_sandbox", delete)
    monkeypatch.setattr(BenchmarkRepository, "save", save_task)
    monkeypatch.setattr(SandboxBuildReservationRepository, "save", save_reservation)
    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
    monkeypatch.setattr(coordinator, "_RECOVERY_SECONDS", 600.0)
    service = AsyncMock(spec=BenchmarkServiceClient)
    _use_real_sandbox_recovery(service)
    service.retrieve_task.return_value = RetrieveTaskResponse(
        source=_SOURCE, resources=_RESOURCES, problem_path="/tmp/problem.txt", cwd="/testbed"
    )
    worker_state = (await task_control.read(attempt)).task
    worker = asyncio.create_task(
        task_execution.process_task(
            worker_state,
            benchmark.managed_start_benchmark_request(),
            cast(BenchmarkServiceClient, service),
            benchmark.id,
            task.task_id,
            runtime_services,
            OrgContext(id=org.id, name=org.name, storage_key=org.storage_key),
            sandbox_provider_config=DaytonaProviderConfig(
                DAYTONA_API_KEY="controlled", DAYTONA_API_URL="http://localhost:8001", DAYTONA_TARGET="us"
            ),
            sandbox_provider=context.provider,
            creation_semaphore=Semaphore(1),
            control=task_control,
            queue_context=context,
            started_by_email=benchmark.started_by_email,
        )
    )
    scheduler = coordinator.SharedPoolScheduler(
        postgres_engine,
        provider_factory=lambda _reference: asyncio.sleep(0, result=context.provider),
        interval_seconds=0.02,
        rate_per_minute=0,
    )
    scheduler.start()
    try:
        async with asyncio.timeout(25):
            await create_entered.wait()
            worker.cancel()
            release_create.set()
            await delete_entered.wait()
            building = state()
            assert building[0] == TaskStatus.BUILDING
            assert building[1] == expected_owner and building[2] is not None and not building[3]
            release_delete.set()
            assert await asyncio.to_thread(disposition_entered.wait, 10)
            for _ in range(2):
                worker.cancel()
                delivered = asyncio.Event()
                asyncio.get_running_loop().call_soon(delivered.set)
                await delivered.wait()
                assert state() == building and not worker.done()
            disposition_release.set()
            assert await asyncio.to_thread(handoff_entered.wait, 10)
            after_disposition = state()
            assert after_disposition[0] == TaskStatus.ERROR
            assert after_disposition[1:] == building[1:]
            for _ in range(2):
                worker.cancel()
                delivered = asyncio.Event()
                asyncio.get_running_loop().call_soon(delivered.set)
                await delivered.wait()
                assert state() == after_disposition and not worker.done()
            handoff_release.set()
            with pytest.raises(asyncio.CancelledError):
                await worker
            settled = state()
            assert settled[0] == TaskStatus.ERROR and not settled[3]
            if reply == "lost-successor":
                successor_control, successor_phase = successors[0]
                assert settled[1] == successor_control.ref.dispatch_id
                assert settled[2] == successor_phase.token
                assert len(successor_control.active_snapshot()) == 1
            else:
                assert settled[1] is None and settled[2] is None
            assert _reservation_count(postgres_engine, pool_id) == 1
            assert len(created) == 1 and delete_calls == [created[0].id]
            assert len(handoff_requests) == (1 if reply == "received" else 2)
            if reply != "received":
                assert handoff_requests[0] == handoff_requests[1]
            assert not context.control.active_snapshot()
    finally:
        disposition_release.set()
        handoff_release.set()
        release_create.set()
        release_delete.set()
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await scheduler.stop()


@pytest.mark.description("A database error on promotion cleans up before releasing ownership.")
@pytest.mark.parametrize(
    ("reserved", "delete_fails"),
    [
        pytest.param(True, False, id="reserved-acknowledged"),
        pytest.param(True, True, id="reserved-provider-delete-error"),
        pytest.param(False, False, id="legacy-acknowledged"),
    ],
)
async def test_promotion_database_error_cleans_up_before_releasing_ownership(
    reserved: bool,
    delete_fails: bool,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id)) if reserved else queue_pool_id(provider_pool_id)
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("promotion-error", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(cast(QueueControl, bound["queue_control"]), provider_pool_id, [], reserved=reserved)
    creation_state = SandboxCreationState(cleanup_pending=False)
    expected_count = 1 if reserved and delete_fails else 0
    cleanup_ownership: list[bool] = []
    phase_owner = UUID(cast(str, bound["executor_dispatch_id"]))
    provider = cast(MockProvider, context.provider)
    provider_delete = provider.delete_sandbox

    async def observe_delete(sandbox_id: str) -> None:
        assert creation_state.cleanup_pending
        assert _task(postgres_engine, task).status == TaskStatus.BUILDING
        if reserved:
            reservation = _reservation(postgres_engine, task, task.started_at)
            cleanup_ownership.append(
                reservation.phase_dispatch_id == phase_owner and reservation.phase_token is not None
            )
        else:
            building = _task(postgres_engine, task)
            cleanup_ownership.append(
                building.build_phase_dispatch_id == phase_owner and building.build_phase_token is not None
            )
        if delete_fails:
            raise SandboxError("deletion rejected")
        await provider_delete(sandbox_id)

    monkeypatch.setattr(provider, "delete_sandbox", observe_delete)
    function_name = f"scheduler_promotion_error_{task.id.hex}"
    trigger_name = f"promotion_error_{task.id.hex}"
    promotion_error = "scheduler promotion failed after create"
    try:
        with postgres_engine.begin() as connection:
            connection.execute(
                text(
                    f"""CREATE FUNCTION {function_name}() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        RAISE EXCEPTION '{promotion_error}';
                    END;
                    $$"""
                )
            )
            connection.execute(
                text(
                    f"""CREATE TRIGGER {trigger_name} BEFORE UPDATE OF status ON task
                    FOR EACH ROW WHEN (
                        OLD.id = '{task.id}'::uuid
                        AND OLD.status = 'BUILDING' AND NEW.status = 'IN_PROGRESS'
                    ) EXECUTE FUNCTION {function_name}()"""
                )
            )

        async with AsyncExitStack() as stack:
            with pytest.raises(DBAPIError, match=promotion_error):
                await asyncio.wait_for(
                    _enter_provider_sandbox(stack, context, task, creation_state, Semaphore(1), postgres_engine),
                    timeout=10,
                )
            assert cleanup_ownership == [True]
            assert creation_state.cleanup_pending is delete_fails
            _assert_reserved_demand(postgres_engine, context.pool_id, expected_count)

        building = _task(postgres_engine, task)
        if delete_fails or not reserved:
            assert building.status == TaskStatus.BUILDING
        if delete_fails:
            if reserved:
                assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id == phase_owner
            else:
                assert building.build_phase_dispatch_id == phase_owner
        with Session(postgres_engine) as session:
            dispatch = session.get(ExecutorDispatch, phase_owner)
            assert dispatch is not None
            dispatch.status = ExecutorDispatchStatus.FAILED
            session.commit()
        recovery = bound_controls(benchmark, session=postgres_session)
        context = _context(cast(QueueControl, recovery["queue_control"]), provider_pool_id, [], reserved=reserved)
        if delete_fails:
            monkeypatch.setattr(
                context.provider, "get_sandbox", AsyncMock(side_effect=SandboxError("provider unavailable"))
            )
        await admission.recover_queued_pool(context)
        recovered = _task(postgres_engine, task)
        assert recovered.status == (TaskStatus.BUILDING if delete_fails else TaskStatus.PENDING)
        _assert_reserved_demand(postgres_engine, context.pool_id, expected_count)
        with Session(postgres_engine) as session:
            if reserved:
                _prepare(
                    session,
                    context.pool_id,
                    recovered,
                    dispatch_id=UUID(cast(str, recovery["executor_dispatch_id"])),
                )
                assert SchedulerRepository(session).grant_prepared_tasks(
                    context.pool_id, {("region-a", "sysbox"): _CAPACITY}, SchedulerRepository(session).database_now(), 1
                ) == (0 if delete_fails else 1)
            else:
                assert SchedulerRepository(session).eligible_task_is(
                    context.pool_id, task.id, recovered.started_at, 1
                ) is (not delete_fails)
    finally:
        with postgres_engine.begin() as connection:
            connection.execute(text(f"DROP TRIGGER IF EXISTS {trigger_name} ON task"))
            connection.execute(text(f"DROP FUNCTION IF EXISTS {function_name}()"))


@pytest.mark.description(
    "Cancelling after the capacity reads but before create leaves no reserved demand and frees the build and pool locks."
)
async def test_reserved_cancellation_before_provider_entry_releases_demand(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    events: list[str] = []
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("waiting", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(cast(QueueControl, bound["queue_control"]), provider_pool_id, events, reserved=True)
    creation_state = SandboxCreationState(cleanup_pending=False)
    waiting = asyncio.Event()

    class ObservedSemaphore(Semaphore):
        async def acquire(self) -> Literal[True]:
            waiting.set()
            return await super().acquire()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter_provider_sandbox(stack, context, task, creation_state, ObservedSemaphore(0), postgres_engine)
        )
        await asyncio.wait_for(waiting.wait(), timeout=10)
        _assert_reserved_demand(postgres_engine, context.pool_id, 1)
        assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id == UUID(
            cast(str, bound["executor_dispatch_id"])
        )
        entering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await entering
    assert not creation_state.cleanup_pending
    assert events[:2] == ["capacity", "read_capacity"]
    assert all(event == "read_capacity" for event in events[2:])
    _assert_reserved_demand(postgres_engine, context.pool_id, 0)
    assert _reservation(postgres_engine, task, task.started_at).phase_token is None
    assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id is None


@pytest.mark.description(
    "An interrupted reserved promotion releases its demand, and recovery leaves the task `BUILDING` if stopped or `PENDING` otherwise."
)
@pytest.mark.parametrize("interruption", ["superseded", "stopped", "revoked"])
async def test_reserved_promotion_requires_exact_attempt_live_run_and_authority(
    interruption: str,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    org, benchmark, (task,) = _run(postgres_session, pool_id, [("fenced", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(cast(QueueControl, bound["queue_control"]), provider_pool_id, events, reserved=True)
    phase_owner = UUID(cast(str, bound["executor_dispatch_id"]))

    async def interrupt() -> None:
        if interruption == "superseded":
            with Session(postgres_engine) as session:
                row = session.get(Benchmark, benchmark.id)
                owner = session.get(Org, org.id)
                assert row is not None and owner is not None
                lifecycle_control.apply_stop_benchmark(row, session, force=True, org=owner, task_ids=[task.task_id])
                retry_control.reset_to_in_progress_status(
                    row,
                    session,
                    retry=False,
                    retry_mode=RetryMode.FROM_SCRATCH,
                    rerun_task_ids=[task.task_id],
                    org=owner,
                    verified_task_ids=[task.task_id],
                )
                session.commit()
        elif interruption == "stopped":
            _stop_run(postgres_engine, benchmark)
        else:
            with Session(postgres_engine) as session:
                dispatch = session.get(ExecutorDispatch, phase_owner)
                assert dispatch is not None
                dispatch.status = ExecutorDispatchStatus.FAILED
                session.commit()

    cleaning = asyncio.Event()
    finish_cleanup = asyncio.Event()

    provider = cast(MockProvider, context.provider)
    provider_create = provider.create_sandbox
    provider_delete = provider.delete_sandbox

    async def hold_create(request: SandboxCreateRequest) -> Sandbox:
        sandbox = await provider_create(request)
        await interrupt()
        return sandbox

    async def hold_cleanup(sandbox_id: str) -> None:
        cleaning.set()
        await finish_cleanup.wait()
        await provider_delete(sandbox_id)

    monkeypatch.setattr(provider, "create_sandbox", hold_create)
    monkeypatch.setattr(provider, "delete_sandbox", hold_cleanup)
    creation_state = SandboxCreationState(cleanup_pending=False)
    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter_provider_sandbox(stack, context, task, creation_state, Semaphore(1), postgres_engine)
        )
        try:
            await asyncio.wait_for(cleaning.wait(), timeout=10)
            assert creation_state.cleanup_pending
            _assert_reserved_demand(postgres_engine, context.pool_id, 1)
            assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id == phase_owner
            assert _reservation(postgres_engine, task, task.started_at).phase_token is not None
            expected_status = TaskStatus.PENDING if interruption == "superseded" else TaskStatus.BUILDING
            assert _task(postgres_engine, task).status == expected_status
        finally:
            finish_cleanup.set()
        sandbox = await asyncio.wait_for(entering, timeout=10)
        assert sandbox is None

    assert not creation_state.cleanup_pending
    assert events[0] == "capacity"
    assert events[1:-2] and all(event == "read_capacity" for event in events[1:-2])
    assert events[-2:] == ["create", "cleanup"]
    if interruption == "superseded":
        assert _task(postgres_engine, task).started_at > _ATTEMPT
        with Session(postgres_engine) as session:
            dispatch = session.get(ExecutorDispatch, phase_owner)
            assert dispatch is not None
            dispatch.status = ExecutorDispatchStatus.FAILED
            session.commit()
    if interruption != "revoked":
        assert _task(postgres_engine, task).status == expected_status
    _assert_reserved_demand(postgres_engine, pool_id, 0)

    recovery = bound_controls(benchmark, session=postgres_session)
    control = cast(QueueControl, recovery["queue_control"])
    context = _context(control, provider_pool_id, events, reserved=True)
    if interruption == "superseded":
        expected_name = f"queued-{task.id.hex}-{int(_ATTEMPT.replace(tzinfo=UTC).timestamp() * 1_000_000):x}"

        async def find_old_sandbox(name: str) -> Sandbox:
            assert name == expected_name
            assert _reservation(postgres_engine, task, _ATTEMPT).phase_dispatch_id == control.ref.dispatch_id
            raise SandboxNotFoundError("old attempt sandbox absent")

        monkeypatch.setattr(context.provider, "get_sandbox", find_old_sandbox)
    await admission.recover_queued_pool(context)

    recovered_status = TaskStatus.BUILDING if interruption == "stopped" else TaskStatus.PENDING
    assert _task(postgres_engine, task).status == recovered_status
    _assert_reserved_demand(postgres_engine, pool_id, 0)
    if interruption == "superseded":
        stale = AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=_ATTEMPT)
        assert (await control.state(pool_id, stale)).status is None
        assert await control.accept(pool_id, stale) is None
        current = stale.model_copy(update={"started_at": _task(postgres_engine, task).started_at})
        assert (await control.state(pool_id, current)).status == TaskStatus.PENDING.value


@pytest.mark.description(
    "Concurrent queue state reads for one pool share one `queue-states` request and keep per-attempt results."
)
async def test_concurrent_queue_state_reads_share_one_request(
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    pool_id = reserved_queue_pool_id(queue_pool_id(f"daytona:{uuid4()}"))
    _, benchmark, tasks = _run(
        postgres_session,
        pool_id,
        [("pending", TaskStatus.PENDING, _ATTEMPT), ("building", TaskStatus.BUILDING, _ATTEMPT)],
    )
    control = cast(QueueControl, bound_controls(benchmark, session=postgres_session)["queue_control"])
    routes: list[str] = []
    call = control.control.call

    async def record(route: str, *args: Any, **kwargs: Any) -> Any:
        routes.append(route)
        return await call(route, *args, **kwargs)

    monkeypatch.setattr(control.control, "call", record)
    attempts = [
        AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=_ATTEMPT) for task in tasks
    ]
    stale = attempts[0].model_copy(update={"started_at": _ATTEMPT - timedelta(minutes=1)})

    states = await asyncio.gather(*(control.state(pool_id, attempt) for attempt in [*attempts, stale]))

    assert [state.status for state in states] == [TaskStatus.PENDING.value, TaskStatus.BUILDING.value, None]
    assert routes == ["queue-states"]


@pytest.mark.description("Orphan recovery keeps phase ownership in the original pool across benchmarks.")
async def test_cross_benchmark_orphan_recovery_keeps_pool_and_phase_ownership(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    unrelated_pool_id = queue_pool_id(provider_pool_id)
    assert pool_id != unrelated_pool_id
    assert queue_pool_lock_id(pool_id) == unrelated_pool_id
    assert reserved_queue_pool_id(pool_id) == pool_id
    _, source_benchmark, (source_task,) = _run(postgres_session, pool_id, [("source", TaskStatus.PENDING, _ATTEMPT)])
    _, recovery_benchmark, (other_task,) = _run(postgres_session, pool_id, [("other", TaskStatus.PENDING, _ATTEMPT)])
    _, _, (unrelated_task,) = _run(postgres_session, unrelated_pool_id, [("unrelated", TaskStatus.PENDING, _ATTEMPT)])
    control, _ = await _recovery_orphan_control(
        postgres_session,
        source_benchmark,
        recovery_benchmark,
        source_task,
        pool_id,
        provider_pool_id,
        bound_controls,
    )
    context = _context(control, provider_pool_id, [], reserved=True)
    assert context.serves_queue(pool_id)
    assert not context.serves_queue(queue_pool_id(f"daytona:{uuid4()}"))
    assert (
        _reservation(postgres_engine, source_task, source_task.started_at).phase_dispatch_id != control.ref.dispatch_id
    )
    await admission.recover_queued_pool(context)
    assert _task(postgres_engine, source_task).status == TaskStatus.PENDING
    assert _reservation(postgres_engine, source_task, source_task.started_at).phase_dispatch_id is None
    assert _task(postgres_engine, other_task).status == TaskStatus.PENDING
    assert _task(postgres_engine, unrelated_task).status == TaskStatus.PENDING
    assert _reservation_count(postgres_engine, pool_id) == 0


@pytest.mark.description(
    "Recovery keeps builds changed after selection, and an exited or expired predecessor cannot clear the successor's phase."
)
@pytest.mark.usefixtures("scheduler_schema_revision")
@pytest.mark.parametrize(
    ("interruption", "expected_status"),
    [
        ("promoted", TaskStatus.IN_PROGRESS),
        ("stopped", TaskStatus.BUILDING),
    ],
)
async def test_recovery_preserves_builds_changed_after_candidate_selection(
    interruption: str,
    expected_status: TaskStatus,
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (task,) = _run(
        postgres_session,
        pool_id,
        [("racing", TaskStatus.PENDING, _ATTEMPT)],
        executor=ExecutorDispatchStatus.RUNNING,
    )
    control, original_control = await _recovery_orphan_control(
        postgres_session,
        benchmark,
        benchmark,
        task,
        pool_id,
        provider_pool_id,
        bound_controls,
    )
    (orphan,) = (await control.orphans(pool_id)).orphans
    assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id == control.ref.dispatch_id
    assert _reservation(postgres_engine, task, task.started_at).phase_token == orphan.phase.token
    assert not (await original_control.control.call("queue-exit", original_control.ref, QueueBuildResponse)).started
    assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id == control.ref.dispatch_id
    assert _reservation(postgres_engine, task, task.started_at).phase_token == orphan.phase.token

    if interruption == "promoted":
        assert (await control.build("queue-finish", pool_id, orphan.attempt, orphan.phase)).started
        with pytest.raises(ExecutionAuthorityRevoked):
            await control.reconcile(pool_id, orphan.attempt, orphan.phase, cleaned=True)
    else:
        _stop_run(postgres_engine, benchmark)
        await control.reconcile(pool_id, orphan.attempt, orphan.phase, cleaned=True)

    recovered = _task(postgres_engine, task)
    assert recovered.status == expected_status
    assert recovered.started_at == _ATTEMPT
    assert _reservation(postgres_engine, task, task.started_at).phase_dispatch_id is None
    assert _reservation_count(postgres_engine, pool_id) == 0


@pytest.mark.description("Reserved snapshot builds reserve provider demand during parallel creation.")
async def test_reserved_snapshot_builds_reserve_provider_demand_during_parallel_creation(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    demand = SandboxReservationDemand(
        target_id="region-b",
        sandbox_class="vm",
        vcpu=3,
        memory=6,
        disk=20.5,
        gpu=0,
    )
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (first, second) = _run(
        postgres_session,
        pool_id,
        [
            ("first", TaskStatus.PENDING, _ATTEMPT),
            ("second", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(
        cast(QueueControl, bound["queue_control"]), provider_pool_id, events, reserved=True, demand=demand
    )
    cast(MockProvider, context.provider).domains = [
        SandboxCapacityDomain(target_id="region-a", sandbox_class="sysbox", capacity=_CAPACITY),
        SandboxCapacityDomain(
            target_id="region-b",
            sandbox_class="vm",
            capacity=_CAPACITY.model_copy(update={"disk": ResourceCapacity(total=100, used=0)}),
        ),
    ]
    creating = 0
    both_creating = asyncio.Event()
    finish_creating = asyncio.Event()
    observed: list[tuple[int, tuple[float, float, float], set[UUID]]] = []

    async def hold_create() -> None:
        nonlocal creating
        creating += 1
        if creating == 2:
            with Session(postgres_engine) as session:
                reserved = SchedulerRepository(session).active_reservations(context.pool_id)
            observed.append(
                (
                    reserved.count,
                    (reserved.vcpu, reserved.memory, reserved.disk),
                    {
                        _reservation(postgres_engine, task, task.started_at).phase_dispatch_id
                        for task in (first, second)
                    },
                )
            )
            both_creating.set()
        await finish_creating.wait()

    source = SnapshotSource(snapshot="snapshot")
    async with AsyncExitStack() as first_stack, AsyncExitStack() as second_stack:
        entering = asyncio.gather(
            _enter(first_stack, context, first, events, postgres_engine, source=source, on_create=hold_create),
            _enter(second_stack, context, second, events, postgres_engine, source=source, on_create=hold_create),
        )
        try:
            await asyncio.wait_for(both_creating.wait(), timeout=10)
        finally:
            finish_creating.set()
        sandboxes = await asyncio.wait_for(entering, timeout=10)

        assert all(sandboxes)
        assert {_task(postgres_engine, task).status for task in (first, second)} == {TaskStatus.IN_PROGRESS}

    assert observed == [(2, (6, 12, 41), {UUID(cast(str, bound["executor_dispatch_id"]))})]
    with Session(postgres_engine) as session:
        rows = session.exec(
            select(SandboxBuildReservation).where(
                col(SandboxBuildReservation.pool_id) == queue_pool_lock_id(context.pool_id)
            )
        ).all()

    assert {(row.target_id, row.sandbox_class, row.requested_disk, row.requested_gpu) for row in rows} == {
        ("region-b", "vm", 20.5, 0),
    }
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert events.count("create") == 2


@pytest.mark.description(
    "A snapshot metadata error leaves the task `PENDING` with no reservation, no candidate, and no create call."
)
async def test_snapshot_metadata_error_never_becomes_a_requested_resource_reservation(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    events: list[str] = []
    error = SandboxError("snapshot metadata unavailable")
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("unknown", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(
        cast(QueueControl, bound["queue_control"]), provider_pool_id, events, reserved=True, demand=error
    )

    async with AsyncExitStack() as stack:
        with pytest.raises(SandboxError) as raised:
            await _enter(stack, context, task, events, postgres_engine, source=SnapshotSource(snapshot="snapshot"))

    assert raised.value is error
    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert _candidate_count(postgres_engine, context.pool_id) == 0
    assert "create" not in events


@pytest.mark.description(
    "A provider rejection publishes a non-ready candidate and frees the preparation slot without a reservation."
)
@pytest.mark.parametrize("outcome", ["ready", "cancelled"])
async def test_provider_rejection_publishes_candidate_and_releases_preparation_slot(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
    outcome: str,
) -> None:
    events: list[str] = []
    demand = _DEMAND.model_copy(update={"vcpu": 2.5})
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("rejected", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(
        cast(QueueControl, bound["queue_control"]),
        provider_pool_id,
        events,
        reserved=True,
        capacity=(False, True),
        demand=demand,
    )
    preparation_slot = Semaphore(1)
    creation_state = SandboxCreationState(cleanup_pending=False)

    async with AsyncExitStack() as stack:

        async def enter() -> Sandbox | None:
            async with AsyncExitStack() as preparation_stack:
                await preparation_stack.enter_async_context(preparation_slot)
                return await admission.enter_queued_sandbox(
                    stack=stack,
                    context=context,
                    attempt=AttemptRef(
                        org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at
                    ),
                    source=SnapshotSource(snapshot="provider-rejected"),
                    resources=_RESOURCES,
                    creation_state=creation_state,
                    create=_sandbox(events, creation_state=creation_state),
                    preparation_stack=preparation_stack,
                    on_phase_claimed=lambda _phase: None,
                    on_phase_released=lambda _started: None,
                    on_phase_abandon=lambda _phase: None,
                )

        entering = asyncio.create_task(enter())
        try:
            async with asyncio.timeout(10):
                while _candidate_count(postgres_engine, context.pool_id) != 1 or preparation_slot.locked():
                    await asyncio.sleep(0.01)
            with Session(postgres_engine) as session:
                candidate = session.get(SandboxAdmissionCandidate, (task.id, task.started_at))
                assert candidate is not None
                assert candidate.admission_ready is False
                assert candidate.requested_vcpu == demand.vcpu
                assert (candidate.target_id, candidate.sandbox_class) == (demand.target_id, demand.sandbox_class)
            async with asyncio.timeout(10):
                await preparation_slot.acquire()
            preparation_slot.release()
            assert not preparation_slot.locked()
            assert _task(postgres_engine, task).status == TaskStatus.PENDING
            assert _reservation_count(postgres_engine, context.pool_id) == 0
            assert events == ["capacity"]
            assert _task(postgres_engine, task).build_phase_dispatch_id is None

            if outcome == "ready":
                with Session(postgres_engine) as session:
                    assert (
                        SchedulerRepository(session).request_readiness_checks(
                            context.pool_id, limit=1, retry_delay=timedelta(0)
                        )
                        == 1
                    )
                    session.commit()
                async with _drive_queue(postgres_engine, context):
                    assert await asyncio.wait_for(entering, timeout=10) is not None
                assert _task(postgres_engine, task).status == TaskStatus.IN_PROGRESS
                assert events.count("capacity") == 2
                assert events.count("create") == 1
            else:
                entering.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await entering
                assert _task(postgres_engine, task).status == TaskStatus.PENDING
                assert "create" not in events
        finally:
            if not entering.done():
                entering.cancel()
            await asyncio.gather(entering, return_exceptions=True)

    assert _candidate_count(postgres_engine, context.pool_id) == 0
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert not preparation_slot.locked()
    assert _task(postgres_engine, task).build_phase_dispatch_id is None


@pytest.mark.description("Cancelling during preparation makes no create call and frees the build lock.")
@pytest.mark.parametrize("stage", ["metadata", "provider-check"])
async def test_cancellation_during_preparation_leaves_no_candidate_or_reservation(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
    stage: str,
) -> None:
    events: list[str] = []
    started = asyncio.Event()
    hold = asyncio.Event()

    async def block() -> None:
        started.set()
        await hold.wait()

    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("cancelled", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(
        cast(QueueControl, bound["queue_control"]),
        provider_pool_id,
        events,
        reserved=True,
        on_check=block if stage == "provider-check" else None,
    )
    if stage == "metadata":

        async def prepare(_source: SandboxSource, _resources: Resources) -> SandboxReservationDemand:
            await block()
            return _DEMAND

        monkeypatch.setattr(context.provider, "reservable_demand", prepare)

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(_enter(stack, context, task, events, postgres_engine))
        try:
            await asyncio.wait_for(started.wait(), timeout=10)
            assert _candidate_count(postgres_engine, context.pool_id) == 0
            assert _task(postgres_engine, task).build_phase_dispatch_id is None
        finally:
            entering.cancel()
            with pytest.raises(asyncio.CancelledError):
                await entering

    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert _candidate_count(postgres_engine, context.pool_id) == 0
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert "create" not in events
    assert _task(postgres_engine, task).build_phase_dispatch_id is None


@pytest.mark.description("Cancelling while capacity is unavailable removes the prepared candidate and phase.")
async def test_cancellation_while_waiting_for_capacity_removes_the_prepared_candidate(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    events: list[str] = []
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("cancelled", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(
        cast(QueueControl, bound["queue_control"]),
        provider_pool_id,
        events,
        reserved=True,
        snapshot=_CAPACITY.model_copy(update={"cpu": ResourceCapacity(total=0, used=0)}),
    )
    waiting = asyncio.Event()
    hold = asyncio.Event()

    async def poll(_context: admission.SandboxQueueContext) -> None:
        waiting.set()
        await hold.wait()

    monkeypatch.setattr(admission, "_poll_sleep", poll)
    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(_enter(stack, context, task, events, postgres_engine))
        try:
            await asyncio.wait_for(waiting.wait(), timeout=10)
            assert _candidate_count(postgres_engine, context.pool_id) == 1
            assert _reservation_count(postgres_engine, context.pool_id) == 0
            assert _task(postgres_engine, task).build_phase_dispatch_id is None
        finally:
            entering.cancel()
            with pytest.raises(asyncio.CancelledError):
                await entering

    assert _candidate_count(postgres_engine, context.pool_id) == 0
    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert _task(postgres_engine, task).build_phase_dispatch_id is None


@pytest.mark.description("A reserved build waits for a readable capacity snapshot before sandbox creation.")
async def test_reserved_build_waits_for_a_readable_capacity_snapshot(
    postgres_engine: Engine,
    postgres_session: Session,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    pool_id = reserved_queue_pool_id(queue_pool_id(provider_pool_id))
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("waiting", TaskStatus.PENDING, _ATTEMPT)])
    bound = bound_controls(benchmark, session=postgres_session)
    context = _context(
        cast(QueueControl, bound["queue_control"]),
        provider_pool_id,
        events,
        reserved=True,
        snapshot=SandboxError("capacity unavailable"),
    )
    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(_enter(stack, context, task, events, postgres_engine))
        async with asyncio.timeout(10):
            while _candidate_count(postgres_engine, context.pool_id) == 0 or "read_capacity" not in events:
                await asyncio.sleep(0.01)
        assert _task(postgres_engine, task).status == TaskStatus.PENDING
        assert _task(postgres_engine, task).build_phase_dispatch_id is None
        assert not entering.done()
        cast(MockProvider, context.provider).snapshot = _CAPACITY
        sandbox = await asyncio.wait_for(entering, timeout=10)

        assert sandbox is not None
        assert _task(postgres_engine, task).status == TaskStatus.IN_PROGRESS

    assert events.count("read_capacity") >= 2
    assert events.index("read_capacity") < events.index("create")
    assert events[-1] == "cleanup"


@pytest.mark.description("A legacy build waits for an active reserved build to finish before claiming the pool.")
@pytest.mark.parametrize("fits", [True, False])
async def test_legacy_queue_build_waits_for_all_reservations_to_drain(
    fits: bool,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    snapshot = (
        _CAPACITY if fits else _CAPACITY.model_copy(update={"cpu": ResourceCapacity(total=_RESOURCES.vcpu, used=0)})
    )
    legacy_pool = queue_pool_id(provider_pool_id)
    reserved_pool = reserved_queue_pool_id(legacy_pool)
    _, source_benchmark, (reserved_task,) = _run(
        postgres_session, reserved_pool, [("reserved", TaskStatus.PENDING, _ATTEMPT)]
    )
    _, benchmark, (snapshot_task,) = _run(
        postgres_session, legacy_pool, [("snapshot", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1))]
    )
    source = bound_controls(source_benchmark, session=postgres_session)
    bound = bound_controls(benchmark, session=postgres_session)
    source_context = _context(cast(QueueControl, source["queue_control"]), provider_pool_id, [], reserved=True)
    context = _context(cast(QueueControl, bound["queue_control"]), provider_pool_id, events, snapshot=snapshot)
    creating = asyncio.Event()
    finish_creation = asyncio.Event()
    polled: list[tuple[TaskStatus, int]] = []

    async def hold_source_creation() -> None:
        creating.set()
        await finish_creation.wait()

    async with AsyncExitStack() as source_stack, AsyncExitStack() as stack:
        source_runner = asyncio.create_task(
            _enter(source_stack, source_context, reserved_task, [], postgres_engine, on_create=hold_source_creation)
        )
        await asyncio.wait_for(creating.wait(), timeout=10)
        assert (
            _reservation(postgres_engine, reserved_task, reserved_task.started_at).phase_dispatch_id
            == source_context.control.ref.dispatch_id
        )
        assert _reservation_count(postgres_engine, reserved_pool) == 1

        async def promote_reserved_build(_context: admission.SandboxQueueContext) -> None:
            polled.append(
                (_task(postgres_engine, snapshot_task).status, _reservation_count(postgres_engine, legacy_pool))
            )
            finish_creation.set()
            assert await asyncio.wait_for(source_runner, timeout=10) is not None

        monkeypatch.setattr(admission, "_poll_sleep", promote_reserved_build)
        sandbox = await _enter(
            stack, context, snapshot_task, events, postgres_engine, source=SnapshotSource(snapshot="snapshot")
        )
        assert sandbox is not None
        assert _task(postgres_engine, snapshot_task).status == TaskStatus.IN_PROGRESS
        assert _task(postgres_engine, reserved_task).status == TaskStatus.IN_PROGRESS

    assert polled == [(TaskStatus.PENDING, 1)]
    assert _reservation_count(postgres_engine, legacy_pool) == 0
    assert events[0] == "capacity"
    assert events[-2:] == ["create", "cleanup"]


@pytest.fixture
async def runtime_services(aws_runtime: AWSRuntime) -> AsyncGenerator[RuntimeServices, None]:
    runtime = CloudRuntimeFactory.create_runtime(aws_runtime)
    runtime.objects = AsyncMock(spec=type(runtime.objects))
    runtime.secrets = AsyncMock(spec=type(runtime.secrets))
    runtime.logs = Mock(spec=type(runtime.logs))
    runtime.log_reader = Mock(spec=type(runtime.log_reader))
    yield runtime


@pytest.mark.description("Evaluation uses the saved project grading root.")
@pytest.mark.parametrize(
    "grading_path",
    ["project", "old-service", "byok", "custom-service", "projectless-retag", "mismatched-root", "mismatched-url"],
)
async def test_evaluation_uses_saved_project_grading_root(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
    runtime_services: RuntimeServices,
    grading_path: str,
) -> None:
    events: list[str] = []
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (task,) = _run(
        postgres_session, queue_pool_id(provider_pool_id), [("task_0", TaskStatus.PENDING, _ATTEMPT)]
    )
    org.name = "vals.ai"
    saved_project = Project(org_id=org.id, handle="selected", name="Selected", spend_type=SpendType.INTERNAL)
    current_project = Project(org_id=org.id, handle="retagged", name="Retagged", spend_type=SpendType.INTERNAL)
    saved_project.gateway_key_id = uuid4()
    current_project.gateway_key_id = uuid4()
    postgres_session.add_all([saved_project, current_project])
    postgres_session.flush()
    postgres_session.add_all(
        [
            RunProject(
                benchmark_id=benchmark.id,
                project_id=current_project.id if grading_path == "projectless-retag" else saved_project.id,
            ),
            RunProject(
                benchmark_id=benchmark.id,
                project_id=saved_project.id if grading_path == "projectless-retag" else current_project.id,
            ),
        ]
    )
    reference = f"{tenant_secret_prefix(org.id)}model-gateway-{saved_project.gateway_key_id}"
    if grading_path == "byok":
        reference = f"{tenant_secret_prefix(org.id)}own-gateway"
    if grading_path == "custom-service":
        benchmark.custom_benchmark_service = "https://custom.example"
    benchmark.arguments = benchmark.arguments.model_copy(
        update={
            "contract": benchmark.arguments.contract.model_copy(
                update={"secrets": {"MODEL_GATEWAY_URL": reference, "MODEL_GATEWAY_API_KEY": reference}}
            ),
            "selected_project_gateway_secret_ref": None if grading_path == "projectless-retag" else reference,
        }
    )
    postgres_session.add_all([org, benchmark])
    postgres_session.commit()
    if grading_path in {"mismatched-root", "mismatched-url"}:
        replacement = f"{tenant_secret_prefix(org.id)}model-gateway-{current_project.gateway_key_id}"
        updated_secrets = {"MODEL_GATEWAY_URL": replacement}
        if grading_path == "mismatched-root":
            updated_secrets["MODEL_GATEWAY_API_KEY"] = replacement
        update_benchmark_resume_arguments(
            benchmark.id,
            postgres_session,
            org,
            secrets=updated_secrets,
            concurrency=None,
            benchmark_url=None,
            lambda_function=None,
        )
        postgres_session.commit()
        assert benchmark.arguments.selected_project_gateway_secret_ref == reference
        assert benchmark.arguments.contract.secrets["MODEL_GATEWAY_URL"] == replacement
    controls = bound_controls(benchmark, session=postgres_session)
    task_control = cast(TaskControl, controls["task_control"])
    context = _context(cast(QueueControl, controls["queue_control"]), provider_pool_id, events)
    task_state = (
        await task_control.read(
            AttemptRef(org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
        )
    ).task

    async def resolve(_names: dict[str, str], _store: Any) -> dict[str, str]:
        assert _names["MODEL_GATEWAY_API_KEY"] == benchmark.arguments.contract.secrets["MODEL_GATEWAY_API_KEY"]
        return {"MODEL_GATEWAY_URL": "https://gateway.test", "MODEL_GATEWAY_API_KEY": "saved-project-root"}

    gateway_calls: list[tuple[str, dict[str, Any], str]] = []

    def gateway(request: httpx.Request) -> httpx.Response:
        gateway_calls.append((request.url.path, json.loads(request.content), request.headers["Authorization"]))
        if request.url.path == "/service-auth":
            return httpx.Response(
                200, json={"token": "grading-child", "lease_id": "grading-lease", "expires_at": 1_800_000_000}
            )
        return httpx.Response(200)

    monkeypatch.setattr(model_gateway, "_client", httpx.AsyncClient(transport=httpx.MockTransport(gateway)))
    monkeypatch.setattr(task_execution, "resolve_secrets", resolve)

    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())

    def mock_create_sandbox(
        *_args: Any, creation_state: SandboxCreationState, **_kwargs: Any
    ) -> AbstractAsyncContextManager[Sandbox]:
        return _sandbox(events, creation_state=creation_state)()

    monkeypatch.setattr(task_execution, "create_sandbox", mock_create_sandbox)
    monkeypatch.setattr(task_execution, "upload_agent_artifacts", AsyncMock())
    monkeypatch.setattr(task_execution, "install_agent_dependencies", AsyncMock())
    monkeypatch.setattr(task_execution, "run_agent", AsyncMock(side_effect=_run_agent_success))
    service = AsyncMock(spec=BenchmarkServiceClient)
    _use_real_sandbox_recovery(service)
    service.retrieve_task.return_value = RetrieveTaskResponse(
        source=_SOURCE, resources=_RESOURCES, problem_path="/tmp/problem.txt", cwd="/testbed"
    )
    service.setup_task.return_value = SetupTaskResponse(status="ok")
    service.grading_requirements.return_value = ["openai/gpt-5", "anthropic/claude-sonnet"]
    if grading_path == "old-service":
        service.grading_requirements.side_effect = BenchmarkServiceError("Not found", status_code=404)
    service.evaluate_instance.return_value = {"score": 1.0}

    result = await task_execution.process_task(
        task_state,
        benchmark.managed_start_benchmark_request(),
        cast(BenchmarkServiceClient, service),
        benchmark.id,
        task.task_id,
        runtime_services,
        OrgContext(id=org.id, name=org.name, storage_key=org.storage_key),
        sandbox_provider_config=DaytonaProviderConfig(
            DAYTONA_API_KEY="test-api-key", DAYTONA_API_URL="http://localhost:8001", DAYTONA_TARGET="us"
        ),
        sandbox_provider=context.provider,
        creation_semaphore=Semaphore(1),
        queue_context=context,
        control=task_control,
        started_by_email=benchmark.started_by_email,
    )

    assert result == {"task_0": {"score": 1.0}}
    assert _task(postgres_engine, task).status == TaskStatus.FINISHED
    evaluation = service.evaluate_instance.await_args
    assert evaluation.kwargs["grading_token"] == ("grading-child" if grading_path == "project" else None)
    if grading_path in {"byok", "custom-service", "projectless-retag", "mismatched-root", "mismatched-url"}:
        service.grading_requirements.assert_not_awaited()
    else:
        service.grading_requirements.assert_awaited_once_with("task_0", dataset=None)
    if grading_path == "project":
        assert gateway_calls == [
            (
                "/service-auth",
                {
                    "run_id": str(benchmark.id),
                    "task_id": "task_0",
                    "attempt_id": task_execution.task_attempt_id(task.id, task.started_at),
                    "allowed_models": ["openai/gpt-5", "anthropic/claude-sonnet"],
                    "allowed_operations": ["query", "tokens_count"],
                    "identity": {"benchmark_name": benchmark.name, "agent_name": "grader"},
                    "egress": "*",
                    "ttl_seconds": model_gateway.GATEWAY_TOKEN_TTL_SECONDS,
                },
                "Bearer saved-project-root",
            ),
            ("/service-auth/revoke", {"lease_id": "grading-lease"}, "Bearer saved-project-root"),
        ]
    else:
        assert gateway_calls == []


@pytest.mark.description(
    (
        "Opt-in real Gateway ledger proof using direct Tracker start-admission calls and framework HTTP/WS.\n"
        "\n"
        "Secret resolution, model-library scope, upstream provider, and agent/sandbox use test fixtures."
    )
)
async def test_saved_project_grader_records_real_gateway_ledger(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    bound_controls: Callable[..., dict[str, object]],
    runtime_services: RuntimeServices,
) -> None:
    source_value = os.environ.get("TRACKER_GATEWAY_PROOF_SOURCE")
    if source_value is None:
        pytest.skip("set TRACKER_GATEWAY_PROOF_SOURCE to the pinned model-proxy checkout")
    source = Path(source_value)
    assert subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip() == (
        "f1d737c28a43ac35339217ca63f3775fdc48211d"
    )
    gateway_python = source / ".venv" / "bin" / "python"
    assert gateway_python.exists()
    project_id = uuid4()
    with DockerContainer("amazon/dynamodb-local:latest").with_exposed_ports(8000) as dynamodb:
        dynamodb_url = f"http://127.0.0.1:{dynamodb.get_exposed_port(8000)}"
        with tempfile.TemporaryDirectory() as directory:
            ready_file = Path(directory) / "gateway-ready.json"
            gateway_log = Path(directory) / "gateway.log"
            with socket.socket() as bound:
                bound.bind(("127.0.0.1", 0))
                gateway_port = bound.getsockname()[1]
            with socket.socket() as bound:
                bound.bind(("127.0.0.1", 0))
                redis_port = bound.getsockname()[1]
            redis_process = subprocess.Popen(
                ["valkey-server", "--port", str(redis_port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )
            gateway_env = {
                **os.environ,
                "MODEL_GATEWAY_AUTH_DATABASE_SECRET": json.dumps(
                    {
                        "username": postgres_engine.url.username,
                        "password": postgres_engine.url.password,
                        "host": postgres_engine.url.host,
                        "port": postgres_engine.url.port,
                    }
                ),
                "MODEL_GATEWAY_AUTH_DATABASE_NAME": postgres_engine.url.database,
                "REDIS_URL": f"redis://127.0.0.1:{redis_port}/0",
                "TRACKER_PROOF_PROJECT_ID": str(project_id),
                "TRACKER_PROOF_READY_FILE": str(ready_file),
                "TRACKER_PROOF_GATEWAY_PORT": str(gateway_port),
                "AWS_ENDPOINT_URL_DYNAMODB": dynamodb_url,
                "AWS_ACCESS_KEY_ID": "local",
                "AWS_SECRET_ACCESS_KEY": "local",
                "AWS_REGION": "us-east-1",
                "GATEWAY_USAGE_LEDGER_MODE": "enforced",
                "GATEWAY_USAGE_LEDGER_TABLE_NAME": "tracker-grader-proof",
                "GATEWAY_USAGE_LEDGER_REGION": "us-east-1",
            }
            dynamodb_client = boto3.client(
                "dynamodb",
                endpoint_url=dynamodb_url,
                region_name="us-east-1",
                aws_access_key_id="local",
                aws_secret_access_key="local",
            )
            dynamodb_client.create_table(
                TableName="tracker-grader-proof",
                KeySchema=[{"AttributeName": "PK", "KeyType": "HASH"}, {"AttributeName": "SK", "KeyType": "RANGE"}],
                AttributeDefinitions=[
                    {"AttributeName": "PK", "AttributeType": "S"},
                    {"AttributeName": "SK", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            with gateway_log.open("w") as output:
                gateway_process = subprocess.Popen(
                    [str(gateway_python), str(Path(__file__).with_name("_gateway_proof_service.py"))],
                    cwd=source,
                    env=gateway_env,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                )
                try:
                    for _ in range(300):
                        if ready_file.exists():
                            async with httpx.AsyncClient() as health:
                                try:
                                    await health.get(
                                        f"http://127.0.0.1:{gateway_port}/query",
                                        headers={"Authorization": "Bearer sk-local"},
                                    )
                                    break
                                except httpx.ConnectError:
                                    pass
                        if gateway_process.poll() is not None:
                            pytest.fail(gateway_log.read_text()[-5000:])
                        await asyncio.sleep(0.1)
                    else:
                        pytest.fail(f"Gateway startup timed out: {gateway_log.read_text()[-5000:]}")
                    proof = json.loads(ready_file.read_text())
                    gateway_url = f"http://127.0.0.1:{gateway_port}"
                    monkeypatch.setenv("MODEL_GATEWAY_URL", gateway_url)
                    monkeypatch.setenv("MODEL_GATEWAY_API_KEY", "wrong-service-root")
                    monkeypatch.setenv("GRADING_MODELS", "openai/gpt-4o")
                    monkeypatch.setenv("AUTH_REQUIRED", "false")
                    gateway_module = types.ModuleType("model_library.base.gateway")

                    @contextmanager
                    def gateway_token_scope(_token: str) -> Generator[None]:
                        yield

                    gateway_module.gateway_token_scope = gateway_token_scope
                    monkeypatch.setitem(sys.modules, "model_library.base.gateway", gateway_module)

                    @asynccontextmanager
                    async def provider_context() -> AsyncGenerator[Any]:
                        provider = Mock()
                        provider.get_sandbox = AsyncMock(return_value=Mock(id="sandbox-proof"))
                        yield provider

                    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", lambda _self: provider_context())

                    class Grader(BenchmarkService):
                        async def load_datasets(self) -> dict[str, dict[str, Any]]:
                            return {"default": {"task_0": {}}}

                        async def retrieve_task(
                            self, task_id: str, skip_validation: bool = False, dataset: str | None = None
                        ) -> RetrieveTaskResponse:
                            return RetrieveTaskResponse(
                                source=_SOURCE, resources=_RESOURCES, problem_path="/tmp/problem.txt", cwd="/testbed"
                            )

                        async def setup_task(
                            self, task_id: str, sandbox: Sandbox, dataset: str | None = None
                        ) -> AsyncGenerator[StreamChunk]:
                            yield StreamResultChunk(type="result", data={})

                        async def evaluate_response(
                            self, request: EvaluateResponseRequest, dataset: str | None = None
                        ) -> Any:
                            return {}

                        async def evaluate_instance(
                            self, task_id: str, sandbox: Sandbox, dataset: str | None = None
                        ) -> AsyncGenerator[StreamChunk]:
                            credentials = current_grading_credentials()
                            assert credentials is not None
                            async with httpx.AsyncClient() as client:
                                query = await client.post(
                                    f"{credentials.gateway_url}/query",
                                    headers={"Authorization": f"Bearer {credentials.api_key}"},
                                    json={"model": "openai/gpt-4o", "inputs": [{"kind": "text", "text": "grade"}]},
                                )
                                query.raise_for_status()
                            yield StreamResultChunk(type="result", data={"score": 1.0})

                        async def calculate_final_score(
                            self, evaluation_results: dict[str, Any], dataset: str | None = None
                        ) -> FinalScoreResult:
                            return FinalScoreResult(score=1.0, metadata={})

                    with socket.socket() as bound:
                        bound.bind(("127.0.0.1", 0))
                        framework_port = bound.getsockname()[1]
                    framework = uvicorn.Server(
                        uvicorn.Config(
                            BenchmarkServiceApp(Grader), host="127.0.0.1", port=framework_port, log_level="error"
                        )
                    )
                    framework_task = asyncio.create_task(framework.serve())
                    try:
                        for _ in range(100):
                            if framework.started:
                                break
                            await asyncio.sleep(0.1)
                        assert framework.started
                        events: list[str] = []
                        provider_pool_id = f"daytona:{uuid4()}"
                        pool_id = queue_pool_id(provider_pool_id)
                        org, seed, _ = _run(postgres_session, pool_id, [("fixture", TaskStatus.PENDING, _ATTEMPT)])
                        org.name = "vals.ai"
                        project = Project(
                            id=project_id,
                            org_id=org.id,
                            handle="selected",
                            name="Selected",
                            spend_type=SpendType.INTERNAL,
                        )
                        project.gateway_key_id = UUID(proof["key_id"])
                        postgres_session.add_all([org, project])
                        assert seed.current_execution_release_id is not None
                        promote_release(postgres_session, seed.current_execution_release_id)
                        postgres_session.commit()
                        monkeypatch.setattr(tracker_main, "secret_store", lambda _org: Mock())
                        request = await tracker_main._with_project_gateway_secrets(
                            seed.managed_start_benchmark_request(), project_id, org, postgres_engine
                        )
                        reference = f"{tenant_secret_prefix(org.id)}model-gateway-{project.gateway_key_id}"
                        assert request.contract.secrets["MODEL_GATEWAY_API_KEY"] == reference
                        assert request.contract.secrets["MODEL_GATEWAY_URL"] == reference
                        benchmark = Benchmark(
                            org_id=org.id,
                            name=f"run-{uuid4()}",
                            arguments=seed.arguments.model_copy(update={"contract": request.contract}),
                        )
                        monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
                        monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"0" * 32).decode())
                        dispatch_id = uuid4()
                        tracker_main._commit_start(
                            postgres_engine,
                            benchmark.model_dump_json(),
                            request.model_copy(update={"benchmark_name": benchmark.name, "priority": 0}),
                            dispatch_id,
                            ["task_0"],
                            pool_id,
                            {"request_id": "local-ledger-proof", "trace_headers": {}},
                            (),
                            project_id=project_id,
                            selected_project_gateway_root=True,
                        )
                        benchmark = postgres_session.get(Benchmark, benchmark.id)
                        assert benchmark is not None
                        assert benchmark.arguments.selected_project_gateway_secret_ref == reference
                        assert "selected_project_gateway_secret_ref" not in benchmark.model_dump_json()
                        task = postgres_session.exec(select(Task).where(Task.benchmark == benchmark.id)).one()
                        first_link = postgres_session.exec(
                            select(RunProject).where(RunProject.benchmark_id == benchmark.id)
                        ).one()
                        assert first_link.project_id == project_id
                        controls = bound_controls(benchmark, dispatch_id=dispatch_id, session=postgres_session)
                        task_control = cast(TaskControl, controls["task_control"])
                        context = _context(cast(QueueControl, controls["queue_control"]), provider_pool_id, events)
                        task_state = (
                            await task_control.read(
                                AttemptRef(
                                    org_id=task.org_id,
                                    task_row_id=task.id,
                                    task_id=task.task_id,
                                    started_at=task.started_at,
                                )
                            )
                        ).task

                        async def resolve(names: dict[str, str], _store: Any) -> dict[str, str]:
                            assert names["MODEL_GATEWAY_API_KEY"] == reference
                            return {"MODEL_GATEWAY_URL": gateway_url, "MODEL_GATEWAY_API_KEY": proof["root"]}

                        monkeypatch.setattr(task_execution, "resolve_secrets", resolve)

                        monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
                        monkeypatch.setattr(
                            task_execution,
                            "create_sandbox",
                            lambda *_args, creation_state, **_kwargs: _sandbox(events, creation_state=creation_state)(),
                        )
                        monkeypatch.setattr(task_execution, "upload_agent_artifacts", AsyncMock())
                        monkeypatch.setattr(task_execution, "install_agent_dependencies", AsyncMock())
                        monkeypatch.setattr(task_execution, "run_agent", AsyncMock(side_effect=_run_agent_success))
                        real_client = BenchmarkServiceClient(
                            url=f"http://127.0.0.1:{framework_port}",
                            headers={"Authorization": "Bearer local"},
                        )
                        service = AsyncMock(spec=BenchmarkServiceClient)
                        _use_real_sandbox_recovery(service)
                        service.retrieve_task.return_value = RetrieveTaskResponse(
                            source=_SOURCE, resources=_RESOURCES, problem_path="/tmp/problem.txt", cwd="/testbed"
                        )
                        service.setup_task.return_value = SetupTaskResponse(status="ok")
                        service.grading_requirements.side_effect = real_client.grading_requirements
                        service.evaluate_instance.side_effect = real_client.evaluate_instance
                        try:
                            result = await task_execution.process_task(
                                task_state,
                                benchmark.managed_start_benchmark_request(),
                                cast(BenchmarkServiceClient, service),
                                benchmark.id,
                                task.task_id,
                                runtime_services,
                                OrgContext(id=org.id, name=org.name, storage_key=org.storage_key),
                                sandbox_provider_config=DaytonaProviderConfig(
                                    DAYTONA_API_KEY="local",
                                    DAYTONA_API_URL="http://localhost:8001",
                                    DAYTONA_TARGET="us",
                                ),
                                sandbox_provider=context.provider,
                                creation_semaphore=Semaphore(1),
                                queue_context=context,
                                control=task_control,
                                started_by_email=benchmark.started_by_email,
                            )
                        finally:
                            await real_client.close()
                        assert result == {"task_0": {"score": 1.0}}
                        rows = dynamodb_client.scan(TableName="tracker-grader-proof")["Items"]
                        assert len(rows) == 1
                        assert rows[0]["managed_key_id"]["S"] == proof["key_id"]
                        assert rows[0]["project_id"]["S"] == str(project_id)
                        assert rows[0]["run_id"]["S"] == str(benchmark.id)
                        assert rows[0]["question_id"]["S"] == "task_0"
                        assert rows[0]["attempt_id"]["S"] == task_execution.task_attempt_id(task.id, task.started_at)
                        assert rows[0]["model"]["S"] == "openai/gpt-4o"
                        assert rows[0]["agent_name"]["S"] == "grader"
                    finally:
                        framework.should_exit = True
                        await framework_task
                finally:
                    gateway_process.terminate()
                    gateway_process.wait(timeout=10)
                    redis_process.terminate()
                    redis_process.wait(timeout=10)
