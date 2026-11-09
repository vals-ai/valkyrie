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
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

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
from sqlalchemy import and_, event, text
from sqlalchemy.engine import Connection, Engine, ExecutionContext
from sqlalchemy.exc import DBAPIError
from sqlmodel import Session, col, create_engine, func, select, update
from testcontainers.core.container import DockerContainer
import uvicorn

from tests.factories import make_task
from tests.unit.services.task_execution_support import finalize_agent_generation
from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tracker.repositories import advisory_lock
from tracker.repositories.scheduler import SchedulerRepository
from tracker.secret_access import tenant_secret_prefix
from tracker.auth import RequestIdentity
from tracker.aws.runtime import AWSRuntime
from tracker.aws.services import CloudRuntimeFactory
from tracker.runtime.services import RuntimeServices
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    AWSBenchmarkArguments,
    ErrorResult,
    EvaluationResult,
    FailurePhase,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorRelease,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    Org,
    OrgProviderSecret,
    Project,
    RunProject,
    RetryMode,
    SandboxAdmissionCandidate,
    SandboxBuildReservation,
    SpendType,
    Task,
    TaskStatus,
)
from tracker.exceptions import SandboxSetupError
import tracker.executor.dispatch_control as dispatch_control
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.executor.release_control import promote_release
import tracker.runtime.model_gateway as model_gateway
import tracker.scheduler.admission as admission
import tracker.scheduler.coordinator as coordinator
import tracker.scheduler.store as store
from tracker.sandbox import SandboxCreationState, create_sandbox
from tracker.types import StartBenchmarkRequest
import tracker.services.tasks.execute as task_execution
import tracker.services.tasks.monitor as task_monitor_module
import tracker.services.runs.start as run_orchestration
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
            artifact_uri=f"s3://artifacts/{_RELEASE_ID}.pex",
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


def _set_concurrency(engine: Engine, benchmark: Benchmark, concurrency: int) -> None:
    with Session(engine) as session:
        row = session.get(Benchmark, benchmark.id)
        assert row
        row.arguments = row.arguments.model_copy(update={"concurrency": concurrency})
        session.commit()


def _revoke_dispatch(engine: Engine, authority: ExecutionAuthority) -> None:
    with Session(engine) as session:
        dispatch = session.get(ExecutorDispatch, authority.dispatch_id)
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
    engine: Engine,
    provider_pool_id: str,
    events: list[str],
    *,
    capacity: Sequence[bool] = (True,),
    on_check: Callable[[], Awaitable[None]] | None = None,
    snapshot: SandboxCapacity | Exception = _CAPACITY,
    demand: SandboxReservationDemand | Exception | None = None,
) -> admission.SandboxQueueContext:
    provider = MockProvider(
        provider_pool_id, events, capacity=capacity, on_check=on_check, snapshot=snapshot, demand=demand
    )
    context = admission.SandboxQueueContext(
        provider=cast(SandboxProvider, provider),
        pool_id=store.reserved_queue_pool_id(provider_pool_id),
        engine=engine,
        poll_interval_seconds=0.01,
    )
    return context


def _reservation_count(engine: Engine, pool_id: str) -> int:
    with Session(engine) as session:
        return SchedulerRepository(session).active_reservations(pool_id).count


async def _build_lock_is_free(engine: Engine, task: Task) -> bool:
    lock = advisory_lock.task_build_lock(engine, task.id)
    if not await lock.acquire():
        return False
    await lock.release()
    return True


async def _pool_lock_is_free(engine: Engine, pool_id: str) -> bool:
    async with store.queue_pool_lock(engine, pool_id) as acquired:
        return acquired


@pytest.mark.description("A queue context needs a managed provider.")
def test_queue_context_requires_managed_provider() -> None:
    provider = Mock(admission_pool_id=None)

    with pytest.raises(ValueError, match="does not support queued admission"):
        admission.create_queue_context(
            engine=cast(Engine, Mock()),
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
    executor_authority: Any,
) -> None:
    org, benchmark, _ = _run(
        postgres_session,
        store.reserved_queue_pool_id(f"daytona:{uuid4()}"),
        [("locked-admission", TaskStatus.STOPPED, _ATTEMPT)],
    )
    executor_authority(benchmark, session=postgres_session)
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
        original_lock = tracker_main.lock_executor_admission

        def observed_recovery_lock(session: Session) -> object:
            session.connection().execute(text("SET LOCAL lock_timeout = '5s'"))
            lock_entered.set()
            return original_lock(session)

        monkeypatch.setattr(tracker_main, "lock_executor_admission", observed_recovery_lock)
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
    executor_authority: Any,
    runtime_services: RuntimeServices,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (selected, unselected) = _run(
        postgres_session,
        store.reserved_queue_pool_id(provider_pool_id),
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
    executor_authority(benchmark, dispatch_id=dispatch.id, session=postgres_session)
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
        with Session(postgres_engine) as session:
            dispatch = session.get(ExecutorDispatch, dispatch_id)
            assert dispatch is not None
            dispatch.status = ExecutorDispatchStatus.RUNNING
            session.commit()
        events: list[str] = []
        context = _context(postgres_engine, provider_pool_id, events)
        service = AsyncMock(spec=BenchmarkServiceClient)
        service.retrieve_task.return_value = RetrieveTaskResponse(
            source=_SOURCE, problem_path="/tmp/problem.txt", cwd="/testbed", resources=_RESOURCES
        )
        service.evaluate_instance.return_value = {"status": "success", "score": 1.0}
        _use_real_sandbox_recovery(service)
        monkeypatch.setattr(task_execution, "engine", postgres_engine)
        monkeypatch.setattr("tracker.services.tasks.monitor.engine", postgres_engine)
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
        with Session(postgres_engine) as session:
            admitted_task = session.get(Task, selected.id)
            assert admitted_task is not None
            session.expunge(admitted_task)
        async with _drive_queue(context, ExecutionAuthority(benchmark.id, dispatch_id)):
            await task_execution.process_task(
                admitted_task,
                benchmark.managed_start_benchmark_request(),
                cast(BenchmarkServiceClient, service),
                benchmark.id,
                selected.task_id,
                runtime_services,
                org,
                sandbox_provider_config=DaytonaProviderConfig(
                    DAYTONA_API_KEY="test-api-key", DAYTONA_API_URL="http://localhost:8001", DAYTONA_TARGET="us"
                ),
                sandbox_provider=context.provider,
                creation_semaphore=Semaphore(1),
                authority=ExecutionAuthority(benchmark.id, dispatch_id),
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
    executor_authority: Any,
) -> None:
    org, benchmark, _ = _run(
        postgres_session,
        store.reserved_queue_pool_id(f"daytona:{uuid4()}"),
        [("payload-key", TaskStatus.STOPPED, _ATTEMPT)],
    )
    executor_authority(benchmark, session=postgres_session)
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
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    events: list[str] = []
    source = TargetedSnapshotSource(snapshot="snapshot", target="us-west-3")
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, events)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("targeted", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)

    creation_state = SandboxCreationState(cleanup_pending=False)
    async with _drive_queue(context, authority), AsyncExitStack() as stack, AsyncExitStack() as preparation_stack:
        sandbox = await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            task_row_id=task.id,
            expected_started_at=task.started_at,
            authority=authority,
            source=source,
            resources=_RESOURCES,
            creation_state=creation_state,
            create=_sandbox(events, creation_state=creation_state),
            preparation_stack=preparation_stack,
        )

        assert sandbox is not None
        provider = cast(MockProvider, context.provider)
        assert provider.sources == [source]
        assert provider.sources[0] is source
        assert events == ["capacity", "read_capacity", "create"]


def _publish_run_state(context: admission.SandboxQueueContext, authority: ExecutionAuthority) -> None:
    with Session(context.engine) as session:
        rows = session.exec(
            select(Task, SandboxAdmissionCandidate.readiness_requested_at)
            .outerjoin(
                SandboxAdmissionCandidate,
                and_(
                    col(SandboxAdmissionCandidate.task_row_id) == col(Task.id),
                    col(SandboxAdmissionCandidate.attempt_started_at) == col(Task.started_at),
                    col(SandboxAdmissionCandidate.executor_dispatch_id) == authority.dispatch_id,
                ),
            )
            .where(col(Task.benchmark) == authority.benchmark_id)
        ).all()
    context.admission_signals.publish(
        [task for task, _requested_at in rows],
        {task.id: requested_at for task, requested_at in rows if requested_at is not None},
    )


@asynccontextmanager
async def _drive_queue(
    context: admission.SandboxQueueContext,
    authority: ExecutionAuthority,
) -> AsyncGenerator[None, None]:

    async def provider_factory(_reference: coordinator.PoolReference) -> SandboxProvider:
        return context.provider

    async def refresh_run() -> None:
        while True:
            _publish_run_state(context, authority)
            await asyncio.sleep(0.01)

    scheduler = coordinator.SharedPoolScheduler(
        context.engine, provider_factory=provider_factory, interval_seconds=0.01, rate_per_minute=0
    )
    scheduler.start()
    refresh = asyncio.create_task(refresh_run())
    try:
        async with asyncio.timeout(10):
            yield
    finally:
        refresh.cancel()
        await asyncio.gather(refresh, return_exceptions=True)
        await scheduler.stop()


async def _recover_once(context: admission.SandboxQueueContext) -> None:
    await admission.reconcile_abandoned_reservations(context)
    lock = store.queue_pool_lock(context.engine, context.pool_id)
    async with lock as acquired:
        assert acquired
        with Session(lock.connection) as session:
            SchedulerRepository(session).expire_unaccepted_grants(context.pool_id)
            session.commit()
        admission.reset_abandoned_pool_builds(lock.connection, context)


async def _enter(
    stack: AsyncExitStack,
    context: admission.SandboxQueueContext,
    task: Task,
    events: list[str],
    authority: Any,
    *,
    source: SandboxSource = _SOURCE,
    resources: Resources = _RESOURCES,
    on_create: Callable[[], Awaitable[None]] | None = None,
    on_cleanup: Callable[[], Awaitable[None]] | None = None,
) -> Sandbox | None:
    creation_state = SandboxCreationState(cleanup_pending=False)
    async with _drive_queue(context, authority), AsyncExitStack() as preparation_stack:
        return await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            task_row_id=task.id,
            expected_started_at=task.started_at,
            authority=authority,
            source=source,
            resources=resources,
            creation_state=creation_state,
            create=_sandbox(events, creation_state=creation_state, on_create=on_create, on_cleanup=on_cleanup),
            preparation_stack=preparation_stack,
        )


async def _enter_provider_sandbox(
    stack: AsyncExitStack,
    context: admission.SandboxQueueContext,
    task: Task,
    authority: ExecutionAuthority,
    creation_state: SandboxCreationState,
    creation_semaphore: Semaphore,
) -> Sandbox | None:
    def create() -> AbstractAsyncContextManager[Sandbox]:
        return create_sandbox(
            provider=context.provider,
            sandbox_name="scheduler-sandbox",
            source=_SOURCE,
            resources=_RESOURCES,
            creation_semaphore=creation_semaphore,
            creation_state=creation_state,
            unique_name=False,
        )

    async with _drive_queue(context, authority), AsyncExitStack() as preparation_stack:
        return await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            task_row_id=task.id,
            expected_started_at=task.started_at,
            authority=authority,
            source=_SOURCE,
            resources=_RESOURCES,
            creation_state=creation_state,
            create=create,
            preparation_stack=preparation_stack,
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
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, [])
    _, _, (task,) = _run(
        postgres_session,
        context.pool_id,
        [("abandoned", TaskStatus.BUILDING, _ATTEMPT)],
    )

    await _recover_once(context)

    assert _task(postgres_engine, task).status == TaskStatus.PENDING


@pytest.mark.description("The coordinator owns the pool lock. That must not block another run's finalization.")
async def test_completed_run_finalizes_while_another_run_holds_pool_lock_during_creation(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events)
    org, completing_run, (evaluating,) = _run(
        postgres_session, context.pool_id, [("evaluating", TaskStatus.EVALUATING, _ATTEMPT)]
    )
    evaluating.eval_resume_state = {"cursor": "evaluation"}
    _, building_run, (building,) = _run(postgres_session, context.pool_id, [("building", TaskStatus.PENDING, _ATTEMPT)])
    completing_authority = executor_authority(completing_run, session=postgres_session)
    building_authority = executor_authority(building_run, session=postgres_session)
    evaluation_started = asyncio.Event()
    creation_started = asyncio.Event()
    release_creation = asyncio.Event()

    async def finish_evaluation(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, float]]:
        evaluation_started.set()
        await creation_started.wait()
        _update_task(postgres_engine, task_row, TaskStatus.FINISHED, task_row.started_at)
        return {task_row.task_id: {"score": 1.0}}

    async def hold_creation() -> None:
        creation_started.set()
        await release_creation.wait()

    monkeypatch.setattr(run_orchestration, "engine", postgres_engine)
    monkeypatch.setattr("tracker.services.runs.finalize.engine", postgres_engine)
    monkeypatch.setattr(task_execution, "engine", postgres_engine)
    monkeypatch.setattr("tracker.services.tasks.monitor.engine", postgres_engine)
    monkeypatch.setattr(task_monitor_module.TaskMonitor, "_TRACK_INTERVAL", 0.01)
    monkeypatch.setattr(run_orchestration, "process_task", finish_evaluation)
    coordinator = asyncio.create_task(
        run_orchestration._run_queued_tasks(
            benchmark_id=completing_run.id,
            task_rows=[(evaluating.task_id, evaluating)],
            start_benchmark_request=completing_run.managed_start_benchmark_request(),
            benchmark_service=Mock(spec=BenchmarkServiceClient),
            runtime=Mock(spec=RuntimeServices),
            org=org,
            sandbox_provider_config=Mock(spec=SandboxProviderConfig),
            sandbox_provider=context.provider,
            creation_semaphore=Semaphore(1),
            queue_context=context,
            started_by_email=None,
            record_cancellation=lambda _attempts: None,
            authority=completing_authority,
        )
    )
    async with AsyncExitStack() as stack:
        entering: asyncio.Task[Sandbox | None] | None = None
        try:
            await asyncio.wait_for(evaluation_started.wait(), timeout=2)
            entering = asyncio.create_task(
                _enter(
                    stack,
                    _context(postgres_engine, cast(MockProvider, context.provider).admission_pool_id, events),
                    building,
                    events,
                    building_authority,
                    source=SnapshotSource(snapshot="snapshot"),
                    on_create=hold_creation,
                )
            )
            await asyncio.wait_for(creation_started.wait(), timeout=2)
            await asyncio.wait_for(coordinator, timeout=2)

            with Session(postgres_engine) as session:
                completed = session.get(Benchmark, completing_run.id)
                assert completed is not None
                run_orchestration.set_benchmark_final_status(completed, session, org, authority=completing_authority)
                assert completed.status == BenchmarkStatus.FINISHED

            assert _task(postgres_engine, building).status == TaskStatus.BUILDING
            assert not entering.done()
            assert not await _pool_lock_is_free(postgres_engine, context.pool_id)
        finally:
            release_creation.set()
            coordinator.cancel()
            await asyncio.gather(coordinator, return_exceptions=True)
            if entering is not None:
                await asyncio.wait_for(entering, timeout=2)

    assert await _pool_lock_is_free(postgres_engine, context.pool_id)


@pytest.mark.description("Admission returns no sandbox and calls no provider for a task that has left the queue.")
async def test_admission_ignores_task_that_is_no_longer_waiting(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    events: list[str] = []
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, events)
    _, benchmark, (task,) = _run(
        postgres_session,
        context.pool_id,
        [("stopped", TaskStatus.STOPPED, _ATTEMPT)],
    )
    authority = executor_authority(benchmark, session=postgres_session)

    async with AsyncExitStack() as stack:
        sandbox = await _enter(stack, context, task, events, authority)

    assert sandbox is None
    assert events == []


@pytest.mark.description(
    "A pool lock blocks a second holder of the same pool but not another pool, and is free again after release."
)
async def test_pool_locks_are_isolated_and_reusable(postgres_engine: Engine) -> None:
    first_pool = store.queue_pool_id(f"daytona:{uuid4()}")
    second_pool = store.queue_pool_id(f"daytona:{uuid4()}")

    async with store.queue_pool_lock(postgres_engine, first_pool) as first:
        async with store.queue_pool_lock(postgres_engine, first_pool) as duplicate:
            pass
        async with store.queue_pool_lock(postgres_engine, second_pool) as independent:
            pass
    async with store.queue_pool_lock(postgres_engine, first_pool) as reused:
        pass

    assert (first, duplicate, independent, reused) == (True, False, True, True)


@pytest.mark.description("The queue pool lock cannot be taken while the legacy advisory key is held.")
async def test_queue_pool_lock_contends_with_legacy_key(postgres_engine: Engine) -> None:
    pool_id = store.queue_pool_id(f"daytona:{uuid4()}")
    legacy_lock_key = int.from_bytes(sha256(pool_id.encode()).digest()[:8], byteorder="big", signed=True)

    with postgres_engine.connect() as legacy_connection:
        legacy_acquired = bool(
            legacy_connection.execute(
                text("SELECT pg_try_advisory_lock(:lock_key)"),
                {"lock_key": legacy_lock_key},
            ).scalar_one()
        )
        legacy_connection.commit()
        assert legacy_acquired
        try:
            async with store.queue_pool_lock(postgres_engine, pool_id) as new_version_acquired:
                pass
            assert new_version_acquired is False
        finally:
            legacy_released = bool(
                legacy_connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": legacy_lock_key},
                ).scalar_one()
            )
            legacy_connection.commit()
            assert legacy_released


@pytest.mark.description("A cancelled evaluation releases its task lock for reuse.")
async def test_cancelled_evaluation_releases_and_reuses_task_lock(postgres_engine: Engine) -> None:
    task_row_id = uuid4()
    acquired = asyncio.Event()
    hold = asyncio.Event()

    async def owner() -> None:
        async with advisory_lock.task_evaluation_lock(postgres_engine, task_row_id) as owns_lock:
            assert owns_lock
            acquired.set()
            await hold.wait()

    owner_task = asyncio.create_task(owner())
    await acquired.wait()
    owner_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner_task

    async with advisory_lock.task_evaluation_lock(postgres_engine, task_row_id) as reused:
        pass
    assert reused is True


@pytest.mark.description(
    "Revoked authority before the claim returns no sandbox and leaves the task `PENDING` after the capacity check."
)
async def test_revocation_before_claim_creates_no_sandbox(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session,
        store.reserved_queue_pool_id(provider_pool_id),
        [("revoked-before-claim", TaskStatus.PENDING, _ATTEMPT)],
    )
    authority = executor_authority(benchmark, session=postgres_session)
    events: list[str] = []

    async def revoke() -> None:
        _revoke_dispatch(postgres_engine, authority)

    async with AsyncExitStack() as stack:
        sandbox = await _enter(
            stack,
            _context(postgres_engine, provider_pool_id, events, on_check=revoke),
            task,
            events,
            authority,
        )

    assert sandbox is None
    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert events == ["capacity"]


@pytest.mark.description(
    "Revocation during creation cleans up the sandbox, and recovery returns the task to `PENDING` with a new start time."
)
async def test_revocation_during_creation_cleans_up_without_starting(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session,
        store.reserved_queue_pool_id(provider_pool_id),
        [("revoked-during-creation", TaskStatus.PENDING, _ATTEMPT)],
    )
    authority = executor_authority(benchmark, session=postgres_session)
    events: list[str] = []

    async def revoke() -> None:
        _revoke_dispatch(postgres_engine, authority)

    async with AsyncExitStack() as stack:
        sandbox = await _enter(
            stack,
            _context(postgres_engine, provider_pool_id, events),
            task,
            events,
            authority,
            on_create=revoke,
        )

    assert sandbox is None
    await _recover_once(_context(postgres_engine, provider_pool_id, []))
    recovered = _task(postgres_engine, task)
    assert recovered.status == TaskStatus.PENDING
    assert recovered.started_at > task.started_at
    assert events == ["capacity", "read_capacity", "create", "cleanup"]


@pytest.mark.description("Cancelling admission while it waits frees the pool lock for the next holder.")
async def test_cancellation_releases_pool_lock(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.reserved_queue_pool_id(provider_pool_id)
    _, benchmark, (task,) = _run(
        postgres_session,
        pool_id,
        [("cancelled", TaskStatus.PENDING, _ATTEMPT)],
    )
    authority = executor_authority(benchmark, session=postgres_session)
    capacity_started = asyncio.Event()
    hold_capacity = asyncio.Event()

    async def block_capacity() -> None:
        capacity_started.set()
        await hold_capacity.wait()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter(
                stack,
                _context(postgres_engine, provider_pool_id, [], on_check=block_capacity),
                task,
                [],
                authority,
            )
        )
        await capacity_started.wait()
        entering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await entering

    async with store.queue_pool_lock(postgres_engine, pool_id) as acquired:
        pass
    assert acquired is True


@pytest.mark.description("Cancelling during a stale creation waits for cleanup and leaves the task `PENDING`.")
async def test_stale_creation_is_cleaned_before_cancellation(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.reserved_queue_pool_id(provider_pool_id)
    _, benchmark, (capacity_task, creation_task) = _run(
        postgres_session,
        pool_id,
        [
            ("stale-capacity", TaskStatus.PENDING, _ATTEMPT),
            ("stale-creation", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    authority = executor_authority(benchmark, session=postgres_session)
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
                _context(postgres_engine, provider_pool_id, capacity_events, on_check=supersede_during_capacity),
                capacity_task,
                capacity_events,
                authority,
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
                _context(postgres_engine, provider_pool_id, events),
                creation_task,
                events,
                authority,
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
    assert events == ["capacity", "read_capacity", "create", "cleanup"]


@pytest.mark.description("A held evaluation lock rejects recovery and changes nothing.")
async def test_held_evaluation_lock_rejects_recovery_without_mutation(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (task,) = _run(
        postgres_session,
        store.reserved_queue_pool_id(provider_pool_id),
        [("owned-evaluation", TaskStatus.EVALUATING, _ATTEMPT)],
    )
    task.eval_resume_state = {"job_id": "active-job"}
    postgres_session.add(task)
    postgres_session.commit()
    authority = executor_authority(benchmark, session=postgres_session)
    dispatch = postgres_session.get(ExecutorDispatch, authority.dispatch_id)
    assert dispatch is not None
    task.started_at = dispatch.created_at
    postgres_session.add(task)
    postgres_session.commit()
    original_started_at = task.started_at
    dispatch_count = postgres_session.exec(
        select(func.count()).select_from(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark.id)
    ).one()
    enqueue = AsyncMock()
    monkeypatch.setattr(tracker_main, "_launch_executor_dispatch", enqueue)
    _use_managed_runtime(monkeypatch, aws_runtime)

    async with advisory_lock.task_evaluation_lock(postgres_engine, task.id) as acquired:
        assert acquired
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
    executor_authority: Any,
    runtime_services: RuntimeServices,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (task,) = _run(
        postgres_session,
        store.reserved_queue_pool_id(provider_pool_id),
        [("raced-evaluation", TaskStatus.EVALUATING, _ATTEMPT)],
    )
    task.eval_resume_state = {"job_id": "raced-job"}
    postgres_session.add(task)
    postgres_session.commit()
    executor_authority(benchmark, session=postgres_session)
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

    with Session(postgres_engine) as stale_session:
        stale_task = stale_session.get(Task, task.id)
        assert stale_task is not None
        stale_session.expunge(stale_task)
    stale_task.started_at = first_dispatch.created_at
    service = AsyncMock(spec=BenchmarkServiceClient)
    service.resume_evaluation.return_value = {"score": 1.0}
    _use_real_sandbox_recovery(service)
    monkeypatch.setattr(task_execution, "engine", postgres_engine)
    monkeypatch.setattr("tracker.services.tasks.monitor.engine", postgres_engine)
    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
    request = benchmark.managed_start_benchmark_request()

    async def run(task_row: Task, authority: ExecutionAuthority) -> dict[str, dict[str, Any] | None]:
        return await task_execution.process_task(
            task_row,
            request,
            cast(BenchmarkServiceClient, service),
            benchmark.id,
            task.task_id,
            runtime_services,
            org,
            sandbox_provider_config=cast(SandboxProviderConfig, object()),
            sandbox_provider=cast(SandboxProvider, object()),
            creation_semaphore=Semaphore(1),
            authority=authority,
        )

    stale_result = await run(stale_task, ExecutionAuthority(benchmark.id, first_dispatch.id))
    winner_result = await run(winning_task, ExecutionAuthority(benchmark.id, winning_dispatch.id))

    assert stale_result == {task.task_id: None}
    assert winner_result == {task.task_id: {"score": 1.0}}
    service.resume_evaluation.assert_awaited_once()
    assert _task(postgres_engine, task).status == TaskStatus.FINISHED


@pytest.mark.description(
    "A resumed evaluation gets a child grading token and stores the updated `eval_resume_state` when the task finishes."
)
@pytest.mark.parametrize("grading_path", ["nonproject", "project"])
async def test_resumed_evaluation_uses_lock_connection_for_callback_and_finalization(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
    executor_authority: Any,
    runtime_services: RuntimeServices,
    grading_path: str,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (task,) = _run(
        postgres_session,
        store.reserved_queue_pool_id(provider_pool_id),
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
    authority = executor_authority(benchmark, session=postgres_session)
    dispatch = postgres_session.get(ExecutorDispatch, authority.dispatch_id)
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
        on_eval_resume_state({"job_id": "updated-job"})
        return {"score": 1.0}

    service.resume_evaluation.side_effect = resume_evaluation
    _use_real_sandbox_recovery(service)
    single_connection_engine = create_engine(
        postgres_engine.url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.05,
    )
    monkeypatch.setattr(task_execution, "engine", single_connection_engine)
    monkeypatch.setattr("tracker.services.tasks.monitor.engine", single_connection_engine)
    if grading_path == "project":

        async def resolve(names: dict[str, str], _store: Any) -> dict[str, str]:
            assert set(names) == {"MODEL_GATEWAY_URL", "MODEL_GATEWAY_API_KEY"}
            return {"MODEL_GATEWAY_URL": "https://gateway.test", "MODEL_GATEWAY_API_KEY": "saved-project-root"}
    else:
        resolve = AsyncMock(side_effect=AssertionError("nonproject resume must not load grading secrets"))
    monkeypatch.setattr(task_execution, "resolve_secrets", resolve)
    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
    request = benchmark.managed_start_benchmark_request()
    try:
        result = await task_execution.process_task(
            task,
            request,
            cast(BenchmarkServiceClient, service),
            benchmark.id,
            task.task_id,
            runtime_services,
            org,
            sandbox_provider_config=cast(SandboxProviderConfig, object()),
            sandbox_provider=cast(SandboxProvider, object()),
            creation_semaphore=Semaphore(1),
            authority=authority,
        )
    finally:
        single_connection_engine.dispose()

    assert result == {task.task_id: {"score": 1.0}}
    service.resume_evaluation.assert_awaited_once()
    if grading_path == "project":
        assert service.resume_evaluation.await_args.kwargs["grading_token"] == "grader-child"
    else:
        resolve.assert_not_awaited()
    persisted_task = _task(postgres_engine, task)
    assert persisted_task.status == TaskStatus.FINISHED
    assert persisted_task.eval_resume_state == {"job_id": "updated-job"}


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
                    col(SandboxAdmissionCandidate.pool_id) == store.queue_pool_lock_id(pool_id)
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
            pool_id=store.queue_pool_lock_id(pool_id),
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
    "Two reserved image builds create sandboxes concurrently, and their reservations and build locks are released afterward."
)
async def test_reserved_image_builds_provision_concurrently_outside_the_pool_lock(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, [])
    _, benchmark, (first, second) = _run(
        postgres_session,
        context.pool_id,
        [
            ("first", TaskStatus.PENDING, _ATTEMPT),
            ("second", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    authority = executor_authority(benchmark, session=postgres_session)
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
            _enter(first_stack, context, first, events, authority, on_create=hold_create),
            _enter(second_stack, context, second, events, authority, on_create=hold_create),
        )
        try:
            await asyncio.wait_for(both_creating.wait(), timeout=10)

            assert _reservation_count(postgres_engine, context.pool_id) == 2
            assert {_task(postgres_engine, task).status for task in (first, second)} == {TaskStatus.BUILDING}
            assert not await _pool_lock_is_free(postgres_engine, context.pool_id)
            assert not await _build_lock_is_free(postgres_engine, first)
            assert not await _build_lock_is_free(postgres_engine, second)
        finally:
            finish_creating.set()
        sandboxes = await asyncio.wait_for(entering, timeout=10)

        assert all(sandboxes)
        assert {_task(postgres_engine, task).status for task in (first, second)} == {TaskStatus.IN_PROGRESS}
        assert _reservation_count(postgres_engine, context.pool_id) == 0
        assert await _build_lock_is_free(postgres_engine, first)
        assert await _build_lock_is_free(postgres_engine, second)

    assert events.count("create") == 2
    assert events.index("cleanup") > max(index for index, event in enumerate(events) if event == "create")


@pytest.mark.description("Candidate cleanup needs the exact attempt and dispatch.")
def test_candidate_cleanup_requires_the_exact_attempt_and_dispatch(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
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
    interruption: str,
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("stale", TaskStatus.PENDING, _ATTEMPT)])
    dispatch_id = _prepare(postgres_session, context.pool_id, task)
    if interruption == "superseded":
        _update_task(postgres_engine, task, TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=1))
    elif interruption == "revoked":
        _revoke_dispatch(postgres_engine, ExecutionAuthority(benchmark_id=benchmark.id, dispatch_id=dispatch_id))
    else:
        _stop_run(postgres_engine, benchmark)

    capacities = {("region-a", "sysbox"): _CAPACITY}
    assert (
        SchedulerRepository(postgres_session).grant_prepared_tasks(
            context.pool_id, capacities, SchedulerRepository(postgres_session).database_now(), limit=10
        )
        == 0
    )
    postgres_session.commit()
    await _recover_once(context)

    assert _candidate_count(postgres_engine, context.pool_id) == 0
    assert _reservation_count(postgres_engine, context.pool_id) == 0


@pytest.mark.description("After a failed reserved create is cleaned up, the task can be admitted again and starts.")
async def test_failed_reserved_create_recovers_after_provider_cleanup(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    context = _context(postgres_engine, provider_pool_id, events)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("failing", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)

    async def fail_create() -> None:
        raise SandboxSetupError("provider rejected the create")

    async with AsyncExitStack() as stack:
        with pytest.raises(SandboxSetupError):
            await _enter(stack, context, task, events, authority, on_create=fail_create)

    assert events == ["capacity", "read_capacity", "create"]
    assert _reservation_count(postgres_engine, context.pool_id) == 1
    assert await _build_lock_is_free(postgres_engine, task)

    await _recover_once(context)

    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    recovered = _task(postgres_engine, task)

    async with AsyncExitStack() as stack:
        assert await _enter(stack, context, recovered, events, authority) is not None
        assert _task(postgres_engine, task).status == TaskStatus.IN_PROGRESS

    assert _reservation_count(postgres_engine, context.pool_id) == 0


@pytest.mark.description(
    "A setup retry schedules an error row, runs setup a second time on a fresh grant, and evaluates once."
)
async def test_queued_worker_setup_retry_waits_for_retention_then_finishes_with_a_fresh_grant(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
    runtime_services: RuntimeServices,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events)
    org, benchmark, (task,) = _run(postgres_session, context.pool_id, [("setup-retry", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
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
            assert grant.executor_dispatch_id == authority.dispatch_id
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

    async def provider_factory(_reference: coordinator.PoolReference) -> SandboxProvider:
        return context.provider

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
    monkeypatch.setattr(task_execution, "engine", postgres_engine)
    monkeypatch.setattr("tracker.services.tasks.monitor.engine", postgres_engine)
    monkeypatch.setattr(run_orchestration, "engine", postgres_engine)
    monkeypatch.setattr("tracker.services.runs.finalize.engine", postgres_engine)
    monkeypatch.setattr(task_execution, "_SANDBOX_RETRY_DELAY_SECONDS", 0)
    monkeypatch.setattr(task_monitor_module.TaskMonitor, "_TRACK_INTERVAL", 0.01)
    monkeypatch.setattr(coordinator, "_RECOVERY_SECONDS", 0.01)
    monkeypatch.setattr(task_execution.TaskLogBuffer, "buffer_logs", Mock())
    monkeypatch.setattr(task_execution, "upload_agent_artifacts", AsyncMock())
    monkeypatch.setattr(task_execution, "install_agent_dependencies", AsyncMock())
    monkeypatch.setattr(task_execution, "run_agent", _run_agent_success)

    scheduler = coordinator.SharedPoolScheduler(
        postgres_engine, provider_factory=provider_factory, interval_seconds=0.01, rate_per_minute=0
    )
    scheduler.start()
    worker = asyncio.create_task(
        run_orchestration._run_queued_tasks(
            benchmark_id=benchmark.id,
            task_rows=[(task.task_id, task)],
            start_benchmark_request=benchmark.managed_start_benchmark_request(),
            benchmark_service=cast(BenchmarkServiceClient, service),
            runtime=runtime_services,
            org=org,
            sandbox_provider_config=DaytonaProviderConfig(
                DAYTONA_API_KEY="test-api-key", DAYTONA_API_URL="http://localhost:8001", DAYTONA_TARGET="us"
            ),
            sandbox_provider=context.provider,
            creation_semaphore=Semaphore(1),
            queue_context=context,
            started_by_email=None,
            record_cancellation=lambda _attempts: None,
            authority=authority,
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
                current = _task(postgres_engine, task)
                assert current.started_at == task.started_at, (current.status, current.started_at, task.started_at)
                assert not worker.done()
                await asyncio.sleep(0.01)
            assert candidate.executor_dispatch_id == authority.dispatch_id
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
                        - admission._RELEASED_RESERVATION_RETENTION
                        - timedelta(seconds=1)
                    )
                )
                session.commit()
            await worker
    finally:
        allow_cleanup.set()
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await scheduler.stop()

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


@pytest.mark.description("An abandoned reservation needs confirmed provider cleanup.")
@pytest.mark.parametrize("provider_outcome", ["deleted", "missing", "lookup_failed", "delete_failed", "wrong_name"])
async def test_abandoned_reservation_requires_confirmed_provider_cleanup(
    postgres_engine: Engine, postgres_session: Session, monkeypatch: pytest.MonkeyPatch, provider_outcome: str
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    _, _, (task,) = _run(postgres_session, context.pool_id, [("abandoned", TaskStatus.PENDING, _ATTEMPT)])
    _reserve(postgres_session, context.pool_id, task)
    expected_name = f"queued-{task.id.hex}-{int(_ATTEMPT.replace(tzinfo=UTC).timestamp() * 1_000_000):x}"

    async def lookup(name: str) -> Sandbox:
        assert name == expected_name
        assert await _pool_lock_is_free(postgres_engine, context.pool_id)
        assert not await _build_lock_is_free(postgres_engine, task)
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
        assert not await _build_lock_is_free(postgres_engine, task)
        assert _reservation_count(postgres_engine, context.pool_id) == 1
        if provider_outcome == "delete_failed":
            raise SandboxError("Deletion failed")

    deletion = AsyncMock(side_effect=remove)
    monkeypatch.setattr(context.provider, "get_sandbox", AsyncMock(side_effect=lookup))
    monkeypatch.setattr(context.provider, "delete_sandbox", deletion)

    await _recover_once(context)

    expected_reservations = 0 if provider_outcome in {"deleted", "missing"} else 1
    assert _reservation_count(postgres_engine, context.pool_id) == expected_reservations
    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert await _build_lock_is_free(postgres_engine, task)
    if provider_outcome not in {"deleted", "delete_failed"}:
        deletion.assert_not_awaited()

    if expected_reservations:
        monkeypatch.setattr(context.provider, "get_sandbox", AsyncMock(side_effect=SandboxNotFoundError("Absent")))
        await _recover_once(context)
        assert _reservation_count(postgres_engine, context.pool_id) == 0


@pytest.mark.description(
    "Recovery releases an abandoned reservation whose cleanup keeps failing, frees its build lock, and returns the task to `PENDING`."
)
@pytest.mark.parametrize("provider_outcome", ["lookup_failed", "delete_failed", "wrong_name"])
async def test_abandoned_reservation_with_failing_cleanup_is_released_after_grace(
    postgres_engine: Engine, postgres_session: Session, monkeypatch: pytest.MonkeyPatch, provider_outcome: str
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    _, _, (task,) = _run(postgres_session, context.pool_id, [("leaked", TaskStatus.PENDING, _ATTEMPT)])
    _reserve(postgres_session, context.pool_id, task)
    postgres_session.exec(
        update(SandboxBuildReservation)
        .where(col(SandboxBuildReservation.task_row_id) == task.id)
        .values(reserved_at=datetime.now(UTC) - admission._ORPHAN_CLEANUP_GRACE - timedelta(seconds=1))
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

    await _recover_once(context)

    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert await _build_lock_is_free(postgres_engine, task)


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
    executor_authority: Any,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("cancelled", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    creation_state = SandboxCreationState(cleanup_pending=False)
    creating = asyncio.Event()
    finish_creation = asyncio.Event()
    deleting = asyncio.Event()
    finish_deletion = asyncio.Event()
    acknowledged = outcome in ("acknowledged", "already_gone")
    expected_count = 0 if acknowledged else 1
    create_error = SandboxError("creation unknown")
    delete_error = SandboxError("deletion rejected")
    lock_releases: list[int] = []

    build_locks: list[advisory_lock.PostgresAdvisoryLock] = []
    original_release = advisory_lock.PostgresAdvisoryLock.release

    async def observe_release(lock: advisory_lock.PostgresAdvisoryLock) -> None:
        if lock in build_locks:
            assert lock.held
            _assert_reserved_demand(postgres_engine, context.pool_id, expected_count)
            lock_releases.append(expected_count)
        await original_release(lock)

    def build_lock(engine: Engine, task_row_id: UUID) -> advisory_lock.PostgresAdvisoryLock:
        lock = advisory_lock.task_build_lock(engine, task_row_id)
        build_locks.append(lock)
        return lock

    monkeypatch.setattr(advisory_lock.PostgresAdvisoryLock, "release", observe_release)
    monkeypatch.setattr(admission, "task_build_lock", build_lock)

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
            _enter_provider_sandbox(stack, context, task, authority, creation_state, Semaphore(1))
        )
        try:
            await asyncio.wait_for(creating.wait(), timeout=10)
            _assert_reserved_demand(postgres_engine, context.pool_id, 1)
            assert not await _build_lock_is_free(postgres_engine, task)
            assert not await _pool_lock_is_free(postgres_engine, context.pool_id)
            _stop_run(postgres_engine, benchmark)
            entering.cancel()
            delivered = asyncio.Event()
            asyncio.get_running_loop().call_soon(delivered.set)
            await delivered.wait()
            if outcome == "second_cancel_create":
                entering.cancel()
            else:
                finish_creation.set()
                if outcome != "unknown_create":
                    await asyncio.wait_for(deleting.wait(), timeout=10)
                    assert creation_state.cleanup_pending
                    _assert_reserved_demand(postgres_engine, context.pool_id, 1)
                    assert not await _build_lock_is_free(postgres_engine, task)
                    assert not entering.done()
                    if outcome == "second_cancel_delete":
                        entering.cancel()
                    else:
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
    assert lock_releases == [expected_count]
    assert await _build_lock_is_free(postgres_engine, task)
    assert await _pool_lock_is_free(postgres_engine, context.pool_id)
    with Session(postgres_engine) as session:
        reservations = session.exec(
            select(SandboxBuildReservation).where(col(SandboxBuildReservation.released_at).is_(None))
        ).all()
        assert len(reservations) == expected_count
        if reservations:
            assert reservations[0].task_row_id == task.id
            assert reservations[0].attempt_started_at == task.started_at

    if acknowledged:
        new_attempt = task.started_at + timedelta(microseconds=1)
        _update_task(postgres_engine, task, TaskStatus.PENDING, new_attempt)
        with Session(postgres_engine) as session:
            run = session.get(Benchmark, benchmark.id)
            assert run is not None
            run.status = BenchmarkStatus.IN_PROGRESS
            session.commit()
            current = session.get(Task, task.id)
            assert current is not None
            _prepare(session, context.pool_id, current, dispatch_id=authority.dispatch_id)
            assert (
                SchedulerRepository(session).grant_prepared_tasks(
                    context.pool_id, {("region-a", "sysbox"): _CAPACITY}, SchedulerRepository(session).database_now(), 1
                )
                == 1
            )
            assert not SchedulerRepository(session).accept_grant(task.id, task.started_at, authority.dispatch_id)
            assert SchedulerRepository(session).accept_grant(task.id, new_attempt, authority.dispatch_id)
            session.commit()


@pytest.mark.description("A database error on promotion cleans up before releasing ownership.")
@pytest.mark.parametrize("delete_fails", [False, True], ids=["acknowledged", "provider-delete-error"])
async def test_promotion_database_error_cleans_up_before_releasing_ownership(
    delete_fails: bool,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("promotion-error", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    creation_state = SandboxCreationState(cleanup_pending=False)
    expected_count = 1 if delete_fails else 0
    cleanup_ownership: list[bool] = []
    lock_releases: list[int] = []
    build_locks: list[advisory_lock.PostgresAdvisoryLock] = []
    original_release = advisory_lock.PostgresAdvisoryLock.release

    async def observe_release(lock: advisory_lock.PostgresAdvisoryLock) -> None:
        if lock in build_locks:
            assert lock.held
            assert _task(postgres_engine, task).status == TaskStatus.BUILDING
            lock_releases.append(_reservation_count(postgres_engine, context.pool_id))
        await original_release(lock)

    def build_lock(engine: Engine, task_row_id: UUID) -> advisory_lock.PostgresAdvisoryLock:
        lock = advisory_lock.task_build_lock(engine, task_row_id)
        build_locks.append(lock)
        return lock

    monkeypatch.setattr(advisory_lock.PostgresAdvisoryLock, "release", observe_release)
    monkeypatch.setattr(admission, "task_build_lock", build_lock)
    provider = cast(MockProvider, context.provider)
    provider_delete = provider.delete_sandbox

    async def observe_delete(sandbox_id: str) -> None:
        assert creation_state.cleanup_pending
        cleanup_ownership.append(not await _build_lock_is_free(postgres_engine, task))
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
                    _enter_provider_sandbox(stack, context, task, authority, creation_state, Semaphore(1)),
                    timeout=10,
                )

            assert lock_releases == [expected_count]
            assert cleanup_ownership == [True]
            assert creation_state.cleanup_pending is delete_fails
            _assert_reserved_demand(postgres_engine, context.pool_id, expected_count)

        assert await _build_lock_is_free(postgres_engine, task)
        assert await _pool_lock_is_free(postgres_engine, context.pool_id)
        if delete_fails:
            monkeypatch.setattr(provider, "get_sandbox", AsyncMock(side_effect=SandboxError("provider unavailable")))

        await _recover_once(context)
        recovered = _task(postgres_engine, task)
        assert recovered.status == TaskStatus.PENDING
        _assert_reserved_demand(postgres_engine, context.pool_id, expected_count)
        with Session(postgres_engine) as session:
            _prepare(session, context.pool_id, recovered, dispatch_id=authority.dispatch_id)
            assert SchedulerRepository(session).grant_prepared_tasks(
                context.pool_id, {("region-a", "sysbox"): _CAPACITY}, SchedulerRepository(session).database_now(), 1
            ) == (0 if delete_fails else 1)
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
    executor_authority: Any,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("waiting", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    creation_state = SandboxCreationState(cleanup_pending=False)
    waiting = asyncio.Event()

    class ObservedSemaphore(Semaphore):
        async def acquire(self) -> Literal[True]:
            waiting.set()
            return await super().acquire()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter_provider_sandbox(stack, context, task, authority, creation_state, ObservedSemaphore(0))
        )
        await asyncio.wait_for(waiting.wait(), timeout=10)
        _assert_reserved_demand(postgres_engine, context.pool_id, 1)
        assert not await _build_lock_is_free(postgres_engine, task)
        entering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await entering
    assert not creation_state.cleanup_pending
    assert events == ["capacity", "read_capacity"]
    _assert_reserved_demand(postgres_engine, context.pool_id, 0)
    assert await _build_lock_is_free(postgres_engine, task)
    assert await _pool_lock_is_free(postgres_engine, context.pool_id)


@pytest.mark.description(
    "An interrupted reserved promotion releases its demand, and recovery leaves the task `BUILDING` if stopped or `PENDING` otherwise."
)
@pytest.mark.parametrize("interruption", ["superseded", "stopped", "revoked"])
async def test_reserved_promotion_requires_exact_attempt_live_run_and_authority(
    interruption: str,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    events: list[str] = []
    context = _context(postgres_engine, provider_pool_id, events)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("fenced", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)

    async def interrupt() -> None:
        if interruption == "superseded":
            _update_task(postgres_engine, task, TaskStatus.PENDING, task.started_at + timedelta(microseconds=1))
        elif interruption == "stopped":
            _stop_run(postgres_engine, benchmark)
        else:
            _revoke_dispatch(postgres_engine, authority)

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
            _enter_provider_sandbox(stack, context, task, authority, creation_state, Semaphore(1))
        )
        try:
            await asyncio.wait_for(cleaning.wait(), timeout=10)
            assert creation_state.cleanup_pending
            _assert_reserved_demand(postgres_engine, context.pool_id, 1)
            assert not await _build_lock_is_free(postgres_engine, task)
            assert not await _pool_lock_is_free(postgres_engine, context.pool_id)
            expected_status = TaskStatus.PENDING if interruption == "superseded" else TaskStatus.BUILDING
            assert _task(postgres_engine, task).status == expected_status
        finally:
            finish_cleanup.set()
        sandbox = await asyncio.wait_for(entering, timeout=10)

    assert sandbox is None
    assert not creation_state.cleanup_pending
    assert events == ["capacity", "read_capacity", "create", "cleanup"]
    assert await _build_lock_is_free(postgres_engine, task)
    _assert_reserved_demand(postgres_engine, context.pool_id, 0)

    await _recover_once(context)

    recovered_status = TaskStatus.BUILDING if interruption == "stopped" else TaskStatus.PENDING
    assert _task(postgres_engine, task).status == recovered_status
    _assert_reserved_demand(postgres_engine, context.pool_id, 0)


@pytest.mark.description("Recovery keeps builds that changed after candidate selection.")
@pytest.mark.parametrize(
    ("interruption", "expected_status", "expected_reservations"),
    [
        ("promoted", TaskStatus.IN_PROGRESS, 0),
        ("stopped", TaskStatus.BUILDING, 0),
    ],
)
async def test_recovery_preserves_builds_changed_after_candidate_selection(
    interruption: str,
    expected_status: TaskStatus,
    expected_reservations: int,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, [])
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("racing", TaskStatus.PENDING, _ATTEMPT)])
    _reserve(postgres_session, context.pool_id, task)
    original_try_locks = SchedulerRepository.try_task_build_transaction_locks
    interleaved = False

    def interleave_before_lock(repository: SchedulerRepository, task_row_ids: Sequence[UUID]) -> set[UUID]:
        nonlocal interleaved
        if not interleaved and task_row_ids:
            interleaved = True
            (task_row_id,) = task_row_ids
            with Session(postgres_engine) as concurrent_session:
                if interruption == "promoted":
                    assert admission._start_claimed_task(
                        concurrent_session, task_row_id=task_row_id, expected_started_at=_ATTEMPT
                    )
                else:
                    row = concurrent_session.get(Benchmark, benchmark.id)
                    assert row
                    row.status = BenchmarkStatus.STOPPED
                concurrent_session.commit()
        return original_try_locks(repository, task_row_ids)

    monkeypatch.setattr(SchedulerRepository, "try_task_build_transaction_locks", interleave_before_lock)

    await _recover_once(context)

    recovered = _task(postgres_engine, task)
    assert interleaved
    assert recovered.status == expected_status
    assert recovered.started_at == _ATTEMPT
    assert _reservation_count(postgres_engine, context.pool_id) == expected_reservations


@pytest.mark.description("Reserved snapshot builds reserve provider demand outside the pool lock.")
async def test_reserved_snapshot_builds_reserve_the_provider_demand_outside_the_pool_lock(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
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
    context = _context(postgres_engine, provider_pool_id, events, demand=demand)
    cast(MockProvider, context.provider).domains = [
        SandboxCapacityDomain(target_id="region-a", sandbox_class="sysbox", capacity=_CAPACITY),
        SandboxCapacityDomain(
            target_id="region-b",
            sandbox_class="vm",
            capacity=_CAPACITY.model_copy(update={"disk": ResourceCapacity(total=100, used=0)}),
        ),
    ]
    _, benchmark, (first, second) = _run(
        postgres_session,
        context.pool_id,
        [
            ("first", TaskStatus.PENDING, _ATTEMPT),
            ("second", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    authority = executor_authority(benchmark, session=postgres_session)
    creating = 0
    both_creating = asyncio.Event()
    finish_creating = asyncio.Event()
    observed: list[tuple[bool, int, tuple[float, float, float]]] = []

    async def hold_create() -> None:
        nonlocal creating
        creating += 1
        if creating == 2:
            with Session(postgres_engine) as session:
                reserved = SchedulerRepository(session).active_reservations(context.pool_id)
            observed.append(
                (
                    await _pool_lock_is_free(postgres_engine, context.pool_id),
                    reserved.count,
                    (reserved.vcpu, reserved.memory, reserved.disk),
                )
            )
            both_creating.set()
        await finish_creating.wait()

    source = SnapshotSource(snapshot="snapshot")
    async with AsyncExitStack() as first_stack, AsyncExitStack() as second_stack:
        entering = asyncio.gather(
            _enter(first_stack, context, first, events, authority, source=source, on_create=hold_create),
            _enter(second_stack, context, second, events, authority, source=source, on_create=hold_create),
        )
        try:
            await asyncio.wait_for(both_creating.wait(), timeout=10)
        finally:
            finish_creating.set()
        sandboxes = await asyncio.wait_for(entering, timeout=10)

        assert all(sandboxes)
        assert {_task(postgres_engine, task).status for task in (first, second)} == {TaskStatus.IN_PROGRESS}

    assert observed == [(False, 2, (6, 12, 41))]
    with Session(postgres_engine) as session:
        rows = session.exec(
            select(SandboxBuildReservation).where(
                col(SandboxBuildReservation.pool_id) == store.queue_pool_lock_id(context.pool_id)
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
    executor_authority: Any,
) -> None:
    events: list[str] = []
    error = SandboxError("snapshot metadata unavailable")
    context = _context(postgres_engine, f"daytona:{uuid4()}", events, demand=error)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("unknown", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)

    async with AsyncExitStack() as stack:
        with pytest.raises(SandboxError) as raised:
            await _enter(stack, context, task, events, authority, source=SnapshotSource(snapshot="snapshot"))

    assert raised.value is error
    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert _candidate_count(postgres_engine, context.pool_id) == 0
    assert "create" not in events


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


@pytest.mark.description(
    "A provider rejection leaves no candidate or reservation and frees the preparation slot and build lock."
)
@pytest.mark.parametrize("outcome", ["ready", "cancelled"])
async def test_provider_rejection_publishes_candidate_and_releases_preparation_slot(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
    outcome: str,
) -> None:
    events: list[str] = []
    demand = _DEMAND.model_copy(update={"vcpu": 2.5})
    context = _context(
        postgres_engine,
        f"daytona:{uuid4()}",
        events,
        capacity=(False, True),
        demand=demand,
    )
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("rejected", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    preparation_slot = Semaphore(1)
    creation_state = SandboxCreationState(cleanup_pending=False)
    async with AsyncExitStack() as stack:

        async def enter() -> Sandbox | None:
            async with AsyncExitStack() as preparation_stack:
                await preparation_stack.enter_async_context(preparation_slot)
                return await admission.enter_queued_sandbox(
                    stack=stack,
                    context=context,
                    task_row_id=task.id,
                    expected_started_at=task.started_at,
                    authority=authority,
                    source=SnapshotSource(snapshot="provider-rejected"),
                    resources=_RESOURCES,
                    creation_state=creation_state,
                    create=_sandbox(events, creation_state=creation_state),
                    preparation_stack=preparation_stack,
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
            assert not preparation_slot.locked()
            assert _task(postgres_engine, task).status == TaskStatus.PENDING
            assert _reservation_count(postgres_engine, context.pool_id) == 0
            assert events.count("capacity") == 1
            assert "create" not in events
            assert await _build_lock_is_free(postgres_engine, task)

            for _ in range(3):
                _publish_run_state(context, authority)
                await asyncio.sleep(0)
            assert events.count("capacity") == 1

            if outcome == "ready":
                with Session(postgres_engine) as session:
                    assert (
                        SchedulerRepository(session).request_readiness_checks(
                            context.pool_id, limit=1, retry_delay=timedelta(0)
                        )
                        == 1
                    )
                    session.commit()
                _publish_run_state(context, authority)
                async with _drive_queue(context, authority):
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
    assert await _build_lock_is_free(postgres_engine, task)


@pytest.mark.description("Cancelling during preparation makes no create call and frees the build lock.")
@pytest.mark.parametrize("stage", ["metadata", "provider-check"])
async def test_cancellation_during_preparation_leaves_no_candidate_or_reservation(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
    stage: str,
) -> None:
    events: list[str] = []
    started = asyncio.Event()
    hold = asyncio.Event()

    async def block() -> None:
        started.set()
        await hold.wait()

    context = _context(
        postgres_engine,
        f"daytona:{uuid4()}",
        events,
        on_check=block if stage == "provider-check" else None,
    )
    if stage == "metadata":

        async def prepare(_source: SandboxSource, _resources: Resources) -> SandboxReservationDemand:
            await block()
            return _DEMAND

        monkeypatch.setattr(context.provider, "reservable_demand", prepare)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("cancelled", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(_enter(stack, context, task, events, authority))
        try:
            await asyncio.wait_for(started.wait(), timeout=10)
            assert _candidate_count(postgres_engine, context.pool_id) == 0
            assert await _pool_lock_is_free(postgres_engine, context.pool_id)
        finally:
            entering.cancel()
            with pytest.raises(asyncio.CancelledError):
                await entering

    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert _candidate_count(postgres_engine, context.pool_id) == 0
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    assert "create" not in events
    assert await _build_lock_is_free(postgres_engine, task)


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
    executor_authority: Any,
    runtime_services: RuntimeServices,
    grading_path: str,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events)
    org, benchmark, (task,) = _run(postgres_session, context.pool_id, [("task_0", TaskStatus.PENDING, _ATTEMPT)])
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
    authority = executor_authority(benchmark, session=postgres_session)

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
    monkeypatch.setattr(task_execution, "engine", postgres_engine)
    monkeypatch.setattr("tracker.services.tasks.monitor.engine", postgres_engine)
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

    async with _drive_queue(context, authority):
        result = await task_execution.process_task(
            task,
            benchmark.managed_start_benchmark_request(),
            cast(BenchmarkServiceClient, service),
            benchmark.id,
            task.task_id,
            runtime_services,
            org,
            sandbox_provider_config=DaytonaProviderConfig(
                DAYTONA_API_KEY="test-api-key", DAYTONA_API_URL="http://localhost:8001", DAYTONA_TARGET="us"
            ),
            sandbox_provider=context.provider,
            creation_semaphore=Semaphore(1),
            queue_context=context,
            authority=authority,
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
    executor_authority: Any,
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
                        context = _context(postgres_engine, f"daytona:{uuid4()}", events)
                        org, seed, _ = _run(
                            postgres_session, context.pool_id, [("fixture", TaskStatus.PENDING, _ATTEMPT)]
                        )
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
                            context.pool_id,
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
                        authority = executor_authority(benchmark, dispatch_id=dispatch_id, session=postgres_session)

                        async def resolve(names: dict[str, str], _store: Any) -> dict[str, str]:
                            assert names["MODEL_GATEWAY_API_KEY"] == reference
                            return {"MODEL_GATEWAY_URL": gateway_url, "MODEL_GATEWAY_API_KEY": proof["root"]}

                        monkeypatch.setattr(task_execution, "resolve_secrets", resolve)
                        monkeypatch.setattr(task_execution, "engine", postgres_engine)
                        monkeypatch.setattr("tracker.services.tasks.monitor.engine", postgres_engine)
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
                            async with _drive_queue(context, authority):
                                result = await task_execution.process_task(
                                    task,
                                    benchmark.managed_start_benchmark_request(),
                                    cast(BenchmarkServiceClient, service),
                                    benchmark.id,
                                    task.task_id,
                                    runtime_services,
                                    org,
                                    sandbox_provider_config=DaytonaProviderConfig(
                                        DAYTONA_API_KEY="local",
                                        DAYTONA_API_URL="http://localhost:8001",
                                        DAYTONA_TARGET="us",
                                    ),
                                    sandbox_provider=context.provider,
                                    creation_semaphore=Semaphore(1),
                                    queue_context=context,
                                    authority=authority,
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
