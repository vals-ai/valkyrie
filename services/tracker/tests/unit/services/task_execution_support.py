import asyncio
from asyncio import Semaphore
from datetime import datetime
from collections.abc import Awaitable, Callable, Generator
from typing import Any, cast
from uuid import UUID

import pytest
from benchmark_service import ImageSource, Resources
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import RetrieveTaskResponse
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from main import app
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity, get_current_org
from tracker.aws.runtime import AWSResources
from tracker.database.models import Org, Task
from tracker.database.session import get_session
from tracker.domain import BenchmarkStatus
from tracker.executor.control_protocol import TaskRef
from tracker.executor.control_tasks import TaskControl
from tracker.external_service_gateway import ExternalServiceDeadlineController, GenerationAccountingSummary
from tracker.runtime.services import RuntimeServices
from tracker.scheduler.admission import SandboxQueueContext
from tracker.types import AgentContractRequest, OrgContext, StartBenchmarkRequest
from tracker.services.tasks.execute import process_task
from tracker.services.benchmarks import start_benchmark_request_to_benchmark

TEST_ORG = Org(id=TEST_ORG_ID, name="default")
_TEST_STARTER = RequestIdentity(org=TEST_ORG)


class FixedClock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    @classmethod
    def at(cls, moment: datetime, monkeypatch: pytest.MonkeyPatch) -> "FixedClock":
        clock = cls(moment)
        monkeypatch.setattr("tracker.services.tasks.execute.sighting_time", lambda: clock.moment)
        return clock


@pytest.fixture
def task_api_client(control_postgres_session: Session, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    engine = control_postgres_session.bind
    org = control_postgres_session.exec(select(Org).where(Org.id == TEST_ORG_ID)).one()

    def request_session() -> Generator[Session, None, None]:
        with Session(engine) as session:
            yield session

    monkeypatch.setitem(app.dependency_overrides, get_session, request_session)
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: org)
    return TestClient(app)


async def finalize_agent_generation(
    *,
    deadline_controller: ExternalServiceDeadlineController,
    on_external_service_finalized: Callable[[GenerationAccountingSummary], Awaitable[None]],
) -> None:
    await deadline_controller.finalize_after_confirmed_stop(absence_confirmed_at=asyncio.get_running_loop().time())
    await on_external_service_finalized(deadline_controller.summary())


class MockLauncher:
    def __init__(self) -> None:
        self.queued_calls: list[dict[str, Any]] = []

    def record(self, payload: dict[str, Any]) -> None:
        self.queued_calls.append(payload)


def make_retrieve_task_response(problem_path: str = "/tmp/problem_statement.txt") -> RetrieveTaskResponse:
    return RetrieveTaskResponse(
        source=ImageSource(image="test-image:latest"),
        problem_path=problem_path,
        cwd="/testbed",
        resources=Resources(vcpu=2, memory=4, disk=5),
    )


def create_task_environment(
    contract: AgentContractRequest,
    database_session: Session,
    bound_controls: Callable[..., dict[str, object]],
    run_starter: RequestIdentity | None = None,
) -> tuple[StartBenchmarkRequest, Task, UUID, TaskControl]:
    start_benchmark_request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        concurrency=1,
        task_ids=["task_0"],
        sandbox_provider="daytona",
        sandbox_provider_secret_name="test-daytona-secret",
        properties=AWSResources(
            region="us-east-1",
            s3_bucket="test-bucket",
            log_group="test-log-group",
            log_retention_days=30,
        ),
    )
    benchmark_row = start_benchmark_request_to_benchmark(
        start_benchmark_request,
        run_starter or _TEST_STARTER,
    )
    benchmark_row.status = BenchmarkStatus.IN_PROGRESS
    database_session.add(benchmark_row)
    database_session.commit()

    task_row = Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark_row.id)
    database_session.add(task_row)
    database_session.commit()

    controls = bound_controls(benchmark_row, session=database_session)
    return start_benchmark_request, task_row, benchmark_row.id, cast(TaskControl, controls["task_control"])


async def run_process_task(
    start_benchmark_request: StartBenchmarkRequest,
    task_row: Task,
    benchmark_id: UUID,
    runtime_services: RuntimeServices,
    control: TaskControl,
    *,
    queue_context: SandboxQueueContext | None = None,
    service: BenchmarkServiceClient | None = None,
) -> dict[str, dict[str, Any] | None]:
    response = await control.read(TaskRef(org_id=task_row.org_id, task_row_id=task_row.id, task_id=task_row.task_id))
    state = response.task
    sandbox_provider_config = await runtime_services.get_sandbox_provider_config()
    async with (
        service or start_benchmark_request.benchmark_service as benchmark_service,
        sandbox_provider_config.create_provider() as sandbox_provider,
    ):
        return await process_task(
            task_row=state,
            start_benchmark_request=start_benchmark_request,
            benchmark_service=benchmark_service,
            benchmark_id=benchmark_id,
            task_id=task_row.task_id,
            runtime=runtime_services,
            org=OrgContext(id=TEST_ORG_ID, name="default", storage_key="test-bucket"),
            sandbox_provider_config=sandbox_provider_config,
            sandbox_provider=sandbox_provider,
            creation_semaphore=Semaphore(1),
            queue_context=queue_context,
            control=control,
            started_by_email=response.benchmark.started_by_email,
        )
