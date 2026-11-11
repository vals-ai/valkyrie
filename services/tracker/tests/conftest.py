from collections.abc import AsyncGenerator, Callable, Generator
from datetime import UTC, datetime, timedelta
from sqlite3 import Connection, Cursor
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from dotenv import load_dotenv
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import ConnectionPoolEntry
from sqlmodel import Session, SQLModel, StaticPool, create_engine

from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID, FakeAccessKeys, add_sqlite_btrim
from tracker.database.models import (
    Benchmark,
    DEFAULT_ORG_NAME,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorRelease,
    Org,
)
from executor_protocol import ExecutorDispatchStatus
from tracker.types import AgentContractRequest
from tracker.domain import ExecutorDispatchKind
from tracker.aws.clients import AWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.database.session import get_session
from tracker.executor.control_app import app as control_app
from tracker.executor.control_client import ControlClient
from tracker.executor.control_protocol import DispatchRef
from tracker.executor.control_queue import QueueControl
from tracker.executor.control_runs import RunControl
from tracker.executor.control_store import invocation_digest
from tracker.executor.control_tasks import TaskControl

if TYPE_CHECKING:
    from testcontainers.postgres import PostgresContainer

_ = load_dotenv()


@pytest.fixture
def aws_runtime() -> AWSRuntime:
    return AWSRuntime(
        resources=AWSResources(
            region="us-east-1",
            s3_bucket="test-bucket",
            log_group="test-log-group",
            log_retention_days=30,
        ),
        clients=AWSClientProvider("us-east-1"),
        expected_bucket_owner="123456789012",
    )


@pytest.fixture(scope="function")
def database_session() -> Generator[Session, None, None]:
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(test_engine, "connect")
    def set_sqlite_pragma(dbapi_connection: Connection, _connection_record: ConnectionPoolEntry) -> None:  # type: ignore
        cursor: Cursor = cast(Cursor, dbapi_connection.cursor())  # type: ignore
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

    event.listen(test_engine, "connect", add_sqlite_btrim)
    SQLModel.metadata.create_all(test_engine)

    try:
        with Session(test_engine, expire_on_commit=False) as session:
            session.add(ExecutorAdmission())
            session.add(Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME, storage_key="test-bucket"))
            session.commit()
            yield session
    finally:
        SQLModel.metadata.drop_all(test_engine)
        test_engine.dispose()


@pytest.fixture(scope="session")
def control_postgres_container() -> Generator["PostgresContainer", None, None]:
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
def control_postgres_engine(control_postgres_container: "PostgresContainer") -> Generator[Engine, None, None]:
    engine = create_engine(control_postgres_container.get_connection_url())
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(ExecutorAdmission())
        session.add(Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME, storage_key="test-bucket"))
        session.commit()
    try:
        yield engine
    finally:
        SQLModel.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture
def control_postgres_session(control_postgres_engine: Engine) -> Generator[Session, None, None]:
    with Session(control_postgres_engine, expire_on_commit=False) as session:
        yield session


@pytest.fixture
async def bound_controls(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[Callable[..., dict[str, object]], None]:
    clients: list[ControlClient] = []

    def create(benchmark: Benchmark, *, session: Session, dispatch_id: UUID | None = None) -> dict[str, object]:
        engine = session.bind
        assert engine is not None and engine.dialect.name == "postgresql"
        if benchmark.current_execution_release_id is None:
            release_id = "control-test-release"
            release = session.get(ExecutorRelease, release_id)
            if release is None:
                release = ExecutorRelease(
                    id=release_id,
                    artifact_uri="source:///control-test-release",
                    artifact_digest="a" * 64,
                    protocol_version=SUPPORTED_PROTOCOL_VERSION,
                )
                session.add(release)
                session.flush()
            benchmark.executor_release_id = release.id
            benchmark.current_execution_release_id = release.id
            benchmark.executor_artifact_uri = release.artifact_uri
            benchmark.executor_artifact_digest = release.artifact_digest
            benchmark.executor_protocol_version = release.protocol_version
        assert benchmark.current_execution_release_id is not None
        assert benchmark.executor_artifact_uri is not None
        assert benchmark.executor_artifact_digest is not None
        assert benchmark.executor_protocol_version is not None
        dispatch = session.get(ExecutorDispatch, dispatch_id) if dispatch_id is not None else None
        if dispatch is None:
            dispatch = ExecutorDispatch(
                id=dispatch_id or uuid4(),
                benchmark_id=benchmark.id,
                kind=ExecutorDispatchKind.START,
                executor_release_id=benchmark.current_execution_release_id,
                executor_artifact_uri=benchmark.executor_artifact_uri,
                executor_artifact_digest=benchmark.executor_artifact_digest,
                executor_protocol_version=benchmark.executor_protocol_version,
            )
        invocation_key = f"test-invocation-{dispatch.id}"
        dispatch.status = ExecutorDispatchStatus.RUNNING
        dispatch.started_at = datetime.now(UTC)
        dispatch.lease_expires_at = datetime.now(UTC) + timedelta(hours=1)
        dispatch.invocation_key_digest = invocation_digest(invocation_key)
        session.add(dispatch)
        session.commit()

        def server_session() -> Generator[Session, None, None]:
            with Session(engine, expire_on_commit=False) as request_session:
                yield request_session

        monkeypatch.setitem(control_app.dependency_overrides, get_session, server_session)
        http = httpx.AsyncClient(transport=httpx.ASGITransport(app=control_app), base_url="http://control.test")
        client = ControlClient(http=http, dispatch_id=dispatch.id, invocation_key=invocation_key)
        clients.append(client)
        ref = DispatchRef(benchmark_id=benchmark.id, dispatch_id=dispatch.id)
        return {
            "executor_dispatch_id": str(dispatch.id),
            "run_control": RunControl(client, ref),
            "task_control": TaskControl(client, ref),
            "queue_control": QueueControl(client, ref),
        }

    yield create
    for client in clients:
        await client.close()


@pytest.fixture
def contract() -> AgentContractRequest:
    return AgentContractRequest(
        name="dummy",
        install_cmd="echo installing dependencies...",
        run_cmd="echo running agent...",
    )


@pytest.fixture
def example_benchmark_object(contract: AgentContractRequest) -> Benchmark:
    return make_benchmark(contract=contract, concurrency=5)


@pytest.fixture
def fake_access_keys(monkeypatch: pytest.MonkeyPatch) -> FakeAccessKeys:
    access_keys = FakeAccessKeys()
    monkeypatch.setattr("tracker.run_access_keys.descope_access_keys", lambda: access_keys)
    return access_keys


@pytest.fixture
def ecs_runner_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "ecs")
    monkeypatch.setenv("EXECUTOR_RUNNER_CLUSTER", "cluster")
    monkeypatch.setenv("EXECUTOR_RUNNER_SUBNETS", "subnet-a,subnet-b")
    monkeypatch.setenv("EXECUTOR_RUNNER_SECURITY_GROUP", "sg-runner")
    monkeypatch.setenv("EXECUTOR_RUNNER_CONTAINER", "ExecutorRunner")


@pytest.fixture(autouse=True)
def record_prior_attempts(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    recorded = AsyncMock(return_value=[])
    monkeypatch.setattr("main.record_prior_attempts", recorded)
    return recorded


@pytest.fixture(autouse=True)
def keep_replaced_attempts(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    kept = AsyncMock()
    monkeypatch.setattr("tracker.services.tasks.execute.keep_replaced_attempts", kept)
    return kept
