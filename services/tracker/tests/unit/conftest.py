import base64
import os
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager
from types import ModuleType
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import (
    FinalScoreResponse,
    HealthCheckResponse,
    RetrieveTaskResponse,
    SetupTaskResponse,
    VerifyTaskIdsResponse,
    VersionResponse,
)
import pytest
from sqlalchemy import event
from sqlmodel import Session, SQLModel, create_engine
from tenacity import wait_none

from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tests.unit.services.task_execution_support import (
    MockLauncher,
    finalize_agent_generation,
    make_retrieve_task_response,
)
from tests.utils import TEST_ORG_ID, add_sqlite_btrim, inline_database_calls_in
import tracker.auth as auth_module
from tracker import sandbox as sandbox_module
from tracker.auth import RequestIdentity, get_current_org, get_current_runner, get_current_starter
from tracker.database.models import (
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchPayload,
    ExecutorRelease,
    ExecutorReleaseStatus,
    Org,
    OrgProviderSecret,
)
from tracker.database.session import get_session
from tracker.executor.dispatch_payload import SealedPayload, open_payload
from tracker.aws.runtime import AWSRuntime
from tracker.aws.services import CloudRuntimeFactory
from tracker.runtime.services import RuntimeServices
from tracker.services.tasks.execute import TaskMonitor

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from main import app


@pytest.fixture
def empty_database_session() -> Generator[Session, None, None]:
    engine = create_engine("sqlite://")
    event.listen(engine, "connect", add_sqlite_btrim)
    SQLModel.metadata.create_all(engine)

    try:
        with Session(engine) as session:
            yield session
    finally:
        SQLModel.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture(autouse=True)
def mock_s3(monkeypatch: pytest.MonkeyPatch) -> None:

    async def _mock_get_bytes(*_args: Any, **_kwargs: Any) -> bytes:
        return b"mock-contract-content"

    async def _mock_upload_to_s3(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def _mock_copy_agent_to_benchmark(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.get_bytes", _mock_get_bytes)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.put_bytes", _mock_upload_to_s3)
    monkeypatch.setattr("main.copy_agent_to_benchmark", _mock_copy_agent_to_benchmark)


@pytest.fixture(autouse=True)
def managed_deployment_config(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in {
        "AWS_DEPLOYMENT_ACCOUNT_ID": "123456789012",
        "AWS_DEPLOYMENT_REGION": "us-east-1",
        "AWS_DEPLOYMENT_S3_BUCKET": "test-bucket",
        "AWS_DEPLOYMENT_LOG_GROUP": "test-log-group",
        "AWS_DEPLOYMENT_LOG_RETENTION_DAYS": "30",
    }.items():
        monkeypatch.setattr("tracker.config." + key, value)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", AsyncMock(return_value=True))


@pytest.fixture(autouse=True)
def override_database_session(database_session: Session, monkeypatch: pytest.MonkeyPatch) -> None:

    def get_test_session() -> Generator[Session, None, None]:
        with Session(database_session.get_bind(), expire_on_commit=False) as request_session:
            yield request_session
        database_session.expire_all()

    monkeypatch.setitem(app.dependency_overrides, get_session, get_test_session)
    monkeypatch.setattr("tracker.sandbox_cost.engine", database_session.get_bind())
    monkeypatch.setattr("tracker.services.sandboxes.engine", database_session.get_bind())


@pytest.fixture(autouse=True)
def inline_database_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    inline_database_calls_in(monkeypatch)


@pytest.fixture(autouse=True)
def override_org(monkeypatch: pytest.MonkeyPatch) -> None:
    test_org = Org(id=TEST_ORG_ID, name="default", storage_key="test-bucket")
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: test_org)


@pytest.fixture(autouse=True)
def override_starter_and_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    test_org = Org(id=TEST_ORG_ID, name="default", storage_key="test-bucket")
    identity = RequestIdentity(org=test_org)
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)
    monkeypatch.setitem(app.dependency_overrides, get_current_runner, lambda: identity)


@pytest.fixture
def mapped_providers(database_session: Session) -> None:
    database_session.add_all(
        OrgProviderSecret(org_id=TEST_ORG_ID, provider=provider, secret_name=secret)
        for provider, secret in {"daytona": "provider-secret", "modal": "ModalSecrets"}.items()
    )
    database_session.commit()


@pytest.fixture
def active_executor_release(database_session: Session) -> None:
    release = ExecutorRelease(
        id="test-release",
        artifact_uri="s3://artifacts/test-release.pex",
        artifact_digest="digest-test-release",
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
        status=ExecutorReleaseStatus.ACTIVE,
    )
    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    database_session.add(release)
    database_session.flush()
    admission.release_id = release.id
    database_session.add(admission)
    database_session.commit()


@pytest.fixture(autouse=True)
def mock_benchmark_service(monkeypatch: pytest.MonkeyPatch) -> None:

    async def _mock_health_check(*_args: Any, **_kwargs: Any) -> HealthCheckResponse:
        return HealthCheckResponse(status="ok")

    async def _mock_version(*_args: Any, **_kwargs: Any) -> VersionResponse:
        return VersionResponse(framework_version="test")

    async def _mock_setup_task(*_args: Any, **_kwargs: Any) -> SetupTaskResponse:
        return SetupTaskResponse(status="ok")

    async def _mock_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=[])

    def _mock_get_sandbox_provider(*_args: Any, **_kwargs: Any) -> Mock:
        return Mock()

    monkeypatch.setattr(BenchmarkServiceClient, "health_check", _mock_health_check)
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", _mock_get_sandbox_provider)
    monkeypatch.setattr(BenchmarkServiceClient, "version", _mock_version)
    monkeypatch.setattr(BenchmarkServiceClient, "setup_task", _mock_setup_task)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)


@pytest.fixture(autouse=True)
def mock_cloudwatch(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    if cast(ModuleType, request.module).__name__ == "tests.unit.aws.test_clients":
        return

    async def _mock_create_benchmark(*_args: Any, **_kwargs: Any) -> None:
        return None

    def _mock_write(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(
        "tracker.aws.cloudwatch_logs.CloudWatchBenchmarkLogSink.create_benchmark", _mock_create_benchmark
    )
    monkeypatch.setattr("tracker.aws.cloudwatch_logs.CloudWatchBenchmarkLogSink.write", _mock_write)


@pytest.fixture(autouse=True)
def mock_secret_store(monkeypatch: pytest.MonkeyPatch) -> None:
    async def get(_self: object, _name: str) -> dict[str, str]:
        return {
            "DAYTONA_API_KEY": "test-key",
            "DAYTONA_API_URL": "http://localhost:8001",
            "DAYTONA_ORGANIZATION_ID": "test-organization",
            "DAYTONA_TARGET": "us",
        }

    monkeypatch.setattr("tracker.aws.secrets.SecretsManagerStore.get", get)


@pytest.fixture(autouse=True)
def mock_lambda_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("main.dry_run_lambda", AsyncMock())


@pytest.fixture(autouse=True)
def no_retry_waits(monkeypatch: pytest.MonkeyPatch) -> None:
    for retried in (
        getattr(auth_module, "_exchange_access_key"),
        getattr(sandbox_module, "upload_agent_artifacts"),
        getattr(sandbox_module, "_run_egress_operation"),
    ):
        monkeypatch.setattr(retried.retry, "wait", wait_none())


@pytest.fixture(autouse=True)
def mock_sandbox_operations(monkeypatch: pytest.MonkeyPatch) -> None:

    async def _noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def _noop_run_agent(*_args: Any, **kwargs: Any) -> tuple[None, float]:
        await finalize_agent_generation(
            deadline_controller=kwargs["deadline_controller"],
            on_external_service_finalized=kwargs["on_external_service_finalized"],
        )
        return None, 0.0

    monkeypatch.setattr("tracker.services.tasks.execute.upload_agent_artifacts", _noop)
    monkeypatch.setattr("tracker.services.tasks.execute.apply_egress_policy", _noop)
    monkeypatch.setattr("tracker.runtime.run_egress.apply_egress_policy", _noop)
    monkeypatch.setattr("tracker.services.tasks.execute.install_agent_dependencies", _noop)
    monkeypatch.setattr("tracker.services.tasks.execute.run_agent", _noop_run_agent)


@pytest.fixture(autouse=True)
def mock_launcher(monkeypatch: pytest.MonkeyPatch, database_session: Session) -> MockLauncher:
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"k" * 32).decode())
    monkeypatch.delenv("EXECUTOR_PAYLOAD_KMS_KEY_ID", raising=False)
    launcher = MockLauncher()

    async def launch(dispatch: ExecutorDispatch) -> None:
        with Session(database_session.get_bind()) as session:
            row = session.get(ExecutorDispatchPayload, dispatch.id)
            assert row is not None
            payload = open_payload(
                dispatch.id,
                SealedPayload(row.ciphertext, row.encrypted_data_key, row.nonce),
            )
            payload.update(
                executor_dispatch_id=str(dispatch.id),
                executor_release_id=dispatch.executor_release_id,
                executor_artifact_uri=dispatch.executor_artifact_uri,
                executor_artifact_digest=dispatch.executor_artifact_digest,
                executor_protocol_version=dispatch.executor_protocol_version,
            )
            launcher.record(payload)

    monkeypatch.setattr("main.launch_dispatch", launch)
    return launcher


@pytest.fixture
def process_benchmark_env(monkeypatch: pytest.MonkeyPatch) -> None:
    sandbox_count = 0

    @asynccontextmanager
    async def _mock_create_sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[AsyncMock, None]:
        mock_sandbox = AsyncMock()
        nonlocal sandbox_count
        sandbox_count += 1
        mock_sandbox.id = "mock-sandbox-id" if sandbox_count == 1 else f"mock-sandbox-id-{sandbox_count}"
        yield mock_sandbox

    async def _mock_retrieve_task(*_args: Any, **_kwargs: Any) -> RetrieveTaskResponse:
        return make_retrieve_task_response()

    async def _mock_evaluate_instance(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"status": "success", "score": 1.0}

    async def _mock_final_score(*_args: Any, evaluation_results: dict[str, Any], **_kwargs: Any) -> FinalScoreResponse:
        tasks_evaluated = list(evaluation_results.keys())
        return FinalScoreResponse(
            tasks_evaluated=tasks_evaluated,
            final_score=50.0,
            metadata={"resolved_tasks": [], "unresolved_tasks": tasks_evaluated},
        )

    async def _mock_verify_task_ids(*_args: Any, task_ids: list[str], **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=task_ids)

    monkeypatch.setattr(TaskMonitor, "_TRACK_INTERVAL", 0)
    monkeypatch.setattr("tracker.services.tasks.execute.create_sandbox", _mock_create_sandbox)
    monkeypatch.setattr(BenchmarkServiceClient, "retrieve_task", _mock_retrieve_task)
    monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", _mock_evaluate_instance)
    monkeypatch.setattr(BenchmarkServiceClient, "final_score", _mock_final_score)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)
    monkeypatch.setattr("tracker.aws.services.dry_run_lambda", AsyncMock())


@pytest.fixture
async def runtime_services(aws_runtime: AWSRuntime) -> AsyncGenerator[RuntimeServices, None]:
    runtime = CloudRuntimeFactory.create_runtime(
        aws_runtime,
        sandbox_provider_secret_name="test-daytona-secret",
    )
    yield runtime


@pytest.fixture
def without_descope_management(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tracker.run_access_keys.AUTH_REQUIRED", True)
    monkeypatch.setattr("tracker.run_access_keys.descope_access_keys", lambda: None)
