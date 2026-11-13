import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock
from uuid import UUID

import httpx
import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import VerifyTaskIdsResponse, VersionResponse
from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from fastapi.testclient import TestClient
from sqlmodel import Session, col, select

import main as main_module
import tracker.repositories.project as project_repository_module
import tracker.services.runs.limits as run_limits_module
from tests.factories import make_error_result
from tests.utils import TEST_ORG_ID
from tracker.api import dependencies as run_dependencies
from tracker.clients.projects import ProjectServiceClient
from tracker.repositories.executor_release import ExecutorReleaseRepository
from tracker.repositories.provider_secret import ProviderSecretRepository
from tracker.auth import RequestIdentity
from tracker.aws.runtime import AWSResources
from tracker.database.models import (
    DEFAULT_ORG_NAME,
    Benchmark,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchPayload,
    ExecutorRelease,
    ErrorResult,
    Org,
    Project,
    RunProject,
    Task,
)
from tracker.database.models import FinalEvaluation
from tracker.domain import SpendType, TaskStatus
from tracker.domain import BenchmarkStatus, ExecutorReleaseStatus
from tracker.services.runs.limits import OrgRunLimitsCache
from tracker.types import AWSBenchmarkArguments, AgentContractRequest, StartBenchmarkRequest
from tracker.aws.secrets import SecretsManagerStore
from tracker.exceptions import SecretsError
from tracker.executor.dependencies import get_execution_runtime
from tracker.executor.dispatch_payload import SealedPayload, open_payload
from tracker.local import config as local_config
from tracker.local.resources import LocalResources
from tracker.local.runtime import LocalRuntimeFactory
from tracker.services.runs import attempts as attempts_module
from tracker.services.tasks.attempts import keep_replaced_attempts
from tracker.services.tasks.execute import task_attempt_id
from tracker.runtime.services import RuntimeServices
from tracker.secret_access import tenant_secret_prefix

ORG = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
PROVIDER_SETTINGS = {
    "daytona": "DAYTONA_API_KEY=daytona-key\nDAYTONA_API_URL=https://daytona.test/api\nDAYTONA_TARGET=us\n",
    "modal": "MODAL_TOKEN_ID=modal-id\nMODAL_TOKEN_SECRET=modal-secret\n",
}


async def _version(*_args: Any, **_kwargs: Any) -> VersionResponse:
    return VersionResponse(framework_version="test")


@pytest.fixture
def local_tracker(control_postgres_session: Session, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    release = ExecutorRelease(
        id="local-release",
        artifact_uri="source:///local-release",
        artifact_digest="a" * 64,
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
        status=ExecutorReleaseStatus.ACTIVE,
    )
    admission = control_postgres_session.get(ExecutorAdmission, 1)
    assert admission is not None
    control_postgres_session.add(release)
    control_postgres_session.flush()
    admission.release_id = release.id
    control_postgres_session.add(admission)
    control_postgres_session.commit()

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    async def health_check(*_args: Any, **_kwargs: Any) -> object:
        return object()

    async def noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    overrides = main_module.app.dependency_overrides
    monkeypatch.setitem(overrides, main_module.get_session, lambda: control_postgres_session)
    monkeypatch.setitem(overrides, main_module.get_current_org, lambda: ORG)
    monkeypatch.setitem(overrides, run_dependencies.get_current_org, lambda: ORG)
    monkeypatch.setitem(overrides, main_module.get_current_runner, lambda: RequestIdentity(org=ORG))
    monkeypatch.setattr(BenchmarkServiceClient, "health_check", health_check)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr(BenchmarkServiceClient, "version", _version)
    monkeypatch.setattr(BenchmarkServiceClient, "close", noop)
    monkeypatch.setattr(main_module, "copy_agent_to_benchmark", noop)
    monkeypatch.setattr(main_module, "launch_dispatch", noop)
    monkeypatch.setattr(main_module, "SANDBOX_QUEUE_ENABLED", False)
    return TestClient(main_module.app, base_url="http://localhost")


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, settings: str | None) -> None:
    secrets_file = None
    if settings is not None:
        secrets_file = tmp_path / "credentials.env"
        secrets_file.write_text(settings)
    resources = LocalResources(data_root=tmp_path / "data", secrets_file=secrets_file)
    monkeypatch.setattr(local_config, "resources", resources)


def _start(client: TestClient, provider: str, secrets: dict[str, str] | None = None) -> httpx.Response:
    contract = AgentContractRequest(name="agent", install_cmd="true", run_cmd="true", secrets=secrets or {})
    request = StartBenchmarkRequest(contract=contract, benchmark_name="hello", sandbox_provider=provider)
    return client.post("/start-benchmark", json=request.model_dump(mode="json"))


def _benchmark(session: Session, response: httpx.Response) -> Benchmark:
    assert response.status_code == 200, response.text
    session.expire_all()
    benchmark = session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    return benchmark


@pytest.mark.description(
    "A run keeps its provider and the executor rebuilds it from the file without persisting credentials."
)
@pytest.mark.parametrize("provider", ["daytona", "modal"])
async def test_executor_builds_saved_provider_from_secrets_file(
    provider: str,
    local_tracker: TestClient,
    control_postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS[provider] + "MODEL_KEY=model-value\n")

    benchmark = _benchmark(control_postgres_session, _start(local_tracker, provider, {"MODEL_KEY": "model-key"}))
    runtime = await get_execution_runtime(
        benchmark.local_start_benchmark_request(service_headers={}),
        benchmark_id=benchmark.id,
        org_id=ORG.id,
        org_name=ORG.name,
        org_storage_key=ORG.storage_key,
        saved_environment=benchmark.arguments.environment,
        saved_properties=benchmark.arguments.properties,
        saved_sandbox_provider=benchmark.arguments.sandbox_provider,
        saved_sandbox_provider_secret_name=benchmark.arguments.sandbox_provider_secret_name,
        context_version=None,
    )

    assert benchmark.arguments.environment == "local"
    assert benchmark.arguments.sandbox_provider == provider
    assert (await runtime.get_sandbox_provider_config()).type == provider
    assert await runtime.secrets.get("model-key") == {"MODEL_KEY": "model-value"}
    stored = benchmark.model_dump_json()
    assert all(value not in stored for value in ("daytona-key", "modal-secret", "model-value"))


@pytest.mark.description("Start names the missing provider settings without echoing configured values.")
@pytest.mark.parametrize("queue_enabled", [False, True])
def test_start_rejects_incomplete_provider_settings(
    queue_enabled: bool,
    local_tracker: TestClient,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main_module, "SANDBOX_QUEUE_ENABLED", queue_enabled)
    _configure(monkeypatch, tmp_path, "DAYTONA_API_KEY=daytona-key\n")

    response = _start(local_tracker, "daytona")

    assert response.status_code == 400
    assert "DAYTONA_API_URL: Field required" in response.json()["detail"]
    assert "daytona-key" not in response.text


@pytest.mark.description("With the sandbox queue on, admission builds the Daytona provider from the local file.")
def test_queue_admission_reads_provider_from_secrets_file(
    local_tracker: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(main_module, "SANDBOX_QUEUE_ENABLED", True)
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])

    response = _start(local_tracker, "daytona")

    assert response.status_code == 200, response.text


@pytest.mark.description("Starting a local run on a hosted provider needs a secrets file.")
def test_start_requires_secrets_file_for_hosted_provider(
    local_tracker: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, tmp_path, None)

    response = _start(local_tracker, "modal")

    assert response.status_code == 400
    assert "Local modal execution requires a secrets_file" in response.json()["detail"]


@pytest.mark.description("Dispatch refuses contract secrets that would expose the operator's provider credentials.")
@pytest.mark.parametrize("name", ["DAYTONA_API_KEY", "MODAL_TOKEN_SECRET"])
async def test_contract_cannot_read_provider_settings(
    name: str,
    local_tracker: TestClient,
    control_postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"] + PROVIDER_SETTINGS["modal"])
    benchmark = _benchmark(control_postgres_session, _start(local_tracker, "daytona", {name: "provider"}))

    with pytest.raises(SecretsError, match=f"cannot read sandbox provider settings: {name}"):
        await get_execution_runtime(
            benchmark.local_start_benchmark_request(service_headers={}),
            benchmark_id=benchmark.id,
            org_id=ORG.id,
            org_name=ORG.name,
            org_storage_key=ORG.storage_key,
            saved_environment=benchmark.arguments.environment,
            saved_properties=benchmark.arguments.properties,
            saved_sandbox_provider=benchmark.arguments.sandbox_provider,
            saved_sandbox_provider_secret_name=benchmark.arguments.sandbox_provider_secret_name,
            context_version=None,
        )


@pytest.mark.description("Force-stop signals the run's own provider, configured from the original secrets file.")
async def test_force_stop_rebuilds_saved_provider(
    local_tracker: TestClient,
    control_postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["modal"])
    benchmark = _benchmark(control_postgres_session, _start(local_tracker, "modal"))
    runtimes: list[RuntimeServices] = []

    async def force_stop_sandboxes(
        _benchmark: Benchmark, runtime: RuntimeServices, *_args: Any, **_kwargs: Any
    ) -> None:
        runtimes.append(runtime)

    monkeypatch.setattr(main_module, "force_stop_sandboxes", force_stop_sandboxes)
    (tmp_path / "other").mkdir()
    _configure(monkeypatch, tmp_path / "other", PROVIDER_SETTINGS["daytona"])

    response = local_tracker.post(f"/stop-benchmark/{benchmark.id}?force=true")

    assert response.status_code == 200, response.text
    assert (await runtimes[0].get_sandbox_provider_config()).type == "modal"


def _use_secrets_manager(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        local_config, "resources", LocalResources(data_root=tmp_path / "data", secrets_manager_region="us-east-1")
    )

    def provider_secrets(*_args: object) -> dict[str, str]:
        return {"daytona": "provider-secret"}

    async def get(_store: SecretsManagerStore, _name: str) -> dict[str, str]:
        return {"DAYTONA_API_KEY": "daytona-key", "DAYTONA_API_URL": "https://daytona.test/api", "DAYTONA_TARGET": "us"}

    monkeypatch.setattr(ProviderSecretRepository, "list_all", provider_secrets)
    monkeypatch.setattr(SecretsManagerStore, "get", get)


@pytest.mark.description(
    "A Secrets Manager-backed run keeps its provider secret, and force-stop rebuilds the same runtime."
)
async def test_secrets_manager_run_rebuilds_saved_provider_for_force_stop(
    local_tracker: TestClient,
    control_postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_secrets_manager(monkeypatch, tmp_path)
    monkeypatch.setattr(main_module, "AUTH_REQUIRED", True)
    monkeypatch.setattr("tracker.config.AWS_SHARED_SECRET_TENANTS", "")
    benchmark = _benchmark(control_postgres_session, _start(local_tracker, "daytona"))
    runtimes: list[RuntimeServices] = []

    async def force_stop_sandboxes(
        _benchmark: Benchmark, runtime: RuntimeServices, *_args: Any, **_kwargs: Any
    ) -> None:
        runtimes.append(runtime)

    monkeypatch.setattr(main_module, "force_stop_sandboxes", force_stop_sandboxes)

    response = local_tracker.post(f"/stop-benchmark/{benchmark.id}?force=true")

    assert response.status_code == 200, response.text
    assert benchmark.arguments.sandbox_provider_secret_name == f"{tenant_secret_prefix(TEST_ORG_ID)}provider-secret"
    assert isinstance(runtimes[0].secrets, SecretsManagerStore)
    assert (await runtimes[0].get_sandbox_provider_config()).type == "daytona"


@pytest.mark.description("Combinations the local runtime cannot serve are rejected before a run is created.")
@pytest.mark.parametrize(
    ("secrets_manager", "fields", "detail"),
    [
        (False, {"lambda_function": "callback-fn"}, "Lambda callbacks require S3 storage"),
        (False, {"sandbox_provider": "docker"}, "Docker sandbox provider requires a local benchmark service"),
        (True, {"lambda_function": "callback-fn"}, "Lambda callbacks require S3 storage"),
        (True, {}, "Sandbox access requires a provider secret name"),
        (True, {}, "daytona.DAYTONA_TARGET: Field required"),
    ],
    ids=["lambda", "docker-hosted-service", "lambda-secrets-manager", "no-provider-secret", "invalid-provider-secret"],
)
def test_start_rejects_incompatible_composition(
    secrets_manager: bool,
    fields: dict[str, str],
    detail: str,
    local_tracker: TestClient,
    control_postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    if secrets_manager:
        _use_secrets_manager(monkeypatch, tmp_path)

        if "DAYTONA_TARGET" in detail:

            async def get(_store: SecretsManagerStore, _name: str) -> dict[str, str]:
                return {"DAYTONA_API_KEY": "daytona-key", "DAYTONA_API_URL": "https://daytona.test/api"}

            monkeypatch.setattr(SecretsManagerStore, "get", get)
        else:

            def no_provider_secrets(*_args: object) -> dict[str, str]:
                return {}

            monkeypatch.setattr(ProviderSecretRepository, "list_all", no_provider_secrets)
    contract = AgentContractRequest(name="agent", install_cmd="true", run_cmd="true")
    request = {"benchmark_name": "hello", "sandbox_provider": "daytona", **fields}

    response = local_tracker.post("/start-benchmark", json={**request, "contract": contract.model_dump(mode="json")})

    assert response.status_code == 400, response.text
    assert detail in response.json()["detail"]
    assert not control_postgres_session.exec(select(Benchmark)).all()


@pytest.mark.description("Resuming with a Lambda callback and exporting to S3 both need S3 storage.")
def test_filesystem_runs_reject_resume_callbacks_and_s3_export(
    local_tracker: TestClient,
    control_postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    benchmark = _benchmark(control_postgres_session, _start(local_tracker, "daytona"))
    benchmark.status = BenchmarkStatus.STOPPED
    control_postgres_session.add(benchmark)
    control_postgres_session.commit()

    resume = local_tracker.post(
        f"/retry-or-resume-benchmark/{benchmark.id}",
        json={"task_ids": ["task-1"], "lambda_function": "callback-fn"},
    )
    export = local_tracker.get("/retrieve-results", params={"benchmark_id": str(benchmark.id), "s3": True})

    assert (resume.status_code, resume.json()["detail"]) == (400, "Lambda callbacks require S3 storage")
    assert (export.status_code, export.json()["detail"]) == (400, "S3 export requires S3 storage")


@pytest.mark.description("Resuming a Secrets Manager-backed run checks secret ownership like an AWS run.")
@pytest.mark.parametrize(
    ("secret_name", "expected_status"),
    [("ModelKey", 200), ("valkyrie/tenants/7f1c2a1e-0000-4000-8000-000000000000/ModelKey", 404)],
    ids=["own", "other-org"],
)
def test_secrets_manager_resume_limits_secret_names_to_the_organization(
    secret_name: str,
    expected_status: int,
    local_tracker: TestClient,
    control_postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_secrets_manager(monkeypatch, tmp_path)
    benchmark = _benchmark(control_postgres_session, _start(local_tracker, "daytona"))
    benchmark.status = BenchmarkStatus.STOPPED
    control_postgres_session.add(benchmark)
    control_postgres_session.commit()
    monkeypatch.setattr(main_module, "AUTH_REQUIRED", True)
    monkeypatch.setattr("tracker.config.AWS_SHARED_SECRET_TENANTS", "")

    response = local_tracker.post(
        f"/retry-or-resume-benchmark/{benchmark.id}",
        json={"task_ids": ["task-1"], "secrets": {"MODEL_KEY": secret_name}},
    )

    assert response.status_code == expected_status, response.text


@pytest.mark.description("Secret names from an uploaded agent are resolved for the organization.")
def test_secrets_manager_start_authorizes_uploaded_agent_secrets(
    local_tracker: TestClient,
    control_postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_secrets_manager(monkeypatch, tmp_path)
    monkeypatch.setattr(main_module, "AUTH_REQUIRED", True)
    monkeypatch.setattr("tracker.config.AWS_SHARED_SECRET_TENANTS", "")

    async def uploaded_contract(*_args: object) -> AgentContractRequest:
        return AgentContractRequest(name="agent", install_cmd="true", run_cmd="true", secrets={"MODEL_KEY": "AgentKey"})

    monkeypatch.setattr(main_module, "_resolve_contract_from_s3", uploaded_contract)
    response = local_tracker.post(
        "/start-benchmark",
        json={"benchmark_name": "hello", "sandbox_provider": "daytona", "contract": {"name": "agent"}},
    )

    benchmark = _benchmark(control_postgres_session, response)
    assert benchmark.arguments.contract.secrets == {"MODEL_KEY": f"{tenant_secret_prefix(TEST_ORG_ID)}AgentKey"}


def _finished_source(session: Session, response: httpx.Response) -> Benchmark:
    source = _benchmark(session, response)
    source.status = BenchmarkStatus.FINISHED
    source.arguments = source.arguments.model_copy(update={"lambda_function": "vals-format"})
    (task,) = session.exec(select(Task).where(Task.benchmark == source.id)).all()
    task.status, task.eval_resume_state = TaskStatus.FINISHED, {"job": "pass"}
    failed = Task(
        org_id=source.org_id,
        benchmark=source.id,
        task_id="task-2",
        status=TaskStatus.FINISHED,
        eval_resume_state={"job": "fail"},
    )
    stateless = Task(org_id=source.org_id, benchmark=source.id, task_id="task-3", status=TaskStatus.ERROR)
    session.add_all([source, task, failed, stateless])
    session.add(FinalEvaluation(org_id=source.org_id, benchmark=source.id, final_score=0.5))
    session.commit()
    return source


def _snapshot(session: Session, benchmark_id: UUID) -> list[dict[str, Any]]:
    session.expire_all()
    rows: list[Any] = [session.get(Benchmark, benchmark_id)]
    rows += session.exec(select(Task).where(Task.benchmark == benchmark_id).order_by(col(Task.task_id))).all()
    rows += session.exec(select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark_id)).all()
    return [row.model_dump(mode="json") for row in rows]


@pytest.mark.description(
    "Regrade copies a finished run's stateful tasks into a new run on the target dataset, reports the stateless "
    "ones, forwards the service headers, and leaves the source run and its evaluation unchanged."
)
def test_regrade_copies_saved_states_into_a_new_run(
    local_tracker: TestClient, control_postgres_session: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    source = _finished_source(control_postgres_session, _start(local_tracker, "daytona"))
    project = Project(org_id=source.org_id, handle="regrade", name="Regrade", spend_type=SpendType.INTERNAL)
    control_postgres_session.add(project)
    control_postgres_session.flush()
    project_id = project.id
    control_postgres_session.add(RunProject(benchmark_id=source.id, project_id=project_id))
    control_postgres_session.commit()
    before = _snapshot(control_postgres_session, source.id)
    verified: list[tuple[list[str], str | None]] = []

    async def verify_task_ids(
        _client: BenchmarkServiceClient, *, task_ids: list[str], slice_str: str | None, dataset: str | None
    ) -> VerifyTaskIdsResponse:
        verified.append((task_ids, dataset))
        return VerifyTaskIdsResponse(task_ids=task_ids)

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)

    service_headers = {"Authorization": "service-credential"}
    response = local_tracker.post(
        f"/regrade-benchmark/{source.id}",
        json={"dataset": "target-dataset", "concurrency": 8, "service_headers": service_headers},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["regraded_from_run_id"] == str(source.id)
    assert (body["task_ids"], body["skipped_task_ids"]) == (["task-1", "task-2"], ["task-3"])
    assert verified == [(["task-1", "task-2"], "target-dataset")]
    assert _snapshot(control_postgres_session, source.id) == before
    run = control_postgres_session.get(Benchmark, UUID(body["benchmark_id"]))
    assert run is not None
    assert (run.status, run.name, run.arguments.regraded_from_run_id) == (
        BenchmarkStatus.IN_PROGRESS,
        source.name,
        source.id,
    )
    assert (run.arguments.dataset, run.arguments.lambda_function) == ("target-dataset", None)
    assert run.arguments.contract == source.arguments.contract
    assert source.arguments.agent_attempts == run.arguments.agent_attempts == []
    assert (source.arguments.concurrency, run.arguments.concurrency) == (5, 8)
    assert run.arguments.sandbox_provider == source.arguments.sandbox_provider
    dispatch = control_postgres_session.exec(
        select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == run.id)
    ).one()
    sealed = control_postgres_session.get(ExecutorDispatchPayload, dispatch.id)
    assert sealed is not None
    payload = open_payload(dispatch.id, SealedPayload(sealed.ciphertext, sealed.encrypted_data_key, sealed.nonce))
    assert payload["execution_context_json"]["start_benchmark_request"]["service_headers"] == service_headers
    tasks = control_postgres_session.exec(
        select(Task).where(Task.benchmark == run.id).order_by(col(Task.task_id))
    ).all()
    assert [(task.task_id, task.status, task.eval_resume_state, task.started_at) for task in tasks] == [
        ("task-1", TaskStatus.EVALUATING, {"job": "pass"}, dispatch.created_at),
        ("task-2", TaskStatus.EVALUATING, {"job": "fail"}, dispatch.created_at),
        ("task-3", TaskStatus.ERROR, None, dispatch.created_at),
    ]
    (error,) = control_postgres_session.exec(select(ErrorResult).where(ErrorResult.task == tasks[2].id)).all()
    assert error.error_message == f"Source run {source.id} saved no eval_resume_state for this task."
    links = control_postgres_session.exec(select(RunProject).where(RunProject.benchmark_id == run.id)).all()
    assert [link.project_id for link in links] == [project_id]
    fetched = local_tracker.get("/fetch-benchmark", params={"benchmark_id": str(run.id)}).json()
    metadata = local_tracker.get(f"/fetch-benchmark-metadata/{run.id}").json()
    for payload in (fetched, metadata):
        assert (payload["regraded_from_run_id"], payload["regrade_skipped_task_ids"]) == (str(source.id), ["task-3"])


@pytest.mark.description(
    "With reads on, a regrade reads its project from the service before it takes the run admission lock."
)
def test_regrade_reads_the_project_service_before_the_admission_lock(
    local_tracker: TestClient, control_postgres_session: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    source = _finished_source(control_postgres_session, _start(local_tracker, "daytona"))
    project = Project(org_id=source.org_id, handle="regrade", name="Regrade", spend_type=SpendType.INTERNAL)
    control_postgres_session.add(project)
    control_postgres_session.flush()
    control_postgres_session.add(RunProject(benchmark_id=source.id, project_id=project.id))
    control_postgres_session.commit()
    row = project.model_dump(mode="json", exclude={"org_id"})
    events: list[str] = []

    def answer(_request: httpx.Request) -> httpx.Response:
        events.append("service")
        return httpx.Response(200, json=row)

    service = ProjectServiceClient(httpx.Client(base_url="http://projects", transport=httpx.MockTransport(answer)))
    monkeypatch.setattr(project_repository_module, "project_service_reader", lambda: service)
    admission = ExecutorReleaseRepository.admission

    def locked_admission(self: ExecutorReleaseRepository, *, for_update: bool = False) -> ExecutorAdmission | None:
        if for_update:
            events.append("lock")
        return admission(self, for_update=for_update)

    monkeypatch.setattr(ExecutorReleaseRepository, "admission", locked_admission)

    async def verify_task_ids(
        _client: BenchmarkServiceClient, *, task_ids: list[str], slice_str: str | None, dataset: str | None
    ) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=task_ids)

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)

    response = local_tracker.post(f"/regrade-benchmark/{source.id}", json={"dataset": "target-dataset"})

    assert response.status_code == 200, response.text
    assert "service" in events and "lock" in events, events
    assert "service" not in events[events.index("lock") :], events
    links = control_postgres_session.exec(
        select(RunProject).where(RunProject.benchmark_id == UUID(response.json()["benchmark_id"]))
    ).all()
    assert [link.project_id for link in links] == [project.id]


@pytest.mark.description("Regrade returns 429 and creates no run when the org's run limit is reached.")
def test_regrade_at_run_limit_is_refused(
    local_tracker: TestClient, control_postgres_session: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    source = _finished_source(control_postgres_session, _start(local_tracker, "daytona"))
    limits = OrgRunLimitsCache(read=lambda: json.dumps({"default": 0}))
    limits.refresh()
    monkeypatch.setattr(run_limits_module, "run_limits", limits)

    async def verify_task_ids(
        _client: BenchmarkServiceClient, *, task_ids: list[str], slice_str: str | None, dataset: str | None
    ) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=task_ids)

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    runs_before = len(control_postgres_session.exec(select(Benchmark)).all())

    response = local_tracker.post(f"/regrade-benchmark/{source.id}", json={"dataset": "target-dataset"})

    assert response.status_code == 429, response.text
    assert (response.headers["Retry-After"], response.headers["X-RateLimit-Limit"]) == ("60", "0")
    control_postgres_session.expire_all()
    assert len(control_postgres_session.exec(select(Benchmark)).all()) == runs_before


@pytest.mark.description(
    "Regrade of a run whose linked project the service answers 404 for raises `LookupError` and commits no new run or link."
)
def test_regrade_of_a_run_whose_project_the_service_lost_creates_no_run(
    local_tracker: TestClient, control_postgres_session: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    source = _finished_source(control_postgres_session, _start(local_tracker, "daytona"))
    project = Project(org_id=source.org_id, handle="regrade", name="Regrade", spend_type=SpendType.INTERNAL)
    control_postgres_session.add(project)
    control_postgres_session.flush()
    control_postgres_session.add(RunProject(benchmark_id=source.id, project_id=project.id))
    control_postgres_session.commit()
    source_status = source.status
    service = ProjectServiceClient(
        httpx.Client(base_url="http://projects", transport=httpx.MockTransport(lambda _request: httpx.Response(404)))
    )
    monkeypatch.setattr(project_repository_module, "project_service_reader", lambda: service)

    async def verify_task_ids(
        _client: BenchmarkServiceClient, *, task_ids: list[str], slice_str: str | None, dataset: str | None
    ) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=task_ids)

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    runs_before = len(control_postgres_session.exec(select(Benchmark)).all())
    links_before = len(control_postgres_session.exec(select(RunProject)).all())

    with pytest.raises(LookupError, match=str(project.id)):
        _ = local_tracker.post(f"/regrade-benchmark/{source.id}", json={"dataset": "target-dataset"})

    control_postgres_session.expire_all()
    assert len(control_postgres_session.exec(select(Benchmark)).all()) == runs_before
    assert len(control_postgres_session.exec(select(RunProject)).all()) == links_before
    stored = control_postgres_session.get(Benchmark, source.id)
    assert stored is not None and stored.status == source_status


@pytest.mark.description(
    "Regrade onto the source run's own dataset works, so a run can be regraded as a noise control."
)
def test_regrade_onto_the_source_dataset(
    local_tracker: TestClient, control_postgres_session: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    source = _finished_source(control_postgres_session, _start(local_tracker, "daytona"))
    source.arguments = source.arguments.model_copy(update={"dataset": "terminal-bench-4.0"})
    control_postgres_session.add(source)
    control_postgres_session.commit()
    before = _snapshot(control_postgres_session, source.id)
    verified: list[tuple[list[str], str | None]] = []

    async def verify_task_ids(
        _client: BenchmarkServiceClient, *, task_ids: list[str], slice_str: str | None, dataset: str | None
    ) -> VerifyTaskIdsResponse:
        verified.append((task_ids, dataset))
        return VerifyTaskIdsResponse(task_ids=task_ids)

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)

    response = local_tracker.post(f"/regrade-benchmark/{source.id}", json={"dataset": "terminal-bench-4.0"})

    assert response.status_code == 200, response.text
    run = control_postgres_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert run is not None
    assert (run.arguments.dataset, run.arguments.regraded_from_run_id) == ("terminal-bench-4.0", source.id)
    assert verified == [(["task-1", "task-2"], "terminal-bench-4.0")]
    assert _snapshot(control_postgres_session, source.id) == before


@pytest.mark.description(
    "Regrade gives a source run from before managed AWS the organization's current start properties, so the new "
    "run passes the managed resource check, and leaves the source run's properties unchanged."
)
def test_regrade_gives_a_pre_migration_source_fresh_start_properties(
    local_tracker: TestClient, control_postgres_session: Session, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    source = _finished_source(control_postgres_session, _start(local_tracker, "daytona"))
    legacy = AWSResources(
        region="us-east-1", s3_bucket="legacy-bucket", log_group="/valkyrie/benchmarks", log_retention_days=7
    )
    source.arguments = AWSBenchmarkArguments(
        **source.arguments.model_dump(exclude={"environment", "properties", "sandbox_provider_secret_name"}),
        sandbox_provider_secret_name=f"{tenant_secret_prefix(TEST_ORG_ID)}provider-secret",
        properties=legacy,
    )
    control_postgres_session.add(source)
    control_postgres_session.commit()
    before = _snapshot(control_postgres_session, source.id)
    fresh = AWSResources(
        region="us-east-1",
        s3_bucket="org-bucket",
        log_group=f"/valkyrie/benchmarks/tenants/{TEST_ORG_ID}",
        log_retention_days=30,
    )
    monkeypatch.setattr(main_module, "resolve_start_aws_runtime", Mock(return_value=SimpleNamespace(resources=fresh)))
    launched: list[UUID] = []

    async def launch(dispatch: ExecutorDispatch, **_kwargs: Any) -> None:
        launched.append(dispatch.benchmark_id)

    async def verify_task_ids(
        _client: BenchmarkServiceClient, *, task_ids: list[str], slice_str: str | None, dataset: str | None
    ) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=task_ids)

    monkeypatch.setattr(main_module, "_launch_executor_dispatch", launch)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)

    response = local_tracker.post(f"/regrade-benchmark/{source.id}", json={"dataset": "target-dataset"})

    assert response.status_code == 200, response.text
    run = control_postgres_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert run is not None
    assert (run.arguments.environment, run.arguments.properties, launched) == ("aws", fresh, [run.id])
    assert _snapshot(control_postgres_session, source.id) == before


@pytest.mark.description(
    (
        "Retry and resume on a regrade run refuse anything that would run the agent.\n"
        "\n"
        "Test cases:\n"
        "- from_scratch mode.\n"
        "- A skipped task ID.\n"
        "- A task ID outside the run.\n"
        "- Resume with a task ID outside the run."
    )
)
@pytest.mark.parametrize(
    ("params", "task_ids"),
    [
        ({"retry": "true", "retry_mode": "from_scratch"}, ["task-1"]),
        ({"retry": "true"}, ["task-3"]),
        ({"retry": "true"}, ["task-9"]),
        ({}, ["task-9"]),
    ],
    ids=["from-scratch", "skipped-task", "unknown-task", "resume-unknown-task"],
)
def test_regrade_run_refuses_agent_reruns(
    params: dict[str, str],
    task_ids: list[str],
    local_tracker: TestClient,
    control_postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    source = _finished_source(control_postgres_session, _start(local_tracker, "daytona"))

    async def verify_task_ids(
        _client: BenchmarkServiceClient, *, task_ids: list[str], slice_str: str | None, dataset: str | None
    ) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=task_ids)

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    response = local_tracker.post(f"/regrade-benchmark/{source.id}", json={"dataset": "target-dataset"})
    assert response.status_code == 200, response.text
    run = control_postgres_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert run is not None
    run.status, run.finished_at = BenchmarkStatus.FINISHED, run.started_at
    for task in control_postgres_session.exec(select(Task).where(Task.benchmark == run.id)).all():
        if task.status == TaskStatus.EVALUATING:
            task.status = TaskStatus.ERROR
            control_postgres_session.add(task)
    control_postgres_session.add(run)
    control_postgres_session.commit()
    before = _snapshot(control_postgres_session, run.id)

    response = local_tracker.post(f"/retry-or-resume-benchmark/{run.id}", params=params, json={"task_ids": task_ids})

    assert response.status_code == 400, response.text
    assert "regrade run" in response.json()["detail"]
    assert _snapshot(control_postgres_session, run.id) == before


@pytest.mark.description(
    (
        "Regrade refuses without creating a run.\n"
        "\n"
        "Test cases:\n"
        "- The source run is still in progress.\n"
        "- The target dataset lacks a copied task.\n"
        "- The source run's project is archived."
    )
)
@pytest.mark.parametrize(
    ("case", "status_code"),
    [("unfinished", 409), ("missing-task", 400), ("archived-project", 409)],
    ids=["unfinished", "missing-task", "archived-project"],
)
def test_regrade_refuses_without_creating_a_run(
    case: str,
    status_code: int,
    local_tracker: TestClient,
    control_postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    response = _start(local_tracker, "daytona")
    source = (
        _benchmark(control_postgres_session, response)
        if case == "unfinished"
        else _finished_source(control_postgres_session, response)
    )
    if case == "archived-project":
        project = Project(
            org_id=source.org_id,
            handle="archived",
            name="Archived",
            spend_type=SpendType.INTERNAL,
            archived_at=datetime.now(UTC),
        )
        control_postgres_session.add(project)
        control_postgres_session.flush()
        control_postgres_session.add(RunProject(benchmark_id=source.id, project_id=project.id))
        control_postgres_session.commit()

        async def verify_task_ids(
            _client: BenchmarkServiceClient, *, task_ids: list[str], slice_str: str | None, dataset: str | None
        ) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=task_ids)

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    before = _snapshot(control_postgres_session, source.id)

    response = local_tracker.post(f"/regrade-benchmark/{source.id}", json={"dataset": "target-dataset"})

    assert response.status_code == status_code, response.text
    assert _snapshot(control_postgres_session, source.id) == before
    assert len(control_postgres_session.exec(select(Benchmark)).all()) == 1


@pytest.mark.description(
    "After a retry, the replaced attempt's `vals_format/result.json` and error download from local storage."
)
async def test_retry_keeps_failed_attempt_result_and_error_in_local_storage(
    local_tracker: TestClient,
    control_postgres_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure(monkeypatch, tmp_path, PROVIDER_SETTINGS["daytona"])
    monkeypatch.setattr(main_module, "record_prior_attempts", attempts_module.record_prior_attempts)
    monkeypatch.setitem(main_module.app.dependency_overrides, run_dependencies.get_current_org, lambda: ORG)
    benchmark = _benchmark(control_postgres_session, _start(local_tracker, "daytona"))
    benchmark.status = BenchmarkStatus.STOPPED
    task = control_postgres_session.exec(select(Task).where(Task.benchmark == benchmark.id)).one()
    task.status = TaskStatus.ERROR
    control_postgres_session.add_all([benchmark, task])
    control_postgres_session.flush()
    control_postgres_session.add(make_error_result(task, "agent crashed", task.started_at))
    control_postgres_session.commit()
    attempt = task_attempt_id(task.id, task.started_at)
    assert isinstance(benchmark.arguments.properties, LocalResources)
    objects = LocalRuntimeFactory.create_runtime(benchmark.arguments.properties, TEST_ORG_ID).objects
    await objects.put_bytes(f"benchmarks/{benchmark.id}/task-1/vals_format/result.json", b'{"score": 0}')

    retry = local_tracker.post(
        f"/retry-or-resume-benchmark/{benchmark.id}", params={"retry": True}, json={"task_ids": ["task-1"]}
    )
    assert retry.status_code == 200, retry.text
    await keep_replaced_attempts(objects, str(benchmark.id), "task-1", datetime.now(UTC))
    await objects.put_bytes(f"benchmarks/{benchmark.id}/task-1/vals_format/result.json", b'{"score": 1}')

    listed = local_tracker.get(f"/benchmarks/{benchmark.id}/artifacts", params={"prefix": "task-1", "attempts": True})
    assert listed.status_code == 200, listed.text
    assert [entry["path"] for entry in listed.json()["artifacts"]] == [
        f"task-1/{attempt}/attempt.json",
        f"task-1/{attempt}/vals_format/result.json",
    ]

    def download(path: str) -> bytes:
        link = local_tracker.get(
            f"/benchmarks/{benchmark.id}/artifacts/download-url", params={"path": path, "attempts": True}
        )
        assert link.status_code == 200, link.text
        content = local_tracker.get(link.json()["download_url"])
        assert content.status_code == 200, content.text
        return content.content

    assert download(f"task-1/{attempt}/vals_format/result.json") == b'{"score": 0}'
    assert json.loads(download(f"task-1/{attempt}/attempt.json"))["error"] == "agent crashed"
    current = local_tracker.get(f"/benchmarks/{benchmark.id}/artifacts", params={"prefix": "task-1"})
    assert [entry["path"] for entry in current.json()["artifacts"]] == ["task-1/vals_format/result.json"]
