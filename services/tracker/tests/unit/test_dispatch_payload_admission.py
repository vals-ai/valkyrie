import io
from hashlib import sha256
from collections.abc import AsyncIterator
import json
import zipfile
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.schemas import VerifyTaskIdsResponse
from botocore.exceptions import ClientError
from tracker.exceptions import S3Error
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlmodel import Session, select

import main
from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from main import app
from tests.project_service import MockProjectRoutes
from tests.conftest import TEST_ORG_ID
from tracker import config
from tracker.auth import RequestIdentity, get_current_runner, get_current_starter
from tracker.types import AgentContractRequest, AgentReleaseSelection, RunMonitoringRules
from tracker.database.models import FinalEvaluation
from tracker.database.models import (
    Benchmark,
    ExecutorDispatch,
    ExecutorDispatchPayload,
    ExecutorRelease,
    Org,
    Project,
    RunProject,
    Task,
    UserBenchmarkService,
)
from tracker.domain import SpendType
from tracker.domain import BenchmarkStatus, TaskStatus
from tracker.executor.dispatch_payload import SealedPayload, open_payload
from tracker.executor.release_control import promote_release
from tracker.local import config as local_config
from tracker.local.resources import LocalResources
from tracker.runtime.storage import ObjectStore, StoredObject, StoredObjectCopy
from tracker.types import StartBenchmarkRequest

pytestmark = pytest.mark.usefixtures("mapped_providers")
client = TestClient(app)

_DISPATCH_ENVELOPE_KEYS = {
    "telemetry_context_json",
    "executor_dispatch_id",
    "executor_release_id",
    "executor_artifact_uri",
    "executor_artifact_digest",
    "executor_protocol_version",
}
_MANAGED_ENVELOPE_KEYS = {"execution_context_json"} | _DISPATCH_ENVELOPE_KEYS


def _configure_managed_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ACCOUNT_ID", "123456789012")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_REGION", "deployment-region")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_S3_BUCKET", "deployment-bucket")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_GROUP", "deployment-log-group")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "30")


def _capture_dispatch_payloads(monkeypatch: pytest.MonkeyPatch, database_session: Session) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []

    async def capture(dispatch: ExecutorDispatch) -> None:
        with Session(database_session.get_bind()) as session:
            row = session.get(ExecutorDispatchPayload, dispatch.id)
            assert row is not None
            payload = open_payload(dispatch.id, SealedPayload(row.ciphertext, row.encrypted_data_key, row.nonce))
            payload.update(
                executor_dispatch_id=str(dispatch.id),
                executor_release_id=dispatch.executor_release_id,
                executor_artifact_uri=dispatch.executor_artifact_uri,
                executor_artifact_digest=dispatch.executor_artifact_digest,
                executor_protocol_version=dispatch.executor_protocol_version,
            )
            payloads.append(payload)

    monkeypatch.setattr("main.launch_dispatch", capture)
    return payloads


def _stop_benchmark(benchmark: Benchmark, session: Session) -> None:
    benchmark.status = BenchmarkStatus.STOPPED
    session.add(benchmark)
    session.commit()


def _promote_test_release(
    session: Session,
    *,
    release_id: str = "producer-test-release",
    protocol_version: str = SUPPORTED_PROTOCOL_VERSION,
) -> None:
    release = ExecutorRelease(
        id=release_id,
        artifact_uri=f"source:///{release_id}",
        artifact_digest="a" * 64,
        protocol_version=protocol_version,
    )
    session.add(release)
    session.commit()
    promote_release(session, release.id)
    session.commit()


def _assert_no_aws_authority(payload: dict[str, Any]) -> None:
    serialized = json.dumps(payload).lower().replace("-", "_")
    for forbidden_key in (
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "aws_profile",
    ):
        assert forbidden_key not in serialized
    assert "caller_" not in serialized


def _start_request(contract: AgentContractRequest) -> StartBenchmarkRequest:
    return StartBenchmarkRequest(
        contract=contract,
        benchmark_name="producer-contract-test",
        task_ids=["task-1"],
        sandbox_provider="daytona",
        sandbox_provider_secret_name="provider-secret",
    )


@pytest.mark.description("Managed start and resume emit a v3 payload with no credentials.")
def test_managed_start_and_resume_emit_credential_free_v3(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    payloads = _capture_dispatch_payloads(monkeypatch, database_session)

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    async def agent_exists(*_args: Any, **_kwargs: Any) -> bool:
        return True

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", agent_exists)
    monkeypatch.setattr("main.copy_agent_to_benchmark", AsyncMock(return_value=True))
    reset_to_in_progress = Mock(return_value=["task-2"])
    monkeypatch.setattr("main.reset_to_in_progress_status", reset_to_in_progress)

    contract.model = None
    contract.kwargs["model"] = "recorded-model"

    response = client.post("/start-benchmark", json=_start_request(contract).model_dump(mode="json"))

    assert response.status_code == 200
    benchmark = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    assert benchmark.arguments.agent_attempts == []
    assert len(payloads) == 1
    assert set(payloads[0]) == _MANAGED_ENVELOPE_KEYS
    start_context = payloads[0]["execution_context_json"]
    assert start_context["version"] == 3
    _assert_no_aws_authority(start_context)

    _stop_benchmark(benchmark, database_session)
    payloads.clear()

    response = client.post(
        f"/retry-or-resume-benchmark/{benchmark.id}",
        json={"secrets": {"MODEL_API_KEY": "resume-model-secret"}},
    )

    assert response.status_code == 200
    assert len(payloads) == 1
    assert set(payloads[0]) == _MANAGED_ENVELOPE_KEYS
    resume_context = payloads[0]["execution_context_json"]
    assert resume_context["version"] == 3
    assert resume_context["benchmark_id"] == str(benchmark.id)
    assert resume_context["verified_task_ids"] == ["task-2"]
    assert resume_context["start_benchmark_request"]["contract"]["secrets"] == {"MODEL_API_KEY": "resume-model-secret"}
    _assert_no_aws_authority(resume_context)

    _stop_benchmark(benchmark, database_session)
    payloads.clear()
    response = client.post(
        f"/retry-or-resume-benchmark/{benchmark.id}",
        json={"secrets": {"aws_secret_access_key": "credential"}},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed execution cannot include AWS credentials"
    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.STOPPED
    assert reset_to_in_progress.call_count == 1
    assert payloads == []


@pytest.mark.description("A managed run without a project keeps the gateway references it was given.")
def test_projectless_managed_run_keeps_supplied_gateway_references(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MODEL_GATEWAY_PROJECT_KEYS_MODE", "automatic")
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    org.name = "vals.ai"
    database_session.add(org)
    database_session.commit()
    identity = RequestIdentity(org=org)
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)
    monkeypatch.setitem(app.dependency_overrides, get_current_runner, lambda: identity)
    monkeypatch.delenv("MODEL_GATEWAY_URL", raising=False)
    monkeypatch.delenv("MODEL_GATEWAY_MACHINE_API_KEY", raising=False)
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    payloads = _capture_dispatch_payloads(monkeypatch, database_session)
    monkeypatch.setattr(
        BenchmarkServiceClient, "verify_task_ids", AsyncMock(return_value=VerifyTaskIdsResponse(task_ids=["task-1"]))
    )
    request = _start_request(
        contract.model_copy(
            update={"secrets": {"MODEL_GATEWAY_URL": "caller-url", "MODEL_GATEWAY_API_KEY": "caller-key"}}
        )
    )

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 200, response.text
    assert response.json()["project"]["handle"] == "default"
    assert len(payloads) == 1
    assert payloads[0]["execution_context_json"]["start_benchmark_request"]["contract"]["secrets"] == {
        "MODEL_GATEWAY_URL": "caller-url",
        "MODEL_GATEWAY_API_KEY": "caller-key",
    }


@pytest.mark.description("A project selected only by file keeps its manual gateway references.")
def test_file_only_selected_project_keeps_manual_gateway_references(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    project_service: MockProjectRoutes,
) -> None:
    from tracker import project_gateway

    monkeypatch.setenv("MODEL_GATEWAY_PROJECT_KEYS_MODE", "automatic")
    monkeypatch.delenv("MODEL_GATEWAY_URL", raising=False)
    monkeypatch.delenv("MODEL_GATEWAY_MACHINE_API_KEY", raising=False)
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    org.name = "vals.ai"
    database_session.add(org)
    database_session.commit()
    identity = RequestIdentity(org=org)
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)
    monkeypatch.setitem(app.dependency_overrides, get_current_runner, lambda: identity)

    secrets_file = tmp_path / "credentials.env"
    secrets_file.write_text("MODEL_GATEWAY_URL=https://manual.example\nMODEL_GATEWAY_API_KEY=manual-key\n")
    monkeypatch.setattr(
        local_config,
        "resources",
        LocalResources(data_root=tmp_path / "runs", secrets_file=secrets_file),
    )
    monkeypatch.setattr(main, "SANDBOX_QUEUE_ENABLED", False)
    monkeypatch.setattr(
        project_gateway,
        "project_gateway_configuration",
        lambda: (_ for _ in ()).throw(AssertionError("file-only execution must not configure Gateway")),
    )
    _promote_test_release(database_session)
    payloads = _capture_dispatch_payloads(monkeypatch, database_session)
    monkeypatch.setattr(
        BenchmarkServiceClient, "verify_task_ids", AsyncMock(return_value=VerifyTaskIdsResponse(task_ids=["task-1"]))
    )
    database_session.add(
        UserBenchmarkService(
            org_id=TEST_ORG_ID,
            user_id="local",
            benchmark="producer-contract-test",
            url="http://localhost:8001",
        )
    )
    database_session.commit()
    local_client = TestClient(app, base_url="http://localhost")
    created = local_client.post(
        "/projects",
        json={"handle": "manual", "name": "Manual Gateway", "spend_type": "internal", "subcategory": "blog"},
    )
    assert created.status_code == 200, created.text
    (project,) = project_service.projects()
    assert (project.gateway_request_id, project.gateway_key_id) == (None, None)
    request = _start_request(
        contract.model_copy(
            update={"secrets": {"MODEL_GATEWAY_URL": "caller-url", "MODEL_GATEWAY_API_KEY": "caller-key"}}
        )
    ).model_copy(
        update={
            "sandbox_provider": "docker",
            "sandbox_provider_secret_name": None,
            "project": "manual",
        }
    )

    response = local_client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 200, response.text
    assert len(payloads) == 1
    assert payloads[0]["execution_context_json"]["start_benchmark_request"]["contract"]["secrets"] == {
        "MODEL_GATEWAY_URL": "caller-url",
        "MODEL_GATEWAY_API_KEY": "caller-key",
    }


@pytest.mark.description("Rebuilding from the bundle is what makes the settings trustworthy.")
async def test_resolving_a_contract_from_s3_attests_its_inference_settings(
    contract: AgentContractRequest,
) -> None:
    contract = contract.model_copy(
        update={"model": "test-model", "kwargs": {"variant": "custom"}, "secrets": {"API_KEY": "override"}}
    )
    object_store = AsyncMock()
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr(
            "dummy/contract.yaml",
            "name: declared-name\ninstall_cmd: 'true'\nrun_cmd: 'echo {problem_statement_path} {variant}'\n"
            "secrets:\n  API_KEY: default\n  EXTRA_KEY: extra\n"
            "kwargs:\n  variant:\n    type: str\n    default: max\n    required: false\n",
        )

    async def get_bytes(key: str) -> bytes:
        if key == "agents/dummy.zip":
            return archive.getvalue()
        raise FileNotFoundError(key)

    object_store.get_bytes.side_effect = get_bytes

    resolved = await main._resolve_contract_from_s3(_start_request(contract), cast(ObjectStore, object_store))

    assert resolved.inference_settings_attested is True
    assert resolved.name == "dummy"
    assert resolved.model == "test-model"
    assert resolved.kwargs == {"variant": "custom"}
    assert resolved.secrets == {"API_KEY": "override", "EXTRA_KEY": "extra"}
    object_store.get_bytes.assert_awaited_once_with("agents/dummy.zip")


@pytest.mark.description("A caller cannot mark its own inference settings tracker-attested.")
def test_start_clears_a_caller_asserted_attestation(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    payloads = _capture_dispatch_payloads(monkeypatch, database_session)

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    async def agent_exists(*_args: Any, **_kwargs: Any) -> bool:
        return True

    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", agent_exists)
    monkeypatch.setattr("main.copy_agent_to_benchmark", AsyncMock(return_value=True))
    resolve_from_s3 = AsyncMock()
    monkeypatch.setattr("main._resolve_contract_from_s3", resolve_from_s3)

    claimed = contract.model_copy(
        update={
            "install_cmd": "echo install",
            "run_cmd": "echo run",
            "kwargs": {"variant": "caller-variant"},
            "inference_settings_attested": True,
        }
    )

    response = client.post("/start-benchmark", json=_start_request(claimed).model_dump(mode="json"))

    assert response.status_code == 200
    resolve_from_s3.assert_not_awaited()
    assert len(payloads) == 1
    started_contract = payloads[0]["execution_context_json"]["start_benchmark_request"]["contract"]
    assert started_contract["inference_settings_attested"] is False
    assert started_contract["kwargs"] == {"variant": "caller-variant"}

    benchmark = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    _stop_benchmark(benchmark, database_session)


@pytest.mark.description("A managed start with AWS authority is rejected before anything is saved.")
def test_managed_start_rejects_aws_authority_before_persistence(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    payloads = _capture_dispatch_payloads(monkeypatch, database_session)

    async def agent_exists(*_args: Any, **_kwargs: Any) -> bool:
        raise AssertionError("invalid managed requests must be rejected before checking S3")

    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", agent_exists)
    request = _start_request(contract).model_copy(update={"service_headers": {"AWS_SECRET_ACCESS_KEY": "credential"}})

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed execution cannot include AWS credentials"
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []
    assert payloads == []


@pytest.mark.description("A managed start whose contract adds AWS credentials is rejected and saves no run or payload.")
def test_managed_start_rejects_aws_authority_from_resolved_contract(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    payloads = _capture_dispatch_payloads(monkeypatch, database_session)
    request = _start_request(contract.model_copy(update={"install_cmd": "", "run_cmd": ""}))
    resolved_contract = contract.model_copy(update={"secrets": {"aws_profile": "credential"}})
    monkeypatch.setattr("main._resolve_contract_from_s3", AsyncMock(return_value=resolved_contract))

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 400
    assert response.json()["detail"] == "Managed execution cannot include AWS credentials"
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []
    assert payloads == []


@pytest.mark.description("Short secret names in a managed contract are saved with the org's secret prefix.")
def test_managed_start_and_resume_store_short_secret_names_under_the_org_prefix(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    monkeypatch.setattr(main, "AUTH_REQUIRED", True)
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "internal.example")
    _capture_dispatch_payloads(monkeypatch, database_session)
    monkeypatch.setattr(
        BenchmarkServiceClient, "verify_task_ids", AsyncMock(return_value=VerifyTaskIdsResponse(task_ids=["task-1"]))
    )
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", AsyncMock(return_value=True))
    monkeypatch.setattr("main.copy_agent_to_benchmark", AsyncMock(return_value=True))
    monkeypatch.setattr("main.reset_to_in_progress_status", Mock(return_value=["task-1"]))
    request = _start_request(contract.model_copy(update={"install_cmd": "", "run_cmd": ""}))
    resolved_contract = contract.model_copy(
        update={"secrets": {"MODEL_KEY": "model-keys", "OLD_KEY": f"valkyrie/tenants/{TEST_ORG_ID}/old-key"}}
    )
    monkeypatch.setattr("main._resolve_contract_from_s3", AsyncMock(return_value=resolved_contract))
    prefix = f"valkyrie/tenants/{TEST_ORG_ID}/"

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 200, response.text
    benchmark = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
    assert benchmark is not None
    assert benchmark.arguments.sandbox_provider_secret_name == f"{prefix}provider-secret"
    assert benchmark.arguments.contract.secrets == {"MODEL_KEY": f"{prefix}model-keys", "OLD_KEY": f"{prefix}old-key"}

    _stop_benchmark(benchmark, database_session)
    response = client.post(f"/retry-or-resume-benchmark/{benchmark.id}", json={"secrets": {"EXTRA_KEY": "extra-key"}})

    assert response.status_code == 200, response.text
    database_session.refresh(benchmark)
    assert benchmark.arguments.contract.secrets["EXTRA_KEY"] == f"{prefix}extra-key"


@pytest.mark.description("A managed start cannot use another org's secret by its short name.")
def test_managed_start_rejects_another_orgs_secret_by_its_short_name(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    monkeypatch.setattr(main, "AUTH_REQUIRED", True)
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "internal.example")
    payloads = _capture_dispatch_payloads(monkeypatch, database_session)
    request = _start_request(contract.model_copy(update={"install_cmd": "", "run_cmd": ""}))
    other = "valkyrie/tenants/00000000-0000-0000-0000-000000000002/model-keys"
    resolved_contract = contract.model_copy(update={"secrets": {"MODEL_KEY": other}})
    monkeypatch.setattr("main._resolve_contract_from_s3", AsyncMock(return_value=resolved_contract))

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))

    assert response.status_code == 404, response.text
    assert response.json()["detail"] == "Secret 'model-keys' does not exist"
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []
    assert payloads == []


@pytest.mark.description("A managed start needs a compatible executor release.")
@pytest.mark.parametrize("protocol_version", ["1", "2"])
def test_managed_start_requires_a_compatible_executor_release(
    protocol_version: str,
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session, protocol_version=protocol_version)
    copy = AsyncMock()
    monkeypatch.setattr("main.copy_agent_to_benchmark", copy)

    response = client.post("/start-benchmark", json=_start_request(contract).model_dump(mode="json"))

    assert response.status_code == 503
    copy.assert_not_awaited()
    assert response.json()["detail"] == "Activate an executor release that supports managed runs"
    assert database_session.exec(select(Benchmark).where(Benchmark.name == "producer-contract-test")).all() == []


@pytest.mark.description(
    "A resume against an incompatible active release leaves the run `STOPPED` and writes no payload."
)
def test_managed_resume_rolls_back_when_the_active_release_is_incompatible(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    payloads = _capture_dispatch_payloads(monkeypatch, database_session)
    monkeypatch.setattr(
        BenchmarkServiceClient,
        "verify_task_ids",
        AsyncMock(return_value=VerifyTaskIdsResponse(task_ids=["task-1"])),
    )
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", AsyncMock(return_value=True))

    response = client.post("/start-benchmark", json=_start_request(contract).model_dump(mode="json"))
    assert response.status_code == 200
    benchmark_id = UUID(response.json()["benchmark_id"])
    benchmark = database_session.get(Benchmark, benchmark_id)
    task = database_session.exec(select(Task).where(Task.benchmark == benchmark_id)).one()
    assert benchmark is not None
    benchmark.status = BenchmarkStatus.STOPPED
    task.status = TaskStatus.ERROR
    database_session.add(benchmark)
    database_session.add(task)
    database_session.commit()
    _promote_test_release(database_session, release_id="legacy-release", protocol_version="1")
    payloads.clear()

    response = client.post(f"/retry-or-resume-benchmark/{benchmark_id}?retry=true")

    assert response.status_code == 503
    assert response.json()["detail"] == "Activate an executor release that supports managed runs"
    database_session.expire_all()
    persisted_benchmark = database_session.get(Benchmark, benchmark_id)
    persisted_task = database_session.get(Task, task.id)
    assert persisted_benchmark is not None
    assert persisted_task is not None
    assert persisted_benchmark.status == BenchmarkStatus.STOPPED
    assert persisted_task.status == TaskStatus.ERROR
    assert payloads == []


@pytest.mark.description("A payload failure on managed resume rolls back the recovery state.")
def test_managed_resume_payload_failure_rolls_back_recovery_state(
    contract: AgentContractRequest,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    payloads = _capture_dispatch_payloads(monkeypatch, database_session)
    monkeypatch.setattr(
        BenchmarkServiceClient,
        "verify_task_ids",
        AsyncMock(return_value=VerifyTaskIdsResponse(task_ids=["task-1"])),
    )
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.exists", AsyncMock(return_value=True))
    monkeypatch.setattr("main.copy_agent_to_benchmark", AsyncMock(return_value=True))

    response = client.post("/start-benchmark", json=_start_request(contract).model_dump(mode="json"))

    assert response.status_code == 200
    benchmark_id = UUID(response.json()["benchmark_id"])
    benchmark = database_session.get(Benchmark, benchmark_id)
    task = database_session.exec(select(Task).where(Task.benchmark == benchmark_id)).one()
    assert benchmark is not None
    benchmark.status = BenchmarkStatus.STOPPED
    task.status = TaskStatus.ERROR
    database_session.add(benchmark)
    database_session.add(task)
    database_session.commit()
    original_dispatch_ids = {
        dispatch.id
        for dispatch in database_session.exec(
            select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark_id)
        ).all()
    }
    payloads.clear()

    def fail_payload_build(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("payload validation failed")

    monkeypatch.setattr("main._process_benchmark_kwargs", fail_payload_build)

    response = TestClient(app, raise_server_exceptions=False).post(
        f"/retry-or-resume-benchmark/{benchmark_id}?retry=true"
    )

    assert response.status_code == 500
    database_session.expire_all()
    persisted_benchmark = database_session.get(Benchmark, benchmark_id)
    persisted_task = database_session.get(Task, task.id)
    assert persisted_benchmark is not None
    assert persisted_task is not None
    assert persisted_benchmark.status == BenchmarkStatus.STOPPED
    assert persisted_task.status == TaskStatus.ERROR
    assert {
        dispatch.id
        for dispatch in database_session.exec(
            select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark_id)
        ).all()
    } == original_dispatch_ids
    assert payloads == []


@pytest.mark.description("Retries keep the selected agent version and explicit updates preserve earlier attempts.")
@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("protected_gateway", [False, True])
def test_exact_agent_selection_survives_alias_and_explicit_update(
    legacy: bool,
    protected_gateway: bool,
    database_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    project_service: MockProjectRoutes,
) -> None:

    _configure_managed_runtime(monkeypatch)
    _promote_test_release(database_session)
    payloads = _capture_dispatch_payloads(monkeypatch, database_session)
    archives: dict[str, bytes] = {}
    selections: list[AgentReleaseSelection] = []
    for reference in ("A1", "A2"):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr(
                "example/contract.yaml",
                f"name: example\ninstall_cmd: echo setup\nrun_cmd: echo {reference} {{problem_statement_path}}\nkwargs:\n  budget:\n    type: int\n    required: false\n    default: 1\n"
                + (
                    "secrets:\n  MODEL_GATEWAY_URL: changed-url\n  MODEL_GATEWAY_API_KEY: changed-key\n"
                    if protected_gateway and reference == "A2"
                    else ""
                ),
            )
        content = buffer.getvalue()
        selection = AgentReleaseSelection(
            reference=reference,
            archive_sha256=sha256(content).hexdigest(),
            prescribed_configuration={"budget": "2"},
            overrides={"budget": "5"},
        )
        selections.append(selection)
        archives[f"agent-releases/example/{reference}/{selection.archive_sha256}.zip"] = content
        archives[f"agent-releases/example/{reference}/release.json"] = json.dumps(
            {"schema_version": 1, "agent": "example", "archive": "example.zip", "sha256": selection.archive_sha256}
        ).encode()

    async def get_bytes(_self: object, key: str) -> bytes:
        if "/denied/" in key:
            raise S3Error("Failed to download from S3") from ClientError(
                {"Error": {"Code": "AccessDenied"}}, "GetObject"
            )
        if key not in archives:
            raise S3Error("Failed to download from S3") from ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return archives[key]

    async def objects(_self: object, _prefix: str) -> AsyncIterator[StoredObject]:
        for key, content in tuple(archives.items()):
            if key.startswith(_prefix):
                yield StoredObject(key=key, size=len(content))

    async def copy_object(_self: object, source_key: str, destination_key: str) -> StoredObjectCopy:
        archives[destination_key] = archives[source_key]
        return StoredObjectCopy(deletion_token=None)

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        archives["agents/example.zip"] = archives[f"agent-releases/example/A2/{selections[1].archive_sha256}.zip"]
        return VerifyTaskIdsResponse(task_ids=["task-1"])

    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.get_bytes", get_bytes)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.list_objects", objects)
    monkeypatch.setattr("tracker.aws.s3.S3ObjectStore.copy", copy_object)
    monkeypatch.setattr(main.SchedulerRepository, "try_task_evaluation_transaction_lock", lambda *_args: True)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    request = _start_request(AgentContractRequest(name="example", agent_release=selections[0]))
    request.admission_id = UUID("11111111-1111-4111-8111-111111111111")
    missing_request = request.model_copy(
        update={
            "contract": request.contract.model_copy(
                update={"agent_release": selections[0].model_copy(update={"reference": "missing"})}
            )
        }
    )
    missing = client.post("/start-benchmark", json=missing_request.model_dump(mode="json"))
    assert missing.status_code == 404
    assert database_session.exec(select(ExecutorDispatch)).all() == []
    denied_request = request.model_copy(
        update={
            "contract": request.contract.model_copy(
                update={"agent_release": selections[0].model_copy(update={"reference": "denied"})}
            )
        }
    )
    denied = client.post("/start-benchmark", json=denied_request.model_dump(mode="json"))
    assert denied.status_code == 403
    key = f"agent-releases/example/A1/{selections[0].archive_sha256}.zip"
    original = archives[key]
    archives[key] = b"corrupt"
    corrupted = client.post("/start-benchmark", json=request.model_dump(mode="json"))
    assert corrupted.status_code == 400
    assert database_session.exec(select(ExecutorDispatch)).all() == []
    archives[key] = original

    response = client.post("/start-benchmark", json=request.model_dump(mode="json"))
    assert response.status_code == 200, response.text
    benchmark_id = UUID(response.json()["benchmark_id"])
    repeated = client.post("/start-benchmark", json=request.model_dump(mode="json"))
    assert repeated.status_code == 409, repeated.text
    assert repeated.json()["detail"]["benchmark_id"] == str(benchmark_id)
    assert len(payloads) == 1
    if protected_gateway:
        project = Project(
            org_id=TEST_ORG_ID, handle="exact-release", name="Exact release", spend_type=SpendType.INTERNAL
        )
        database_session.add(project)
        database_session.flush()
        project_service.seed(project)
        database_session.add(RunProject(benchmark_id=benchmark_id, project_id=project.id))
        database_session.commit()
        monkeypatch.setattr(main, "selects_project_gateway_key", lambda _org: True)
    archives[f"benchmarks/{benchmark_id}/task-1/output.txt"] = b"A1 output"
    task = database_session.exec(select(Task).where(Task.benchmark == benchmark_id)).one()
    task.eval_resume_state = {"cursor": "A1-evaluation"}
    database_session.add(task)
    if legacy:
        benchmark = database_session.get(Benchmark, benchmark_id)
        assert benchmark is not None
        contract = benchmark.arguments.contract.model_copy(
            update={"agent_release": None, "kwargs": {"budget": "3", "model": "recorded-model"}}
        )
        benchmark.arguments = benchmark.arguments.model_copy(update={"contract": contract, "agent_attempts": []})
        database_session.add(benchmark)
        database_session.add(FinalEvaluation(org_id=benchmark.org_id, benchmark=benchmark_id, final_score=0.5))
        database_session.commit()
    database_session.commit()
    for replacement in (selections[1],) if legacy else (None, selections[1]):
        database_session.expire_all()
        benchmark = database_session.get(Benchmark, benchmark_id)
        assert benchmark is not None
        assert benchmark.arguments.contract.run_cmd == "echo A1 {problem_statement_path}"
        _stop_benchmark(benchmark, database_session)
        body: dict[str, Any] = {"task_ids": ["task-1"]}
        if replacement is not None:
            body["agent_release"] = replacement.model_dump(mode="json")
        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_id}",
            params={
                "retry": "true",
                "update_agent": str(replacement is not None).lower(),
                "retry_mode": "from_scratch" if replacement else "auto",
            },
            json=body,
        )
        assert response.status_code == 200, response.text
        database_session.expire_all()
        recovered_task = database_session.exec(select(Task).where(Task.benchmark == benchmark_id)).one()
        assert recovered_task.eval_resume_state == (None if replacement else {"cursor": "A1-evaluation"})

    database_session.expire_all()
    benchmark = database_session.get(Benchmark, benchmark_id)
    assert benchmark is not None
    assert benchmark.arguments.contract.run_cmd == "echo A2 {problem_statement_path}"
    if protected_gateway:
        assert "MODEL_GATEWAY_URL" not in benchmark.arguments.contract.secrets
        assert "MODEL_GATEWAY_API_KEY" not in benchmark.arguments.contract.secrets
    assert [a.agent_release["reference"] for a in benchmark.arguments.agent_attempts if a.agent_release] == (
        ["A2"] if legacy else ["A1", "A1", "A2"]
    )
    assert len(payloads) == (2 if legacy else 3)

    metadata = client.get(f"/fetch-benchmark-metadata/{benchmark_id}")
    assert metadata.status_code == 200
    attempts = metadata.json()["benchmark_arguments"]["agent_attempts"]
    if legacy:
        assert attempts[0]["agent_release"] is None
        assert attempts[0]["model"] == "recorded-model"
        assert attempts[0]["effective_configuration"] == {"budget": "3"}
        assert attempts[0]["final_evaluation"]["final_score"] == 0.5
    else:
        assert [attempt["agent_release"]["reference"] for attempt in attempts] == ["A1", "A1", "A2"]
    assert attempts[0]["retained_outputs"] is not None

    archives[f"benchmarks/{benchmark_id}/task-1/output.txt"] = b"A2 output"
    assert archives[attempts[0]["retained_outputs"] + "task-1/output.txt"] == b"A1 output"
    if not legacy:
        assert [attempt["effective_configuration"] for attempt in attempts] == [{"budget": "5"}] * 3


class _MonitoringClient:
    def __init__(self, result):
        self.result = result
        self.calls = []

    async def monitoring_requirements(self, task_ids, dataset=None):
        self.calls.append((task_ids, dataset))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


@pytest.mark.asyncio
@pytest.mark.description("Run start stores the monitor rules the benchmark declares for its tasks.")
async def test_monitoring_rules_come_from_the_benchmark():
    declared = RunMonitoringRules(policy="Range only", watcher_policy_pack="core+cyber-range", allowed_hosts=["local"])
    client = _MonitoringClient(declared)

    assert await main._monitoring_rules(client, ["task"], "ga") == declared
    assert client.calls == [(["task"], "ga")]


@pytest.mark.asyncio
@pytest.mark.description("A benchmark without the monitoring route starts with the monitor's default rules.")
async def test_benchmark_without_monitoring_route_has_no_rules():
    client = _MonitoringClient(BenchmarkServiceError("missing", status_code=404))

    assert await main._monitoring_rules(client, ["task"], None) is None


@pytest.mark.asyncio
@pytest.mark.description("A monitored run does not start when the benchmark cannot return its rules.")
async def test_monitoring_rules_failure_blocks_the_run():
    client = _MonitoringClient(BenchmarkServiceError("failed", status_code=500))

    with pytest.raises(HTTPException) as error:
        await main._monitoring_rules(client, ["task"], None)

    assert error.value.status_code == 502
