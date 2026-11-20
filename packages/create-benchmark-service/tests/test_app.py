"""Tests for FastAPI app endpoints."""

from collections.abc import AsyncGenerator, Generator, Mapping
from typing import Any, cast

import pytest
from fastapi import WebSocket
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from benchmark_service.app import BenchmarkServiceApp, send_json_if_connected
from benchmark_service.context import current_child_egress, current_sandbox_provider
from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from benchmark_service.sandbox.modal import ModalProviderConfig
from benchmark_service.sandbox.types import (
    ExecResult,
    Sandbox,
    SandboxCreateRequest,
    SandboxProvider,
    SandboxQuery,
)
from benchmark_service.schemas import (
    BenchmarkEgressPlan,
    CreditedGeneration,
    EvaluateResponseRequest,
    RetrieveTaskResponse,
    StreamResultChunk,
)
from tests.conftest import StubBenchmark


class ProviderSelectionSandbox(Sandbox):
    @property
    def id(self) -> str:
        return "selected-sandbox-id"

    @property
    def name(self) -> str:
        return "selected-sandbox-name"

    @property
    def state(self) -> str:
        return "running"

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        return ExecResult(exit_code=0, output="")

    def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        if False:
            yield ""

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        pass

    async def download_file(self, remote_path: str) -> bytes:
        return b""


class ProviderSelectionProvider(SandboxProvider):
    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        return ProviderSelectionSandbox()

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        assert instance_id == "i-1"
        return ProviderSelectionSandbox()

    async def delete_sandbox(self, instance_id: str) -> None:
        pass

    def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        if False:
            yield ProviderSelectionSandbox()


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_verify_task_ids_all(client: TestClient) -> None:
    response = client.get("/verify-task-ids")
    assert response.status_code == 200
    assert response.json() == {"task_ids": ["task-1", "task-2", "task-3"]}


@pytest.mark.parametrize(
    ("task_ids", "expected"),
    [
        (["task-1"], ["task-1"]),
        (["task-1", "task-3"], ["task-1", "task-3"]),
        (["task-1", "task-1"], ["task-1"]),  # deduplication
    ],
)
def test_verify_task_ids_filtered(client: TestClient, task_ids: list[str], expected: list[str]) -> None:
    response = client.get("/verify-task-ids", params=[("task_ids", t) for t in task_ids])
    assert response.status_code == 200
    assert response.json() == {"task_ids": expected}


@pytest.mark.parametrize(
    ("slice_str", "expected"),
    [
        ("0:1", ["task-1"]),
        ("0:2", ["task-1", "task-2"]),
        ("1:3", ["task-2", "task-3"]),
    ],
)
def test_verify_task_ids_slice(client: TestClient, slice_str: str, expected: list[str]) -> None:
    response = client.get("/verify-task-ids", params={"slice": slice_str})
    assert response.status_code == 200
    assert response.json() == {"task_ids": expected}


def test_retrieve_task(client: TestClient) -> None:
    response = client.get("/retrieve-task/", params={"task_id": "task-1"})
    assert response.status_code == 200
    data = response.json()
    assert data["problem_path"] == "/tmp/problem_statement.txt"
    assert data["source"] == {"type": "image", "image": "python:3.12-slim"}
    assert data["docker_image"] == "python:3.12-slim"
    assert data["agent_install_order"] == "before_setup"
    assert data["egress"] == {
        "setup_task": None,
        "run": None,
        "evaluation": None,
        "run_through_proxy": False,
        "sandbox_local_hosts": [],
        "run_inspect": [],
        "children": {},
    }
    assert data["credited_generation"] is None
    restored = RetrieveTaskResponse.model_validate(data)
    assert restored.egress.setup_task is None
    assert restored.egress.evaluation is None


def test_retrieve_task_credited_generation_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    class CreditedBenchmark(StubBenchmark):
        async def retrieve_task(
            self,
            task_id: str,
            skip_validation: bool = False,
            dataset: str | None = None,
        ) -> RetrieveTaskResponse:
            response = await super().retrieve_task(task_id, skip_validation, dataset)
            return response.model_copy(
                update={
                    "credited_generation": CreditedGeneration(
                        allowance_seconds=18000, stage_protocol="valkyrie-stage/1"
                    )
                }
            )

    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    with TestClient(BenchmarkServiceApp(CreditedBenchmark)) as client:
        response = client.get("/retrieve-task/", params={"task_id": "task-1"})

    assert response.status_code == 200
    assert response.json()["credited_generation"] == {
        "allowance_seconds": 18000.0,
        "stage_protocol": "valkyrie-stage/1",
    }


def test_retrieve_task_explicit_agent_install_order(monkeypatch: pytest.MonkeyPatch) -> None:
    class AfterSetupBenchmark(StubBenchmark):
        async def retrieve_task(
            self,
            task_id: str,
            skip_validation: bool = False,
            dataset: str | None = None,
        ) -> RetrieveTaskResponse:
            response = await super().retrieve_task(task_id, skip_validation, dataset)
            return response.model_copy(update={"agent_install_order": "after_setup"})

    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    with TestClient(BenchmarkServiceApp(AfterSetupBenchmark)) as client:
        response = client.get("/retrieve-task/", params={"task_id": "task-1"})

    assert response.status_code == 200
    assert response.json()["agent_install_order"] == "after_setup"


def test_retrieve_task_explicit_egress_policies(monkeypatch: pytest.MonkeyPatch) -> None:
    class RestrictedBenchmark(StubBenchmark):
        async def retrieve_task(
            self,
            task_id: str,
            skip_validation: bool = False,
            dataset: str | None = None,
        ) -> RetrieveTaskResponse:
            response = await super().retrieve_task(task_id, skip_validation, dataset)
            return response.model_copy(
                update={
                    "egress": BenchmarkEgressPlan(
                        setup_task=[],
                        run=["api.openai.com"],
                        evaluation=[],
                        run_through_proxy=True,
                        sandbox_local_hosts=["api", "db"],
                    )
                }
            )

    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    with TestClient(BenchmarkServiceApp(RestrictedBenchmark)) as client:
        response = client.get("/retrieve-task/", params={"task_id": "task-1"})

    assert response.status_code == 200
    assert response.json()["egress"] == {
        "setup_task": [],
        "run": ["api.openai.com"],
        "evaluation": [],
        "run_through_proxy": True,
        "sandbox_local_hosts": ["api", "db"],
        "run_inspect": [],
        "children": {},
    }


def test_retrieve_task_invalid(client: TestClient) -> None:
    response = client.get("/retrieve-task/", params={"task_id": "nonexistent"})
    assert response.status_code == 400


def test_retrieve_task_skip_validation(client: TestClient) -> None:
    response = client.get("/retrieve-task/", params={"task_id": "nonexistent", "skip_validation": True})
    assert response.status_code == 200


@pytest.mark.parametrize(
    ("task_id", "response_text", "expected_resolved"),
    [
        ("task-1", "2", True),
        ("task-1", "wrong", False),
        ("task-2", "4", True),
        ("task-2", "wrong", False),
    ],
)
def test_evaluate_response(
    client: TestClient,
    task_id: str,
    response_text: str,
    expected_resolved: bool,
) -> None:
    response = client.post("/evaluate-response/", json={"task_id": task_id, "response": response_text})
    assert response.status_code == 200
    assert response.json()["resolved"] is expected_resolved


def test_evaluate_response_invalid_task(monkeypatch: pytest.MonkeyPatch) -> None:
    # raise_server_exceptions=False so we observe the handler's 500 response rather than
    # ServerErrorMiddleware re-raising the underlying error into the test.
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    with TestClient(BenchmarkServiceApp(StubBenchmark), raise_server_exceptions=False) as c:
        response = c.post("/evaluate-response/", json={"task_id": "nonexistent", "response": "2"})
    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}


@pytest.mark.parametrize(
    ("evaluation_results", "expected_score"),
    [
        ({"task-1": {"resolved": True}, "task-2": {"resolved": True}}, 100.0),
        ({"task-1": {"resolved": True}, "task-2": {"resolved": False}}, 50.0),
        ({"task-1": {"resolved": False}, "task-2": {"resolved": False}}, 0.0),
    ],
)
def test_final_score(
    client: TestClient,
    evaluation_results: dict[str, dict[str, bool]],
    expected_score: float,
) -> None:
    response = client.post("/final-score/", json={"evaluation_results": evaluation_results})
    assert response.status_code == 200
    data = response.json()
    assert data["final_score"] == expected_score
    assert set(data["tasks_evaluated"]) == set(evaluation_results.keys())


def test_final_score_invalid_task(client: TestClient) -> None:
    response = client.post("/final-score/", json={"evaluation_results": {"nonexistent": {"resolved": True}}})
    assert response.status_code == 400


def test_verify_task_ids_with_dataset(client: TestClient) -> None:
    response = client.get("/verify-task-ids", params={"dataset": "alt"})
    assert response.status_code == 200
    assert response.json() == {"task_ids": ["alt-task-1", "alt-task-2"]}


def test_verify_task_ids_invalid_dataset(client: TestClient) -> None:
    response = client.get("/verify-task-ids", params={"dataset": "nonexistent"})
    assert response.status_code == 400


def test_retrieve_task_with_dataset(client: TestClient) -> None:
    response = client.get("/retrieve-task/", params={"task_id": "alt-task-1", "dataset": "alt"})
    assert response.status_code == 200
    assert response.json()["problem_path"] == "/tmp/problem_statement.txt"


def test_evaluate_response_with_dataset(client: TestClient) -> None:
    response = client.post("/evaluate-response/", json={"task_id": "alt-task-1", "response": "10", "dataset": "alt"})
    assert response.status_code == 200
    assert response.json()["resolved"] is True


def test_final_score_with_dataset(client: TestClient) -> None:
    response = client.post(
        "/final-score/",
        json={
            "evaluation_results": {"alt-task-1": {"resolved": True}},
            "dataset": "alt",
        },
    )
    assert response.status_code == 200
    assert response.json()["final_score"] == 100.0


def test_websocket_setup_task_missing_headers(client: TestClient) -> None:
    with client.websocket_connect("/ws/setup-task") as ws:
        ws.close()


def test_websocket_evaluate_instance_missing_headers(client: TestClient) -> None:
    with client.websocket_connect("/ws/evaluate-instance") as ws:
        ws.close()


def test_websocket_evaluate_response_with_eval_resume_state(client: TestClient) -> None:
    with client.websocket_connect("/ws/evaluate-response") as ws:
        ws.send_json({"task_id": "task-1", "eval_resume_state": {"artifact_prefix": "s3://bucket/run"}})
        assert ws.receive_json() == {
            "type": "result",
            "data": {
                "task_id": "task-1",
                "state": {"artifact_prefix": "s3://bucket/run"},
            },
        }


@pytest.mark.parametrize("stream", [False, True])
def test_resume_keeps_caller_owned_sandbox_outside_saved_state(monkeypatch: pytest.MonkeyPatch, stream: bool) -> None:
    class BorrowedSandboxBenchmark(StubBenchmark):
        async def evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None) -> Any:
            return {
                "instance_id": getattr(request, "instance_id", None),
                "state": request.eval_resume_state,
                "dataset": dataset,
            }

    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    state = {"checkpoint": "immutable-artifact"}
    body = {"task_id": "task-1", "eval_resume_state": state, "instance_id": "owned-grader", "dataset": "default"}
    with TestClient(BenchmarkServiceApp(BorrowedSandboxBenchmark)) as client:
        if stream:
            with client.websocket_connect("/ws/evaluate-response") as websocket:
                websocket.send_json(body)
                result = websocket.receive_json()
                assert result["type"] == "result"
                observed = result["data"]
        else:
            response = client.post("/evaluate-response/", json=body)
            assert response.status_code == 200
            observed = response.json()

    assert observed == {"instance_id": "owned-grader", "state": state, "dataset": "default"}


def test_resume_sandbox_metadata_reuses_the_existing_resource_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    declaration = {"source": None, "resources": None, "timeout_s": 30, "network_block_all": True}

    class FreshResumeBenchmark(StubBenchmark):
        async def retrieve_task(
            self, task_id: str, skip_validation: bool = False, dataset: str | None = None
        ) -> RetrieveTaskResponse:
            response = await super().retrieve_task(task_id, skip_validation, dataset)
            return RetrieveTaskResponse.model_validate({**response.model_dump(), "resume_sandbox": declaration})

    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    with TestClient(BenchmarkServiceApp(FreshResumeBenchmark)) as client:
        response = client.get("/retrieve-task/", params={"task_id": "task-1"})
        assert response.status_code == 200
        assert response.json().get("resume_sandbox") == declaration


def test_text_evaluation_cannot_borrow_a_resume_sandbox(client: TestClient) -> None:
    response = client.post(
        "/evaluate-response/", json={"task_id": "task-1", "response": "answer", "instance_id": "owned-grader"}
    )
    assert response.status_code == 422


def test_websocket_setup_task_resolves_sandbox_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Setup-task websockets should resolve request-scoped sandbox provider config.

    Test cases:
    - A provider config object in the request body creates that provider directly.
    """

    def create_provider(_config: ModalProviderConfig | DaytonaProviderConfig) -> SandboxProvider:
        return ProviderSelectionProvider()

    class RuntimeProviderBenchmark(StubBenchmark):
        async def setup_task(self, task_id: str, sandbox: Sandbox, dataset: str | None = None):
            yield StreamResultChunk(type="result", data={"task_id": task_id, "sandbox_name": sandbox.name})

    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", create_provider)
    monkeypatch.setattr(ModalProviderConfig, "create_provider", create_provider)
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)

    with TestClient(BenchmarkServiceApp(RuntimeProviderBenchmark)) as c:
        with c.websocket_connect("/ws/setup-task") as ws:
            ws.send_json(
                {
                    "task_id": "task-1",
                    "instance_id": "i-1",
                    "sandbox_provider": {
                        "type": "modal",
                        "MODAL_TOKEN_ID": "id",
                        "MODAL_TOKEN_SECRET": "secret",
                    },
                }
            )
            assert ws.receive_json() == {
                "type": "result",
                "data": {"task_id": "task-1", "sandbox_name": "selected-sandbox-name"},
            }


@pytest.mark.parametrize(
    "sandbox_provider", [None, {"type": "modal", "MODAL_TOKEN_ID": "id", "MODAL_TOKEN_SECRET": "secret"}]
)
def test_websocket_evaluate_response_binds_the_requests_sandbox_provider(
    monkeypatch: pytest.MonkeyPatch, sandbox_provider: dict[str, str] | None
) -> None:
    """A retry sees the request's sandbox provider in the request scope, as evaluate-instance does."""
    created = ProviderSelectionProvider()

    class ProviderReadingBenchmark(StubBenchmark):
        async def stream_evaluate_response(self, request: EvaluateResponseRequest, dataset: str | None = None):
            yield StreamResultChunk(type="result", data={"bound": current_sandbox_provider() is created})

    def create_provider(_config: ModalProviderConfig) -> SandboxProvider:
        return created

    monkeypatch.setattr(ModalProviderConfig, "create_provider", create_provider)
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    body: dict[str, Any] = {"task_id": "task-1", "eval_resume_state": {"s3_key": "k"}}
    if sandbox_provider is not None:
        body["sandbox_provider"] = sandbox_provider

    with TestClient(BenchmarkServiceApp(ProviderReadingBenchmark)) as c:
        with c.websocket_connect("/ws/evaluate-response") as ws:
            ws.send_json(body)
            assert ws.receive_json() == {"type": "result", "data": {"bound": sandbox_provider is not None}}


def test_websocket_setup_task_falls_back_to_header_provider_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Setup-task websockets should support clients that omit sandbox_provider.

    Test cases:
    - Daytona headers create the provider when the request body has no provider config.
    """

    def create_provider(_config: DaytonaProviderConfig) -> SandboxProvider:
        return ProviderSelectionProvider()

    class RuntimeProviderBenchmark(StubBenchmark):
        async def setup_task(self, task_id: str, sandbox: Sandbox, dataset: str | None = None):
            yield StreamResultChunk(type="result", data={"task_id": task_id, "sandbox_name": sandbox.name})

    monkeypatch.setattr(DaytonaProviderConfig, "create_provider", create_provider)
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)

    with TestClient(BenchmarkServiceApp(RuntimeProviderBenchmark)) as c:
        with c.websocket_connect(
            "/ws/setup-task",
            headers={
                "DAYTONA_API_KEY": "key",
                "DAYTONA_API_URL": "url",
                "DAYTONA_TARGET": "target",
            },
        ) as ws:
            ws.send_json({"task_id": "task-1", "instance_id": "i-1"})
            assert ws.receive_json() == {
                "type": "result",
                "data": {"task_id": "task-1", "sandbox_name": "selected-sandbox-name"},
            }


async def test_send_json_if_connected_handles_disconnect() -> None:
    class ClosedWebSocket:
        async def send_json(self, _payload: dict[str, object]) -> None:
            raise WebSocketDisconnect(1001)

    websocket = cast(WebSocket, ClosedWebSocket())

    assert await send_json_if_connected(websocket, {"type": "message"}) is False


class TestAuthNotRequired:
    """Both templates allow local requests unless AUTH_REQUIRED is enabled."""

    @pytest.fixture
    def client(self, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
        monkeypatch.delenv("AUTH_REQUIRED", raising=False)
        monkeypatch.delenv("DESCOPE_PROJECT_ID", raising=False)

        with TestClient(BenchmarkServiceApp(StubBenchmark)) as c:
            yield c

    def test_allows_unauthenticated_requests(self, client: TestClient) -> None:
        assert client.get("/verify-task-ids").status_code == 200


def test_core_app_ignores_hosted_policy_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DESCOPE_TENANT_ALLOWLIST_JSON", "malformed hosted policy")
    monkeypatch.setenv("BENCHMARK_CATALOG_API_URL", "https://catalog.invalid")
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.setenv("GRADING_MAX_CONCURRENCY", "0")
    monkeypatch.setenv("GRADING_SANDBOX_PROVIDER", "unknown-provider")
    monkeypatch.setenv("SUBMISSION_ARTIFACT_BUCKET", "unused-hosted-bucket")
    monkeypatch.setenv("SUBMISSION_ARTIFACT_MAX_DOWNLOAD_BYTES", "invalid")
    monkeypatch.delenv("AWS_REGION", raising=False)

    with TestClient(BenchmarkServiceApp(StubBenchmark)) as client:
        assert client.get("/verify-task-ids").status_code == 200
        response = client.post("/v1/score", json={"run_id": "local-run", "evaluation_results": {}})
        assert response.status_code == 404


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("POST", "/v1/evaluate"),
        ("POST", "/v1/score"),
        ("POST", "/v1/submissions/upload-url"),
        ("GET", "/v1/datasets/default/tasks"),
    ],
)
def test_core_app_does_not_expose_provider_routes(client: TestClient, method: str, path: str) -> None:
    """Keep every provider-facing endpoint out of the standard application.

    Test cases:
    - Local-development authentication does not make a provider route available.
    - OpenAPI omits all provider routes.
    """
    assert client.request(method, path).status_code == 404
    provider_paths = {"/v1/evaluate", "/v1/score", "/v1/submissions/upload-url", "/v1/datasets/{dataset}/tasks"}
    assert provider_paths.isdisjoint(client.get("/openapi.json").json()["paths"])


async def test_core_dataset_access_does_not_depend_on_hosted_allowlist() -> None:
    service = await StubBenchmark.create()
    assert await service.check_dataset_access("custom-tenant", "default") is True


@pytest.mark.parametrize("endpoint", ["setup-task", "evaluate-instance"])
def test_websocket_binds_the_requests_child_egress(monkeypatch: pytest.MonkeyPatch, endpoint: str) -> None:
    """Setup and grading see the request's child egress, and nothing outside the request."""
    child_egress = {
        "proxy_url": "https://key:token@proxy.example:3128",
        "proxy_cidrs": ["192.0.2.1/32"],
        "ca_certificate": "certificate",
    }

    class ChildEgressBenchmark(StubBenchmark):
        async def setup_task(self, task_id: str, sandbox: Sandbox, dataset: str | None = None):
            yield StreamResultChunk(type="result", data={"proxy_url": current_child_egress("gpu").proxy_url})

        async def evaluate_instance(self, task_id: str, sandbox: Sandbox, dataset: str | None = None):
            yield StreamResultChunk(type="result", data={"proxy_url": current_child_egress("gpu").proxy_url})

    def create_provider(_config: ModalProviderConfig) -> SandboxProvider:
        return ProviderSelectionProvider()

    monkeypatch.setattr(ModalProviderConfig, "create_provider", create_provider)
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)

    with TestClient(BenchmarkServiceApp(ChildEgressBenchmark)) as c:
        with c.websocket_connect(f"/ws/{endpoint}") as ws:
            ws.send_json(
                {
                    "task_id": "task-1",
                    "instance_id": "i-1",
                    "sandbox_provider": {"type": "modal", "MODAL_TOKEN_ID": "id", "MODAL_TOKEN_SECRET": "secret"},
                    "child_egress": {"gpu": child_egress},
                }
            )
            assert ws.receive_json() == {"type": "result", "data": {"proxy_url": child_egress["proxy_url"]}}

    with pytest.raises(LookupError):
        current_child_egress("gpu")
