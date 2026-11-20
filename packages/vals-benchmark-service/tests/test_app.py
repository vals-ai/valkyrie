"""Tests for FastAPI app endpoints."""

import json
from collections.abc import AsyncGenerator, Generator
from typing import Any
from unittest.mock import patch

import pytest
from descope.exceptions import AuthException
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from vals_benchmark_service import auth as auth_module
from vals_benchmark_service.app import ValsBenchmarkServiceApp
from benchmark_service.app import BenchmarkServiceApp
from benchmark_service.context import current_tenant
from benchmark_service.schemas import (
    StreamResultChunk,
)
from conftest import StubBenchmark, ValsStubBenchmark


class TestAuthMiddleware:
    """Test that a custom resolve_tenant override rejects unauthorized requests."""

    AUTH_TOKEN = "my-secret-token"

    @pytest.fixture
    def auth_client(self, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
        monkeypatch.setenv("AUTH_REQUIRED", "true")

        class AuthBenchmark(StubBenchmark):
            async def resolve_tenant(self, headers: dict[str, str]) -> str | None:
                if headers.get("authorization") != TestAuthMiddleware.AUTH_TOKEN:
                    return None
                return "custom-tenant"

            async def check_dataset_access(self, tenant: str, dataset: str | None) -> bool:
                return True

        with TestClient(BenchmarkServiceApp(AuthBenchmark)) as c:
            yield c

    def test_no_auth_returns_401(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids")
        assert response.status_code == 401

    def test_wrong_auth_returns_401(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids", headers={"Authorization": "wrong"})
        assert response.status_code == 401

    def test_correct_auth_returns_200(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids", headers={"Authorization": self.AUTH_TOKEN})
        assert response.status_code == 200

    def test_custom_tenant_does_not_enable_provider_routes(self, auth_client: TestClient) -> None:
        response = auth_client.post(
            "/v1/evaluate",
            headers={"Authorization": self.AUTH_TOKEN},
            json={
                "run_id": "custom-run",
                "task_id": "task-1",
                "payload": {"type": "text", "schema": "text.v1", "data": "2"},
            },
        )
        assert response.status_code == 404

    def test_health_skips_auth(self, auth_client: TestClient) -> None:
        response = auth_client.get("/health")
        assert response.status_code == 200

    @pytest.mark.parametrize("path", ["/ws/setup-task", "/ws/evaluate-response", "/ws/evaluate-instance"])
    def test_websocket_auth_failure_closes_1008(self, auth_client: TestClient, path: str) -> None:
        with auth_client.websocket_connect(path) as ws:
            with pytest.raises(WebSocketDisconnect) as exc_info:
                ws.receive_json()

        assert exc_info.value.code == 1008


class TestUnconfiguredAuth:
    """A deploy without configured authentication rejects protected requests."""

    API_KEY = "test-api-key-123"

    @pytest.fixture(autouse=True)
    def unconfigured_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AUTH_REQUIRED", "true")
        monkeypatch.delenv("AUTH_DISABLED", raising=False)
        monkeypatch.delenv("DESCOPE_PROJECT_ID", raising=False)
        monkeypatch.setenv("BENCHMARK_API_KEY", self.API_KEY)

    @pytest.fixture(params=["core", "vals"])
    def client(self, request: pytest.FixtureRequest) -> Generator[TestClient, None, None]:
        app = (
            BenchmarkServiceApp(StubBenchmark)
            if request.param == "core"
            else ValsBenchmarkServiceApp(ValsStubBenchmark)
        )
        with TestClient(app) as c:
            yield c

    def test_missing_credential_returns_401(self, client: TestClient) -> None:
        assert client.get("/verify-task-ids").status_code == 401

    def test_static_bearer_key_is_not_accepted(self, client: TestClient) -> None:
        response = client.get(
            "/verify-task-ids",
            headers={"Authorization": f"Bearer {self.API_KEY}"},
        )
        assert response.status_code == 401

    def test_health_skips_auth(self, client: TestClient) -> None:
        assert client.get("/health").status_code == 200

    def test_auth_disabled_cannot_bypass_required_auth(
        self,
        monkeypatch: pytest.MonkeyPatch,
        client: TestClient,
    ) -> None:
        monkeypatch.setenv("AUTH_DISABLED", "true")
        assert client.get("/verify-task-ids").status_code == 401
        with client.websocket_connect("/ws/evaluate-response") as ws:
            with pytest.raises(WebSocketDisconnect) as exc_info:
                ws.receive_json()
        assert exc_info.value.code == 1008


@pytest.mark.parametrize("app_cls", [BenchmarkServiceApp, ValsBenchmarkServiceApp])
@pytest.mark.parametrize("auth_required", [None, "false"])
@pytest.mark.parametrize("auth_hook", ["resolve_tenant", "check_auth"])
def test_local_http_and_websocket_bypass_custom_auth(
    monkeypatch: pytest.MonkeyPatch,
    app_cls: type[BenchmarkServiceApp],
    auth_required: str | None,
    auth_hook: str,
) -> None:
    """The auth opt-in flag governs custom authentication on both transports.

    Test cases:
    - Absent or false AUTH_REQUIRED bypasses custom tenant and boolean auth hooks.
    - Both application templates serve HTTP and WebSocket evaluation locally.
    """
    if auth_required is None:
        monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    else:
        monkeypatch.setenv("AUTH_REQUIRED", auth_required)

    class TenantAuthBenchmark(ValsStubBenchmark):
        async def resolve_tenant(self, headers: dict[str, str]) -> str | None:
            raise AssertionError("custom tenant authentication was called")

    class BooleanAuthBenchmark(ValsStubBenchmark):
        async def check_auth(self, headers: dict[str, str]) -> bool:
            raise AssertionError("custom boolean authentication was called")

    service_cls = TenantAuthBenchmark if auth_hook == "resolve_tenant" else BooleanAuthBenchmark
    with TestClient(app_cls(service_cls)) as client:
        response = client.post("/evaluate-response/", json={"task_id": "task-1", "response": "2"})
        assert response.status_code == 200
        assert response.json() == {"resolved": True}
        with client.websocket_connect("/ws/evaluate-response") as ws:
            ws.send_json({"task_id": "task-1", "response": "2"})
            assert ws.receive_json() == {"type": "result", "data": {"resolved": True}}


@pytest.mark.parametrize("app_cls", [BenchmarkServiceApp, ValsBenchmarkServiceApp])
def test_websocket_evaluation_binds_the_authenticated_tenant(
    monkeypatch: pytest.MonkeyPatch, app_cls: type[BenchmarkServiceApp]
) -> None:
    """A benchmark's eval-resume store reads the caller's tenant from the request scope."""
    monkeypatch.setenv("AUTH_REQUIRED", "true")

    class TenantBenchmark(ValsStubBenchmark):
        async def resolve_tenant(self, headers: dict[str, str]) -> str | None:
            return headers.get("x-tenant")

        async def check_dataset_access(self, tenant: str, dataset: str | None) -> bool:
            return True

        async def stream_evaluate_response(  # type: ignore[override]
            self, request: Any, dataset: str | None = None
        ) -> AsyncGenerator[Any, None]:
            yield StreamResultChunk(type="result", data={"tenant": current_tenant()})

    with TestClient(app_cls(TenantBenchmark)) as client:
        with client.websocket_connect("/ws/evaluate-response", headers={"x-tenant": "tenant-a"}) as ws:
            ws.send_json({"task_id": "task-1", "eval_resume_state": {}})
            assert ws.receive_json() == {"type": "result", "data": {"tenant": "tenant-a"}}
    assert current_tenant() is None


class TestDescopeAuth:
    """Test the default Descope access-key authentication."""

    PROJECT_ID = "descope-project"

    @pytest.fixture
    def exchange_calls(self, monkeypatch: pytest.MonkeyPatch) -> Generator[list[tuple[str, str]], None, None]:
        auth_module.clear_auth_cache()
        monkeypatch.setenv("AUTH_REQUIRED", "true")
        monkeypatch.setenv("DESCOPE_PROJECT_ID", self.PROJECT_ID)
        monkeypatch.setenv(
            "DESCOPE_TENANT_ALLOWLIST_JSON",
            json.dumps({"tenants": {"tenant-a": {"datasets": ["default"]}}}),
        )
        monkeypatch.delenv("BENCHMARK_API_KEY", raising=False)

        calls: list[tuple[str, str]] = []

        async def exchange(project_id: str, access_key: str) -> dict[str, dict[str, dict[str, str]]]:
            calls.append((project_id, access_key))
            if access_key == "valid-key":
                return {"tenants": {"tenant-a": {}}}
            if access_key == "multi-tenant-key":
                return {"tenants": {"tenant-a": {}, "tenant-b": {}}}
            raise AuthException(401, "invalid token", "invalid access key")

        monkeypatch.setattr(auth_module, "_exchange_descope_access_key", exchange)

        yield calls

        auth_module.clear_auth_cache()

    @pytest.fixture
    def auth_client(self, exchange_calls: list[tuple[str, str]]) -> Generator[TestClient, None, None]:
        with TestClient(ValsBenchmarkServiceApp(ValsStubBenchmark)) as c:
            yield c

    def test_missing_descope_header_returns_401(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids")
        assert response.status_code == 401

    def test_invalid_descope_key_returns_401(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids", headers={"X-Descope-Api-Key": "invalid-key"})
        assert response.status_code == 401

    def test_multi_tenant_descope_key_returns_401(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids", headers={"X-Descope-Api-Key": "multi-tenant-key"})
        assert response.status_code == 401

    def test_valid_descope_key_returns_200(self, auth_client: TestClient) -> None:
        response = auth_client.get("/verify-task-ids", headers={"X-Descope-Api-Key": "valid-key"})
        assert response.status_code == 200

    def test_valid_descope_key_is_cached(
        self,
        auth_client: TestClient,
        exchange_calls: list[tuple[str, str]],
    ) -> None:
        headers = {"X-Descope-Api-Key": "valid-key"}

        assert auth_client.get("/verify-task-ids", headers=headers).status_code == 200
        assert auth_client.get("/retrieve-task/", params={"task_id": "task-1"}, headers=headers).status_code == 200

        assert exchange_calls == [(self.PROJECT_ID, "valid-key")]


@pytest.fixture
def auth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps(
            {
                "tenants": {
                    "acme-corp": {"datasets": ["validation"]},
                    "vals-internal": {"datasets": ["default", "alt"]},
                }
            }
        ),
    )


def _patch_descope(tenants: list[str]) -> Any:
    return patch.object(
        auth_module,
        "_exchange_descope_access_key",
        return_value={"tenants": {t: {} for t in tenants}},
    )


@pytest.fixture
def auth_client(auth_env: None) -> Generator[TestClient, None, None]:
    """A TestClient configured for Descope authentication."""
    with TestClient(ValsBenchmarkServiceApp(ValsStubBenchmark)) as c:
        yield c


def test_verify_task_ids_403_for_disallowed_dataset(auth_client: TestClient) -> None:
    with _patch_descope(["acme-corp"]):
        response = auth_client.get(
            "/verify-task-ids",
            headers={"x-descope-api-key": "key-acme"},
            params={"dataset": "alt"},
        )
    assert response.status_code == 403
    assert response.json() == {"detail": "Dataset not allowed"}


@pytest.mark.parametrize("path", ["/ws/setup-task", "/ws/evaluate-instance", "/ws/evaluate-response"])
@pytest.mark.parametrize("enabled", [False, True])
def test_request_docker_provider_requires_service_opt_in(
    auth_client: TestClient, monkeypatch: pytest.MonkeyPatch, path: str, enabled: bool
) -> None:
    """
    Verify that a request can select the Docker provider only on a service that opts in.

    Test cases:
    - Without `CBS_DOCKER_ENABLED`, each websocket endpoint closes with a policy violation before provider work.
    - With `CBS_DOCKER_ENABLED=true`, the request passes the Docker check and reaches the dataset check.
    """
    if enabled:
        monkeypatch.setenv("CBS_DOCKER_ENABLED", "true")
    else:
        monkeypatch.delenv("CBS_DOCKER_ENABLED", raising=False)
    with _patch_descope(["acme-corp"]):
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with auth_client.websocket_connect(path, headers={"x-descope-api-key": "key-acme"}) as ws:
                ws.send_json(
                    {
                        "task_id": "task-1",
                        "instance_id": "i-1",
                        "eval_resume_state": {"checkpoint": "saved"},
                        "sandbox_provider": {"type": "docker"},
                        "dataset": "alt",
                    }
                )
                ws.receive_json()
    assert exc_info.value.code == 1008
    assert exc_info.value.reason == (
        "Dataset not allowed" if enabled else "Docker sandboxes are disabled on this service"
    )


@pytest.mark.parametrize("enabled", [False, True])
def test_evaluate_response_docker_provider_requires_service_opt_in(
    auth_client: TestClient, monkeypatch: pytest.MonkeyPatch, enabled: bool
) -> None:
    """Reject an HTTP evaluate-response that selects Docker unless the service opts in with `CBS_DOCKER_ENABLED`."""
    if enabled:
        monkeypatch.setenv("CBS_DOCKER_ENABLED", "true")
    else:
        monkeypatch.delenv("CBS_DOCKER_ENABLED", raising=False)
    with _patch_descope(["acme-corp"]):
        response = auth_client.post(
            "/evaluate-response/",
            headers={"x-descope-api-key": "key-acme"},
            json={"task_id": "task-1", "response": "answer", "sandbox_provider": {"type": "docker"}, "dataset": "alt"},
        )
    assert response.status_code == 403
    assert response.json() == {
        "detail": "Dataset not allowed" if enabled else "Docker sandboxes are disabled on this service"
    }


def test_setup_task_ws_close_for_disallowed_dataset(auth_client: TestClient) -> None:
    with _patch_descope(["acme-corp"]):
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with auth_client.websocket_connect(
                "/ws/setup-task",
                headers={"x-descope-api-key": "key-acme"},
            ) as ws:
                ws.send_json(
                    {
                        "task_id": "task-1",
                        "instance_id": "i-1",
                        "sandbox_provider": {
                            "type": "modal",
                            "MODAL_TOKEN_ID": "id",
                            "MODAL_TOKEN_SECRET": "secret",
                        },
                        "dataset": "alt",
                    }
                )
                ws.receive_json()
    assert exc_info.value.code == 1008
    assert exc_info.value.reason == "Dataset not allowed"
