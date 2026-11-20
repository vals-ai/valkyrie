"""Tests for InflightMiddleware."""

import json

import pytest

from benchmark_service.inflight import InflightMiddleware
from vals_benchmark_service import auth as auth_module
from vals_benchmark_service.app import ValsBenchmarkServiceApp


@pytest.mark.parametrize(
    ("headers", "status_code"),
    [({}, 401), ({"x-descope-api-key": "trial-key"}, 403)],
)
def test_only_vals_template_installs_inflight_middleware(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    headers: dict[str, str],
    status_code: int,
) -> None:
    """Install hosted metrics after HTTP authentication and authorization.

    Test cases:
    - The core app omits hosted metrics, while the Vals app preserves its namespace.
    - Unauthenticated and trial-restricted requests emit no in-flight samples.
    """
    from fastapi.testclient import TestClient

    from benchmark_service.app import BenchmarkServiceApp
    from conftest import StubBenchmark, ValsStubBenchmark

    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "P_test")
    monkeypatch.delenv("BENCHMARK_CATALOG_API_URL", raising=False)
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"trial": {"datasets": ["default"], "trial_mode": True}}}),
    )

    async def exchange_access_key(_project_id: str, _access_key: str) -> dict[str, dict[str, dict[str, str]]]:
        return {"tenants": {"trial": {}}}

    monkeypatch.setattr(auth_module, "_exchange_descope_access_key", exchange_access_key)

    app = BenchmarkServiceApp(StubBenchmark)
    assert all(m.cls is not InflightMiddleware for m in app.user_middleware)
    vals_app = ValsBenchmarkServiceApp(ValsStubBenchmark)
    middleware = next(m for m in vals_app.user_middleware if m.cls is InflightMiddleware)
    assert middleware.kwargs["metric_namespace"] == "Vals/BenchmarkServices"

    with TestClient(vals_app) as client:
        response = client.get("/verify-task-ids", headers=headers)

    assert response.status_code == status_code
    assert '"InFlightRequests"' not in capsys.readouterr().out
