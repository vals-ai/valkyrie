"""Dataset-name discovery across the service, hosted policy, and Python client."""

import json
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from vals_benchmark_service import auth
from vals_benchmark_service.app import ValsBenchmarkServiceApp
from conftest import ValsStubBenchmark


@pytest.mark.parametrize(
    ("tenant", "grants", "trial", "expected"),
    [
        ("reader", ["default", "nonexistent"], False, ["default"]),
        ("reader", [], False, []),
        ("reader", ["alt", "default"], False, ["alt", "default"]),
        ("reader", ["default"], True, ["default"]),
        ("unknown", ["default"], False, None),
        (None, ["default"], False, None),
    ],
)
def test_hosted_discovery_obeys_authentication_and_dataset_grants(
    monkeypatch: pytest.MonkeyPatch,
    tenant: str | None,
    grants: list[str],
    trial: bool,
    expected: list[str] | None,
) -> None:
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("DESCOPE_PROJECT_ID", "test-project")
    monkeypatch.delenv("BENCHMARK_CATALOG_API_URL", raising=False)
    monkeypatch.setenv(
        "DESCOPE_TENANT_ALLOWLIST_JSON",
        json.dumps({"tenants": {"reader": {"datasets": grants, "trial_mode": trial}}}),
    )
    exchange = AsyncMock(return_value={"tenants": {tenant: {}}})
    monkeypatch.setattr(auth, "_exchange_descope_access_key", exchange)
    app = ValsBenchmarkServiceApp(ValsStubBenchmark)
    with TestClient(app) as client:
        discovery = AsyncMock(wraps=app.service.list_datasets)
        monkeypatch.setattr(app.service, "list_datasets", discovery)
        response = client.get("/v1/datasets", headers={"x-descope-api-key": "test-key"} if tenant else {})
        if expected is None:
            assert response.status_code == 401
            discovery.assert_not_called()
        else:
            assert response.status_code == 200
            assert response.json() == {"datasets": expected}
