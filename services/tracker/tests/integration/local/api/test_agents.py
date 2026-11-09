import pytest

from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
from pytest import MonkeyPatch, mark


@pytest.mark.description("Listing agents returns nothing when the bucket is empty.")
def test_agents_empty_when_bucket_empty(client: TestClient, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr("tracker.api.agents.list_agents", AsyncMock(return_value=[]))

    response = client.get(
        "/agents",
        headers={"X-Api-Key": "fake"},
    )

    assert response.status_code == 200
    assert response.json()["agents"] == []


@pytest.mark.description(
    (
        "The agents catalog must not be readable without authentication.\n"
        "\n"
        "Test cases:\n"
        "- A request without a bearer session receives 401."
    )
)
@mark.parametrize(("method", "path"), [("GET", "/agents"), ("PUT", "/agents/demo"), ("DELETE", "/agents/demo")])
def test_agents_unauth_401(client: TestClient, method: str, path: str) -> None:
    response = client.request(method, path)

    assert response.status_code == 401
