from collections.abc import Iterator
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from projects.database import engine
from projects.main import app
from projects.models import Base
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

ORG = UUID("00000000-0000-0000-0000-0000000000a1")
TOKEN = {"Authorization": "Bearer project-token"}
ROUTER_BENCH = {
    "handle": "router-bench",
    "name": "Router Bench",
    "spend_type": "customer",
    "subcategory": "new_benchmark_generation",
    "customer": "acme",
    "extra_tags": {"engagement": "q4"},
    "gateway_request_id": "00000000-0000-0000-0000-0000000000b1",
    "gateway_key_id": "00000000-0000-0000-0000-0000000000b2",
    "created_at": "2026-11-01T12:00:00Z",
    "created_by_email": "owner@vals.ai",
    "archived_at": None,
}


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("PROJECT_SERVICE_TOKEN", "project-token")
    bound = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(bound)
    app.dependency_overrides[engine] = lambda: bound
    app.state.bound = bound
    yield TestClient(app)
    app.dependency_overrides.clear()
