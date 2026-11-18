from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity, get_current_starter
from tracker.database.models import Org

_EMAIL = "alice@vals.ai"

_CREATE = {
    "handle": "router-bench",
    "name": "Router Bench",
    "spend_type": "development",
    "subcategory": "new_benchmark_generation",
}

_UPDATE = {
    "handle": "router-bench",
    "name": "Customer Router",
    "spend_type": "customer",
    "subcategory": "report",
    "customer": "acme",
    "extra_tags": {"engagement": "q4"},
}


@pytest.fixture
def project_client(monkeypatch: pytest.MonkeyPatch, tracker_database: Session) -> Iterator[TestClient]:
    org = tracker_database.get(Org, TEST_ORG_ID)
    assert org is not None
    identity = RequestIdentity(org=org, email=_EMAIL)
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)

    client = TestClient(app)
    yield client
    client.close()


def _project_body(created: dict[str, Any], fields: dict[str, Any], archived_at: str | None = None) -> dict[str, Any]:
    return {
        "id": created["id"],
        "handle": fields["handle"],
        "name": fields["name"],
        "spend_type": fields["spend_type"],
        "subcategory": fields["subcategory"],
        "customer": fields.get("customer"),
        "extra_tags": fields.get("extra_tags", {}),
        "created_at": created["created_at"],
        "created_by_email": _EMAIL,
        "archived_at": archived_at,
    }


@pytest.mark.description(
    (
        "A project's route lifecycle returns today's status codes and bodies.\n"
        "\n"
        "Test cases:\n"
        "- Create and update return the full project body.\n"
        "- Archive sets `archived_at` and hides the project from `GET /projects` unless `include_archived` is set.\n"
        "- Unarchive clears `archived_at` and lists the project again.\n"
        "- Get by ID returns the project ID and org ID.\n"
        "- Delete returns the project, then get by ID is 404 and the list is empty."
    )
)
def test_project_lifecycle_replays_the_same_status_codes_and_bodies(project_client: TestClient) -> None:
    created = project_client.post("/projects", json=_CREATE)

    assert created.status_code == 200, created.text
    project: dict[str, Any] = created.json()
    assert project == _project_body(project, _CREATE)

    updated = project_client.put("/projects/router-bench", json=_UPDATE)

    assert updated.status_code == 200, updated.text
    assert updated.json() == _project_body(project, _UPDATE)

    archived = project_client.post("/projects/router-bench/archive")

    assert archived.status_code == 200, archived.text
    archived_at = archived.json()["archived_at"]
    assert isinstance(archived_at, str)
    assert archived.json() == _project_body(project, _UPDATE, archived_at)

    hidden = project_client.get("/projects")
    shown = project_client.get("/projects", params={"include_archived": True})

    assert hidden.status_code == 200, hidden.text
    assert hidden.json() == {"projects": []}
    assert shown.status_code == 200, shown.text
    assert shown.json() == {"projects": [_project_body(project, _UPDATE, archived_at)]}

    unarchived = project_client.post("/projects/router-bench/unarchive")

    assert unarchived.status_code == 200, unarchived.text
    assert unarchived.json() == _project_body(project, _UPDATE)

    listed = project_client.get("/projects")
    fetched = project_client.get(f"/projects/by-id/{project['id']}")

    assert listed.status_code == 200, listed.text
    assert listed.json() == {"projects": [_project_body(project, _UPDATE)]}
    assert fetched.status_code == 200, fetched.text
    assert fetched.json() == {"id": project["id"], "org_id": str(TEST_ORG_ID)}

    deleted = project_client.delete("/projects/router-bench")

    assert deleted.status_code == 200, deleted.text
    assert deleted.json() == _project_body(project, _UPDATE)

    gone = project_client.get(f"/projects/by-id/{project['id']}")
    emptied = project_client.get("/projects", params={"include_archived": True})

    assert gone.status_code == 404
    assert gone.json() == {"detail": f"Project '{project['id']}' not found"}
    assert emptied.status_code == 200, emptied.text
    assert emptied.json() == {"projects": []}


@pytest.mark.description("`GET /projects/{handle}` returns an archived project, and an unknown handle is 404.")
def test_get_project_returns_an_archived_project_and_404s_an_unknown_handle(project_client: TestClient) -> None:
    created = project_client.post("/projects", json=_CREATE).json()
    archived_at = project_client.post("/projects/router-bench/archive").json()["archived_at"]

    fetched = project_client.get("/projects/router-bench")
    missing = project_client.get("/projects/missing")

    assert fetched.status_code == 200, fetched.text
    assert fetched.json() == _project_body(created, _CREATE, archived_at)
    assert missing.status_code == 404
    assert missing.json() == {"detail": "Project not found"}
