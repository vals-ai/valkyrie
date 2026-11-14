from collections.abc import Iterator
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.pool import StaticPool

from projects.database import engine
from projects.main import app
from projects.models import Base, Project
from sqlalchemy.orm import Session

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


def _create(client: TestClient, row: dict[str, object], org: UUID = ORG) -> None:
    assert client.post(f"/orgs/{org}/projects", json=row, headers=TOKEN).status_code == 201


def _row(**changes: object) -> dict[str, object]:
    return {"id": str(uuid4())} | ROUTER_BENCH | changes


def _handles(client: TestClient) -> list[tuple[str, str]]:
    listed = client.get(f"/orgs/{ORG}/projects", params={"include_archived": True}, headers=TOKEN).json()
    return [(project["handle"], project["name"]) for project in listed["projects"]]


def _project(client: TestClient, project_id: object, org: UUID = ORG) -> dict[str, object] | None:
    response = client.get(f"/orgs/{org}/projects/{project_id}", headers=TOKEN)

    return None if response.status_code == 404 else response.json()


def _stored(row: dict[str, object], **changes: object) -> dict[str, object]:
    return row | {"created_at": "2026-11-01T12:00:00+00:00"} | changes


_CHANGES: dict[str, object] = {
    "name": "Router Q4",
    "spend_type": "internal",
    "subcategory": None,
    "customer": None,
    "extra_tags": {},
}


class TestListProjects:
    @pytest.mark.description("A created project lists in Tracker's `GET /projects` body and keeps both Gateway IDs.")
    def test_create_lists_as_tracker_body(self, client: TestClient) -> None:
        row = _row()

        _create(client, row)

        listed = client.get(f"/orgs/{ORG}/projects", headers=TOKEN).json()
        assert listed == {
            "projects": [
                {
                    "id": row["id"],
                    "handle": "router-bench",
                    "name": "Router Bench",
                    "spend_type": "customer",
                    "subcategory": "new_benchmark_generation",
                    "customer": "acme",
                    "extra_tags": {"engagement": "q4"},
                    "created_at": "2026-11-01T12:00:00+00:00",
                    "created_by_email": "owner@vals.ai",
                    "archived_at": None,
                }
            ]
        }
        with Session(app.state.bound) as session:
            saved = session.get_one(Project, UUID(str(row["id"])))
            gateway_ids = {
                "gateway_request_id": str(saved.gateway_request_id),
                "gateway_key_id": str(saved.gateway_key_id),
            }
        assert gateway_ids.items() <= ROUTER_BENCH.items()

    @pytest.mark.description("Archived projects are listed only when `include_archived` is true.")
    def test_lists_archived_only_on_request(self, client: TestClient) -> None:
        _create(client, _row(archived_at="2026-11-02T12:00:00Z"))

        hidden = client.get(f"/orgs/{ORG}/projects", headers=TOKEN).json()
        shown = client.get(f"/orgs/{ORG}/projects", params={"include_archived": True}, headers=TOKEN).json()

        assert hidden == {"projects": []}
        assert [project["archived_at"] for project in shown["projects"]] == ["2026-11-02T12:00:00+00:00"]

    @pytest.mark.description("Project routes return 401 without the project service token.")
    def test_rejects_missing_token(self, client: TestClient) -> None:
        response = client.get(f"/orgs/{ORG}/projects")

        assert response.status_code == 401


class TestProjectRoutes:
    @pytest.mark.description("A created project returns 201 with both Gateway IDs and shows in the org's list.")
    def test_create_returns_the_full_row(self, client: TestClient) -> None:
        row = _row()

        response = client.post(f"/orgs/{ORG}/projects", json=row, headers=TOKEN)

        assert response.status_code == 201
        assert response.json() == _stored(row)
        assert _handles(client) == [("router-bench", "Router Bench")]

    @pytest.mark.description(
        "A create gets 409 when its handle, trimmed lower-cased name or ID is taken, the ID in any org."
    )
    def test_create_refuses_taken_handle_name_or_id(self, client: TestClient) -> None:
        row = _row()
        assert client.post(f"/orgs/{ORG}/projects", json=row, headers=TOKEN).status_code == 201

        for taken in (_row(name="Other"), _row(handle="other", name=" router bench ")):
            assert client.post(f"/orgs/{ORG}/projects", json=taken, headers=TOKEN).status_code == 409
        assert client.post(f"/orgs/{uuid4()}/projects", json=row, headers=TOKEN).status_code == 409

    @pytest.mark.description(
        "A save stores the metadata Tracker's `PUT /projects/{handle}` changes and leaves every other field as it was."
    )
    def test_save_changes_only_tracker_metadata(self, client: TestClient) -> None:
        row = _row()
        _create(client, row)

        response = client.put(f"/orgs/{ORG}/projects/{row['id']}", json=_CHANGES, headers=TOKEN)

        assert response.status_code == 200
        assert _project(client, row["id"]) == _stored(row, **_CHANGES)

    @pytest.mark.description(
        "A refused save leaves the org's projects as they were.\n\n"
        "Test cases:\n"
        "- An unknown project ID gets 404.\n"
        "- A name another project has after trimming and lower-casing gets 409.\n"
        "- A body without a name gets 422."
    )
    def test_save_refusals(self, client: TestClient) -> None:
        one, two = _row(handle="one", name="One"), _row(handle="two", name="Two")
        _create(client, one)
        _create(client, two)

        assert client.put(f"/orgs/{ORG}/projects/{uuid4()}", json=_CHANGES, headers=TOKEN).status_code == 404
        taken = _CHANGES | {"name": " two "}
        assert client.put(f"/orgs/{ORG}/projects/{one['id']}", json=taken, headers=TOKEN).status_code == 409
        nameless = {"spend_type": "internal"}
        assert client.put(f"/orgs/{ORG}/projects/{one['id']}", json=nameless, headers=TOKEN).status_code == 422
        assert _handles(client) == [("one", "One"), ("two", "Two")]

    @pytest.mark.description(
        "Archiving keeps the first archive time, and unarchiving clears it.\n\n"
        "Test cases:\n"
        "- A second archive returns the first archive time.\n"
        "- An unarchive returns the project with no archive time.\n"
        "- An unknown project ID gets 404."
    )
    def test_archive_and_unarchive(self, client: TestClient) -> None:
        row = _row()
        _create(client, row)
        path = f"/orgs/{ORG}/projects/{row['id']}"

        for archived_at in ("2026-11-02T12:00:00Z", "2026-11-03T12:00:00Z"):
            response = client.post(f"{path}/archive", json={"archived_at": archived_at}, headers=TOKEN)
            assert response.json()["archived_at"] == "2026-11-02T12:00:00+00:00"

        assert client.post(f"{path}/unarchive", headers=TOKEN).json()["archived_at"] is None
        assert client.post(f"/orgs/{ORG}/projects/{uuid4()}/unarchive", headers=TOKEN).status_code == 404

    @pytest.mark.parametrize(
        ("method", "suffix", "body"),
        [
            ("GET", "", None),
            ("PUT", "", _CHANGES),
            ("POST", "/archive", {"archived_at": "2026-11-02T12:00:00Z"}),
            ("POST", "/unarchive", None),
            ("POST", "/gateway-key-id", {"gateway_key_id": "00000000-0000-0000-0000-0000000000c1"}),
            ("POST", "/gateway-request", {"gateway_request_id": "00000000-0000-0000-0000-0000000000c2"}),
            ("POST", "/delete", None),
        ],
    )
    @pytest.mark.description(
        "A per-project route called under another org's path gets 404 and leaves the project as it was."
    )
    def test_other_org_gets_404(
        self, client: TestClient, method: str, suffix: str, body: dict[str, object] | None
    ) -> None:
        row = _row(gateway_key_id=None)
        _create(client, row)

        response = client.request(method, f"/orgs/{uuid4()}/projects/{row['id']}{suffix}", json=body, headers=TOKEN)

        assert response.status_code == 404
        assert _project(client, row["id"]) == _stored(row)

    @pytest.mark.parametrize(
        ("method", "suffix"),
        [
            ("POST", ""),
            ("GET", "/{id}"),
            ("PUT", "/{id}"),
            ("POST", "/{id}/archive"),
            ("POST", "/{id}/unarchive"),
            ("POST", "/{id}/gateway-key-id"),
            ("POST", "/{id}/gateway-request"),
        ],
    )
    @pytest.mark.description("A per-project route returns 401 without the project service token.")
    def test_routes_reject_missing_token(self, client: TestClient, method: str, suffix: str) -> None:
        response = client.request(method, f"/orgs/{ORG}/projects" + suffix.format(id=uuid4()), json={})

        assert response.status_code == 401

    @pytest.mark.parametrize(
        ("suffix", "field"), [("gateway-key-id", "gateway_key_id"), ("gateway-request", "gateway_request_id")]
    )
    @pytest.mark.description(
        "The first Gateway key or request ID set on a project stays, and a later one gets the first back."
    )
    def test_first_gateway_id_wins(self, client: TestClient, suffix: str, field: str) -> None:
        row = _row(**{field: None})
        _create(client, row)
        first, second = str(uuid4()), str(uuid4())

        for value in (first, second):
            response = client.post(f"/orgs/{ORG}/projects/{row['id']}/{suffix}", json={field: value}, headers=TOKEN)
            assert response.status_code == 200
            assert response.json()[field] == first
        assert _project(client, row["id"]) == _stored(row, **{field: first})


class TestDeletedProjects:
    @pytest.mark.description(
        "A deleted project is hidden from every route, and its handle and name can be used again.\n\n"
        "Test cases:\n"
        "- A delete needs the token, returns 204, and a second delete gets 404.\n"
        "- Read, list, save, archive, unarchive and the key route no longer see it.\n"
        "- A list with `include_deleted` still returns it.\n"
        "- A new project with the same handle and name is created."
    )
    def test_delete_hides_the_project(self, client: TestClient) -> None:
        row = _row(gateway_key_id=None)
        path = f"/orgs/{ORG}/projects/{row['id']}"
        assert client.post(f"/orgs/{ORG}/projects", json=row, headers=TOKEN).status_code == 201

        assert client.post(f"{path}/delete").status_code == 401
        assert client.post(f"{path}/delete", headers=TOKEN).status_code == 204
        assert client.post(f"{path}/delete", headers=TOKEN).status_code == 404

        assert _project(client, row["id"]) is None
        assert _handles(client) == []

        everything = client.get(f"/orgs/{ORG}/projects?include_archived=true&include_deleted=true", headers=TOKEN)
        assert [project["id"] for project in everything.json()["projects"]] == [row["id"]]
        assert client.put(path, json=_CHANGES, headers=TOKEN).status_code == 404
        assert (
            client.post(f"{path}/archive", json={"archived_at": "2026-11-02T12:00:00Z"}, headers=TOKEN).status_code
            == 404
        )
        assert client.post(f"{path}/unarchive", headers=TOKEN).status_code == 404
        key = {"gateway_key_id": "00000000-0000-0000-0000-0000000000c1"}
        assert client.post(f"{path}/gateway-key-id", json=key, headers=TOKEN).status_code == 404

        request = {"gateway_request_id": "00000000-0000-0000-0000-0000000000c2"}
        assert client.post(f"{path}/gateway-request", json=request, headers=TOKEN).status_code == 404
        with Session(app.state.bound) as session:
            stored = select(Project.__table__.c["deleted_at"], Project.archived_at, Project.gateway_key_id)
            kept = session.execute(stored.where(Project.id == UUID(str(row["id"])))).one()
        assert kept[0] is not None
        assert kept[1:] == (None, None)
        assert client.post(f"/orgs/{ORG}/projects", json=_row(), headers=TOKEN).status_code == 201
        assert _handles(client) == [("router-bench", "Router Bench")]
