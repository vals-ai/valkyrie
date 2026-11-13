import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlmodel import Session, select

from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.api import projects as projects_api
from tracker.auth import RequestIdentity
from tracker.clients.projects import ProjectServiceClient
from tracker.database.models import Org, Project, RunProject
from tracker.domain import SpendType
from tracker.exceptions import ProjectHandleTakenError, ProjectNameTakenError
from tracker.repositories import project as project_repository
from tracker.repositories.project import ProjectRepository
from tracker.types import NewProjectRequest


_WRITES_PENDING = pytest.mark.xfail(reason="Tracker still writes its project table", strict=True)


class MockProjectRoutes:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    def _taken(self, row: dict[str, Any], field: str) -> bool:
        value = row[field].strip().lower()
        live = [other for other in self.rows.values() if other.get("deleted_at") is None]
        return any(other[field].strip().lower() == value for other in live if other["id"] != row["id"])

    def handle(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.split("/")
        assert parts[2] == str(TEST_ORG_ID)
        body: dict[str, Any] = json.loads(request.content) if request.content else {}
        if len(parts) == 4 and request.method == "GET":
            archived = request.url.params["include_archived"] == "true"
            deleted = request.url.params.get("include_deleted") == "true"
            listed = [
                row
                for row in self.rows.values()
                if (archived or row["archived_at"] is None) and (deleted or row.get("deleted_at") is None)
            ]
            return httpx.Response(200, json={"projects": sorted(listed, key=lambda row: row["handle"])})
        if len(parts) == 4 and request.method != "POST":
            return httpx.Response(405, request=request)
        if len(parts) == 4:
            if self._taken(body, "handle") or self._taken(body, "name"):
                return httpx.Response(409, request=request)
            self.rows[body["id"]] = body | {"archived_at": None}
            return httpx.Response(201, json=self.rows[body["id"]])
        row = self.rows.get(parts[4])
        if row is None or row.get("deleted_at") is not None:
            return httpx.Response(404, request=request)
        if parts[-1] == "delete":
            row["deleted_at"] = "2026-11-03T12:00:00Z"
            return httpx.Response(204)
        if request.method == "PUT":
            if self._taken(row | body, "name"):
                return httpx.Response(409, request=request)
            row.update(body)
        elif parts[-1] == "archive":
            row["archived_at"] = row["archived_at"] or body["archived_at"]
        elif parts[-1] == "unarchive":
            row["archived_at"] = None
        elif parts[-1] in {"gateway-key-id", "gateway-request"}:
            (field,) = body
            row[field] = row[field] or body[field]
        return httpx.Response(200, json=row)


@pytest.fixture
def routes(monkeypatch: pytest.MonkeyPatch) -> MockProjectRoutes:
    service = MockProjectRoutes()
    client = ProjectServiceClient(
        httpx.Client(base_url="http://projects", transport=httpx.MockTransport(service.handle))
    )
    monkeypatch.setattr(project_repository, "project_service_reader", lambda: client)
    monkeypatch.setattr(project_repository, "project_service_writer", lambda: client)
    return service


def _project(handle: str, name: str | None = None) -> Project:
    return Project(org_id=TEST_ORG_ID, handle=handle, name=name or handle.title(), spend_type=SpendType.DEVELOPMENT)


def _tracker_rows(tracker_database: Session) -> list[Project]:
    return list(tracker_database.exec(select(Project)).all())


class TestProjectServiceWrites:
    @_WRITES_PENDING
    @pytest.mark.description(
        "With both project service switches on, create, rename, archive, unarchive and delete change only the service."
    )
    def test_writes_skip_the_tracker_table(self, tracker_database: Session, routes: MockProjectRoutes) -> None:
        repository = ProjectRepository(tracker_database)

        router = repository.create(_project("router-bench"))
        default = repository.get_or_create_default(TEST_ORG_ID, "alice@vals.ai")
        assert repository.get_or_create_default(TEST_ORG_ID, "bob@vals.ai").id == default.id
        router.name = "Router Bench Q4"
        _ = repository.update(router)
        routes.rows[str(router.id)]["name"] = "Renamed elsewhere"
        router.archived_at = datetime(2026, 11, 2, tzinfo=UTC)
        _ = repository.save(router)
        assert routes.rows[str(router.id)]["name"] == "Renamed elsewhere"
        assert routes.rows[str(router.id)]["archived_at"] is not None
        router.archived_at = None
        _ = repository.save(router)
        assert routes.rows[str(router.id)]["archived_at"] is None
        assert repository.delete(router) is True

        assert [row["handle"] for row in routes.rows.values() if row.get("deleted_at") is None] == ["default"]
        assert routes.rows[str(router.id)]["deleted_at"] is not None
        assert repository.create(_project("router-bench", "Renamed elsewhere")).id != router.id
        assert _tracker_rows(tracker_database) == []

    @_WRITES_PENDING
    @pytest.mark.description("A create that reuses a handle or a name is rejected with the matching error.")
    def test_taken_handle_and_name(self, tracker_database: Session, routes: MockProjectRoutes) -> None:
        repository = ProjectRepository(tracker_database)
        _ = repository.create(_project("router-bench"))

        with pytest.raises(ProjectHandleTakenError):
            _ = repository.create(_project("router-bench", "Other"))
        with pytest.raises(ProjectNameTakenError):
            _ = repository.create(_project("other-bench", " router-BENCH "))
        assert len(routes.rows) == 1
        assert _tracker_rows(tracker_database) == []

    @_WRITES_PENDING
    @pytest.mark.description("A delete leaves a project with a linked run in the service.")
    def test_delete_keeps_a_linked_project(self, tracker_database: Session, routes: MockProjectRoutes) -> None:
        repository = ProjectRepository(tracker_database)
        project = repository.create(_project("router-bench"))
        tracker_database.add(
            RunProject(benchmark_id=make_benchmark(session=tracker_database).id, project_id=project.id)
        )
        tracker_database.commit()

        assert repository.delete(project) is False

        assert routes.rows[str(project.id)].get("deleted_at") is None

    @_WRITES_PENDING
    @pytest.mark.description(
        "A rename the service refuses because the name was just taken raises the name-taken error."
    )
    def test_rename_race_is_name_taken(
        self, tracker_database: Session, routes: MockProjectRoutes, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        repository = ProjectRepository(tracker_database)
        router = repository.create(_project("router-bench"))
        other = repository.create(_project("other-bench"))
        holders = iter([None, other])

        def get_by_name(*_: object) -> Project | None:
            return next(holders)

        monkeypatch.setattr(repository, "get_by_name", get_by_name)
        router.name = other.name

        with pytest.raises(ProjectNameTakenError):
            _ = repository.update(router)
        assert routes.rows[str(router.id)]["name"] == "Router-Bench"

    @_WRITES_PENDING
    @pytest.mark.description(
        "A project created with a Gateway key request keeps its Gateway request ID in the service."
    )
    def test_create_keeps_the_gateway_request_id(
        self, tracker_database: Session, routes: MockProjectRoutes, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        identity = RequestIdentity(org=tracker_database.get_one(Org, TEST_ORG_ID), email="alice@vals.ai")
        request_id = uuid4()

        def request_key(*_: object) -> UUID:
            return request_id

        def requests_key(_: Org) -> bool:
            return True

        monkeypatch.setattr(projects_api, "requests_project_gateway_key", requests_key)
        monkeypatch.setattr(projects_api, "request_project_gateway_key", request_key)
        request = NewProjectRequest.model_validate(
            {"handle": "blog", "name": "Blog", "spend_type": "internal", "subcategory": "blog"}
            | {"budget": "300", "budget_reason": "3 runs"}
        )

        created = projects_api.create_project(request, ProjectRepository(tracker_database), identity)

        assert routes.rows[str(created.id)]["gateway_request_id"] == str(request_id)
        assert _tracker_rows(tracker_database) == []
