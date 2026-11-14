from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlmodel import Session, select

from tests.factories import make_benchmark
from tests.project_service import MockProjectRoutes
from tests.utils import TEST_ORG_ID
from tracker.api import projects as projects_api
from tracker.auth import RequestIdentity
from tracker.database.models import Org, Project, RunProject
from tracker.domain import SpendType
from tracker.exceptions import ProjectHandleTakenError, ProjectNameTakenError
from tracker.repositories.project import ProjectRepository
from tracker.types import NewProjectRequest


def _project(handle: str, name: str | None = None) -> Project:
    return Project(org_id=TEST_ORG_ID, handle=handle, name=name or handle.title(), spend_type=SpendType.DEVELOPMENT)


def _tracker_rows(tracker_database: Session) -> list[Project]:
    return list(tracker_database.exec(select(Project)).all())


class TestProjectServiceWrites:
    @pytest.mark.description(
        "With both project service switches on, create, rename, archive, unarchive and delete change only the service."
    )
    def test_writes_skip_the_tracker_table(self, tracker_database: Session, project_service: MockProjectRoutes) -> None:
        repository = ProjectRepository(tracker_database)

        router = repository.create(_project("router-bench"))
        default = repository.get_or_create_default(TEST_ORG_ID, "alice@vals.ai")
        assert repository.get_or_create_default(TEST_ORG_ID, "bob@vals.ai").id == default.id
        router.name = "Router Bench Q4"
        _ = repository.update(router)
        project_service.rows[str(router.id)]["name"] = "Renamed elsewhere"
        router.archived_at = datetime(2026, 11, 2, tzinfo=UTC)
        _ = repository.save(router)
        assert project_service.rows[str(router.id)]["name"] == "Renamed elsewhere"
        assert project_service.rows[str(router.id)]["archived_at"] is not None
        router.archived_at = None
        _ = repository.save(router)
        assert project_service.rows[str(router.id)]["archived_at"] is None
        assert repository.delete(router) is True

        assert [row["handle"] for row in project_service.rows.values() if row.get("deleted_at") is None] == ["default"]
        assert project_service.rows[str(router.id)]["deleted_at"] is not None
        assert repository.create(_project("router-bench", "Renamed elsewhere")).id != router.id
        assert _tracker_rows(tracker_database) == []

    @pytest.mark.description("A create that reuses a handle or a name is rejected with the matching error.")
    def test_taken_handle_and_name(self, tracker_database: Session, project_service: MockProjectRoutes) -> None:
        repository = ProjectRepository(tracker_database)
        _ = repository.create(_project("router-bench"))

        with pytest.raises(ProjectHandleTakenError):
            _ = repository.create(_project("router-bench", "Other"))
        with pytest.raises(ProjectNameTakenError):
            _ = repository.create(_project("other-bench", " router-BENCH "))
        assert len(project_service.rows) == 1
        assert _tracker_rows(tracker_database) == []

    @pytest.mark.description("A delete leaves a project with a linked run in the service.")
    def test_delete_keeps_a_linked_project(self, tracker_database: Session, project_service: MockProjectRoutes) -> None:
        repository = ProjectRepository(tracker_database)
        project = repository.create(_project("router-bench"))
        tracker_database.add(
            RunProject(benchmark_id=make_benchmark(session=tracker_database).id, project_id=project.id)
        )
        tracker_database.commit()

        assert repository.delete(project) is False

        assert project_service.rows[str(project.id)].get("deleted_at") is None

    @pytest.mark.description(
        "A rename the service refuses because the name was just taken raises the name-taken error."
    )
    def test_rename_race_is_name_taken(
        self, tracker_database: Session, project_service: MockProjectRoutes, monkeypatch: pytest.MonkeyPatch
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
        assert project_service.rows[str(router.id)]["name"] == "Router-Bench"

    @pytest.mark.description(
        "A project created with a Gateway key request keeps its Gateway request ID in the service."
    )
    def test_create_keeps_the_gateway_request_id(
        self, tracker_database: Session, project_service: MockProjectRoutes, monkeypatch: pytest.MonkeyPatch
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

        assert project_service.rows[str(created.id)]["gateway_request_id"] == str(request_id)
        assert _tracker_rows(tracker_database) == []
