from datetime import UTC, datetime

import httpx
import pytest
from sqlmodel import Session

from tests.integration.local.test_project_copy import MockProjectService
from tests.utils import TEST_ORG_ID
from tracker.clients import projects as project_client
from tracker.clients.projects import ProjectServiceClient
from tracker.database.models import Org, Project
from tracker.domain import SpendType
from tracker.exceptions import ProjectServiceUnavailableError
from tracker.repositories import project as project_repository
from tracker.repositories.project import ProjectRepository


def _repository(
    tracker_database: Session, monkeypatch: pytest.MonkeyPatch, handler: MockProjectService | None
) -> ProjectRepository:
    writer = None
    if handler is not None:
        transport = httpx.MockTransport(handler.handle)
        writer = ProjectServiceClient(httpx.Client(base_url="http://projects", transport=transport))
    monkeypatch.setattr(project_repository, "project_service_writer", lambda: writer)
    return ProjectRepository(tracker_database)


def _handles(service: MockProjectService) -> list[str]:
    return sorted(row["handle"] for row in service.orgs[str(TEST_ORG_ID)])


def _project(handle: str) -> Project:
    return Project(org_id=TEST_ORG_ID, handle=handle, name=handle.title(), spend_type=SpendType.DEVELOPMENT)


class TestProjectWrites:
    @pytest.mark.description(
        "With `PROJECT_SERVICE_WRITES` on, each create, rename, archive and delete sends the org's full list."
    )
    def test_writes_send_the_org_list(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        service = MockProjectService()
        repository = _repository(tracker_database, monkeypatch, service)

        router = repository.create(_project("router-bench"))
        _ = repository.get_or_create_default(TEST_ORG_ID, "alice@vals.ai")
        assert _handles(service) == ["default", "router-bench"]

        router.name = "Router Bench Q4"
        _ = repository.update(router)
        assert service.orgs[str(TEST_ORG_ID)][1]["name"] == "Router Bench Q4"

        assert repository.delete(router) is True
        assert _handles(service) == ["default"]

    @pytest.mark.description("A rename sends the archive another request committed after the rename read the project.")
    def test_sends_rows_committed_meanwhile(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        service = MockProjectService()
        repository = _repository(tracker_database, monkeypatch, service)
        router = repository.create(_project("router-bench"))
        _ = router.name
        with Session(tracker_database.get_bind()) as other:
            archived = ProjectRepository(other).get(TEST_ORG_ID, "router-bench")
            assert archived is not None
            archived.archived_at = datetime.now(UTC)
            _ = ProjectRepository(other).save(archived)

        router.name = "Router Bench Q4"
        _ = repository.update(router)

        assert service.orgs[str(TEST_ORG_ID)][0]["archived_at"] is not None

    @pytest.mark.description("A project service failure leaves the Tracker write undone and raises.")
    def test_failed_write_rolls_back(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        service = MockProjectService()
        service.fail = True
        repository = _repository(tracker_database, monkeypatch, service)

        with pytest.raises(ProjectServiceUnavailableError):
            _ = repository.create(_project("router-bench"))

        assert repository.list_all(TEST_ORG_ID, include_archived=True) == []

    @pytest.mark.description("With the switch off, writes stay in Tracker and send nothing.")
    def test_switch_off_sends_nothing(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        repository = _repository(tracker_database, monkeypatch, None)

        _ = repository.create(_project("router-bench"))

        assert [project.handle for project in repository.list_all(TEST_ORG_ID)] == ["router-bench"]

    @pytest.mark.description("A write with the switch on leaves the caller's org loaded after the session closes.")
    def test_write_keeps_the_org_loaded(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        org = tracker_database.get_one(Org, TEST_ORG_ID)
        repository = _repository(tracker_database, monkeypatch, MockProjectService())

        _ = repository.get_or_create_default(TEST_ORG_ID, "alice@vals.ai")
        tracker_database.close()

        assert org.name is not None

    @pytest.mark.description("Turning writes on without the project service URL and token fails before any write.")
    def test_writes_need_configuration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PROJECT_SERVICE_WRITES", "true")
        monkeypatch.delenv("PROJECT_SERVICE_URL", raising=False)
        monkeypatch.delenv("PROJECT_SERVICE_TOKEN", raising=False)
        project_client.project_service_writer.cache_clear()

        with pytest.raises(KeyError):
            _ = project_client.project_service_writer()
        project_client.project_service_writer.cache_clear()
