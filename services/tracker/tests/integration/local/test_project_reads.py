import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import HTTPException
from sqlmodel import Session, select

from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.api.projects import find_project, reject_archived_project
from tracker.clients import projects as project_client
from tracker.clients.projects import ProjectServiceClient
from tracker.database.models import Project, RunProject
from tracker.domain import SpendType
from tracker.repositories import project as project_repository
from tracker.repositories.project import ProjectRepository

ARCHIVED_AT = datetime(2026, 11, 2, 12, tzinfo=UTC)


class ReadService:
    def __init__(self) -> None:
        self.rows: dict[UUID, dict[str, Any]] = {}
        self.writes = 0

    def add(self, handle: str, **fields: Any) -> UUID:
        project_id = uuid4()
        self.rows[project_id] = {
            "id": str(project_id),
            "handle": handle,
            "name": handle.title(),
            "spend_type": "development",
            "subcategory": None,
            "customer": None,
            "extra_tags": {},
            "gateway_request_id": None,
            "gateway_key_id": None,
            "created_at": "2026-11-01T12:00:00+00:00",
            "created_by_email": None,
            "archived_at": None,
        } | fields
        return project_id

    def handle(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.split("/")
        assert parts[2] == str(TEST_ORG_ID)
        if request.method == "PUT":
            self.writes += 1
            return httpx.Response(200, json={"projects": []})
        if request.method == "POST":
            row = self.rows[UUID(parts[4])]
            row["gateway_key_id"] = row["gateway_key_id"] or json.loads(request.content)["gateway_key_id"]
            return httpx.Response(200, json=row)
        if len(parts) == 5:
            row = self.rows.get(UUID(parts[4]))
            return httpx.Response(404) if row is None else httpx.Response(200, json=row)
        archived = request.url.params["include_archived"] == "true"
        listed = [
            {key: value for key, value in row.items() if not key.startswith("gateway_")}
            for row in sorted(self.rows.values(), key=lambda row: row["handle"])
            if archived or row["archived_at"] is None
        ]
        return httpx.Response(200, json={"projects": listed})


def _repository(tracker_database: Session, monkeypatch: pytest.MonkeyPatch, service: ReadService) -> ProjectRepository:
    transport = httpx.MockTransport(service.handle)
    client = ProjectServiceClient(httpx.Client(base_url="http://projects", transport=transport))
    monkeypatch.setattr(project_repository, "project_service_reader", lambda: client, raising=False)
    monkeypatch.setattr(project_repository, "project_service_writer", lambda: client)
    return ProjectRepository(tracker_database)


def _link(tracker_database: Session, project_id: UUID, benchmark_id: UUID | None = None) -> UUID:
    if benchmark_id is None:
        benchmark = make_benchmark()
        tracker_database.add(benchmark)
        benchmark_id = benchmark.id
    tracker_database.add(RunProject(benchmark_id=benchmark_id, project_id=project_id))
    tracker_database.commit()
    return benchmark_id


def _mirrored(tracker_database: Session, service: ReadService, handle: str) -> Project:
    project = Project(org_id=TEST_ORG_ID, handle=handle, name=f"Stale {handle}", spend_type=SpendType.DEVELOPMENT)
    tracker_database.add(project)
    tracker_database.commit()
    service.rows[project.id] = service.rows.pop(service.add(handle, id=str(project.id)))
    return project


class TestProjectReads:
    @pytest.mark.description("With `PROJECT_SERVICE_READS` on, the project list comes from the project service.")
    def test_lists_from_the_service(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        service = ReadService()
        _ = service.add("router-bench")
        _ = service.add("old-bench", archived_at=ARCHIVED_AT.isoformat())
        repository = _repository(tracker_database, monkeypatch, service)

        assert [row.handle for row in repository.list_all(TEST_ORG_ID)] == ["router-bench"]
        assert [row.handle for row in repository.list_all(TEST_ORG_ID, include_archived=True)] == [
            "old-bench",
            "router-bench",
        ]

    @pytest.mark.description("A project looked up by ID comes from the service with its Gateway key ID.")
    def test_gets_by_id(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        service = ReadService()
        key_id = uuid4()
        project_id = service.add("router-bench", gateway_key_id=str(key_id))
        repository = _repository(tracker_database, monkeypatch, service)

        project = repository.get_by_id(TEST_ORG_ID, project_id)

        assert project is not None
        assert (project.org_id, project.handle, project.gateway_key_id) == (TEST_ORG_ID, "router-bench", key_id)
        assert repository.get_by_id(TEST_ORG_ID, uuid4()) is None

    @pytest.mark.description(
        "A run's project handle resolves through the service, and an archived one is refused with 409."
    )
    def test_finds_by_handle(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        service = ReadService()
        key_id = uuid4()
        project_id = service.add("router-bench", gateway_key_id=str(key_id))
        _ = service.add("old-bench", archived_at=ARCHIVED_AT.isoformat())
        repository = _repository(tracker_database, monkeypatch, service)

        project = find_project(repository, TEST_ORG_ID, "router-bench")
        assert (project.id, project.gateway_key_id) == (project_id, key_id)
        with pytest.raises(HTTPException) as archived:
            _ = find_project(repository, TEST_ORG_ID, "old-bench")
        assert archived.value.status_code == 409

    @pytest.mark.description("A run's newest linked project is read from the service.")
    def test_gets_the_run_project(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        service = ReadService()
        older = _mirrored(tracker_database, service, "old-bench")
        project = _mirrored(tracker_database, service, "router-bench")
        benchmark_id = _link(tracker_database, project.id, _link(tracker_database, older.id))
        repository = _repository(tracker_database, monkeypatch, service)

        run_project = repository.get_for_run(benchmark_id)

        assert run_project is not None
        assert (run_project.id, run_project.name) == (project.id, "Router-Bench")

    @pytest.mark.description(
        "An archive that lands after a run's project check doesn't stop the run's link, since no row lock is held."
    )
    def test_archive_after_check_keeps_link(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        service = ReadService()
        project = _mirrored(tracker_database, service, "router-bench")
        repository = _repository(tracker_database, monkeypatch, service)

        checked = reject_archived_project(repository.get_for_link(TEST_ORG_ID, project.id))
        service.rows[project.id]["archived_at"] = ARCHIVED_AT.isoformat()
        benchmark_id = _link(tracker_database, checked.id)

        links = tracker_database.exec(select(RunProject).where(RunProject.benchmark_id == benchmark_id)).all()
        assert [link.project_id for link in links] == [project.id]
        assert repository.get_for_link(TEST_ORG_ID, project.id).archived_at == ARCHIVED_AT

    @pytest.mark.description(
        "With reads on, the service keeps the first Gateway key set for a project, and reads return that key."
    )
    def test_gateway_key_first_wins(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        service = ReadService()
        project = _mirrored(tracker_database, service, "router-bench")
        repository = _repository(tracker_database, monkeypatch, service)
        first, second = uuid4(), uuid4()

        assert repository.set_gateway_key(project, first) == first
        assert repository.set_gateway_key(project, second) == first
        locked = repository.get_by_id(TEST_ORG_ID, project.id, lock="update")
        assert locked is not None and locked.gateway_key_id == first

    @pytest.mark.description(
        "A run linked to a project the service answers 404 for raises LookupError, which the run routes return as a 500."
    )
    def test_link_to_missing_row_raises(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        service = ReadService()
        project = _mirrored(tracker_database, service, "router-bench")
        del service.rows[project.id]
        repository = _repository(tracker_database, monkeypatch, service)

        with pytest.raises(LookupError, match=str(project.id)):
            _ = repository.get_for_link(TEST_ORG_ID, project.id)

    @pytest.mark.description(
        "A run whose linked project the service answers 404 for raises `LookupError` for retry, resume and regrade."
    )
    def test_run_with_missing_row_raises_for_writes(
        self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        service = ReadService()
        project = _mirrored(tracker_database, service, "router-bench")
        benchmark_id = _link(tracker_database, project.id)
        del service.rows[project.id]
        repository = _repository(tracker_database, monkeypatch, service)

        with pytest.raises(LookupError, match=str(project.id)):
            _ = repository.get_for_run(benchmark_id, strict=True)

    @pytest.mark.description(
        "A run whose linked project the service answers 404 for reads as no project in its metadata."
    )
    def test_run_with_missing_row_reads_no_project(
        self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        service = ReadService()
        project = _mirrored(tracker_database, service, "router-bench")
        benchmark_id = _link(tracker_database, project.id)
        del service.rows[project.id]
        repository = _repository(tracker_database, monkeypatch, service)

        assert repository.get_for_run(benchmark_id) is None

    @pytest.mark.description("A run with no project link has no project, even for retry, resume and regrade.")
    def test_unlinked_run_has_no_project(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        benchmark = make_benchmark()
        tracker_database.add(benchmark)
        tracker_database.commit()
        repository = _repository(tracker_database, monkeypatch, ReadService())

        assert repository.get_for_run(benchmark.id, strict=True) is None

    @pytest.mark.description("Turning reads on without writes fails at startup instead of serving stale projects.")
    def test_reads_need_writes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PROJECT_SERVICE_READS", "true")
        monkeypatch.setenv("PROJECT_SERVICE_WRITES", "false")
        project_client.project_service_reader.cache_clear()

        with pytest.raises(ValueError, match="PROJECT_SERVICE_WRITES"):
            _ = project_client.project_service_reader()
        project_client.project_service_reader.cache_clear()
