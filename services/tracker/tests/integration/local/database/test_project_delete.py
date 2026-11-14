import time
from collections.abc import Callable, Generator
from concurrent.futures import Future, ThreadPoolExecutor
from uuid import UUID

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, select

from main import app
from tests.factories import make_benchmark
from tests.project_service import MockProjectRoutes
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity, get_current_org, get_current_starter
from tracker.database.models import DEFAULT_ORG_NAME, Org, Project, RunProject, SpendType
from tracker.clients.projects import ProjectServiceClient
from tracker.database.session import get_session
from tracker.repositories import project as project_repository
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.project import ProjectRepository


@pytest.fixture(autouse=True)
def migration_database(monkeypatch: pytest.MonkeyPatch, postgres_engine: Engine) -> None:
    monkeypatch.setattr("tracker.database.session.engine", postgres_engine)


@pytest.mark.description("A project can be created, edited, and deleted over HTTP against Postgres.")
def test_project_create_edit_delete_http_lifecycle_uses_postgres(
    monkeypatch: pytest.MonkeyPatch,
    postgres_session: Session,
) -> None:
    org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
    postgres_session.add(org)
    postgres_session.commit()

    def request_session() -> Generator[Session, None, None]:
        yield postgres_session

    identity = RequestIdentity(org=org, email="alice@vals.ai")
    monkeypatch.setitem(app.dependency_overrides, get_session, request_session)
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: org)
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)

    with TestClient(app) as client:
        created = client.post(
            "/projects",
            json={
                "handle": "router-bench",
                "name": "Router Bench",
                "spend_type": "development",
                "subcategory": "new_benchmark_generation",
            },
        )
        edited = client.put(
            "/projects/router-bench",
            json={
                "handle": "router-bench",
                "name": "Customer Router",
                "spend_type": "customer",
                "subcategory": "report",
                "customer": "acme",
                "extra_tags": {"engagement": "q4"},
            },
        )
        deleted = client.delete("/projects/router-bench")
        listed = client.get("/projects")

    assert created.status_code == 200, created.text
    assert edited.status_code == 200, edited.text
    assert edited.json()["name"] == "Customer Router"
    assert deleted.status_code == 200, deleted.text
    assert deleted.json() == edited.json()
    assert listed.json() == {"projects": []}


@pytest.mark.description(
    "With both project service switches on, a delete that races a run link keeps the service row for the run's spend."
)
def test_delete_racing_a_run_link_keeps_the_service_row(
    monkeypatch: pytest.MonkeyPatch,
    postgres_engine: Engine,
) -> None:
    org, benchmark_id, project = _seed_service_project(postgres_engine)
    routes = MockProjectRoutes()
    routes.seed(project)

    def link_before_service_delete(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/delete"):
            with Session(postgres_engine) as session:
                session.add(RunProject(benchmark_id=benchmark_id, project_id=project.id))
                session.commit()
        return routes.handle(request)

    _use_project_service(monkeypatch, link_before_service_delete)
    with Session(postgres_engine, expire_on_commit=False) as session:
        identity = RequestIdentity(org=org, email="alice@vals.ai")
        monkeypatch.setitem(app.dependency_overrides, get_session, lambda: session)
        monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: org)
        monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)
        with TestClient(app) as client:
            response = client.delete("/projects/router-bench")

    assert response.status_code == 200, response.text
    assert routes.rows[str(project.id)]["deleted_at"] is not None
    with Session(postgres_engine) as session:
        assert [link.project_id for link in session.exec(select(RunProject)).all()] == [project.id]


@pytest.mark.description("With both project service switches on, a run cannot link a project deleted in the service.")
def test_run_cannot_link_a_deleted_project(monkeypatch: pytest.MonkeyPatch, postgres_engine: Engine) -> None:
    _, _, project = _seed_service_project(postgres_engine)
    routes = MockProjectRoutes()
    routes.seed(project)
    routes.rows[str(project.id)]["deleted_at"] = "2026-11-03T12:00:00Z"
    _use_project_service(monkeypatch, routes.handle)

    with Session(postgres_engine) as session, pytest.raises(LookupError):
        _ = ProjectRepository(session).get_for_link(TEST_ORG_ID, project.id)


def _use_project_service(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]) -> None:
    client = ProjectServiceClient(httpx.Client(base_url="http://projects", transport=httpx.MockTransport(handler)))
    monkeypatch.setattr(project_repository, "project_service", lambda: client)


def _seed_service_project(engine: Engine) -> tuple[Org, UUID, Project]:
    with Session(engine, expire_on_commit=False) as session:
        org = Org(id=TEST_ORG_ID, name=DEFAULT_ORG_NAME)
        session.add(org)
        session.commit()
        benchmark = make_benchmark(session=session)
    project = Project(
        org_id=TEST_ORG_ID,
        handle="router-bench",
        name="Router Bench",
        spend_type=SpendType.DEVELOPMENT,
        subcategory="new_benchmark_generation",
    )
    return org, benchmark.id, project


@pytest.mark.description(
    "With both project service switches on, a delete that races a run link is refused and the service row stays live."
)
def test_delete_racing_a_run_link_with_reads_on_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    postgres_engine: Engine,
) -> None:
    _, benchmark_id, project = _seed_service_project(postgres_engine)
    routes = MockProjectRoutes()
    routes.seed(project)
    _use_project_service(monkeypatch, routes.handle)

    with Session(postgres_engine) as run_session, ThreadPoolExecutor(max_workers=1) as pool:
        linked = ProjectRepository(run_session).get_for_link(TEST_ORG_ID, project.id)
        run_session.add(RunProject(benchmark_id=benchmark_id, project_id=linked.id))
        run_session.flush()

        def delete() -> bool:
            with Session(postgres_engine) as delete_session:
                repository = ProjectRepository(delete_session)
                row = repository.get(TEST_ORG_ID, project.handle)
                assert row is not None
                return repository.delete(row)

        deleted = pool.submit(delete)
        time.sleep(1)
        run_session.commit()
        refused = not deleted.result(timeout=30)

    with Session(postgres_engine) as session:
        assert [link.project_id for link in session.exec(select(RunProject)).all()] == [project.id]
    assert routes.rows[str(project.id)].get("deleted_at") is None
    assert refused


@pytest.mark.description(
    "With both project service switches on, two deletes that read the same project both succeed and it is deleted once."
)
def test_racing_deletes_of_one_project_both_succeed(monkeypatch: pytest.MonkeyPatch, postgres_engine: Engine) -> None:
    _, _, project = _seed_service_project(postgres_engine)
    routes = MockProjectRoutes()
    routes.seed(project)
    _use_project_service(monkeypatch, routes.handle)

    with Session(postgres_engine) as first, Session(postgres_engine) as second:
        rows = [ProjectRepository(session).get(TEST_ORG_ID, project.handle) for session in (first, second)]
        assert rows[0] is not None and rows[1] is not None
        deleted = [ProjectRepository(first).delete(rows[0])]
        first.commit()
        deleted.append(ProjectRepository(second).delete(rows[1]))

    assert deleted == [True, True]
    assert routes.rows[str(project.id)]["deleted_at"] is not None


def _delete_waits(engine: Engine) -> None:
    for _ in range(50):
        with engine.connect() as connection:
            waiting = connection.execute(
                text("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted")
            )
            if waiting.scalar_one():
                return
        time.sleep(0.1)
    pytest.fail("The delete never waited on the project links lock")


@pytest.mark.description(
    "With both project service switches on, a delete that races moving a run into the project is refused."
)
def test_delete_racing_a_retag_with_reads_on_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    postgres_engine: Engine,
) -> None:
    org, benchmark_id, project = _seed_service_project(postgres_engine)
    routes = MockProjectRoutes()
    routes.seed(project)
    _use_project_service(monkeypatch, routes.handle)

    def delete() -> bool:
        with Session(postgres_engine) as delete_session:
            repository = ProjectRepository(delete_session)
            row = repository.get(TEST_ORG_ID, project.handle)
            assert row is not None
            return repository.delete(row)

    with Session(postgres_engine, expire_on_commit=False) as session, ThreadPoolExecutor(max_workers=1) as pool:
        save = BenchmarkRepository.save
        deletes: list[Future[bool]] = []

        def save_then_delete(self: BenchmarkRepository, *rows: SQLModel) -> None:
            save(self, *rows)
            session.flush()
            deletes.append(pool.submit(delete))
            _delete_waits(postgres_engine)

        monkeypatch.setattr(BenchmarkRepository, "save", save_then_delete)
        identity = RequestIdentity(org=org, email="alice@vals.ai")
        monkeypatch.setitem(app.dependency_overrides, get_session, lambda: session)
        monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)
        with TestClient(app) as client:
            response = client.post(f"/benchmarks/{benchmark_id}/project", json={"project": project.handle})
        refused = not deletes[0].result(timeout=30)

    assert response.status_code == 200, response.text
    with Session(postgres_engine) as session:
        assert [link.project_id for link in session.exec(select(RunProject)).all()] == [project.id]
    assert routes.rows[str(project.id)].get("deleted_at") is None
    assert refused
