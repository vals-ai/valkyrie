import asyncio
import json
import threading
from collections.abc import Iterator
from decimal import Decimal
from functools import partial
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import ScalarResult, Select, create_engine, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool
from starlette.types import Message

from projects import gateway, main
from projects.database import engine
from projects.main import app
from projects.models import Base, Project, ProjectLimit

ORG = UUID("00000000-0000-0000-0000-0000000000a1")
TOKEN = {"Authorization": "Bearer project-token"}


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("PROJECT_SERVICE_TOKEN", "project-token")
    bound = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(bound)
    app.dependency_overrides[engine] = lambda: bound
    app.state.bound = bound
    yield TestClient(app)
    app.dependency_overrides.clear()


ROW = {"handle": "router-bench", "name": "Router Bench", "spend_type": "customer", "created_at": "2026-11-01T12:00:00Z"}


def _project(client: TestClient) -> str:
    project_id = str(uuid4())
    assert client.post(f"/orgs/{ORG}/projects", json={"id": project_id} | ROW, headers=TOKEN).status_code == 201
    return project_id


def _limits() -> list[tuple[Decimal, Decimal | None]]:
    with Session(app.state.bound) as session:
        return [(r.limit_dollars, r.gateway_confirmed_limit) for r in session.scalars(select(ProjectLimit))]


def _set(client: TestClient, project_id: str, limit: str, org: UUID = ORG) -> int:
    return client.put(
        f"/orgs/{org}/projects/{project_id}/limit", json={"limit_dollars": limit}, headers=TOKEN
    ).status_code


def _accepts(_project: Project, _limit: ProjectLimit) -> None:
    pass


def _refuses(_project: Project, _limit: ProjectLimit) -> None:
    raise OSError("gateway down")


def _record(monkeypatch: pytest.MonkeyPatch) -> list[tuple[UUID, Decimal]]:
    sent: list[tuple[UUID, Decimal]] = []

    def record(project: Project, limit: ProjectLimit) -> None:
        assert project.org_id == ORG
        sent.append((project.id, limit.limit_dollars))

    monkeypatch.setattr(gateway, "put_project_budget", record)
    return sent


class TestSetLimit:
    @pytest.mark.description("A set is pushed at once, even at the last pushed value, and a metadata save keeps it.")
    def test_sets_and_pushes(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        sent = _record(monkeypatch)
        project_id = _project(client)
        assert _set(client, project_id, "80") == 200
        assert _set(client, project_id, "80") == 200

        changes = {"name": "Router Bench 2", "spend_type": "customer"}
        assert client.put(f"/orgs/{ORG}/projects/{project_id}", json=changes, headers=TOKEN).status_code == 200
        assert sent == [(UUID(project_id), Decimal(80))] * 2
        assert _limits() == [(Decimal(80), Decimal(80))]

    @pytest.mark.description("A refused push stays unconfirmed until the next push lands.")
    def test_retries_until_the_gateway_accepts(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(gateway, "put_project_budget", _refuses)
        project_id = _project(client)
        assert _set(client, project_id, "80") == 200
        assert _limits() == [(Decimal(80), None)]

        monkeypatch.setattr(gateway, "put_project_budget", _accepts)
        gateway.push_budgets(app.state.bound)

        assert _limits() == [(Decimal(80), Decimal(80))]

    @pytest.mark.description("A retry sends the version the failed push sent.")
    def test_a_retry_sends_the_same_version(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        versions: list[int] = []

        def refuse(_project: Project, limit: ProjectLimit) -> None:
            versions.append(limit.version)
            raise OSError("gateway down")

        monkeypatch.setattr(gateway, "put_project_budget", refuse)
        project_id = _project(client)
        assert _set(client, project_id, "80") == 200
        assert _set(client, project_id, "90") == 200
        gateway.push_budgets(app.state.bound)

        assert versions == [1, 2, 2]

    @pytest.mark.description("A newer limit set before the push lands replaces the older one.")
    def test_pushes_a_newer_limit(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(gateway, "put_project_budget", _refuses)
        project_id = _project(client)
        assert _set(client, project_id, "80") == 200
        assert _set(client, project_id, "90") == 200
        sent = _record(monkeypatch)
        gateway.push_budgets(app.state.bound)

        assert sent == [(UUID(project_id), Decimal(90))]
        assert _limits() == [(Decimal(90), Decimal(90))]

    @pytest.mark.description("A later set keeps one row with the newest limit and clears its confirmation.")
    def test_saves_the_newest_limit(self, client: TestClient) -> None:
        project_id = _project(client)
        assert _set(client, project_id, "80") == 200
        with Session(app.state.bound) as session:
            session.get_one(ProjectLimit, UUID(project_id)).gateway_confirmed_limit = Decimal(80)
            session.commit()
        assert _set(client, project_id, "90") == 200

        assert _limits() == [(Decimal(90), None)]

    @pytest.mark.description("Every save raises the limit's version by one and stores its reason, or none.")
    def test_counts_each_save_and_keeps_its_reason(self, client: TestClient) -> None:
        project_id = _project(client)
        url = f"/orgs/{ORG}/projects/{project_id}/limit"
        assert client.put(url, json={"limit_dollars": "80", "reason": "full rerun"}, headers=TOKEN).status_code == 200
        assert _set(client, project_id, "80") == 200

        with Session(app.state.bound) as session:
            saved = session.get_one(ProjectLimit, UUID(project_id))
            assert (saved.version, saved.reason) == (2, None)
        assert _set(client, project_id, "90") == 200
        with Session(app.state.bound) as session:
            assert session.get_one(ProjectLimit, UUID(project_id)).version == 3

    @pytest.mark.description("A reason is saved with its limit.")
    def test_saves_the_reason(self, client: TestClient) -> None:
        project_id = _project(client)
        url = f"/orgs/{ORG}/projects/{project_id}/limit"
        assert client.put(url, json={"limit_dollars": "80", "reason": "full rerun"}, headers=TOKEN).status_code == 200

        with Session(app.state.bound) as session:
            saved = session.get_one(ProjectLimit, UUID(project_id))
            assert (saved.version, saved.reason) == (1, "full rerun")

    @pytest.mark.description("No token gets 401, another org or unknown project 404, and no limit is saved.")
    def test_refuses_another_org(self, client: TestClient) -> None:
        project_id = _project(client)

        assert _set(client, project_id, "80", org=uuid4()) == 404
        assert _set(client, str(uuid4()), "80") == 404
        assert client.put(f"/orgs/{ORG}/projects/{project_id}/limit", json={"limit_dollars": "80"}).status_code == 401
        assert _limits() == []

    @pytest.mark.description("The route pushes only its own project and leaves other limits to the loop.")
    def test_route_pushes_only_its_project(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(gateway, "put_project_budget", _refuses)
        assert _set(client, _project(client), "70") == 200
        sent = _record(monkeypatch)
        project_id = str(uuid4())
        row = {"id": project_id} | ROW | {"handle": "second-bench", "name": "Second Bench"}
        assert client.post(f"/orgs/{ORG}/projects", json=row, headers=TOKEN).status_code == 201
        assert _set(client, project_id, "80") == 200

        assert sent == [(UUID(project_id), Decimal(80))]

    @pytest.mark.description("On Postgres the limit is saved with a Postgres insert-on-conflict.")
    def test_saves_with_the_postgres_upsert(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        project_id = _project(client)
        statements: list[object] = []

        def capture(_session: Session, statement: object) -> None:
            statements.append(statement)

        monkeypatch.setattr(SQLiteDialect_pysqlite, "name", "postgresql")
        monkeypatch.setattr(Session, "execute", capture)
        assert _set(client, project_id, "80") == 200

        assert [type(s) for s in statements] == [postgresql.Insert]

    @pytest.mark.description("The route answers before its push starts, so a stalled Gateway never stalls a set.")
    def test_answers_before_the_push(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        project_id = _project(client)
        sent = _record(monkeypatch)
        pushed_at_reply: list[int] = []
        headers = [(b"authorization", b"Bearer project-token"), (b"content-type", b"application/json")]
        path = f"/orgs/{ORG}/projects/{project_id}/limit"
        scope = {"type": "http", "method": "PUT", "path": path, "headers": headers, "query_string": b""}

        async def receive() -> Message:
            return {"type": "http.request", "body": b'{"limit_dollars": "80"}'}

        async def send(_message: Message) -> None:
            pushed_at_reply.append(len(sent))

        asyncio.run(app(scope, receive, send))
        assert (pushed_at_reply, sent) == ([0, 0], [(UUID(project_id), Decimal(80))])

    @pytest.mark.description("The push commits after each row, so a row's lock lasts one Gateway call.")
    def test_push_commits_each_row(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(gateway, "put_project_budget", _refuses)
        assert _set(client, _project(client), "70") == 200
        project_id = str(uuid4())
        row = {"id": project_id} | ROW | {"handle": "second-bench", "name": "Second Bench"}
        assert client.post(f"/orgs/{ORG}/projects", json=row, headers=TOKEN).status_code == 201
        assert _set(client, project_id, "80") == 200
        commits: list[bool] = []
        commit = Session.commit

        def count(session: Session) -> None:
            commits.append(True)
            commit(session)

        monkeypatch.setattr(Session, "commit", count)
        gateway.push_budgets(app.state.bound)

        assert len(commits) == 3

    @pytest.mark.description(
        "The outbox loop drains again 30 seconds after any failed drain and after a successful one."
    )
    def test_loop_drains_every_30_seconds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        outcomes: list[BaseException | None] = [
            OperationalError("SELECT 1", {}, OSError("database down")),
            None,
            asyncio.CancelledError(),
        ]
        waits: list[float] = []

        drains: list[tuple[bool, object]] = []

        def drain(bound: object) -> None:
            drains.append((threading.current_thread() is threading.main_thread(), bound))
            if outcome := outcomes.pop(0):
                raise outcome

        async def wait(seconds: float) -> None:
            waits.append(seconds)
            monkeypatch.setenv("DATABASE_URL", "sqlite://")

        monkeypatch.delenv("DATABASE_URL", raising=False)
        engine.cache_clear()
        monkeypatch.setattr(main, "push_budgets", drain)
        monkeypatch.setattr(asyncio, "sleep", wait)
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(main.push_forever())

        assert outcomes == []
        assert waits == [30, 30, 30]
        assert drains == [(False, engine())] * 3

    @pytest.mark.description("Starting the service starts the outbox loop.")
    def test_startup_starts_the_loop(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        started: list[bool] = []

        async def loop() -> None:
            started.append(True)

        monkeypatch.setattr(main, "push_forever", loop)
        with client:
            assert client.get("/health").status_code in (200, 503)

        assert started == [True]

    @pytest.mark.description("Stopping the service cancels the outbox loop.")
    def test_shutdown_cancels_the_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PROJECT_SERVICE_TOKEN", "project-token")
        monkeypatch.setattr(main, "push_forever", partial(asyncio.sleep, 3600))

        async def cycle() -> bool:
            async with main.lifespan(app):
                (pusher,) = asyncio.all_tasks() - {asyncio.current_task()}
            await asyncio.sleep(0)
            return pusher.cancelled()

        assert asyncio.run(cycle())

    @pytest.mark.description("Each pending row is read with `FOR UPDATE SKIP LOCKED` on Postgres.")
    def test_push_locks_each_row(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(gateway, "put_project_budget", _refuses)
        assert _set(client, _project(client), "80") == 200
        statements: list[str] = []
        scalars = Session.scalars

        def capture(session: Session, statement: Select[tuple[object]]) -> ScalarResult[tuple[object]]:
            statements.append(str(statement.compile(dialect=postgresql.dialect())))
            return scalars(session, statement)

        monkeypatch.setattr(Session, "scalars", capture)
        gateway.push_budgets(app.state.bound)

        assert [s for s in statements if "FOR UPDATE SKIP LOCKED" in s] == statements[1:]

    @pytest.mark.description("Pending limits are pushed oldest change first.")
    def test_pushes_the_oldest_change_first(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(gateway, "put_project_budget", _refuses)
        first = _project(client)
        assert _set(client, first, "70") == 200
        second = str(uuid4())
        row = {"id": second} | ROW | {"handle": "second-bench", "name": "Second Bench"}
        assert client.post(f"/orgs/{ORG}/projects", json=row, headers=TOKEN).status_code == 201
        assert _set(client, second, "80") == 200
        assert _set(client, first, "75") == 200
        sent = _record(monkeypatch)
        gateway.push_budgets(app.state.bound)

        assert sent == [(UUID(second), Decimal(80)), (UUID(first), Decimal(75))]

    @pytest.mark.description("A row confirmed by another push between the read and the lock is not pushed again.")
    def test_skips_a_row_confirmed_before_its_lock(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(gateway, "put_project_budget", _refuses)
        project_id = _project(client)
        assert _set(client, project_id, "80") == 200
        reads: list[bool] = []
        scalars = Session.scalars

        def confirm_before_lock(session: Session, statement: Select[tuple[object]]) -> ScalarResult[tuple[object]]:
            if reads:
                with Session(app.state.bound) as other:
                    other.get_one(ProjectLimit, UUID(project_id)).gateway_confirmed_limit = Decimal(80)
                    other.commit()
            reads.append(True)
            return scalars(session, statement)

        monkeypatch.setattr(Session, "scalars", confirm_before_lock)
        sent = _record(monkeypatch)
        gateway.push_budgets(app.state.bound)

        assert sent == []


class TestGatewayClient:
    @pytest.mark.description("The client puts the limit on the Gateway route with the machine key and a 10s timeout.")
    def test_puts_the_budget(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: list[httpx.Request] = []
        answer = httpx.MockTransport(lambda request: seen.append(request) or httpx.Response(200))
        monkeypatch.setattr(httpx, "Client", partial(httpx.Client, transport=answer))
        project = Project(id=uuid4(), org_id=ORG)
        limit = ProjectLimit(project_id=project.id, limit_dollars=Decimal(80), version=3, reason="full rerun")
        gateway.GatewayClient("https://gateway.test", "machine-key").put_project_budget(project, "dev", limit)

        assert [(r.method, str(r.url), r.headers["Authorization"], r.extensions["timeout"]["read"]) for r in seen] == [
            (
                "PUT",
                f"https://gateway.test/admin/managed-auth/machine/projects/{project.id}/budget",
                "Bearer machine-key",
                10,
            )
        ]
        assert json.loads(seen[0].content) == {
            "project_organization_id": str(ORG),
            "project_environment": "dev",
            "limit_usd": "80",
            "version": 3,
            "reason": "full rerun",
        }

    @pytest.mark.description("A push reads the Gateway settings once, and a 409 stays unconfirmed with a warning.")
    def test_a_refusal_stays_unconfirmed(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        sent: list[httpx.Request] = []
        refused = httpx.MockTransport(lambda request: sent.append(request) or httpx.Response(409))
        monkeypatch.setattr(httpx, "Client", partial(httpx.Client, transport=refused))
        monkeypatch.setenv("MODEL_GATEWAY_URL", "https://gateway.test")
        monkeypatch.setenv("MODEL_GATEWAY_MACHINE_API_KEY", "machine-key")
        monkeypatch.setenv("VALKYRIE_STAGE", "bench")
        gateway.gateway.cache_clear()
        assert _set(client, _project(client), "80") == 200
        assert gateway.gateway() is gateway.gateway()
        gateway.gateway.cache_clear()

        assert [
            (r.url.host, r.headers["Authorization"], json.loads(r.content)["project_environment"]) for r in sent
        ] == [("gateway.test", "Bearer machine-key", "bench")]
        assert _limits() == [(Decimal(80), None)]
        assert "Gateway budget push failed" in caplog.text
