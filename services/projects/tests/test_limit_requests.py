from collections.abc import Iterator
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from projects import gateway
from projects.database import engine
from projects.main import app
from projects.models import Base, Project, ProjectLimit

ORG = UUID("00000000-0000-0000-0000-0000000000a1")
TOKEN = {"Authorization": "Bearer project-token"}
ROW = {"handle": "router-bench", "name": "Router Bench", "spend_type": "customer", "created_at": "2026-11-01T12:00:00Z"}
RAISE = {"limit_dollars": "120", "reason": "full rerun", "requested_by_email": "member@vals.ai"}
ADMIN = {"decided_by_email": "admin@vals.ai"}


@pytest.fixture
def sent(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[Decimal]]:
    monkeypatch.setenv("PROJECT_SERVICE_TOKEN", "project-token")
    bound = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(bound)
    app.dependency_overrides[engine] = lambda: bound
    app.state.bound = bound
    pushed: list[Decimal] = []

    def record(_project: Project, limit: ProjectLimit) -> None:
        pushed.append(limit.limit_dollars)

    monkeypatch.setattr(gateway, "put_project_budget", record)
    yield pushed
    app.dependency_overrides.clear()


client = TestClient(app)


def _project(org: UUID = ORG) -> str:
    project_id = str(uuid4())
    assert client.post(f"/orgs/{org}/projects", json={"id": project_id} | ROW, headers=TOKEN).status_code == 201
    return project_id


def _file(project_id: str, org: UUID = ORG) -> dict[str, str]:
    response = client.post(f"/orgs/{org}/projects/{project_id}/limit-requests", json=RAISE, headers=TOKEN)
    assert response.status_code == 201, response.text
    return response.json()


def _decide(project_id: str, request_id: str, action: str, org: UUID = ORG) -> int:
    url = f"/orgs/{org}/projects/{project_id}/limit-requests/{request_id}/{action}"
    return client.post(url, json=ADMIN, headers=TOKEN).status_code


def _listed(project_id: str) -> list[dict[str, str]]:
    return client.get(f"/orgs/{ORG}/projects/{project_id}/limit-requests", headers=TOKEN).json()


def _limit(project_id: str) -> tuple[Decimal, str | None] | None:
    with Session(app.state.bound) as session:
        saved = session.get(ProjectLimit, UUID(project_id))
        return None if saved is None else (saved.limit_dollars, saved.reason)


class TestLimitRequests:
    @pytest.mark.description("Filing a raise records it as pending and leaves the limit and the Gateway alone.")
    def test_filing_leaves_the_limit_alone(self, sent: list[Decimal]) -> None:
        project_id = _project()

        filed = _file(project_id)

        assert (filed["status"], Decimal(filed["limit_dollars"])) == ("pending", Decimal(120))
        assert filed["requested_by_email"] == RAISE["requested_by_email"]
        assert (_limit(project_id), sent) == (None, [])

    @pytest.mark.description("Approving a raise saves its limit and reason through the limit setter and pushes it.")
    def test_approval_sets_and_pushes_the_limit(self, sent: list[Decimal]) -> None:
        project_id = _project()
        filed = _file(project_id)

        assert _decide(project_id, filed["id"], "approve") == 200

        assert _limit(project_id) == (Decimal(120), "full rerun")
        assert sent == [Decimal(120)]
        (listed,) = _listed(project_id)
        assert (listed["status"], listed["decided_by_email"]) == ("approved", ADMIN["decided_by_email"])

    @pytest.mark.parametrize("target", ["120", "200"])
    @pytest.mark.description(
        "A raise that no longer exceeds the limit answers 409 and stays pending without another push."
    )
    def test_approval_rejects_a_superseded_raise(self, sent: list[Decimal], target: str) -> None:
        project_id = _project()
        path = f"/orgs/{ORG}/projects/{project_id}"
        assert client.put(f"{path}/limit", json={"limit_dollars": "80"}, headers=TOKEN).status_code == 200
        requests = [
            client.post(f"{path}/limit-requests", json=RAISE | {"limit_dollars": amount}, headers=TOKEN)
            for amount in (target, "200")
        ]
        assert all(response.status_code == 201 for response in requests)
        lower, higher = [response.json()["id"] for response in requests]
        assert _decide(project_id, higher, "approve") == 200

        response = client.post(f"{path}/limit-requests/{lower}/approve", json=ADMIN, headers=TOKEN)

        assert response.status_code == 409, response.text
        assert response.json()["detail"] == "Requested limit must exceed the project's current limit"
        assert _limit(project_id) == (Decimal(200), "full rerun")
        assert sent == [Decimal(80), Decimal(200)]
        assert {item["id"]: item["status"] for item in _listed(project_id)} == {lower: "pending", higher: "approved"}
        with Session(app.state.bound) as session:
            saved = session.get(ProjectLimit, UUID(project_id))
            assert saved is not None
            assert saved.version == 2

    @pytest.mark.description("A denied raise leaves the limit alone and answers 409 to any second decision.")
    def test_a_denied_raise_cannot_be_decided_again(self, sent: list[Decimal]) -> None:
        project_id = _project()
        filed = _file(project_id)

        assert _decide(project_id, filed["id"], "deny") == 200

        assert _decide(project_id, filed["id"], "approve") == 409
        assert _decide(project_id, filed["id"], "deny") == 409
        assert (_limit(project_id), sent) == (None, [])

    @pytest.mark.description("Another org or an unknown raise answers 404, and no token answers 401.")
    def test_refuses_other_orgs_and_missing_tokens(self, sent: list[Decimal]) -> None:
        project_id = _project()
        filed = _file(project_id)

        assert _decide(project_id, filed["id"], "approve", org=uuid4()) == 404
        assert _decide(project_id, str(uuid4()), "approve") == 404
        assert client.post(f"/orgs/{ORG}/projects/{project_id}/limit-requests", json=RAISE).status_code == 401
        assert client.get(f"/orgs/{uuid4()}/projects/{project_id}/limit-requests", headers=TOKEN).status_code == 404
        assert (_limit(project_id), sent) == (None, [])

    @pytest.mark.description("A project's raises list newest first.")
    def test_lists_newest_first(self, sent: list[Decimal]) -> None:
        project_id = _project()
        first, second = _file(project_id), _file(project_id)

        assert [item["id"] for item in _listed(project_id)] == [second["id"], first["id"]]

    @pytest.mark.description("The queue lists every org's raises newest first with their org, and filters by status.")
    def test_the_queue_spans_orgs(self, sent: list[Decimal]) -> None:
        other = uuid4()
        mine, theirs = _project(), _project(other)
        first, second = _file(mine), _file(theirs, other)
        assert _decide(mine, first["id"], "deny") == 200

        queue = client.get("/limit-requests", headers=TOKEN).json()
        pending = client.get("/limit-requests", params={"status": "pending"}, headers=TOKEN).json()

        assert [(item["id"], item["org_id"]) for item in queue] == [(second["id"], str(other)), (first["id"], str(ORG))]
        assert [item["id"] for item in pending] == [second["id"]]
        assert client.get("/limit-requests").status_code == 401
