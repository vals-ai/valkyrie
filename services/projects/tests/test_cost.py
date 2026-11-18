import http.client
import io
import json
import urllib.error
import urllib.request
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from email.message import Message
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from projects.database import engine
from projects.main import app
from projects.models import Base, Project, SpendType


_ORG = UUID("00000000-0000-0000-0000-0000000000a1")
_PROJECT = UUID("00000000-0000-0000-0000-0000000000c1")
_TOKEN = {"Authorization": "Bearer project-token"}
_TRACKER = "http://tracker.test"
_GATEWAY = "http://gateway.test"
_THROUGH = "2026-11-11T23:45:00Z"
_KEPT = "00000000-0000-0000-0000-0000000000d1"
_IDLE = "00000000-0000-0000-0000-0000000000d2"
_RELINKED = "00000000-0000-0000-0000-0000000000d3"
_SETTINGS = ("TRACKER_URL", "MODEL_GATEWAY_URL", "MODEL_GATEWAY_MACHINE_API_KEY")


class Upstreams:
    def __init__(self) -> None:
        self.tracker_runs: list[dict[str, object]] = []
        self.gateway_runs: dict[str, str] = {}
        self.project_key: list[dict[str, object]] = []
        self.linked: list[str] = []
        self.throughs: list[str] = []
        self.failure: tuple[str, int | Exception] | None = None
        self.calls: list[tuple[str, str, dict[str, str], Any]] = []
        self.timeouts: list[float | None] = []

    def urlopen(self, request: urllib.request.Request, timeout: float | None = None) -> io.BytesIO:
        url = request.full_url
        body: Any = json.loads(request.data) if isinstance(request.data, bytes) else None
        self.calls.append((request.get_method(), url, dict(request.header_items()), body))
        self.timeouts.append(timeout)
        if self.failure is not None and url.startswith(self.failure[0]):
            error = self.failure[1]
            if isinstance(error, int):
                raise urllib.error.HTTPError(url, error, "failed", Message(), io.BytesIO(b"{}"))
            raise error
        if url.endswith("/linked-runs"):
            if not all(isinstance(run, str) for run in body["run_ids"]):
                raise urllib.error.HTTPError(url, 422, "unprocessable", Message(), io.BytesIO(b"{}"))
            reply: object = {"org_id": str(_ORG), "run_ids": [run for run in body["run_ids"] if run in self.linked]}
        elif url.startswith(_TRACKER):
            reply = {"org_id": str(_ORG), "project_id": str(_PROJECT), "environment": "dev"}
            runs = [{"by_type": {"sandbox": run["cost_usd"]}, **run} for run in self.tracker_runs]
            reply = {**reply, "runs": runs}
        else:
            batch = len(self.gateway_bodies()) - 1
            runs = [{"run_id": run, "amount_usd": self.gateway_runs.get(run, "0")} for run in body["run_ids"]]
            reply = {
                "runs": runs,
                "project_key": self.project_key if body["project_id"] else [],
                "through": self.throughs[batch] if self.throughs else _THROUGH,
            }
        return io.BytesIO(json.dumps(reply).encode())

    def gateway_bodies(self) -> list[Any]:
        return [body for _, url, _, body in self.calls if url.startswith(_GATEWAY)]


@pytest.fixture
def upstreams(monkeypatch: pytest.MonkeyPatch) -> Upstreams:
    fake = Upstreams()
    monkeypatch.setattr(urllib.request, "urlopen", fake.urlopen)
    return fake


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("PROJECT_SERVICE_TOKEN", "project-token")
    monkeypatch.setenv("TRACKER_URL", _TRACKER)
    monkeypatch.setenv("MODEL_GATEWAY_URL", _GATEWAY)
    monkeypatch.setenv("MODEL_GATEWAY_MACHINE_API_KEY", "gateway-key")
    bound = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(bound)
    with Session(bound) as session:
        session.add(
            Project(
                id=_PROJECT,
                org_id=_ORG,
                handle="router-bench",
                name="Router Bench",
                spend_type=SpendType.CUSTOMER,
                extra_tags={},
                created_at=datetime(2026, 11, 1, tzinfo=UTC),
            )
        )
        session.commit()
    app.dependency_overrides[engine] = lambda: bound
    yield TestClient(app)
    app.dependency_overrides.clear()


def _cost(
    client: TestClient, project: UUID = _PROJECT, org: UUID = _ORG, environment: str = "dev", **headers: str
) -> Any:
    return client.get(
        f"/orgs/{org}/projects/{project}/cost", params={"environment": environment}, headers=headers or _TOKEN
    )


def _dollars(response: Any, *fields: str) -> tuple[Decimal, ...]:
    return tuple(Decimal(response.json()[field]) for field in fields)


class TestProjectCost:
    @pytest.mark.description(
        "Total is Tracker dollars plus model dollars on the project's runs and on its key outside any linked run."
    )
    def test_total_adds_tracker_runs_and_key_outside_runs(self, client: TestClient, upstreams: Upstreams) -> None:
        upstreams.tracker_runs = [{"run_id": _KEPT, "cost_usd": 1.5}, {"run_id": _IDLE, "cost_usd": 0.0}]
        upstreams.gateway_runs = {_KEPT: "2.25", _IDLE: "0.25"}
        upstreams.project_key = [
            {"run_id": _KEPT, "amount_usd": "2.0"},
            {"run_id": _RELINKED, "amount_usd": "7.0"},
            {"run_id": "a1b2c3d4", "amount_usd": "0.5"},
            {"run_id": None, "amount_usd": "0.25"},
        ]
        upstreams.linked = [_KEPT, _RELINKED]

        response = _cost(client)

        assert response.status_code == 200
        body = response.json()
        assert (body["org_id"], body["project_id"], body["environment"]) == (str(_ORG), str(_PROJECT), "dev")
        assert body["through"] == _THROUGH
        assert _dollars(response, "tracker_usd", "run_model_usd", "key_model_usd", "model_usd", "total_usd") == (
            Decimal("1.5"),
            Decimal("2.5"),
            Decimal("0.75"),
            Decimal("3.25"),
            Decimal("4.75"),
        )
        assert {key: Decimal(value) for key, value in body["tracker_by_type_usd"].items()} == {
            "sandbox": Decimal("1.5")
        }
        method, url, headers, _ = upstreams.calls[0]
        assert (method, url) == (
            "GET",
            f"{_TRACKER}/project-service/orgs/{_ORG}/projects/{_PROJECT}/run-costs?environment=dev",
        )
        assert headers["Authorization"] == "Bearer project-token"
        method, url, headers, body = upstreams.calls[1]
        assert (method, url) == ("POST", f"{_GATEWAY}/admin/managed-auth/machine/usage/run-sums")
        assert headers["Authorization"] == "Bearer gateway-key"
        assert body == {"run_ids": [_KEPT, _IDLE], "project_id": str(_PROJECT), "project_environment": "dev"}
        method, url, _, body = upstreams.calls[2]
        assert (method, url) == ("POST", f"{_TRACKER}/project-service/orgs/{_ORG}/linked-runs")
        assert body == {"run_ids": sorted([_RELINKED, "a1b2c3d4"])}
        assert all(timeout for timeout in upstreams.timeouts)

    @pytest.mark.description("A project with no runs still counts model dollars on its key.")
    def test_no_runs_counts_key(self, client: TestClient, upstreams: Upstreams) -> None:
        upstreams.project_key = [{"run_id": "a1b2c3d4", "amount_usd": "0.75"}]

        response = _cost(client)

        assert response.status_code == 200
        assert _dollars(response, "model_usd", "total_usd") == (Decimal("0.75"), Decimal("0.75"))
        assert upstreams.gateway_bodies() == [
            {"run_ids": [], "project_id": str(_PROJECT), "project_environment": "dev"}
        ]

    @pytest.mark.description(
        "Runs go to the Gateway in batches of 1000, the key is asked once, the earliest cutoff wins."
    )
    def test_batches_runs(self, client: TestClient, upstreams: Upstreams) -> None:
        runs = [str(uuid4()) for _ in range(1001)]
        upstreams.tracker_runs = [{"run_id": run, "cost_usd": 0.0} for run in runs]
        upstreams.gateway_runs = {run: "1" for run in runs}
        upstreams.project_key = [{"run_id": "a1b2c3d4", "amount_usd": "1"}]
        upstreams.throughs = ["2026-11-11T23:46:00Z", _THROUGH]

        response = _cost(client)

        assert response.status_code == 200
        assert _dollars(response, "total_usd") == (Decimal(1002),)
        assert response.json()["through"] == _THROUGH
        bodies = upstreams.gateway_bodies()
        assert [len(body["run_ids"]) for body in bodies] == [1000, 1]
        assert [body["project_id"] for body in bodies] == [str(_PROJECT), None]

    @pytest.mark.description("The environment is sent to Tracker URL-encoded and to the Gateway as given.")
    def test_encodes_environment(self, client: TestClient, upstreams: Upstreams) -> None:
        assert _cost(client, environment="prod&environment=dev").status_code == 200
        assert upstreams.calls[0][1].endswith("/run-costs?environment=prod%26environment%3Ddev")
        assert upstreams.gateway_bodies()[0]["project_environment"] == "prod&environment=dev"

    @pytest.mark.parametrize("org", [_ORG, uuid4()], ids=["unknown-project", "other-org"])
    @pytest.mark.description("A project not in the org is 404 and neither upstream is called.")
    def test_unknown_project(self, client: TestClient, upstreams: Upstreams, org: UUID) -> None:
        response = _cost(client, project=uuid4() if org == _ORG else _PROJECT, org=org)

        assert (response.status_code, response.json()) == (404, {"detail": "Project not found"})
        assert upstreams.calls == []

    @pytest.mark.description("A deleted project is 404 and neither upstream is called.")
    def test_deleted_project(self, client: TestClient, upstreams: Upstreams) -> None:
        with Session(app.dependency_overrides[engine]()) as session:
            session.get_one(Project, _PROJECT).deleted_at = datetime(2026, 11, 2, tzinfo=UTC)
            session.commit()

        response = _cost(client)

        assert (response.status_code, response.json()) == (404, {"detail": "Project not found"})
        assert upstreams.calls == []

    @pytest.mark.description("Only the project service token is accepted.")
    def test_needs_token(self, client: TestClient, upstreams: Upstreams) -> None:
        assert _cost(client, Authorization="Bearer wrong").status_code == 401
        assert upstreams.calls == []

    @pytest.mark.description("The cost route answers 409 when Tracker serves another environment.")
    def test_tracker_other_environment(self, client: TestClient, upstreams: Upstreams) -> None:
        upstreams.failure = (_TRACKER, 409)

        assert _cost(client).status_code == 409
        assert upstreams.gateway_bodies() == []

    @pytest.mark.parametrize(
        "failure",
        [
            (_TRACKER, 500),
            (_GATEWAY, 409),
            (_GATEWAY, 503),
            (_GATEWAY, urllib.error.URLError("refused")),
            (_GATEWAY, TimeoutError()),
            (_GATEWAY, http.client.RemoteDisconnected("closed")),
            (_GATEWAY, http.client.IncompleteRead(b"")),
        ],
        ids=["tracker-500", "gateway-409", "gateway-503", "refused", "timeout", "disconnected", "incomplete"],
    )
    @pytest.mark.description("The cost route answers 503 when an upstream fails, never a partial total.")
    def test_upstream_failure(
        self, client: TestClient, upstreams: Upstreams, failure: tuple[str, int | Exception]
    ) -> None:
        upstreams.failure = failure

        assert _cost(client).status_code == 503

    @pytest.mark.parametrize(
        "reply",
        [
            {"tracker_runs": [{"run_id": _KEPT, "cost_usd": "lots"}]},
            {"tracker_runs": [{"run_id": _KEPT, "cost_usd": 1.5, "by_type": None}]},
            {"gateway_runs": {_KEPT: "lots"}},
            {"project_key": [{"run_id": "a1b2c3d4", "amount_usd": None}]},
            {"throughs": ["yesterday"]},
        ],
        ids=["tracker-cost", "tracker-by-type", "gateway-amount", "key-amount", "through"],
    )
    @pytest.mark.description("The cost route answers 503 when an upstream reply carries a malformed field.")
    def test_malformed_reply(self, client: TestClient, upstreams: Upstreams, reply: dict[str, Any]) -> None:
        upstreams.tracker_runs = [{"run_id": _KEPT, "cost_usd": 1.5}]
        for name, value in reply.items():
            setattr(upstreams, name, value)

        assert _cost(client).status_code == 503

    @pytest.mark.parametrize("setting", _SETTINGS)
    @pytest.mark.description("The cost route answers 503 without calling upstreams when a setting is unset.")
    def test_unset_setting(
        self, client: TestClient, upstreams: Upstreams, monkeypatch: pytest.MonkeyPatch, setting: str
    ) -> None:
        monkeypatch.delenv(setting)

        assert _cost(client, Authorization="Bearer project-token").status_code == 503
        assert upstreams.calls == []
