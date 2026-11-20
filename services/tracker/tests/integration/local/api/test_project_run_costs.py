from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session

import tracker.config as config_module
from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.database.models import BenchmarkStatus, Org, RunCost, RunProject
from tracker.domain import SandboxResource
from tracker.repositories import cost as cost_repository
from tracker.repositories.cost import SANDBOX_COST_TYPE, SEARCH_COST_TYPE

TOKEN = "project-service-token"
STAMP = datetime(2026, 10, 20, tzinfo=UTC)
_LAST_YEAR = datetime(2025, 1, 15, tzinfo=UTC)


@pytest.fixture
def machine_client(local_app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    monkeypatch.setattr(config_module, "PROJECT_SERVICE_TOKEN", TOKEN, raising=False)
    monkeypatch.setattr(config_module, "VALKYRIE_STAGE", "dev")
    with TestClient(local_app) as client:
        yield client


def _run(session: Session, costs: list[tuple[str, float, datetime]], org_id: UUID = TEST_ORG_ID) -> UUID:
    benchmark = make_benchmark(status=BenchmarkStatus.FINISHED, started_at=STAMP, org_id=org_id, session=session)
    benchmark.finished_at = STAMP
    for cost_type, amount, usage_end in costs:
        session.add(
            RunCost(
                benchmark_id=benchmark.id,
                cost_type=cost_type,
                source_id=str(uuid4()),
                provider="daytona",
                amount_usd=amount,
                usage_end=usage_end,
            )
        )
    session.commit()
    return benchmark.id


def _link(session: Session, run_id: UUID, *project_ids: UUID) -> None:
    for offset, project_id in enumerate(project_ids):
        session.add(
            RunProject(benchmark_id=run_id, project_id=project_id, created_at=STAMP + timedelta(minutes=offset))
        )
        session.commit()


def _one_run_costs(client: TestClient, session: Session, costs: list[tuple[str, float]]) -> float:
    project = uuid4()
    run = _run(session, [(cost_type, amount, STAMP) for cost_type, amount in costs])
    _link(session, run, project)
    response = client.get(_path(project), params={"environment": "dev"}, headers={"Authorization": f"Bearer {TOKEN}"})
    assert response.status_code == 200, response.text
    [row] = response.json()["runs"]
    assert row["run_id"] == str(run)
    return row["cost_usd"]


def _path(project_id: UUID, org_id: UUID = TEST_ORG_ID) -> str:
    return f"/project-service/orgs/{org_id}/projects/{project_id}/run-costs"


@pytest.mark.description("Each run's project-total dollars for the project service, by its newest project link.")
class TestProjectRunCosts:
    @pytest.mark.description(
        "The project service reads a project's runs and their all-time sandbox and search dollars.\n"
        "\n"
        "Test cases:\n"
        "- A run whose newest link is the project sums its sandbox ($1.00 + $0.50 from last year) and search ($4.00).\n"
        "- A run linked to the project before it moved to another project is left out.\n"
        "- A linked run with no cost is listed at $0.\n"
        "- Another org's run linked to the same project id is left out.\n"
        "- The response echoes the org, project and environment it answers for."
    )
    def test_lists_newest_link_runs_with_their_dollars(
        self, machine_client: TestClient, database_session: Session
    ) -> None:
        router, blog = uuid4(), uuid4()
        kept = _run(
            database_session,
            [(SANDBOX_COST_TYPE, 1.0, STAMP), (SANDBOX_COST_TYPE, 0.5, _LAST_YEAR), (SEARCH_COST_TYPE, 4.0, STAMP)],
        )
        _link(database_session, kept, blog, router)
        moved = _run(database_session, [(SANDBOX_COST_TYPE, 7.0, STAMP)])
        _link(database_session, moved, router, blog)
        idle = _run(database_session, [])
        _link(database_session, idle, router)
        other_org = Org(id=uuid4(), name="other-tenant")
        database_session.add(other_org)
        database_session.commit()
        foreign = _run(database_session, [(SANDBOX_COST_TYPE, 9.0, STAMP)], org_id=other_org.id)
        _link(database_session, foreign, router)

        response = machine_client.get(
            _path(router), params={"environment": "dev"}, headers={"Authorization": f"Bearer {TOKEN}"}
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["org_id"] == str(TEST_ORG_ID)
        assert body["project_id"] == str(router)
        assert body["environment"] == "dev"
        assert sorted(body["runs"], key=lambda row: row["run_id"]) == sorted(
            [
                {"run_id": str(kept), "cost_usd": 5.5, "by_type": {"sandbox": 1.5, "search": 4.0}},
                {"run_id": str(idle), "cost_usd": 0.0, "by_type": {}},
            ],
            key=lambda row: row["run_id"],
        )

    @pytest.mark.description("Each cost type in the project total counts toward it.")
    @pytest.mark.parametrize("cost_type", ["sandbox", "search"])
    def test_a_listed_cost_type_counts(
        self, cost_type: str, machine_client: TestClient, database_session: Session
    ) -> None:
        assert _one_run_costs(machine_client, database_session, [(cost_type, 2.0)]) == 2.0

    @pytest.mark.description("A cost type nobody has classified yet stays out of the project total.")
    def test_an_unlisted_cost_type_stays_out(self, machine_client: TestClient, database_session: Session) -> None:
        assert _one_run_costs(machine_client, database_session, [(SANDBOX_COST_TYPE, 1.0), ("made_up", 8.0)]) == 1.0

    @pytest.mark.description(
        "A run holding an old sandbox line and four per-resource sandbox lines counts all five under the `sandbox` key."
    )
    def test_old_and_split_sandbox_lines_both_count(
        self, machine_client: TestClient, database_session: Session
    ) -> None:
        project = uuid4()
        run = _run(database_session, [(SANDBOX_COST_TYPE, 1.0, STAMP)])
        database_session.add_all(
            [
                RunCost(
                    benchmark_id=run,
                    cost_type=SANDBOX_COST_TYPE,
                    resource=resource.value,
                    source_id="sb-1:2026-10-20T00:00:00+00:00",
                    provider="daytona",
                    amount_usd=0.25,
                    usage_end=STAMP,
                )
                for resource in SandboxResource
            ]
        )
        database_session.commit()
        _link(database_session, run, project)

        response = machine_client.get(
            _path(project), params={"environment": "dev"}, headers={"Authorization": f"Bearer {TOKEN}"}
        )

        [row] = response.json()["runs"]
        assert (row["cost_usd"], row["by_type"]) == (2.0, {"sandbox": 2.0})

    @pytest.mark.description("Every cost type Tracker names is counted in the project total.")
    def test_every_cost_type_is_classified(self) -> None:
        named = {value for name, value in vars(cost_repository).items() if name.endswith("_COST_TYPE")}

        assert named == set(cost_repository.PROJECT_COST_TYPES)

    @pytest.mark.description("A project with no linked runs answers with an empty list, not 404.")
    def test_a_project_without_runs_has_no_rows(self, machine_client: TestClient) -> None:
        response = machine_client.get(
            _path(uuid4()), params={"environment": "dev"}, headers={"Authorization": f"Bearer {TOKEN}"}
        )

        assert response.status_code == 200, response.text
        assert response.json()["runs"] == []

    @pytest.mark.description("Only the project service token is accepted.")
    @pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong"}, {"X-Api-Key": "fake"}])
    def test_other_callers_get_401(self, machine_client: TestClient, headers: dict[str, str]) -> None:
        response = machine_client.get(_path(uuid4()), params={"environment": "dev"}, headers=headers)

        assert response.status_code == 401

    @pytest.mark.description("A valid org access key is refused too.")
    @pytest.mark.usefixtures("access_key_auth")
    def test_a_valid_org_key_gets_401(self, machine_client: TestClient) -> None:
        response = machine_client.get(_path(uuid4()), params={"environment": "dev"}, headers={"X-Api-Key": "K_caller"})

        assert response.status_code == 401

    @pytest.mark.description(
        "A request for another environment's costs is refused instead of answered with this one's."
    )
    def test_another_environment_gets_409(self, machine_client: TestClient) -> None:
        response = machine_client.get(
            _path(uuid4()), params={"environment": "prod"}, headers={"Authorization": f"Bearer {TOKEN}"}
        )

        assert response.status_code == 409


@pytest.mark.description("Which run IDs are linked to any project, for counting key spend outside tagged runs.")
class TestLinkedRuns:
    @pytest.mark.description(
        "Only the org's runs with a project link come back.\n"
        "\n"
        "Test cases:\n"
        "- A run linked to a project, even one it later moved from, is listed.\n"
        "- A run with no project link is left out.\n"
        "- Another org's linked run is left out.\n"
        "- A random 8-hex run ID from a call without a run is left out."
    )
    def test_lists_linked_runs_only(self, machine_client: TestClient, database_session: Session) -> None:
        linked, moved, loose = (_run(database_session, []) for _ in range(3))
        _link(database_session, linked, uuid4())
        _link(database_session, moved, uuid4(), uuid4())
        other_org = Org(id=uuid4(), name="other-linked")
        database_session.add(other_org)
        database_session.commit()
        foreign = _run(database_session, [], org_id=other_org.id)
        _link(database_session, foreign, uuid4())

        response = machine_client.post(
            f"/project-service/orgs/{TEST_ORG_ID}/linked-runs",
            json={"run_ids": [str(linked), str(moved), str(loose), str(foreign), "a1b2c3d4"]},
            headers={"Authorization": f"Bearer {TOKEN}"},
        )

        assert response.status_code == 200, response.text
        assert response.json() == {"org_id": str(TEST_ORG_ID), "run_ids": sorted([str(linked), str(moved)])}

    @pytest.mark.description("Only the project service token is accepted.")
    @pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong"}, {"X-Api-Key": "fake"}])
    def test_other_callers_get_401(self, machine_client: TestClient, headers: dict[str, str]) -> None:
        response = machine_client.post(
            f"/project-service/orgs/{TEST_ORG_ID}/linked-runs", json={"run_ids": []}, headers=headers
        )

        assert response.status_code == 401

    @pytest.mark.description("A valid org access key is refused too.")
    @pytest.mark.usefixtures("access_key_auth")
    def test_a_valid_org_key_gets_401(self, machine_client: TestClient) -> None:
        response = machine_client.post(
            f"/project-service/orgs/{TEST_ORG_ID}/linked-runs", json={"run_ids": []}, headers={"X-Api-Key": "K_caller"}
        )

        assert response.status_code == 401
