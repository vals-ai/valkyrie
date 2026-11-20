from collections.abc import Generator
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Engine
from sqlmodel import Session, select

import tracker.api.cost_reports as cost_reports
from tests.factories import make_benchmark
from tracker.database.models import Benchmark, BenchmarkStatus, Org, RunCost, RunProject
from tracker.database.session import get_session
from tracker.repositories.cost import CostRepository
from tracker.services.cost_reports import (
    COST_REPORT_TOKEN_HEADER,
    issued_cost_report_tokens,
    with_cost_report_token,
)


@pytest.fixture
def reporting_client(postgres_engine: Engine) -> Generator[TestClient, None, None]:
    app = FastAPI()
    app.include_router(cost_reports.router)

    def request_session() -> Generator[Session, None, None]:
        with Session(postgres_engine) as session:
            yield session

    app.dependency_overrides[get_session] = request_session
    with TestClient(app) as client:
        yield client


def _run_with_token(session: Session, org_id: UUID, **fields: Any) -> tuple[Benchmark, str]:
    benchmark = make_benchmark(org_id=org_id)
    for field, value in fields.items():
        setattr(benchmark, field, value)
    session.add(benchmark)
    session.commit()
    headers = with_cost_report_token({"X-Customer": "kept"})
    session.add_all(issued_cost_report_tokens(benchmark.id, headers))
    session.commit()
    return benchmark, headers[COST_REPORT_TOKEN_HEADER]


@pytest.fixture
def org_id(postgres_session: Session) -> UUID:
    org = Org(id=uuid4(), name=f"reported-costs-{uuid4().hex[:8]}")
    postgres_session.add(org)
    postgres_session.commit()
    return org.id


@pytest.fixture
def run(postgres_session: Session, org_id: UUID) -> tuple[Benchmark, str]:
    return _run_with_token(postgres_session, org_id, status=BenchmarkStatus.IN_PROGRESS)


def _cost(**overrides: Any) -> dict[str, Any]:
    return {
        "cost_type": "cluster_gpu",
        "provider": "coreweave",
        "source_id": "nodes",
        "attempt_id": "attempt-1",
        "quantity": 2.5,
        "unit": "node_hour",
        "amount_usd": 40.0,
        **overrides,
    }


def _report(client: TestClient, run_id: UUID, costs: list[dict[str, Any]], token: str | None) -> Any:
    headers = {COST_REPORT_TOKEN_HEADER: token} if token is not None else {}
    return client.post(f"/costs/runs/{run_id}/reported", json=costs, headers=headers)


def _rows(session: Session, run_id: UUID) -> list[tuple[Any, ...]]:
    rows = session.exec(select(RunCost).where(RunCost.benchmark_id == run_id).order_by(RunCost.cost_type)).all()
    return [(row.cost_type, row.provider, row.attempt_id, row.quantity, row.unit, row.amount_usd) for row in rows]


def _reporters(session: Session, run_id: UUID) -> list[str | None]:
    return list(session.exec(select(RunCost.reporter).where(RunCost.benchmark_id == run_id)).all())


class TestReportRunCosts:
    @pytest.mark.description(
        "A repeated `source_id` replaces the attempt's earlier cost, and another attempt's cost is kept beside it."
    )
    def test_repeated_source_id_replaces_only_within_its_attempt(
        self, reporting_client: TestClient, postgres_session: Session, run: tuple[Benchmark, str]
    ) -> None:
        benchmark, token = run
        reports = [_cost(), _cost(quantity=3.0, amount_usd=60.0), _cost(attempt_id="attempt-2", amount_usd=8.0)]

        statuses = [_report(reporting_client, benchmark.id, [cost], token).status_code for cost in reports]

        assert statuses == [200, 200, 200]
        assert sorted(_rows(postgres_session, benchmark.id)) == [
            ("cluster_gpu", "coreweave", "attempt-1", 3.0, "node_hour", 60.0),
            ("cluster_gpu", "coreweave", "attempt-2", 2.5, "node_hour", 8.0),
        ]

    @pytest.mark.description(
        "A service may report a cost type the Tracker has never seen, and it counts toward the run's project."
    )
    def test_any_reported_type_counts_toward_the_project(
        self, reporting_client: TestClient, postgres_session: Session, run: tuple[Benchmark, str]
    ) -> None:
        benchmark, token = run
        project_id = uuid4()
        postgres_session.add(RunProject(benchmark_id=benchmark.id, project_id=project_id))
        postgres_session.commit()

        response = _report(
            reporting_client, benchmark.id, [_cost(cost_type="tpu_pod", provider="gcp", amount_usd=12.5)], token
        )

        costs = CostRepository(postgres_session).project_run_costs(benchmark.org_id, project_id)
        assert response.status_code == 200
        assert [(row.run_id, row.cost_type, row.cost_usd) for row in costs] == [(benchmark.id, "tpu_pod", 12.5)]

    @pytest.mark.description(
        "A reported row names the run's benchmark service as `hosted:<benchmark>` or `custom:<service URL>`."
    )
    def test_reporter_names_the_runs_service(
        self, reporting_client: TestClient, postgres_session: Session, org_id: UUID, run: tuple[Benchmark, str]
    ) -> None:
        hosted, hosted_token = run
        custom, custom_token = _run_with_token(
            postgres_session, org_id, custom_benchmark_service="https://bench.customer.example"
        )

        _report(reporting_client, hosted.id, [_cost()], hosted_token)
        _report(reporting_client, custom.id, [_cost()], custom_token)

        assert _reporters(postgres_session, hosted.id) == ["hosted:swebench"]
        assert _reporters(postgres_session, custom.id) == ["custom:https://bench.customer.example"]

    @pytest.mark.description(
        "A report is refused with 422 and stores nothing when any cost in it is invalid.\n"
        "\n"
        "Test cases:\n"
        "- A cost type the Tracker records itself, such as `sandbox`.\n"
        "- A cost without `amount_usd`."
    )
    @pytest.mark.parametrize("bad_cost", [_cost(cost_type="sandbox"), _cost(amount_usd=None)])
    def test_bad_cost_is_refused(
        self,
        reporting_client: TestClient,
        postgres_session: Session,
        run: tuple[Benchmark, str],
        bad_cost: dict[str, Any],
    ) -> None:
        benchmark, token = run
        response = _report(reporting_client, benchmark.id, [_cost(source_id="ok"), bad_cost], token)

        assert response.status_code == 422
        assert _rows(postgres_session, benchmark.id) == []


@pytest.mark.description(
    "Only the token the Tracker sent a run's benchmark service can report that run's costs, and only until the run ends.\n"
    "\n"
    "Test cases:\n"
    "- The run's own token is accepted.\n"
    "- A missing token is refused with 401.\n"
    "- An unknown token, another run's token, and a finished run's token are refused with 403."
)
@pytest.mark.parametrize(
    ("credential", "expected_status"),
    [("own", 200), ("missing", 401), ("unknown", 403), ("other-run", 403), ("finished-run", 403)],
)
def test_only_the_runs_own_token_may_report(
    reporting_client: TestClient,
    postgres_session: Session,
    org_id: UUID,
    run: tuple[Benchmark, str],
    credential: str,
    expected_status: int,
) -> None:
    benchmark, own_token = run
    _, other_token = _run_with_token(postgres_session, org_id)
    if credential == "finished-run":
        benchmark.status = BenchmarkStatus.FINISHED
        benchmark.finished_at = benchmark.started_at
        postgres_session.add(benchmark)
        postgres_session.commit()
    tokens = {"own": own_token, "missing": None, "unknown": "not-a-token", "other-run": other_token}

    response = _report(reporting_client, benchmark.id, [_cost()], tokens.get(credential, own_token))

    assert response.status_code == expected_status
    assert len(_rows(postgres_session, benchmark.id)) == (1 if expected_status == 200 else 0)
