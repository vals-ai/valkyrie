from collections.abc import Callable
import json
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.project_service import MockProjectRoutes
from tests.factories import link_run_project, make_benchmark, make_project
from tests.utils import TEST_ORG_ID
from tracker import project_gateway
import tracker.api.costs as costs_api
from tracker.clients import gateway_usage
from tracker.database.models import (
    BenchmarkStatus,
    Org,
    RunCost,
    RunProject,
    SandboxUnmatchedUsage,
    SpendType,
)


def _run_with_cost(
    session: Session,
    *,
    email: str | None,
    started_at: datetime,
    amounts: list[float],
    org_id: UUID = TEST_ORG_ID,
    provider: str = "daytona",
) -> None:
    benchmark = make_benchmark(
        status=BenchmarkStatus.FINISHED,
        started_at=started_at,
        started_by_email=email,
        org_id=org_id,
        session=session,
    )
    for amount in amounts:
        session.add(
            RunCost(
                benchmark_id=benchmark.id,
                cost_type="sandbox",
                source_id=str(uuid4()),
                provider=provider,
                amount_usd=amount,
            )
        )
    session.commit()


@pytest.mark.description("Per-email cost totals for runs started in a window.")
class TestCostsByEmail:
    @pytest.mark.description(
        (
            "Costs must be summed per starter email over runs started inside [start, end).\n"
            "\n"
            "Test cases:\n"
            "- Two runs by the same person, with differently cased emails, sum into one lowercased row.\n"
            "- A run started exactly at `start` is counted. One started exactly at `end` is not.\n"
            "- A run with no starter email is left out.\n"
            "- Another organization's runs are left out.\n"
            "- The same person's costs from another provider come back as their own row."
        )
    )
    def test_sums_each_starter_for_runs_started_in_window(self, client: TestClient, database_session: Session) -> None:
        utc = ZoneInfo("UTC")
        _run_with_cost(
            database_session, email="Alice@Vals.ai", started_at=datetime(2026, 10, 1, tzinfo=utc), amounts=[1.0, 0.5]
        )
        _run_with_cost(
            database_session, email="alice@vals.ai ", started_at=datetime(2026, 10, 31, 23, tzinfo=utc), amounts=[2.0]
        )
        _run_with_cost(
            database_session, email="bob@vals.ai", started_at=datetime(2026, 10, 15, tzinfo=utc), amounts=[0.25]
        )
        _run_with_cost(
            database_session, email="bob@vals.ai", started_at=datetime(2026, 11, 1, tzinfo=utc), amounts=[9.0]
        )
        _run_with_cost(
            database_session,
            email="alice@vals.ai",
            started_at=datetime(2026, 10, 10, tzinfo=utc),
            amounts=[0.75],
            provider="modal",
        )
        _run_with_cost(database_session, email=None, started_at=datetime(2026, 10, 15, tzinfo=utc), amounts=[9.0])
        other_org = Org(id=uuid4(), name="other-tenant")
        database_session.add(other_org)
        database_session.commit()
        _run_with_cost(
            database_session,
            email="alice@vals.ai",
            started_at=datetime(2026, 10, 15, tzinfo=utc),
            amounts=[9.0],
            org_id=other_org.id,
        )

        response = client.get(
            "/costs/by-email",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["start"] == "2026-10-01T00:00:00Z"
        assert body["end"] == "2026-11-01T00:00:00Z"
        assert body["costs"] == [
            {
                "email": "alice@vals.ai",
                "cost_type": "sandbox",
                "provider": "daytona",
                "amount_usd": 3.5,
                "run_count": 2,
            },
            {"email": "alice@vals.ai", "cost_type": "sandbox", "provider": "modal", "amount_usd": 0.75, "run_count": 1},
            {"email": "bob@vals.ai", "cost_type": "sandbox", "provider": "daytona", "amount_usd": 0.25, "run_count": 1},
        ]

    @pytest.mark.description(
        (
            "A window whose end is not after its start must be rejected.\n"
            "\n"
            "Test cases:\n"
            "- Equal start and end receives 422."
        )
    )
    def test_end_must_follow_start(self, client: TestClient) -> None:
        response = client.get(
            "/costs/by-email",
            params={"start": "2026-10-01", "end": "2026-10-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 422

    @pytest.mark.description(
        (
            "A window given as UTC datetimes must count runs by their exact start, not by their date.\n"
            "\n"
            "Test cases:\n"
            "- A run started at 21:30 UTC is counted in a window starting at 21:00 UTC that day.\n"
            "- A run started at 20:30 UTC the same day is left out.\n"
            "- The response echoes the window's exact start and end."
        )
    )
    def test_a_datetime_window_counts_runs_by_the_hour(self, client: TestClient, database_session: Session) -> None:
        utc = ZoneInfo("UTC")
        _run_with_cost(
            database_session,
            email="alice@vals.ai",
            started_at=datetime(2026, 10, 30, 21, 30, tzinfo=utc),
            amounts=[1.0],
        )
        _run_with_cost(
            database_session,
            email="alice@vals.ai",
            started_at=datetime(2026, 10, 30, 20, 30, tzinfo=utc),
            amounts=[2.0],
        )

        response = client.get(
            "/costs/by-email",
            params={"start": "2026-10-30T21:00:00Z", "end": "2026-10-31T21:00:00Z"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["start"] == "2026-10-30T21:00:00Z"
        assert body["end"] == "2026-10-31T21:00:00Z"
        assert [(cost["email"], cost["amount_usd"], cost["run_count"]) for cost in body["costs"]] == [
            ("alice@vals.ai", 1.0, 1)
        ]

    @pytest.mark.description(
        (
            "A window given as dates must keep meaning midnight UTC to midnight UTC.\n"
            "\n"
            "Test cases:\n"
            "- Runs started at 20:30 and 21:30 UTC on the same day are both counted in that day's date window.\n"
            "- The response echoes each date as midnight UTC."
        )
    )
    def test_a_date_window_counts_the_whole_utc_day(self, client: TestClient, database_session: Session) -> None:
        utc = ZoneInfo("UTC")
        for started_at, amount in [
            (datetime(2026, 10, 30, 21, 30, tzinfo=utc), 1.0),
            (datetime(2026, 10, 30, 20, 30, tzinfo=utc), 2.0),
        ]:
            _run_with_cost(database_session, email="alice@vals.ai", started_at=started_at, amounts=[amount])

        response = client.get(
            "/costs/by-email",
            params={"start": "2026-10-30", "end": "2026-10-31"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["start"] == "2026-10-30T00:00:00Z"
        assert body["end"] == "2026-10-31T00:00:00Z"
        assert [(cost["email"], cost["amount_usd"], cost["run_count"]) for cost in body["costs"]] == [
            ("alice@vals.ai", 3.0, 2)
        ]

    @pytest.mark.description(
        "Cost totals must require authentication.\n\nTest cases:\n- A request without a bearer session receives 401."
    )
    def test_costs_unauth_401(self, client: TestClient) -> None:
        response = client.get("/costs/by-email", params={"start": "2026-10-01", "end": "2026-11-01"})

        assert response.status_code == 401


def _tagged_run(
    session: Session,
    *,
    started_at: datetime,
    amounts: list[float],
    finished_at: datetime | None = None,
    usage_end: datetime | None = None,
    provider: str = "daytona",
    org_id: UUID = TEST_ORG_ID,
    status: BenchmarkStatus = BenchmarkStatus.FINISHED,
) -> UUID:
    benchmark = make_benchmark(
        status=status,
        started_at=started_at,
        started_by_email="alice@vals.ai",
        org_id=org_id,
        session=session,
    )
    benchmark.finished_at = finished_at or (started_at if status is BenchmarkStatus.FINISHED else None)
    session.add(benchmark)
    for amount in amounts:
        session.add(
            RunCost(
                benchmark_id=benchmark.id,
                cost_type="sandbox",
                source_id=str(uuid4()),
                provider=provider,
                amount_usd=amount,
                usage_end=usage_end,
            )
        )
    session.commit()
    return benchmark.id


@pytest.mark.description("Costs per month, vendor and project for costs placed in a window.")
class TestCostsByProject:
    @pytest.mark.description(
        (
            "Costs must be grouped under the project of each run's newest link.\n"
            "\n"
            "Test cases:\n"
            "- A run moved from one project to another counts once, under the newest project.\n"
            "- Two links written with the same created_at resolve to the one appended last.\n"
            "- A run with no project comes back with null project fields, and is never dropped.\n"
            "- The vendor name is the provider title-cased, and the month is the UTC month the run finished.\n"
            "- A run finished exactly at `end` and another organization's run are left out.\n"
            "- Every run in the window is listed with its project handle."
        )
    )
    def test_groups_costs_under_each_runs_newest_project(
        self, client: TestClient, database_session: Session, project_service: MockProjectRoutes
    ) -> None:
        utc = ZoneInfo("UTC")
        stamp = datetime(2026, 10, 20, tzinfo=utc)
        router = make_project(project_service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation")
        blog = make_project(project_service, "blog-xyz", SpendType.INTERNAL, "blog")
        google = make_project(project_service, "google-report-xyz", SpendType.CUSTOMER, "report")

        moved = _tagged_run(database_session, started_at=datetime(2026, 10, 5, tzinfo=utc), amounts=[1.0, 0.5])
        link_run_project(database_session, moved, router, created_at=stamp)
        link_run_project(database_session, moved, blog, created_at=stamp + timedelta(minutes=5))
        tied = _tagged_run(database_session, started_at=datetime(2026, 10, 6, tzinfo=utc), amounts=[2.0])
        link_run_project(database_session, tied, google, created_at=stamp)
        link_run_project(database_session, tied, router, created_at=stamp)
        untagged = _tagged_run(
            database_session, started_at=datetime(2026, 10, 7, tzinfo=utc), amounts=[0.25], provider="modal"
        )
        november = _tagged_run(database_session, started_at=datetime(2026, 11, 1, tzinfo=utc), amounts=[9.0])
        link_run_project(database_session, november, router, created_at=stamp)
        other_org = Org(id=uuid4(), name="other-tenant")
        database_session.add(other_org)
        database_session.commit()
        foreign = _tagged_run(
            database_session, started_at=datetime(2026, 10, 8, tzinfo=utc), amounts=[9.0], org_id=other_org.id
        )
        foreign_project = make_project(
            project_service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation", org_id=other_org.id
        )
        link_run_project(database_session, foreign, foreign_project, created_at=stamp)
        database_session.commit()

        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["start"] == "2026-10-01T00:00:00Z"
        assert body["end"] == "2026-11-01T00:00:00Z"
        assert body["rows"] == [
            {
                "month": "2026-10",
                "vendor": "Daytona",
                "spend_type": "internal",
                "subcategory": "blog",
                "project_handle": "blog-xyz",
                "project_name": "Blog Xyz",
                "customer": None,
                "amount_usd": 1.5,
                "run_count": 1,
            },
            {
                "month": "2026-10",
                "vendor": "Daytona",
                "spend_type": "development",
                "subcategory": "new_benchmark_generation",
                "project_handle": "router-bench",
                "project_name": "Router Bench",
                "customer": None,
                "amount_usd": 2.0,
                "run_count": 1,
            },
            {
                "month": "2026-10",
                "vendor": "Modal",
                "spend_type": None,
                "subcategory": None,
                "project_handle": None,
                "project_name": None,
                "customer": None,
                "amount_usd": 0.25,
                "run_count": 1,
            },
        ]
        assert {(run["run_id"], run["project_handle"]) for run in body["runs"]} == {
            (str(moved), "blog-xyz"),
            (str(tied), "router-bench"),
            (str(untagged), None),
        }

    @pytest.mark.description(
        (
            "A run that spent inside the window must be listed with its project, whatever day it started.\n"
            "\n"
            "Test cases:\n"
            "- Started before the window, finished inside it: listed.\n"
            "- Started before the window, finished after it: listed.\n"
            "- Started before the window, still running: listed.\n"
            "- Started and finished before the window: left out.\n"
            "- Started on the window's end date: left out.\n"
            "- Stopped with no finish, started before the window: left out.\n"
            "- Stopped with no finish, started inside the window: listed."
        )
    )
    def test_runs_lists_every_run_alive_during_the_window(
        self, client: TestClient, database_session: Session, project_service: MockProjectRoutes
    ) -> None:
        utc = ZoneInfo("UTC")
        stamp = datetime(2026, 10, 20, tzinfo=utc)
        project = make_project(project_service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation")
        finished_inside = _tagged_run(
            database_session,
            started_at=datetime(2026, 9, 28, tzinfo=utc),
            finished_at=datetime(2026, 10, 2, tzinfo=utc),
            amounts=[],
        )
        link_run_project(database_session, finished_inside, project, created_at=stamp)
        finished_after = _tagged_run(
            database_session,
            started_at=datetime(2026, 9, 29, tzinfo=utc),
            finished_at=datetime(2026, 11, 3, tzinfo=utc),
            amounts=[],
        )
        still_running = _tagged_run(
            database_session,
            started_at=datetime(2026, 9, 30, tzinfo=utc),
            status=BenchmarkStatus.IN_PROGRESS,
            amounts=[],
        )
        _tagged_run(
            database_session,
            started_at=datetime(2026, 9, 1, tzinfo=utc),
            finished_at=datetime(2026, 9, 30, 23, 59, tzinfo=utc),
            amounts=[],
        )
        _tagged_run(database_session, started_at=datetime(2026, 11, 1, tzinfo=utc), amounts=[])
        _tagged_run(
            database_session,
            started_at=datetime(2026, 9, 27, tzinfo=utc),
            status=BenchmarkStatus.STOPPED,
            amounts=[],
        )
        stopped_inside = _tagged_run(
            database_session,
            started_at=datetime(2026, 10, 3, tzinfo=utc),
            status=BenchmarkStatus.STOPPED,
            amounts=[],
        )
        database_session.commit()

        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        assert {(run["run_id"], run["project_handle"]) for run in response.json()["runs"]} == {
            (str(finished_inside), "router-bench"),
            (str(finished_after), None),
            (str(still_running), None),
            (str(stopped_inside), None),
        }

    @pytest.mark.description(
        (
            "Costs must be placed by `usage_end`, else by `finished_at`, with `start` included and `end` excluded.\n"
            "\n"
            "Test cases:\n"
            "- A usage end exactly at `start` is counted and one exactly at `end` is left out.\n"
            "- With no usage end, a run finished exactly at `start` is counted and one finished exactly at `end` is left out.\n"
            "- A usage end at 23:30 UTC on the last day of October counts in 2026-10, whatever the host timezone.\n"
            "- Each run starts, finishes and ends its usage on different days, so only the intended date can place it.\n"
            "- Every returned start time carries an explicit UTC offset."
        )
    )
    def test_window_edges_are_half_open_for_usage_end_and_for_the_finish_fallback(
        self, client: TestClient, database_session: Session
    ) -> None:
        utc = ZoneInfo("UTC")
        october = datetime(2026, 10, 1, tzinfo=utc)
        november = datetime(2026, 11, 1, tzinfo=utc)
        _tagged_run(
            database_session,
            started_at=datetime(2026, 9, 27, tzinfo=utc),
            finished_at=datetime(2026, 11, 3, tzinfo=utc),
            usage_end=october,
            amounts=[1.0],
        )
        _tagged_run(
            database_session,
            started_at=datetime(2026, 10, 20, tzinfo=utc),
            finished_at=datetime(2026, 10, 28, tzinfo=utc),
            usage_end=november,
            amounts=[2.0],
        )
        _tagged_run(
            database_session,
            started_at=datetime(2026, 9, 30, tzinfo=utc),
            finished_at=datetime(2026, 11, 2, tzinfo=utc),
            usage_end=datetime(2026, 10, 31, 23, 30, tzinfo=utc),
            amounts=[4.0],
        )
        _tagged_run(database_session, started_at=datetime(2026, 9, 29, tzinfo=utc), finished_at=october, amounts=[8.0])
        _tagged_run(
            database_session, started_at=datetime(2026, 10, 15, tzinfo=utc), finished_at=november, amounts=[16.0]
        )

        in_october = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )
        in_november = client.get(
            "/costs/by-project",
            params={"start": "2026-11-01", "end": "2026-12-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert in_october.status_code == 200, in_october.text
        assert in_november.status_code == 200, in_november.text
        assert [(row["month"], row["amount_usd"]) for row in in_october.json()["rows"]] == [("2026-10", 13.0)]
        assert [(row["month"], row["amount_usd"]) for row in in_november.json()["rows"]] == [("2026-11", 18.0)]
        assert all(run["started_at"].endswith("Z") for run in in_october.json()["runs"])

    @pytest.mark.description(
        (
            "`runs` must list each run started in the window or holding a cost placed in it, so every run behind `rows` can be joined.\n"
            "\n"
            "Test cases:\n"
            "- A run started in September whose costs ended in October is listed for October, once, with its project.\n"
            "- A run started in October with no cost row is listed.\n"
            "- A run started in September whose cost ended in September is left out."
        )
    )
    def test_runs_lists_every_run_with_a_cost_in_the_window(
        self,
        client: TestClient,
        database_session: Session,
        project_service: MockProjectRoutes,
    ) -> None:
        utc = ZoneInfo("UTC")
        router = make_project(project_service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation")
        carried_in = _tagged_run(
            database_session,
            started_at=datetime(2026, 9, 28, tzinfo=utc),
            finished_at=datetime(2026, 10, 2, tzinfo=utc),
            usage_end=datetime(2026, 10, 1, 6, tzinfo=utc),
            amounts=[1.0, 2.0],
        )
        link_run_project(database_session, carried_in, router, created_at=datetime(2026, 9, 28, tzinfo=utc))
        idle = make_benchmark(
            status=BenchmarkStatus.IN_PROGRESS,
            started_at=datetime(2026, 10, 10, tzinfo=utc),
            started_by_email="alice@vals.ai",
            session=database_session,
        )
        _tagged_run(
            database_session,
            started_at=datetime(2026, 9, 20, tzinfo=utc),
            finished_at=datetime(2026, 9, 22, tzinfo=utc),
            usage_end=datetime(2026, 9, 21, tzinfo=utc),
            amounts=[4.0],
        )
        database_session.commit()

        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        assert [(run["run_id"], run["project_handle"]) for run in response.json()["runs"]] == [
            (str(carried_in), "router-bench"),
            (str(idle.id), None),
        ]

    @pytest.mark.description(
        (
            "A run must never show the details of a project that belongs to another organization.\n"
            "\n"
            "Test cases:\n"
            "- A run linked to another org's project comes back untagged, with null project fields."
        )
    )
    def test_a_link_to_another_orgs_project_is_not_attributed(
        self,
        client: TestClient,
        database_session: Session,
        project_service: MockProjectRoutes,
    ) -> None:
        utc = ZoneInfo("UTC")
        other_org = Org(id=uuid4(), name="other-tenant")
        database_session.add(other_org)
        database_session.commit()
        foreign_project = make_project(
            project_service, "secret-project", SpendType.CUSTOMER, "report", org_id=other_org.id
        )
        run = _tagged_run(database_session, started_at=datetime(2026, 10, 5, tzinfo=utc), amounts=[1.0])
        link_run_project(database_session, run, foreign_project, created_at=datetime(2026, 10, 6, tzinfo=utc))
        database_session.commit()

        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert [row["project_handle"] for row in body["rows"]] == [None]
        assert [run["project_handle"] for run in body["runs"]] == [None]

    @pytest.mark.description(
        (
            "Archiving a project must leave its cost rows and run list exactly as they were.\n"
            "\n"
            "Test cases:\n"
            "- The by-project response is identical before and after the project is archived."
        )
    )
    def test_an_archived_projects_spend_keeps_its_totals(
        self, client: TestClient, database_session: Session, project_service: MockProjectRoutes
    ) -> None:
        utc = ZoneInfo("UTC")
        router = make_project(project_service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation")
        run = _tagged_run(database_session, started_at=datetime(2026, 10, 5, tzinfo=utc), amounts=[1.0, 0.5])
        link_run_project(database_session, run, router, created_at=datetime(2026, 10, 5, tzinfo=utc))
        database_session.commit()
        window = {"start": "2026-10-01", "end": "2026-11-01"}
        before = client.get("/costs/by-project", params=window, headers={"X-Api-Key": "fake"}).json()

        archived = client.post("/projects/router-bench/archive", headers={"X-Api-Key": "fake"})
        after = client.get("/costs/by-project", params=window, headers={"X-Api-Key": "fake"}).json()

        assert archived.status_code == 200, archived.text
        assert [(row["project_handle"], row["amount_usd"]) for row in before["rows"]] == [("router-bench", 1.5)]
        assert after == before

    @pytest.mark.description(
        (
            "A sandbox cost must count in the month its usage ended, not the month its run started or finished.\n"
            "\n"
            "Test cases:\n"
            "- A sandbox whose usage ended in October, on a run that finished in November, lands in October only.\n"
            "- A sandbox whose usage ended in October, on a run started in September, lands in October under its project.\n"
            "- The run started in September is listed in `runs`, since its cost counts in October."
        )
    )
    def test_a_sandbox_is_placed_in_the_month_its_usage_ended(
        self,
        client: TestClient,
        database_session: Session,
        project_service: MockProjectRoutes,
    ) -> None:
        utc = ZoneInfo("UTC")
        router = make_project(project_service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation")
        ran_into_november = _tagged_run(
            database_session,
            started_at=datetime(2026, 10, 30, tzinfo=utc),
            finished_at=datetime(2026, 11, 2, tzinfo=utc),
            usage_end=datetime(2026, 10, 31, 23, tzinfo=utc),
            amounts=[1.0],
        )
        started_in_september = _tagged_run(
            database_session,
            started_at=datetime(2026, 9, 29, tzinfo=utc),
            finished_at=datetime(2026, 10, 3, tzinfo=utc),
            usage_end=datetime(2026, 10, 2, tzinfo=utc),
            amounts=[2.0],
        )
        link_run_project(database_session, started_in_september, router, created_at=datetime(2026, 9, 29, tzinfo=utc))
        database_session.commit()

        october = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )
        november = client.get(
            "/costs/by-project",
            params={"start": "2026-11-01", "end": "2026-12-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert october.status_code == 200, october.text
        assert november.status_code == 200, november.text
        assert [
            (row["month"], row["project_handle"], row["amount_usd"], row["run_count"]) for row in october.json()["rows"]
        ] == [("2026-10", None, 1.0, 1), ("2026-10", "router-bench", 2.0, 1)]
        assert [run["run_id"] for run in october.json()["runs"]] == [str(started_in_september), str(ran_into_november)]
        assert november.json()["rows"] == []

    @pytest.mark.description(
        (
            "A cost row saved before usage ends were recorded must be placed by its run's `finished_at`.\n"
            "\n"
            "Test cases:\n"
            "- A run started in October and finished in November, with no usage end, counts in November only."
        )
    )
    def test_a_cost_with_no_usage_end_falls_back_to_its_runs_finish(
        self, client: TestClient, database_session: Session
    ) -> None:
        utc = ZoneInfo("UTC")
        _tagged_run(
            database_session,
            started_at=datetime(2026, 10, 31, 22, tzinfo=utc),
            finished_at=datetime(2026, 11, 1, 1, tzinfo=utc),
            amounts=[3.0],
        )

        october = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )
        november = client.get(
            "/costs/by-project",
            params={"start": "2026-11-01", "end": "2026-12-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert october.status_code == 200, october.text
        assert november.status_code == 200, november.text
        assert october.json()["rows"] == []
        assert [(row["month"], row["amount_usd"]) for row in november.json()["rows"]] == [("2026-11", 3.0)]

    @pytest.mark.description(
        (
            "A cost row with neither a usage end nor a run finish must still count, placed by its run's `started_at`.\n"
            "\n"
            "Test cases:\n"
            "- A run stopped without a `finished_at`, started in October, counts its cost in October.\n"
            "- A run started in October whose usage ended in November counts in November alongside it."
        )
    )
    def test_a_cost_with_no_usage_end_on_a_run_with_no_finish_falls_back_to_its_start(
        self, client: TestClient, database_session: Session
    ) -> None:
        utc = ZoneInfo("UTC")
        benchmark = make_benchmark(
            status=BenchmarkStatus.STOPPED,
            started_at=datetime(2026, 10, 30, 12, tzinfo=utc),
            started_by_email="alice@vals.ai",
            session=database_session,
        )
        database_session.add(
            RunCost(
                benchmark_id=benchmark.id,
                cost_type="sandbox",
                source_id=str(uuid4()),
                provider="daytona",
                amount_usd=0.5,
            )
        )
        database_session.commit()
        _tagged_run(
            database_session,
            started_at=datetime(2026, 10, 29, tzinfo=utc),
            finished_at=datetime(2026, 11, 3, tzinfo=utc),
            usage_end=datetime(2026, 11, 2, tzinfo=utc),
            amounts=[1.0],
        )

        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": "2026-12-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert benchmark.finished_at is None
        assert response.status_code == 200, response.text
        assert [(row["month"], row["amount_usd"]) for row in response.json()["rows"]] == [
            ("2026-10", 0.5),
            ("2026-11", 1.0),
        ]

    @pytest.mark.description(
        (
            "A window given as UTC datetimes must count costs by their exact usage end, not by their date.\n"
            "\n"
            "Test cases:\n"
            "- A usage end at 21:30 UTC is counted in a window starting at 21:00 UTC that day.\n"
            "- A usage end at 20:30 UTC the same day is left out.\n"
            "- The response echoes the window's exact start and end."
        )
    )
    def test_a_datetime_window_places_costs_by_the_hour(self, client: TestClient, database_session: Session) -> None:
        utc = ZoneInfo("UTC")
        _tagged_run(
            database_session,
            started_at=datetime(2026, 10, 30, 19, tzinfo=utc),
            finished_at=datetime(2026, 10, 30, 22, tzinfo=utc),
            usage_end=datetime(2026, 10, 30, 21, 30, tzinfo=utc),
            amounts=[1.0],
        )
        _tagged_run(
            database_session,
            started_at=datetime(2026, 10, 30, 19, tzinfo=utc),
            finished_at=datetime(2026, 10, 30, 22, tzinfo=utc),
            usage_end=datetime(2026, 10, 30, 20, 30, tzinfo=utc),
            amounts=[2.0],
        )

        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-30T21:00:00Z", "end": "2026-10-31T21:00:00Z"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["start"] == "2026-10-30T21:00:00Z"
        assert body["end"] == "2026-10-31T21:00:00Z"
        assert [(row["month"], row["amount_usd"]) for row in body["rows"]] == [("2026-10", 1.0)]

    @pytest.mark.description(
        (
            "A window given as dates must keep meaning midnight UTC to midnight UTC.\n"
            "\n"
            "Test cases:\n"
            "- Usage ends at 20:30 and 21:30 UTC on the same day are both counted in that day's date window.\n"
            "- The response echoes each date as midnight UTC."
        )
    )
    def test_a_date_window_counts_the_whole_utc_day(self, client: TestClient, database_session: Session) -> None:
        utc = ZoneInfo("UTC")
        for usage_end, amount in [
            (datetime(2026, 10, 30, 21, 30, tzinfo=utc), 1.0),
            (datetime(2026, 10, 30, 20, 30, tzinfo=utc), 2.0),
        ]:
            _tagged_run(
                database_session,
                started_at=datetime(2026, 10, 30, 19, tzinfo=utc),
                finished_at=datetime(2026, 10, 30, 22, tzinfo=utc),
                usage_end=usage_end,
                amounts=[amount],
            )

        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-30", "end": "2026-10-31"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["start"] == "2026-10-30T00:00:00Z"
        assert body["end"] == "2026-10-31T00:00:00Z"
        assert [(row["month"], row["amount_usd"], row["run_count"]) for row in body["rows"]] == [("2026-10", 3.0, 2)]

    @pytest.mark.description(
        (
            "A datetime bound with no timezone must be rejected rather than guessed.\n"
            "\n"
            "Test cases:\n"
            "- A naive `start` receives 422 on every cost endpoint.\n"
            "- A naive `end` receives 422 on every cost endpoint."
        )
    )
    @pytest.mark.parametrize(
        "path", ["/costs/by-email", "/costs/by-project", "/costs/project-daytona", "/costs/project-sandbox"]
    )
    @pytest.mark.parametrize(
        ("start", "end"),
        [("2026-10-30T21:00:00", "2026-10-31T21:00:00Z"), ("2026-10-30T21:00:00Z", "2026-10-31T21:00:00")],
    )
    def test_a_datetime_without_a_timezone_is_rejected(
        self, client: TestClient, path: str, start: str, end: str
    ) -> None:
        response = client.get(path, params={"start": start, "end": end}, headers={"X-Api-Key": "fake"})

        assert response.status_code == 422

    @pytest.mark.description(
        (
            "A window whose end is not after its start must be rejected.\n"
            "\n"
            "Test cases:\n"
            "- Equal start and end receives 422, given as dates or as a date and its midnight UTC.\n"
            "- An end before the start receives 422, given as a date or as a datetime."
        )
    )
    @pytest.mark.parametrize("end", ["2026-10-01", "2026-09-30", "2026-10-01T00:00:00Z", "2026-09-30T23:59:00Z"])
    def test_end_must_follow_start(self, client: TestClient, end: str) -> None:
        response = client.get(
            "/costs/by-project",
            params={"start": "2026-10-01", "end": end},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 422

    @pytest.mark.description(
        (
            "Project cost totals must require authentication.\n"
            "\n"
            "Test cases:\n"
            "- A request without a bearer session receives 401."
        )
    )
    def test_costs_by_project_unauth_401(self, client: TestClient) -> None:
        response = client.get("/costs/by-project", params={"start": "2026-10-01", "end": "2026-11-01"})

        assert response.status_code == 401


_GOLDEN_WINDOW = {"start": "2026-10-01", "end": "2026-11-01"}
_GOLDEN_SANDBOX_USD_BY_PROJECT: dict[str | None, float] = {
    "blog-xyz": 3.5,
    "google-report-xyz": 5.5,
    "router-bench": 11.0,
    None: 0.25,
}


def _seed_golden_cost_org(session: Session, project_service: MockProjectRoutes) -> None:
    utc = ZoneInfo("UTC")
    router = make_project(project_service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation")
    blog = make_project(project_service, "blog-xyz", SpendType.INTERNAL, "blog")
    google = make_project(project_service, "google-report-xyz", SpendType.CUSTOMER, "report")
    october_5 = datetime(2026, 10, 5, tzinfo=utc)

    router_run = _tagged_run(
        session, started_at=october_5, usage_end=datetime(2026, 10, 6, tzinfo=utc), amounts=[1.25, 0.75]
    )
    link_run_project(session, router_run, router, created_at=october_5)
    moved_run = _tagged_run(
        session, started_at=october_5, usage_end=datetime(2026, 10, 20, tzinfo=utc), amounts=[3.0, 0.5]
    )
    link_run_project(session, moved_run, router, created_at=october_5)
    link_run_project(session, moved_run, blog, created_at=october_5 + timedelta(hours=1))
    google_daytona = _tagged_run(
        session, started_at=october_5, usage_end=datetime(2026, 10, 31, 23, tzinfo=utc), amounts=[4.0]
    )
    link_run_project(session, google_daytona, google, created_at=october_5)
    google_modal = _tagged_run(
        session,
        started_at=october_5,
        usage_end=datetime(2026, 10, 12, tzinfo=utc),
        amounts=[1.5],
        provider="modal",
    )
    link_run_project(session, google_modal, google, created_at=october_5)
    _tagged_run(session, started_at=october_5, usage_end=datetime(2026, 10, 7, tzinfo=utc), amounts=[0.25])
    carried_in = _tagged_run(
        session,
        started_at=datetime(2026, 9, 28, tzinfo=utc),
        finished_at=datetime(2026, 10, 2, tzinfo=utc),
        amounts=[9.0],
    )
    link_run_project(session, carried_in, router, created_at=datetime(2026, 9, 28, tzinfo=utc))
    september = _tagged_run(
        session,
        started_at=datetime(2026, 9, 20, tzinfo=utc),
        usage_end=datetime(2026, 9, 30, 23, tzinfo=utc),
        amounts=[16.0],
    )
    link_run_project(session, september, router, created_at=datetime(2026, 9, 20, tzinfo=utc))
    november = _tagged_run(
        session,
        started_at=datetime(2026, 10, 30, tzinfo=utc),
        usage_end=datetime(2026, 11, 1, tzinfo=utc),
        amounts=[32.0],
    )
    link_run_project(session, november, blog, created_at=datetime(2026, 10, 30, tzinfo=utc))
    other_org = Org(id=uuid4(), name="other-tenant")
    session.add(other_org)
    session.commit()
    foreign_project = make_project(
        project_service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation", org_id=other_org.id
    )
    foreign = _tagged_run(
        session,
        started_at=october_5,
        usage_end=datetime(2026, 10, 6, tzinfo=utc),
        amounts=[64.0],
        org_id=other_org.id,
    )
    link_run_project(session, foreign, foreign_project, created_at=october_5)
    session.commit()


@pytest.mark.description("A fixed fixture org pins the sandbox dollars `/costs/by-project` reports per project.")
class TestCostsByProjectGolden:
    @pytest.mark.description(
        (
            "The golden org's October sandbox dollars per project must match the pinned totals.\n"
            "\n"
            "Test cases:\n"
            "- Each project sums its sandbox rows across vendors and runs.\n"
            "- A run moved between projects counts only under its newest project.\n"
            "- A run with no project sums under a null handle.\n"
            "- A row with no usage end that its run finished inside the window is counted.\n"
            "- Rows whose usage ended before `start` or exactly at `end` are left out.\n"
            "- Another org's rows under a project with the same handle are left out.\n"
            "- Each counted run is listed under its newest project."
        )
    )
    def test_sandbox_dollars_per_project_match_the_golden_totals(
        self, client: TestClient, database_session: Session, project_service: MockProjectRoutes
    ) -> None:
        _seed_golden_cost_org(database_session, project_service)

        response = client.get("/costs/by-project", params=_GOLDEN_WINDOW, headers={"X-Api-Key": "fake"})

        assert response.status_code == 200, response.text
        totals: dict[str | None, float] = {}
        for row in response.json()["rows"]:
            totals[row["project_handle"]] = totals.get(row["project_handle"], 0.0) + row["amount_usd"]
        assert totals == _GOLDEN_SANDBOX_USD_BY_PROJECT
        assert sorted(run["project_handle"] or "" for run in response.json()["runs"]) == [
            "",
            "blog-xyz",
            "blog-xyz",
            "google-report-xyz",
            "google-report-xyz",
            "router-bench",
            "router-bench",
        ]


@pytest.mark.description("Daytona sandbox totals per project for runs started in a window.")
class TestProjectDaytonaCosts:
    @pytest.mark.description(
        (
            "A window given as UTC datetimes must count runs by their exact start, not by their date.\n"
            "\n"
            "Test cases:\n"
            "- A run started at 21:30 UTC is counted in a window starting at 21:00 UTC that day.\n"
            "- A run started at 20:30 UTC the same day is left out.\n"
            "- The response echoes the window's exact start and end."
        )
    )
    def test_a_datetime_window_counts_runs_by_the_hour(
        self, client: TestClient, database_session: Session, project_service: MockProjectRoutes
    ) -> None:
        utc = ZoneInfo("UTC")
        project = make_project(project_service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation")
        for started_at, amount in [
            (datetime(2026, 10, 30, 21, 30, tzinfo=utc), 1.0),
            (datetime(2026, 10, 30, 20, 30, tzinfo=utc), 2.0),
        ]:
            run = _tagged_run(database_session, started_at=started_at, amounts=[amount])
            link_run_project(database_session, run, project, created_at=started_at)
        database_session.commit()

        response = client.get(
            "/costs/project-daytona",
            params={"start": "2026-10-30T21:00:00Z", "end": "2026-10-31T21:00:00Z"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["start"] == "2026-10-30T21:00:00+00:00"
        assert body["end"] == "2026-10-31T21:00:00+00:00"
        assert body["rows"] == [{"project_id": str(project.id), "total_usd": 1.0}]

    @pytest.mark.description(
        (
            "A window given as dates must keep meaning midnight UTC to midnight UTC.\n"
            "\n"
            "Test cases:\n"
            "- Runs started at 20:30 and 21:30 UTC on the same day are both counted in that day's date window.\n"
            "- The response echoes each date as given."
        )
    )
    def test_a_date_window_counts_the_whole_utc_day(
        self, client: TestClient, database_session: Session, project_service: MockProjectRoutes
    ) -> None:
        utc = ZoneInfo("UTC")
        project = make_project(project_service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation")
        for started_at, amount in [
            (datetime(2026, 10, 30, 21, 30, tzinfo=utc), 1.0),
            (datetime(2026, 10, 30, 20, 30, tzinfo=utc), 2.0),
        ]:
            run = _tagged_run(database_session, started_at=started_at, amounts=[amount])
            link_run_project(database_session, run, project, created_at=started_at)
        database_session.commit()

        response = client.get(
            "/costs/project-daytona",
            params={"start": "2026-10-30", "end": "2026-10-31"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["start"] == "2026-10-30"
        assert body["end"] == "2026-10-31"
        assert body["rows"] == [{"project_id": str(project.id), "total_usd": 3.0}]


class TestProjectSandboxCosts:
    @pytest.mark.description(
        (
            "Every priced sandbox provider must get its own total per project, while the Daytona route keeps Daytona.\n"
            "\n"
            "Test cases:\n"
            "- A project with a Daytona run and a Modal run gets one row per provider.\n"
            "- The Daytona route still returns only the Daytona total for that project."
        )
    )
    def test_each_provider_gets_its_own_row(
        self, client: TestClient, database_session: Session, project_service: MockProjectRoutes
    ) -> None:
        utc = ZoneInfo("UTC")
        started_at = datetime(2026, 10, 30, 21, 30, tzinfo=utc)
        project = make_project(project_service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation")
        for provider, amounts in [("daytona", [1.0, 0.5]), ("modal", [32.69])]:
            run = _tagged_run(database_session, started_at=started_at, amounts=amounts, provider=provider)
            link_run_project(database_session, run, project, created_at=started_at)
        database_session.commit()
        params = {"start": "2026-10-30", "end": "2026-10-31"}

        sandbox = client.get("/costs/project-sandbox", params=params, headers={"X-Api-Key": "fake"})
        daytona = client.get("/costs/project-daytona", params=params, headers={"X-Api-Key": "fake"})

        assert sandbox.status_code == 200, sandbox.text
        assert sandbox.json()["rows"] == [
            {"project_id": str(project.id), "provider": "daytona", "total_usd": 1.5},
            {"project_id": str(project.id), "provider": "modal", "total_usd": 32.69},
        ]
        assert daytona.json()["rows"] == [{"project_id": str(project.id), "total_usd": 1.5}]


def _sandbox_cost(
    session: Session,
    run_id: UUID,
    source_id: str,
    amount_usd: float,
    usage_end: datetime | None,
    provider: str = "daytona",
) -> None:
    session.add(
        RunCost(
            benchmark_id=run_id,
            cost_type="sandbox",
            source_id=source_id,
            provider=provider,
            amount_usd=amount_usd,
            usage_end=usage_end,
        )
    )
    session.commit()


def _seed_november_sandbox_rows(session: Session, service: MockProjectRoutes) -> dict[str, UUID]:
    utc = ZoneInfo("UTC")
    blog = make_project(service, "blog-xyz", SpendType.INTERNAL, "blog")
    router = make_project(service, "router-bench", SpendType.DEVELOPMENT, "new_benchmark_generation")
    tagged = _tagged_run(
        session,
        started_at=datetime(2026, 10, 20, tzinfo=utc),
        finished_at=datetime(2026, 11, 3, 12, tzinfo=utc),
        amounts=[],
    )
    link_run_project(session, tagged, blog, created_at=datetime(2026, 10, 19, tzinfo=utc))
    link_run_project(session, tagged, router, created_at=datetime(2026, 10, 20, tzinfo=utc))
    session.commit()
    _sandbox_cost(session, tagged, "sb-october", 4.0, datetime(2026, 11, 2, 3, tzinfo=utc))
    _sandbox_cost(session, tagged, "sb-a:2026-11-01T05:00:00+00:00", 0.5, datetime(2026, 11, 1, 5, 59, tzinfo=utc))
    _sandbox_cost(session, tagged, "sb-a:2026-11-04T23:00:00+00:00", 0.25, datetime(2026, 11, 4, 23, 59, tzinfo=utc))
    _sandbox_cost(session, tagged, "sb-a:2026-11-06T00:00:00+00:00", 9.0, datetime(2026, 11, 6, tzinfo=utc))
    _sandbox_cost(session, tagged, "sb-before", 9.0, datetime(2026, 10, 31, 23, 59, tzinfo=utc))
    _sandbox_cost(session, tagged, "modal-run", 9.0, datetime(2026, 11, 2, tzinfo=utc), provider="modal")
    untagged = _tagged_run(
        session,
        started_at=datetime(2026, 11, 2, tzinfo=utc),
        finished_at=datetime(2026, 11, 3, 8, tzinfo=utc),
        amounts=[],
    )
    _sandbox_cost(session, untagged, "sb-no-usage-end", 1.5, None)
    stopped = make_benchmark(
        status=BenchmarkStatus.STOPPED,
        started_at=datetime(2026, 11, 5, 10, tzinfo=utc),
        started_by_email="alice@vals.ai",
        session=session,
    )
    _sandbox_cost(session, stopped.id, "sb-stopped", 2.0, None)
    other_org = Org(id=uuid4(), name="other-tenant")
    session.add(other_org)
    session.commit()
    foreign = _tagged_run(session, started_at=datetime(2026, 11, 1, tzinfo=utc), amounts=[], org_id=other_org.id)
    _sandbox_cost(session, foreign, "sb-foreign:2026-11-02T00:00:00+00:00", 9.0, datetime(2026, 11, 2, tzinfo=utc))
    return {"tagged": tagged, "untagged": untagged, "stopped": stopped.id}


@pytest.mark.description("One entry per sandbox cost row placed in a window, with each row's sweep kind and UTC day.")
class TestSandboxCostRows:
    @pytest.mark.description(
        (
            "Each sandbox cost row must come back once, placed by the same rule as `/by-project`, with its sweep kind.\n"
            "\n"
            "Test cases:\n"
            "- A source id with a colon is hourly. One without is lifetime.\n"
            "- A lifetime row for October usage whose usage ended on Nov 2 is placed on Nov 2.\n"
            "- A row with no usage end is placed at its run's finish. A stopped run's row is placed at its start.\n"
            "- Rows placed before `start` or exactly at `end`, Modal rows, and another org's rows are left out.\n"
            "- A run moved from one project to another comes back once per row, under its newest handle."
        )
    )
    def test_each_row_comes_back_with_its_kind_and_day(
        self, client: TestClient, database_session: Session, project_service: MockProjectRoutes
    ) -> None:
        runs = _seed_november_sandbox_rows(database_session, project_service)

        response = client.get(
            "/costs/sandbox-rows",
            params={"provider": "daytona", "start": "2026-11-01", "end": "2026-11-06"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["start"] == "2026-11-01T00:00:00Z"
        assert body["end"] == "2026-11-06T00:00:00Z"
        assert body["provider"] == "daytona"
        assert body["total_rows"] == 5
        assert [
            (
                row["source_id"],
                row["kind"],
                row["placed_at"][:10],
                row["amount_usd"],
                row["run_id"],
                row["project_handle"],
            )
            for row in body["rows"]
        ] == [
            ("sb-a:2026-11-01T05:00:00+00:00", "hourly", "2026-11-01", 0.5, str(runs["tagged"]), "router-bench"),
            ("sb-october", "lifetime", "2026-11-02", 4.0, str(runs["tagged"]), "router-bench"),
            ("sb-no-usage-end", "lifetime", "2026-11-03", 1.5, str(runs["untagged"]), None),
            ("sb-a:2026-11-04T23:00:00+00:00", "hourly", "2026-11-04", 0.25, str(runs["tagged"]), "router-bench"),
            ("sb-stopped", "lifetime", "2026-11-05", 2.0, str(runs["stopped"]), None),
        ]
        assert body["rows"][1]["usage_end"] == "2026-11-02T03:00:00Z"
        assert body["rows"][2]["usage_end"] is None
        assert body["rows"][2]["placed_at"] == "2026-11-03T08:00:00Z"

    @pytest.mark.description(
        (
            "Per-day totals must split hourly from lifetime and add up to `/by-project`'s Daytona amount.\n"
            "\n"
            "Test cases:\n"
            "- Each UTC day in the seeded window has its hourly, lifetime and total amounts.\n"
            "- The day totals sum to the Daytona rows of `/by-project` for the same window and org."
        )
    )
    def test_day_totals_match_by_project(
        self, client: TestClient, database_session: Session, project_service: MockProjectRoutes
    ) -> None:
        _seed_november_sandbox_rows(database_session, project_service)
        params = {"start": "2026-11-01", "end": "2026-11-06"}

        rows = client.get("/costs/sandbox-rows", params=params, headers={"X-Api-Key": "fake"})
        by_project = client.get("/costs/by-project", params=params, headers={"X-Api-Key": "fake"})

        assert rows.status_code == 200, rows.text
        assert by_project.status_code == 200, by_project.text
        assert rows.json()["days"] == [
            {"day": "2026-11-01", "hourly_usd": 0.5, "lifetime_usd": 0.0, "total_usd": 0.5},
            {"day": "2026-11-02", "hourly_usd": 0.0, "lifetime_usd": 4.0, "total_usd": 4.0},
            {"day": "2026-11-03", "hourly_usd": 0.0, "lifetime_usd": 1.5, "total_usd": 1.5},
            {"day": "2026-11-04", "hourly_usd": 0.25, "lifetime_usd": 0.0, "total_usd": 0.25},
            {"day": "2026-11-05", "hourly_usd": 0.0, "lifetime_usd": 2.0, "total_usd": 2.0},
        ]
        daytona = sum(row["amount_usd"] for row in by_project.json()["rows"] if row["vendor"] == "Daytona")
        assert sum(day["total_usd"] for day in rows.json()["days"]) == pytest.approx(daytona)

    @pytest.mark.description(
        (
            "Pages must walk every row once in placement order while the day totals cover the whole window.\n"
            "\n"
            "Test cases:\n"
            "- Pages of two at offsets 0, 2 and 4 together return the five rows of one unpaged call, in order.\n"
            "- Every page reports the same five-row total and the same day totals.\n"
            "- A limit above 20,000 or below 1, or a negative offset, receives 422."
        )
    )
    def test_pages_walk_every_row_once(
        self, client: TestClient, database_session: Session, project_service: MockProjectRoutes
    ) -> None:
        _seed_november_sandbox_rows(database_session, project_service)
        params = {"start": "2026-11-01", "end": "2026-11-06"}
        whole = client.get("/costs/sandbox-rows", params=params, headers={"X-Api-Key": "fake"}).json()

        pages = [
            client.get(
                "/costs/sandbox-rows", params={**params, "limit": 2, "offset": offset}, headers={"X-Api-Key": "fake"}
            ).json()
            for offset in (0, 2, 4)
        ]

        assert [len(page["rows"]) for page in pages] == [2, 2, 1]
        assert [row for page in pages for row in page["rows"]] == whole["rows"]
        assert {page["total_rows"] for page in pages} == {5}
        assert all(page["days"] == whole["days"] for page in pages)
        for bad in ({"limit": 20001}, {"limit": 0}, {"offset": -1}):
            response = client.get("/costs/sandbox-rows", params={**params, **bad}, headers={"X-Api-Key": "fake"})
            assert response.status_code == 422, bad

    @pytest.mark.description(
        "Sandbox cost rows must require authentication.\n\nTest cases:\n- A request without a bearer session receives 401."
    )
    def test_sandbox_rows_unauth_401(self, client: TestClient) -> None:
        response = client.get("/costs/sandbox-rows", params={"start": "2026-11-01", "end": "2026-11-06"})

        assert response.status_code == 401


class TestDaytonaUnmatchedCosts:
    @pytest.mark.description(
        "Unmatched Daytona usage is summed for this organization's hours in [start, end) only.\n"
        "\n"
        "Test cases:\n"
        "- Hours inside the window are summed, with their unpriced sandbox counts.\n"
        "- The hour at end and another organization's hour are left out."
    )
    def test_sums_this_orgs_hours_before_end(self, client: TestClient, database_session: Session) -> None:
        utc = ZoneInfo("UTC")
        other_org = Org(id=uuid4(), name="other-tenant")
        database_session.add(other_org)
        database_session.commit()
        for org_id, hour, amount, unpriced in [
            (TEST_ORG_ID, datetime(2026, 11, 1, 0, tzinfo=utc), 1.25, 1),
            (TEST_ORG_ID, datetime(2026, 11, 1, 23, tzinfo=utc), 0.5, 0),
            (TEST_ORG_ID, datetime(2026, 11, 2, 0, tzinfo=utc), 4.0, 2),
            (other_org.id, datetime(2026, 11, 1, 5, tzinfo=utc), 8.0, 3),
        ]:
            database_session.add(
                SandboxUnmatchedUsage(
                    org_id=org_id,
                    secret_name="daytona",
                    hour=hour,
                    amount_usd=amount,
                    sandboxes=unpriced + 1,
                    unpriced_sandboxes=unpriced,
                )
            )
        database_session.commit()

        response = client.get(
            "/costs/daytona-unmatched",
            params={"start": "2026-11-01", "end": "2026-11-02"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        assert response.json() == {
            "start": "2026-11-01",
            "end": "2026-11-02",
            "total_usd": 1.75,
            "unpriced_sandboxes": 1,
        }


MACHINE_KEY = "machine-key-secret"


class FakeRunSums:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.open_transactions: list[bool] = []
        self.calls: list[dict[str, Any]] = []
        self.runs: dict[UUID, str] = {}
        self.project_key: list[tuple[UUID | str | None, str]] = []
        self.throughs = ["2026-11-16T12:00:00+00:00"]
        self.answer: Callable[[httpx.Request], httpx.Response] | None = None

    def returns(self, runs: dict[UUID, str], project_key: list[tuple[UUID | str | None, str]]) -> None:
        self.runs = runs
        self.project_key = project_key

    def handle(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/admin/managed-auth/machine/usage/run-sums"
        self.open_transactions.append(self.session.in_transaction())
        if self.answer is not None:
            return self.answer(request)
        body = json.loads(request.content)
        self.calls.append(body)
        through = self.throughs[min(len(self.calls), len(self.throughs)) - 1]

        return httpx.Response(
            200,
            json={
                "runs": [
                    {"run_id": run_id, "amount_usd": self.runs.get(UUID(run_id), "0")} for run_id in body["run_ids"]
                ],
                "project_key": [
                    {"run_id": None if run_id is None else str(run_id), "amount_usd": amount}
                    for run_id, amount in (self.project_key if body["project_id"] else [])
                ],
                "through": through,
            },
        )


@pytest.fixture
def fake_run_sums(monkeypatch: pytest.MonkeyPatch, database_session: Session) -> FakeRunSums:
    fake = FakeRunSums(database_session)
    monkeypatch.setattr(gateway_usage, "VALKYRIE_STAGE", "dev")
    monkeypatch.setattr(
        gateway_usage,
        "gateway_client",
        lambda: httpx.AsyncClient(
            base_url="https://model-gateway.example/",
            headers={"Authorization": f"Bearer {MACHINE_KEY}"},
            transport=httpx.MockTransport(fake.handle),
        ),
    )
    return fake


@pytest.mark.description("One project's model spend per UTC month, read from the model Gateway.")
class TestProjectModelCosts:
    @pytest.mark.description(
        (
            "Each run's model dollars land in the UTC month it finished, or started while running.\n"
            "\n"
            "Test cases:\n"
            "- A run started in September and finished on 31 October counts in October.\n"
            "- A run still running counts in the month it started.\n"
            "- A run that finished before the window or moved to another project is not asked about.\n"
            "- Key spend outside any run or on a run with no project counts as unattributed.\n"
            "- Key spend on a run linked to this or another project is not unattributed.\n"
            "- The database transaction is ended before the Gateway is called."
        )
    )
    def test_places_model_dollars_in_each_runs_finish_month(
        self,
        client: TestClient,
        database_session: Session,
        project_service: MockProjectRoutes,
        fake_run_sums: FakeRunSums,
    ) -> None:
        utc = ZoneInfo("UTC")
        project = make_project(project_service, "p")
        other = make_project(project_service, "other")
        finished = _tagged_run(
            database_session,
            started_at=datetime(2026, 9, 30, tzinfo=utc),
            finished_at=datetime(2026, 10, 31, 23, tzinfo=utc),
            amounts=[],
        )
        running = _tagged_run(
            database_session,
            started_at=datetime(2026, 11, 2, tzinfo=utc),
            amounts=[],
            status=BenchmarkStatus.IN_PROGRESS,
        )
        earlier = _tagged_run(database_session, started_at=datetime(2026, 9, 15, tzinfo=utc), amounts=[])
        moved = _tagged_run(database_session, started_at=datetime(2026, 10, 10, tzinfo=utc), amounts=[])
        unlinked = _tagged_run(database_session, started_at=datetime(2026, 10, 12, tzinfo=utc), amounts=[])
        for run in (finished, running, earlier, moved):
            link_run_project(database_session, run, project, created_at=datetime(2026, 9, 1, tzinfo=utc))
        link_run_project(database_session, moved, other, created_at=datetime(2026, 10, 11, tzinfo=utc))
        database_session.commit()
        fake_run_sums.returns(
            runs={finished: "1.50", running: "0.25"},
            project_key=[(None, "0.10"), (unlinked, "0.05"), (finished, "9"), (earlier, "3"), (moved, "4")],
        )

        response = client.get(
            "/costs/project-model",
            params={"project": "p", "start": "2026-10-01", "end": "2026-12-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        assert [call["run_ids"] for call in fake_run_sums.calls] == [[str(finished), str(running)]]

        body = response.json()
        assert body["start"] == "2026-10-01T00:00:00Z"
        assert body["end"] == "2026-12-01T00:00:00Z"
        assert body["months"] == [{"month": "2026-10", "amount_usd": 1.5}, {"month": "2026-11", "amount_usd": 0.25}]
        assert body["unattributed_usd"] == 0.15
        assert body["through"] == "2026-11-16T12:00:00Z"
        assert fake_run_sums.open_transactions == [False]

    @pytest.mark.description(
        (
            "More than 1000 runs are asked about in batches of 1000.\n"
            "\n"
            "Test cases:\n"
            "- Only the first batch carries the project ID, and every batch names the environment.\n"
            "- `through` is the oldest time any batch reports."
        )
    )
    def test_batches_past_1000_runs(
        self,
        client: TestClient,
        database_session: Session,
        project_service: MockProjectRoutes,
        fake_run_sums: FakeRunSums,
    ) -> None:
        started_at = datetime(2026, 10, 5, tzinfo=ZoneInfo("UTC"))
        project = make_project(project_service, "p")
        runs = [make_benchmark(started_at=started_at) for _ in range(1001)]
        database_session.add_all(runs)
        database_session.flush()
        database_session.add_all([RunProject(benchmark_id=run.id, project_id=project.id) for run in runs])
        database_session.commit()
        fake_run_sums.throughs = ["2026-11-16T11:00:00+00:00", "2026-11-16T12:00:00+00:00"]

        response = client.get(
            "/costs/project-model",
            params={"project": "p", "start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        assert [len(call["run_ids"]) for call in fake_run_sums.calls] == [1000, 1]
        assert [call["project_id"] for call in fake_run_sums.calls] == [str(project.id), None]
        assert {call["project_environment"] for call in fake_run_sums.calls} == {"dev"}
        assert response.json()["through"] == "2026-11-16T11:00:00Z"

    @pytest.mark.description(
        (
            "Another organization's projects and runs stay out of this organization's model spend.\n"
            "\n"
            "Test cases:\n"
            "- A handle only another organization has gets a 404 and no Gateway call.\n"
            "- Another organization's run linked to a project with the same handle is not sent to the Gateway.\n"
            "- Key spend on another organization's linked run or a non-UUID run ID counts as unattributed."
        )
    )
    def test_keeps_other_organizations_out(
        self,
        client: TestClient,
        database_session: Session,
        project_service: MockProjectRoutes,
        fake_run_sums: FakeRunSums,
    ) -> None:
        utc = ZoneInfo("UTC")
        started_at = datetime(2026, 10, 5, tzinfo=utc)
        other_org = Org(id=uuid4(), name="other-tenant")
        database_session.add(other_org)
        database_session.commit()
        project = make_project(project_service, "p")
        foreign_project = make_project(project_service, "p", org_id=other_org.id)
        _ = make_project(project_service, "solo", org_id=other_org.id)
        ours = _tagged_run(database_session, started_at=started_at, amounts=[])
        foreign = _tagged_run(database_session, started_at=started_at, amounts=[], org_id=other_org.id)
        link_run_project(database_session, ours, project, created_at=started_at)
        link_run_project(database_session, foreign, foreign_project, created_at=started_at)
        database_session.commit()
        fake_run_sums.returns(runs={ours: "1"}, project_key=[(foreign, "0.5"), ("external-run", "0.25")])
        window = {"start": "2026-10-01", "end": "2026-11-01"}

        missing = client.get(
            "/costs/project-model", params={"project": "solo", **window}, headers={"X-Api-Key": "fake"}
        )
        calls_after_missing = len(fake_run_sums.calls)
        response = client.get("/costs/project-model", params={"project": "p", **window}, headers={"X-Api-Key": "fake"})

        assert (missing.status_code, missing.json()["detail"]) == (404, "Project not found")
        assert calls_after_missing == 0
        assert response.status_code == 200, response.text
        assert [call["run_ids"] for call in fake_run_sums.calls] == [[str(ours)]]
        assert response.json()["months"] == [{"month": "2026-10", "amount_usd": 1.0}]
        assert response.json()["unattributed_usd"] == 0.75

    @pytest.mark.description("A project with no runs in the window still reports its key's unattributed spend.")
    def test_a_project_without_runs_still_reports_key_spend(
        self, client: TestClient, project_service: MockProjectRoutes, fake_run_sums: FakeRunSums
    ) -> None:
        project = make_project(project_service, "p")
        fake_run_sums.returns(runs={}, project_key=[(None, "2.5")])

        response = client.get(
            "/costs/project-model",
            params={"project": "p", "start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        assert fake_run_sums.calls == [{"run_ids": [], "project_id": str(project.id), "project_environment": "dev"}]
        assert response.json()["months"] == []
        assert response.json()["unattributed_usd"] == 2.5

    @pytest.mark.description(
        (
            "A Gateway that cannot answer gets a 503 and a warning that names only the error type and HTTP status.\n"
            "\n"
            "Test cases:\n"
            "- The Gateway answers 503.\n"
            "- The connection fails.\n"
            "- The body is not the run sums shape.\n"
            "- The Gateway URL is not configured.\n"
            "- The warning carries no header, URL or machine key."
        )
    )
    @pytest.mark.parametrize(
        ("failure", "logged"),
        [
            ("status", "HTTPStatusError status=503"),
            ("connection", "ConnectError status=None"),
            ("malformed", "ValidationError status=None"),
            ("unconfigured", "ValueError status=None"),
        ],
    )
    def test_gateway_failure_returns_503(
        self,
        client: TestClient,
        project_service: MockProjectRoutes,
        fake_run_sums: FakeRunSums,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        failure: str,
        logged: str,
    ) -> None:
        monkeypatch.setattr(costs_api.logger, "handlers", [caplog.handler])
        _ = make_project(project_service, "p")

        def connection_error(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused", request=request)

        answers: dict[str, Callable[[httpx.Request], httpx.Response]] = {
            "status": lambda _request: httpx.Response(503, json={"detail": "Redshift is unavailable"}),
            "connection": connection_error,
            "malformed": lambda _request: httpx.Response(200, json={"runs": "none"}),
        }
        if failure == "unconfigured":
            monkeypatch.setattr(gateway_usage, "gateway_client", project_gateway.gateway_client)
            monkeypatch.delenv("MODEL_GATEWAY_URL", raising=False)
        else:
            fake_run_sums.answer = answers[failure]

        response = client.get(
            "/costs/project-model",
            params={"project": "p", "start": "2026-10-01", "end": "2026-11-01"},
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 503
        assert response.json()["detail"] == "The model Gateway is unavailable"
        [record] = [record for record in caplog.records if record.levelname == "WARNING"]
        assert record.getMessage() == f"Model Gateway is unavailable: {logged}"
        assert MACHINE_KEY not in caplog.text
