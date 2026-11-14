from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlmodel import Session

from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.database.models import BenchmarkStatus, Project, RunCost, RunProject, SpendType
from tracker.repositories import project as project_repository
from tracker.repositories.cost import CostRepository

_WINDOW = (datetime(2026, 11, 1, tzinfo=UTC), datetime(2026, 11, 8, tzinfo=UTC))


class MockProjectList:
    def __init__(self, projects: list[Project]) -> None:
        self._projects = projects

    def list_rows(self, org_id: UUID, include_archived: bool, include_deleted: bool = False) -> list[Project]:
        assert (org_id, include_archived, include_deleted) == (TEST_ORG_ID, True, True)
        return self._projects


def _linked_run(tracker_database: Session, handle: str) -> tuple[Project, UUID]:
    project = Project(org_id=TEST_ORG_ID, handle=handle, name=f"Stale {handle}", spend_type=SpendType.DEVELOPMENT)
    run = make_benchmark(status=BenchmarkStatus.FINISHED, started_at=datetime(2026, 11, 2, tzinfo=UTC))
    tracker_database.add_all([project, run])
    tracker_database.commit()
    tracker_database.add_all(
        [
            RunProject(benchmark_id=run.id, project_id=project.id),
            RunCost(
                benchmark_id=run.id,
                cost_type="sandbox",
                source_id=f"sb-{handle}",
                provider="daytona",
                amount_usd=2.0,
                usage_end=datetime(2026, 11, 3, tzinfo=UTC),
            ),
        ]
    )
    tracker_database.commit()
    return project, run.id


def _costs(tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> tuple[CostRepository, UUID, UUID, UUID]:
    kept, kept_run = _linked_run(tracker_database, "router-bench")
    _, gone_run = _linked_run(tracker_database, "gone-bench")
    served = Project.model_validate(
        kept.model_dump()
        | {"name": "Router bench", "spend_type": SpendType.CUSTOMER, "subcategory": "routing", "customer": "acme"}
    )
    monkeypatch.setattr(project_repository, "project_service", lambda: MockProjectList([served]))
    return CostRepository(tracker_database), kept.id, kept_run, gone_run


class TestProjectCostReads:
    @pytest.mark.description(
        (
            "Cost reports label runs with the project service's names.\n"
            "\n"
            "Test cases:\n"
            "- Run and cost rows carry the service's name, spend type, subcategory and customer.\n"
            "- The service is asked for archived and deleted projects too.\n"
            "- A run whose project the service lacks has no project labels."
        )
    )
    def test_by_project_uses_service_names(self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        repository, _, kept_run, gone_run = _costs(tracker_database, monkeypatch)

        runs, costs = repository.by_project(TEST_ORG_ID, *_WINDOW)

        served = ("Router bench", SpendType.CUSTOMER, "routing", "acme")
        labels = {row.run_id: (row.handle, row.name, row.spend_type, row.subcategory, row.customer) for row in runs}
        assert labels == {kept_run: ("router-bench", *served), gone_run: (None,) * 5}
        cost_labels = {row.run_id: (row.name, row.spend_type, row.subcategory, row.customer) for row in costs}
        assert cost_labels == {kept_run: served, gone_run: (None,) * 4}

    @pytest.mark.description("Sandbox rows and project totals cover only projects the service has.")
    def test_sandbox_costs_use_service_projects(
        self, tracker_database: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        repository, kept_id, kept_run, gone_run = _costs(tracker_database, monkeypatch)

        rows = repository.sandbox_rows(TEST_ORG_ID, "daytona", *_WINDOW, limit=10, offset=0)
        totals = repository.project_sandbox_totals(TEST_ORG_ID, *_WINDOW)

        assert {row.run_id: row.handle for row in rows} == {kept_run: "router-bench", gone_run: None}
        assert [(row.project_id, row.provider, row.total_usd) for row in totals] == [(kept_id, "daytona", 2.0)]
