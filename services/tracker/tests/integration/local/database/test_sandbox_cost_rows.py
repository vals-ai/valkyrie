from datetime import UTC, date, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import BenchmarkStatus, Org, RunCost
from tracker.domain import SandboxResource
from tracker.repositories.cost import CostRepository, SandboxDayTotalRow


@pytest.mark.description(
    (
        "Postgres must place sandbox rows on their UTC day whatever the session time zone.\n"
        "\n"
        "Test cases:\n"
        "- An hourly row ending 23:30 UTC on Nov 4 counts on Nov 4 under a Los Angeles session.\n"
        "- A lifetime row ending Nov 2 counts as lifetime on Nov 2, and rows page in placement order."
    )
)
def test_sandbox_rows_are_placed_on_their_utc_day(postgres_session: Session) -> None:
    org = Org(id=uuid4(), name="sandbox-rows")
    postgres_session.add(org)
    postgres_session.commit()
    run = make_benchmark(
        status=BenchmarkStatus.FINISHED,
        started_at=datetime(2026, 10, 20, tzinfo=UTC),
        org_id=org.id,
        session=postgres_session,
    )
    for source_id, amount_usd, usage_end in (
        ("sb-a:2026-11-04T23:00:00+00:00", 0.25, datetime(2026, 11, 4, 23, 30, tzinfo=UTC)),
        ("sb-october", 4.0, datetime(2026, 11, 2, 3, tzinfo=UTC)),
    ):
        postgres_session.add(
            RunCost(
                benchmark_id=run.id,
                cost_type="sandbox",
                source_id=source_id,
                provider="daytona",
                amount_usd=amount_usd,
                usage_end=usage_end,
            )
        )
    postgres_session.commit()
    postgres_session.connection().execute(text("SET TIME ZONE 'America/Los_Angeles'"))
    repository = CostRepository(postgres_session)
    window = (org.id, "daytona", datetime(2026, 11, 1, tzinfo=UTC), datetime(2026, 11, 6, tzinfo=UTC))

    days = repository.sandbox_day_totals(*window)
    first_page = repository.sandbox_rows(*window, limit=1, offset=0)
    second_page = repository.sandbox_rows(*window, limit=1, offset=1)

    assert list(days) == [
        SandboxDayTotalRow(day=date(2026, 11, 2), hourly=False, amount_usd=4.0, row_count=1),
        SandboxDayTotalRow(day=date(2026, 11, 4), hourly=True, amount_usd=0.25, row_count=1),
    ]
    assert [row.source_id for row in (*first_page, *second_page)] == ["sb-october", "sb-a:2026-11-04T23:00:00+00:00"]


@pytest.mark.description("Modal cost rows for each attempt of a run count as lifetime rows and name their resource.")
def test_modal_attempt_rows_are_lifetime_rows(postgres_session: Session) -> None:
    org = Org(id=uuid4(), name="modal-rows")
    postgres_session.add(org)
    postgres_session.commit()
    run = make_benchmark(
        status=BenchmarkStatus.FINISHED,
        started_at=datetime(2026, 11, 3, tzinfo=UTC),
        org_id=org.id,
        session=postgres_session,
    )
    repository = CostRepository(postgres_session)
    repository.replace_modal_run_costs(
        benchmark_id=run.id,
        costs={
            ("attempt-1", SandboxResource.CPU): 1.5,
            ("attempt-1", SandboxResource.MEMORY): 0.5,
            (None, SandboxResource.CPU): 0.25,
        },
        usage_end=datetime(2026, 11, 4, 2, tzinfo=UTC),
    )
    postgres_session.commit()

    window = (org.id, "modal", datetime(2026, 11, 1, tzinfo=UTC), datetime(2026, 11, 6, tzinfo=UTC))

    days = repository.sandbox_day_totals(*window)
    rows = repository.sandbox_rows(*window, limit=10, offset=0)

    assert list(days) == [SandboxDayTotalRow(day=date(2026, 11, 4), hourly=False, amount_usd=2.25, row_count=3)]
    assert sorted((row.source_id, row.resource) for row in rows) == sorted(
        [("attempt-1", "cpu"), ("attempt-1", "memory"), (str(run.id), "cpu")]
    )
