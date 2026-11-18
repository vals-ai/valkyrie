from collections.abc import Callable
from datetime import date, datetime, timezone
from uuid import UUID

import httpx
import pytest

from valkyrie.sdk import ValkyrieClient


@pytest.mark.description(
    "`by_project` sends `start` and `end` as ISO dates or datetimes and returns the typed spend rows and runs."
)
async def test_by_project_sends_the_window_and_returns_spend(make_client: Callable[..., ValkyrieClient]) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "start": "2026-06-01T00:00:00Z",
                "end": "2026-12-01T12:00:00Z",
                "rows": [
                    {
                        "month": "2026-10",
                        "vendor": "Daytona",
                        "spend_type": "customer",
                        "subcategory": "report",
                        "project_handle": "alpha",
                        "project_name": "Alpha",
                        "customer": "acme",
                        "amount_usd": 2.5,
                        "run_count": 3,
                    }
                ],
                "runs": [
                    {
                        "project_handle": "alpha",
                        "run_id": "00000000-0000-0000-0000-000000000001",
                        "started_at": "2026-10-02T08:00:00Z",
                    }
                ],
            },
        )

    async with make_client(handler) as client:
        costs = await client.costs.by_project(date(2026, 6, 1), datetime(2026, 12, 1, 12, tzinfo=timezone.utc))

    assert [(request.method, request.url.path) for request in requests] == [("GET", "/costs/by-project")]
    assert dict(requests[0].url.params) == {"start": "2026-06-01", "end": "2026-12-01T12:00:00+00:00"}
    assert [(row.project_handle, row.vendor, row.amount_usd) for row in costs.rows] == [("alpha", "Daytona", 2.5)]
    assert [run.run_id for run in costs.runs] == [UUID("00000000-0000-0000-0000-000000000001")]


@pytest.mark.description("`project_model` sends the project and window and returns its model spend by month.")
async def test_project_model_sends_the_project_and_window(make_client: Callable[..., ValkyrieClient]) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "start": "2026-06-01T00:00:00Z",
                "end": "2026-12-01T00:00:00Z",
                "months": [{"month": "2026-10", "amount_usd": 1.25}],
                "unattributed_usd": 6.5,
                "through": "2026-11-16T09:30:00Z",
            },
        )

    async with make_client(handler) as client:
        costs = await client.costs.project_model("alpha", date(2026, 6, 1), date(2026, 12, 1))

    assert [(request.method, request.url.path) for request in requests] == [("GET", "/costs/project-model")]
    assert dict(requests[0].url.params) == {"project": "alpha", "start": "2026-06-01", "end": "2026-12-01"}
    assert ([(month.month, month.amount_usd) for month in costs.months], costs.unattributed_usd) == (
        [("2026-10", 1.25)],
        6.5,
    )
    assert costs.through == datetime(2026, 11, 16, 9, 30, tzinfo=timezone.utc)
