from __future__ import annotations

from datetime import UTC, date, datetime, time
from enum import Enum
from typing import Annotated, Any
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, PlainValidator

from tracker.auth import get_current_org
from tracker.config import AUTH_REQUIRED
from tracker.database.models import Org
from tracker.database.session import database_call
from tracker.repositories.cost import DAYTONA_PROVIDER, CostRepositoryDependency
from tracker.repositories.provider_secret import ProviderSecretRepositoryDependency
from tracker.sandbox_cost import daytona_last_ends
from tracker.secret_access import resolve_secret_name

router = APIRouter(prefix="/costs")


def _parse_window_bound(value: Any) -> date | datetime:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except ValueError:
        pass
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        raise ValueError("a datetime bound must carry a timezone, e.g. 2026-10-30T21:00:00Z")
    return moment


WindowBound = Annotated[date | datetime, PlainValidator(_parse_window_bound, json_schema_input_type=date | datetime)]


def _window_moment(bound: date | datetime) -> datetime:
    utc = ZoneInfo("UTC")
    if isinstance(bound, datetime):
        return bound.astimezone(utc)
    return datetime.combine(bound, time(), tzinfo=utc)


def _window(start: date | datetime, end: date | datetime) -> tuple[datetime, datetime]:
    window_start, window_end = _window_moment(start), _window_moment(end)
    if window_end <= window_start:
        raise HTTPException(status_code=422, detail="end must be after start")
    return window_start, window_end


class EmailCost(BaseModel):
    email: str
    cost_type: str
    provider: str
    amount_usd: float
    run_count: int


class CostsByEmailResponse(BaseModel):
    start: datetime
    end: datetime
    costs: list[EmailCost]


@router.get(
    "/by-email",
    response_model=CostsByEmailResponse,
    description="List each starter's recorded run costs in this organization, per cost type and provider, for runs started in [start, end).",
)
def get_costs_by_email(
    repository: CostRepositoryDependency,
    start: Annotated[
        WindowBound,
        Query(description="First UTC date, or timezone-aware ISO-8601 datetime, whose runs are counted."),
    ],
    end: Annotated[
        WindowBound,
        Query(description="UTC date, or timezone-aware ISO-8601 datetime, at which counting stops (excluded)."),
    ],
    org: Org = Depends(get_current_org),
) -> CostsByEmailResponse:
    window_start, window_end = _window(start, end)
    rows = repository.by_email(org.id, window_start, window_end)
    return CostsByEmailResponse(
        start=window_start,
        end=window_end,
        costs=[EmailCost(**row._asdict()) for row in rows],
    )


class ProjectCostRow(BaseModel):
    month: str
    vendor: str
    spend_type: str | None
    subcategory: str | None
    project_handle: str | None
    project_name: str | None
    customer: str | None
    amount_usd: float
    run_count: int


class ProjectRun(BaseModel):
    project_handle: str | None
    run_id: UUID
    started_at: datetime


class CostsByProjectResponse(BaseModel):
    start: datetime
    end: datetime
    rows: list[ProjectCostRow]
    runs: list[ProjectRun]


def _as_utc(moment: datetime) -> datetime:
    utc = ZoneInfo("UTC")
    return moment.replace(tzinfo=utc) if moment.tzinfo is None else moment.astimezone(utc)


def _vendor(provider: str) -> str:
    return provider.title()


@router.get(
    "/by-project",
    response_model=CostsByProjectResponse,
    description=(
        "List recorded run costs per month, vendor, and project for costs that ended in the `[start, end)` window.\n"
        "\n"
        "A cost is placed in the UTC month it ended. That is its `usage_end`, else its run's `finished_at`, else its run's\n"
        "`started_at`.\n"
        "\n"
        "A cost counts under its run's newest project link. Runs with no project return null project fields.\n"
        "\n"
        "`runs` lists every run alive during the window or holding a cost in it, with its project. Model spend dated in the\n"
        "window can then join to its project whatever day its run started.\n"
        "\n"
        "A run is alive if it started before `end` and finished on or after `start`, or is still in progress or stopping."
    ),
)
def get_costs_by_project(
    repository: CostRepositoryDependency,
    start: Annotated[
        WindowBound,
        Query(description="First UTC date, or timezone-aware ISO-8601 datetime, whose costs and runs are counted."),
    ],
    end: Annotated[
        WindowBound,
        Query(description="UTC date, or timezone-aware ISO-8601 datetime, at which counting stops (excluded)."),
    ],
    org: Org = Depends(get_current_org),
) -> CostsByProjectResponse:
    window_start, window_end = _window(start, end)
    run_rows, cost_rows = repository.by_project(org.id, window_start, window_end)
    grouped: dict[tuple[str, str, str | None], dict[str, Any]] = {}
    for cost in cost_rows:
        month = _as_utc(cost.placed_at).strftime("%Y-%m")
        key = (month, _vendor(cost.provider), cost.handle)
        group = grouped.setdefault(key, {"project": cost, "amount_usd": 0.0, "run_ids": set()})
        group["amount_usd"] += cost.amount_usd
        group["run_ids"].add(cost.run_id)
    rows = [
        ProjectCostRow(
            month=month,
            vendor=vendor,
            spend_type=getattr(group["project"].spend_type, "value", group["project"].spend_type),
            subcategory=group["project"].subcategory,
            project_handle=handle,
            project_name=group["project"].name,
            customer=group["project"].customer,
            amount_usd=group["amount_usd"],
            run_count=len(group["run_ids"]),
        )
        for (month, vendor, handle), group in sorted(
            grouped.items(), key=lambda item: (item[0][0], item[0][1], item[0][2] or "")
        )
    ]
    return CostsByProjectResponse(
        start=window_start,
        end=window_end,
        rows=rows,
        runs=[
            ProjectRun(project_handle=row.handle, run_id=row.run_id, started_at=_as_utc(row.started_at))
            for row in run_rows
        ],
    )


class ProjectDaytonaRow(BaseModel):
    project_id: UUID
    total_usd: float


class ProjectDaytonaResponse(BaseModel):
    start: str
    end: str
    rows: list[ProjectDaytonaRow]


class ProjectSandboxRow(BaseModel):
    project_id: UUID
    provider: str
    total_usd: float


class ProjectSandboxResponse(BaseModel):
    start: str
    end: str
    rows: list[ProjectSandboxRow]


@router.get(
    "/project-daytona",
    response_model=ProjectDaytonaResponse,
    description="List Daytona sandbox charges by each run's newest project link in this organization, for [start, end).",
)
def get_project_daytona_costs(
    repository: CostRepositoryDependency,
    start: Annotated[
        WindowBound,
        Query(description="First UTC date, or timezone-aware ISO-8601 datetime, whose runs are counted."),
    ],
    end: Annotated[
        WindowBound,
        Query(description="UTC date, or timezone-aware ISO-8601 datetime, at which counting stops (excluded)."),
    ],
    org: Org = Depends(get_current_org),
) -> ProjectDaytonaResponse:
    window_start, window_end = _window(start, end)
    rows = repository.project_daytona_totals(org.id, window_start, window_end)
    return ProjectDaytonaResponse(
        start=start.isoformat(),
        end=end.isoformat(),
        rows=[ProjectDaytonaRow(**row._asdict()) for row in rows],
    )


@router.get(
    "/project-sandbox",
    response_model=ProjectSandboxResponse,
    description="List sandbox charges per provider by each run's newest project link in this organization, for [start, end).",
)
def get_project_sandbox_costs(
    repository: CostRepositoryDependency,
    start: Annotated[
        WindowBound,
        Query(description="First UTC date, or timezone-aware ISO-8601 datetime, whose runs are counted."),
    ],
    end: Annotated[
        WindowBound,
        Query(description="UTC date, or timezone-aware ISO-8601 datetime, at which counting stops (excluded)."),
    ],
    org: Org = Depends(get_current_org),
) -> ProjectSandboxResponse:
    window_start, window_end = _window(start, end)
    rows = repository.project_sandbox_totals(org.id, window_start, window_end)
    return ProjectSandboxResponse(
        start=start.isoformat(),
        end=end.isoformat(),
        rows=[ProjectSandboxRow(**row._asdict()) for row in rows],
    )


class DaytonaUnmatchedResponse(BaseModel):
    start: str
    end: str
    total_usd: float
    unpriced_sandboxes: int


@router.get(
    "/daytona-unmatched",
    response_model=DaytonaUnmatchedResponse,
    description=(
        "Sum Daytona usage in this organization's accounts from sandboxes no run recorded, "
        "over the usage hours in [start, end), and count the listed sandboxes that had no price."
    ),
)
def get_daytona_unmatched_costs(
    repository: CostRepositoryDependency,
    start: Annotated[
        WindowBound,
        Query(description="First UTC date, or timezone-aware ISO-8601 datetime, whose usage hours are counted."),
    ],
    end: Annotated[
        WindowBound,
        Query(description="UTC date, or timezone-aware ISO-8601 datetime, at which counting stops (excluded)."),
    ],
    org: Org = Depends(get_current_org),
) -> DaytonaUnmatchedResponse:
    window_start, window_end = _window(start, end)
    total_usd, unpriced_sandboxes = repository.unmatched_usage(org.id, window_start, window_end)
    return DaytonaUnmatchedResponse(
        start=start.isoformat(),
        end=end.isoformat(),
        total_usd=total_usd,
        unpriced_sandboxes=unpriced_sandboxes,
    )


class SandboxRowKind(str, Enum):
    HOURLY = "hourly"
    LIFETIME = "lifetime"


SANDBOX_ROWS_PAGE_SIZE = 5_000
SANDBOX_ROWS_MAX_PAGE_SIZE = 20_000


class SandboxCostEntry(BaseModel):
    source_id: str | None
    kind: SandboxRowKind
    placed_at: datetime
    amount_usd: float
    usage_end: datetime | None
    run_id: UUID
    project_handle: str | None


class SandboxDayTotal(BaseModel):
    day: date
    hourly_usd: float
    lifetime_usd: float
    total_usd: float


class SandboxCostRowsResponse(BaseModel):
    start: datetime
    end: datetime
    provider: str
    limit: int
    offset: int
    total_rows: int
    rows: list[SandboxCostEntry]
    days: list[SandboxDayTotal]


@router.get(
    "/sandbox-rows",
    response_model=SandboxCostRowsResponse,
    description="Read out every sandbox cost row of one provider in this organization placed in [start, end), with per-UTC-day totals.",
)
def get_sandbox_cost_rows(
    repository: CostRepositoryDependency,
    start: Annotated[
        WindowBound,
        Query(description="First UTC date, or timezone-aware ISO-8601 datetime, whose costs are listed."),
    ],
    end: Annotated[
        WindowBound,
        Query(description="UTC date, or timezone-aware ISO-8601 datetime, at which listing stops (excluded)."),
    ],
    provider: Annotated[str, Query(description="Sandbox provider whose cost rows are listed.")] = DAYTONA_PROVIDER,
    limit: Annotated[
        int,
        Query(ge=1, le=SANDBOX_ROWS_MAX_PAGE_SIZE, description="Most rows returned in one page."),
    ] = SANDBOX_ROWS_PAGE_SIZE,
    offset: Annotated[int, Query(ge=0, description="Rows skipped before the page starts.")] = 0,
    org: Org = Depends(get_current_org),
) -> SandboxCostRowsResponse:
    window_start, window_end = _window(start, end)
    rows = repository.sandbox_rows(org.id, provider, window_start, window_end, limit=limit, offset=offset)
    days: dict[date, dict[SandboxRowKind, float]] = {}
    total_rows = 0
    for total in repository.sandbox_day_totals(org.id, provider, window_start, window_end):
        amounts = days.setdefault(total.day, dict.fromkeys(SandboxRowKind, 0.0))
        amounts[SandboxRowKind.HOURLY if total.hourly else SandboxRowKind.LIFETIME] += total.amount_usd
        total_rows += total.row_count
    return SandboxCostRowsResponse(
        start=window_start,
        end=window_end,
        provider=provider,
        limit=limit,
        offset=offset,
        total_rows=total_rows,
        rows=[
            SandboxCostEntry(
                source_id=row.source_id,
                kind=SandboxRowKind.HOURLY if row.hourly else SandboxRowKind.LIFETIME,
                placed_at=_as_utc(row.placed_at),
                amount_usd=row.amount_usd,
                usage_end=None if row.usage_end is None else _as_utc(row.usage_end),
                run_id=row.run_id,
                project_handle=row.handle,
            )
            for row in rows
        ],
        days=[
            SandboxDayTotal(
                day=day,
                hourly_usd=amounts[SandboxRowKind.HOURLY],
                lifetime_usd=amounts[SandboxRowKind.LIFETIME],
                total_usd=sum(amounts.values()),
            )
            for day, amounts in sorted(days.items())
        ],
    )


class UsageEndFill(BaseModel):
    run_cost_id: UUID
    source_id: str
    amount_usd: float
    placed_at: datetime
    usage_end: datetime


class UsageEndFillResponse(BaseModel):
    start: datetime
    end: datetime
    applied: bool
    rows: list[UsageEndFill]


@router.post(
    "/sandbox-usage-end",
    response_model=UsageEndFillResponse,
    include_in_schema=False,
    description="Fill the missing `usage_end` of this organization's Daytona sandbox rows whose sandbox ended in [start, end), from Daytona's last usage time.",
)
async def fill_sandbox_usage_end(
    repository: CostRepositoryDependency,
    secrets: ProviderSecretRepositoryDependency,
    start: Annotated[
        WindowBound,
        Query(description="First UTC date, or timezone-aware ISO-8601 datetime, of Daytona usage read."),
    ],
    end: Annotated[
        WindowBound,
        Query(description="UTC date, or timezone-aware ISO-8601 datetime, at which the Daytona read stops (excluded)."),
    ],
    apply: Annotated[bool, Query(description="Write the rows and save their IDs. Defaults to a dry run.")] = False,
    org: Org = Depends(get_current_org),
) -> UsageEndFillResponse:
    window_start, window_end = _window(start, end)
    secret_name = (await database_call(lambda: secrets.list_all(org.id))).get(DAYTONA_PROVIDER)
    if secret_name is None:
        raise HTTPException(status_code=409, detail=f"{org.name} has no Daytona secret.")
    reference = resolve_secret_name(secret_name, org_id=org.id, org_name=org.name, auth_required=AUTH_REQUIRED)
    last_ends = await daytona_last_ends(org, reference, window_start, window_end)
    rows = await database_call(lambda: repository.missing_usage_end_rows(org.id, DAYTONA_PROVIDER, list(last_ends)))
    if apply:
        usage_ends = {row.id: last_ends[row.source_id] for row in rows}
        changed = set(await database_call(lambda: repository.backfill_usage_end(usage_ends, datetime.now(UTC))))
        rows = [row for row in rows if row.id in changed]
    return UsageEndFillResponse(
        start=window_start,
        end=window_end,
        applied=apply,
        rows=[
            UsageEndFill(
                run_cost_id=row.id,
                source_id=row.source_id,
                amount_usd=row.amount_usd,
                placed_at=_as_utc(row.placed_at),
                usage_end=last_ends[row.source_id],
            )
            for row in rows
        ],
    )
