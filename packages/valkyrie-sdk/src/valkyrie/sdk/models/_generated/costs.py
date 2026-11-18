from __future__ import annotations
from valkyrie.sdk.models._base import ResponseModel, UTCDateTime
from uuid import UUID


class ProjectCostRow(ResponseModel):
    """Spend in one month for one vendor and project, with the number of runs that incurred it."""

    month: str
    vendor: str
    spend_type: str | None
    subcategory: str | None
    project_handle: str | None
    project_name: str | None
    customer: str | None
    amount_usd: float
    run_count: int


class ProjectRun(ResponseModel):
    """One run that incurred spend in the window, with the project it ran in."""

    project_handle: str | None
    run_id: UUID
    started_at: UTCDateTime


class CostsByProjectResponse(ResponseModel):
    """Spend grouped by month, vendor and project over a time window, with the runs behind it."""

    start: UTCDateTime
    end: UTCDateTime
    rows: list[ProjectCostRow]
    runs: list[ProjectRun]


class MonthModelCost(ResponseModel):
    """A project's model spend in one UTC month."""

    month: str
    amount_usd: float


class ProjectModelCosts(ResponseModel):
    """A project's model spend per UTC month, with its key's spend on no project's run."""

    start: UTCDateTime
    end: UTCDateTime
    months: list[MonthModelCost]
    unattributed_usd: float
    through: UTCDateTime
