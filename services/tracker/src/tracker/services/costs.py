from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import httpx
from pydantic import BaseModel

from tracker.clients.model_gateway import machine
from tracker.database.session import database_call
from tracker.exceptions import GatewayUnavailableError
from tracker.logging import get_logger
from tracker.repositories.cost import CostRepository
from tracker.repositories.project import ProjectRepository
from tracker.types import ProjectBudget

logger = get_logger(__name__)
MODEL_COST_TYPE = "model"
ALL_TIME = (datetime(1970, 1, 1, tzinfo=UTC), datetime(9999, 1, 1, tzinfo=UTC))


class MonthModelCost(BaseModel):
    month: str
    amount_usd: float


class ProjectModelCosts(BaseModel):
    start: datetime
    end: datetime
    months: list[MonthModelCost]
    unattributed_usd: float
    through: datetime


class RunResourceCost(BaseModel):
    cost_type: str
    provider: str
    resource: str | None
    unit: str | None
    quantity: float | None
    amount_usd: float


class RunCosts(BaseModel):
    run_id: UUID
    total_usd: float
    by_type_usd: dict[str, float]
    resources: list[RunResourceCost]
    model_spend_available: bool
    model_through: datetime | None


def as_utc(moment: datetime) -> datetime:
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)


async def project_model_spend(
    repository: CostRepository, org_id: UUID, project_id: UUID, start: datetime, end: datetime
) -> ProjectModelCosts:
    placed = await database_call(lambda: repository.project_runs(org_id, project_id, start, end))
    await database_call(repository.rollback)
    sums = await machine.run_sums(list(placed), project_id)
    amounts = {row.run_id: row.amount_usd for row in sums.runs}
    key_amounts: list[tuple[UUID | None, Decimal]] = []
    for row in sums.project_key:
        try:
            run_id = UUID(row.run_id) if row.run_id is not None else None
        except ValueError:
            run_id = None
        key_amounts.append((run_id, row.amount_usd))
    asked = sorted({run_id for run_id, _ in key_amounts if run_id is not None} - placed.keys())
    linked = placed.keys() | await database_call(lambda: repository.linked_run_ids(org_id, asked))
    months: dict[str, Decimal] = {}
    for run_id, placed_at in placed.items():
        month = as_utc(placed_at).strftime("%Y-%m")
        months[month] = months.get(month, Decimal(0)) + amounts.get(run_id, Decimal(0))
    return ProjectModelCosts(
        start=start,
        end=end,
        months=[MonthModelCost(month=month, amount_usd=float(amount)) for month, amount in sorted(months.items())],
        unattributed_usd=float(sum((amount for run_id, amount in key_amounts if run_id not in linked), Decimal(0))),
        through=sums.through,
    )


async def run_model_spend(run_id: UUID) -> tuple[float, datetime] | None:
    try:
        sums = await machine.run_sums([run_id], None)
    except GatewayUnavailableError as error:
        cause = error.__cause__
        logger.warning(
            "Model Gateway is unavailable for run %s: %s status=%s",
            run_id,
            type(cause).__name__,
            cause.response.status_code if isinstance(cause, httpx.HTTPStatusError) else None,
        )
        return None
    return float(sum((row.amount_usd for row in sums.runs if row.run_id == run_id), Decimal(0))), sums.through


async def run_spend(costs: CostRepository, run_id: UUID) -> RunCosts:
    rows = await database_call(lambda: costs.run_resource_costs(run_id))
    await database_call(costs.rollback)
    model = await run_model_spend(run_id)
    by_type: dict[str, float] = {}
    for row in rows:
        by_type[row.cost_type] = by_type.get(row.cost_type, 0.0) + row.amount_usd
    if model is not None:
        by_type[MODEL_COST_TYPE] = by_type.get(MODEL_COST_TYPE, 0.0) + model[0]
    return RunCosts(
        run_id=run_id,
        total_usd=sum(by_type.values()),
        by_type_usd=by_type,
        resources=[RunResourceCost(**row._asdict()) for row in rows],
        model_spend_available=model is not None,
        model_through=model[1] if model else None,
    )


async def project_model_total(costs: CostRepository, org_id: UUID, project_id: UUID) -> tuple[float, datetime] | None:
    try:
        model = await project_model_spend(costs, org_id, project_id, *ALL_TIME)
    except GatewayUnavailableError as error:
        cause = error.__cause__
        logger.warning(
            "Model Gateway is unavailable for project %s: %s status=%s",
            project_id,
            type(cause).__name__,
            cause.response.status_code if isinstance(cause, httpx.HTTPStatusError) else None,
        )
        return None
    return sum(month.amount_usd for month in model.months) + model.unattributed_usd, model.through


async def project_budget(
    costs: CostRepository, projects: ProjectRepository, org_id: UUID, handle: str
) -> ProjectBudget:
    project = await database_call(lambda: projects.resolve(org_id, handle))
    limit = await database_call(lambda: projects.limit(project))
    rows = await database_call(lambda: costs.project_run_costs(org_id, project.id))
    model = await project_model_total(costs, org_id, project.id)
    by_type: dict[str, float] = {}
    for row in rows:
        if row.cost_type is not None:
            by_type[row.cost_type] = by_type.get(row.cost_type, 0.0) + row.cost_usd
    if model is not None:
        by_type[MODEL_COST_TYPE] = by_type.get(MODEL_COST_TYPE, 0.0) + model[0]
    return ProjectBudget(
        limit_dollars=limit["limit_dollars"],
        limit_reason=limit["reason"],
        limit_applied=limit["applied"],
        total_usd=sum(by_type.values()),
        by_type_usd=by_type,
        model_spend_available=model is not None,
        model_through=model[1] if model else None,
    )
