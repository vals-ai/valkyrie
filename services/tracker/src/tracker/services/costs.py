from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel

from tracker.clients import gateway_usage
from tracker.database.session import database_call
from tracker.repositories.cost import CostRepository


class MonthModelCost(BaseModel):
    month: str
    amount_usd: float


class ProjectModelCosts(BaseModel):
    start: datetime
    end: datetime
    months: list[MonthModelCost]
    unattributed_usd: float
    through: datetime


def as_utc(moment: datetime) -> datetime:
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)


async def project_model_spend(
    repository: CostRepository, org_id: UUID, project_id: UUID, start: datetime, end: datetime
) -> ProjectModelCosts:
    placed = await database_call(lambda: repository.project_runs(org_id, project_id, start, end))
    await database_call(repository.rollback)
    sums = await gateway_usage.run_sums(list(placed), project_id)
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
