import hashlib
import secrets
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from tracker.database.models import RunCostReportToken
from tracker.repositories.cost import PROJECT_COST_TYPES

COST_REPORT_TOKEN_HEADER = "X-Valkyrie-Cost-Report-Token"


class UnreportableCostType(Exception):
    def __init__(self, cost_type: str) -> None:
        super().__init__(f"The Tracker records `{cost_type}` costs itself, so they can't be reported.")


class ReportedCost(BaseModel):
    cost_type: str = Field(min_length=1, max_length=64, description="Kind of cost, such as `cluster_gpu`.")
    provider: str = Field(min_length=1, max_length=64, description="Provider the service paid, such as `coreweave`.")
    source_id: str = Field(min_length=1, max_length=200, description="Reporter's key for this cost within the attempt.")
    attempt_id: str = Field(min_length=1, description="Attempt the cost belongs to.")
    quantity: float = Field(ge=0, allow_inf_nan=False, description="Usage measured in `unit`.")
    unit: str = Field(min_length=1, max_length=64, description="Unit of `quantity`, such as `node_hour`.")
    amount_usd: float = Field(ge=0, allow_inf_nan=False, description="What the service paid for this usage, in USD.")
    usage_end: datetime | None = Field(default=None, description="When the usage ended. Defaults to the report time.")


def cost_report_token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def with_cost_report_token(service_headers: Mapping[str, str]) -> dict[str, str]:
    headers = {
        name: value for name, value in service_headers.items() if name.lower() != COST_REPORT_TOKEN_HEADER.lower()
    }
    return headers | {COST_REPORT_TOKEN_HEADER: secrets.token_urlsafe(32)}


def issued_cost_report_tokens(run_id: UUID, service_headers: Mapping[str, str]) -> list[RunCostReportToken]:
    return [
        RunCostReportToken(token_hash=cost_report_token_hash(value), benchmark_id=run_id)
        for name, value in service_headers.items()
        if name.lower() == COST_REPORT_TOKEN_HEADER.lower()
    ]


def reported_cost_rows(run_id: UUID, reporter: str, costs: list[ReportedCost]) -> list[dict[str, Any]]:
    reported_at = datetime.now(UTC)
    rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for cost in costs:
        if cost.cost_type in PROJECT_COST_TYPES:
            raise UnreportableCostType(cost.cost_type)
        rows[(cost.attempt_id, cost.provider, cost.source_id, cost.cost_type)] = {
            "benchmark_id": run_id,
            "attempt_id": cost.attempt_id,
            "cost_type": cost.cost_type,
            "provider": cost.provider,
            "source_id": cost.source_id,
            "amount_usd": cost.amount_usd,
            "quantity": cost.quantity,
            "unit": cost.unit,
            "usage_end": cost.usage_end or reported_at,
            "reporter": reporter,
        }
    return list(rows.values())
