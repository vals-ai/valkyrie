from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Header, HTTPException
from pydantic import BaseModel

from tracker.config import BenchmarkServiceDestination, classify_benchmark_service_destination
from tracker.database.models import FINISHED_BENCHMARK_STATUSES, Benchmark
from tracker.repositories.benchmark import BenchmarkRepositoryDependency
from tracker.repositories.cost import CostRepositoryDependency
from tracker.repositories.executor_dispatch import ExecutorDispatchRepositoryDependency
from tracker.services.cost_reports import (
    COST_REPORT_TOKEN_HEADER,
    ReportedCost,
    UnreportableCostType,
    cost_report_token_hash,
    reported_cost_rows,
)

router = APIRouter(prefix="/costs")

RUN_SERVICE_ONLY = "Only the benchmark service of an unfinished run can report its costs"


class ReportedCostsResponse(BaseModel):
    recorded: int


def _reporter(run: Benchmark) -> str:
    destination = classify_benchmark_service_destination(run.name, run.custom_benchmark_service)
    service = run.name if destination is BenchmarkServiceDestination.HOSTED else run.custom_benchmark_service
    return f"{destination.value}:{service}"


@router.post(
    "/runs/{run_id}/reported",
    response_model=ReportedCostsResponse,
    responses={
        401: {"description": "The cost report token is missing"},
        403: {"description": RUN_SERVICE_ONLY},
        422: {"description": "A cost has a type that can't be reported"},
    },
    description=(
        "Record costs that the benchmark service of a run paid outside the Tracker's view, "
        f"authenticated by the `{COST_REPORT_TOKEN_HEADER}` header the Tracker sent that service.\n\n"
        "A repeated `source_id` within an attempt replaces the earlier cost."
    ),
)
def report_run_costs(
    run_id: UUID,
    costs: Annotated[list[ReportedCost], Body(description="Costs to record against the run.")],
    benchmarks: BenchmarkRepositoryDependency,
    dispatches: ExecutorDispatchRepositoryDependency,
    ledger: CostRepositoryDependency,
    token: Annotated[
        str | None,
        Header(alias=COST_REPORT_TOKEN_HEADER, description="Token the Tracker sent the run's benchmark service."),
    ] = None,
) -> ReportedCostsResponse:
    if not token:
        raise HTTPException(status_code=401, detail=f"Send the run's cost report token as {COST_REPORT_TOKEN_HEADER}")
    run = (
        benchmarks.get_by_id(run_id) if ledger.cost_report_token_run(cost_report_token_hash(token)) == run_id else None
    )
    if run is None or (run.status in FINISHED_BENCHMARK_STATUSES and not dispatches.has_active(run_id)):
        raise HTTPException(status_code=403, detail=RUN_SERVICE_ONLY)
    try:
        rows = reported_cost_rows(run_id, _reporter(run), costs)
    except UnreportableCostType as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    recorded = ledger.upsert_reported_costs(rows) if rows else 0
    return ReportedCostsResponse(recorded=recorded)
