from __future__ import annotations


from fastapi import APIRouter, Depends
from pydantic import BaseModel

from tracker.auth import get_current_org
from tracker.database.models import Org
from tracker.repositories.benchmark import BenchmarkRepositoryDependency

router = APIRouter(prefix="/benchmarks")


class FilterOptionsResponse(BaseModel):
    benchmark_names: list[str]
    agent_names: list[str]
    models: list[str]
    datasets: list[str]
    started_by_emails: list[str]


@router.get(
    "/filter-options",
    response_model=FilterOptionsResponse,
    description="List the distinct benchmark, agent, model, dataset, and starter values in this organization's runs, for the runs-list filters.",
)
def get_filter_options(
    repository: BenchmarkRepositoryDependency,
    org: Org = Depends(get_current_org),
) -> FilterOptionsResponse:

    rows = repository.filter_rows(org.id)
    return FilterOptionsResponse(
        benchmark_names=sorted({row.benchmark_name for row in rows}),
        agent_names=sorted({row.agent_name for row in rows}),
        models=sorted({row.model for row in rows if row.model}),
        datasets=sorted({row.dataset or "default" for row in rows}),
        started_by_emails=sorted({row.email for row in rows if row.email}),
    )
