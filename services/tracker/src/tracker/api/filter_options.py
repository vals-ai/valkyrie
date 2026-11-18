from __future__ import annotations


from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from tracker.api.projects import PROJECT_NOT_FOUND
from tracker.auth import get_current_org
from tracker.database.models import Org
from tracker.repositories.benchmark import BenchmarkRepositoryDependency
from tracker.repositories.project import ProjectRepositoryDependency

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
    responses=PROJECT_NOT_FOUND,
)
def get_filter_options(
    repository: BenchmarkRepositoryDependency,
    projects: ProjectRepositoryDependency,
    project: str | None = Query(default=None),
    org: Org = Depends(get_current_org),
) -> FilterOptionsResponse:
    options = repository.filter_options(
        org.id, project_id=None if project is None else projects.resolve(org.id, project).id
    )
    return FilterOptionsResponse(**{name: sorted(values) for name, values in options._asdict().items()})
