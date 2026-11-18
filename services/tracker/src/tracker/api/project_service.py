import hmac
from itertools import groupby
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

import tracker.config as config
from tracker.repositories.cost import CostRepositoryDependency

router = APIRouter(prefix="/project-service", include_in_schema=False)


def project_service_caller(authorization: Annotated[str, Header()] = "") -> None:
    token = config.PROJECT_SERVICE_TOKEN
    if not token or not hmac.compare_digest(authorization.encode(), f"Bearer {token}".encode()):
        raise HTTPException(status_code=401, detail="Invalid project service token")


class ProjectRunCost(BaseModel):
    run_id: UUID
    cost_usd: float
    by_type: dict[str, float]


class ProjectRunCostsResponse(BaseModel):
    org_id: UUID
    project_id: UUID
    environment: str
    runs: list[ProjectRunCost]


@router.get(
    "/orgs/{org_id}/projects/{project_id}/run-costs",
    dependencies=[Depends(project_service_caller)],
    description="List the org's runs whose newest project link is this project, with each run's all-time counted dollars, in total and by cost type. "
    "Returns 409 when `environment` is not the one this Tracker serves.",
)
def project_run_costs(
    org_id: UUID,
    project_id: UUID,
    environment: Annotated[str, Query()],
    repository: CostRepositoryDependency,
) -> ProjectRunCostsResponse:
    if environment != config.VALKYRIE_STAGE:
        raise HTTPException(status_code=409, detail=f"This Tracker serves {config.VALKYRIE_STAGE!r}")
    return ProjectRunCostsResponse(
        org_id=org_id,
        project_id=project_id,
        environment=environment,
        runs=[
            ProjectRunCost(
                run_id=run_id,
                cost_usd=sum(row.cost_usd for row in rows),
                by_type={row.cost_type: row.cost_usd for row in rows if row.cost_type is not None},
            )
            for run_id, rows in groupby(repository.project_run_costs(org_id, project_id), key=lambda row: row.run_id)
            for rows in [list(rows)]
        ],
    )


class LinkedRunsRequest(BaseModel):
    run_ids: list[str] = Field(max_length=10000)


class LinkedRunsResponse(BaseModel):
    org_id: UUID
    run_ids: list[UUID]


@router.post(
    "/orgs/{org_id}/linked-runs",
    dependencies=[Depends(project_service_caller)],
    description="Return which of the given run IDs belong to the org and are linked to any project. "
    "IDs that are not UUIDs are never linked.",
)
def linked_runs(org_id: UUID, body: LinkedRunsRequest, repository: CostRepositoryDependency) -> LinkedRunsResponse:
    candidates: list[UUID] = []
    for run_id in body.run_ids:
        try:
            candidates.append(UUID(run_id))
        except ValueError:
            continue
    return LinkedRunsResponse(org_id=org_id, run_ids=repository.linked_run_ids(org_id, candidates))
