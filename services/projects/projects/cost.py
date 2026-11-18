import http.client
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from projects.database import engine
from projects.models import LIVE, Project

GATEWAY_RUN_LIMIT = 1000
TRACKER_LINKED_RUN_LIMIT = 10000
UPSTREAM_TIMEOUT_SECONDS = 20
UPSTREAM_SETTINGS = ("TRACKER_URL", "MODEL_GATEWAY_URL", "PROJECT_SERVICE_TOKEN", "MODEL_GATEWAY_MACHINE_API_KEY")

router = APIRouter()


class ProjectCost(BaseModel):
    org_id: UUID
    project_id: UUID
    environment: str
    tracker_usd: Decimal
    tracker_by_type_usd: dict[str, Decimal]
    model_usd: Decimal
    run_model_usd: Decimal
    key_model_usd: Decimal
    total_usd: Decimal
    through: datetime


def _call(url: str, token: str, body: dict[str, object] | None = None) -> Any:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, headers={"Authorization": f"Bearer {token}"})
    if data is not None:
        request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, timeout=UPSTREAM_TIMEOUT_SECONDS) as response:
        return json.loads(response.read())


@router.get(
    "/orgs/{org_id}/projects/{project_id}/cost",
    description="Total a project's all-time dollars: Tracker sandbox and search plus Gateway model dollars. "
    "Returns 409 when Tracker serves another environment and 503 when an upstream fails.",
)
def project_cost(
    org_id: UUID, project_id: UUID, environment: str, bound: Annotated[Engine, Depends(engine)]
) -> ProjectCost:
    with Session(bound) as session:
        if session.scalar(select(Project.id).where(Project.id == project_id, Project.org_id == org_id, LIVE)) is None:
            raise HTTPException(status_code=404, detail="Project not found")
    settings = [os.environ.get(name, "") for name in UPSTREAM_SETTINGS]
    if not all(settings):
        raise HTTPException(status_code=503, detail="Tracker or the Gateway is not configured")
    tracker_url, gateway_url, tracker_token, gateway_token = settings
    tracker = f"{tracker_url.rstrip('/')}/project-service/orgs/{org_id}/projects/{project_id}"
    gateway = f"{gateway_url.rstrip('/')}/admin/managed-auth/machine/usage/run-sums"
    try:
        runs = _call(f"{tracker}/run-costs?{urllib.parse.urlencode({'environment': environment})}", tracker_token)[
            "runs"
        ]
        run_ids = [run["run_id"] for run in runs]
        sums = [
            _call(
                gateway,
                gateway_token,
                {
                    "run_ids": run_ids[start : start + GATEWAY_RUN_LIMIT],
                    "project_id": str(project_id) if start == 0 else None,
                    "project_environment": environment,
                },
            )
            for start in range(0, max(len(run_ids), 1), GATEWAY_RUN_LIMIT)
        ]
        key_rows = [row for part in sums for row in part["project_key"]]
        asked = sorted({row["run_id"] for row in key_rows} - {None} - set(run_ids))
        linked = set(run_ids).union(
            *(
                _call(
                    f"{tracker_url.rstrip('/')}/project-service/orgs/{org_id}/linked-runs",
                    tracker_token,
                    {"run_ids": asked[start : start + TRACKER_LINKED_RUN_LIMIT]},
                )["run_ids"]
                for start in range(0, len(asked), TRACKER_LINKED_RUN_LIMIT)
            )
        )
        tracker_total = sum((Decimal(str(run["cost_usd"])) for run in runs), Decimal(0))
        by_type: dict[str, Decimal] = {}
        for run in runs:
            for cost_type, amount in run["by_type"].items():
                by_type[cost_type] = by_type.get(cost_type, Decimal(0)) + Decimal(str(amount))
        run_model = sum((Decimal(row["amount_usd"]) for part in sums for row in part["runs"]), Decimal(0))
        key_model = sum((Decimal(row["amount_usd"]) for row in key_rows if row["run_id"] not in linked), Decimal(0))
        model = run_model + key_model
        through = min(datetime.fromisoformat(part["through"]) for part in sums)
    except urllib.error.HTTPError as error:
        if error.code == 409 and error.url.startswith(tracker):
            raise HTTPException(status_code=409, detail="Tracker serves another environment") from error
        raise HTTPException(status_code=503, detail="Tracker or the Gateway failed") from error
    except (
        OSError,
        http.client.HTTPException,
        ArithmeticError,
        AttributeError,
        ValueError,
        KeyError,
        TypeError,
    ) as error:
        raise HTTPException(status_code=503, detail="Tracker or the Gateway failed") from error
    return ProjectCost(
        org_id=org_id,
        project_id=project_id,
        environment=environment,
        tracker_usd=tracker_total,
        tracker_by_type_usd=by_type,
        model_usd=model,
        run_model_usd=run_model,
        key_model_usd=key_model,
        total_usd=tracker_total + model,
        through=through,
    )
