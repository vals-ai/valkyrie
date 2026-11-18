from datetime import datetime
from decimal import Decimal
from itertools import batched
from uuid import UUID

import httpx
from pydantic import BaseModel

from tracker.config import VALKYRIE_STAGE
from tracker.exceptions import GatewayUnavailableError
from tracker.project_gateway import gateway_client

RUN_SUMS_PATH = "admin/managed-auth/machine/usage/run-sums"
RUN_SUMS_BATCH = 1000
RUN_SUMS_TIMEOUT_SECONDS = 20


class _RunAmount(BaseModel):
    run_id: UUID
    amount_usd: Decimal


class _KeyAmount(BaseModel):
    run_id: str | None
    amount_usd: Decimal


class RunSums(BaseModel):
    runs: list[_RunAmount]
    project_key: list[_KeyAmount]
    through: datetime


async def run_sums(run_ids: list[UUID], project_id: UUID) -> RunSums:
    try:
        async with gateway_client() as client:
            bodies: list[RunSums] = []
            for index, batch in enumerate(list(batched(run_ids, RUN_SUMS_BATCH)) or [()]):
                response = await client.post(
                    RUN_SUMS_PATH,
                    json={
                        "run_ids": [str(run_id) for run_id in batch],
                        "project_id": str(project_id) if index == 0 else None,
                        "project_environment": VALKYRIE_STAGE,
                    },
                    timeout=RUN_SUMS_TIMEOUT_SECONDS,
                )
                _ = response.raise_for_status()
                bodies.append(RunSums.model_validate_json(response.content))
    except (httpx.HTTPError, ValueError) as error:
        raise GatewayUnavailableError(str(error)) from error
    return RunSums(
        runs=[row for body in bodies for row in body.runs],
        project_key=[row for body in bodies for row in body.project_key],
        through=min(body.through for body in bodies),
    )
