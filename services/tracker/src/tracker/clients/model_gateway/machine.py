import asyncio
import logging
import os
from datetime import datetime
from decimal import Decimal
from enum import Enum
from itertools import batched
from uuid import UUID

import httpx
from pydantic import BaseModel

from tracker.clients.model_gateway import MODEL_GATEWAY_URL
from tracker.config import VALKYRIE_STAGE
from tracker.database.models import Project
from tracker.exceptions import GatewayUnavailableError


_GATEWAY_PATH = "admin/managed-auth/machine"
_KEY_PUBLICATION_TIMEOUT_SECONDS = 45
_KEY_PUBLICATION_POLL_SECONDS = 5
_RUN_SUMS_BATCH = 1000
_RUN_SUMS_TIMEOUT_SECONDS = 20

logger = logging.getLogger(__name__)


class ProjectGatewayKeysMode(str, Enum):
    MANUAL = "manual"
    PROVISION = "provision"
    AUTOMATIC = "automatic"


def project_gateway_keys_mode() -> ProjectGatewayKeysMode:
    try:
        return ProjectGatewayKeysMode(os.environ.get("MODEL_GATEWAY_PROJECT_KEYS_MODE", "manual"))
    except ValueError:
        raise ValueError("Set MODEL_GATEWAY_PROJECT_KEYS_MODE to manual, provision, or automatic.") from None


def project_gateway_configuration() -> tuple[str, str]:
    message = f"Set {MODEL_GATEWAY_URL} to the model Gateway's absolute HTTP or HTTPS URL."
    try:
        url = httpx.URL(os.environ.get(MODEL_GATEWAY_URL, "").strip())
    except httpx.InvalidURL:
        raise ValueError(message) from None
    if url.scheme not in {"http", "https"} or not url.host or (url.port is not None and not 0 < url.port <= 65535):
        raise ValueError(message)
    machine_api_key = os.environ.get("MODEL_GATEWAY_MACHINE_API_KEY", "").strip()
    if not machine_api_key:
        raise ValueError("Set MODEL_GATEWAY_MACHINE_API_KEY to the model Gateway's machine bearer token.")
    return str(url), machine_api_key


def validate_enabled_project_gateway() -> None:
    if project_gateway_keys_mode() is ProjectGatewayKeysMode.MANUAL and not any(
        os.environ.get(name, "").strip() for name in (MODEL_GATEWAY_URL, "MODEL_GATEWAY_MACHINE_API_KEY")
    ):
        return
    _ = project_gateway_configuration()
    if not VALKYRIE_STAGE.strip():
        raise ValueError("Set VALKYRIE_STAGE to the environment used for this deployment's model Gateway project keys.")


def gateway_client() -> httpx.AsyncClient:
    url, machine_key = project_gateway_configuration()
    return httpx.AsyncClient(base_url=url, headers={"Authorization": f"Bearer {machine_key}"})


async def request_project_gateway_key(project: Project, budget: Decimal, budget_reason: str) -> UUID:
    async with gateway_client() as client:
        response = await client.post(
            f"{_GATEWAY_PATH}/requests",
            json={
                "project_id": str(project.id),
                "project_organization_id": str(project.org_id),
                "project_environment": VALKYRIE_STAGE,
                "display_name": project.name,
                "description": f"Valkyrie project {project.handle}",
                "requested_budget": str(budget),
                "budget_reason": budget_reason,
            },
        )
        response.raise_for_status()
        return UUID(response.json()["id"])


async def requested_key_id(project: Project) -> UUID | None:
    async with gateway_client() as gateway:
        response = await gateway.get(f"{_GATEWAY_PATH}/requests/{project.gateway_request_id}")
        response.raise_for_status()
        requested = response.json()
    return UUID(requested["key_id"]) if requested["status"] == "approved" and requested.get("key_id") else None


async def wait_for_gateway_to_accept(secret: str, project: Project) -> None:
    async with gateway_client() as client:
        try:
            async with asyncio.timeout(_KEY_PUBLICATION_TIMEOUT_SECONDS):
                while (await client.get("registry", headers={"Authorization": f"Bearer {secret}"})).status_code == 401:
                    await asyncio.sleep(_KEY_PUBLICATION_POLL_SECONDS)
        except TimeoutError:
            logger.warning("Gateway still rejects the new key for project %s; starting anyway", project.handle)
        except httpx.TransportError:
            logger.warning(
                "Could not reach the Gateway to check the new key for project %s; starting anyway", project.handle
            )


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


async def run_sums(run_ids: list[UUID], project_id: UUID | None) -> RunSums:
    try:
        async with gateway_client() as client:
            bodies: list[RunSums] = []
            for index, batch in enumerate(list(batched(run_ids, _RUN_SUMS_BATCH)) or [()]):
                response = await client.post(
                    f"{_GATEWAY_PATH}/usage/run-sums",
                    json={
                        "run_ids": [str(run_id) for run_id in batch],
                        "project_id": str(project_id) if index == 0 and project_id else None,
                        "project_environment": VALKYRIE_STAGE,
                    },
                    timeout=_RUN_SUMS_TIMEOUT_SECONDS,
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
