import asyncio
import json
from decimal import Decimal
from uuid import UUID

import httpx
from fastapi import HTTPException
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session

from tracker.aws.secrets import SecretsManagerStore
from tracker.aws.tags import ORG_ID_TAG
from tracker.config import (
    GATEWAY_KEY_ORGS,
    VALKYRIE_STAGE,
    ProjectGatewayKeysMode,
    project_gateway_configuration,
    project_gateway_keys_mode,
)
from tracker.database.models import Org, Project
from tracker.repositories.project import ProjectRepository
from tracker.local import config as local_config
from tracker.local.resources import reads_secrets_file
from tracker.secret_access import GATEWAY_SECRET_NAME_PREFIX, tenant_secret_prefix


_GATEWAY_PATH = "admin/managed-auth/machine"


def automatic_project_gateway(org: Org) -> bool:
    return org.name in GATEWAY_KEY_ORGS and not reads_secrets_file(local_config.resources)


def requests_project_gateway_key(org: Org) -> bool:
    return automatic_project_gateway(org) and project_gateway_keys_mode() in {
        ProjectGatewayKeysMode.PROVISION,
        ProjectGatewayKeysMode.AUTOMATIC,
    }


def selects_project_gateway_key(org: Org) -> bool:
    return automatic_project_gateway(org) and project_gateway_keys_mode() is ProjectGatewayKeysMode.AUTOMATIC


def _gateway_client() -> httpx.AsyncClient:
    url, machine_key = project_gateway_configuration()
    return httpx.AsyncClient(base_url=url, headers={"Authorization": f"Bearer {machine_key}"})


async def _request_project_gateway_key(project: Project, budget: Decimal, budget_reason: str) -> UUID:
    async with _gateway_client() as client:
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


def request_project_gateway_key(project: Project, budget: Decimal, budget_reason: str) -> UUID:
    return asyncio.run(_request_project_gateway_key(project, budget, budget_reason))


def _gateway_secret_reference(org: Org, key_id: UUID) -> str:
    return f"{tenant_secret_prefix(org.id)}{GATEWAY_SECRET_NAME_PREFIX}{key_id}"


async def _collect_project_gateway_key(project: Project, org: Org, secret_store: SecretsManagerStore) -> UUID:
    request_id = project.gateway_request_id
    async with _gateway_client() as client:
        response = await client.get(f"{_GATEWAY_PATH}/requests/{request_id}")
        response.raise_for_status()
        requested = response.json()
        if requested["status"] != "approved":
            raise HTTPException(
                status_code=409,
                detail=f"Gateway key request for project '{project.handle}' is {requested['status']}",
            )
        key_id = UUID(requested["key_id"])
        response = await client.get(f"{_GATEWAY_PATH}/keys/{key_id}")
        response.raise_for_status()
        key = response.json()
        response = await client.post(
            f"{_GATEWAY_PATH}/keys/{key_id}/collect",
            json={"expected_revision": key["revision"]},
        )
        response.raise_for_status()
        secret = response.json()["secret"]

    url, _ = project_gateway_configuration()
    await secret_store.put(
        _gateway_secret_reference(org, key_id),
        json.dumps({"MODEL_GATEWAY_URL": url, "MODEL_GATEWAY_API_KEY": secret}),
        tags={ORG_ID_TAG: str(org.id)},
    )
    return key_id


def _prepare_project_gateway_key(
    bind: Engine | Connection, project_id: UUID, org: Org, secret_store: SecretsManagerStore
) -> dict[str, str]:
    with Session(bind, expire_on_commit=False) as session:
        project = ProjectRepository(session).get_by_id(org.id, project_id, lock="update")
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project {project_id} not found")
        if project.gateway_key_id is None:
            if project.gateway_request_id is None:
                raise HTTPException(
                    status_code=409,
                    detail=f"Project '{project.handle}' has no Gateway key request; configure its Gateway binding before starting a run",
                )
            project.gateway_key_id = asyncio.run(_collect_project_gateway_key(project, org, secret_store))
            ProjectRepository(session).save(project)
        reference = _gateway_secret_reference(org, project.gateway_key_id)
        return {"MODEL_GATEWAY_URL": reference, "MODEL_GATEWAY_API_KEY": reference}


async def prepare_project_gateway_key(
    bind: Engine | Connection, project_id: UUID, org: Org, secret_store: SecretsManagerStore
) -> dict[str, str]:
    return await asyncio.to_thread(_prepare_project_gateway_key, bind, project_id, org, secret_store)
