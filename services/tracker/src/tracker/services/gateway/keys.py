import asyncio
from typing import cast
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session

from tracker.aws.secrets import SecretsManagerStore
from tracker.clients.model_gateway import MODEL_GATEWAY_API_KEY, MODEL_GATEWAY_URL
from tracker.clients.model_gateway.machine import (
    ProjectGatewayKeysMode,
    project_gateway_keys_mode,
    requested_key_id,
    wait_for_gateway_to_accept,
)
from tracker.clients.projects import GatewayKeyConflictError, ProjectServiceUnavailableError, project_service
from tracker.config import GATEWAY_KEY_ORGS
from tracker.database.models import Org, Project
from tracker.types import GatewayKeyHandOverResponse
from tracker.exceptions import SecretAccessDeniedError, SecretNotFoundError
from tracker.repositories.project import ProjectRepository
from tracker.local import config as local_config
from tracker.local.resources import reads_secrets_file
from tracker.secret_access import (
    GATEWAY_SECRET_NAME_PREFIX,
    PROJECT_SERVICE_GATEWAY_SECRET_PREFIX,
    tenant_secret_prefix,
)


def automatic_project_gateway(org: Org) -> bool:
    return org.name in GATEWAY_KEY_ORGS and not reads_secrets_file(local_config.resources)


def requests_project_gateway_key(org: Org) -> bool:
    return automatic_project_gateway(org) and project_gateway_keys_mode() in {
        ProjectGatewayKeysMode.PROVISION,
        ProjectGatewayKeysMode.AUTOMATIC,
    }


def selects_project_gateway_key(org: Org) -> bool:
    return automatic_project_gateway(org) and project_gateway_keys_mode() is ProjectGatewayKeysMode.AUTOMATIC


def _gateway_secret_reference(org: Org, key_id: UUID) -> str:
    return f"{tenant_secret_prefix(org.id)}{PROJECT_SERVICE_GATEWAY_SECRET_PREFIX}{key_id}"


async def _project_service_gateway_key(project: Project, org: Org, secret_store: SecretsManagerStore) -> UUID:
    projects = project_service()
    try:
        key_id = projects.mint_gateway_key(org.id, project.id)
    except GatewayKeyConflictError as conflict:
        held_key_id = project.gateway_key_id or await requested_key_id(project)
        held = None
        if held_key_id is not None:
            try:
                held = await secret_store.get(
                    f"{tenant_secret_prefix(org.id)}{GATEWAY_SECRET_NAME_PREFIX}{held_key_id}"
                )
            except (SecretNotFoundError, SecretAccessDeniedError):
                held = None
        if held is None or held_key_id is None:
            raise HTTPException(status_code=409, detail=f"{conflict} for project '{project.handle}'") from conflict
        if project.gateway_key_id is None:
            _ = projects.set_gateway_key(org.id, project.id, held_key_id)
        try:
            projects.hand_over_gateway_key(
                org.id, project.id, held_key_id, cast(dict[str, str], held)[MODEL_GATEWAY_API_KEY]
            )
        except GatewayKeyConflictError as refused:
            raise HTTPException(status_code=409, detail=f"{refused} for project '{project.handle}'") from refused
        return held_key_id
    await wait_for_gateway_to_accept(
        cast(dict[str, str], await secret_store.get(_gateway_secret_reference(org, key_id)))[MODEL_GATEWAY_API_KEY],
        project,
    )
    return key_id


def _gateway_key(project: Project, org: Org, secret_store: SecretsManagerStore) -> UUID:
    try:
        return asyncio.run(_project_service_gateway_key(project, org, secret_store))
    except ProjectServiceUnavailableError as error:
        raise HTTPException(
            status_code=503, detail=f"Project service unavailable for project '{project.handle}', try again"
        ) from error


def prepare_project_gateway_key(
    bind: Engine | Connection, project: Project, org: Org, secret_store: SecretsManagerStore
) -> dict[str, str]:
    if project.org_id != org.id:
        raise HTTPException(status_code=404, detail=f"Project {project.id} not found")
    if project.gateway_key_id is not None:
        reference = _gateway_secret_reference(org, _gateway_key(project, org, secret_store))
        return {MODEL_GATEWAY_URL: reference, MODEL_GATEWAY_API_KEY: reference}
    with Session(bind, expire_on_commit=False) as session:
        repository = ProjectRepository(session)
        repository.lock_gateway_key(project.id)
        locked = repository.get_by_id(org.id, project.id)
        if locked is None:
            raise HTTPException(status_code=404, detail=f"Project {project.id} not found")
        if locked.gateway_key_id is None and locked.gateway_request_id is None:
            raise HTTPException(
                status_code=409,
                detail=f"Project '{locked.handle}' has no Gateway key request; configure its Gateway binding before starting a run",
            )
        reference = _gateway_secret_reference(org, _gateway_key(locked, org, secret_store))
        return {MODEL_GATEWAY_URL: reference, MODEL_GATEWAY_API_KEY: reference}


async def _collected_key_id(project: Project, tracker_copies: set[str]) -> UUID | None:
    if project.gateway_key_id is not None or project.gateway_request_id is None:
        return project.gateway_key_id
    key_id = await requested_key_id(project)
    return (
        key_id
        if f"{tenant_secret_prefix(project.org_id)}{GATEWAY_SECRET_NAME_PREFIX}{key_id}" in tracker_copies
        else None
    )


async def hand_over_gateway_keys(
    org: Org, secret_store: SecretsManagerStore, apply: bool
) -> GatewayKeyHandOverResponse:
    service = project_service()
    projects = [
        project
        for row in service.list_rows(org.id, include_archived=True)
        if (project := service.get(org.id, row.id)) is not None
    ]
    tenant = tenant_secret_prefix(org.id)
    held = set(await secret_store.list_names(f"{tenant}{PROJECT_SERVICE_GATEWAY_SECRET_PREFIX}"))
    copies = set(await secret_store.list_names(f"{tenant}{GATEWAY_SECRET_NAME_PREFIX}"))
    key_ids = [(project, await _collected_key_id(project, copies)) for project in projects]
    keyed = [(project, key_id) for project, key_id in key_ids if key_id is not None]
    copied = [project for project, key_id in keyed if f"{tenant}{GATEWAY_SECRET_NAME_PREFIX}{key_id}" in copies]
    missing = [(project, key_id) for project, key_id in keyed if _gateway_secret_reference(org, key_id) not in held]
    movable = [project for project, _ in missing if project in copied]
    if apply:
        for project in copied:
            _ = await _project_service_gateway_key(project, org, secret_store)
    return GatewayKeyHandOverResponse(
        applied=apply,
        projects_with_key=len(keyed),
        held=len(keyed) - len(missing),
        to_hand_over=len(movable),
        without_tracker_copy=sorted(project.handle for project, _ in missing if project not in movable),
    )
