import asyncio
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from tracker.auth import (
    NOT_A_MEMBER,
    TENANT_HEADER,
    LoginIdentity,
    RequestIdentity,
    get_current_login,
    get_current_org,
    get_current_runner,
    get_current_starter,
)
from tracker.config import DESCOPE_ORG_ADMIN_ROLE, PLATFORM_ADMIN_PERMISSION
from tracker.database.models import Org, Project
from tracker.exceptions import (
    ProjectHandleTakenError,
    ProjectLimitRequestDecidedError,
    ProjectLimitRequestNotFoundError,
    ProjectNameTakenError,
)
from tracker.repositories.cost import CostRepositoryDependency
from tracker.repositories.organization import OrganizationRepository, OrganizationRepositoryDependency
from tracker.repositories.project import ProjectRepository, ProjectRepositoryDependency
from tracker.api.secrets import secret_store
from tracker.clients.model_gateway.machine import request_project_gateway_key
from tracker.services.costs import project_budget
from tracker.services.gateway.keys import hand_over_gateway_keys, requests_project_gateway_key
from tracker.types import (
    ErrorMessage,
    GatewayKeyEligibilityResponse,
    GatewayKeyHandOverResponse,
    LimitDecision,
    LimitRequest,
    LimitRequestInput,
    LimitRequestStatus,
    NewProjectRequest,
    ProjectBudget,
    ProjectCreateRequest,
    ProjectIdResponse,
    ProjectLimit,
    ProjectResponse,
    ProjectsResponse,
    QueuedLimitRequest,
)

router = APIRouter(prefix="/projects")
limit_requests_router = APIRouter(prefix="/limit-requests")


PROJECT_NOT_FOUND: dict[int | str, dict[str, Any]] = {
    404: {"model": ErrorMessage, "description": "The organization has no project with this handle."}
}


def reject_archived_project(project: Project) -> Project:
    if project.archived_at is not None:
        raise HTTPException(
            status_code=409,
            detail=f"Project '{project.handle}' is archived, so new runs can't use it; unarchive it first",
        )
    return project


def reject_missing_project(org: Org, project: str | None) -> None:
    if org.requires_project and project is None:
        raise HTTPException(
            status_code=422,
            detail="This organization requires a project for every run. Pass --project HANDLE "
            "(or project= in the SDK); create one with: valkyrie project create HANDLE --name NAME "
            "--spend-type development|internal|customer --subcategory SUBCATEGORY",
        )


def find_project(repository: ProjectRepository, org_id: UUID, handle: str) -> Project:
    project = repository.find(org_id, handle)
    if project is not None:
        return reject_archived_project(project)
    handles = ", ".join(row.handle for row in repository.list_all(org_id)) or "none"
    raise HTTPException(
        status_code=422,
        detail=f"Project '{handle}' does not exist. Existing projects: {handles}. "
        f"Create it with: valkyrie project create {handle} --name NAME "
        "--spend-type development|internal|customer --subcategory SUBCATEGORY --budget AMOUNT --budget-reason REASON",
    )


class PreliminaryProjectCache:
    def __init__(self) -> None:
        self._pending: dict[tuple[UUID, str], asyncio.Task[Project]] = {}

    async def get(self, repository: ProjectRepository, org_id: UUID, handle: str) -> Project:
        key = org_id, handle
        task = self._pending.get(key)
        if task is None:
            task = asyncio.create_task(self._load(repository, key))
            task.add_done_callback(self._observe)
            self._pending[key] = task
        project = await asyncio.shield(task)
        return project.model_copy(deep=True)

    @staticmethod
    def _observe(task: asyncio.Task[Project]) -> None:
        if not task.cancelled():
            task.exception()

    async def _load(self, repository: ProjectRepository, key: tuple[UUID, str]) -> Project:
        try:
            project = await asyncio.to_thread(find_project, repository, *key)
            return project
        finally:
            self._pending.pop(key, None)


preliminary_projects = PreliminaryProjectCache()


def _project_or_404(project: Project | None, handle: str | UUID) -> Project:
    if project is None:
        raise HTTPException(status_code=404, detail=f"Project '{handle}' not found")
    return project


@router.post(
    "",
    response_model=ProjectResponse,
    responses={409: {"description": "The handle or the name is already taken"}},
    description="Create a project for runs with a handle and a name unique within the organization. Request its Gateway key in provision or automatic mode.",
)
def create_project(
    request: NewProjectRequest,
    repository: ProjectRepositoryDependency,
    identity: RequestIdentity = Depends(get_current_starter),
) -> ProjectResponse:
    if not requests_project_gateway_key(identity.org):
        return ProjectResponse.from_row(_insert_project(repository, identity, request))
    if request.budget is not None and request.budget_reason is not None:
        project = _insert_project(repository, identity, request)
        repository.set_gateway_request(
            project, asyncio.run(request_project_gateway_key(project, request.budget, request.budget_reason))
        )
        return ProjectResponse.from_row(project)
    raise HTTPException(
        status_code=422,
        detail=f"Project '{request.handle}' needs --budget and --budget-reason: "
        "creating it requests its main Gateway key, which carries that budget",
    )


def _insert_project(repository: ProjectRepository, identity: RequestIdentity, request: NewProjectRequest) -> Project:
    project = Project(
        org_id=identity.org.id,
        created_by_email=identity.email,
        **request.model_dump(exclude={"budget", "budget_reason"}),
    )
    try:
        repository.create(project)
    except (ProjectHandleTakenError, ProjectNameTakenError) as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return project


@router.put(
    "/{handle}",
    response_model=ProjectResponse,
    responses={404: {"description": "The project was not found"}, 409: {"description": "Another project has the name"}},
    description="Change a project's shared metadata. Linked spend is reclassified under the current values.",
)
def update_project(
    handle: str,
    request: ProjectCreateRequest,
    repository: ProjectRepositoryDependency,
    identity: RequestIdentity = Depends(get_current_starter),
) -> ProjectResponse:
    if request.handle != handle:
        raise HTTPException(status_code=422, detail="Project handle in the body must match the path")
    project = _project_or_404(repository.get(identity.org.id, handle), handle)
    project.name = request.name
    project.spend_type = request.spend_type
    project.subcategory = request.subcategory
    project.customer = request.customer
    project.extra_tags = request.extra_tags
    try:
        repository.update(project)
    except ProjectNameTakenError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return ProjectResponse.from_row(project)


@router.delete(
    "/{handle}",
    response_model=ProjectResponse,
    responses={404: {"description": "The project was not found"}, 409: {"description": "The project has run history"}},
    description="Delete an unused project. Any historical run link keeps it for spend attribution.",
)
def delete_project(
    handle: str,
    repository: ProjectRepositoryDependency,
    identity: RequestIdentity = Depends(get_current_starter),
) -> ProjectResponse:
    project = _project_or_404(repository.get(identity.org.id, handle), handle)
    response = ProjectResponse.from_row(project)
    if not repository.delete(project):
        raise HTTPException(status_code=409, detail=f"Project '{handle}' has run history and cannot be deleted")
    return response


@router.post(
    "/{handle}/archive",
    response_model=ProjectResponse,
    responses={404: {"description": "The project was not found"}},
    description="Archive a project so new runs can't use it. Its runs and spend stay as they are, and unarchive brings it back.",
)
def archive_project(
    handle: str,
    repository: ProjectRepositoryDependency,
    identity: RequestIdentity = Depends(get_current_starter),
) -> ProjectResponse:
    project = _project_or_404(repository.get(identity.org.id, handle), handle)
    if project.archived_at is None:
        project.archived_at = datetime.now(UTC)
    repository.save(project)
    return ProjectResponse.from_row(project)


@router.post(
    "/{handle}/unarchive",
    response_model=ProjectResponse,
    responses={404: {"description": "The project was not found"}},
    description="Let new runs use an archived project again. A project that is not archived is returned as it is.",
)
def unarchive_project(
    handle: str,
    repository: ProjectRepositoryDependency,
    identity: RequestIdentity = Depends(get_current_starter),
) -> ProjectResponse:
    project = _project_or_404(repository.get(identity.org.id, handle), handle)
    project.archived_at = None
    repository.save(project)
    return ProjectResponse.from_row(project)


@router.get(
    "",
    response_model=ProjectsResponse,
    description="List the organization's projects by handle, leaving out archived ones unless include_archived is set.",
)
def list_projects(
    repository: ProjectRepositoryDependency, org: Org = Depends(get_current_org), include_archived: bool = False
) -> ProjectsResponse:
    return ProjectsResponse(
        projects=[ProjectResponse.from_row(row) for row in repository.list_all(org.id, include_archived)]
    )


@limit_requests_router.get(
    "",
    response_model=list[QueuedLimitRequest],
    responses={403: {"description": "Platform administrator required"}},
    description="List every org's limit requests from the project service for platform admins, newest first.",
)
def queue_limit_requests(
    repository: ProjectRepositoryDependency,
    organizations: OrganizationRepositoryDependency,
    status: LimitRequestStatus | None = None,
    login: LoginIdentity = Depends(get_current_login),
) -> list[QueuedLimitRequest]:
    _require_platform_admin(login)
    rows = repository.limit_request_queue(status.value if status else None)
    named = [(row, organizations.get(UUID(row["org_id"]))) for row in rows]
    return [QueuedLimitRequest.model_validate(row | {"organization": org.name}) for row, org in named if org]


@router.get(
    "/{handle}",
    response_model=ProjectResponse,
    description="Return one of the organization's projects by handle, archived ones included.",
    responses=PROJECT_NOT_FOUND,
)
def get_project(
    handle: str, repository: ProjectRepositoryDependency, org: Org = Depends(get_current_org)
) -> ProjectResponse:
    return ProjectResponse.from_row(repository.resolve(org.id, handle))


@router.get(
    "/{handle}/budget",
    response_model=ProjectBudget,
    description=(
        "Return a project's limit and its all-time spend by cost type, archived projects included.\n"
        "\n"
        "Spend adds Tracker's sandbox, search and reported costs to the Gateway's model spend.\n"
        "\n"
        "`limit_dollars` is null when no limit is set. When the Gateway is unavailable, model spend is left out "
        "and `model_spend_available` is false."
    ),
    responses={
        **PROJECT_NOT_FOUND,
        503: {"model": ErrorMessage, "description": "The project service is unavailable."},
    },
)
async def get_project_budget(
    handle: str,
    repository: ProjectRepositoryDependency,
    costs: CostRepositoryDependency,
    org: Org = Depends(get_current_org),
) -> ProjectBudget:
    return await project_budget(costs, repository, org.id, handle)


@router.get(
    "/by-id/{project_id}",
    response_model=ProjectIdResponse,
    description="Look up a project by ID within the caller's organization.",
)
def get_project_by_id(
    project_id: UUID, repository: ProjectRepositoryDependency, org: Org = Depends(get_current_org)
) -> ProjectIdResponse:
    project = _project_or_404(repository.get_by_id(org.id, project_id), project_id)
    return ProjectIdResponse(id=project.id, org_id=project.org_id)


@router.put(
    "/by-id/{project_id}/limit",
    response_model=ProjectLimit,
    responses={
        403: {"model": ErrorMessage, "description": "The caller is not an admin of the org or a platform admin."},
        404: {"model": ErrorMessage, "description": "Tracker has no org with org_id, or the org has no such project."},
    },
    description="Set a project's limit in the project service, as an admin of the organization named by org_id "
    "or a platform admin.",
)
def set_project_limit(
    project_id: UUID,
    org_id: UUID,
    request: ProjectLimit,
    repository: ProjectRepositoryDependency,
    organizations: OrganizationRepositoryDependency,
    login: LoginIdentity = Depends(get_current_login),
) -> ProjectLimit:
    org = organizations.get(org_id)
    if org is None:
        raise HTTPException(status_code=404, detail="Not found")
    if not _may_set_limit(login, org):
        raise HTTPException(
            status_code=403, detail=f"Only {DESCOPE_ORG_ADMIN_ROLE}s of {org.name} or platform admins can set a limit"
        )
    project = _project_or_404(repository.get_by_id(org.id, project_id), project_id)
    repository.set_limit(project, request)
    return request


def _may_set_limit(login: LoginIdentity, org: Org) -> bool:
    is_org_admin = DESCOPE_ORG_ADMIN_ROLE in login.organizations.get(org.name, ())
    return is_org_admin or PLATFORM_ADMIN_PERMISSION in login.permissions


def platform_admin_org(
    request: Request, organizations: OrganizationRepositoryDependency, login: LoginIdentity = Depends(get_current_login)
) -> Org:
    _require_platform_admin(login)
    return _requested_org(request, organizations, login)


def member_or_platform_admin_org(
    request: Request, organizations: OrganizationRepositoryDependency, login: LoginIdentity = Depends(get_current_login)
) -> Org:
    org = _requested_org(request, organizations, login)
    if PLATFORM_ADMIN_PERMISSION not in login.permissions:
        _require_member(login, org)
    return org


def member_org(
    request: Request, organizations: OrganizationRepositoryDependency, login: LoginIdentity = Depends(get_current_login)
) -> Org:
    org = _requested_org(request, organizations, login)
    _require_member(login, org)
    return org


def _require_member(login: LoginIdentity, org: Org) -> None:
    if org.name not in login.organizations:
        raise HTTPException(status_code=403, detail=NOT_A_MEMBER)


def _requested_org(request: Request, organizations: OrganizationRepository, login: LoginIdentity) -> Org:
    tenant = request.headers.get(TENANT_HEADER)
    name = tenant or login.current_organization
    org = (_org_by_id(organizations, tenant) if tenant else None) or organizations.get_by_name(name)
    if org is None:
        raise HTTPException(status_code=404, detail=f"Organization '{name}' not found")
    return org


def _org_by_id(organizations: OrganizationRepository, value: str) -> Org | None:
    try:
        return organizations.get(UUID(value))
    except ValueError:
        return None


@router.post(
    "/by-id/{project_id}/limit-requests",
    status_code=201,
    response_model=LimitRequest,
    responses={400: {"model": ErrorMessage, "description": "The login has no selected organization."}},
    description="File a request to raise a project's limit in the project service, pending a platform admin's decision.",
)
def file_limit_request(
    project_id: UUID,
    request: LimitRequestInput,
    repository: ProjectRepositoryDependency,
    login: LoginIdentity = Depends(get_current_login),
    org: Org = Depends(member_org),
) -> LimitRequest:
    project = _project_or_404(repository.get_by_id(org.id, project_id), project_id)
    body = request.model_dump(mode="json") | {"requested_by_email": login.email}
    try:
        return repository.limit_requests(project, "POST", body=body)
    except ProjectLimitRequestNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ProjectLimitRequestDecidedError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


def _require_platform_admin(login: LoginIdentity) -> None:
    if PLATFORM_ADMIN_PERMISSION not in login.permissions:
        raise HTTPException(status_code=403, detail="Platform administrator required")


@router.get(
    "/by-id/{project_id}/limit-requests",
    response_model=list[LimitRequest],
    responses={400: {"model": ErrorMessage, "description": "The login has no selected organization."}},
    description="List a project's limit requests from the project service, newest first.",
)
def list_limit_requests(
    project_id: UUID, repository: ProjectRepositoryDependency, org: Org = Depends(member_or_platform_admin_org)
) -> list[LimitRequest]:
    project = _project_or_404(repository.get_by_id(org.id, project_id), project_id)
    try:
        return repository.limit_requests(project, "GET")
    except ProjectLimitRequestNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ProjectLimitRequestDecidedError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post(
    "/by-id/{project_id}/limit-requests/{request_id}/{decision}",
    response_model=LimitRequest,
    responses={
        400: {"model": ErrorMessage, "description": "The login has no selected organization."},
        403: {"model": ErrorMessage, "description": "The caller is not a platform administrator."},
        409: {"model": ErrorMessage, "description": "The limit request is already decided."},
    },
    description="Approve or deny a pending limit request as a platform admin, in any org named by the tenant header. "
    "An approval sets the project's limit.",
)
def decide_limit_request(
    project_id: UUID,
    request_id: UUID,
    decision: LimitDecision,
    repository: ProjectRepositoryDependency,
    login: LoginIdentity = Depends(get_current_login),
    org: Org = Depends(platform_admin_org),
) -> LimitRequest:
    project = _project_or_404(repository.get_by_id(org.id, project_id), project_id)
    body = {"decided_by_email": login.email}
    try:
        return repository.limit_requests(project, "POST", f"/{request_id}/{decision.value}", body)
    except ProjectLimitRequestNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ProjectLimitRequestDecidedError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.get(
    "/by-id/{project_id}/gateway-key-eligibility",
    response_model=GatewayKeyEligibilityResponse,
    description="Verify the human login and run identity match for a project in the caller's organization.",
)
def get_gateway_key_eligibility(
    project_id: UUID,
    repository: ProjectRepositoryDependency,
    login: LoginIdentity = Depends(get_current_login),
    runner: RequestIdentity = Depends(get_current_runner),
) -> GatewayKeyEligibilityResponse:
    if (
        runner.access_key_id is not None
        or runner.user_id != login.user_id
        or runner.org.name != login.current_organization
    ):
        raise HTTPException(status_code=403, detail="This login cannot request a gateway key for this project")
    project = _project_or_404(repository.get_by_id(runner.org.id, project_id), project_id)
    return GatewayKeyEligibilityResponse(project_id=project.id, org_id=project.org_id, subject=login.user_id)


@router.post(
    "/gateway-keys/hand-over",
    response_model=GatewayKeyHandOverResponse,
    include_in_schema=False,
    description="Hand every project's Tracker-held Gateway key secret to the project service, the way a run does. "
    "Defaults to a dry run that counts secrets by name and reads none.",
)
def hand_over_project_gateway_keys(
    org: Org = Depends(get_current_org),
    apply: Annotated[bool, Query(description="Hand the secrets over. Defaults to a dry run.")] = False,
) -> GatewayKeyHandOverResponse:
    return asyncio.run(hand_over_gateway_keys(org, secret_store(org), apply))
