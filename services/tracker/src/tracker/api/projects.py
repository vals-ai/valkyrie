from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from tracker.auth import (
    LoginIdentity,
    RequestIdentity,
    get_current_login,
    get_current_org,
    get_current_runner,
    get_current_starter,
)
from tracker.database.models import Org, Project
from tracker.exceptions import ProjectHandleTakenError, ProjectNameTakenError
from tracker.repositories.project import ProjectRepository, ProjectRepositoryDependency
from tracker.project_gateway import requests_project_gateway_key, request_project_gateway_key
from tracker.types import (
    GatewayKeyEligibilityResponse,
    NewProjectRequest,
    ProjectCreateRequest,
    ProjectIdResponse,
    ProjectResponse,
    ProjectsResponse,
)

router = APIRouter(prefix="/projects")


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
        project.gateway_request_id = request_project_gateway_key(project, request.budget, request.budget_reason)
        repository.save(project)
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
    project = _project_or_404(repository.get(identity.org.id, handle, "update"), handle)
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
    project = _project_or_404(repository.get(identity.org.id, handle, "update"), handle)
    if project.archived_at is not None:
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
