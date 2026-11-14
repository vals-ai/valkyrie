import hmac
import os
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from contextlib import asynccontextmanager
from functools import cache
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy import Engine, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import InstrumentedAttribute, Session

from projects.database import database_reachable, engine
from projects.models import LIVE, Project
from projects.schemas import (
    Archive,
    GatewayKey,
    GatewayRequest,
    ProjectChanges,
    ProjectCopy,
    ProjectRecord,
    ProjectResponse,
    ProjectsResponse,
)


@cache
def project_service_token() -> str:
    token = os.environ.get("PROJECT_SERVICE_TOKEN", "")
    if not token:
        raise RuntimeError("PROJECT_SERVICE_TOKEN must be set")
    return token


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    _ = project_service_token()
    yield


app = FastAPI(title="Valkyrie project service", lifespan=lifespan)


def machine_caller(authorization: Annotated[str, Header()] = "") -> None:
    expected = f"Bearer {project_service_token()}"
    if not hmac.compare_digest(authorization.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Invalid project service token")


def _list(session: Session, org_id: UUID, include_archived: bool, include_deleted: bool = False) -> ProjectsResponse:
    query = select(Project).where(Project.org_id == org_id).order_by(Project.handle)
    if not include_deleted:
        query = query.where(LIVE)
    if not include_archived:
        query = query.where(Project.archived_at.is_(None))
    return ProjectsResponse(projects=[ProjectResponse.model_validate(row) for row in session.scalars(query)])


@app.get(
    "/orgs/{org_id}/projects",
    dependencies=[Depends(machine_caller)],
    description="List an org's projects by handle, in the same body as Tracker's `GET /projects`. "
    "Archived projects are left out unless `include_archived` is true, and deleted ones unless `include_deleted` is.",
)
def list_projects(
    org_id: UUID,
    bound: Annotated[Engine, Depends(engine)],
    include_archived: bool = False,
    include_deleted: bool = False,
) -> ProjectsResponse:
    with Session(bound) as session:
        return _list(session, org_id, include_archived, include_deleted)


def _project(session: Session, org_id: UUID, project_id: UUID) -> Project:
    project = session.scalar(select(Project).where(Project.id == project_id, Project.org_id == org_id, LIVE))
    if project is None:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found")
    return project


def _commit(session: Session, project: Project) -> ProjectRecord:
    try:
        session.commit()
    except IntegrityError as error:
        raise HTTPException(status_code=409, detail="The project's ID, handle or name is taken") from error
    return ProjectRecord.model_validate(project)


@app.post(
    "/orgs/{org_id}/projects",
    status_code=201,
    dependencies=[Depends(machine_caller)],
    description="Create one project with the ID Tracker gave it and return it with its Gateway IDs. "
    "Returns 409 when the ID is taken in any org or the handle or name is taken in this org.",
)
def create_project(org_id: UUID, body: ProjectCopy, bound: Annotated[Engine, Depends(engine)]) -> ProjectRecord:
    with Session(bound) as session:
        project = Project(org_id=org_id, **body.model_dump())
        session.add(project)
        return _commit(session, project)


@app.get(
    "/orgs/{org_id}/projects/{project_id}",
    dependencies=[Depends(machine_caller)],
    description="Return one of the org's projects with its Gateway IDs. Returns 404 when the org has no project with that ID.",
)
def get_project(org_id: UUID, project_id: UUID, bound: Annotated[Engine, Depends(engine)]) -> ProjectRecord:
    with Session(bound) as session:
        return ProjectRecord.model_validate(_project(session, org_id, project_id))


@app.put(
    "/orgs/{org_id}/projects/{project_id}",
    dependencies=[Depends(machine_caller)],
    description="Save the metadata Tracker's `PUT /projects/{handle}` changes and return the project. "
    "Returns 404 for an unknown ID and 409 when the name is taken.",
)
def save_project(
    org_id: UUID, project_id: UUID, body: ProjectChanges, bound: Annotated[Engine, Depends(engine)]
) -> ProjectRecord:
    with Session(bound) as session:
        project = _project(session, org_id, project_id)
        for field, value in body.model_dump().items():
            setattr(project, field, value)
        return _commit(session, project)


@app.post(
    "/orgs/{org_id}/projects/{project_id}/archive",
    dependencies=[Depends(machine_caller)],
    description="Archive a project at the time sent unless it is archived already, and return it. "
    "Returns 404 when the org has no project with that ID.",
)
def archive_project(
    org_id: UUID, project_id: UUID, body: Archive, bound: Annotated[Engine, Depends(engine)]
) -> ProjectRecord:
    with Session(bound) as session:
        _ = session.execute(
            update(Project)
            .where(Project.id == project_id, Project.org_id == org_id, LIVE, Project.archived_at.is_(None))
            .values(archived_at=body.archived_at)
        )
        session.commit()
        return ProjectRecord.model_validate(_project(session, org_id, project_id))


@app.post(
    "/orgs/{org_id}/projects/{project_id}/unarchive",
    dependencies=[Depends(machine_caller)],
    description="Let new runs use an archived project again and return it. Returns 404 when the org has no project with that ID.",
)
def unarchive_project(org_id: UUID, project_id: UUID, bound: Annotated[Engine, Depends(engine)]) -> ProjectRecord:
    with Session(bound) as session:
        project = _project(session, org_id, project_id)
        project.archived_at = None
        return _commit(session, project)


@app.post(
    "/orgs/{org_id}/projects/{project_id}/delete",
    status_code=204,
    dependencies=[Depends(machine_caller)],
    description="Mark a project deleted, which hides it from every route and frees its handle and name for a new project. "
    "Returns 404 when the org has no live project with that ID.",
)
def delete_project(org_id: UUID, project_id: UUID, bound: Annotated[Engine, Depends(engine)]) -> None:
    with Session(bound) as session:
        live = update(Project).where(Project.id == project_id, Project.org_id == org_id, LIVE)
        if session.scalar(live.values(deleted_at=datetime.now(UTC)).returning(Project.id)) is None:
            raise HTTPException(status_code=404, detail=f"Project '{project_id}' not found")
        session.commit()


@app.post(
    "/orgs/{org_id}/projects/{project_id}/gateway-key-id",
    dependencies=[Depends(machine_caller)],
    description="Set a project's Gateway key ID only when it has none, and return the project with the key ID that won. "
    "Returns 404 when the org has no project with that ID.",
)
def set_gateway_key(
    org_id: UUID, project_id: UUID, body: GatewayKey, bound: Annotated[Engine, Depends(engine)]
) -> ProjectRecord:
    return _set_once(bound, org_id, project_id, Project.gateway_key_id, body.gateway_key_id)


@app.post(
    "/orgs/{org_id}/projects/{project_id}/gateway-request",
    dependencies=[Depends(machine_caller)],
    description="Set a project's Gateway request ID only when it has none, and return the project with the request ID "
    "that won. Returns 404 when the org has no project with that ID.",
)
def set_gateway_request(
    org_id: UUID, project_id: UUID, body: GatewayRequest, bound: Annotated[Engine, Depends(engine)]
) -> ProjectRecord:
    return _set_once(bound, org_id, project_id, Project.gateway_request_id, body.gateway_request_id)


def _set_once(
    bound: Engine, org_id: UUID, project_id: UUID, column: InstrumentedAttribute[UUID | None], value: UUID
) -> ProjectRecord:
    with Session(bound) as session:
        _ = session.execute(
            update(Project)
            .where(Project.id == project_id, Project.org_id == org_id, LIVE, column.is_(None))
            .values({column: value})
        )
        session.commit()
        return ProjectRecord.model_validate(_project(session, org_id, project_id))


@app.get(
    "/health",
    description="Report whether the project service and its database are up. Returns 503 when the database is unreachable.",
)
def health(bound: Annotated[Engine, Depends(engine)]) -> dict[str, str]:
    if not database_reachable(bound):
        raise HTTPException(status_code=503, detail="Database is not accessible")
    return {"status": "ok"}
