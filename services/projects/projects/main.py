import asyncio
import hmac
import logging
import os
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from contextlib import asynccontextmanager
from functools import cache
from typing import Annotated
from uuid import UUID, uuid4

import boto3
import httpx
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, ConnectionError, HTTPClientError, SSLError
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import Engine, select, update
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import InstrumentedAttribute, Session

from projects.cost import router as cost_router
from projects.database import database_reachable, engine
from projects.gateway import (
    TIMEOUT_SECONDS,
    GatewayClient,
    KeyRequestStatus,
    KeyStatus,
    SecretStore,
    gateway,
    new_secret,
    push_budgets,
    secret_name,
    tracker_copy_name,
    VerifierOutcome,
    verifier,
)
from projects.models import LIVE, LimitRequestStatus, Project, ProjectLimit, ProjectLimitRequest
from projects.schemas import (
    Archive,
    GatewayKey,
    GatewayKeySecret,
    GatewayRequest,
    LimitDecision,
    LimitInput,
    LimitRequestInput,
    LimitRequestRecord,
    ProjectChanges,
    ProjectCopy,
    ProjectRecord,
    ProjectResponse,
    ProjectsResponse,
    QueuedLimitRequest,
)

logger = logging.getLogger(__name__)
PUSH_INTERVAL_SECONDS = 30


@cache
def project_service_token() -> str:
    token = os.environ.get("PROJECT_SERVICE_TOKEN", "")
    if not token:
        raise RuntimeError("PROJECT_SERVICE_TOKEN must be set")
    return token


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    _ = project_service_token()
    pusher = asyncio.create_task(push_forever())
    yield
    _ = pusher.cancel()


async def push_forever() -> None:
    while True:
        try:
            await asyncio.to_thread(push_budgets, engine())
        except Exception:
            logger.exception("Gateway budget outbox drain failed")
        await asyncio.sleep(PUSH_INTERVAL_SECONDS)


app = FastAPI(title="Valkyrie project service", lifespan=lifespan)
logger = logging.getLogger(__name__)
TRANSIENT_AWS_CODES = {"InternalServiceError", "ThrottlingException"}


@app.exception_handler(httpx.HTTPError)
async def gateway_unavailable(_request: Request, error: Exception) -> JSONResponse:
    logger.warning("Gateway call failed: %r", error)
    if (
        isinstance(error, httpx.HTTPStatusError)
        and error.response.status_code != 429
        and error.response.status_code < 500
    ):
        detail = f"Gateway refused the request with HTTP {error.response.status_code}"
        return JSONResponse(status_code=500, content={"detail": detail})
    return JSONResponse(status_code=503, content={"detail": "Gateway unavailable, try again"})


def _secrets_retryable(error: Exception) -> bool:
    if isinstance(error, HTTPClientError | ConnectionError) and not isinstance(error, SSLError):
        return True
    return isinstance(error, ClientError) and error.response.get("Error", {}).get("Code", "") in TRANSIENT_AWS_CODES


@app.exception_handler(BotoCoreError)
@app.exception_handler(ClientError)
async def secrets_failed(_request: Request, error: Exception) -> JSONResponse:
    logger.warning("Secrets Manager call failed: %r", error)
    if _secrets_retryable(error):
        return JSONResponse(status_code=503, content={"detail": "Secrets Manager unavailable, try again"})
    return JSONResponse(status_code=500, content={"detail": "Secrets Manager refused the request"})


def machine_caller(authorization: Annotated[str, Header()] = "") -> None:
    expected = f"Bearer {project_service_token()}"
    if not hmac.compare_digest(authorization.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Invalid project service token")


@cache
def secret_store() -> SecretStore:
    config = Config(
        connect_timeout=TIMEOUT_SECONDS,
        read_timeout=TIMEOUT_SECONDS,
        retries={"total_max_attempts": 2, "mode": "standard"},
    )
    return SecretStore(boto3.client("secretsmanager", config=config), os.environ["MODEL_GATEWAY_URL"])  # pyright: ignore[reportUnknownMemberType]


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


@app.put(
    "/orgs/{org_id}/projects/{project_id}/limit",
    dependencies=[Depends(machine_caller)],
    description="Set a project's limit and push it to the Gateway's `PUT /machine/projects/{id}/budget`, "
    "retried until the Gateway accepts it. Returns 404 when the project is not in this org.",
)
def set_limit(
    org_id: UUID,
    project_id: UUID,
    body: LimitInput,
    bound: Annotated[Engine, Depends(engine)],
    background: BackgroundTasks,
) -> LimitInput:
    with Session(bound) as session:
        _ = _project(session, org_id, project_id)
        _save_limit(session, project_id, body)
    background.add_task(push_budgets, bound, project_id)
    return body


def _save_limit(session: Session, project_id: UUID, body: LimitInput, *, raise_only: bool = False) -> None:
    insert = postgresql.insert if session.get_bind().dialect.name == "postgresql" else sqlite.insert
    values = {
        "limit_dollars": body.limit_dollars,
        "reason": body.reason,
        "gateway_confirmed_limit": None,
        "updated_at": datetime.now(UTC),
    }
    upsert = insert(ProjectLimit).values(project_id=project_id, version=1, **values)
    saved = values | {"version": ProjectLimit.version + 1}
    changed = upsert.on_conflict_do_update(
        index_elements=[ProjectLimit.project_id],
        set_=saved,
        where=ProjectLimit.limit_dollars < body.limit_dollars if raise_only else None,
    )
    if raise_only:
        if session.scalar(changed.returning(ProjectLimit.project_id)) is None:
            raise HTTPException(status_code=409, detail="Requested limit must exceed the project's current limit")
    else:
        _ = session.execute(changed)
    session.commit()


@app.post(
    "/orgs/{org_id}/projects/{project_id}/limit-requests",
    status_code=201,
    dependencies=[Depends(machine_caller)],
    description="File a request to change a project's limit, pending until it is approved or denied. "
    "Returns 404 when the project is not in this org.",
)
def file_limit_request(
    org_id: UUID, project_id: UUID, body: LimitRequestInput, bound: Annotated[Engine, Depends(engine)]
) -> LimitRequestRecord:
    with Session(bound) as session:
        _ = _project(session, org_id, project_id)
        item = ProjectLimitRequest(
            id=uuid4(),
            project_id=project_id,
            status=LimitRequestStatus.PENDING,
            created_at=datetime.now(UTC),
            **body.model_dump(),
        )
        session.add(item)
        session.commit()
        return LimitRequestRecord.model_validate(item)


@app.get(
    "/orgs/{org_id}/projects/{project_id}/limit-requests",
    dependencies=[Depends(machine_caller)],
    description="List a project's limit requests, newest first. Returns 404 when the project is not in this org.",
)
def list_limit_requests(
    org_id: UUID, project_id: UUID, bound: Annotated[Engine, Depends(engine)]
) -> list[LimitRequestRecord]:
    with Session(bound) as session:
        _ = _project(session, org_id, project_id)
        query = select(ProjectLimitRequest).where(ProjectLimitRequest.project_id == project_id)
        rows = session.scalars(query.order_by(ProjectLimitRequest.created_at.desc()))
        return [LimitRequestRecord.model_validate(row) for row in rows]


@app.post(
    "/orgs/{org_id}/projects/{project_id}/limit-requests/{request_id}/approve",
    dependencies=[Depends(machine_caller)],
    description="Approve a pending limit request: save its limit as `PUT .../limit` does and push it to the Gateway. "
    "Returns 404 for an unknown request and 409 when already decided or no longer a raise.",
)
def approve_limit_request(
    org_id: UUID,
    project_id: UUID,
    request_id: UUID,
    body: LimitDecision,
    bound: Annotated[Engine, Depends(engine)],
    background: BackgroundTasks,
) -> LimitRequestRecord:
    with Session(bound) as session:
        item = _decide(session, org_id, project_id, request_id, LimitRequestStatus.APPROVED, body)
        _save_limit(
            session, project_id, LimitInput(limit_dollars=item.limit_dollars, reason=item.reason), raise_only=True
        )
        record = LimitRequestRecord.model_validate(item)
    background.add_task(push_budgets, bound, project_id)
    return record


@app.post(
    "/orgs/{org_id}/projects/{project_id}/limit-requests/{request_id}/deny",
    dependencies=[Depends(machine_caller)],
    description="Deny a pending limit request and leave the limit as it is. "
    "Returns 404 for an unknown request and 409 when it is already decided.",
)
def deny_limit_request(
    org_id: UUID, project_id: UUID, request_id: UUID, body: LimitDecision, bound: Annotated[Engine, Depends(engine)]
) -> LimitRequestRecord:
    with Session(bound) as session:
        item = _decide(session, org_id, project_id, request_id, LimitRequestStatus.DENIED, body)
        session.commit()
        return LimitRequestRecord.model_validate(item)


@app.get(
    "/limit-requests",
    dependencies=[Depends(machine_caller)],
    description="List every org's limit requests on live projects, newest first, optionally with one status.",
)
def queue_limit_requests(
    bound: Annotated[Engine, Depends(engine)], status: LimitRequestStatus | None = None
) -> list[QueuedLimitRequest]:
    query = select(ProjectLimitRequest, Project.org_id).join(Project, Project.id == ProjectLimitRequest.project_id)
    query = query.where(LIVE).order_by(ProjectLimitRequest.created_at.desc())
    if status is not None:
        query = query.where(ProjectLimitRequest.status == status)
    with Session(bound) as session:
        rows = session.execute(query)
        return [_queued(item, org) for item, org in rows]


def _queued(item: ProjectLimitRequest, org_id: UUID) -> QueuedLimitRequest:
    return QueuedLimitRequest.model_validate(LimitRequestRecord.model_validate(item).__dict__ | {"org_id": org_id})


def _decide(
    session: Session, org_id: UUID, project_id: UUID, request_id: UUID, status: LimitRequestStatus, body: LimitDecision
) -> ProjectLimitRequest:
    _ = _project(session, org_id, project_id)
    item = session.get(ProjectLimitRequest, request_id)
    if item is None or item.project_id != project_id:
        raise HTTPException(status_code=404, detail=f"Limit request '{request_id}' not found")
    pending = update(ProjectLimitRequest).where(
        ProjectLimitRequest.id == request_id, ProjectLimitRequest.status == LimitRequestStatus.PENDING
    )
    decided = pending.values(status=status, decided_by_email=body.decided_by_email, decided_at=datetime.now(UTC))
    if session.scalar(decided.returning(ProjectLimitRequest.id)) is None:
        raise HTTPException(status_code=409, detail=f"Limit request '{request_id}' is already {item.status}")
    session.refresh(item)
    return item


app.include_router(cost_router, dependencies=[Depends(machine_caller)])


@app.get(
    "/health",
    description="Report whether the project service and its database are up. Returns 503 when the database is unreachable.",
)
def health(bound: Annotated[Engine, Depends(engine)]) -> dict[str, str]:
    if not database_reachable(bound):
        raise HTTPException(status_code=503, detail="Database is not accessible")
    return {"status": "ok"}


@app.post(
    "/orgs/{org_id}/projects/{project_id}/gateway-key",
    dependencies=[Depends(machine_caller)],
    description="Mint the project's Gateway key secret, store it, send the Gateway only its hash and return the key ID. "
    "Returns 409 while the request is unapproved or the key was collected elsewhere.\n\n"
    "Returns 503 to retry when the Gateway holds another secret's hash. The unused secret stays stored but unconfirmed, "
    "and every route treats it as missing until Tracker hands its own over.\n\n"
    "Once the Gateway accepts the hash, a copy goes under Tracker's `model-gateway-` name so an older Tracker can start runs.\n\n"
    "Returns 503 when the Gateway or Secrets Manager is unavailable, and 500 when the Gateway refuses the request.",
)
def mint_gateway_key(
    org_id: UUID,
    project_id: UUID,
    bound: Annotated[Engine, Depends(engine)],
    client: Annotated[GatewayClient, Depends(gateway)],
    store: Annotated[SecretStore, Depends(secret_store)],
) -> GatewayKey:
    with Session(bound) as session:
        project = _project(session, org_id, project_id)
        request_id, key_id = project.gateway_request_id, project.gateway_key_id
    if key_id is not None:
        if not _held_by_gateway(client, key_id, store.get(secret_name(org_id, key_id))):
            raise HTTPException(status_code=409, detail="Gateway key was collected elsewhere")
        return GatewayKey(gateway_key_id=key_id)
    if request_id is None:
        raise HTTPException(status_code=409, detail="Project has no Gateway key request")
    status, key_id = client.request_status(request_id)
    if status != KeyRequestStatus.APPROVED or key_id is None:
        raise HTTPException(status_code=409, detail=f"Gateway key request is {status}")
    name = secret_name(org_id, key_id)
    state, revision = client.key_state(key_id)
    if state not in (KeyStatus.PENDING_COLLECTION, KeyStatus.ACTIVE):
        raise HTTPException(status_code=409, detail="Gateway key was collected elsewhere")
    secret = store.keep_first(name, org_id, new_secret()) if state == KeyStatus.PENDING_COLLECTION else store.get(name)
    if secret is None:
        raise HTTPException(status_code=409, detail="Gateway key was collected elsewhere")
    outcome = client.set_verifier(key_id, revision, verifier(secret))
    if outcome == VerifierOutcome.CHANGED:
        raise HTTPException(status_code=409, detail="Gateway key changed while minting; retry")
    if outcome == VerifierOutcome.SET:
        _ = store.keep_first(tracker_copy_name(org_id, key_id), org_id, secret)
    with Session(bound) as session:
        _project(session, org_id, project_id).gateway_key_id = key_id
        session.commit()
    if outcome == VerifierOutcome.ANOTHER:
        raise HTTPException(status_code=503, detail="Gateway key was collected elsewhere; retry to hand it over")
    return GatewayKey(gateway_key_id=key_id)


@app.put(
    "/orgs/{org_id}/projects/{project_id}/gateway-key/secret",
    dependencies=[Depends(machine_caller)],
    status_code=204,
    description="Take over the secret Tracker already collected for the project's own key. "
    "A stored secret is replaced only when the Gateway holds the new one's hash.\n\n"
    "Returns 409 for another key ID, or when the stored secret differs and the Gateway does not hold the new one.\n\n"
    "Returns 503 when the Gateway or Secrets Manager is unavailable.",
)
def hand_over_gateway_key_secret(
    org_id: UUID,
    project_id: UUID,
    body: GatewayKeySecret,
    bound: Annotated[Engine, Depends(engine)],
    client: Annotated[GatewayClient, Depends(gateway)],
    store: Annotated[SecretStore, Depends(secret_store)],
) -> None:
    with Session(bound) as session:
        key_id = _project(session, org_id, project_id).gateway_key_id
    if key_id != body.gateway_key_id:
        raise HTTPException(status_code=409, detail="Secret is not for the project's Gateway key")
    name = secret_name(org_id, body.gateway_key_id)
    stored = store.keep_first(name, org_id, body.secret)
    if stored != body.secret:
        if not _held_by_gateway(client, body.gateway_key_id, body.secret):
            raise HTTPException(status_code=409, detail="Project already has a stored Gateway key secret")
        store.replace(name, body.secret)


def _held_by_gateway(client: GatewayClient, key_id: UUID, secret: str | None) -> bool:
    if secret is None:
        return False
    state, revision = client.key_state(key_id)
    return state == KeyStatus.ACTIVE and client.set_verifier(key_id, revision, verifier(secret)) == VerifierOutcome.SET
