import asyncio
import hmac
import os
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session

from tracker.api.secrets import secret_store
from tracker.config import AUTH_REQUIRED
from tracker.database.models import RemoteGraderGrant, RemoteGraderRegistration, TaskStatus
from tracker.database.session import get_session
from tracker.logging import get_logger
from tracker.outbound_security import validate_custom_service_destination, validate_service_url_syntax
from tracker.remote_grader import attempt_id_for
from tracker.remote_grader_crypt import decrypt_grader_token, encrypt_grader_token
from tracker.repositories.remote_grader import RemoteGraderRepository
from tracker.runtime.model_gateway import mint_gateway_lease, revoke_gateway_lease
from tracker.runtime.secrets import resolve_secrets

router = APIRouter(prefix="/remote-grader", tags=["remote-grader"])
logger = get_logger(__name__)
_WORKERS = {
    "vcb": "VCB_REMOTE_GRADER_WORKER_API_KEY",
    "vcb-1-100": "VCB100_REMOTE_GRADER_WORKER_API_KEY",
    "cyber-range": "CYBER_REMOTE_GRADER_WORKER_API_KEY",
}


class RemoteGraderRegistrationRequest(BaseModel):
    model_config = ConfigDict(hide_input_in_errors=True)

    authorization: str = Field(repr=False)
    job_id: str
    task_id: str
    attempt_id: str
    allowed_models: list[str] = Field(min_length=1)
    first_pickup_deadline: float = Field(allow_inf_nan=False)


class RemoteGraderRegistrationResponse(BaseModel):
    registration_id: UUID


class RemoteGraderAttempt(BaseModel):
    job_id: str
    task_id: str
    attempt_id: str


class RemoteGraderPickup(RemoteGraderAttempt):
    allowed_models: list[str] = Field(min_length=1)
    ttl_seconds: int = Field(gt=0)


class RemoteGraderCredential(BaseModel):
    gateway_url: str
    token: str
    lease_id: str
    expires_at: float


class RemoteGraderStopped(BaseModel):
    stopped: bool


def _worker_service(request: Request) -> str:
    supplied = request.headers.get("X-Remote-Grader-Worker-Key", "")
    for worker, setting in _WORKERS.items():
        configured = os.environ.get(setting, "")
        if configured and hmac.compare_digest(supplied, configured):
            return worker
    raise HTTPException(status_code=401, detail="Invalid remote grader worker key")


def _registration(
    repository: RemoteGraderRepository, registration_id: UUID, service: str, attempt: RemoteGraderAttempt
) -> RemoteGraderRegistration:
    registration = repository.lock_registration(
        registration_id, service, attempt.job_id, attempt.task_id, attempt.attempt_id
    )
    if registration is None:
        raise HTTPException(status_code=404, detail="Registered remote grader attempt not found")
    return registration


def _root(
    repository: RemoteGraderRepository, grant: RemoteGraderGrant, loop: asyncio.AbstractEventLoop
) -> tuple[str, str]:
    org = repository.get_org(grant.org_id)
    if org is None:
        raise HTTPException(status_code=409, detail="Saved project Gateway binding is unavailable")
    secrets = asyncio.run_coroutine_threadsafe(
        resolve_secrets(
            {"MODEL_GATEWAY_URL": grant.gateway_secret_ref, "MODEL_GATEWAY_API_KEY": grant.gateway_secret_ref},
            secret_store(org),
        ),
        loop,
    ).result()
    url = validate_service_url_syntax(secrets["MODEL_GATEWAY_URL"])
    validate_custom_service_destination(url, org_name=org.name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False)
    return url, secrets["MODEL_GATEWAY_API_KEY"]


def _register(
    bind: Engine | Connection, service: str, body: RemoteGraderRegistrationRequest
) -> RemoteGraderRegistrationResponse:
    with Session(bind=bind) as session:
        repository = RemoteGraderRepository(session)
        grant = repository.lock_grant_by_digest(sha256(body.authorization.encode()).hexdigest())
        if grant is None or grant.worker_service != service:
            raise HTTPException(status_code=403, detail="Invalid remote grader authorization")
        existing = repository.find_registration(service, body.job_id, body.task_id, body.attempt_id)
        if existing is not None:
            if existing.grant_id != grant.id:
                raise HTTPException(status_code=409, detail="Remote attempt belongs to another Tracker attempt")
            if existing.allowed_models != body.allowed_models:
                raise HTTPException(status_code=409, detail="Remote grader lease scope cannot change")
            return RemoteGraderRegistrationResponse(registration_id=existing.id)
        task = repository.get_task(grant.task_row_id)
        if (
            grant.closed_at is not None
            or task is None
            or attempt_id_for(task) != grant.attempt_id
            or task.status not in (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
        ):
            raise HTTPException(status_code=409, detail="Tracker attempt is closed to new registrations")
        registration = RemoteGraderRegistration(
            grant_id=grant.id,
            worker_service=service,
            job_id=body.job_id,
            task_id=body.task_id,
            attempt_id=body.attempt_id,
            allowed_models=body.allowed_models,
            first_pickup_deadline=body.first_pickup_deadline,
        )
        repository.add_registration(registration)
        session.commit()
        return RemoteGraderRegistrationResponse(registration_id=registration.id)


@router.post(
    "/registrations",
    response_model=RemoteGraderRegistrationResponse,
    description="Register a remote grader attempt with its fixed model scope and first-pickup deadline.",
)
async def register_remote_grader(
    body: RemoteGraderRegistrationRequest,
    session: Session = Depends(get_session),
    service: str = Depends(_worker_service),
) -> RemoteGraderRegistrationResponse:
    return await asyncio.to_thread(_register, session.get_bind(), service, body)


def _pickup(
    bind: Engine | Connection,
    registration_id: UUID,
    service: str,
    body: RemoteGraderPickup,
    loop: asyncio.AbstractEventLoop,
) -> RemoteGraderCredential:
    with Session(bind=bind) as session:
        repository = RemoteGraderRepository(session)
        registration = _registration(repository, registration_id, service, body)
        if registration.stopped_at is not None:
            raise HTTPException(status_code=409, detail="Remote grader attempt has stopped")
        if registration.allowed_models != body.allowed_models:
            raise HTTPException(status_code=409, detail="Remote grader lease scope cannot change")
        if registration.gateway_lease_id is not None:
            assert registration.gateway_url is not None
            assert registration.expires_at is not None
            if registration.expires_at <= datetime.now(UTC).timestamp():
                raise HTTPException(status_code=409, detail="Remote grader lease has expired")
            assert registration.token_ciphertext is not None
            return RemoteGraderCredential(
                gateway_url=registration.gateway_url,
                token=decrypt_grader_token(
                    registration.token_ciphertext, record_id=registration.id, kind="registration"
                ),
                lease_id=registration.gateway_lease_id,
                expires_at=registration.expires_at,
            )
        if registration.first_pickup_deadline <= datetime.now(UTC).timestamp():
            raise HTTPException(status_code=409, detail="Remote grader first pickup deadline has passed")
        grant = repository.get_grant(registration.grant_id)
        assert grant is not None
        url, api_key = _root(repository, grant, loop)
        task = repository.get_task(grant.task_row_id)
        assert task is not None
        issued = asyncio.run_coroutine_threadsafe(
            mint_gateway_lease(
                url,
                api_key,
                run_id=str(task.benchmark),
                task_id=task.task_id,
                attempt_id=grant.attempt_id,
                allowed_models=registration.allowed_models,
                identity=grant.identity,
                egress="*",
                operations=["query", "tokens_count"],
                ttl_seconds=body.ttl_seconds,
            ),
            loop,
        ).result()
        commit_started = False
        try:
            registration.gateway_url = url
            registration.gateway_lease_id = issued.lease_id
            registration.token_ciphertext = encrypt_grader_token(
                issued.token, record_id=registration.id, kind="registration"
            )
            registration.expires_at = issued.expires_at
            commit_started = True
            session.commit()
        except Exception:
            should_revoke = not commit_started
            if commit_started:
                try:
                    session.rollback()
                    should_revoke = repository.get_registration_lease_id(registration_id) != issued.lease_id
                except Exception:
                    logger.exception("Failed to read remote grader lease after pickup commit failure")
            if should_revoke:
                try:
                    asyncio.run_coroutine_threadsafe(revoke_gateway_lease(url, api_key, issued.lease_id), loop).result()
                except Exception:
                    logger.exception("Failed to revoke remote grader lease after pickup failure")
            raise
        return RemoteGraderCredential(
            gateway_url=url, token=issued.token, lease_id=issued.lease_id, expires_at=issued.expires_at
        )


@router.post(
    "/registrations/{registration_id}/lease",
    response_model=RemoteGraderCredential,
    description="Return a Gateway lease for a registered model scope, minting only before the first-pickup deadline.",
)
async def pickup_remote_grader_lease(
    registration_id: UUID,
    body: RemoteGraderPickup,
    session: Session = Depends(get_session),
    service: str = Depends(_worker_service),
) -> RemoteGraderCredential:
    return await asyncio.to_thread(
        _pickup, session.get_bind(), registration_id, service, body, asyncio.get_running_loop()
    )


def _stop(
    bind: Engine | Connection,
    registration_id: UUID,
    service: str,
    body: RemoteGraderAttempt,
    loop: asyncio.AbstractEventLoop,
) -> RemoteGraderStopped:
    with Session(bind=bind) as session:
        repository = RemoteGraderRepository(session)
        registration = _registration(repository, registration_id, service, body)
        if registration.stopped_at is None:
            registration.stopped_at = datetime.now(UTC)
            registration.token_ciphertext = None
            session.commit()
            registration = _registration(repository, registration_id, service, body)
        if registration.revoked_at is not None:
            return RemoteGraderStopped(stopped=True)
        if registration.gateway_lease_id is not None:
            url = registration.gateway_url
            assert url is not None
            lease_id = registration.gateway_lease_id
            grant = repository.get_grant(registration.grant_id)
            assert grant is not None
            _, api_key = _root(repository, grant, loop)
            try:
                asyncio.run_coroutine_threadsafe(revoke_gateway_lease(url, api_key, lease_id), loop).result()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    raise
        registration.revoked_at = datetime.now(UTC)
        session.commit()
        return RemoteGraderStopped(stopped=True)


@router.post(
    "/registrations/{registration_id}/stop",
    response_model=RemoteGraderStopped,
    description="Close a remote grader registration by ID and revoke its Gateway lease.",
)
async def stop_remote_grader_lease(
    registration_id: UUID,
    body: RemoteGraderAttempt,
    session: Session = Depends(get_session),
    service: str = Depends(_worker_service),
) -> RemoteGraderStopped:
    return await asyncio.to_thread(
        _stop, session.get_bind(), registration_id, service, body, asyncio.get_running_loop()
    )
