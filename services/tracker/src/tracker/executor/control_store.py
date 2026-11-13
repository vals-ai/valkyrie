from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from hmac import compare_digest
from typing import Literal, TypeVar
from uuid import UUID

from fastapi import HTTPException, Request
from pydantic import BaseModel
from sqlmodel import Session

from executor_protocol import ExecutorDispatchStatus
from tracker.database.models import (
    Benchmark,
    ExecutorAdmission,
    ExecutorDispatch,
    Task,
)
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.control_protocol import MAINTENANCE_ADMISSION_PAUSED, PhaseRef
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository

ResponseT = TypeVar("ResponseT", bound=BaseModel)


def complete_response(session: Session, response: ResponseT) -> ResponseT:
    session.rollback()
    return response


def park_new_build_admission(admission: ExecutorAdmission) -> None:
    if admission.maintenance_target_sha is not None:
        raise HTTPException(
            status_code=423,
            detail={"code": MAINTENANCE_ADMISSION_PAUSED, "message": "New build admission is parked"},
        )


def dispatch_storage_time(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None)


@dataclass(frozen=True)
class Invocation:
    dispatch_id: UUID
    invocation_key: str


def invocation_digest(key: str) -> bytes:
    return sha256(key.encode()).digest()


def authenticated_invocation(request: Request) -> Invocation:
    dispatch_id = request.headers.get("X-Executor-Dispatch-ID")
    authorization = request.headers.get("Authorization")
    if dispatch_id is None or authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401)
    try:
        return Invocation(UUID(dispatch_id), authorization.removeprefix("Bearer "))
    except ValueError as error:
        raise HTTPException(status_code=401) from error


def require_dispatch_owner(
    session: Session,
    dispatch_id: UUID,
    invocation_key: str,
    *,
    lock: bool,
    require_live: bool,
    shared: bool = False,
) -> ExecutorDispatch:
    repository = ExecutorDispatchRepository(session)
    dispatch = repository.lock(dispatch_id, shared=shared) if lock else repository.get(dispatch_id)
    if (
        dispatch is None
        or dispatch.invocation_key_digest is None
        or not compare_digest(dispatch.invocation_key_digest, invocation_digest(invocation_key))
    ):
        raise HTTPException(status_code=403)
    if require_live and (
        dispatch.status != ExecutorDispatchStatus.RUNNING
        or (
            dispatch.ecs_task_arn is None
            and (
                dispatch.lease_expires_at is None
                or dispatch.lease_expires_at <= dispatch_storage_time(repository.clock_timestamp())
            )
        )
    ):
        raise ExecutionAuthorityRevoked("Dispatch authority expired")
    return dispatch


def require_task_phase_owner(
    dispatch_id: UUID,
    phase_ref: PhaseRef,
    *,
    scope: Literal["build", "evaluation"],
    task: Task,
) -> None:
    if (
        getattr(task, f"{scope}_phase_dispatch_id") != dispatch_id
        or getattr(task, f"{scope}_phase_token") != phase_ref.token
    ):
        raise ExecutionAuthorityRevoked("Task phase authority changed")


def require_publication_phase_owner(
    dispatch_id: UUID,
    phase_ref: PhaseRef,
    *,
    benchmark: Benchmark,
) -> None:
    if benchmark.publication_phase_dispatch_id != dispatch_id or benchmark.publication_phase_token != phase_ref.token:
        raise ExecutionAuthorityRevoked("Publication authority changed")
