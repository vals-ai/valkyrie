import os
from datetime import datetime, timedelta
from hmac import compare_digest

from botocore.exceptions import ClientError
from cryptography.exceptions import InvalidTag
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.exc import OperationalError
from sqlmodel import Session

from executor_protocol import (
    DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS,
    ExecutorDispatchStatus,
    executor_payload_benchmark_id,
)
from tracker.database.models import ExecutorDispatch, ExecutorDispatchPayload
from tracker.database.session import get_session
from tracker.domain import BenchmarkStatus, FailureCategory
from tracker.executor.control_protocol import (
    AckDispatchRequest,
    AckDispatchResponse,
    ClaimDispatchRequest,
    ClaimDispatchResponse,
    FailedFinishDispatchRequest,
    FinishDispatchRequest,
    FinishDispatchResponse,
    RenewDispatchRequest,
    RenewDispatchResponse,
)
from tracker.executor.control_store import (
    Invocation,
    complete_response,
    authenticated_invocation,
    dispatch_storage_time,
    invocation_digest,
    require_dispatch_owner,
)
from tracker.executor.dispatch_control import record_dispatch_failure
from tracker.executor.dispatch_payload import SealedPayload, open_payload
from tracker.executor.release_control import share_executor_admission
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository

router = APIRouter(prefix="/executor-control/v1")


def _now(session: Session) -> datetime:
    return ExecutorDispatchRepository(session).clock_timestamp()


def _payload(dispatch: ExecutorDispatch, payload: dict[str, object]) -> dict[str, object]:
    execution_context = payload.get("execution_context_json")
    task_ids = (execution_context if execution_context is not None else payload).get("verified_task_ids")
    if executor_payload_benchmark_id(payload) != str(dispatch.benchmark_id) or set(task_ids or []) != set(
        dispatch.assigned_task_ids or []
    ):
        raise HTTPException(status_code=409, detail="Dispatch payload does not match assignment")
    payload.update(
        executor_dispatch_id=str(dispatch.id),
        executor_release_id=dispatch.executor_release_id,
        executor_artifact_uri=dispatch.executor_artifact_uri,
        executor_artifact_digest=dispatch.executor_artifact_digest,
        executor_protocol_version=dispatch.executor_protocol_version,
    )
    return payload


def _eligible_claim(
    session: Session, request: ClaimDispatchRequest, digest: bytes, must_still_replay: bool
) -> ClaimDispatchResponse | tuple[ExecutorDispatch, bool, datetime, datetime, ExecutorDispatchPayload]:
    admission = share_executor_admission(session)
    repository = ExecutorDispatchRepository(session)
    dispatch_identity = repository.benchmark_id(request.dispatch_id)
    if dispatch_identity is None:
        return complete_response(session, ClaimDispatchResponse(claimed=False, payload=None))
    benchmark = BenchmarkRepository(session).lock_by_id(dispatch_identity)
    dispatch = repository.lock(request.dispatch_id)
    assert dispatch is not None
    now = _now(session)
    storage_now = dispatch_storage_time(now)
    replay = dispatch.status == ExecutorDispatchStatus.RUNNING and dispatch.invocation_key_digest is not None
    if must_still_replay and not replay:
        return complete_response(session, ClaimDispatchResponse(claimed=False, payload=None))
    if replay:
        if not compare_digest(dispatch.invocation_key_digest, digest) or (
            dispatch.ecs_task_arn is None
            and (dispatch.lease_expires_at is None or dispatch.lease_expires_at <= storage_now)
        ):
            return complete_response(session, ClaimDispatchResponse(claimed=False, payload=None))
    else:
        if (
            admission.maintenance_target_sha is not None
            or dispatch.status != ExecutorDispatchStatus.QUEUED
            or benchmark.status != BenchmarkStatus.IN_PROGRESS
            or benchmark.current_execution_release_id != dispatch.executor_release_id
            or dispatch.claim_deadline_at is None
            or dispatch.claim_deadline_at <= storage_now
        ):
            return complete_response(session, ClaimDispatchResponse(claimed=False, payload=None))
    if not replay and os.environ["EXECUTOR_LAUNCHER"] == "ecs" and dispatch.ecs_task_arn is None:
        raise HTTPException(status_code=503, detail="Executor ECS task identity is not yet recorded")

    sealed_row = repository.payload(dispatch.id)
    if sealed_row is None:
        raise HTTPException(status_code=409, detail="Claim payload already acknowledged")
    return dispatch, replay, now, storage_now, sealed_row


@router.post(
    "/claim_dispatch",
    response_model=ClaimDispatchResponse,
    description="Claim a queued executor dispatch or replay a living claim by its ID.",
)
def claim_dispatch(
    request: ClaimDispatchRequest,
    http_request: Request,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> ClaimDispatchResponse:
    bootstrap = http_request.headers.get("X-Executor-Bootstrap")
    if bootstrap is None or not compare_digest(bootstrap, os.environ["EXECUTOR_BOOTSTRAP_KEY"]):
        raise HTTPException(status_code=401)
    if request.dispatch_id != invocation.dispatch_id:
        raise HTTPException(status_code=403)
    digest = invocation_digest(invocation.invocation_key)

    snapshot = _eligible_claim(session, request, digest, must_still_replay=False)
    if isinstance(snapshot, ClaimDispatchResponse):
        return snapshot
    _, initial_replay, _, _, sealed_row = snapshot
    sealed = SealedPayload(sealed_row.ciphertext, sealed_row.encrypted_data_key, sealed_row.nonce)
    session.rollback()
    try:
        opened = open_payload(request.dispatch_id, sealed)
    except (ClientError, InvalidTag) as error:
        if initial_replay:
            raise
        error_type = error.response["Error"]["Code"] if isinstance(error, ClientError) else type(error).__name__
        decrypt_failed = True
    else:
        decrypt_failed = False

    revalidated = _eligible_claim(session, request, digest, must_still_replay=initial_replay)
    if isinstance(revalidated, ClaimDispatchResponse):
        return revalidated
    dispatch, replay, now, storage_now, _ = revalidated
    if decrypt_failed:
        if replay:
            raise HTTPException(status_code=409, detail="Executor dispatch payload decryption failed")
        record_dispatch_failure(
            session,
            benchmark_id=dispatch.benchmark_id,
            dispatch_id=request.dispatch_id,
            task_ids=dispatch.assigned_task_ids or [],
            error_message=f"Executor payload decrypt failed: {error_type}",
            producer="executor_dispatch",
            operation="claim_dispatch",
            error_type=error_type,
            category=FailureCategory.INFRASTRUCTURE,
            cause_code="PAYLOAD_DECRYPT_FAILED",
            failure_reason="PAYLOAD_DECRYPT_FAILED",
            dispatch_status=ExecutorDispatchStatus.QUEUED,
        )
        session.commit()
        raise HTTPException(status_code=409, detail="Executor dispatch payload decryption failed")

    payload = _payload(dispatch, opened)
    if replay:
        return complete_response(session, ClaimDispatchResponse(claimed=True, payload=payload))
    dispatch.status = ExecutorDispatchStatus.RUNNING
    dispatch.started_at = storage_now
    if dispatch.ecs_task_arn is None:
        dispatch.lease_expires_at = storage_now + timedelta(seconds=DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS)
    dispatch.invocation_key_digest = digest
    dispatch.claim_receipt_at = now
    ExecutorDispatchRepository(session).save(dispatch)
    session.commit()
    return complete_response(session, ClaimDispatchResponse(claimed=True, payload=payload))


@router.post(
    "/ack_dispatch",
    response_model=AckDispatchResponse,
    description="Acknowledge a claimed dispatch and delete its sealed payload.",
)
def ack_dispatch(
    request: AckDispatchRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> AckDispatchResponse:
    if request.ref.dispatch_id != invocation.dispatch_id:
        raise HTTPException(status_code=403)
    dispatch = require_dispatch_owner(
        session, invocation.dispatch_id, invocation.invocation_key, lock=True, require_live=True
    )
    if dispatch.benchmark_id != request.ref.benchmark_id:
        raise HTTPException(status_code=403)
    ExecutorDispatchRepository(session).delete_payload(dispatch.id)
    session.commit()
    return complete_response(session, AckDispatchResponse(acknowledged=True))


@router.post(
    "/renew_dispatch",
    response_model=RenewDispatchResponse,
    description="Renew a living local dispatch lease and return its current status.",
)
def renew_dispatch(
    request: RenewDispatchRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> RenewDispatchResponse:
    if request.ref.dispatch_id != invocation.dispatch_id:
        raise HTTPException(status_code=403)
    share_executor_admission(session)
    dispatch = require_dispatch_owner(
        session, invocation.dispatch_id, invocation.invocation_key, lock=False, require_live=False
    )
    if dispatch.benchmark_id != request.ref.benchmark_id:
        raise HTTPException(status_code=403)
    dispatch = ExecutorDispatchRepository(session).lock_skip_locked(dispatch.id)
    renewed = False
    if dispatch is not None:
        storage_now = dispatch_storage_time(_now(session))
        renewed = (
            dispatch.status == ExecutorDispatchStatus.RUNNING
            and dispatch.ecs_task_arn is None
            and dispatch.lease_expires_at is not None
            and dispatch.lease_expires_at > storage_now
        )
        if renewed:
            dispatch.lease_expires_at = max(
                dispatch.lease_expires_at,
                storage_now + timedelta(seconds=DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS),
            )
            ExecutorDispatchRepository(session).save(dispatch)
    session.commit()
    session.expire_all()
    try:
        if session.bind.dialect.name == "postgresql":
            ExecutorDispatchRepository(session).set_short_lock_timeout()
        share_executor_admission(session)
        live = ExecutorDispatchRepository(session).get_current(request.ref.dispatch_id)
        benchmark_status = BenchmarkRepository(session).status_by_id(request.ref.benchmark_id)
        return complete_response(
            session,
            RenewDispatchResponse(
                renewed=renewed,
                live=live is not None
                and live.status == ExecutorDispatchStatus.RUNNING
                and (
                    live.ecs_task_arn is not None
                    or (
                        live.lease_expires_at is not None
                        and live.lease_expires_at > dispatch_storage_time(_now(session))
                    )
                ),
                stopped=benchmark_status == BenchmarkStatus.STOPPED,
            ),
        )
    except OperationalError:
        session.rollback()
        return complete_response(session, RenewDispatchResponse(renewed=renewed, live=None, stopped=None))


@router.post(
    "/finish_dispatch",
    response_model=FinishDispatchResponse,
    description="Finish an owned dispatch when its run and migration state allow it.",
)
def finish_dispatch(
    request: FinishDispatchRequest | FailedFinishDispatchRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> FinishDispatchResponse:
    if request.ref.dispatch_id != invocation.dispatch_id:
        raise HTTPException(status_code=403)
    share_executor_admission(session)
    benchmark = BenchmarkRepository(session).get_by_id(request.ref.benchmark_id, for_update=True)
    if benchmark is None:
        return complete_response(session, FinishDispatchResponse(finished=False))
    dispatch = require_dispatch_owner(
        session, invocation.dispatch_id, invocation.invocation_key, lock=True, require_live=False
    )
    if dispatch.benchmark_id != benchmark.id:
        raise HTTPException(status_code=403)
    if dispatch.status == ExecutorDispatchStatus.FINISHED or (
        dispatch.status == ExecutorDispatchStatus.FAILED and dispatch.failure_reason == "RUN_ERROR"
    ):
        return complete_response(session, FinishDispatchResponse(finished=True))
    if dispatch.status != ExecutorDispatchStatus.RUNNING or (
        dispatch.ecs_task_arn is None
        and (dispatch.lease_expires_at is None or dispatch.lease_expires_at <= dispatch_storage_time(_now(session)))
    ):
        return complete_response(session, FinishDispatchResponse(finished=False))
    if isinstance(request, FailedFinishDispatchRequest):
        finished = record_dispatch_failure(
            session,
            benchmark_id=benchmark.id,
            dispatch_id=dispatch.id,
            task_ids=dispatch.assigned_task_ids or [],
            error_message=request.error_message,
            producer="executor_dispatch",
            operation="finish_dispatch",
            error_type=request.error_type,
            category=request.category,
            failure_reason="EXECUTOR_FAILED",
        )
        session.commit()
        return complete_response(session, FinishDispatchResponse(finished=finished))
    now = _now(session)
    dispatch.status = ExecutorDispatchStatus.FINISHED
    dispatch.finished_at = dispatch_storage_time(now)
    ExecutorDispatchRepository(session).save(dispatch)
    session.flush()
    if not ExecutorDispatchRepository(session).has_active(benchmark.id):
        if benchmark.status == BenchmarkStatus.IN_PROGRESS:
            benchmark.status = BenchmarkStatus.ERROR
            benchmark.finished_at = now
            benchmark.error_message = "Executor exited without finalizing benchmark"
            BenchmarkRepository(session).save(benchmark)
        elif benchmark.status == BenchmarkStatus.STOPPING:
            benchmark.status = BenchmarkStatus.STOPPED
            benchmark.finished_at = now
            BenchmarkRepository(session).save(benchmark)
    session.commit()
    return complete_response(session, FinishDispatchResponse(finished=True))
