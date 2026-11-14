from datetime import UTC, datetime, timedelta
from uuid import uuid4

from benchmark_service import SandboxReservationDemand
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlmodel import Session
from executor_protocol import ExecutorDispatchStatus
from tracker.config import SANDBOX_BUILD_RATE_PER_MINUTE
from tracker.database.models import Benchmark, Task
from tracker.database.session import get_session
from tracker.domain import BenchmarkStatus, TaskStatus
from tracker.executor.control_protocol import AttemptRef, DispatchRef, PhaseRef
from tracker.executor.control_queue import (
    QueueAttemptRequest,
    QueueBuildRequest,
    QueueBuildResponse,
    QueueClaimResponse,
    QueueEligibilityResponse,
    QueueOrphan,
    QueueOrphanResultRequest,
    QueueOrphansResponse,
    QueuePrepareRequest,
    QueueReadinessRequest,
    QueueReadinessResponse,
    QueueRequest,
    QueueRoundRequest,
    QueueRoundResponse,
    QueueStateResponse,
    QueueStatesRequest,
    QueueStatesResponse,
    QueueTimeResponse,
    is_reserved_queue_pool_id,
    queue_pool_lock_id,
    queued_sandbox_name,
)
from tracker.executor.control_store import (
    authenticated_invocation,
    complete_response,
    park_new_build_admission,
    require_dispatch_owner,
    require_task_phase_owner,
)
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.release_control import share_executor_admission
from tracker.repositories.advisory_lock import QueuePoolAdvisoryLock
from tracker.remote_grader import attempt_id_for
from tracker.repositories.remote_grader import RemoteGraderRepository
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.repositories.sandbox_build_reservation import SandboxBuildReservationRepository
from tracker.repositories.scheduler import SchedulerRepository

router = APIRouter(prefix="/executor-control/v1")
_RATE_WINDOW = timedelta(minutes=1)
_RETENTION = 2 * _RATE_WINDOW


def _utc_naive(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo is not None else value


def _owner(session: Session, request: Request, ref, pool_id: str | None = None, *, lock: bool = True):
    invocation = authenticated_invocation(request)
    if invocation.dispatch_id != ref.dispatch_id:
        raise HTTPException(status_code=403)
    admission = share_executor_admission(session)
    if pool_id is None:
        found = BenchmarkRepository(session).get_by_id(ref.benchmark_id, for_update=lock)
        if found is None:
            raise HTTPException(status_code=403)
        benchmark = found
    else:
        benchmarks = BenchmarkRepository(session).lock_pool(pool_id)
        benchmark = next(row for row in benchmarks if row.id == ref.benchmark_id)
    dispatch = require_dispatch_owner(
        session, invocation.dispatch_id, invocation.invocation_key, lock=lock, require_live=True
    )
    if dispatch.benchmark_id != benchmark.id:
        raise HTTPException(status_code=403)
    return benchmark, dispatch, admission


def _cleanup_owner(session: Session, request: Request, ref: DispatchRef) -> Benchmark:
    invocation = authenticated_invocation(request)
    if invocation.dispatch_id != ref.dispatch_id:
        raise HTTPException(status_code=403)
    share_executor_admission(session)
    benchmark = BenchmarkRepository(session).lock_by_id(ref.benchmark_id)
    dispatch = require_dispatch_owner(
        session, invocation.dispatch_id, invocation.invocation_key, lock=True, require_live=False
    )
    if dispatch.benchmark_id != benchmark.id:
        raise HTTPException(status_code=403)
    return benchmark


def _attempt(session: Session, attempt: AttemptRef, benchmark: Benchmark, *, lock: bool = True) -> Task | None:
    repository = BenchmarkRepository(session)
    task = (
        repository.lock_task_by_row_id(attempt.task_row_id) if lock else repository.task_by_row_id(attempt.task_row_id)
    )
    if task.benchmark != benchmark.id or task.org_id != attempt.org_id or task.task_id != attempt.task_id:
        raise ValueError("Queue attempt identity differs")
    return task if task.started_at == attempt.started_at else None


def _pool(request: QueueRequest, benchmark: Benchmark) -> None:
    if benchmark.arguments.queue_pool_id != request.pool_id:
        raise ValueError("Queue pool differs from persisted execution")


def _budget(session: Session, request: QueueRequest) -> int | None:
    if not SANDBOX_BUILD_RATE_PER_MINUTE:
        return None
    repository = SchedulerRepository(session)
    return SANDBOX_BUILD_RATE_PER_MINUTE - repository.admissions_since(
        request.pool_id, repository.database_now() - _RATE_WINDOW
    )


def _waiting(session: Session, task: Task, benchmark: Benchmark) -> bool:
    return task.status == TaskStatus.PENDING and benchmark.status == BenchmarkStatus.IN_PROGRESS


def _claim_lock(session: Session, pool_id: str) -> None:
    QueuePoolAdvisoryLock(session).lock(pool_id)


@router.post(
    "/queue-clock", response_model=QueueTimeResponse, description="Return the database clock for an owned queue pool."
)
def clock(body: QueueRequest, request: Request, session: Session = Depends(get_session)) -> QueueTimeResponse:
    benchmark, _, _ = _owner(session, request, body.ref, lock=False)
    _pool(body, benchmark)
    return complete_response(session, QueueTimeResponse(read_at=SchedulerRepository(session).database_now()))


@router.post(
    "/queue-eligibility",
    response_model=QueueEligibilityResponse,
    description="Return eligibility for an owned queued task attempt.",
)
def eligibility(
    body: QueueAttemptRequest, request: Request, session: Session = Depends(get_session)
) -> QueueEligibilityResponse:
    benchmark, _, _ = _owner(session, request, body.ref, lock=False)
    _pool(body, benchmark)
    task = _attempt(session, body.attempt, benchmark, lock=False)
    if task is None:
        return complete_response(session, QueueEligibilityResponse(eligible=None))
    eligible = SchedulerRepository(session).eligible_task_is(body.pool_id, task.id, body.attempt.started_at, limit=1)
    return complete_response(
        session,
        QueueEligibilityResponse(eligible=True if eligible else False if _waiting(session, task, benchmark) else None),
    )


@router.post(
    "/queue-round",
    response_model=QueueRoundResponse,
    description="Return the eligible waiters and admission budget for a queue pool.",
)
def round_waiters(
    body: QueueRoundRequest, request: Request, session: Session = Depends(get_session)
) -> QueueRoundResponse:
    benchmark, _, _ = _owner(session, request, body.ref, lock=False)
    _pool(body, benchmark)
    waiting = []
    for waiter in body.waiters:
        task = _attempt(session, waiter.attempt, benchmark, lock=False)
        if task is not None and _waiting(session, task, benchmark):
            waiting.append(task.id)
    eligible = set(SchedulerRepository(session).eligible_task_ids(body.pool_id, waiting)) if waiting else set()
    retries = (
        [
            waiter.attempt.task_row_id
            for waiter in body.waiters
            if not waiter.ready and waiter.attempt.task_row_id in eligible
        ]
        if body.retry_rejected
        else []
    )
    return complete_response(
        session,
        QueueRoundResponse(waiting=waiting, selected=list(eligible), retries=retries, budget=_budget(session, body)),
    )


@router.post(
    "/queue-prepare",
    response_model=QueueBuildResponse,
    description="Prepare a queued task attempt for sandbox admission.",
)
def prepare(body: QueuePrepareRequest, request: Request, session: Session = Depends(get_session)) -> QueueBuildResponse:
    benchmark, _, admission = _owner(session, request, body.ref)
    _pool(body, benchmark)
    park_new_build_admission(admission)
    task = _attempt(session, body.attempt, benchmark)
    if task is None:
        return complete_response(session, QueueBuildResponse(started=False))
    if not _waiting(session, task, benchmark):
        return complete_response(session, QueueBuildResponse(started=False))
    demand = SandboxReservationDemand(**body.demand.model_dump())
    SchedulerRepository(session).prepare_admission_candidate(
        body.pool_id,
        task.id,
        task.started_at,
        body.ref.dispatch_id,
        demand,
        admission_ready=body.admission_ready,
        grading_demand=None
        if body.grading_demand is None
        else SandboxReservationDemand(**body.grading_demand.model_dump()),
    )
    session.commit()
    return complete_response(session, QueueBuildResponse(started=True))


@router.post(
    "/queue-state",
    response_model=QueueStateResponse,
    description="Return an owned queued attempt's task status and scheduler readiness request.",
)
def queue_state(
    body: QueueAttemptRequest, request: Request, session: Session = Depends(get_session)
) -> QueueStateResponse:
    benchmark, _, _ = _owner(session, request, body.ref, lock=False)
    _pool(body, benchmark)
    task = _attempt(session, body.attempt, benchmark, lock=False)
    if task is None:
        return complete_response(session, QueueStateResponse(status=None, readiness_requested_at=None))
    status, readiness_requested_at = SchedulerRepository(session).candidate_state(
        task.id, body.attempt.started_at, body.ref.dispatch_id
    )
    return complete_response(
        session,
        QueueStateResponse(
            status=status.value if status is not None else None, readiness_requested_at=readiness_requested_at
        ),
    )


@router.post(
    "/queue-states",
    response_model=QueueStatesResponse,
    description="Return the task status and scheduler readiness request for each owned queued attempt in request order.",
)
def queue_states(
    body: QueueStatesRequest, request: Request, session: Session = Depends(get_session)
) -> QueueStatesResponse:
    benchmark, _, _ = _owner(session, request, body.ref, lock=False)
    _pool(body, benchmark)
    tasks = {
        task.id: task
        for task in BenchmarkRepository(session).tasks_by_row_ids({attempt.task_row_id for attempt in body.attempts})
    }
    current: list[tuple[AttemptRef, Task | None]] = []
    for attempt in body.attempts:
        task = tasks[attempt.task_row_id]
        if task.benchmark != benchmark.id or task.org_id != attempt.org_id or task.task_id != attempt.task_id:
            raise ValueError("Queue attempt identity differs")
        current.append((attempt, task if task.started_at == attempt.started_at else None))
    readiness = SchedulerRepository(session).readiness_requests(
        {(task.id, task.started_at) for _, task in current if task is not None}, body.ref.dispatch_id
    )
    return complete_response(
        session,
        QueueStatesResponse(
            states=[
                QueueStateResponse(status=None, readiness_requested_at=None)
                if task is None
                else QueueStateResponse(
                    status=task.status.value, readiness_requested_at=readiness.get((task.id, task.started_at))
                )
                for _, task in current
            ]
        ),
    )


@router.post(
    "/queue-begin-readiness",
    response_model=QueueReadinessResponse,
    description="Acknowledge a scheduler readiness request before rechecking provider admission.",
)
def queue_begin_readiness(
    body: QueueReadinessRequest, request: Request, session: Session = Depends(get_session)
) -> QueueReadinessResponse:
    benchmark, _, admission = _owner(session, request, body.ref)
    _pool(body, benchmark)
    park_new_build_admission(admission)
    task = _attempt(session, body.attempt, benchmark)
    started = task is not None and SchedulerRepository(session).begin_readiness_check(
        task.id, task.started_at, body.ref.dispatch_id, body.requested_at
    )
    session.commit()
    return complete_response(session, QueueReadinessResponse(started=started))


@router.post(
    "/queue-accept",
    response_model=QueueClaimResponse,
    description="Accept a scheduler reservation within its thirty-second grant window.",
)
def queue_accept(
    body: QueueAttemptRequest, request: Request, session: Session = Depends(get_session)
) -> QueueClaimResponse:
    benchmark, _, admission = _owner(session, request, body.ref)
    _pool(body, benchmark)
    if not is_reserved_queue_pool_id(body.pool_id):
        raise HTTPException(status_code=409, detail="Only reserved pools accept scheduler grants")
    task = _attempt(session, body.attempt, benchmark)
    if task is None:
        return complete_response(session, QueueClaimResponse(phase=None, waiting=False))
    reservations = SandboxBuildReservationRepository(session)
    reservation = reservations.lock_active(task.id, body.attempt.started_at)
    if (
        reservation is not None
        and reservation.phase_dispatch_id == body.ref.dispatch_id
        and reservation.phase_token is not None
    ):
        return complete_response(
            session, QueueClaimResponse(phase=PhaseRef(token=reservation.phase_token), waiting=False)
        )
    park_new_build_admission(admission)
    if task.status != TaskStatus.BUILDING or not SchedulerRepository(session).accept_grant(
        task.id, task.started_at, body.ref.dispatch_id
    ):
        return complete_response(session, QueueClaimResponse(phase=None, waiting=task.status == TaskStatus.PENDING))
    assert reservation is not None
    token = uuid4()
    reservation.phase_dispatch_id = body.ref.dispatch_id
    reservation.phase_token = token
    reservations.save(reservation)
    session.commit()
    return complete_response(session, QueueClaimResponse(phase=PhaseRef(token=token), waiting=False))


@router.post(
    "/queue-claim",
    response_model=QueueClaimResponse,
    description="Claim a legacy queue build phase for an eligible task attempt.",
)
def claim(body: QueueAttemptRequest, request: Request, session: Session = Depends(get_session)) -> QueueClaimResponse:
    benchmark, _, admission = _owner(session, request, body.ref)
    _pool(body, benchmark)
    if is_reserved_queue_pool_id(body.pool_id):
        raise HTTPException(status_code=409, detail="Reserved queues accept API scheduler grants")
    _claim_lock(session, body.pool_id)
    task = _attempt(session, body.attempt, benchmark)
    if task is None:
        return complete_response(session, QueueClaimResponse(phase=None, waiting=False))
    if task.status == TaskStatus.BUILDING and task.build_phase_dispatch_id == body.ref.dispatch_id:
        return complete_response(
            session, QueueClaimResponse(phase=PhaseRef(token=task.build_phase_token), waiting=False)
        )
    park_new_build_admission(admission)
    if not _waiting(session, task, benchmark):
        return complete_response(session, QueueClaimResponse(phase=None, waiting=False))
    active_build = BenchmarkRepository(session).has_build_in_pool(body.pool_id)
    admitted = (
        not active_build
        and not SchedulerRepository(session).reserved_builds(body.pool_id)
        and SchedulerRepository(session).claim_eligible_task(body.pool_id, task.id, task.started_at)
    )
    if not admitted:
        return complete_response(session, QueueClaimResponse(phase=None, waiting=True))
    token = uuid4()
    task.build_phase_dispatch_id = body.ref.dispatch_id
    task.build_phase_token = token
    BenchmarkRepository(session).save(task)
    session.commit()
    return complete_response(session, QueueClaimResponse(phase=PhaseRef(token=token), waiting=False))


def _build(body: QueueBuildRequest, request: Request, session: Session, *, finish: bool) -> QueueBuildResponse:
    benchmark = (
        _owner(session, request, body.ref)[0]
        if finish or not is_reserved_queue_pool_id(body.pool_id)
        else _cleanup_owner(session, request, body.ref)
    )
    _pool(body, benchmark)
    task = BenchmarkRepository(session).lock_task_by_row_id(body.attempt.task_row_id)
    if task.benchmark != benchmark.id or task.org_id != body.attempt.org_id or task.task_id != body.attempt.task_id:
        raise ValueError("Queue attempt identity differs")
    stale_attempt = task.started_at != body.attempt.started_at
    if is_reserved_queue_pool_id(body.pool_id):
        reservation = SandboxBuildReservationRepository(session).lock_active(task.id, body.attempt.started_at)
        if reservation is None:
            raise ValueError("Queue attempt identity differs")
        if reservation.phase_dispatch_id != body.ref.dispatch_id or reservation.phase_token != body.phase.token:
            raise ExecutionAuthorityRevoked("Reservation phase authority changed")
    else:
        if stale_attempt:
            return QueueBuildResponse(started=False)
        require_task_phase_owner(body.ref.dispatch_id, body.phase, scope="build", task=task)
    if finish and (
        stale_attempt or task.status != TaskStatus.BUILDING or benchmark.status != BenchmarkStatus.IN_PROGRESS
    ):
        return QueueBuildResponse(started=False)
    reserved = is_reserved_queue_pool_id(body.pool_id)
    if reserved:
        if finish:
            started = SchedulerRepository(session).start_building_task(task.id, body.attempt.started_at)
        else:
            started = False
        reservation.phase_dispatch_id = None
        reservation.phase_token = None
        SandboxBuildReservationRepository(session).save(reservation)
        SchedulerRepository(session).release_reservation(task.id, body.attempt.started_at)
        if started:
            task.build_phase_dispatch_id = body.ref.dispatch_id
            BenchmarkRepository(session).save(task)
    else:
        task.build_phase_dispatch_id = body.ref.dispatch_id if finish else None
        task.build_phase_token = None
        if finish:
            task.status = TaskStatus.IN_PROGRESS
        if not stale_attempt:
            BenchmarkRepository(session).save(task)
        started = finish
    session.commit()
    return QueueBuildResponse(started=started)


@router.post(
    "/queue-finish",
    response_model=QueueBuildResponse,
    description="Finish the build phase for an owned queued task attempt.",
)
def finish(body: QueueBuildRequest, request: Request, session: Session = Depends(get_session)) -> QueueBuildResponse:
    return complete_response(session, _build(body, request, session, finish=True))


@router.post(
    "/queue-abort",
    response_model=QueueBuildResponse,
    description="Abort the build phase for an owned queued task attempt.",
)
def abort(body: QueueBuildRequest, request: Request, session: Session = Depends(get_session)) -> QueueBuildResponse:
    return complete_response(session, _build(body, request, session, finish=False))


@router.post(
    "/queue-abandon",
    response_model=QueueBuildResponse,
    description="Hand off a settled reserved build for scheduler cleanup or confirm its original phase is no longer owned.",
)
def abandon(body: QueueBuildRequest, request: Request, session: Session = Depends(get_session)) -> QueueBuildResponse:
    benchmark = _cleanup_owner(session, request, body.ref)
    _pool(body, benchmark)
    if not is_reserved_queue_pool_id(body.pool_id):
        raise HTTPException(status_code=409, detail="Only reserved builds can be handed off to the scheduler")
    task = BenchmarkRepository(session).lock_task_by_row_id(body.attempt.task_row_id)
    if task.benchmark != benchmark.id or task.org_id != body.attempt.org_id or task.task_id != body.attempt.task_id:
        raise ValueError("Queue attempt identity differs")
    reservations = SandboxBuildReservationRepository(session)
    reservation = reservations.lock_attempt(task.id, body.attempt.started_at)
    if (
        reservation is None
        or reservation.accepted_at is None
        or reservation.pool_id != queue_pool_lock_id(body.pool_id)
    ):
        raise ValueError("Accepted reservation does not exist")
    if reservation.phase_dispatch_id == body.ref.dispatch_id and reservation.phase_token == body.phase.token:
        if reservation.released_at is not None or task.started_at != body.attempt.started_at:
            raise ExecutionAuthorityRevoked("Reservation phase authority changed")
        reservation.phase_dispatch_id = None
        reservation.phase_token = None
        reservations.save(reservation)
        session.commit()
    elif reservation.executor_dispatch_id != body.ref.dispatch_id:
        raise ExecutionAuthorityRevoked("Reservation phase authority changed")
    return complete_response(session, QueueBuildResponse(started=False))


@router.post(
    "/queue-clear",
    response_model=QueueBuildResponse,
    description="Clear an admission candidate for an owned queued task attempt.",
)
def clear(body: QueueAttemptRequest, request: Request, session: Session = Depends(get_session)) -> QueueBuildResponse:
    benchmark = _cleanup_owner(session, request, body.ref)
    _pool(body, benchmark)
    _attempt(session, body.attempt, benchmark)
    SchedulerRepository(session).clear_admission_candidate(
        body.attempt.task_row_id, body.attempt.started_at, body.ref.dispatch_id
    )
    session.commit()
    return complete_response(session, QueueBuildResponse(started=False))


@router.post(
    "/queue-release-grading",
    response_model=QueueBuildResponse,
    description="Release the capacity held for an owned task attempt's grading sandbox once that sandbox exists.",
)
def release_grading(
    body: QueueAttemptRequest, request: Request, session: Session = Depends(get_session)
) -> QueueBuildResponse:
    benchmark, _, _ = _owner(session, request, body.ref)
    _pool(body, benchmark)
    if _attempt(session, body.attempt, benchmark) is not None:
        SchedulerRepository(session).release_grading_reservation(body.attempt.task_row_id, body.attempt.started_at)
    session.commit()
    return complete_response(session, QueueBuildResponse(started=False))


@router.post(
    "/queue-exit",
    response_model=QueueBuildResponse,
    description="Hand off accepted build phases after a local executor exits.",
)
def queue_exit(body: DispatchRef, request: Request, session: Session = Depends(get_session)) -> QueueBuildResponse:
    invocation = authenticated_invocation(request)
    if invocation.dispatch_id != body.dispatch_id:
        raise HTTPException(status_code=403)
    share_executor_admission(session)
    benchmark = BenchmarkRepository(session).lock_by_id(body.benchmark_id)
    dispatch = ExecutorDispatchRepository(session).lock(body.dispatch_id)
    if dispatch is None or dispatch.benchmark_id != benchmark.id:
        raise HTTPException(status_code=403)
    require_dispatch_owner(session, dispatch.id, invocation.invocation_key, lock=True, require_live=False)
    SchedulerRepository(session).handoff_dispatch_phases(body.dispatch_id)
    session.commit()
    return complete_response(session, QueueBuildResponse(started=False))


@router.post(
    "/queue-orphans",
    response_model=QueueOrphansResponse,
    description="Claim orphaned build phases in an owned queue pool.",
)
def orphans(body: QueueRequest, request: Request, session: Session = Depends(get_session)) -> QueueOrphansResponse:
    benchmark, _, admission = _owner(session, request, body.ref, body.pool_id)
    _pool(body, benchmark)
    _claim_lock(session, body.pool_id)
    park_new_build_admission(admission)
    repository = SchedulerRepository(session)
    now = repository.database_now()
    repository.purge_stale_admission_candidates(body.pool_id)
    repository.purge_released_reservations(body.pool_id, now - _RETENTION)
    rows = SandboxBuildReservationRepository(session).lock_pool_orphans(body.pool_id)
    result = []
    for task, owner_benchmark, reservation in rows:
        if reservation is not None and reservation.accepted_at is None:
            continue
        if reservation is not None:
            reservation = SandboxBuildReservationRepository(session).lock_active(
                task.id, reservation.attempt_started_at
            )
            assert reservation is not None
            if reservation.phase_dispatch_id is not None:
                continue
        owner_id = reservation.phase_dispatch_id if reservation is not None else task.build_phase_dispatch_id
        owner = ExecutorDispatchRepository(session).get(owner_id) if owner_id is not None else None
        if (
            owner is not None
            and owner.status == ExecutorDispatchStatus.RUNNING
            and (
                owner.ecs_task_arn is not None
                or (owner.lease_expires_at is not None and owner.lease_expires_at > _utc_naive(now))
            )
        ):
            continue
        attempt_started_at = reservation.attempt_started_at if reservation is not None else task.started_at
        token = uuid4()
        if reservation is not None:
            reservation.phase_dispatch_id = body.ref.dispatch_id
            reservation.phase_token = token
            SandboxBuildReservationRepository(session).save(reservation)
        else:
            task.build_phase_dispatch_id = body.ref.dispatch_id
            task.build_phase_token = token
            BenchmarkRepository(session).save(task)
        result.append(
            QueueOrphan(
                attempt=AttemptRef(
                    org_id=task.org_id, task_row_id=task.id, task_id=task.task_id, started_at=attempt_started_at
                ),
                phase=PhaseRef(token=token),
                status=task.status.value,
                sandbox_name=queued_sandbox_name(task.id, attempt_started_at),
                reserved_at=reservation.reserved_at if reservation is not None else task.started_at,
            )
        )
    session.commit()
    return complete_response(session, QueueOrphansResponse(orphans=result, server_now=now))


@router.post(
    "/queue-reconcile",
    response_model=QueueBuildResponse,
    description="Reconcile an orphaned queued build after cleanup.",
)
def reconcile(
    body: QueueOrphanResultRequest, request: Request, session: Session = Depends(get_session)
) -> QueueBuildResponse:
    benchmark, _, admission = _owner(session, request, body.ref, body.pool_id)
    _pool(body, benchmark)
    task = BenchmarkRepository(session).lock_task_by_row_id(body.attempt.task_row_id)
    owner_benchmark = BenchmarkRepository(session).get_by_id(task.benchmark)
    _pool(body, owner_benchmark)
    if (
        task.benchmark != owner_benchmark.id
        or task.org_id != body.attempt.org_id
        or task.task_id != body.attempt.task_id
    ):
        raise ValueError("Queue attempt identity differs")
    reservation = SandboxBuildReservationRepository(session).lock_active(task.id, body.attempt.started_at)
    stale_attempt = task.started_at != body.attempt.started_at
    if reservation is not None:
        if reservation.phase_dispatch_id != body.ref.dispatch_id or reservation.phase_token != body.phase.token:
            raise ExecutionAuthorityRevoked("Reservation phase authority changed")
    else:
        if stale_attempt:
            raise ValueError("Queue attempt identity differs")
        require_task_phase_owner(body.ref.dispatch_id, body.phase, scope="build", task=task)
    now = _utc_naive(SchedulerRepository(session).database_now())
    released = body.cleaned or reservation is None or reservation.reserved_at < now - timedelta(minutes=5)
    if reservation is not None:
        if released:
            reservation.released_at = now
        reservation.phase_dispatch_id = None
        reservation.phase_token = None
        SandboxBuildReservationRepository(session).save(reservation)
    else:
        task.build_phase_dispatch_id = None
        task.build_phase_token = None
    if (
        released
        and not stale_attempt
        and task.status == TaskStatus.BUILDING
        and owner_benchmark.status == BenchmarkStatus.IN_PROGRESS
    ):
        RemoteGraderRepository(session).close_attempt(task.id, attempt_id_for(task))
        task.status = TaskStatus.PENDING
        task.started_at = max(task.started_at + timedelta(microseconds=1), now)
    if not stale_attempt:
        BenchmarkRepository(session).save(task)
    session.commit()
    return complete_response(session, QueueBuildResponse(started=False))
