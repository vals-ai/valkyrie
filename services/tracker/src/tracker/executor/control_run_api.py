import asyncio
import json
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlmodel import Session

from tracker.database.models import Benchmark, ErrorResult, ExecutorDispatch, FinalEvaluation
from executor_protocol import ExecutorDispatchStatus
from tracker.database.session import get_session
from tracker.domain import BenchmarkStatus, DocentReadingStatus, FailureCategory, TaskStatus
from tracker.executor.control_protocol import AttemptRef, PhaseRef
from tracker.executor.control_runs import (
    CommitFinalScoreRequest,
    CommitRunErrorDecision,
    CommitRunErrorRequest,
    DatasetVersionSnapshot,
    FinalizationDecision,
    FinalScoreInputs,
    PrepareFinalizationRequest,
    PublicationClaim,
    PublicationDecision,
    PublishFinalViewRequest,
    ReadDatasetVersionRequest,
    ReadRunStateRequest,
    RecordCancellationRequest,
    RunSnapshot,
    RunTaskState,
    SweepExitRequest,
    WriteDecision,
)
from tracker.executor.control_store import (
    authenticated_invocation,
    complete_response,
    dispatch_storage_time,
    require_dispatch_owner,
    require_publication_phase_owner,
)
from tracker.executor.dispatch_control import record_dispatch_failure, terminalize_active_dispatches
from tracker.executor.release_control import share_executor_admission
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.dataset_version import DatasetVersionRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.repositories.organization import OrganizationRepository
from tracker.remote_grader import attempt_id_for
from tracker.repositories.remote_grader import RemoteGraderRepository
from tracker.repositories.sandbox_build_reservation import SandboxBuildReservationRepository
from tracker.repositories.service_demand import wake_recorded
from tracker.services.datasets.ref import HostedDataset
from tracker.services.final_views import create_final_view

router = APIRouter(prefix="/executor-control/v1")
_RUNNABLE = (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
_TERMINAL = (BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED)


def _owner(session: Session, request: Request, ref, *, live: bool, lock: bool = True) -> Benchmark:
    return _owned_run(session, request, ref, live=live, lock=lock)[0]


def _owned_run(
    session: Session, request: Request, ref, *, live: bool, lock: bool = True
) -> tuple[Benchmark, ExecutorDispatch]:
    invocation = authenticated_invocation(request)
    if invocation.dispatch_id != ref.dispatch_id:
        raise HTTPException(status_code=403)
    share_executor_admission(session)
    benchmark = BenchmarkRepository(session).get_by_id(ref.benchmark_id, for_update=lock)
    if benchmark is None:
        raise HTTPException(status_code=403)
    dispatch = require_dispatch_owner(session, ref.dispatch_id, invocation.invocation_key, lock=lock, require_live=live)
    if dispatch.benchmark_id != benchmark.id:
        raise HTTPException(status_code=403)
    return benchmark, dispatch


def _runnable(session: Session, benchmark: Benchmark) -> bool:
    return BenchmarkRepository(session).has_tasks(benchmark.org_id, benchmark.id, _RUNNABLE)


def _stopped(session: Session, benchmark: Benchmark) -> bool:
    return BenchmarkRepository(session).has_tasks(benchmark.org_id, benchmark.id, (TaskStatus.STOPPED,))


def _score_state(session: Session, benchmark: Benchmark) -> tuple[dict[str, dict[str, Any] | None], str]:
    repository = BenchmarkRepository(session)
    tasks = repository.task_attempts(benchmark.org_id, benchmark.id, for_update=True)
    result_rows = repository.latest_evaluation_results(benchmark.org_id, benchmark.id) if tasks else []
    latest = {}
    for task_row_id, result_id, result in result_rows:
        latest.setdefault(task_row_id, (result_id, result))
    values = {
        task_id: latest[task_row_id][1] if status == TaskStatus.FINISHED and task_row_id in latest else None
        for task_row_id, task_id, _started_at, status in tasks
    }
    fingerprint = [
        (
            str(task_row_id),
            started_at.isoformat(),
            status.value,
            str(latest[task_row_id][0]) if task_row_id in latest else None,
        )
        for task_row_id, _task_id, started_at, status in tasks
    ]
    return values, sha256(json.dumps(fingerprint, separators=(",", ":")).encode()).hexdigest()


def _set_final_status(session: Session, benchmark: Benchmark, dispatch_id: UUID) -> BenchmarkStatus:
    if _runnable(session, benchmark):
        raise HTTPException(status_code=409, detail="Tasks are still runnable")
    status = BenchmarkStatus.STOPPED if _stopped(session, benchmark) else BenchmarkStatus.FINISHED
    terminalize_active_dispatches(session, benchmark.id, except_dispatch_id=dispatch_id)
    benchmark.status = status
    benchmark.finished_at = datetime.now(UTC)
    benchmark.error_message = None
    BenchmarkRepository(session).save(benchmark)
    return status


@router.post(
    "/read_run_state",
    response_model=RunSnapshot,
    description="Return the persisted run and task state for an owned dispatch.",
)
def read_run_state(body: ReadRunStateRequest, request: Request, session: Session = Depends(get_session)) -> RunSnapshot:
    benchmark, dispatch = _owned_run(session, request, body.ref, live=False, lock=body.create_missing_tasks)
    current = (
        dispatch.status == ExecutorDispatchStatus.RUNNING
        and (
            dispatch.ecs_task_arn is not None
            or (
                dispatch.lease_expires_at is not None
                and dispatch.lease_expires_at
                > dispatch_storage_time(ExecutorDispatchRepository(session).clock_timestamp())
            )
        )
        and benchmark.status not in (BenchmarkStatus.STOPPED, BenchmarkStatus.ERROR)
    )
    if body.create_missing_tasks and current:
        repository = BenchmarkRepository(session)
        existing = repository.task_ids(benchmark.org_id, benchmark.id, body.tracked_task_ids)
        repository.add_tasks(
            benchmark.org_id, benchmark.id, [task_id for task_id in body.tracked_task_ids if task_id not in existing]
        )
        session.flush()
    rows = BenchmarkRepository(session).list_tasks(benchmark.org_id, benchmark.id)
    reservations = SandboxBuildReservationRepository(session).active_task_ids([row.id for row in rows]) if rows else []
    org = OrganizationRepository(session).get(benchmark.org_id)
    assert org is not None
    snapshot = RunSnapshot(
        authority_current=current,
        status=benchmark.status,
        concurrency=benchmark.arguments.concurrency,
        queue_pool_id=benchmark.arguments.queue_pool_id,
        tasks=[
            RunTaskState(
                ref=AttemptRef(org_id=row.org_id, task_row_id=row.id, task_id=row.task_id, started_at=row.started_at),
                status=row.status,
                eval_resume_state=row.eval_resume_state,
            )
            for row in rows
        ],
        reserved_task_ids=list(reservations),
        org_id=org.id,
        org_name=org.name,
        org_storage_key=org.storage_key,
        benchmark_name=benchmark.name,
        custom_benchmark_service=benchmark.custom_benchmark_service,
        benchmark_service_woken=wake_recorded(session, benchmark.name),
        arguments=benchmark.arguments.model_dump(mode="json"),
    )
    session.commit()
    return complete_response(session, snapshot)


@router.post(
    "/read_dataset_version",
    response_model=DatasetVersionSnapshot,
    description="Return the task digests and metric script digest of the dataset version an owned run is pinned to.",
)
def read_dataset_version(
    body: ReadDatasetVersionRequest, request: Request, session: Session = Depends(get_session)
) -> DatasetVersionSnapshot:
    benchmark = _owner(session, request, body.ref, live=False)
    ref = benchmark.arguments.dataset_ref
    if not isinstance(ref, HostedDataset) or ref.dataset_id is None or ref.revision is None:
        raise HTTPException(status_code=422, detail="The run is not pinned to a hosted dataset version")
    repository = DatasetVersionRepository(session)
    tasks = repository.task_digests(ref.dataset_id, ref.revision)
    if tasks is None:
        raise HTTPException(
            status_code=422, detail=f"The dataset {ref.benchmark}/{ref.dataset} has no revision {ref.revision}"
        )
    snapshot = DatasetVersionSnapshot(
        task_digests=tasks, metric_digest=repository.metric_digest(ref.dataset_id, ref.revision)
    )
    return complete_response(session, snapshot)


@router.post(
    "/prepare_finalization",
    response_model=FinalizationDecision,
    description="Prepare final scoring for an owned run without runnable tasks.",
)
def prepare_finalization(
    body: PrepareFinalizationRequest, request: Request, session: Session = Depends(get_session)
) -> FinalizationDecision:
    benchmark = _owner(session, request, body.ref, live=True)
    if benchmark.status in _TERMINAL or _runnable(session, benchmark):
        return complete_response(
            session, FinalizationDecision(kind="deferred", inputs=None, task_errors=None, fingerprint=None)
        )
    if _stopped(session, benchmark):
        _set_final_status(session, benchmark, body.ref.dispatch_id)
        session.commit()
        return complete_response(
            session, FinalizationDecision(kind="stopped", inputs=None, task_errors=None, fingerprint=None)
        )
    results, fingerprint = _score_state(session, benchmark)
    if not any(value is not None for value in results.values()):
        errors = {
            task_id: message
            for task_id, (message, _category) in BenchmarkRepository(session).task_failures(benchmark).items()
        }
        return complete_response(
            session, FinalizationDecision(kind="all_error", inputs=None, task_errors=errors, fingerprint=fingerprint)
        )
    if benchmark.publication_phase_dispatch_id not in (None, body.ref.dispatch_id):
        return complete_response(
            session, FinalizationDecision(kind="deferred", inputs=None, task_errors=None, fingerprint=None)
        )
    new_phase = benchmark.publication_phase_token is None
    if new_phase:
        benchmark.publication_phase_dispatch_id = body.ref.dispatch_id
        benchmark.publication_phase_token = uuid4()
        benchmark.publication_phase_fingerprint = fingerprint
        BenchmarkRepository(session).save(benchmark)
    elif benchmark.publication_phase_fingerprint != fingerprint:
        return complete_response(
            session, FinalizationDecision(kind="deferred", inputs=None, task_errors=None, fingerprint=None)
        )
    decision = FinalizationDecision(
        kind="score",
        inputs=FinalScoreInputs(
            phase=PhaseRef(token=benchmark.publication_phase_token), fingerprint=fingerprint, evaluation_results=results
        ),
        task_errors=None,
        fingerprint=None,
    )
    if new_phase:
        session.commit()
    return complete_response(session, decision)


@router.post(
    "/commit_final_score",
    response_model=PublicationClaim,
    description="Commit a final run score when the publication phase and task state match.",
)
def commit_final_score(
    body: CommitFinalScoreRequest, request: Request, session: Session = Depends(get_session)
) -> PublicationClaim:
    benchmark = _owner(session, request, body.ref, live=True)
    if benchmark.status in _TERMINAL or _runnable(session, benchmark):
        return complete_response(session, PublicationClaim(committed=False, phase=None, final_view=None, status=None))
    require_publication_phase_owner(body.ref.dispatch_id, body.phase, benchmark=benchmark)
    if (
        benchmark.publication_phase_fingerprint != body.fingerprint
        or _score_state(session, benchmark)[1] != body.fingerprint
    ):
        return complete_response(session, PublicationClaim(committed=False, phase=None, final_view=None, status=None))
    final_evaluation = FinalEvaluation(
        org_id=benchmark.org_id, benchmark=benchmark.id, final_score=body.final_score, properties=body.metadata
    )
    BenchmarkRepository(session).replace_final_evaluation(benchmark, final_evaluation)
    benchmark.final_evaluation = final_evaluation
    status = _set_final_status(session, benchmark, body.ref.dispatch_id)
    benchmark.publication_status = "FINAL_STATUS_COMMITTED"
    session.flush()
    org = OrganizationRepository(session).get(benchmark.org_id)
    assert org is not None
    final_view = create_final_view(benchmark, session, org)
    session.commit()
    return complete_response(
        session, PublicationClaim(committed=True, phase=body.phase, final_view=final_view, status=status)
    )


@router.post(
    "/publish_final_view",
    response_model=PublicationDecision,
    description="Advance the final view publication phase for an owned run.",
)
def publish_final_view(
    body: PublishFinalViewRequest, request: Request, session: Session = Depends(get_session)
) -> PublicationDecision:
    benchmark = _owner(session, request, body.ref, live=False)
    require_publication_phase_owner(body.ref.dispatch_id, body.phase, benchmark=benchmark)
    transitions = {
        "upload_before": ("FINAL_STATUS_COMMITTED", "UPLOAD_ATTEMPTED"),
        "upload_after": ("UPLOAD_ATTEMPTED", "UPLOAD_COMPLETED"),
        "callback_before": ("UPLOAD_COMPLETED", "CALLBACK_ATTEMPTED"),
        "callback_after": ("CALLBACK_ATTEMPTED", "CALLBACK_COMPLETED"),
    }
    expected, next_state = transitions[body.effect]
    if benchmark.publication_status != expected:
        return complete_response(session, PublicationDecision(allowed=False))
    benchmark.publication_status = next_state
    BenchmarkRepository(session).save(benchmark)
    session.commit()
    return complete_response(session, PublicationDecision(allowed=True))


@router.post(
    "/commit_run_error",
    response_model=CommitRunErrorDecision,
    description="Commit an owned run failure against its current task state.",
)
def commit_run_error(
    body: CommitRunErrorRequest, request: Request, session: Session = Depends(get_session)
) -> CommitRunErrorDecision:
    benchmark = _owner(session, request, body.ref, live=True)
    if benchmark.status in _TERMINAL:
        return complete_response(session, CommitRunErrorDecision(committed=False, status=benchmark.status))
    if body.failure.fingerprint is not None:
        if _runnable(session, benchmark) or _stopped(session, benchmark):
            return complete_response(session, CommitRunErrorDecision(committed=False, status=benchmark.status))
        if _score_state(session, benchmark)[1] != body.failure.fingerprint:
            return complete_response(session, CommitRunErrorDecision(committed=False, status=benchmark.status))
    committed = record_dispatch_failure(
        session,
        benchmark_id=benchmark.id,
        dispatch_id=body.ref.dispatch_id,
        task_ids=body.failure.task_ids,
        error_message=body.failure.error_message,
        producer=body.failure.producer,
        operation=body.failure.operation,
        error_type=body.failure.error_type,
        category=body.failure.category,
        cause_code=body.failure.cause_code,
        failure_reason="RUN_ERROR",
    )
    decision = CommitRunErrorDecision(committed=committed, status=benchmark.status)
    session.commit()
    return complete_response(session, decision)


@router.post(
    "/record_run_cancellation",
    response_model=CommitRunErrorDecision,
    description="Record interrupted task attempts and fail their owned run when complete.",
)
def record_run_cancellation(
    body: RecordCancellationRequest, request: Request, session: Session = Depends(get_session)
) -> CommitRunErrorDecision:
    benchmark = _owner(session, request, body.ref, live=True)
    if benchmark.status in _TERMINAL:
        return complete_response(session, CommitRunErrorDecision(committed=False, status=benchmark.status))
    transitioned = False
    rows = (
        BenchmarkRepository(session).lock_tasks(benchmark.org_id, benchmark.id, row_ids=list(body.owned_attempts))
        if body.owned_attempts
        else []
    )
    for task in rows:
        irrecoverable = task.status == TaskStatus.IN_PROGRESS or (
            task.status == TaskStatus.EVALUATING and task.eval_resume_state is None
        )
        if task.started_at != body.owned_attempts[task.id] or not irrecoverable:
            continue
        BenchmarkRepository(session).save(
            ErrorResult(
                org_id=benchmark.org_id,
                task=task.id,
                error_message="Run was interrupted",
                producer="tracker",
                operation="process_benchmark",
                error_type=asyncio.CancelledError.__name__,
                category=FailureCategory.CANCELLED,
            )
        )
        RemoteGraderRepository(session).close_attempt(task.id, attempt_id_for(task))
        task.status = TaskStatus.ERROR
        task.finished_at = datetime.now(UTC)
        BenchmarkRepository(session).save(task)
        transitioned = True
    if (
        transitioned
        and not _runnable(session, benchmark)
        and set(body.verified_task_ids) <= BenchmarkRepository(session).task_ids(benchmark.org_id, benchmark.id)
    ):
        if benchmark.docent_reading_status == DocentReadingStatus.RUNNING:
            benchmark.docent_reading_status = DocentReadingStatus.ERROR
        benchmark.status = BenchmarkStatus.ERROR
        benchmark.finished_at = datetime.now(UTC)
        benchmark.error_message = "Run was interrupted"
        BenchmarkRepository(session).save(benchmark)
    session.commit()
    return complete_response(session, CommitRunErrorDecision(committed=transitioned, status=benchmark.status))


@router.post(
    "/sweep_exit",
    response_model=WriteDecision,
    description="Fail unfinished tasks when an owned run exits without finalization.",
)
def sweep_exit(body: SweepExitRequest, request: Request, session: Session = Depends(get_session)) -> WriteDecision:
    benchmark = _owner(session, request, body.ref, live=False)
    if benchmark.status in _TERMINAL:
        return complete_response(session, WriteDecision(committed=False))
    rows = BenchmarkRepository(session).lock_tasks(
        benchmark.org_id, benchmark.id, task_ids=body.task_ids, statuses=_RUNNABLE
    )
    if benchmark.docent_reading_status == DocentReadingStatus.RUNNING:
        benchmark.docent_reading_status = DocentReadingStatus.ERROR
        BenchmarkRepository(session).save(benchmark)
    committed = record_dispatch_failure(
        session,
        benchmark_id=benchmark.id,
        dispatch_id=body.ref.dispatch_id,
        task_ids=[task.task_id for task in rows],
        error_message=f"Run {benchmark.id} exited without finishing",
        producer="tracker",
        operation="cleanup",
        error_type="UndetectedExecutorExit",
        category=FailureCategory.INFRASTRUCTURE,
    )
    session.commit()
    return complete_response(session, WriteDecision(committed=committed))
