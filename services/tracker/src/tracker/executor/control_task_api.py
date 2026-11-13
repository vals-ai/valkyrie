from datetime import UTC, datetime, timedelta
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session

from tracker.database.models import (
    Benchmark,
    ErrorResult,
    ExecutorAdmission,
    ExecutorDispatch,
    EgressProxyPolicy,
    EvaluationResult,
    Task,
    TaskBreakdown,
)
from tracker.database.session import get_session
from tracker.domain import AgentCausedExitReason, TaskStatus
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.control_queue import queued_sandbox_name
from tracker.executor.control_protocol import AttemptRef, DispatchRef, PhaseRef, TaskRef
from tracker.executor.control_store import (
    Invocation,
    complete_response,
    authenticated_invocation,
    park_new_build_admission,
    require_dispatch_owner,
    require_task_phase_owner,
)
from tracker.executor.control_tasks import (
    EgressGrantRequest,
    EgressGrantResponse,
    EgressRevokeRequest,
    BenchmarkState,
    DirectAdmissionResponse,
    NativeOutputRequest,
    SandboxCostRequest,
    SandboxLinkRequest,
    TaskAccountingRequest,
    TaskBreakdownRequest,
    TaskDeadlineResponse,
    TaskDecision,
    TaskEvaluationRequest,
    TaskEvaluationResponse,
    TaskFailureRequest,
    TaskMonitorRequest,
    TaskMonitorResponse,
    TaskMutationRequest,
    TaskReadRequest,
    TaskReadResponse,
    TaskResultRequest,
    TaskResumeRequest,
    TaskSearchCostsRequest,
    TaskState,
)
from tracker.executor.native_output import clear_native_output, publish_native_output
from tracker.executor.release_control import MaintenanceModeError, share_executor_admission
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.cost import CostRepository
from tracker.repositories.egress_policy import EgressPolicyRepository
from tracker.runtime.model_gateway import GATEWAY_TOKEN_TTL_SECONDS
from tracker.search_cost import search_cost_source_id

router = APIRouter(prefix="/executor-control/v1")
_TASK_WALL_SECONDS = GATEWAY_TOKEN_TTL_SECONDS - 2 * 60 * 60


def _owned_dispatch(
    session: Session,
    invocation: Invocation,
    ref: DispatchRef,
    *,
    live: bool,
    admission: bool,
    lock: bool = True,
    shared: bool = False,
) -> tuple[Benchmark, ExecutorAdmission, ExecutorDispatch]:
    if invocation.dispatch_id != ref.dispatch_id:
        raise HTTPException(status_code=403)
    state = share_executor_admission(session)
    if admission and state.maintenance_target_sha is not None:
        raise MaintenanceModeError("Executor maintenance is in progress")
    benchmark = BenchmarkRepository(session).get_by_id(ref.benchmark_id, for_update=lock, for_share=lock and shared)
    if benchmark is None:
        raise HTTPException(status_code=403)
    dispatch = require_dispatch_owner(
        session, ref.dispatch_id, invocation.invocation_key, lock=lock, require_live=live, shared=shared
    )
    if dispatch.benchmark_id != benchmark.id:
        raise HTTPException(status_code=403)
    return benchmark, state, dispatch


def _owner(
    session: Session,
    invocation: Invocation,
    ref: DispatchRef,
    *,
    live: bool = True,
    admission: bool = False,
    lock: bool = True,
    shared: bool = False,
) -> Benchmark:
    return _owned_dispatch(session, invocation, ref, live=live, admission=admission, lock=lock, shared=shared)[0]


def _task(session: Session, benchmark: Benchmark, ref: TaskRef) -> Task:
    task = BenchmarkRepository(session).lock_task_by_row_id(ref.task_row_id)
    if task.benchmark != benchmark.id or task.org_id != ref.org_id or task.task_id != ref.task_id:
        raise HTTPException(status_code=409, detail="Task identity changed")
    if isinstance(ref, AttemptRef) and task.started_at != ref.started_at:
        raise ExecutionAuthorityRevoked("Task attempt changed")
    return task


def _state(task: Task) -> TaskState:
    return TaskState(
        id=task.id,
        org_id=task.org_id,
        task_id=task.task_id,
        benchmark=task.benchmark,
        started_at=task.started_at,
        status=task.status,
        eval_resume_state=task.eval_resume_state,
        credited_wall_deadline_at=task.credited_wall_deadline_at,
        build_phase=PhaseRef(token=task.build_phase_token) if task.build_phase_token else None,
        evaluation_phase=PhaseRef(token=task.evaluation_phase_token) if task.evaluation_phase_token else None,
    )


@router.post(
    "/read_task",
    response_model=TaskReadResponse,
    description="Return a task attempt and its run state for an owned dispatch.",
)
def read_task(
    body: TaskReadRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskReadResponse:
    benchmark = _owner(session, invocation, body.ref, shared=True)
    task = _task(session, benchmark, body.task)
    return complete_response(
        session,
        TaskReadResponse(
            task=_state(task),
            benchmark=BenchmarkState(
                name=benchmark.name,
                started_by_email=benchmark.started_by_email,
                selected_project_gateway_secret_ref=benchmark.arguments.selected_project_gateway_secret_ref,
                status=benchmark.status,
                concurrency=benchmark.arguments.concurrency,
                contract_name=benchmark.arguments.contract.name,
            ),
        ),
    )


@router.post(
    "/monitor_tasks",
    response_model=TaskMonitorResponse,
    description="Return the current task states and run concurrency for an owned dispatch.",
)
def monitor_tasks(
    body: TaskMonitorRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskMonitorResponse:
    benchmark = _owner(session, invocation, body.ref, lock=False)
    tasks = (
        BenchmarkRepository(session).list_tasks_by_id_for_benchmark(benchmark.id, body.task_ids)
        if body.task_ids
        else []
    )
    return complete_response(
        session,
        TaskMonitorResponse(
            benchmark_status=benchmark.status,
            concurrency=benchmark.arguments.concurrency,
            tasks={task.task_id: _state(task) for task in tasks},
        ),
    )


@router.post(
    "/admit_direct_task",
    response_model=DirectAdmissionResponse,
    description="Admit an owned direct task when run capacity and build admission allow it.",
)
def admit_direct_task(
    body: TaskEvaluationRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> DirectAdmissionResponse:
    benchmark = _owner(session, invocation, body.ref, admission=True)
    task = _task(session, benchmark, body.attempt)
    if task.status in (TaskStatus.STOPPED, TaskStatus.FINISHED, TaskStatus.ERROR):
        return complete_response(session, DirectAdmissionResponse(admitted=None, phase=None))
    if task.build_phase_token is None:
        park_new_build_admission(share_executor_admission(session))
    if task.status == TaskStatus.PENDING:
        active = BenchmarkRepository(session).count_tasks(
            task.org_id, benchmark.id, (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
        )
        if active >= benchmark.arguments.concurrency:
            return complete_response(session, DirectAdmissionResponse(admitted=False, phase=None))
    if task.build_phase_dispatch_id not in (None, body.ref.dispatch_id):
        raise ExecutionAuthorityRevoked("Task build phase authority changed")
    task.status = TaskStatus.BUILDING
    if task.build_phase_token is None:
        task.build_phase_token = uuid4()
        task.build_phase_dispatch_id = body.ref.dispatch_id
    BenchmarkRepository(session).save(task)
    session.commit()
    return complete_response(
        session, DirectAdmissionResponse(admitted=True, phase=PhaseRef(token=task.build_phase_token))
    )


@router.post(
    "/transition_task",
    response_model=TaskDecision,
    description="Transition an owned task attempt when its expected status matches.",
)
def transition_task(
    body: TaskMutationRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDecision:
    benchmark = _owner(session, invocation, body.ref)
    task = _task(session, benchmark, body.attempt)
    expected = body.expected_status
    allowed = (
        task.status in expected
        if isinstance(expected, list)
        else task.status == expected
        if expected is not None
        else task.status not in (TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED)
    )
    if not allowed:
        return complete_response(session, TaskDecision(accepted=False))
    task.status = body.to_status
    if body.to_status in (TaskStatus.FINISHED, TaskStatus.ERROR):
        task.finished_at = datetime.now(UTC)
    if body.to_status in (TaskStatus.PENDING, TaskStatus.EVALUATING, TaskStatus.STOPPED):
        task.build_phase_dispatch_id = None
        task.build_phase_token = None
    if body.to_status in (TaskStatus.PENDING, TaskStatus.STOPPED):
        task.evaluation_phase_dispatch_id = None
        task.evaluation_phase_token = None
    BenchmarkRepository(session).save(task)
    session.commit()
    return complete_response(session, TaskDecision(accepted=True))


@router.post(
    "/ensure_task_deadline",
    response_model=TaskDeadlineResponse,
    description="Return or create the credited wall deadline for an owned task attempt.",
)
def ensure_task_deadline(
    body: TaskEvaluationRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDeadlineResponse:
    benchmark = _owner(session, invocation, body.ref, shared=True)
    task = _task(session, benchmark, body.attempt)
    if task.status == TaskStatus.STOPPED:
        raise ExecutionAuthorityRevoked("Task stopped")
    if task.credited_wall_deadline_at is None:
        task.credited_wall_deadline_at = datetime.now(UTC) + timedelta(seconds=_TASK_WALL_SECONDS)
        BenchmarkRepository(session).save(task)
        session.commit()
    return complete_response(session, TaskDeadlineResponse(deadline=task.credited_wall_deadline_at))


@router.post(
    "/save_task_resume",
    response_model=TaskDecision,
    description="Save the evaluation checkpoint for an owned task phase.",
)
def save_task_resume(
    body: TaskResumeRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDecision:
    benchmark = _owner(session, invocation, body.ref, shared=True)
    task = _task(session, benchmark, body.attempt)
    require_task_phase_owner(body.ref.dispatch_id, body.phase, scope="evaluation", task=task)
    if task.status != TaskStatus.EVALUATING:
        return complete_response(session, TaskDecision(accepted=False))
    task.eval_resume_state = body.state
    BenchmarkRepository(session).save(task)
    session.commit()
    return complete_response(session, TaskDecision(accepted=True))


@router.post(
    "/claim_task_evaluation",
    response_model=TaskEvaluationResponse,
    description="Claim evaluation authority for an owned task attempt.",
)
def claim_task_evaluation(
    body: TaskEvaluationRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskEvaluationResponse:
    benchmark = _owner(session, invocation, body.ref, shared=True)
    task = _task(session, benchmark, body.attempt)
    if task.status != TaskStatus.EVALUATING:
        return complete_response(session, TaskEvaluationResponse(phase=None, resume_state=None))
    if task.evaluation_phase_dispatch_id not in (None, body.ref.dispatch_id):
        raise ExecutionAuthorityRevoked("Task evaluation phase authority changed")
    if task.evaluation_phase_token is None:
        task.evaluation_phase_token = uuid4()
        task.evaluation_phase_dispatch_id = body.ref.dispatch_id
        BenchmarkRepository(session).save(task)
        session.commit()
    return complete_response(
        session,
        TaskEvaluationResponse(phase=PhaseRef(token=task.evaluation_phase_token), resume_state=task.eval_resume_state),
    )


@router.post(
    "/write_task_breakdown",
    response_model=TaskDecision,
    description="Record build and agent durations for an owned task attempt.",
)
def write_task_breakdown(
    body: TaskBreakdownRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDecision:
    benchmark = _owner(session, invocation, body.ref, shared=True)
    task = _task(session, benchmark, body.attempt)
    if task.status != TaskStatus.IN_PROGRESS:
        return complete_response(session, TaskDecision(accepted=False))
    repository = BenchmarkRepository(session)
    breakdown = repository.get_task_breakdown(task.task_breakdown) if task.task_breakdown else None
    if breakdown is None:
        breakdown = TaskBreakdown()
        repository.attach_task_breakdown(task, breakdown)
    breakdown.sandbox_build_duration = body.sandbox_build_duration
    breakdown.agent_run_duration = body.agent_run_duration
    task.status = TaskStatus.EVALUATING
    task.build_phase_dispatch_id = None
    task.build_phase_token = None
    repository.save(task)
    session.commit()
    return complete_response(session, TaskDecision(accepted=True))


@router.post(
    "/write_task_accounting",
    response_model=TaskDecision,
    description="Record generation accounting for an owned task attempt.",
)
def write_task_accounting(
    body: TaskAccountingRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDecision:
    benchmark = _owner(session, invocation, body.ref, shared=True)
    task = _task(session, benchmark, body.attempt)
    if task.status == TaskStatus.STOPPED:
        return complete_response(session, TaskDecision(accepted=False))
    repository = BenchmarkRepository(session)
    breakdown = repository.get_task_breakdown(task.task_breakdown) if task.task_breakdown else None
    if breakdown is None:
        breakdown = TaskBreakdown()
        repository.attach_task_breakdown(task, breakdown)
    summary = body.summary
    for key in (
        "accounting_id",
        "base_generation_allowance_seconds",
        "cumulative_time_credit_cap_seconds",
        "external_service_overhead_seconds",
        "external_service_credit_applied_seconds",
        "external_service_credit_revision",
        "generation_elapsed_seconds",
    ):
        setattr(breakdown, key, summary[key])
    repository.save(task)
    session.commit()
    return complete_response(session, TaskDecision(accepted=True))


@router.post(
    "/write_task_search_costs",
    response_model=TaskDecision,
    description="Record search costs for an owned task attempt.",
)
def write_task_search_costs(
    body: TaskSearchCostsRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDecision:
    benchmark = _owner(session, invocation, body.ref, shared=True)
    task = _task(session, benchmark, body.attempt)
    attempt_id = queued_sandbox_name(task.id, task.started_at).removeprefix("queued-")
    CostRepository(session).add_search_costs(
        benchmark_id=benchmark.id, attempt_id=search_cost_source_id(attempt_id, body.step_name), charges=body.charges
    )
    session.commit()
    return complete_response(session, TaskDecision(accepted=True))


@router.post(
    "/finish_task_evaluation",
    response_model=TaskDecision,
    description="Persist an evaluation result and finish its owned task attempt.",
)
def finish_task_evaluation(
    body: TaskResultRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDecision:
    benchmark = _owner(session, invocation, body.ref)
    task = _task(session, benchmark, body.attempt)
    require_task_phase_owner(body.ref.dispatch_id, body.phase, scope="evaluation", task=task)
    if task.status not in (TaskStatus.EVALUATING, TaskStatus.ERROR):
        return complete_response(session, TaskDecision(accepted=False))
    repository = BenchmarkRepository(session)
    repository.save(
        EvaluationResult(
            org_id=task.org_id,
            task=task.id,
            instance_id=body.instance_id,
            agent_caused_exit_reason=AgentCausedExitReason(body.agent_caused_exit_reason)
            if body.agent_caused_exit_reason
            else None,
            result=body.result,
        )
    )
    breakdown = repository.get_task_breakdown(task.task_breakdown) if task.task_breakdown else None
    if breakdown is not None:
        breakdown.evaluation_run_duration = body.evaluation_run_duration
        if body.sandbox_run_duration is not None:
            breakdown.sandbox_run_duration = body.sandbox_run_duration
        repository.save(breakdown)
    task.status = TaskStatus.FINISHED
    task.finished_at = datetime.now(UTC)
    task.evaluation_phase_dispatch_id = None
    task.evaluation_phase_token = None
    repository.save(task)
    session.commit()
    return complete_response(session, TaskDecision(accepted=True))


@router.post(
    "/fail_task",
    response_model=TaskDecision,
    description="Record a failure for an owned task attempt and update its status.",
)
def fail_task(
    body: TaskFailureRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDecision:
    benchmark = _owner(session, invocation, body.ref)
    task = _task(session, benchmark, body.attempt)
    if task.status in (TaskStatus.STOPPED, TaskStatus.FINISHED, TaskStatus.ERROR) or (
        body.expected_status is not None and task.status != body.expected_status
    ):
        return complete_response(session, TaskDecision(accepted=False))
    if task.status == TaskStatus.EVALUATING and task.evaluation_phase_dispatch_id not in (None, body.ref.dispatch_id):
        raise ExecutionAuthorityRevoked("Task evaluation phase authority changed")
    retain_evaluation_phase = (
        task.status == TaskStatus.EVALUATING
        and task.evaluation_phase_dispatch_id == body.ref.dispatch_id
        and task.evaluation_phase_token is not None
    )
    failure = body.failure
    repository = BenchmarkRepository(session)
    repository.save(
        ErrorResult(
            org_id=task.org_id,
            task=task.id,
            error_message=failure.message,
            producer=failure.producer,
            operation=failure.operation,
            error_type=failure.error_type,
            category=failure.category,
            cause_code=failure.cause_code,
            phase=failure.phase,
            transient=failure.transient,
            retry_scheduled=body.retry_scheduled,
            failed_attempt_number=body.failed_attempt_number,
        )
    )
    if not body.retry_scheduled:
        task.status = TaskStatus.ERROR
        task.finished_at = datetime.now(UTC)
        task.build_phase_dispatch_id = None
        task.build_phase_token = None
        if not retain_evaluation_phase:
            task.evaluation_phase_dispatch_id = None
            task.evaluation_phase_token = None
        repository.save(task)
    session.commit()
    return complete_response(session, TaskDecision(accepted=True))


@router.post(
    "/publish_native_output",
    response_model=TaskDecision,
    description="Publish a native output descriptor for an owned active task attempt.",
)
def publish_output(
    body: NativeOutputRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDecision:
    benchmark = _owner(session, invocation, body.ref, shared=True)
    task = _task(session, benchmark, body.attempt)
    if task.status not in (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS):
        return complete_response(session, TaskDecision(accepted=False))
    publish_native_output(session, task, body.descriptor)
    session.commit()
    return complete_response(session, TaskDecision(accepted=True))


@router.post(
    "/clear_native_output",
    response_model=TaskDecision,
    description="Clear a native output descriptor for an owned task attempt.",
)
def clear_output(
    body: NativeOutputRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDecision:
    benchmark = _owner(session, invocation, body.ref, live=False, shared=True)
    task = _task(session, benchmark, body.attempt)
    clear_native_output(session, task, body.descriptor)
    session.commit()
    return complete_response(session, TaskDecision(accepted=True))


@router.post(
    "/record_sandbox_link", response_model=TaskDecision, description="Record a sandbox linked to an owned run."
)
def record_link(
    body: SandboxLinkRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDecision:
    benchmark = _owner(session, invocation, body.ref, live=False, shared=True)
    if body.benchmark_id != benchmark.id:
        raise HTTPException(status_code=403)
    BenchmarkRepository(session).record_sandbox_link(
        body.provider, body.sandbox_id, body.benchmark_id, body.seen_at or datetime.now(UTC)
    )
    session.commit()
    return complete_response(session, TaskDecision(accepted=True))


@router.post(
    "/record_sandbox_cost",
    response_model=TaskDecision,
    description="Record the cost of a sandbox linked to an owned run.",
)
def record_cost(
    body: SandboxCostRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> TaskDecision:
    benchmark = _owner(session, invocation, body.ref, live=False, shared=True)
    if body.benchmark_id != benchmark.id:
        raise HTTPException(status_code=403)
    CostRepository(session).add_sandbox_cost(
        benchmark_id=body.benchmark_id,
        provider=body.provider,
        sandbox_id=body.sandbox_id,
        amount_usd=body.amount_usd,
        usage_end=body.usage_end,
    )
    session.commit()
    return complete_response(session, TaskDecision(accepted=True))


@router.post(
    "/grant_egress_policy",
    response_model=EgressGrantResponse,
    description="Grant an egress proxy policy for a task in an owned dispatch.",
)
def grant_egress_policy(
    body: EgressGrantRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> EgressGrantResponse:
    benchmark = _owner(session, invocation, body.ref)
    task = BenchmarkRepository(session).get_task_by_id(body.task_row_id)
    if task is not None and task.benchmark != benchmark.id:
        raise HTTPException(status_code=403)
    accepted = EgressPolicyRepository(session).grant(
        benchmark.id,
        body.task_row_id,
        EgressProxyPolicy(
            key_id=body.key_id,
            benchmark_id=benchmark.id,
            token_sha256=body.token_sha256,
            methods=body.methods,
            domains=body.domains,
            expires_at=body.expires_at,
            inspect_domains=body.inspect_domains,
            gateway_url=body.gateway_url,
        ),
    )
    session.commit()
    return complete_response(session, EgressGrantResponse(accepted=accepted))


@router.post(
    "/revoke_egress_policy",
    response_model=EgressGrantResponse,
    description="Revoke an egress proxy key within an owned dispatch benchmark.",
)
def revoke_egress_policy(
    body: EgressRevokeRequest,
    invocation: Invocation = Depends(authenticated_invocation),
    session: Session = Depends(get_session),
) -> EgressGrantResponse:
    benchmark, _, _ = _owned_dispatch(session, invocation, body.ref, live=False, admission=False)
    EgressPolicyRepository(session).revoke_owned(benchmark.id, body.task_row_id, body.key_id)
    session.commit()
    return complete_response(session, EgressGrantResponse(accepted=True))
