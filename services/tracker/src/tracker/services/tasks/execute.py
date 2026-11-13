import asyncio
import json
import time
import traceback
from hashlib import sha256
from asyncio import Semaphore
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from enum import Enum
from pathlib import PurePosixPath
from types import TracebackType
from typing import Any, TypeVar, cast
from urllib.parse import urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
import logfire
import sentry_sdk
from benchmark_service import (
    ChildEgress,
    ChildPolicy,
    ComposeSource,
    Sandbox,
    SandboxCreatedData,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxProviderConfig,
    SandboxRecoveryAttempt,
)
from benchmark_service.client import BenchmarkServiceError
from benchmark_service.grading_spec import resolve_grading_spec
from benchmark_service.schemas import EgressPolicy

from tracker.runtime.lifecycle import finish_cleanup
from tracker.runtime.logs import task_log_stream_name
from tracker.runtime.secrets import resolve_secrets
from tracker.runtime.services import RuntimeServices
from tracker.runtime.artifacts import step_folder, step_output_name, task_artifact_key
from tracker.runtime.model_gateway import (
    GatewayTokenRotation,
    child_gateway_token,
    grading_gateway_token,
    scoped_gateway_url,
    task_scoped_gateway_key,
)
from tracker.runtime.task_logs import TaskLogBuffer
from tracker.runtime.run_egress import (
    RunAttempt,
    RunStageAccess,
    RunStageEgress,
    SandboxAllowlist,
    SandboxEnvironment,
    ScopedGateway,
)
from tracker.config import (
    BenchmarkServiceDestination,
    ENVIRONMENT,
    EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS,
    EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN,
    EXTERNAL_SERVICE_GATEWAY_URL,
    GATEWAY_KEY_ORGS,
    classify_benchmark_service_destination,
)
from tracker.domain import AgentCausedExitReason, BenchmarkStatus, FailureCategory, FailurePhase, TaskStatus
from tracker.executor.control_protocol import AttemptRef, PhaseRef, TaskRef
from tracker.executor.control_tasks import (
    NativeOutputRequest,
    SandboxLink,
    TaskAccountingRequest,
    TaskBreakdownRequest,
    TaskControl,
    TaskFailure,
    TaskFailureRequest,
    TaskMutationRequest,
    TaskResultRequest,
    TaskResumeRequest,
    TaskSearchCostsRequest,
    TaskState,
)
from tracker.executor.control_queue import queued_sandbox_name
from tracker.services.datasets.config import MultiStepRewardStrategy
from tracker.services.datasets.oracle import ORACLE_AGENT
from tracker.services.datasets.retrieve_task import StepPlan, TaskFolderResponse
from tracker.clients.dataset_tasks import DatasetTaskService
from tracker.clients.task_service import TaskService
from tracker.services.datasets.steps import combine_steps
from tracker.exceptions import (
    AgentRunFailedError,
    CreditedTaskWallTimeExceeded,
    DependencySetupExhaustedError,
    ExecutionAuthorityRevoked,
    GenerationTerminationUnconfirmedError,
    InvalidSandboxConfigurationError,
    SandboxDNSResolutionError,
    SandboxSetupError,
    TrackerServiceError,
)
from tracker.egress import combine_run_egress_policies
from tracker.egress_proxy.resolve import validate_proxy_entries
from tracker.egress_proxy.run_stage import EgressProxy
from tracker.egress_proxy.settings import EGRESS_PROXY
from tracker.external_service_gateway import (
    AccountingSnapshot,
    GenerationAccountingSummary,
    ExternalServiceDeadlineController,
    ExternalServiceGatewayClient,
)

from tracker.generation_plan import GenerationPlan
from tracker.logging import get_logger
from tracker.logging.events import LogEvent
from tracker.observability import TaskOutcome, error_span, incr, record_task_outcome
from tracker.observability.sentry import capture_exception, clear_sandbox_context, task_scope
from tracker.observability.tracing import observability_span
from tracker.sandbox import (
    DependencySetupMode,
    SandboxCreationState,
    apply_egress_policy,
    create_agent_error_redactor,
    create_sandbox,
    install_agent_dependencies,
    run_agent,
    runtime_sandbox,
    sandbox_resolves,
    upload_agent_artifacts,
)
from tracker.secret_access import is_generated_project_gateway_secret_name, tenant_secret_prefix
from tracker.search_cost import SearchCharge
from tracker.scheduler.admission import SandboxQueueContext, enter_queued_sandbox
from tracker.executor.control_queue import is_reserved_queue_pool_id
from tracker.services.tasks.attempts import keep_replaced_attempts
from tracker.types import ActiveNativeOutput, AgentContractRequest, OrgContext, StartBenchmarkRequest

from tracker.services.failures import (
    is_agent_outcome,
    BenchmarkServiceWebSocketDNSResolutionError,
    Disposition,
    Failure,
    classify_failure,
    decide,
    describe_failure,
)
from tracker.services.tasks.steps import StepRun, run_steps
from tracker.clients.benchmark_service import run_benchmark_service_websocket


_Streamed = TypeVar("_Streamed")


def sighting_time() -> datetime:
    return datetime.now(UTC)


logger = get_logger(__name__)

_PTY_TASK_RETRY_LIMIT: int = 1
_SANDBOX_RETRY_DELAY_SECONDS: float = 2
_DIRECT_ADMISSION_POLL_SECONDS: float = 2


def _saved_project_gateway_root(
    request: StartBenchmarkRequest, org: OrgContext, selected_project_gateway_secret_ref: str | None
) -> bool:
    secret_names = request.contract.secrets
    reference = secret_names.get("MODEL_GATEWAY_API_KEY")
    prefix = tenant_secret_prefix(org.id)
    return (
        org.name in GATEWAY_KEY_ORGS
        and classify_benchmark_service_destination(request.benchmark_name, request.custom_benchmark_service).value
        == BenchmarkServiceDestination.HOSTED.value
        and selected_project_gateway_secret_ref is not None
        and reference == selected_project_gateway_secret_ref
        and reference == secret_names.get("MODEL_GATEWAY_URL")
        and reference.startswith(prefix)
        and is_generated_project_gateway_secret_name(reference.removeprefix(prefix))
    )


@dataclass
class _DependencySetupRecoveryState:
    mode: DependencySetupMode = DependencySetupMode.IN_PLACE_RETRIES


def _attested_inference_settings(contract: AgentContractRequest) -> dict[str, str]:
    if not contract.inference_settings_attested:
        return {}
    return {
        "VALKYRIE_AGENT_MODEL": contract.model or "",
        "VALKYRIE_AGENT_VARIANT": contract.kwargs.get("variant", ""),
    }


def _normalized_attempt_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def task_attempt_id(task_row_id: UUID, started_at: datetime) -> str:
    return queued_sandbox_name(task_row_id, started_at).removeprefix("queued-")


@asynccontextmanager
async def _child_egress_grants(
    proxy: EgressProxy,
    children: Mapping[str, ChildPolicy],
    attempt: RunAttempt,
    gateway_env: dict[str, str],
    *,
    task_id: str,
    identity: dict[str, str],
    org_name: str,
) -> AsyncGenerator[dict[str, ChildEgress]]:
    async with AsyncExitStack() as stack:
        grants: dict[str, ChildEgress] = {}
        for name, profile in children.items():
            gateway = await stack.enter_async_context(
                child_gateway_token(
                    gateway_env,
                    profile=name,
                    models=profile.models,
                    egress=profile.egress,
                    run_id=str(attempt.benchmark_id),
                    task_id=task_id,
                    attempt_id=attempt.attempt_id,
                    identity=identity,
                    org_name=org_name,
                )
            )
            grants[name] = await stack.enter_async_context(proxy.child_egress(name, profile, attempt, gateway))
        yield grants


def _exception_message(exc: BaseException) -> str:

    def describe(error: BaseException) -> str:
        detail = str(error).strip()
        name = type(error).__name__
        return f"{name}: {detail}" if detail else name

    message = describe(exc)
    seen = {id(exc)}
    current = exc
    while True:
        if isinstance(current, TrackerServiceError) and not current.include_cause_in_task_error:
            break
        if current.__cause__ is not None:
            next_error = current.__cause__
            relation = "caused by"
        elif not current.__suppress_context__:
            next_error = current.__context__
            relation = "during handling of"
        else:
            break
        if next_error is None or id(next_error) in seen:
            break
        seen.add(id(next_error))
        message = f"{message} ({relation} {describe(next_error)})"
        current = next_error
    return message


async def _record_failure_before_retry(
    task: TaskState,
    control: TaskControl,
    exc: SandboxSetupError,
    failed_attempt_number: int,
) -> None:
    await control.fail(
        TaskFailureRequest(
            ref=control.ref,
            attempt=task.ref,
            failure=TaskFailure(
                message=_exception_message(exc),
                producer="sandbox_provider",
                operation="setup",
                error_type=type(exc).__name__,
                category=FailureCategory.INFRASTRUCTURE,
                cause_code=exc.cause_code,
                phase=exc.phase,
                transient=exc.transient,
            ),
            expected_status=None,
            retry_scheduled=True,
            failed_attempt_number=failed_attempt_number,
        )
    )


_TASK_RETRY_METRIC = "valkyrie.task"


def _observe_task_retry(attempt: SandboxRecoveryAttempt, exc: BaseException) -> None:
    error_class = type(exc).__name__
    failure_category = classify_failure(exc).value
    with observability_span(
        "task.retry", attempt=attempt.number, error_class=error_class, failure_category=failure_category
    ):
        logger.warning(
            "retry.before_sleep",
            extra={
                "event": LogEvent.TASK_RETRY,
                "metric": _TASK_RETRY_METRIC,
                "fn": "_process_task_attempt",
                "attempt": attempt.number,
                "idle_for": _SANDBOX_RETRY_DELAY_SECONDS,
                "error_class": error_class,
                "failure_category": failure_category,
            },
        )
        incr(
            f"{_TASK_RETRY_METRIC}.retry",
            tags={"error_class": error_class, "failure_category": failure_category},
        )


class TrackedTaskStatus(str, Enum):
    WAITING = "waiting"
    RUNNING = "running"
    DONE = "done"


class ResizableLimiter:
    def __init__(self, limit: int):
        if limit < 1:
            raise ValueError("Limit must be greater than 0")
        self._limit = limit
        self._in_flight = 0
        self._condition = asyncio.Condition()

    async def resize(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("Limit must be greater than 0")
        async with self._condition:
            previous_limit = self._limit
            self._limit = limit
            if limit > previous_limit:
                self._condition.notify_all()

    async def __aenter__(self) -> None:
        async with self._condition:
            await self._condition.wait_for(lambda: self._in_flight < self._limit)
            self._in_flight += 1

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        async with self._condition:
            self._in_flight -= 1
            self._condition.notify_all()


class TrackedTask:
    _coro: Coroutine[Any, Any, Any]
    _status: str
    _task: asyncio.Task[Any] | None
    _attempt_started_at: datetime
    _control: TaskControl

    def __init__(self, coro: Coroutine[Any, Any, Any], control: TaskControl, started_at: datetime):
        self._coro = coro
        self._control = control
        self._attempt_started_at = _normalized_attempt_time(started_at)
        self._status = TrackedTaskStatus.WAITING
        self._task = None

    @property
    def status(self) -> str:
        return self._status

    @property
    def task(self) -> asyncio.Task[Any] | None:
        return self._task

    @property
    def attempt_started_at(self) -> datetime:
        return self._attempt_started_at

    async def run(
        self,
        limiter: ResizableLimiter | None,
        task_row: TaskState,
    ) -> dict[str, dict[str, Any] | None]:
        async def _wrap_coro():
            if limiter is None:
                self._status = TrackedTaskStatus.RUNNING

                return await self._coro
            async with limiter:
                self._status = TrackedTaskStatus.RUNNING
                return await self._coro

        with task_scope(task_row.task_id, attempt_started_at=self.attempt_started_at.isoformat()):
            try:
                self._task = asyncio.create_task(_wrap_coro())
                return await self._task
            except asyncio.CancelledError:
                logger.warning("Task %s was cancelled", task_row.task_id, extra={"event": LogEvent.TASK_CANCELLED})
                record_task_outcome(TaskOutcome.CANCELLED)
                self._coro.close()

                return {task_row.task_id: None}
            except ExecutionAuthorityRevoked:
                return {task_row.task_id: None}
            except Exception as e:
                failure = describe_failure(e, _exception_message(e), phase=getattr(e, "phase", None))
                error_message = f"Task error was not handled: {failure.message}\n{traceback.format_exc()}"
                logger.error(
                    error_message,
                    extra={"event": LogEvent.TASK_UNHANDLED_ERROR, "failure_category": failure.category.value},
                )
                logfire.exception("tracked_task_run failed")
                with sentry_sdk.new_scope() as scope:
                    scope.set_tag("failure_category", failure.category.value)
                    capture_exception(e)
                if await self._control.fail(
                    TaskFailureRequest(
                        ref=self._control.ref,
                        attempt=task_row.ref,
                        failure=TaskFailure(
                            message=error_message,
                            producer=failure.producer,
                            operation=failure.operation,
                            error_type=failure.error_type,
                            category=failure.category,
                            cause_code=failure.cause_code,
                            phase=failure.phase,
                            transient=failure.transient,
                        ),
                        expected_status=None,
                        retry_scheduled=False,
                        failed_attempt_number=None,
                    )
                ):
                    record_task_outcome(TaskOutcome.UNHANDLED_ERROR, {"failure_category": failure.category.value})
                return {task_row.task_id: None}
            finally:
                self._status = TrackedTaskStatus.DONE


class TaskMonitor:
    _TRACK_INTERVAL = 2

    def __init__(
        self,
        task_tracking: dict[str, TrackedTask],
        limiter: ResizableLimiter | None,
        *,
        control: TaskControl,
        coordinator_done: asyncio.Event | None = None,
    ):
        self._task_tracking = task_tracking
        self._limiter = limiter
        self._coordinator_done = coordinator_done
        self._cancellation_requested: set[TrackedTask] = set()
        self._control = control

    async def track_tasks(self) -> None:
        revoked = False
        while self._task_tracking or (self._coordinator_done is not None and not self._coordinator_done.is_set()):
            tracking_snapshot = list(self._task_tracking.items())
            tasks_to_check = {
                task_id: tracked for task_id, tracked in tracking_snapshot if tracked.status != TrackedTaskStatus.DONE
            }
            if not revoked:
                try:
                    tick = await self._control.monitor(list(tasks_to_check))
                except ExecutionAuthorityRevoked:
                    revoked = True
                except httpx.ReadTimeout:
                    logger.warning("Task monitor read timed out; retrying next tick", exc_info=True)
                    await asyncio.sleep(self._TRACK_INTERVAL)
                    continue
            for task_id, tracked in tracking_snapshot:
                if self._task_tracking.get(task_id) is not tracked:
                    self._cancellation_requested.discard(tracked)
                elif tracked.status == TrackedTaskStatus.DONE:
                    del self._task_tracking[task_id]
                    self._cancellation_requested.discard(tracked)
            if not self._task_tracking:
                if self._coordinator_done is not None and self._coordinator_done.is_set():
                    break
                await asyncio.sleep(self._TRACK_INTERVAL)
                continue
            if revoked:
                for snapshot in self._control.active_snapshot():
                    task_id = snapshot.task.task_id
                    if task_id in tasks_to_check:
                        tracked = tasks_to_check[task_id]
                        if self._task_tracking.get(task_id) is tracked and tracked.status != TrackedTaskStatus.DONE:
                            self._control.invalidate(snapshot.task.task_row_id)
                for task_id, tracked in tasks_to_check.items():
                    if self._task_tracking.get(task_id) is not tracked or tracked.status == TrackedTaskStatus.DONE:
                        continue
                    task = tracked.task
                    if task is not None and not task.done() and tracked not in self._cancellation_requested:
                        self._cancellation_requested.add(tracked)
                        task.cancel(f"Task {task_id} has been invalidated. Dispatch authority was revoked")
                await asyncio.sleep(self._TRACK_INTERVAL)
                continue
            if self._limiter is not None:
                await self._limiter.resize(tick.concurrency)
            for task_id, tracked in tasks_to_check.items():
                if self._task_tracking.get(task_id) is not tracked or tracked.status == TrackedTaskStatus.DONE:
                    continue
                task_state = tick.tasks[task_id]
                invalid = (
                    task_state.status == TaskStatus.STOPPED
                    or tick.benchmark_status == BenchmarkStatus.ERROR
                    or _normalized_attempt_time(task_state.started_at) != tracked.attempt_started_at
                )
                task = tracked.task
                if invalid:
                    self._control.invalidate(task_state.id)
                if invalid and task is not None and not task.done() and tracked not in self._cancellation_requested:
                    self._cancellation_requested.add(tracked)
                    task.cancel(f"Task {task_id} has been invalidated. Run has been requested to stop")
            await asyncio.sleep(self._TRACK_INTERVAL)


async def commit_task_status_transition(
    task: TaskState,
    control: TaskControl,
    to_status: TaskStatus,
    *,
    expected_status: TaskStatus | tuple[TaskStatus, ...] | None = None,
) -> bool:
    with observability_span(
        "task.status_transition",
        benchmark_id=str(task.benchmark),
        task_id=task.task_id,
        from_status=task.status.value,
        to_status=to_status.value,
    ):
        accepted = await control.transition(
            TaskMutationRequest(
                ref=control.ref,
                attempt=task.ref,
                to_status=to_status,
                expected_status=list(expected_status) if isinstance(expected_status, tuple) else expected_status,
            )
        )
        if accepted:
            task.status = to_status
            if to_status in (TaskStatus.PENDING, TaskStatus.EVALUATING, TaskStatus.STOPPED):
                task.build_phase = None
            if to_status in (TaskStatus.PENDING, TaskStatus.STOPPED):
                task.evaluation_phase = None
            control.track(task)
        return accepted


async def _create_generation_deadline_controller(
    contract: AgentContractRequest,
    plan: GenerationPlan,
    resolved_secrets: dict[str, str],
    scoped_env_vars: dict[str, str],
) -> ExternalServiceDeadlineController:
    if not (
        EXTERNAL_SERVICE_GATEWAY_URL
        and EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN
        and contract.inference_settings_attested
        and contract.model
        and contract.model.strip()
        and resolved_secrets.get("MODEL_GATEWAY_URL")
        and resolved_secrets.get("MODEL_GATEWAY_API_KEY")
    ):
        return ExternalServiceDeadlineController(base_allowance_seconds=plan.allowance_seconds)

    accounting_id = sha256(scoped_env_vars["MODEL_GATEWAY_API_KEY"].encode()).hexdigest()
    client = ExternalServiceGatewayClient(
        EXTERNAL_SERVICE_GATEWAY_URL, control_token=EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN
    )
    return ExternalServiceDeadlineController(
        client=client,
        snapshot=AccountingSnapshot(accounting_id=accounting_id, cumulative_neutral_overhead_ms=0, revision=0),
        base_allowance_seconds=plan.allowance_seconds,
        credit_cap_seconds=EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS,
    )


async def _persist_generation_accounting_summary(
    summary: GenerationAccountingSummary,
    *,
    control: TaskControl,
    attempt: AttemptRef,
) -> None:
    await control.accounting(TaskAccountingRequest(ref=control.ref, attempt=attempt, summary=asdict(summary)))


async def process_task(
    task_row: TaskState,
    start_benchmark_request: StartBenchmarkRequest,
    benchmark_service: TaskService,
    benchmark_id: UUID,
    task_id: str,
    runtime: RuntimeServices,
    org: OrgContext,
    sandbox_provider_config: SandboxProviderConfig,
    creation_semaphore: Semaphore,
    control: TaskControl,
    *,
    sandbox_provider: SandboxProvider | None = None,
    queue_context: SandboxQueueContext | None = None,
    started_by_email: str | None,
) -> dict[str, dict[str, Any] | None]:
    dependency_setup_recovery = _DependencySetupRecoveryState()

    with observability_span(
        "task.started",
        benchmark_id=str(benchmark_id),
        task_id=task_id,
        benchmark_name=start_benchmark_request.benchmark_name,
        agent_name=start_benchmark_request.contract.name,
    ):
        pass

    claimed_at = datetime.now(UTC)

    async def run_attempt(
        recovery_attempt: SandboxRecoveryAttempt,
    ) -> dict[str, dict[str, Any] | None]:
        clear_sandbox_context()
        stream_key = f"{benchmark_id}:{task_log_stream_name(task_id, task_row.started_at)}"
        async with TaskLogBuffer(runtime.logs, stream_key) as task_logs:
            return await _process_task_attempt(
                task_row=task_row,
                start_benchmark_request=start_benchmark_request,
                benchmark_service=benchmark_service,
                benchmark_id=benchmark_id,
                task_id=task_id,
                runtime=runtime,
                task_logs=task_logs,
                org=org,
                sandbox_provider_config=sandbox_provider_config,
                creation_semaphore=creation_semaphore,
                dependency_setup_recovery=dependency_setup_recovery,
                recovery_attempt=recovery_attempt,
                control=control,
                claimed_at=claimed_at,
                sandbox_provider=sandbox_provider,
                queue_context=queue_context,
            )

    async def record_attempt_failure(attempt: SandboxRecoveryAttempt, exc: Exception) -> None:
        _observe_task_retry(attempt, exc)
        if isinstance(exc, SandboxSetupError):
            await _record_failure_before_retry(task_row, control, exc, attempt.number)

    result = await benchmark_service.run_with_sandbox_recovery(
        task_id=task_id,
        run_id=str(benchmark_id),
        attempt_id=task_attempt_id(task_row.id, task_row.started_at),
        email=started_by_email,
        operation=run_attempt,
        dataset=start_benchmark_request.dataset,
        retryable_attempt_errors=(SandboxSetupError,),
        default_max_attempts=_PTY_TASK_RETRY_LIMIT + 1,
        retry_delay_s=_SANDBOX_RETRY_DELAY_SECONDS,
        on_retry=record_attempt_failure,
    )

    if result.get(task_id) is not None:
        with observability_span(
            "task.completed",
            benchmark_id=str(benchmark_id),
            task_id=task_id,
        ):
            pass
        record_task_outcome(TaskOutcome.COMPLETED)

    return result


async def _admit_direct_task(task: TaskState, control: TaskControl) -> bool | None:
    with observability_span(
        "task.status_transition",
        benchmark_id=str(task.benchmark),
        task_id=task.task_id,
        from_status=task.status.value,
        to_status=TaskStatus.BUILDING.value,
    ):
        response = await control.admit(task.ref)
    if response.admitted:
        assert response.phase is not None
        task.status = TaskStatus.BUILDING
        task.build_phase = response.phase
        control.track(task)
    return response.admitted


async def _acquire_queue_preparation(stack: AsyncExitStack, context: SandboxQueueContext | None) -> None:
    if context is not None and is_reserved_queue_pool_id(context.pool_id):
        await stack.enter_async_context(context.preparation_slots)


async def _process_task_attempt(
    task_row: TaskState,
    start_benchmark_request: StartBenchmarkRequest,
    benchmark_service: TaskService,
    benchmark_id: UUID,
    task_id: str,
    runtime: RuntimeServices,
    task_logs: TaskLogBuffer,
    org: OrgContext,
    sandbox_provider_config: SandboxProviderConfig,
    creation_semaphore: Semaphore,
    dependency_setup_recovery: _DependencySetupRecoveryState,
    recovery_attempt: SandboxRecoveryAttempt,
    control: TaskControl,
    *,
    claimed_at: datetime,
    sandbox_provider: SandboxProvider | None = None,
    queue_context: SandboxQueueContext | None = None,
) -> dict[str, dict[str, Any] | None]:
    sentry_sdk.set_tag("benchmark_name", start_benchmark_request.benchmark_name)
    sentry_sdk.set_tag("agent_name", start_benchmark_request.contract.name)

    phase = FailurePhase.ADMISSION

    requested_attempt_started_at = task_row.started_at
    read_result = await control.read(TaskRef(org_id=task_row.org_id, task_row_id=task_row.id, task_id=task_row.task_id))
    task_row = read_result.task
    benchmark_row = read_result.benchmark
    if _normalized_attempt_time(task_row.started_at) != _normalized_attempt_time(requested_attempt_started_at):
        return {task_id: None}
    attempt_started_at = task_row.started_at
    if benchmark_row.status == BenchmarkStatus.STOPPING or task_row.status == TaskStatus.STOPPED:
        await commit_task_status_transition(task_row, control, TaskStatus.STOPPED)
        control.untrack(task_row.id)
        return {task_id: None}
    control.track(task_row)
    try:
        await keep_replaced_attempts(runtime.objects, str(benchmark_id), task_id, claimed_at)
    except Exception as exc:
        logger.exception(
            "Replaced task attempt artifacts were not kept",
            extra={"benchmark_id": str(benchmark_id), "task_id": task_id},
        )
        capture_exception(exc)
    expected_failure_status = (
        TaskStatus.EVALUATING
        if task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is not None
        else None
    )
    benchmark_name = benchmark_row.name
    benchmark_agent_name = benchmark_row.contract_name
    benchmark_started_by_email = benchmark_row.started_by_email

    task_stream_name = task_log_stream_name(task_id, task_row.started_at)
    log_output = task_logs.write

    logger.info(
        "Task output stream selected",
        extra={
            "event": LogEvent.TASK_ATTEMPT_STARTED,
            "benchmark_id": str(benchmark_id),
            "task_id": task_id,
            "cloudwatch_log_url": runtime.log_locations.task_location(
                str(benchmark_id),
                task_stream_name,
            ),
        },
    )

    agent_attempt_started = False

    def require_wall_seconds_remaining(deadline_at: datetime) -> float:
        if deadline_at.tzinfo is None:
            deadline_at = deadline_at.replace(tzinfo=UTC)
        remaining = (deadline_at - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            raise CreditedTaskWallTimeExceeded("Task wall-time limit reached")
        return remaining

    @asynccontextmanager
    async def enforce_wall_deadline(deadline_at: datetime | None) -> AsyncGenerator[asyncio.Timeout | None]:
        if deadline_at is None:
            yield None
            return
        remaining = require_wall_seconds_remaining(deadline_at)
        wall: asyncio.Timeout
        try:
            async with asyncio.timeout_at(asyncio.get_running_loop().time() + remaining) as wall:
                yield wall
        except TimeoutError as error:
            if wall.expired():
                raise CreditedTaskWallTimeExceeded("Task wall-time limit reached") from error
            raise

    async def ensure_wall_deadline() -> datetime:
        deadline = await control.deadline(task_row.ref)
        task_row.credited_wall_deadline_at = deadline
        return deadline

    preparation_stack = AsyncExitStack()

    evaluation_resume_state = task_row.eval_resume_state
    sandbox_id_for_recovery: str | None = None
    exit_reason: AgentCausedExitReason | None = None
    evaluation_start_time: float | None = None
    start_sandbox_run_time: float | None = None
    evaluation_phase = task_row.evaluation_phase

    async def on_eval_resume_state(state: dict[str, Any]) -> None:
        nonlocal evaluation_resume_state
        evaluation_resume_state = state
        assert evaluation_phase is not None
        await control.checkpoint(
            TaskResumeRequest(ref=control.ref, attempt=task_row.ref, phase=evaluation_phase, state=state)
        )

    sandbox_writes: set[asyncio.Future[None]] = set()
    sandbox_write_order = asyncio.Lock()

    async def save_announced_sandbox(link: SandboxLink, sandbox_id: str, seen_at: datetime) -> None:
        async with sandbox_write_order:
            await control.record_sandbox(link, sandbox_id, seen_at)

    async def drain_sandbox_writes() -> None:
        if not sandbox_writes:
            return
        drain = asyncio.ensure_future(asyncio.gather(*sandbox_writes, return_exceptions=True))
        cancelled = False
        while not drain.done():
            try:
                await asyncio.shield(drain)
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError

    def save_sighting(link: SandboxLink, sandbox_id: str, seen_at: datetime) -> None:
        write = asyncio.ensure_future(save_announced_sandbox(link, sandbox_id, seen_at))
        sandbox_writes.add(write)
        write.add_done_callback(sandbox_writes.discard)

    streamed_sandboxes: list[tuple[SandboxLink, str]] = []

    def on_sandbox_created(created: SandboxCreatedData) -> None:
        link = SandboxLink(benchmark_id=benchmark_id, provider=created.provider)
        streamed_sandboxes.append((link, created.sandbox_id))
        save_sighting(link, created.sandbox_id, sighting_time())

    async def until_stream_ends(stream: Awaitable[_Streamed]) -> _Streamed:
        try:
            return await stream
        finally:
            ended_at = sighting_time()
            for link, sandbox_id in streamed_sandboxes:
                save_sighting(link, sandbox_id, ended_at)
            streamed_sandboxes.clear()

    identity = {
        "benchmark_name": benchmark_name,
        "agent_name": benchmark_agent_name,
    }
    if benchmark_started_by_email:
        identity["email"] = benchmark_started_by_email
    attempt_id = task_attempt_id(task_row.id, attempt_started_at)
    attested_model = (
        start_benchmark_request.contract.model if start_benchmark_request.contract.inference_settings_attested else None
    )

    @asynccontextmanager
    async def grading_credentials(secrets: dict[str, str] | None) -> AsyncGenerator[str | None]:
        if not _saved_project_gateway_root(
            start_benchmark_request, org, benchmark_row.selected_project_gateway_secret_ref
        ):
            yield None
            return
        if secrets is None:
            references = start_benchmark_request.contract.secrets
            secrets = await resolve_secrets(
                {name: references[name] for name in ("MODEL_GATEWAY_URL", "MODEL_GATEWAY_API_KEY")},
                runtime.secrets,
            )
        try:
            models = await benchmark_service.grading_requirements(
                task_row.task_id, dataset=start_benchmark_request.dataset
            )
        except BenchmarkServiceError as exc:
            if exc.status_code != 404:
                raise
            yield None
            return
        if not models:
            yield None
            return
        async with grading_gateway_token(
            secrets,
            models=models,
            run_id=str(benchmark_id),
            task_id=task_row.task_id,
            attempt_id=attempt_id,
            identity={**identity, "agent_name": "grader"},
            org_name=org.name,
        ) as token:
            yield token

    child_egress: dict[str, ChildEgress] = {}

    @asynccontextmanager
    async def resumed_child_egress() -> AsyncGenerator[dict[str, ChildEgress]]:
        if child_egress:
            yield child_egress
            return
        if EGRESS_PROXY is None or sandbox_provider_config.type == "docker":
            yield {}
            return
        children = (await recovery_attempt.retrieve_task()).egress.children
        if not children:
            yield {}
            return
        assert task_row.credited_wall_deadline_at is not None
        secrets = await resolve_secrets(start_benchmark_request.contract.secrets, runtime.secrets)
        attempt = RunAttempt(
            benchmark_id,
            task_row.id,
            attempt_id,
            task_row.credited_wall_deadline_at,
            gateway_url=scoped_gateway_url(secrets, attested_model=attested_model, org_name=org.name),
        )
        async with _child_egress_grants(
            EgressProxy(EGRESS_PROXY, control),
            children,
            attempt,
            secrets,
            task_id=task_row.task_id,
            identity=identity,
            org_name=org.name,
        ) as grants:
            yield grants

    async def resume_evaluation_with_wall(state: dict[str, Any]) -> Any:
        async with enforce_wall_deadline(task_row.credited_wall_deadline_at), resumed_child_egress() as grants:
            async with grading_credentials(None) as grading_token:
                result = await until_stream_ends(
                    run_benchmark_service_websocket(
                        benchmark_service.resume_evaluation(
                            task_row.task_id,
                            eval_resume_state=state,
                            on_message=task_logs.write_line,
                            on_eval_resume_state=on_eval_resume_state,
                            on_sandbox_created=on_sandbox_created,
                            dataset=start_benchmark_request.dataset,
                            sandbox_provider=sandbox_provider_config,
                            child_egress=grants,
                            grading_token=grading_token,
                        )
                    )
                )
            if task_row.credited_wall_deadline_at is not None:
                require_wall_seconds_remaining(task_row.credited_wall_deadline_at)
            return result

    def execution_is_current(expected_status: TaskStatus | None = None) -> bool:
        return (
            not control.invalidated(task_row.id)
            and task_row.status != TaskStatus.STOPPED
            and (expected_status is None or task_row.status == expected_status)
        )

    def task_is_stopped() -> bool:
        return not execution_is_current()

    async def return_queued_task_to_pending() -> bool:
        if queue_context is None:
            return True
        transition = commit_task_status_transition(
            task_row, control, TaskStatus.PENDING, expected_status=expected_failure_status
        )
        if pending_build_abandon is None:
            return await transition

        async def persist_retry_disposition() -> bool:
            nonlocal pending_disposition_committed, authority_revoked
            try:
                accepted = await transition
            except ExecutionAuthorityRevoked:
                authority_revoked = True
                raise
            pending_disposition_committed = accepted
            return accepted

        return await finish_cleanup(asyncio.create_task(persist_retry_disposition()))

    async def commit_terminal_error(failure: Failure) -> dict[str, dict[str, Any] | None]:
        exc = failure.exc
        with error_span(
            "task.error",
            exc,
            benchmark_id=str(benchmark_id),
            task_id=task_id,
            producer=failure.producer,
            operation=failure.operation,
            error_type=failure.error_type,
            cause_code=failure.cause_code or "",
            failure_category=failure.category.value,
            failure_phase=phase.value,
            transient="" if failure.transient is None else failure.transient,
        ):
            logger.error(
                "Task execution failed",
                exc_info=(type(exc), exc, exc.__traceback__),
                extra={
                    "event": LogEvent.TASK_FAILED,
                    "benchmark_id": str(benchmark_id),
                    "task_id": task_id,
                    "producer": failure.producer,
                    "operation": failure.operation,
                    "error_type": failure.error_type,
                    "cause_code": failure.cause_code or "",
                    "failure_category": failure.category.value,
                    "failure_phase": phase.value,
                    "transient": failure.transient,
                },
            )
            if not is_agent_outcome(exc):
                with sentry_sdk.new_scope() as scope:
                    scope.set_tag("failure_category", failure.category.value)
                    scope.set_tag("failure_phase", phase.value)
                    capture_exception(exc)
        failure_request = TaskFailureRequest(
            ref=control.ref,
            attempt=task_row.ref,
            failure=TaskFailure(
                message=failure.message,
                producer=failure.producer,
                operation=failure.operation,
                error_type=failure.error_type,
                category=failure.category,
                cause_code=failure.cause_code,
                phase=phase,
                transient=failure.transient,
            ),
            expected_status=expected_failure_status,
            retry_scheduled=False,
            failed_attempt_number=None,
        )
        await task_logs.flush()

        async def persist_terminal_disposition() -> None:
            nonlocal pending_disposition_committed, authority_revoked
            try:
                accepted = await control.fail(failure_request)
            except ExecutionAuthorityRevoked:
                authority_revoked = True
                raise
            if accepted:
                record_task_outcome(
                    TaskOutcome.FAILED,
                    {
                        "failure_category": failure.category.value,
                        "failure_phase": phase.value,
                        "producer": failure.producer,
                    },
                )
            if pending_build_abandon is not None:
                pending_disposition_committed = accepted

        if pending_build_abandon is None:
            await persist_terminal_disposition()
        else:
            await finish_cleanup(asyncio.create_task(persist_terminal_disposition()))
        return {task_id: None}

    async def commit_wall_error(error: CreditedTaskWallTimeExceeded) -> dict[str, dict[str, Any] | None]:
        return await commit_terminal_error(
            Failure(
                error,
                str(error),
                FailureCategory.UNKNOWN,
                "tracker",
                "task_wall_deadline",
                phase,
                cause_code="credited_task_wall_time_exceeded",
            )
        )

    async def commit_resumed_evaluation(
        result: dict[str, Any],
        instance_id: str | None,
        reason: AgentCausedExitReason | None,
        evaluation_run_duration: float,
        sandbox_run_duration: float | None,
    ) -> dict[str, dict[str, Any] | None]:
        await control.flush_attempt(task_row.ref)
        if evaluation_phase is None:
            return {task_id: None}
        with observability_span(
            "task.status_transition",
            benchmark_id=str(benchmark_id),
            task_id=task_row.task_id,
            from_status=task_row.status.value,
            to_status=TaskStatus.FINISHED.value,
        ):
            accepted = await control.finish(
                TaskResultRequest(
                    ref=control.ref,
                    attempt=task_row.ref,
                    phase=evaluation_phase,
                    result=result,
                    instance_id=instance_id,
                    agent_caused_exit_reason=reason.value if reason else None,
                    evaluation_run_duration=evaluation_run_duration,
                    sandbox_run_duration=sandbox_run_duration,
                    resumed=True,
                )
            )
        if not accepted:
            return {task_id: None}
        control.untrack(task_row.id)
        return {task_id: result}

    async def recreate_sandbox(failure: Failure) -> dict[str, dict[str, Any] | None]:
        exc = failure.exc
        if not await return_queued_task_to_pending():
            return {task_id: None}
        log_output(f"\n[ERROR] {_exception_message(exc)}")
        retry = exc if isinstance(exc, SandboxSetupError) else SandboxSetupError(str(exc))
        retry.phase = phase
        retry.cause_code = failure.cause_code
        if retry is exc:
            raise retry
        raise retry from exc

    async def recover_evaluation_stream_failure(error_message: str) -> dict[str, dict[str, Any] | None] | None:
        if evaluation_resume_state is None:
            return None
        if task_is_stopped():
            return {task_id: None}

        recovery_message = f"{error_message}; resuming evaluation from durable benchmark state"
        logger.warning(recovery_message)
        log_output(f"\n[WARN] {recovery_message}\n")
        resume_eval_start_time = time.perf_counter()
        try:
            task_logs.last_log_time = time.monotonic()
            evaluation_result = await resume_evaluation_with_wall(evaluation_resume_state)
        except CreditedTaskWallTimeExceeded as wall_error:
            if task_is_stopped():
                return {task_id: None}
            return await commit_wall_error(wall_error)
        except Exception as resume_error:
            if task_is_stopped():
                return {task_id: None}
            failure = describe_failure(resume_error, _exception_message(resume_error), phase=phase)
            if isinstance(resume_error, BenchmarkServiceWebSocketDNSResolutionError):
                detail = f"WebSocket DNS resolution failed during resume: {_exception_message(resume_error)}"
            else:
                failure = replace(failure, producer="benchmark_service", operation="resume_evaluation")
                detail = f"resume failed: {failure.message}"
            failure = replace(failure, message=f"{recovery_message}; {detail}")
            log_output(f"\n[ERROR] {failure.message}")
            return await commit_terminal_error(failure)

        finished_at = time.perf_counter()
        evaluation_run_duration = finished_at - (evaluation_start_time or resume_eval_start_time)
        sandbox_run_duration = finished_at - start_sandbox_run_time if start_sandbox_run_time is not None else None
        evaluation_result_value = cast(dict[str, Any], evaluation_result)
        await task_logs.flush()
        return await commit_resumed_evaluation(
            evaluation_result_value, sandbox_id_for_recovery, exit_reason, evaluation_run_duration, sandbox_run_duration
        )

    pending_build_abandon: tuple[SandboxQueueContext, AttemptRef, PhaseRef] | None = None
    pending_disposition_committed = False
    authority_revoked = False

    if task_row.credited_wall_deadline_at is not None:
        try:
            require_wall_seconds_remaining(task_row.credited_wall_deadline_at)
        except CreditedTaskWallTimeExceeded as error:
            try:
                return await commit_wall_error(error)
            finally:
                control.untrack(task_row.id)

    try:
        evaluation_resume_state = task_row.eval_resume_state
        if task_row.status == TaskStatus.EVALUATING and evaluation_resume_state is not None:
            phase = FailurePhase.EVALUATION
            claim = await control.evaluation(task_row.ref)
            if claim.phase is None or claim.resume_state is None:
                return {task_id: None}
            evaluation_phase = claim.phase
            task_row.evaluation_phase = claim.phase
            control.track(task_row)
            evaluation_resume_state = claim.resume_state

            try:
                log_output("Resuming evaluation from durable benchmark state\n")
                resume_eval_start_time = time.perf_counter()
                task_logs.last_log_time = time.monotonic()
                evaluation_result = await resume_evaluation_with_wall(evaluation_resume_state)
                resume_eval_duration = time.perf_counter() - resume_eval_start_time
                await task_logs.flush()
                return await commit_resumed_evaluation(
                    cast(dict[str, Any], evaluation_result), None, None, resume_eval_duration, None
                )

            except SandboxNotFoundError:
                if task_is_stopped():
                    return {task_id: None}
                try:
                    await recovery_attempt.retrieve_task()
                except Exception:
                    logger.warning(
                        "Failed to load sandbox recovery policy after grading sandbox loss",
                        exc_info=True,
                    )
                raise
            except Exception as e:
                if task_is_stopped():
                    return {task_id: None}
                raise e from e

        if queue_context is None:
            while not (admitted := await _admit_direct_task(task_row, control)):
                if admitted is None:
                    return {task_id: None}
                await asyncio.sleep(_DIRECT_ADMISSION_POLL_SECONDS)

        await _acquire_queue_preparation(preparation_stack, queue_context)
        async with enforce_wall_deadline(task_row.credited_wall_deadline_at):
            task_data = await recovery_attempt.retrieve_task()
        grading_spec = None if task_data.eval_sandbox is None else resolve_grading_spec(task_data)
        unrestricted_docker = sandbox_provider_config.type == "docker"
        run_egress_policy = combine_run_egress_policies(
            task_data.egress.run, start_benchmark_request.contract.egress_allowlist
        )
        folder = task_data if isinstance(task_data, TaskFolderResponse) else None
        steps = folder.steps if folder is not None else []
        if unrestricted_docker and any(
            policy != "*"
            for policy in (
                start_benchmark_request.contract.install_egress_policy,
                task_data.egress.setup_task,
                run_egress_policy,
                task_data.egress.evaluation,
                *(step.egress_run for step in steps),
                *(step.egress_evaluation for step in steps),
            )
        ):
            raise InvalidSandboxConfigurationError(
                "Local Docker execution requires unrestricted egress for every stage; "
                "egress allowlists and blocked-network policies are not supported"
            )
        stage_egress: RunStageEgress = SandboxAllowlist()
        egress_proxy: EgressProxy | None = None
        if task_data.egress.run_through_proxy or task_data.egress.children:
            if EGRESS_PROXY is None or unrestricted_docker:
                raise InvalidSandboxConfigurationError(
                    "The task asks for the egress proxy, which this deployment doesn't have"
                )
            egress_proxy = EgressProxy(EGRESS_PROXY, control)
            if task_data.egress.run_through_proxy:
                validate_proxy_entries(run_egress_policy, task_data.egress.run_inspect)
                stage_egress = egress_proxy
            for profile in task_data.egress.children.values():
                validate_proxy_entries(profile.egress, profile.inspect)
        if sandbox_provider is None:
            sandbox_provider = benchmark_service.get_sandbox_provider(sandbox_provider_config)

        plan = GenerationPlan.from_task(task_data)

        attempt_id = task_attempt_id(task_row.id, attempt_started_at)
        labels = {
            "Benchmark": start_benchmark_request.benchmark_name,
            "Id": str(benchmark_id),
            "run-id": str(benchmark_id),
            "Task": task_row.task_id,
        }

        if queue_context is None and not execution_is_current(TaskStatus.BUILDING):
            return {task_id: None}

        async with enforce_wall_deadline(task_row.credited_wall_deadline_at):
            resolved_secrets = await resolve_secrets(start_benchmark_request.contract.secrets, runtime.secrets)

        deadline_controller: ExternalServiceDeadlineController

        task_env = folder.env if folder is not None else {}
        task_secret_values = [task_env[name] for name in folder.secret_env] if folder is not None else []
        env_vars = {
            **task_env,
            **resolved_secrets,
            "RUN_ID": str(benchmark_id),
            "TASK_ID": task_row.task_id,
            "ATTEMPT_ID": attempt_id,
            **_attested_inference_settings(start_benchmark_request.contract),
            "IDENTITY": json.dumps(identity),
            "DAYTONA_SANDBOX_OTEL_EXTRA_LABELS": (
                f"benchmark_id={benchmark_id},task_id={task_row.task_id},environment={ENVIRONMENT}"
            ),
            **recovery_attempt.environment,
        }
        redact_error: Callable[[str], str] | None = None
        secret_env_names = (
            (*start_benchmark_request.contract.secrets, *task_data.sandbox_secrets) if task_data.sandbox_secrets else ()
        )

        async def persist_generation_accounting_summary(summary: GenerationAccountingSummary) -> None:
            await _persist_generation_accounting_summary(summary, control=control, attempt=task_row.ref)

        async def save_search_costs(charges: list[SearchCharge], step_name: str | None = None) -> None:
            await control.search_costs(
                TaskSearchCostsRequest(ref=control.ref, attempt=task_row.ref, charges=charges, step_name=step_name)
            )

        start_sandbox_build_time = time.perf_counter()
        object_store = runtime.objects
        creation_state = SandboxCreationState(cleanup_pending=False)

        @asynccontextmanager
        async def sandbox_context() -> AsyncGenerator[Sandbox]:
            nonlocal deadline_controller, start_sandbox_build_time, redact_error, phase
            start_sandbox_build_time = time.perf_counter()
            sandbox_name = (
                task_row.task_id if queue_context is None else queued_sandbox_name(task_row.id, attempt_started_at)
            )
            contract = start_benchmark_request.contract
            wall_deadline_at = await ensure_wall_deadline()
            async with AsyncExitStack() as key_stack, enforce_wall_deadline(wall_deadline_at) as wall_timeout:
                token_rotation = GatewayTokenRotation()
                scoped_env_vars = await key_stack.enter_async_context(
                    task_scoped_gateway_key(
                        env_vars,
                        run_id=str(benchmark_id),
                        task_id=task_row.task_id,
                        attempt_id=attempt_id,
                        attested_model=attested_model,
                        companion_models=contract.kwargs.get("companion_models"),
                        identity=identity,
                        org_name=org.name,
                        egress=run_egress_policy,
                        allow_provider_tools=contract.allow_provider_tools,
                        rotation=token_rotation,
                    )
                )
                deadline_controller = await _create_generation_deadline_controller(
                    contract, plan, resolved_secrets, scoped_env_vars
                )
                gateway_url = scoped_gateway_url(env_vars, attested_model=attested_model, org_name=org.name)
                sandbox_environment = stage_egress.sandbox_environment(
                    SandboxEnvironment(scoped_env_vars, task_data.sandbox_secrets or {}),
                    ScopedGateway(gateway_url, scoped_env_vars["MODEL_GATEWAY_API_KEY"]) if gateway_url else None,
                )
                redact_error = create_agent_error_redactor(
                    [*(scoped_env_vars[name] for name in contract.secrets), *task_secret_values]
                )
                phase = FailurePhase.SANDBOX

                async with create_sandbox(
                    provider=sandbox_provider,
                    sandbox_name=sandbox_name,
                    source=task_data.source,
                    labels=labels,
                    env_vars=sandbox_environment.env_vars,
                    sandbox_secrets=sandbox_environment.secrets,
                    resources=task_data.resources,
                    volumes=task_data.volumes,
                    creation_semaphore=creation_semaphore,
                    creation_state=creation_state,
                    unique_name=queue_context is None,
                    sandbox_link=SandboxLink(benchmark_id=benchmark_id, provider=sandbox_provider_config.type),
                    record_sandbox=control.record_sandbox,
                    record_sandbox_cost=control.record_sandbox_cost,
                ) as sandbox:
                    try:
                        yield sandbox
                    finally:
                        if wall_timeout is not None and not wall_timeout.expired():
                            wall_timeout.reschedule(None)

        async with AsyncExitStack() as sandbox_stack:
            if queue_context is None:
                sandbox = await sandbox_stack.enter_async_context(sandbox_context())
            else:

                def phase_claimed(build_phase: PhaseRef) -> None:
                    task_row.status = TaskStatus.BUILDING
                    task_row.build_phase = build_phase
                    control.track(task_row)

                def phase_released(started: bool) -> None:
                    task_row.build_phase = None
                    if started:
                        task_row.status = TaskStatus.IN_PROGRESS
                    control.track(task_row)

                def phase_abandon(build_phase: PhaseRef) -> None:
                    nonlocal pending_build_abandon
                    pending_build_abandon = (queue_context, task_row.ref, build_phase)

                sandbox = await enter_queued_sandbox(
                    stack=sandbox_stack,
                    context=queue_context,
                    attempt=task_row.ref,
                    source=task_data.source,
                    resources=task_data.resources,
                    grading=None if grading_spec is None else (grading_spec.source, grading_spec.resources),
                    creation_state=creation_state,
                    create=sandbox_context,
                    preparation_stack=preparation_stack,
                    on_phase_claimed=phase_claimed,
                    on_phase_released=phase_released,
                    on_phase_abandon=phase_abandon,
                )
                if sandbox is None:
                    return {task_id: None}
                task_row.status = TaskStatus.IN_PROGRESS
                control.track(task_row)
            sandbox_id_for_recovery = sandbox.id
            phase = FailurePhase.SETUP
            if isinstance(benchmark_service, DatasetTaskService) and folder is not None and folder.cwd_from_image:
                task_data = folder = await benchmark_service.use_image_workdir(
                    task_row.task_id, sandbox.id, sandbox_provider_config
                )
            if start_benchmark_request.contract.final_output is not None:
                descriptor = ActiveNativeOutput(
                    attempt_started_at=attempt_started_at,
                    sandbox_id=sandbox.id,
                    runtime_source=task_data.source,
                    output_path=str(PurePosixPath(task_data.cwd) / start_benchmark_request.contract.final_output),
                )
                output_request = NativeOutputRequest(ref=control.ref, attempt=task_row.ref, descriptor=descriptor)
                try:
                    if not await control.publish_native_output(output_request):
                        raise ExecutionAuthorityRevoked("Task attempt no longer owns native output")
                except ExecutionAuthorityRevoked:
                    raise
                except Exception:
                    logger.exception("Failed to publish native output", extra={"task_id": str(task_row.id)})

                async def revoke_native_output() -> None:
                    try:
                        await control.clear_native_output(output_request)
                    except Exception:
                        logger.exception("Failed to clear native output", extra={"task_id": str(task_row.id)})

                sandbox_stack.push_async_callback(revoke_native_output)
            sandbox_build_duration = time.perf_counter() - start_sandbox_build_time
            start_sandbox_run_time = time.perf_counter()

            try:
                if queue_context is None:
                    if not await commit_task_status_transition(task_row, control, TaskStatus.IN_PROGRESS):
                        return {task_id: None}

                oracle = start_benchmark_request.contract.name == ORACLE_AGENT
                if not oracle:
                    await upload_agent_artifacts(
                        sandbox,
                        start_benchmark_request.contract,
                        str(benchmark_id),
                        object_store,
                    )

                agent_sandbox = runtime_sandbox(sandbox, task_data.source)

                async def install_agent() -> None:
                    if oracle:
                        return
                    if not unrestricted_docker:
                        await apply_egress_policy(
                            agent_sandbox,
                            start_benchmark_request.contract.install_egress_policy,
                        )
                    try:
                        await install_agent_dependencies(
                            agent_sandbox,
                            start_benchmark_request.contract,
                            log_output,
                            dependency_setup_recovery.mode,
                            log_line=task_logs.write_line,
                            redact_error=redact_error,
                            secret_env_names=secret_env_names,
                        )
                    except DependencySetupExhaustedError:
                        dependency_setup_recovery.mode = DependencySetupMode.FINAL_FRESH_SANDBOX
                        raise

                install_after_setup = (
                    isinstance(task_data.source, ComposeSource) or task_data.agent_install_order == "after_setup"
                )
                if not install_after_setup:
                    await install_agent()

                if not unrestricted_docker:
                    await apply_egress_policy(sandbox, task_data.egress.setup_task)

                def run_attempt() -> RunAttempt:
                    assert task_row.credited_wall_deadline_at is not None
                    return RunAttempt(
                        benchmark_id,
                        task_row.id,
                        attempt_id,
                        task_row.credited_wall_deadline_at,
                        gateway_url=scoped_gateway_url(env_vars, attested_model=attested_model, org_name=org.name),
                    )

                if egress_proxy is not None:
                    child_egress.update(
                        await sandbox_stack.enter_async_context(
                            _child_egress_grants(
                                egress_proxy,
                                task_data.egress.children,
                                run_attempt(),
                                env_vars,
                                task_id=task_row.task_id,
                                identity=identity,
                                org_name=org.name,
                            )
                        )
                    )
                    sandbox_stack.callback(child_egress.clear)

                task_logs.last_log_time = time.monotonic()
                _ = await run_benchmark_service_websocket(
                    benchmark_service.setup_task(
                        task_row.task_id,
                        sandbox.id,
                        on_message=task_logs.write_line,
                        dataset=start_benchmark_request.dataset,
                        sandbox_provider=sandbox_provider_config,
                        child_egress=child_egress,
                    )
                )
                recovery_attempt.mark_replacement_ready()

                if install_after_setup:
                    await install_agent()

                task_logs.buffer_logs(force_flush=True)

                agent_output_s3_key = None
                if plan.episode is not None or start_benchmark_request.contract.final_output:
                    agent_output_s3_key = task_artifact_key(str(benchmark_id), task_id, "agent_output.tar.gz")

                async def run_generation(
                    run_plan: GenerationPlan,
                    run_controller: ExternalServiceDeadlineController,
                    run_egress: EgressPolicy,
                    run_user: str | None,
                    output_key: str | None,
                    *,
                    output_folder: str = "",
                    step_name: str | None = None,
                    on_finalized: Callable[
                        [GenerationAccountingSummary], Awaitable[None]
                    ] = persist_generation_accounting_summary,
                ) -> tuple[AgentCausedExitReason | None, float]:
                    nonlocal agent_attempt_started, phase, redact_error
                    run_access = RunStageAccess()
                    if not unrestricted_docker:
                        run_access = await sandbox_stack.enter_async_context(
                            stage_egress.run_stage(
                                agent_sandbox,
                                run_egress,
                                run_attempt(),
                                local_hosts=task_data.egress.sandbox_local_hosts,
                                inspect=task_data.egress.run_inspect,
                            )
                        )
                    solution_env: dict[str, str] = {}
                    solution_secrets: list[str] = []
                    if oracle:
                        if not isinstance(benchmark_service, DatasetTaskService):
                            raise InvalidSandboxConfigurationError("The oracle agent runs only on a dataset")
                        solution_env, solution_secrets = await benchmark_service.upload_solution(
                            task_row.task_id, sandbox.id, step_name, sandbox_provider_config
                        )
                    if run_access.secrets or solution_secrets:
                        egress_redactor = create_agent_error_redactor([*run_access.secrets, *solution_secrets])
                        previous_redactor = redact_error

                        def redact_with_egress_secrets(content: str) -> str:
                            content = egress_redactor(content)
                            return previous_redactor(content) if previous_redactor is not None else content

                        redact_error = redact_with_egress_secrets
                    agent_attempt_started = True
                    phase = FailurePhase.AGENT
                    generation_gateway_url = (
                        EXTERNAL_SERVICE_GATEWAY_URL if run_controller.snapshot is not None else None
                    )
                    try:
                        exit_reason, agent_run_time = await run_agent(
                            agent_sandbox,
                            start_benchmark_request.contract,
                            task_data.problem_path,
                            task_id,
                            log_output,
                            task_data.cwd,
                            object_store=object_store,
                            agent_output_s3_key=output_key,
                            plan=run_plan,
                            benchmark_id=str(benchmark_id),
                            execution_is_current=execution_is_current,
                            deadline_controller=run_controller,
                            generation_gateway_url=generation_gateway_url,
                            extra_env={**run_access.env_vars, **solution_env},
                            log_line=task_logs.write_line,
                            on_external_service_finalized=on_finalized,
                            redact_error=redact_error,
                            secret_env_names=secret_env_names,
                            user=run_user,
                            artifact_folder=output_folder,
                            mcp_servers=folder.mcp_servers if folder is not None else None,
                            skills_dir=folder.skills_dir if folder is not None else None,
                            on_search_costs=lambda charges: save_search_costs(charges, step_name),
                        )
                    except AgentRunFailedError as agent_error:
                        gateway_url = generation_gateway_url or resolved_secrets.get("MODEL_GATEWAY_URL")
                        gateway_host = urlsplit(gateway_url).hostname if gateway_url else None
                        if gateway_host and not await sandbox_resolves(agent_sandbox, gateway_host):
                            raise SandboxDNSResolutionError(
                                f"Sandbox could not resolve the model gateway {gateway_host} after the agent failed"
                            ) from agent_error
                        raise
                    logger.info(
                        "agent.run.complete",
                        extra={
                            "event": LogEvent.AGENT_RUN_COMPLETE,
                            "benchmark_id": str(benchmark_id),
                            "task_id": task_row.task_id,
                            "sandbox_id": sandbox.id,
                            "sandbox_name": sandbox.name,
                            "exit_reason": exit_reason.value if exit_reason else None,
                        },
                    )
                    return exit_reason, agent_run_time

                async def move_to_evaluating(agent_run_time: float) -> PhaseRef | None:
                    nonlocal phase
                    phase = FailurePhase.EVALUATION
                    with observability_span(
                        "task.status_transition",
                        benchmark_id=str(benchmark_id),
                        task_id=task_row.task_id,
                        from_status=task_row.status.value,
                        to_status=TaskStatus.EVALUATING.value,
                    ):
                        accepted = await control.breakdown(
                            TaskBreakdownRequest(
                                ref=control.ref,
                                attempt=task_row.ref,
                                sandbox_build_duration=sandbox_build_duration,
                                agent_run_duration=agent_run_time,
                            )
                        )
                    if not accepted:
                        return None
                    task_row.status = TaskStatus.EVALUATING
                    task_row.build_phase = None
                    control.track(task_row)
                    claim = await control.evaluation(task_row.ref)
                    if claim.phase is None:
                        return None
                    task_row.evaluation_phase = claim.phase
                    control.track(task_row)
                    return claim.phase

                evaluation_result: Any
                if isinstance(benchmark_service, DatasetTaskService) and folder is not None and folder.steps:

                    def step_output_key(step: StepPlan) -> str | None:
                        if agent_output_s3_key is None:
                            return None
                        return task_artifact_key(str(benchmark_id), task_id, step_output_name(step.name))

                    async def generate_step(
                        step: StepPlan,
                        step_controller: ExternalServiceDeadlineController,
                        on_finalized: Callable[[GenerationAccountingSummary], Awaitable[None]],
                    ) -> tuple[AgentCausedExitReason | None, float]:
                        return await run_generation(
                            replace(plan, allowance_seconds=step.agent_timeout),
                            step_controller,
                            combine_run_egress_policies(
                                step.egress_run, start_benchmark_request.contract.egress_allowlist
                            ),
                            step.agent_user,
                            step_output_key(step),
                            output_folder=step_folder(step.name),
                            step_name=step.name,
                            on_finalized=on_finalized,
                        )

                    def set_phase(next_phase: FailurePhase) -> None:
                        nonlocal phase
                        phase = next_phase

                    async def restrict_egress(policy: EgressPolicy) -> None:
                        await apply_egress_policy(sandbox, policy)

                    stepped = await run_steps(
                        StepRun(
                            service=benchmark_service,
                            task=folder,
                            task_id=task_row.task_id,
                            benchmark_id=benchmark_id,
                            sandbox_id=sandbox.id,
                            sandbox_name=sandbox.name,
                            sandbox_provider=sandbox_provider_config,
                            task_logs=task_logs,
                            generate=generate_step,
                            persist_accounting=persist_generation_accounting_summary,
                            on_sandbox_created=on_sandbox_created,
                            until_stream_ends=until_stream_ends,
                            set_phase=set_phase,
                            redact=lambda text: redact_error(text) if redact_error is not None else text,
                            restrict_egress=None if unrestricted_docker else restrict_egress,
                        ),
                        deadline_controller,  # noqa: F821
                    )
                    evaluation_phase = await move_to_evaluating(stepped.agent_time)
                    if evaluation_phase is None:
                        return {task_id: None}
                    if stepped.agent_failure is not None and folder.reward_strategy == MultiStepRewardStrategy.FINAL:
                        phase = FailurePhase.AGENT
                        raise stepped.agent_failure
                    exit_reason = stepped.abnormal_exit
                    evaluation_result = combine_steps(task_id, folder.reward_strategy, stepped.outcomes)
                    evaluation_run_duration = stepped.evaluation_time
                else:
                    exit_reason, agent_run_time = await run_generation(
                        plan,
                        deadline_controller,  # noqa: F821
                        run_egress_policy,
                        folder.agent_user if folder is not None else None,
                        agent_output_s3_key,
                    )
                    evaluation_phase = await move_to_evaluating(agent_run_time)
                    if evaluation_phase is None:
                        return {task_id: None}

                    evaluation_start_time = time.perf_counter()

                    logger.info(
                        "task.evaluation.start",
                        extra={
                            "event": LogEvent.TASK_EVALUATION_START,
                            "benchmark_id": str(benchmark_id),
                            "task_id": task_row.task_id,
                            "sandbox_id": sandbox.id,
                            "sandbox_name": sandbox.name,
                        },
                    )
                    logger.info(
                        "Evaluating agent %s in sandbox %s", start_benchmark_request.contract.name, sandbox.name
                    )
                    if not unrestricted_docker:
                        await apply_egress_policy(sandbox, task_data.egress.evaluation)
                    task_logs.last_log_time = time.monotonic()
                    grading_releases: list[asyncio.Task[None]] = []

                    def on_evaluation_sandbox_created(created: SandboxCreatedData) -> None:
                        on_sandbox_created(created)
                        if grading_spec is not None and queue_context is not None and not grading_releases:
                            grading_releases.append(
                                asyncio.create_task(
                                    queue_context.control.release_grading(queue_context.pool_id, task_row.ref)
                                )
                            )

                    try:
                        async with grading_credentials(resolved_secrets) as grading_token:
                            evaluation_result = await until_stream_ends(
                                run_benchmark_service_websocket(
                                    benchmark_service.evaluate_instance(
                                        task_row.task_id,
                                        sandbox.id,
                                        on_message=task_logs.write_line,
                                        on_eval_resume_state=on_eval_resume_state,
                                        on_sandbox_created=on_evaluation_sandbox_created,
                                        dataset=start_benchmark_request.dataset,
                                        sandbox_provider=sandbox_provider_config,
                                        child_egress=child_egress,
                                        grading_token=grading_token,
                                    )
                                )
                            )
                    finally:
                        for failure in await asyncio.gather(*grading_releases, return_exceptions=True):
                            if failure is not None:
                                logger.warning(
                                    "task.grading_release_failed",
                                    extra={"benchmark_id": str(benchmark_id), "task_id": task_row.task_id},
                                    exc_info=failure,
                                )
                    evaluation_run_duration = time.perf_counter() - evaluation_start_time

                assert start_sandbox_run_time is not None
                sandbox_run_duration = time.perf_counter() - start_sandbox_run_time

                await task_logs.flush()

                await control.flush_attempt(task_row.ref)
                with observability_span(
                    "task.status_transition",
                    benchmark_id=str(benchmark_id),
                    task_id=task_row.task_id,
                    from_status=task_row.status.value,
                    to_status=TaskStatus.FINISHED.value,
                ):
                    accepted = await control.finish(
                        TaskResultRequest(
                            ref=control.ref,
                            attempt=task_row.ref,
                            phase=evaluation_phase,
                            result=cast(dict[str, Any], evaluation_result),
                            instance_id=sandbox.id,
                            agent_caused_exit_reason=exit_reason.value if exit_reason else None,
                            evaluation_run_duration=evaluation_run_duration,
                            sandbox_run_duration=sandbox_run_duration,
                            resumed=False,
                        )
                    )
                if not accepted:
                    return {task_id: None}
                control.untrack(task_row.id)
                return {task_id: cast(dict[str, Any], evaluation_result)}
            except Exception:
                if task_is_stopped():
                    return {task_id: None}
                raise

    except GenerationTerminationUnconfirmedError as e:
        if task_is_stopped():
            return {task_id: None}
        error_message = _exception_message(e)
        log_output(f"\n[ERROR] {error_message}")

        failure = replace(
            describe_failure(e, error_message, phase=phase),
            producer="tracker",
            operation="generation_termination",
            category=FailureCategory.INFRASTRUCTURE,
            cause_code="deadline_expired_termination_unconfirmed",
        )
        return await commit_terminal_error(failure)
    except SandboxSetupError as e:
        if task_is_stopped():
            return {task_id: None}
        if agent_attempt_started:
            error_message = _exception_message(e)
            log_output(f"\n[ERROR] {error_message}")
            failure = replace(
                describe_failure(e, error_message, phase=phase),
                producer="sandbox_provider",
                operation="sandbox_recovery",
                category=FailureCategory.INFRASTRUCTURE,
            )
            return await commit_terminal_error(failure)
        if not await return_queued_task_to_pending():
            return {task_id: None}
        e.phase = phase
        log_output(f"\n[ERROR] {_exception_message(e)}")
        raise
    except SandboxNotFoundError as e:
        if task_is_stopped():
            return {task_id: None}
        if recovery_attempt.sandbox_loss_retry_available and not agent_attempt_started:
            if pending_build_abandon is not None and not await return_queued_task_to_pending():
                return {task_id: None}
            message = (
                "Sandbox disappeared; restoring the task from its durable volume "
                f"(attempt {recovery_attempt.number + 1}/{recovery_attempt.max_attempts})"
            )
            logger.warning(message)
            log_output(f"\n[WARNING] {message}\n")
            raise

        failure = replace(
            describe_failure(e, _exception_message(e), phase=phase),
            producer="sandbox_provider",
            operation="sandbox_recovery",
            category=FailureCategory.INFRASTRUCTURE,
        )
        log_output(f"\n[ERROR] {failure.message}")
        return await commit_terminal_error(failure)
    except CreditedTaskWallTimeExceeded as wall_error:
        if task_is_stopped():
            return {task_id: None}
        log_output(f"\n[ERROR] {wall_error}")
        return await commit_wall_error(wall_error)
    except ExecutionAuthorityRevoked:
        authority_revoked = True
        return {task_id: None}
    except Exception as e:
        if task_is_stopped():
            return {task_id: None}
        failure = describe_failure(
            e, _exception_message(e), phase=phase, idle_seconds=int(time.monotonic() - task_logs.last_log_time)
        )
        disposition = decide(failure)
        if disposition is Disposition.RECREATE_SANDBOX:
            return await recreate_sandbox(failure)
        if disposition is Disposition.RESUME_EVALUATION:
            recovered = await recover_evaluation_stream_failure(failure.message)
            if recovered is not None:
                return recovered
        if failure.category is FailureCategory.UNKNOWN:
            logfire.exception("process_task failed")
        log_output(f"\n[ERROR] {failure.message}")
        return await commit_terminal_error(failure)
    finally:
        try:
            if pending_build_abandon is not None and (
                pending_disposition_committed or task_is_stopped() or authority_revoked
            ):
                context, attempt, build_phase = pending_build_abandon
                await finish_cleanup(
                    asyncio.create_task(context.control.abandon(context.pool_id, attempt, build_phase))
                )
                task_row.build_phase = None
                control.track(task_row)
            await drain_sandbox_writes()
        finally:
            await preparation_stack.aclose()
            control.untrack(task_row.id)
