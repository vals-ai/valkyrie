import asyncio
import json
import socket
import time
from hashlib import sha256
from asyncio import Semaphore, to_thread
from collections.abc import AsyncGenerator, Callable, Coroutine, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from typing import Any, cast
from urllib.parse import urlsplit
from uuid import UUID

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
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from sqlalchemy.engine import Connection
from sqlmodel import Session

from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.runtime.logs import task_log_stream_name
from tracker.runtime.secrets import resolve_secrets
from tracker.runtime.services import RuntimeServices
from tracker.runtime.artifacts import task_artifact_key
from tracker.runtime.model_gateway import (
    GatewayTokenRotation,
    GATEWAY_TOKEN_TTL_SECONDS,
    child_gateway_token,
    grading_gateway_token,
    scoped_gateway_url,
    task_scoped_gateway_key,
)
from tracker.runtime.run_egress import (
    RunAttempt,
    RunStageAccess,
    RunStageEgress,
    SandboxAllowlist,
    SandboxEnvironment,
    ScopedGateway,
)
from tracker.runtime.task_logs import TaskLogBuffer
from tracker.config import (
    BenchmarkServiceDestination,
    ENVIRONMENT,
    EXTERNAL_SERVICE_GATEWAY_CREDIT_CAP_SECONDS,
    EXTERNAL_SERVICE_GATEWAY_CONTROL_TOKEN,
    EXTERNAL_SERVICE_GATEWAY_URL,
    GATEWAY_KEY_ORGS,
    classify_benchmark_service_destination,
)
from tracker.database.models import (
    ActiveNativeOutput,
    AgentCausedExitReason,
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    FailureCategory,
    FailurePhase,
    Org,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.database.session import database_call, engine
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
from tracker.egress_proxy.policy import validate_proxy_entries
from tracker.egress_proxy.run_stage import EgressProxy
from tracker.egress_proxy.settings import EGRESS_PROXY
from tracker.external_service_gateway import (
    AccountingSnapshot,
    GenerationAccountingSummary,
    ExternalServiceDeadlineController,
    ExternalServiceGatewayClient,
)
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.executor.native_output import clear_native_output, publish_native_output
from tracker.generation_plan import GenerationPlan
from tracker.logging import get_logger
from tracker.logging.events import LogEvent
from tracker.project_gateway import is_generated_project_gateway_secret_name
from tracker.observability import TaskOutcome, error_span, incr, record_task_outcome
from tracker.observability.sentry import capture_exception, clear_sandbox_context
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
from tracker.secret_access import tenant_secret_prefix
from tracker.sandbox_cost import SandboxLink, record_sandbox
from tracker.scheduler.admission import SandboxQueueContext, enter_queued_sandbox
from tracker.scheduler.store import is_reserved_queue_pool_id, queued_sandbox_name
from tracker.services.tasks.attempts import keep_replaced_attempts
from tracker.repositories.advisory_lock import PostgresAdvisoryLock, task_evaluation_lock
from tracker.types import (
    StartBenchmarkRequest,
)

from tracker.services.failures import (
    is_agent_outcome,
    BenchmarkServiceWebSocketDNSResolutionError,
    Disposition,
    Failure,
    classify_failure,
    decide,
    describe_failure,
)
from tracker.services.benchmarks import fetch_task_row
from tracker.services.tasks.status import (
    commit_task_error,
    commit_task_status_transition,
    exception_message,
    handle_early_exit,
    normalized_attempt_time,
)

logger = get_logger(__name__)
TASK_WALL_SECONDS = GATEWAY_TOKEN_TTL_SECONDS - 2 * 60 * 60

_PTY_TASK_RETRY_LIMIT: int = 1
_SANDBOX_RETRY_DELAY_SECONDS: float = 2
_DIRECT_ADMISSION_POLL_SECONDS: float = 2


def _saved_project_gateway_root(
    request: StartBenchmarkRequest, org: Org, selected_project_gateway_secret_ref: str | None
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


async def _run_benchmark_service_websocket(operation: Coroutine[Any, Any, Any]) -> Any:
    try:
        return await operation
    except socket.gaierror as exc:
        raise BenchmarkServiceWebSocketDNSResolutionError(exception_message(exc)) from exc


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


def _record_failure_before_retry(
    task_row: Task,
    authority: ExecutionAuthority,
    exc: SandboxSetupError,
    failed_attempt_number: int,
) -> None:
    with Session(bind=engine) as session:
        try:
            lock_execution_authority(session, authority)
        except ExecutionAuthorityRevoked:
            session.rollback()
            return
        repository = BenchmarkRepository(session)
        task = repository.lock_task(
            task_row.org_id,
            authority.benchmark_id,
            task_row.id,
            started_at=task_row.started_at,
            statuses=(TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING),
        )
        if task is None:
            session.rollback()
            return
        repository.save(
            ErrorResult(
                org_id=task.org_id,
                task=task.id,
                error_message=exception_message(exc),
                producer="sandbox_provider",
                operation="setup",
                error_type=type(exc).__name__,
                category=FailureCategory.INFRASTRUCTURE,
                cause_code=exc.cause_code,
                phase=exc.phase,
                transient=exc.transient,
                retry_scheduled=True,
                failed_attempt_number=failed_attempt_number,
            )
        )
        session.commit()


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


def save_eval_resume_state(
    task_row_id: UUID,
    org: Org,
    eval_resume_state: dict[str, Any],
    *,
    expected_started_at: datetime,
    authority: ExecutionAuthority,
    connection: Connection | None = None,
) -> bool:
    with Session(bind=connection if connection is not None else engine) as session:
        try:
            lock_execution_authority(session, authority)
        except ExecutionAuthorityRevoked:
            session.rollback()
            return False
        updated = BenchmarkRepository(session).update_task(
            task_row_id,
            org.id,
            {"eval_resume_state": eval_resume_state},
            expected_started_at=expected_started_at,
            expected_status=TaskStatus.EVALUATING,
        )
        session.commit()
        return updated


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


def _persist_generation_accounting_summary(
    summary: GenerationAccountingSummary,
    *,
    task_breakdown: TaskBreakdown,
    task_row_id: UUID,
    org: Org,
    authority: ExecutionAuthority,
    expected_started_at: datetime,
    open_task_session: Callable[[], Session],
) -> None:
    with open_task_session() as task_session:
        try:
            lock_execution_authority(task_session, authority)
        except ExecutionAuthorityRevoked:
            task_session.rollback()
            raise
        repository = BenchmarkRepository(task_session)
        task_in_session = repository.lock_task(org.id, authority.benchmark_id, task_row_id)
        if (
            task_in_session is None
            or task_in_session.status == TaskStatus.STOPPED
            or normalized_attempt_time(task_in_session.started_at) != normalized_attempt_time(expected_started_at)
        ):
            task_session.rollback()
            raise ExecutionAuthorityRevoked("Task attempt changed before accounting persistence")
        task_breakdown.accounting_id = summary.accounting_id
        task_breakdown.base_generation_allowance_seconds = summary.base_generation_allowance_seconds
        task_breakdown.cumulative_time_credit_cap_seconds = summary.cumulative_time_credit_cap_seconds
        task_breakdown.external_service_overhead_seconds = summary.external_service_overhead_seconds
        task_breakdown.external_service_credit_applied_seconds = summary.external_service_credit_applied_seconds
        task_breakdown.external_service_credit_revision = summary.external_service_credit_revision
        task_breakdown.generation_elapsed_seconds = summary.generation_elapsed_seconds
        repository.attach_task_breakdown(task_in_session, task_breakdown)
        task_session.commit()


async def process_task(
    task_row: Task,
    start_benchmark_request: StartBenchmarkRequest,
    benchmark_service: BenchmarkServiceClient,
    benchmark_id: UUID,
    task_id: str,
    runtime: RuntimeServices,
    org: Org,
    sandbox_provider_config: SandboxProviderConfig,
    creation_semaphore: Semaphore,
    authority: ExecutionAuthority,
    *,
    sandbox_provider: SandboxProvider | None = None,
    queue_context: SandboxQueueContext | None = None,
    started_by_email: str | None = None,
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
                authority=authority,
                claimed_at=claimed_at,
                sandbox_provider=sandbox_provider,
                queue_context=queue_context,
            )

    def record_attempt_failure(attempt: SandboxRecoveryAttempt, exc: Exception) -> None:
        _observe_task_retry(attempt, exc)
        if isinstance(exc, SandboxSetupError):
            _record_failure_before_retry(task_row, authority, exc, attempt.number)

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


def _admit_direct_task(task_row: Task, org: Org, authority: ExecutionAuthority) -> bool | None:
    with Session(bind=engine) as session:
        try:
            benchmark = lock_execution_authority(session, authority)
        except ExecutionAuthorityRevoked:
            return None
        task = fetch_task_row(task_row.id, session, org)
        if task.started_at != task_row.started_at or task.status in (
            TaskStatus.STOPPED,
            TaskStatus.FINISHED,
            TaskStatus.ERROR,
        ):
            return None
        if task.status == TaskStatus.PENDING:
            active = BenchmarkRepository(session).count_tasks(
                org.id, benchmark.id, (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
            )
            if active >= benchmark.arguments.concurrency:
                return False
        if commit_task_status_transition(
            task.id,
            session,
            org,
            TaskStatus.BUILDING,
            expected_started_at=task.started_at,
            expected_status=task.status,
            authority=authority,
        ):
            return True
        return None


async def _acquire_queue_preparation(stack: AsyncExitStack, context: SandboxQueueContext | None) -> None:
    if context is not None and is_reserved_queue_pool_id(context.pool_id):
        await stack.enter_async_context(context.preparation_slots)


async def _process_task_attempt(
    task_row: Task,
    start_benchmark_request: StartBenchmarkRequest,
    benchmark_service: BenchmarkServiceClient,
    benchmark_id: UUID,
    task_id: str,
    runtime: RuntimeServices,
    task_logs: TaskLogBuffer,
    org: Org,
    sandbox_provider_config: SandboxProviderConfig,
    creation_semaphore: Semaphore,
    dependency_setup_recovery: _DependencySetupRecoveryState,
    recovery_attempt: SandboxRecoveryAttempt,
    authority: ExecutionAuthority,
    *,
    claimed_at: datetime,
    sandbox_provider: SandboxProvider | None = None,
    queue_context: SandboxQueueContext | None = None,
) -> dict[str, dict[str, Any] | None]:
    sentry_sdk.set_tag("benchmark_name", start_benchmark_request.benchmark_name)
    sentry_sdk.set_tag("agent_name", start_benchmark_request.contract.name)

    phase = FailurePhase.ADMISSION

    requested_attempt_started_at = task_row.started_at
    task_row_id = task_row.id

    def claim_attempt() -> tuple[Benchmark, Task] | None:
        with Session(bind=engine) as task_session:
            try:
                benchmark_row = lock_execution_authority(task_session, authority)
            except ExecutionAuthorityRevoked:
                task_session.rollback()
                return None
            current_row = fetch_task_row(task_row_id, task_session, org)
            if normalized_attempt_time(current_row.started_at) != normalized_attempt_time(requested_attempt_started_at):
                return None
            if benchmark_row.status == BenchmarkStatus.STOPPING or current_row.status == TaskStatus.STOPPED:
                handle_early_exit(current_row, task_session, authority)
                return None
            return benchmark_row, current_row

    claimed = await database_call(claim_attempt)
    if claimed is None:
        return {task_id: None}
    benchmark_row, task_row = claimed
    attempt_started_at = task_row.started_at
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
    benchmark_agent_name = benchmark_row.arguments.contract.name
    benchmark_started_by_email = benchmark_row.started_by_email
    selected_project_gateway_secret_ref = benchmark_row.arguments.selected_project_gateway_secret_ref

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

    evaluation_lock: PostgresAdvisoryLock | None = None
    evaluation_lock_acquired = False
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

    def ensure_wall_deadline() -> datetime:
        with Session(bind=engine) as wall_session:
            lock_execution_authority(wall_session, authority)
            repository = BenchmarkRepository(wall_session)
            owned = repository.lock_task(org.id, task_row.benchmark, task_row.id)
            if owned is None or owned.started_at != attempt_started_at or owned.status == TaskStatus.STOPPED:
                raise ExecutionAuthorityRevoked("Task attempt no longer owns the wall deadline")
            if owned.credited_wall_deadline_at is None:
                owned.credited_wall_deadline_at = datetime.now(UTC) + timedelta(seconds=TASK_WALL_SECONDS)
                repository.save(owned)
                wall_session.commit()
            task_row.credited_wall_deadline_at = owned.credited_wall_deadline_at
            return owned.credited_wall_deadline_at

    preparation_stack = AsyncExitStack()

    def open_task_session() -> Session:
        bind = evaluation_lock.connection if evaluation_lock_acquired and evaluation_lock is not None else engine
        return Session(bind=bind)

    def stored_task_stopped(session_factory: Callable[[], Session]) -> bool:
        with session_factory() as task_session:
            return fetch_task_row(task_row.id, task_session, org).status == TaskStatus.STOPPED

    evaluation_resume_state = task_row.eval_resume_state
    sandbox_id_for_recovery: str | None = None
    exit_reason: AgentCausedExitReason | None = None
    evaluation_start_time: float | None = None
    start_sandbox_run_time: float | None = None

    def on_eval_resume_state(state: dict[str, Any]) -> None:
        nonlocal evaluation_resume_state
        evaluation_resume_state = state
        save_eval_resume_state(
            task_row.id,
            org,
            state,
            expected_started_at=attempt_started_at,
            authority=authority,
            connection=evaluation_lock.connection if evaluation_lock_acquired and evaluation_lock is not None else None,
        )

    sandbox_writes: set[asyncio.Future[None]] = set()
    sandbox_write_order = asyncio.Lock()

    async def save_announced_sandbox(link: SandboxLink, sandbox_id: str) -> None:
        async with sandbox_write_order:
            await database_call(lambda: record_sandbox(link, sandbox_id))

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

    def on_sandbox_created(created: SandboxCreatedData) -> None:
        link = SandboxLink(benchmark_id=benchmark_id, provider=created.provider)
        write = asyncio.ensure_future(save_announced_sandbox(link, created.sandbox_id))
        sandbox_writes.add(write)
        write.add_done_callback(sandbox_writes.discard)

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
        if not _saved_project_gateway_root(start_benchmark_request, org, selected_project_gateway_secret_ref):
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
            EgressProxy(EGRESS_PROXY, engine),
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
                result = await _run_benchmark_service_websocket(
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
            if task_row.credited_wall_deadline_at is not None:
                require_wall_seconds_remaining(task_row.credited_wall_deadline_at)
            return result

    def execution_is_current(expected_status: TaskStatus | None = None) -> bool:
        with open_task_session() as task_session:
            try:
                lock_execution_authority(task_session, authority)
            except ExecutionAuthorityRevoked:
                task_session.rollback()
                return False
            task = fetch_task_row(task_row.id, task_session, org)
            is_current = (
                task.status != TaskStatus.STOPPED
                and task.started_at == attempt_started_at
                and (expected_status is None or task.status == expected_status)
            )
            task_session.rollback()
            return is_current

    def task_is_stopped() -> bool:
        return not execution_is_current()

    def return_queued_task_to_pending() -> bool:
        if queue_context is None:
            return True
        with Session(bind=engine) as task_session:
            return commit_task_status_transition(
                task_row.id,
                task_session,
                org,
                TaskStatus.PENDING,
                expected_started_at=attempt_started_at,
                expected_status=expected_failure_status,
                authority=authority,
            )

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
        failure_phase = phase
        await task_logs.flush()

        def persist_failure() -> bool:
            with open_task_session() as task_session:
                task = fetch_task_row(task_row.id, task_session, org)
                return commit_task_error(
                    task,
                    task_session,
                    failure.message,
                    producer=failure.producer,
                    operation=failure.operation,
                    error_type=failure.error_type,
                    category=failure.category,
                    cause_code=failure.cause_code,
                    phase=failure_phase,
                    transient=failure.transient,
                    expected_started_at=attempt_started_at,
                    expected_status=expected_failure_status,
                    authority=authority,
                )

        if await database_call(persist_failure):
            record_task_outcome(
                TaskOutcome.FAILED,
                {
                    "failure_category": failure.category.value,
                    "failure_phase": failure_phase.value,
                    "producer": failure.producer,
                },
            )
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

    def commit_resumed_evaluation(
        evaluation_result_row: EvaluationResult,
        evaluation_run_duration: float,
        sandbox_run_duration: float | None,
    ) -> dict[str, dict[str, Any] | None]:
        with open_task_session() as task_session:
            repository = BenchmarkRepository(task_session)
            repository.save(evaluation_result_row)
            task_in_session = fetch_task_row(task_row.id, task_session, org)
            if task_in_session.task_breakdown:
                existing_breakdown = repository.get_task_breakdown(task_in_session.task_breakdown)
                assert existing_breakdown is not None
                existing_breakdown.evaluation_run_duration = evaluation_run_duration
                if sandbox_run_duration is not None:
                    existing_breakdown.sandbox_run_duration = sandbox_run_duration
            if not commit_task_status_transition(
                task_row.id,
                task_session,
                org,
                TaskStatus.FINISHED,
                expected_started_at=attempt_started_at,
                expected_status=(TaskStatus.EVALUATING, TaskStatus.ERROR),
                authority=authority,
            ):
                return {task_id: None}
            return {task_id: evaluation_result_row.result}

    async def recreate_sandbox(failure: Failure) -> dict[str, dict[str, Any] | None]:
        exc = failure.exc
        if not await database_call(return_queued_task_to_pending):
            return {task_id: None}
        log_output(f"\n[ERROR] {exception_message(exc)}")
        retry = exc if isinstance(exc, SandboxSetupError) else SandboxSetupError(str(exc))
        retry.phase = phase
        retry.cause_code = failure.cause_code
        if retry is exc:
            raise retry
        raise retry from exc

    async def recover_evaluation_stream_failure(error_message: str) -> dict[str, dict[str, Any] | None] | None:
        if evaluation_resume_state is None:
            return None
        if await database_call(task_is_stopped):
            return {task_id: None}

        recovery_message = f"{error_message}; resuming evaluation from durable benchmark state"
        logger.warning(recovery_message)
        log_output(f"\n[WARN] {recovery_message}\n")
        resume_eval_start_time = time.perf_counter()
        try:
            task_logs.last_log_time = time.monotonic()
            evaluation_result = await resume_evaluation_with_wall(evaluation_resume_state)
        except CreditedTaskWallTimeExceeded as wall_error:
            if await database_call(task_is_stopped):
                return {task_id: None}
            return await commit_wall_error(wall_error)
        except Exception as resume_error:
            if await database_call(task_is_stopped):
                return {task_id: None}
            failure = describe_failure(resume_error, exception_message(resume_error), phase=phase)
            if isinstance(resume_error, BenchmarkServiceWebSocketDNSResolutionError):
                detail = f"WebSocket DNS resolution failed during resume: {exception_message(resume_error)}"
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
        evaluation_result_row = EvaluationResult(
            org_id=org.id,
            task=task_row.id,
            instance_id=sandbox_id_for_recovery,
            result=evaluation_result_value,
            agent_caused_exit_reason=exit_reason,
        )
        await task_logs.flush()
        return await database_call(
            lambda: commit_resumed_evaluation(evaluation_result_row, evaluation_run_duration, sandbox_run_duration)
        )

    if task_row.credited_wall_deadline_at is not None:
        try:
            require_wall_seconds_remaining(task_row.credited_wall_deadline_at)
        except CreditedTaskWallTimeExceeded as error:
            return await commit_wall_error(error)

    try:
        evaluation_resume_state = task_row.eval_resume_state
        if task_row.status == TaskStatus.EVALUATING and evaluation_resume_state is not None:
            phase = FailurePhase.EVALUATION
            evaluation_lock = task_evaluation_lock(engine, task_row.id)
            evaluation_lock_acquired = await evaluation_lock.__aenter__()
            if not evaluation_lock_acquired:
                return {task_id: None}

            def claim_resume_state() -> dict[str, Any] | None:
                with open_task_session() as ownership_session:
                    try:
                        lock_execution_authority(ownership_session, authority)
                    except ExecutionAuthorityRevoked:
                        ownership_session.rollback()
                        return None
                    owned_task = BenchmarkRepository(ownership_session).lock_task(
                        org.id,
                        task_row.benchmark,
                        task_row.id,
                        started_at=ExecutorDispatchRepository(ownership_session).created_at(authority.dispatch_id),
                        statuses=(TaskStatus.EVALUATING,),
                    )
                    owned_state = owned_task.eval_resume_state if owned_task is not None else None
                    ownership_session.rollback()
                    return owned_state

            owned_resume_state = await database_call(claim_resume_state)
            if owned_resume_state is None:
                return {task_id: None}
            evaluation_resume_state = owned_resume_state

            try:
                log_output("Resuming evaluation from durable benchmark state\n")
                resume_eval_start_time = time.perf_counter()
                task_logs.last_log_time = time.monotonic()
                evaluation_result = await resume_evaluation_with_wall(evaluation_resume_state)
                resume_eval_duration = time.perf_counter() - resume_eval_start_time
                evaluation_result_row = EvaluationResult(
                    org_id=org.id,
                    task=task_row.id,
                    instance_id=None,
                    result=cast(dict[str, Any], evaluation_result),
                    agent_caused_exit_reason=None,
                )

                await task_logs.flush()
                return await database_call(
                    lambda: commit_resumed_evaluation(evaluation_result_row, resume_eval_duration, None)
                )
            except SandboxNotFoundError:
                if await database_call(lambda: stored_task_stopped(lambda: Session(bind=engine))):
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
                if await database_call(lambda: stored_task_stopped(open_task_session)):
                    return {task_id: None}

                raise e from e

        if queue_context is None:
            while not (admitted := await to_thread(_admit_direct_task, task_row, org, authority)):
                if admitted is None:
                    return {task_id: None}
                await asyncio.sleep(_DIRECT_ADMISSION_POLL_SECONDS)

        await _acquire_queue_preparation(preparation_stack, queue_context)
        async with enforce_wall_deadline(task_row.credited_wall_deadline_at):
            task_data = await recovery_attempt.retrieve_task()
        unrestricted_docker = sandbox_provider_config.type == "docker"
        run_egress_policy = combine_run_egress_policies(
            task_data.egress.run, start_benchmark_request.contract.egress_allowlist
        )
        if unrestricted_docker and any(
            policy != "*"
            for policy in (
                start_benchmark_request.contract.install_egress_policy,
                task_data.egress.setup_task,
                run_egress_policy,
                task_data.egress.evaluation,
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
            egress_proxy = EgressProxy(EGRESS_PROXY, engine)
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

        if queue_context is None and not await database_call(lambda: execution_is_current(TaskStatus.BUILDING)):
            return {task_id: None}

        async with enforce_wall_deadline(task_row.credited_wall_deadline_at):
            resolved_secrets = await resolve_secrets(start_benchmark_request.contract.secrets, runtime.secrets)

        deadline_controller: ExternalServiceDeadlineController

        env_vars = {
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

        task_breakdown = TaskBreakdown()

        async def persist_generation_accounting_summary(
            summary: GenerationAccountingSummary,
        ) -> None:
            await database_call(
                lambda: _persist_generation_accounting_summary(
                    summary,
                    task_breakdown=task_breakdown,
                    task_row_id=task_row.id,
                    org=org,
                    authority=authority,
                    expected_started_at=attempt_started_at,
                    open_task_session=open_task_session,
                )
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
            wall_deadline_at = await database_call(ensure_wall_deadline)
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
                redact_error = create_agent_error_redactor(scoped_env_vars[name] for name in contract.secrets)
                phase = FailurePhase.SANDBOX
                async with (
                    create_sandbox(
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
                        sandbox_link=SandboxLink(
                            benchmark_id=benchmark_id,
                            provider=sandbox_provider_config.type,
                        ),
                    ) as sandbox,
                    token_rotation.into(sandbox),
                ):
                    try:
                        yield sandbox
                    finally:
                        if wall_timeout is not None and not wall_timeout.expired():
                            wall_timeout.reschedule(None)

        async with AsyncExitStack() as sandbox_stack:
            if queue_context is None:
                sandbox = await sandbox_stack.enter_async_context(sandbox_context())
            else:
                async with enforce_wall_deadline(task_row.credited_wall_deadline_at):
                    sandbox = await enter_queued_sandbox(
                        stack=sandbox_stack,
                        context=queue_context,
                        task_row_id=task_row.id,
                        expected_started_at=attempt_started_at,
                        authority=authority,
                        source=task_data.source,
                        resources=task_data.resources,
                        creation_state=creation_state,
                        create=sandbox_context,
                        preparation_stack=preparation_stack,
                    )
                if sandbox is None:
                    return {task_id: None}
            sandbox_id_for_recovery = sandbox.id
            phase = FailurePhase.SETUP
            if start_benchmark_request.contract.final_output is not None:
                descriptor = ActiveNativeOutput(
                    attempt_started_at=attempt_started_at,
                    sandbox_id=sandbox.id,
                    runtime_source=task_data.source,
                    output_path=str(PurePosixPath(task_data.cwd) / start_benchmark_request.contract.final_output),
                )
                try:
                    with open_task_session() as task_session:
                        publish_native_output(task_session, task_row.id, descriptor, authority)
                except ExecutionAuthorityRevoked:
                    raise
                except Exception:
                    logger.exception("Failed to publish native output", extra={"task_id": str(task_row.id)})

                def revoke_native_output() -> None:
                    try:
                        with open_task_session() as task_session:
                            clear_native_output(task_session, task_row.id, descriptor)
                    except Exception:
                        logger.exception("Failed to clear native output", extra={"task_id": str(task_row.id)})

                sandbox_stack.push_async_callback(lambda: database_call(revoke_native_output))
            task_breakdown.sandbox_build_duration = time.perf_counter() - start_sandbox_build_time
            start_sandbox_run_time = time.perf_counter()

            try:
                if queue_context is None and not await database_call(
                    lambda: _transition_task(task_row.id, org, TaskStatus.IN_PROGRESS, attempt_started_at, authority)
                ):
                    return {task_id: None}

                await upload_agent_artifacts(
                    sandbox,
                    start_benchmark_request.contract,
                    str(benchmark_id),
                    object_store,
                )

                agent_sandbox = runtime_sandbox(sandbox, task_data.source)

                async def install_agent() -> None:
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
                _ = await _run_benchmark_service_websocket(
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

                run_access = RunStageAccess()
                if not unrestricted_docker:
                    run_access = await sandbox_stack.enter_async_context(
                        stage_egress.run_stage(
                            agent_sandbox,
                            run_egress_policy,
                            run_attempt(),
                            local_hosts=task_data.egress.sandbox_local_hosts,
                            inspect=task_data.egress.run_inspect,
                        )
                    )
                if run_access.secrets:
                    egress_redactor = create_agent_error_redactor(run_access.secrets)
                    previous_redactor = redact_error

                    def redact_with_egress_secrets(content: str) -> str:
                        content = egress_redactor(content)
                        return previous_redactor(content) if previous_redactor is not None else content

                    redact_error = redact_with_egress_secrets
                agent_attempt_started = True
                phase = FailurePhase.AGENT
                generation_gateway_url = (
                    EXTERNAL_SERVICE_GATEWAY_URL if deadline_controller.snapshot is not None else None  # noqa: F821
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
                        agent_output_s3_key=agent_output_s3_key,
                        plan=plan,
                        benchmark_id=str(benchmark_id),
                        execution_is_current=execution_is_current,
                        deadline_controller=deadline_controller,  # noqa: F821
                        generation_gateway_url=generation_gateway_url,
                        extra_env=run_access.env_vars,
                        log_line=task_logs.write_line,
                        on_external_service_finalized=persist_generation_accounting_summary,
                        redact_error=redact_error,
                        secret_env_names=secret_env_names,
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

                task_breakdown.agent_run_duration = agent_run_time
                phase = FailurePhase.EVALUATION
                if not await database_call(
                    lambda: _transition_task(
                        task_row.id, org, TaskStatus.EVALUATING, attempt_started_at, authority, task_breakdown
                    )
                ):
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
                logger.info("Evaluating agent %s in sandbox %s", start_benchmark_request.contract.name, sandbox.name)
                if not unrestricted_docker:
                    await apply_egress_policy(sandbox, task_data.egress.evaluation)
                task_logs.last_log_time = time.monotonic()
                async with grading_credentials(resolved_secrets) as grading_token:
                    evaluation_result = await _run_benchmark_service_websocket(
                        benchmark_service.evaluate_instance(
                            task_row.task_id,
                            sandbox.id,
                            on_message=task_logs.write_line,
                            on_eval_resume_state=on_eval_resume_state,
                            on_sandbox_created=on_sandbox_created,
                            dataset=start_benchmark_request.dataset,
                            sandbox_provider=sandbox_provider_config,
                            child_egress=child_egress,
                            grading_token=grading_token,
                        )
                    )
                task_breakdown.evaluation_run_duration = time.perf_counter() - evaluation_start_time

                assert start_sandbox_run_time is not None
                task_breakdown.sandbox_run_duration = time.perf_counter() - start_sandbox_run_time

                await task_logs.flush()

                evaluation_result_row = EvaluationResult(
                    org_id=org.id,
                    task=task_row.id,
                    instance_id=sandbox.id,
                    result=cast(dict[str, Any], evaluation_result),
                    agent_caused_exit_reason=exit_reason,
                )

                def commit_evaluation() -> dict[str, dict[str, Any] | None]:
                    with Session(bind=engine) as task_session:
                        repository = BenchmarkRepository(task_session)
                        repository.save(evaluation_result_row)
                        task_in_session = fetch_task_row(task_row.id, task_session, org)
                        existing_breakdown = (
                            repository.get_task_breakdown(task_in_session.task_breakdown)
                            if task_in_session.task_breakdown
                            else None
                        )
                        if existing_breakdown is None:
                            raise TrackerServiceError(f"Missing task breakdown for task {task_row.id}")
                        existing_breakdown.evaluation_run_duration = task_breakdown.evaluation_run_duration
                        existing_breakdown.sandbox_run_duration = task_breakdown.sandbox_run_duration
                        if not commit_task_status_transition(
                            task_row.id,
                            task_session,
                            org,
                            TaskStatus.FINISHED,
                            expected_started_at=attempt_started_at,
                            expected_status=(TaskStatus.EVALUATING, TaskStatus.ERROR),
                            authority=authority,
                        ):
                            return {task_id: None}

                        return {task_id: evaluation_result_row.result}

                return await database_call(commit_evaluation)
            except Exception:
                if await database_call(lambda: stored_task_stopped(lambda: Session(bind=engine))):
                    return {task_id: None}

                raise

    except GenerationTerminationUnconfirmedError as e:
        if await database_call(task_is_stopped):
            return {task_id: None}
        error_message = exception_message(e)
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
        if await database_call(task_is_stopped):
            return {task_id: None}
        if agent_attempt_started:
            error_message = exception_message(e)
            log_output(f"\n[ERROR] {error_message}")
            failure = replace(
                describe_failure(e, error_message, phase=phase),
                producer="sandbox_provider",
                operation="sandbox_recovery",
                category=FailureCategory.INFRASTRUCTURE,
            )
            return await commit_terminal_error(failure)
        if not await database_call(return_queued_task_to_pending):
            return {task_id: None}
        e.phase = phase
        log_output(f"\n[ERROR] {exception_message(e)}")
        raise
    except SandboxNotFoundError as e:
        if await database_call(task_is_stopped):
            return {task_id: None}
        if recovery_attempt.sandbox_loss_retry_available and not agent_attempt_started:
            message = (
                "Sandbox disappeared; restoring the task from its durable volume "
                f"(attempt {recovery_attempt.number + 1}/{recovery_attempt.max_attempts})"
            )
            logger.warning(message)
            log_output(f"\n[WARNING] {message}\n")
            raise

        failure = replace(
            describe_failure(e, exception_message(e), phase=phase),
            producer="sandbox_provider",
            operation="sandbox_recovery",
            category=FailureCategory.INFRASTRUCTURE,
        )
        log_output(f"\n[ERROR] {failure.message}")
        return await commit_terminal_error(failure)
    except CreditedTaskWallTimeExceeded as wall_error:
        if await database_call(task_is_stopped):
            return {task_id: None}
        log_output(f"\n[ERROR] {wall_error}")
        return await commit_wall_error(wall_error)
    except Exception as e:
        if await database_call(task_is_stopped):
            return {task_id: None}
        failure = describe_failure(
            e, exception_message(e), phase=phase, idle_seconds=int(time.monotonic() - task_logs.last_log_time)
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
            await drain_sandbox_writes()
        finally:
            await preparation_stack.aclose()
            if evaluation_lock is not None:
                await evaluation_lock.__aexit__(None, None, None)


def _transition_task(
    task_row_id: UUID,
    org: Org,
    status: TaskStatus,
    expected_started_at: datetime,
    authority: ExecutionAuthority,
    task_breakdown: TaskBreakdown | None = None,
) -> bool:
    with Session(bind=engine) as task_session:
        if task_breakdown is not None:
            BenchmarkRepository(task_session).save(task_breakdown)
        return commit_task_status_transition(
            task_row_id,
            task_session,
            org,
            status,
            expected_started_at=expected_started_at,
            authority=authority,
        )
