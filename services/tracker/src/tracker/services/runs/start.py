import asyncio
import traceback
from asyncio import Semaphore, gather
from collections.abc import AsyncIterable, Callable, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID

import sentry_sdk
from benchmark_service import SandboxProvider, SandboxProviderConfig
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError, BenchmarkServiceUnauthenticatedError
from pydantic import ValidationError

from tracker.clients.task_service import TaskService


from tracker.config import AUTH_REQUIRED, BenchmarkServiceDestination, classify_benchmark_service_destination
from tracker.domain import BenchmarkStatus, FailureCategory, TaskStatus
from tracker.exceptions import ExecutionAuthorityRevoked, TrackerServiceError
from tracker.executor.control_protocol import TaskRef
from tracker.executor.control_runs import RunControl, RunFailure, RunTaskState, upload_final_view
from tracker.executor.control_tasks import TaskControl, TaskReadResponse
from tracker.executor.dependencies import get_execution_runtime
from tracker.runtime.lifecycle import finish_cleanup

from tracker.logging import get_logger
from tracker.logging.context import sandbox_provider_scope
from tracker.logging.events import LogEvent
from tracker.observability import error_span
from tracker.observability.sentry import capture_exception
from tracker.observability.tracing import observability_span
from tracker.outbound_security import validate_custom_service_destination
from tracker.runtime.services import RuntimeServices
from tracker.scheduler.admission import SandboxQueueContext, create_queue_context, recover_queued_pool
from tracker.executor.control_queue import QueueControl, is_reserved_queue_pool_id
from tracker.types import ManagedExecutionContext, OrgContext, RunExecutionRequest, benchmark_arguments_adapter
from tracker.clients.benchmark_service import wait_for_benchmark_service
from tracker.services.datasets.artifacts import ArtifactSink
from tracker.services.datasets.env import SecretReader, secret_reader
from tracker.services.datasets.ref import HostedDataset
from tracker.services.datasets.scoring import scorer_for
from tracker.clients.dataset_tasks import dataset_service
from tracker.local.resources import LocalResources, reads_secrets_file
from tracker.local.secrets import SecretsFile
from tracker.runtime.artifacts import task_artifact_key
from tracker.secret_access import resolve_secret_name, validate_secret_names
from tracker.services.failures import classify_failure
from tracker.services.tasks.error_summary import summarize_task_errors
from tracker.services.tasks.execute import ResizableLimiter, TaskMonitor, TrackedTask, process_task

logger = get_logger(__name__)
_SANDBOX_CREATION_CAP = 600
_POOL_RECOVERY_INTERVAL_SECONDS = 30.0
_TERMINAL = (BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED)


def _artifact_sink(runtime: RuntimeServices, benchmark_id: UUID) -> ArtifactSink:
    async def store(task_id: str, name: str, chunks: AsyncIterable[bytes]) -> None:
        await runtime.objects.put_stream(task_artifact_key(str(benchmark_id), task_id, name), chunks)

    return store


def _task_secret_reader(request: RunExecutionRequest, runtime: RuntimeServices, org: OrgContext) -> SecretReader:
    properties = request.properties
    if isinstance(properties, LocalResources) and reads_secrets_file(properties) and properties.secrets_file:
        return secret_reader(SecretsFile(properties.secrets_file), lambda name: name)

    def stored_name(name: str) -> str:
        stored = resolve_secret_name(name, org_id=org.id, org_name=org.name, auth_required=AUTH_REQUIRED)
        validate_secret_names([stored], org_id=org.id, org_name=org.name, auth_required=AUTH_REQUIRED)
        return stored

    return secret_reader(runtime.secrets, stored_name)


@dataclass(frozen=True)
class _QueuedExecution:
    request: RunExecutionRequest
    benchmark_id: UUID
    verified_task_ids: list[str]
    context_version: int


def _parse_queued_execution(
    start_benchmark_request_json: dict[str, Any] | None,
    benchmark_id_str: str | None,
    verified_task_ids: list[str] | None,
    execution_context_json: dict[str, Any] | None,
) -> _QueuedExecution:
    if execution_context_json is None:
        if (
            start_benchmark_request_json is None
            or start_benchmark_request_json.get("environment") != "local"
            or benchmark_id_str is None
            or verified_task_ids is None
        ):
            raise ValueError("Queued benchmark request requires an execution context.")
        execution_context_json = {
            "version": 3,
            "benchmark_id": benchmark_id_str,
            "verified_task_ids": verified_task_ids,
            "start_benchmark_request": start_benchmark_request_json,
        }
    elif start_benchmark_request_json is not None or benchmark_id_str is not None or verified_task_ids is not None:
        raise ValueError("Queued benchmark request must contain only an execution context.")
    try:
        context = ManagedExecutionContext.model_validate(execution_context_json)
    except ValidationError:
        raise ValueError("Queued managed execution context is invalid and cannot be processed.") from None
    return _QueuedExecution(
        context.start_benchmark_request, context.benchmark_id, context.verified_task_ids, context.version
    )


def _queued_benchmark_id(benchmark_id_str: str | None, execution_context_json: dict[str, Any] | None) -> UUID:
    raw = execution_context_json.get("benchmark_id") if execution_context_json is not None else benchmark_id_str
    try:
        return UUID(str(raw))
    except (TypeError, ValueError):
        raise ValueError("Queued benchmark request has no valid benchmark ID.") from None


def _capture_run_error(
    exc: BaseException,
    benchmark_id: UUID,
    *,
    producer: str,
    operation: str,
    category: FailureCategory,
    cause_code: str | None = None,
) -> None:
    with error_span(
        "run.error",
        exc,
        benchmark_id=str(benchmark_id),
        producer=producer,
        operation=operation,
        error_type=type(exc).__name__,
        cause_code=cause_code or "",
        failure_category=category.value,
    ):
        logger.error(
            "Run execution failed",
            exc_info=(type(exc), exc, exc.__traceback__),
            extra={
                "event": LogEvent.RUN_FAILED,
                "benchmark_id": str(benchmark_id),
                "producer": producer,
                "operation": operation,
                "error_type": type(exc).__name__,
                "cause_code": cause_code or "",
                "failure_category": category.value,
            },
        )
        with sentry_sdk.new_scope() as scope:
            scope.set_tag("failure_category", category.value)
            capture_exception(exc)


def _record_run_finalized(benchmark_id: UUID, status: BenchmarkStatus, dispatch_id: UUID) -> None:
    with observability_span(
        "run.finalized", benchmark_id=str(benchmark_id), status=status.value, executor_dispatch_id=str(dispatch_id)
    ):
        pass


async def _commit_run_error(run_control: RunControl, failure: RunFailure, benchmark_id: UUID) -> bool:
    decision = await run_control.commit_run_error(failure)
    if decision.committed and decision.status == BenchmarkStatus.ERROR:
        _record_run_finalized(benchmark_id, BenchmarkStatus.ERROR, run_control.ref.dispatch_id)
    return decision.committed


async def _read_task(control: TaskControl, row: RunTaskState) -> TaskReadResponse:
    response = await control.read(
        TaskRef(org_id=row.ref.org_id, task_row_id=row.ref.task_row_id, task_id=row.ref.task_id)
    )
    if response.task.started_at != row.ref.started_at:
        raise ExecutionAuthorityRevoked("Task attempt changed before execution")
    return response


def _tracked_task(
    response: TaskReadResponse,
    request: RunExecutionRequest,
    service: TaskService,
    runtime: RuntimeServices,
    org: OrgContext,
    config: SandboxProviderConfig,
    provider: SandboxProvider,
    creation_semaphore: Semaphore,
    control: TaskControl,
    queue_context: SandboxQueueContext | None = None,
) -> TrackedTask:
    row = response.task
    return TrackedTask(
        process_task(
            row,
            request,
            service,
            row.benchmark,
            row.task_id,
            runtime,
            org,
            sandbox_provider_config=config,
            sandbox_provider=provider,
            creation_semaphore=creation_semaphore,
            queue_context=queue_context,
            started_by_email=response.benchmark.started_by_email,
            control=control,
        ),
        control,
        row.started_at,
    )


async def _stop_monitor(monitor_task: asyncio.Task[None]) -> None:
    if monitor_task.cancel():
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
    else:
        await monitor_task


async def _run_queued_tasks(
    *,
    task_rows: Sequence[RunTaskState],
    request: RunExecutionRequest,
    service: TaskService,
    runtime: RuntimeServices,
    org: OrgContext,
    config: SandboxProviderConfig,
    provider: SandboxProvider,
    creation_semaphore: Semaphore,
    context: SandboxQueueContext,
    run_control: RunControl,
    task_control: TaskControl,
    record_cancellation: Callable[[dict[UUID, datetime]], None],
) -> None:
    owned_ids = {row.ref.task_row_id for row in task_rows}
    local_runners: dict[UUID, tuple[datetime, asyncio.Task[dict[str, dict[str, Any] | None]]]] = {}
    retired_evaluation_ids: set[UUID] = set()
    tracked: dict[str, TrackedTask] = {}
    coordinator_done = asyncio.Event()
    monitor = TaskMonitor(tracked, None, control=task_control, coordinator_done=coordinator_done)
    monitor_task = asyncio.create_task(monitor.track_tasks())
    cancellation_attempts: dict[UUID, datetime] | None = None

    async def launch(row: RunTaskState) -> None:
        response = await _read_task(task_control, row)
        task = response.task
        tracked_task = _tracked_task(
            response,
            request,
            service,
            runtime,
            org,
            config,
            provider,
            creation_semaphore,
            task_control,
            context,
        )
        tracked[row.ref.task_id] = tracked_task
        local_runners[row.ref.task_row_id] = (row.ref.started_at, asyncio.create_task(tracked_task.run(None, task)))

    next_recovery = 0.0
    try:
        while True:
            now = asyncio.get_running_loop().time()
            if now >= next_recovery and not is_reserved_queue_pool_id(context.pool_id):
                await recover_queued_pool(context)
                next_recovery = now + _POOL_RECOVERY_INTERVAL_SECONDS
            completed: set[UUID] = set()
            for row_id, (_attempt, runner) in list(local_runners.items()):
                if runner.done():
                    await runner
                    del local_runners[row_id]
                    completed.add(row_id)
            snapshot = await run_control.read_run_state([row.ref.task_id for row in task_rows])
            if not snapshot.authority_current:
                raise ExecutionAuthorityRevoked("Executor dispatch authority changed")
            all_rows = snapshot.tasks
            current_rows = [row for row in all_rows if row.ref.task_row_id in owned_ids]
            current = {row.ref.task_row_id: row for row in current_rows}
            retired_evaluation_ids.update(
                row_id
                for row_id in completed
                if row_id in current
                and current[row_id].status == TaskStatus.EVALUATING
                and current[row_id].eval_resume_state is not None
            )
            active_ids = {
                row_id
                for row_id, (attempt, _runner) in local_runners.items()
                if row_id in current and current[row_id].ref.started_at == attempt
            }
            used_slots = sum(
                row.status in (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS)
                or (
                    row.status == TaskStatus.EVALUATING
                    and (
                        row.eval_resume_state is None
                        or row.ref.task_row_id not in owned_ids
                        or row.ref.task_row_id in active_ids
                    )
                )
                for row in all_rows
            )
            available = max(snapshot.concurrency - used_slots, 0)
            if snapshot.status in _TERMINAL:
                break
            if snapshot.status == BenchmarkStatus.IN_PROGRESS and available:
                candidates = sorted(
                    (
                        row
                        for row in current_rows
                        if row.status == TaskStatus.EVALUATING
                        and row.eval_resume_state is not None
                        and row.ref.task_row_id not in local_runners
                        and row.ref.task_row_id not in retired_evaluation_ids
                    ),
                    key=lambda row: (row.ref.started_at, row.ref.task_row_id),
                )
                for row in candidates[:available]:
                    await launch(row)
                available -= min(len(candidates), available)
            pending_contenders = sum(
                row_id in current
                and current[row_id].status == TaskStatus.PENDING
                and current[row_id].ref.started_at == attempt
                for row_id, (attempt, _runner) in local_runners.items()
            )
            reserved_queue = is_reserved_queue_pool_id(context.pool_id)
            budget = min(available, _SANDBOX_CREATION_CAP) - pending_contenders
            if (reserved_queue or budget > 0) and snapshot.status == BenchmarkStatus.IN_PROGRESS:
                reserved_ids = set(snapshot.reserved_task_ids)
                pending = sorted(
                    (
                        row
                        for row in current_rows
                        if row.status == TaskStatus.PENDING
                        and row.ref.task_row_id not in local_runners
                        and row.ref.task_row_id not in reserved_ids
                    ),
                    key=lambda row: (row.ref.started_at, row.ref.task_row_id),
                )
                for row in pending if reserved_queue else pending[:budget]:
                    await launch(row)
            actionable = any(
                row.status in (TaskStatus.PENDING, TaskStatus.BUILDING)
                or (
                    row.status == TaskStatus.EVALUATING
                    and row.eval_resume_state is not None
                    and row.ref.task_row_id not in retired_evaluation_ids
                )
                for row in current_rows
            )
            if not local_runners and not actionable:
                break
            await asyncio.sleep(context.poll_interval_seconds)
    except asyncio.CancelledError:
        cancellation_attempts = {row_id: attempt for row_id, (attempt, _runner) in local_runners.items()}
        raise
    finally:
        coordinator_done.set()
        for _attempt, runner in local_runners.values():
            runner.cancel()
        await gather(*(runner for _attempt, runner in local_runners.values()), return_exceptions=True)
        await _stop_monitor(monitor_task)
        if cancellation_attempts is not None:
            record_cancellation(cancellation_attempts)


async def process_benchmark(
    start_benchmark_request_json: dict[str, Any] | None = None,
    benchmark_id_str: str | None = None,
    verified_task_ids: list[str] | None = None,
    execution_context_json: dict[str, Any] | None = None,
    *,
    executor_dispatch_id: str,
    run_control: RunControl,
    task_control: TaskControl,
    queue_control: QueueControl,
) -> None:
    async with AsyncExitStack() as stack:
        await _process_benchmark(
            start_benchmark_request_json,
            benchmark_id_str,
            verified_task_ids,
            execution_context_json,
            executor_dispatch_id=executor_dispatch_id,
            run_control=run_control,
            task_control=task_control,
            queue_control=queue_control,
            stack=stack,
        )


async def _process_benchmark(
    start_benchmark_request_json: dict[str, Any] | None,
    benchmark_id_str: str | None,
    verified_task_ids: list[str] | None,
    execution_context_json: dict[str, Any] | None,
    *,
    executor_dispatch_id: str,
    run_control: RunControl,
    task_control: TaskControl,
    queue_control: QueueControl,
    stack: AsyncExitStack,
) -> None:
    benchmark_id = _queued_benchmark_id(benchmark_id_str, execution_context_json)
    task_ids: list[str] = []
    queued = False
    post_tasks = False
    finalization_deferred = False
    cancellation_attempts: dict[UUID, datetime] = {}
    try:
        execution = _parse_queued_execution(
            start_benchmark_request_json, benchmark_id_str, verified_task_ids, execution_context_json
        )
        request = execution.request
        task_ids = execution.verified_task_ids
        snapshot = await run_control.read_run_state(task_ids)
        if snapshot.status in _TERMINAL or not snapshot.authority_current:
            return
        arguments = benchmark_arguments_adapter.validate_python(snapshot.arguments)
        queued = snapshot.queue_pool_id is not None
        org = OrgContext(id=snapshot.org_id, name=snapshot.org_name, storage_key=snapshot.org_storage_key)
        sentry_sdk.set_tag("agent_name", request.contract.name)
        with observability_span(
            "run.started",
            benchmark_id=str(benchmark_id),
            benchmark_name=request.benchmark_name,
            agent_name=request.contract.name,
            task_count=len(task_ids),
            executor_dispatch_id=executor_dispatch_id,
        ):
            pass
        if request.resolved_dataset_version != arguments.dataset_version or (
            arguments.dataset_version is not None and request.dataset != arguments.dataset
        ):
            raise TrackerServiceError("Queued dataset selection does not match the saved run")
        if (
            arguments.dataset_version is not None
            and request.custom_benchmark_service != snapshot.custom_benchmark_service
        ):
            raise TrackerServiceError("Queued benchmark service does not match the pinned run")
        if request.custom_benchmark_service is not None:
            validate_custom_service_destination(
                request.custom_benchmark_service, org_name=org.name, auth_required=AUTH_REQUIRED
            )
        runtime = await get_execution_runtime(
            request,
            benchmark_id=benchmark_id,
            org_id=org.id,
            org_name=org.name,
            org_storage_key=org.storage_key,
            saved_environment=arguments.environment,
            saved_properties=arguments.properties,
            saved_sandbox_provider=arguments.sandbox_provider,
            saved_sandbox_provider_secret_name=arguments.sandbox_provider_secret_name,
            context_version=execution.context_version,
        )
        dataset_ref = request.dataset_ref
        dataset_version = await run_control.read_dataset_version() if isinstance(dataset_ref, HostedDataset) else None
        service: TaskService = await stack.enter_async_context(
            request.benchmark_service
            if dataset_ref is None
            else dataset_service(
                dataset_ref,
                runtime.objects,
                _task_secret_reader(request, runtime, org),
                _artifact_sink(runtime, benchmark_id),
                dataset_version.task_digests if dataset_version is not None else None,
            )
        )
        if (
            classify_benchmark_service_destination(snapshot.benchmark_name, request.custom_benchmark_service)
            == BenchmarkServiceDestination.HOSTED
        ):
            wake_snapshot = await run_control.read_run_state(task_ids)
            if not wake_snapshot.authority_current:
                raise ExecutionAuthorityRevoked("Executor dispatch authority changed")
            if wake_snapshot.benchmark_service_woken and isinstance(service, BenchmarkServiceClient):
                await wait_for_benchmark_service(service)
        config = await runtime.get_sandbox_provider_config()
        stack.enter_context(sandbox_provider_scope(config.type))
        provider = config.create_provider()
        stack.push_async_callback(lambda: finish_cleanup(asyncio.create_task(provider.close())))
        snapshot = await run_control.read_run_state(task_ids, create_missing_tasks=True)
        if not snapshot.authority_current:
            raise ExecutionAuthorityRevoked("Executor dispatch authority changed")
        by_id = {row.ref.task_id: row for row in snapshot.tasks}
        missing = [task_id for task_id in task_ids if task_id not in by_id]
        if missing:
            raise TrackerServiceError(
                f"Race condition occurred when resuming run {benchmark_id}. Missing task ids: {', '.join(missing)}"
            )
        run_rows = [
            by_id[task_id]
            for task_id in task_ids
            if by_id[task_id].status
            in (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
        ]
        creation_semaphore = Semaphore(_SANDBOX_CREATION_CAP)
        if queued:
            try:
                context = create_queue_context(control=queue_control, provider=provider)
            except Exception as error:
                logger.warning("Sandbox provider setup failed (%s)", type(error).__name__)
                raise TrackerServiceError("Sandbox provider configuration is unavailable") from error
            if not context.serves_queue(snapshot.queue_pool_id):
                raise TrackerServiceError("Configured sandbox provider does not match the run's queued provider pool")
            context = context.for_queue(snapshot.queue_pool_id)

            def record_cancellation(attempts: dict[UUID, datetime]) -> None:
                cancellation_attempts.update(attempts)

            await _run_queued_tasks(
                task_rows=run_rows,
                request=request,
                service=service,
                runtime=runtime,
                org=org,
                config=config,
                provider=provider,
                creation_semaphore=creation_semaphore,
                context=context,
                run_control=run_control,
                task_control=task_control,
                record_cancellation=record_cancellation,
            )
        else:
            run_rows.sort(key=lambda row: row.status != TaskStatus.EVALUATING)
            limiter = ResizableLimiter(snapshot.concurrency)
            tracked: dict[str, TrackedTask] = {}
            monitor = TaskMonitor(tracked, limiter, control=task_control)
            loaded = [await _read_task(task_control, row) for row in run_rows]
            for response in loaded:
                task = response.task
                tracked[task.task_id] = _tracked_task(
                    response,
                    request,
                    service,
                    runtime,
                    org,
                    config,
                    provider,
                    creation_semaphore,
                    task_control,
                )
            monitor_task = asyncio.create_task(monitor.track_tasks())
            try:
                runners = [
                    asyncio.create_task(tracked[response.task.task_id].run(limiter, response.task))
                    for response in loaded
                ]
                await gather(*runners)
            finally:
                await gather(*runners, return_exceptions=True)
                await _stop_monitor(monitor_task)
        post_tasks = True
        decision = await run_control.prepare_finalization()
        if decision.kind == "deferred":
            finalization_deferred = True
            return
        if decision.kind == "stopped":
            _record_run_finalized(benchmark_id, BenchmarkStatus.STOPPED, run_control.ref.dispatch_id)
            return
        if decision.kind == "all_error":
            finalization_deferred = not await _commit_run_error(
                run_control,
                RunFailure(
                    error_message=summarize_task_errors(cast(dict[str, str], decision.task_errors)),
                    producer="tracker",
                    operation="summarize_task_errors",
                    error_type="AllTasksFailed",
                    category=FailureCategory.UNKNOWN,
                    cause_code=None,
                    task_ids=task_ids,
                    fingerprint=decision.fingerprint,
                ),
                benchmark_id,
            )
            return
        inputs = decision.inputs
        if isinstance(service, BenchmarkServiceClient):
            final_score_response = await service.final_score(
                evaluation_results=inputs.evaluation_results, dataset=request.dataset
            )
            final_score, score_properties = final_score_response.final_score, final_score_response.metadata
        else:
            scorer = await scorer_for(
                dataset_ref,
                org.id,
                dataset_version.metric_digest if dataset_version is not None else None,
                runtime.objects,
            )
            scored = await scorer.score(inputs.evaluation_results)
            final_score, score_properties = scored.final_score, scored.properties
        claim = await run_control.commit_final_score(inputs.phase, inputs.fingerprint, final_score, score_properties)
        if not claim.committed:
            finalization_deferred = True
            return
        _record_run_finalized(benchmark_id, claim.status, run_control.ref.dispatch_id)
        if not (await run_control.publish_final_view(inputs.phase, "upload_before")).allowed:
            finalization_deferred = True
            return
        await upload_final_view(claim.final_view, runtime.objects)
        if not (await run_control.publish_final_view(inputs.phase, "upload_after")).allowed:
            finalization_deferred = True
            return
        if not (await run_control.publish_final_view(inputs.phase, "callback_before")).allowed:
            finalization_deferred = True
            return
        await runtime.run_completion_callback(claim.final_view)
        await run_control.publish_final_view(inputs.phase, "callback_after")
    except asyncio.CancelledError:
        if queued and not post_tasks:
            cancellation = await run_control.record_cancellation(cancellation_attempts, task_ids)
            finalization_deferred = not (cancellation.committed and cancellation.status in _TERMINAL)
        else:
            finalization_deferred = not await _commit_run_error(
                run_control,
                RunFailure(
                    error_message="Run was interrupted",
                    producer="tracker",
                    operation="process_benchmark",
                    error_type=asyncio.CancelledError.__name__,
                    category=FailureCategory.CANCELLED,
                    cause_code=None,
                    task_ids=task_ids,
                    fingerprint=None,
                ),
                benchmark_id,
            )
        raise
    except ExecutionAuthorityRevoked:
        finalization_deferred = True
    except BenchmarkServiceUnauthenticatedError as error:
        _capture_run_error(
            error,
            benchmark_id,
            producer="benchmark_service",
            operation="authenticate",
            category=FailureCategory.BENCHMARK_SERVICE,
            cause_code="authentication_failed",
        )
        error_message = f"{str(error)}\n{traceback.format_exc()}"
        logger.warning(error_message)
        await _commit_run_error(
            run_control,
            RunFailure(
                error_message=error_message,
                producer="benchmark_service",
                operation="authenticate",
                error_type=type(error).__name__,
                category=FailureCategory.BENCHMARK_SERVICE,
                cause_code="authentication_failed",
                task_ids=task_ids,
                fingerprint=None,
            ),
            benchmark_id,
        )
    except BenchmarkServiceError as error:
        _capture_run_error(
            error,
            benchmark_id,
            producer="benchmark_service",
            operation="process_benchmark",
            category=FailureCategory.BENCHMARK_SERVICE,
        )
        await _commit_run_error(
            run_control,
            RunFailure(
                error_message=str(error),
                producer="benchmark_service",
                operation="process_benchmark",
                error_type=type(error).__name__,
                category=FailureCategory.BENCHMARK_SERVICE,
                cause_code=None,
                task_ids=task_ids,
                fingerprint=None,
            ),
            benchmark_id,
        )
    except Exception as error:
        category = classify_failure(error)
        _capture_run_error(error, benchmark_id, producer="tracker", operation="process_benchmark", category=category)
        error_message = (
            f"{type(error).__name__}: {error}"
            if isinstance(error, TrackerServiceError) and not error.include_cause_in_task_error
            else f"{error}\n{traceback.format_exc()}"
        )
        await _commit_run_error(
            run_control,
            RunFailure(
                error_message=error_message,
                producer="tracker",
                operation="process_benchmark",
                error_type=type(error).__name__,
                category=category,
                cause_code=None,
                task_ids=task_ids,
                fingerprint=None,
            ),
            benchmark_id,
        )
    finally:
        if not finalization_deferred:
            await run_control.sweep_exit(task_ids)
