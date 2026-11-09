import asyncio
import traceback
from asyncio import Semaphore, gather
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import sentry_sdk
from benchmark_service import SandboxProvider, SandboxProviderConfig
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError, BenchmarkServiceUnauthenticatedError
from pydantic import ValidationError
from sqlmodel import Session

from tracker.executor.dependencies import get_execution_runtime
from tracker.runtime.lifecycle import finish_cleanup
from tracker.runtime.services import RuntimeServices
from tracker.config import AUTH_REQUIRED, BenchmarkServiceDestination, classify_benchmark_service_destination
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    DocentReadingStatus,
    ErrorResult,
    FailureCategory,
    FinalEvaluation,
    Org,
    Task,
    TaskStatus,
)
from tracker.database.session import database_call, engine
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.organization import OrganizationRepository
from tracker.repositories.scheduler import SchedulerRepository
from tracker.executor.dispatch_control import terminalize_active_dispatches
from tracker.exceptions import ExecutionAuthorityRevoked, TrackerServiceError
from tracker.services.failures import classify_failure
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.logging import get_logger
from tracker.logging.context import sandbox_provider_scope
from tracker.logging.events import LogEvent
from tracker.observability import error_span
from tracker.observability.sentry import capture_exception
from tracker.observability.tracing import observability_span
from tracker.outbound_security import validate_custom_service_destination
from tracker.scheduler.admission import SandboxQueueContext, create_queue_context
from tracker.repositories.service_demand import was_woken
from tracker.scheduler.service_capacity import wait_for_benchmark_service
from tracker.types import (
    FinalViewResponse,
    ManagedExecutionContext,
    RunExecutionRequest,
)

from tracker.services.benchmarks import (
    fetch_benchmark_row,
)
from tracker.services.final_views import create_final_view, upload_final_view
from tracker.services.tasks.execute import process_task
from tracker.services.tasks.monitor import ResizableLimiter, TaskMonitor, TrackedTask
from tracker.services.runs.finalize import (
    RUNNABLE_TASK_STATUSES,
    TERMINAL_BENCHMARK_STATUSES,
    TaskFingerprint,
    catch_errors_during_cleanup,
    commit_benchmark_error,
    fetch_final_score_state,
    finalize_all_error_run,
    set_benchmark_final_status,
)

logger = get_logger(__name__)

_SANDBOX_CREATION_CAP: int = 10


async def _run_queued_tasks(
    *,
    benchmark_id: UUID,
    task_rows: Sequence[tuple[str, Task]],
    start_benchmark_request: RunExecutionRequest,
    benchmark_service: BenchmarkServiceClient,
    runtime: RuntimeServices,
    org: Org,
    sandbox_provider_config: SandboxProviderConfig,
    sandbox_provider: SandboxProvider,
    creation_semaphore: Semaphore,
    queue_context: SandboxQueueContext,
    started_by_email: str | None,
    record_cancellation: Callable[[dict[UUID, datetime]], None],
    authority: ExecutionAuthority,
) -> None:
    owned_task_row_ids = {task_row.id for _, task_row in task_rows}
    local_runners: dict[UUID, tuple[datetime, asyncio.Task[dict[str, dict[str, Any] | None]]]] = {}
    retired_evaluation_row_ids: set[UUID] = set()
    tracked_tasks: dict[str, TrackedTask] = {}
    coordinator_done = asyncio.Event()
    monitor = TaskMonitor(
        benchmark_id,
        tracked_tasks,
        org,
        limiter=None,
        coordinator_done=coordinator_done,
        authority=authority,
    )
    monitor_task = asyncio.create_task(monitor.track_tasks())
    cancellation_attempts: dict[UUID, datetime] | None = None

    def launch(task_row: Task) -> None:
        tracked_task = TrackedTask(
            process_task(
                task_row,
                start_benchmark_request,
                benchmark_service,
                benchmark_id,
                task_row.task_id,
                runtime,
                org,
                sandbox_provider_config=sandbox_provider_config,
                sandbox_provider=sandbox_provider,
                creation_semaphore=creation_semaphore,
                queue_context=queue_context,
                authority=authority,
                started_by_email=started_by_email,
            ),
            org,
            authority,
            task_row.started_at,
        )
        tracked_tasks[task_row.task_id] = tracked_task
        runner = asyncio.create_task(tracked_task.run(None, task_row))
        local_runners[task_row.id] = (task_row.started_at, runner)

    try:
        while True:
            completed_task_row_ids: set[UUID] = set()
            for task_row_id, (_attempt, runner) in list(local_runners.items()):
                if runner.done():
                    await runner
                    del local_runners[task_row_id]
                    completed_task_row_ids.add(task_row_id)

            benchmark_row, task_states, reserved_task_row_ids = await database_call(
                lambda: _read_queue_state(benchmark_id, org, authority, owned_task_row_ids)
            )
            benchmark_task_rows = [task_row for task_row, _requested_at in task_states]
            current_rows = [task_row for task_row in benchmark_task_rows if task_row.id in owned_task_row_ids]
            readiness_requests = {
                task_row.id: requested_at
                for task_row, requested_at in task_states
                if task_row.id in owned_task_row_ids and requested_at is not None
            }
            queue_context.admission_signals.publish(current_rows, readiness_requests)

            current_by_id = {task_row.id: task_row for task_row in current_rows}
            retired_evaluation_row_ids.update(
                task_row_id
                for task_row_id in completed_task_row_ids
                if (completed_row := current_by_id.get(task_row_id)) is not None
                and completed_row.status == TaskStatus.EVALUATING
                and completed_row.eval_resume_state is not None
            )
            active_task_row_ids = {
                task_row_id
                for task_row_id, (attempt, _runner) in local_runners.items()
                if (current_task := current_by_id.get(task_row_id)) is not None and current_task.started_at == attempt
            }
            used_slots = sum(
                task_row.status in (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS)
                or (
                    task_row.status == TaskStatus.EVALUATING
                    and (
                        task_row.eval_resume_state is None
                        or task_row.id not in owned_task_row_ids
                        or task_row.id in active_task_row_ids
                    )
                )
                for task_row in benchmark_task_rows
            )
            available_slots = max(benchmark_row.arguments.concurrency - used_slots, 0)

            if benchmark_row.status in TERMINAL_BENCHMARK_STATUSES:
                break

            if benchmark_row.status == BenchmarkStatus.IN_PROGRESS and available_slots:
                evaluation_candidates = sorted(
                    (
                        task_row
                        for task_row in current_rows
                        if task_row.status == TaskStatus.EVALUATING
                        and task_row.eval_resume_state is not None
                        and task_row.id not in local_runners
                        and task_row.id not in retired_evaluation_row_ids
                    ),
                    key=lambda task_row: (task_row.started_at, task_row.id),
                )
                for task_row in evaluation_candidates[:available_slots]:
                    launch(task_row)
                available_slots -= min(len(evaluation_candidates), available_slots)

            if benchmark_row.status == BenchmarkStatus.IN_PROGRESS:
                pending_candidates = [
                    task_row
                    for task_row in current_rows
                    if task_row.status == TaskStatus.PENDING
                    and task_row.id not in local_runners
                    and task_row.id not in reserved_task_row_ids
                ]
                pending_candidates.sort(key=lambda task_row: (task_row.started_at, task_row.id))
                for task_row in pending_candidates:
                    launch(task_row)

            actionable_rows_remain = any(
                task_row.status in (TaskStatus.PENDING, TaskStatus.BUILDING)
                or (
                    task_row.status == TaskStatus.EVALUATING
                    and task_row.eval_resume_state is not None
                    and task_row.id not in retired_evaluation_row_ids
                )
                for task_row in current_rows
            )
            if not local_runners and not actionable_rows_remain:
                break

            await asyncio.sleep(queue_context.poll_interval_seconds)
    except asyncio.CancelledError:
        cancellation_attempts = {task_row_id: attempt for task_row_id, (attempt, _runner) in local_runners.items()}
        raise
    finally:
        coordinator_done.set()
        for _attempt, runner in local_runners.values():
            runner.cancel()
        await gather(
            *(runner for _attempt, runner in local_runners.values()),
            return_exceptions=True,
        )
        await monitor_task
        if cancellation_attempts is not None:
            owned_attempts = cancellation_attempts
            await database_call(lambda: record_cancellation(owned_attempts))


def _read_queue_state(
    benchmark_id: UUID, org: Org, authority: ExecutionAuthority, owned_task_row_ids: set[UUID]
) -> tuple[Benchmark, Sequence[tuple[Task, datetime | None]], set[UUID]]:
    with Session(bind=engine) as session:
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
        repository = SchedulerRepository(session)
        task_states = repository.task_readiness(org.id, benchmark_id, authority.dispatch_id)
        reserved_task_row_ids = (
            repository.reserved_task_row_ids(owned_task_row_ids)
            if benchmark_row.status == BenchmarkStatus.IN_PROGRESS
            else set[UUID]()
        )
        return benchmark_row, task_states, reserved_task_row_ids


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


def create_task_rows(
    verified_task_ids: list[str],
    benchmark_row: Benchmark,
    session: Session,
    org: Org,
    *,
    authority: ExecutionAuthority,
) -> Sequence[tuple[str, Task]]:
    lock_execution_authority(session, authority)

    repository = BenchmarkRepository(session)
    existing_task_ids = repository.task_ids(org.id, benchmark_row.id, verified_task_ids)

    repository.add_tasks(
        org.id, benchmark_row.id, [task_id for task_id in verified_task_ids if task_id not in existing_task_ids]
    )

    session.commit()
    session.expire_all()

    return _load_verified_task_rows(
        verified_task_ids,
        benchmark_row,
        session,
        org,
        statuses=[TaskStatus.PENDING, TaskStatus.EVALUATING],
    )


def _load_verified_task_rows(
    verified_task_ids: Sequence[str],
    benchmark_row: Benchmark,
    session: Session,
    org: Org,
    *,
    statuses: Sequence[TaskStatus] | None = None,
) -> Sequence[tuple[str, Task]]:
    task_rows_by_id = {
        task_row.task_id: task_row
        for task_row in BenchmarkRepository(session).list_tasks(
            org.id, benchmark_row.id, task_ids=verified_task_ids, statuses=statuses
        )
    }
    return [(task_id, task_rows_by_id[task_id]) for task_id in verified_task_ids if task_id in task_rows_by_id]


@dataclass(frozen=True)
class _QueuedExecution:
    request: RunExecutionRequest
    benchmark_id: UUID
    verified_task_ids: list[str]
    context_version: int | None = None


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
        request=context.start_benchmark_request,
        benchmark_id=context.benchmark_id,
        verified_task_ids=context.verified_task_ids,
        context_version=context.version,
    )


def _queued_benchmark_id(
    benchmark_id_str: str | None,
    execution_context_json: dict[str, Any] | None,
) -> UUID:
    raw_benchmark_id = (
        execution_context_json.get("benchmark_id") if execution_context_json is not None else benchmark_id_str
    )
    try:
        return UUID(str(raw_benchmark_id))
    except (TypeError, ValueError):
        raise ValueError("Queued benchmark request has no valid benchmark ID.") from None


@asynccontextmanager
async def hold_dispatch_authority(
    authority: ExecutionAuthority,
) -> AsyncGenerator[None, None]:
    session = Session(bind=engine)
    try:
        await database_call(lambda: lock_execution_authority(session, authority, require_in_progress=False))
        yield
    finally:
        await database_call(session.close)


async def process_benchmark(
    start_benchmark_request_json: dict[str, Any] | None = None,
    benchmark_id_str: str | None = None,
    verified_task_ids: list[str] | None = None,
    execution_context_json: dict[str, Any] | None = None,
    *,
    executor_dispatch_id: str,
) -> None:
    async with AsyncExitStack() as runtime_stack:
        await _process_benchmark(
            start_benchmark_request_json,
            benchmark_id_str,
            verified_task_ids,
            execution_context_json,
            executor_dispatch_id=executor_dispatch_id,
            runtime_stack=runtime_stack,
        )


def _load_run(benchmark_id: UUID) -> tuple[Benchmark, Org]:
    with Session(bind=engine) as session:
        benchmark_row = BenchmarkRepository(session).get_by_id(benchmark_id)
        if not benchmark_row:
            raise TrackerServiceError(f"Run with id {benchmark_id} not found")
        org = OrganizationRepository(session).get(benchmark_row.org_id)
        if org is None:
            raise TrackerServiceError(f"Org with id {benchmark_row.org_id} not found")
        return benchmark_row, org


async def _process_benchmark(
    start_benchmark_request_json: dict[str, Any] | None,
    benchmark_id_str: str | None,
    verified_task_ids: list[str] | None,
    execution_context_json: dict[str, Any] | None,
    *,
    executor_dispatch_id: str,
    runtime_stack: AsyncExitStack,
) -> None:
    benchmark_id = _queued_benchmark_id(
        benchmark_id_str,
        execution_context_json,
    )
    try:
        authority = ExecutionAuthority(
            benchmark_id=benchmark_id,
            dispatch_id=UUID(executor_dispatch_id),
        )
    except ValueError as error:
        raise TrackerServiceError("Executor dispatch authority is invalid") from error

    benchmark_row, org = await database_call(lambda: _load_run(benchmark_id))
    if benchmark_row.status in TERMINAL_BENCHMARK_STATUSES:
        return
    queued_run = benchmark_row.arguments.queue_pool_id is not None
    started_by_email = benchmark_row.started_by_email

    finalization_deferred = False
    post_task_finalization = False
    queued_cancellation_recorded = False
    queue_context: SandboxQueueContext | None = None
    task_rows: Sequence[tuple[str, Task]] = ()
    run_task_rows: Sequence[tuple[str, Task]] = ()
    limiter: ResizableLimiter | None = None

    def record_queued_cancellation(owned_attempts: dict[UUID, datetime]) -> None:
        nonlocal queued_cancellation_recorded
        with Session(bind=engine) as session:
            queued_cancellation_recorded = _commit_queued_cancellation(
                benchmark_id,
                session,
                org,
                owned_attempts,
                "Run was interrupted",
                authority=authority,
            )

    try:
        execution = _parse_queued_execution(
            start_benchmark_request_json,
            benchmark_id_str,
            verified_task_ids,
            execution_context_json,
        )
        start_benchmark_request = execution.request
        verified_task_ids = execution.verified_task_ids

        sentry_sdk.set_tag("benchmark_name", start_benchmark_request.benchmark_name)
        sentry_sdk.set_tag("agent_name", start_benchmark_request.contract.name)
        with observability_span(
            "run.started",
            benchmark_id=str(benchmark_id),
            benchmark_name=start_benchmark_request.benchmark_name,
            agent_name=start_benchmark_request.contract.name,
            task_count=len(verified_task_ids),
            executor_dispatch_id=executor_dispatch_id,
        ):
            pass

        if start_benchmark_request.resolved_dataset_version != benchmark_row.arguments.dataset_version or (
            benchmark_row.arguments.dataset_version is not None
            and start_benchmark_request.dataset != benchmark_row.arguments.dataset
        ):
            raise TrackerServiceError("Queued dataset selection does not match the saved run")
        if benchmark_row.arguments.dataset_version is not None and (
            start_benchmark_request.custom_benchmark_service != benchmark_row.custom_benchmark_service
        ):
            raise TrackerServiceError("Queued benchmark service does not match the pinned run")

        if start_benchmark_request.custom_benchmark_service is not None:
            validate_custom_service_destination(
                start_benchmark_request.custom_benchmark_service,
                org_name=org.name,
                auth_required=AUTH_REQUIRED,
            )

        runtime = await get_execution_runtime(
            start_benchmark_request,
            benchmark_row,
            org,
            context_version=execution.context_version,
        )
        benchmark_service = await runtime_stack.enter_async_context(start_benchmark_request.benchmark_service)
        if classify_benchmark_service_destination(
            benchmark_row.name,
            start_benchmark_request.custom_benchmark_service,
        ).value == BenchmarkServiceDestination.HOSTED.value and await asyncio.to_thread(
            was_woken, engine, benchmark_row.name
        ):
            await wait_for_benchmark_service(benchmark_service)
        sandbox_provider_config = await runtime.get_sandbox_provider_config()
        runtime_stack.enter_context(sandbox_provider_scope(sandbox_provider_config.type))

        sandbox_provider = sandbox_provider_config.create_provider()
        runtime_stack.push_async_callback(lambda: finish_cleanup(asyncio.create_task(sandbox_provider.close())))

        run_task_ids = verified_task_ids
        if queued_run:

            def create_queued_rows() -> tuple[Benchmark, Sequence[tuple[str, Task]], Sequence[tuple[str, Task]]]:
                with Session(bind=engine) as session:
                    benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
                    task_rows = create_task_rows(run_task_ids, benchmark_row, session, org, authority=authority)
                    return (
                        benchmark_row,
                        task_rows,
                        _load_verified_task_rows(run_task_ids, benchmark_row, session, org),
                    )

            benchmark_row, task_rows, run_task_rows = await database_call(create_queued_rows)
            queued_pool_id = benchmark_row.arguments.queue_pool_id
            assert queued_pool_id is not None

            try:
                queue_context = create_queue_context(
                    engine=engine,
                    provider=sandbox_provider,
                )
            except Exception as error:
                logger.warning("Sandbox provider setup failed (%s)", type(error).__name__)
                raise TrackerServiceError("Sandbox provider configuration is unavailable") from error
            if not queue_context.serves_queue(queued_pool_id):
                raise TrackerServiceError("Configured sandbox provider does not match the run's queued provider pool")
            queue_context = queue_context.for_queue(queued_pool_id)

        if not queued_run:

            def create_rows() -> tuple[Benchmark, Sequence[tuple[str, Task]]]:
                with Session(bind=engine) as session:
                    benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
                    return benchmark_row, create_task_rows(
                        run_task_ids, benchmark_row, session, org, authority=authority
                    )

            benchmark_row, task_rows = await database_call(create_rows)
            run_task_rows = sorted(task_rows, key=lambda item: item[1].status != TaskStatus.EVALUATING)
            limiter = ResizableLimiter(benchmark_row.arguments.concurrency)

        task_row_ids: set[str] = {task_id for task_id, _ in run_task_rows}
        missing_task_ids: list[str] = [task_id for task_id in verified_task_ids if task_id not in task_row_ids]
        if missing_task_ids:
            raise TrackerServiceError(
                f"Race condition occurred when resuming run {benchmark_id}. Missing task ids: {', '.join(missing_task_ids)}"
            )

        creation_semaphore = Semaphore(_SANDBOX_CREATION_CAP)

        if queue_context is not None:
            await _run_queued_tasks(
                benchmark_id=benchmark_id,
                task_rows=run_task_rows,
                start_benchmark_request=start_benchmark_request,
                benchmark_service=benchmark_service,
                runtime=runtime,
                org=org,
                sandbox_provider_config=sandbox_provider_config,
                sandbox_provider=sandbox_provider,
                creation_semaphore=creation_semaphore,
                queue_context=queue_context,
                started_by_email=started_by_email,
                record_cancellation=record_queued_cancellation,
                authority=authority,
            )
        else:
            assert limiter is not None
            tracked_tasks: dict[str, TrackedTask] = {
                task_id: TrackedTask(
                    process_task(
                        task_row,
                        start_benchmark_request,
                        benchmark_service,
                        benchmark_id,
                        task_id,
                        runtime,
                        org,
                        sandbox_provider_config=sandbox_provider_config,
                        sandbox_provider=sandbox_provider,
                        creation_semaphore=creation_semaphore,
                        authority=authority,
                        started_by_email=started_by_email,
                    ),
                    org,
                    authority,
                    task_row.started_at,
                )
                for task_id, task_row in run_task_rows
            }
            monitor = TaskMonitor(
                benchmark_id,
                tracked_tasks,
                org,
                limiter=limiter,
                authority=authority,
            )
            monitor_task = asyncio.create_task(monitor.track_tasks())

            await gather(*[tracked_tasks[task_id].run(limiter, task_row) for task_id, task_row in run_task_rows])
            await monitor_task

        post_task_finalization = True
        task_rows = ()

        def claim_final_score() -> tuple[dict[str, dict[str, Any] | None], TaskFingerprint] | None:
            with Session(bind=engine) as session:
                benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
                if benchmark_row.status in TERMINAL_BENCHMARK_STATUSES:
                    return None
                lock_execution_authority(session, authority)
                if BenchmarkRepository(session).has_tasks(org.id, benchmark_row.id, RUNNABLE_TASK_STATUSES):
                    return None
                terminalize_active_dispatches(
                    session,
                    benchmark_id,
                    except_dispatch_id=authority.dispatch_id,
                )
                final_score_state = fetch_final_score_state(
                    session,
                    benchmark_row,
                    org,
                    for_update=True,
                )
                session.commit()
                return final_score_state

        final_score_state = await database_call(claim_final_score)
        if final_score_state is None:
            finalization_deferred = True
            return
        evaluation_results, task_fingerprint = final_score_state

        if not any(result is not None for result in evaluation_results.values()):
            finalization_deferred = await finalize_all_error_run(benchmark_id, org, authority=authority)
            return

        final_score_response = await benchmark_service.final_score(
            evaluation_results=evaluation_results, dataset=start_benchmark_request.dataset
        )

        def runnable_tasks_remain() -> bool:
            with Session(bind=engine) as session:
                benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
                lock_execution_authority(session, authority)
                return BenchmarkRepository(session).has_tasks(org.id, benchmark_row.id, RUNNABLE_TASK_STATUSES)

        if await database_call(runnable_tasks_remain):
            finalization_deferred = True
            return
        final_evaluation_row = FinalEvaluation(
            org_id=org.id,
            benchmark=benchmark_id,
            final_score=final_score_response.final_score,
            properties=final_score_response.metadata,
        )

        def commit_final_evaluation() -> FinalViewResponse | None:
            with Session(bind=engine) as session:
                benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
                if benchmark_row.status in TERMINAL_BENCHMARK_STATUSES or BenchmarkRepository(session).has_tasks(
                    org.id, benchmark_row.id, RUNNABLE_TASK_STATUSES
                ):
                    return None
                lock_execution_authority(session, authority)
                _current_results, current_fingerprint = fetch_final_score_state(
                    session,
                    benchmark_row,
                    org,
                    for_update=True,
                )
                if current_fingerprint != task_fingerprint:
                    return None

                BenchmarkRepository(session).replace_final_evaluation(benchmark_row, final_evaluation_row)
                set_benchmark_final_status(benchmark_row, session, org, authority=authority)
                return create_final_view(benchmark_row, session, org)

        final_view = await database_call(commit_final_evaluation)
        if final_view is None:
            finalization_deferred = True
            return

        async with hold_dispatch_authority(authority):
            await upload_final_view(final_view, runtime.objects)

        async with hold_dispatch_authority(authority):
            await runtime.run_completion_callback(final_view)

    except asyncio.CancelledError:
        if queued_run and not post_task_finalization:
            finalization_deferred = not queued_cancellation_recorded
        else:
            finalization_deferred = not await database_call(
                lambda: _commit_run_error(
                    benchmark_id,
                    org,
                    "Run was interrupted",
                    producer="tracker",
                    operation="process_benchmark",
                    error_type=asyncio.CancelledError.__name__,
                    category=FailureCategory.CANCELLED,
                    authority=authority,
                    task_ids=verified_task_ids,
                )
            )
        raise
    except ExecutionAuthorityRevoked:
        finalization_deferred = True
    except BenchmarkServiceUnauthenticatedError as e:
        _capture_run_error(
            e,
            benchmark_id,
            producer="benchmark_service",
            operation="authenticate",
            category=FailureCategory.BENCHMARK_SERVICE,
            cause_code="authentication_failed",
        )
        error_message = f"{str(e)}\n{traceback.format_exc()}"
        logger.warning(error_message)
        error_type = type(e).__name__
        await database_call(
            lambda: _commit_run_error(
                benchmark_id,
                org,
                error_message,
                producer="benchmark_service",
                operation="authenticate",
                error_type=error_type,
                category=FailureCategory.BENCHMARK_SERVICE,
                cause_code="authentication_failed",
                authority=authority,
                task_ids=verified_task_ids,
            )
        )
    except BenchmarkServiceError as e:
        _capture_run_error(
            e,
            benchmark_id,
            producer="benchmark_service",
            operation="process_benchmark",
            category=FailureCategory.BENCHMARK_SERVICE,
        )
        error_message = str(e)
        error_type = type(e).__name__
        await database_call(
            lambda: _commit_run_error(
                benchmark_id,
                org,
                error_message,
                producer="benchmark_service",
                operation="process_benchmark",
                error_type=error_type,
                category=FailureCategory.BENCHMARK_SERVICE,
                authority=authority,
                task_ids=verified_task_ids,
            )
        )
    except Exception as e:
        _capture_run_error(
            e, benchmark_id, producer="tracker", operation="process_benchmark", category=classify_failure(e)
        )
        if isinstance(e, TrackerServiceError) and not e.include_cause_in_task_error:
            error_message = f"{type(e).__name__}: {e}"
        else:
            error_message = f"{str(e)}\n{traceback.format_exc()}"
        error_type = type(e).__name__
        category = classify_failure(e)
        await database_call(
            lambda: _commit_run_error(
                benchmark_id,
                org,
                error_message,
                producer="tracker",
                operation="process_benchmark",
                error_type=error_type,
                category=category,
                authority=authority,
                task_ids=verified_task_ids,
            )
        )
    finally:
        if not finalization_deferred:
            await database_call(lambda: _cleanup_run_errors(benchmark_id, org, authority, verified_task_ids))


def _commit_run_error(
    benchmark_id: UUID,
    org: Org,
    error_message: str,
    *,
    producer: str,
    operation: str,
    error_type: str,
    category: FailureCategory,
    authority: ExecutionAuthority,
    cause_code: str | None = None,
    task_ids: list[str] | None = None,
) -> bool:
    with Session(bind=engine) as session:
        benchmark_row = fetch_benchmark_row(benchmark_id, session, org)
        return commit_benchmark_error(
            benchmark_row,
            session,
            error_message,
            producer=producer,
            operation=operation,
            error_type=error_type,
            category=category,
            authority=authority,
            cause_code=cause_code,
            task_ids=task_ids,
        )


def _cleanup_run_errors(
    benchmark_id: UUID, org: Org, authority: ExecutionAuthority, task_ids: list[str] | None
) -> None:
    with Session(bind=engine) as session:
        catch_errors_during_cleanup(benchmark_id, session, org, authority=authority, task_ids=task_ids)


def _commit_queued_cancellation(
    benchmark_id: UUID,
    session: Session,
    org: Org,
    owned_attempts: dict[UUID, datetime],
    error_message: str,
    *,
    authority: ExecutionAuthority,
) -> bool:
    benchmark_row = fetch_benchmark_row(benchmark_id, session, org, for_update=True)
    try:
        benchmark_row = lock_execution_authority(session, authority)
    except ExecutionAuthorityRevoked:
        session.rollback()
        return False
    if benchmark_row.status in TERMINAL_BENCHMARK_STATUSES:
        return False

    transitioned_task = False
    if owned_attempts:
        current_rows = BenchmarkRepository(session).lock_tasks(org.id, benchmark_id, row_ids=list(owned_attempts))
        for task_row in current_rows:
            irrecoverable = task_row.status == TaskStatus.IN_PROGRESS or (
                task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is None
            )
            if task_row.started_at != owned_attempts[task_row.id] or not irrecoverable:
                continue
            BenchmarkRepository(session).save(
                ErrorResult(
                    org_id=org.id,
                    task=task_row.id,
                    error_message=error_message,
                    producer="tracker",
                    operation="process_benchmark",
                    error_type=asyncio.CancelledError.__name__,
                    category=FailureCategory.CANCELLED,
                )
            )
            task_row.status = TaskStatus.ERROR
            transitioned_task = True

    if not transitioned_task or BenchmarkRepository(session).has_tasks(
        org.id, benchmark_row.id, RUNNABLE_TASK_STATUSES
    ):
        session.commit()
        return False

    if benchmark_row.docent_reading_status == DocentReadingStatus.RUNNING:
        benchmark_row.docent_reading_status = DocentReadingStatus.ERROR
    benchmark_row.status = BenchmarkStatus.ERROR
    benchmark_row.error_message = error_message
    session.commit()
    return True
