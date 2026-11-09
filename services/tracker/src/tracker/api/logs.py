from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID
from typing import Annotated, TypedDict

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import Connection, Engine
from sqlalchemy.exc import OperationalError
from sqlmodel import Session

from tracker.api.dependencies import (
    RunRuntimeDependency,
    RunBenchmarkDependency,
    load_task_for_benchmark_or_404,
)
from tracker.auth import get_current_org
from tracker.database.models import Benchmark, Org, TaskStatus
from tracker.database.session import database_call, get_session
from tracker.observability.sentry import capture_exception
from tracker.repositories.benchmark import BenchmarkRepository, BenchmarkRepositoryDependency
from tracker.runtime.logs import (
    LogProvider,
    LogProviderError,
    RunLogReference,
    RunTaskLogReference,
    TaskLogReference,
)
from tracker.types import (
    LogEventResponse,
    LogPageResponse,
    MonitoredRunResponse,
    MonitoredRunsResponse,
    MonitorTaskResponse,
    MonitorTasksResponse,
)
from tracker.services.tasks.execute import task_attempt_id

logger = logging.getLogger(__name__)


class LogFilters(TypedDict):
    query: str | None
    start_time: datetime | None
    end_time: datetime | None


def _log_filters(
    _benchmark: RunBenchmarkDependency,
    query: str | None = Query(default=None, min_length=1),
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> LogFilters:
    if query is not None and not query.strip():
        raise HTTPException(status_code=422, detail="query must not be blank")
    for name, value in (("start_time", start_time), ("end_time", end_time)):
        if value is not None and value.utcoffset() is None:
            raise HTTPException(status_code=422, detail=f"{name} must include a timezone offset")
    if start_time is not None and end_time is not None and end_time <= start_time:
        raise HTTPException(status_code=422, detail="end_time must be later than start_time")
    return LogFilters(query=query, start_time=start_time, end_time=end_time)


LogFiltersDependency = Annotated[LogFilters, Depends(_log_filters)]

router = APIRouter(prefix="/benchmarks")
monitor_router = APIRouter(prefix="/monitor")


def _task_reference(
    benchmark: RunBenchmarkDependency,
    repository: BenchmarkRepositoryDependency,
    task_id: str = Query(min_length=1),
    expected_attempt_id: str | None = Query(default=None, min_length=1),
    org: Org = Depends(get_current_org),
) -> TaskLogReference:
    task = load_task_for_benchmark_or_404(benchmark, task_id, org, repository)
    if expected_attempt_id is not None and task_attempt_id(task.id, task.started_at) != expected_attempt_id:
        raise HTTPException(status_code=409, detail="Task attempt has changed")
    run_tasks = _run_tasks(benchmark, org, repository)
    return TaskLogReference(
        run_id=benchmark.id,
        task_id=task.task_id,
        started_at=task.started_at,
        siblings=tuple(run_task for run_task in run_tasks if run_task.task_id != task.task_id),
        strict_attempt=expected_attempt_id is not None,
    )


TaskLogReferenceDependency = Annotated[TaskLogReference, Depends(_task_reference)]


def _run_tasks(
    benchmark: Benchmark,
    org: Org,
    repository: BenchmarkRepository,
) -> tuple[RunTaskLogReference, ...]:
    tasks = repository.list_tasks(org.id, benchmark.id)
    return tuple(RunTaskLogReference(task_id=task.task_id, started_at=task.started_at) for task in tasks)


def _run_reference(
    benchmark: RunBenchmarkDependency,
    repository: BenchmarkRepositoryDependency,
    org: Org = Depends(get_current_org),
) -> RunLogReference:
    return RunLogReference(
        run_id=benchmark.id,
        tasks=_run_tasks(benchmark, org, repository),
    )


def _log_reference(
    repository: BenchmarkRepositoryDependency,
    benchmark: RunBenchmarkDependency,
    task_id: str | None = Query(default=None, min_length=1),
    expected_attempt_id: str | None = Query(default=None, min_length=1),
    org: Org = Depends(get_current_org),
) -> RunLogReference | TaskLogReference:
    if task_id is not None:
        return _task_reference(benchmark, repository, task_id, expected_attempt_id, org)
    if expected_attempt_id is not None:
        raise HTTPException(status_code=422, detail="expected_attempt_id requires task_id")
    return _run_reference(benchmark, repository, org)


LogReferenceDependency = Annotated[RunLogReference | TaskLogReference, Depends(_log_reference)]


_FINISHED_TASK_LOG_GRACE = timedelta(seconds=10)
_FINISHED_TASK_CHECK_INTERVAL = 5.0
_END_CHECK_MAX_FAILURES = 6
_TERMINAL_TASK_STATUSES = (TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED)


@dataclass(frozen=True)
class _AttemptEnd:
    ended_at: datetime | None


def _attempt_end(bind: Engine | Connection, org_id: UUID, reference: TaskLogReference) -> _AttemptEnd | None:
    with Session(bind) as session:
        task = BenchmarkRepository(session).get_task(org_id, reference.run_id, reference.task_id)
    if task is None:
        return None
    if task.started_at != reference.started_at:
        ended_at = task.started_at
    elif task.status in _TERMINAL_TASK_STATUSES:
        ended_at = task.finished_at
    else:
        return None
    if ended_at is not None and ended_at.tzinfo is None:
        ended_at = ended_at.replace(tzinfo=timezone.utc)
    return _AttemptEnd(ended_at=ended_at)


async def _stream_events(
    log_provider: LogProvider,
    reference: TaskLogReference,
    *,
    query: str | None,
    start_time: datetime | None,
    end_time: datetime | None,
    attempt_end: Callable[[], Awaitable[_AttemptEnd | None]] | None = None,
    keep_alive_interval: float = 15.0,
    finished_check_interval: float | None = None,
) -> AsyncGenerator[str, None]:
    yield ": connected\n\n"
    iterator = log_provider.stream_task(
        reference,
        query=query,
        start_time=start_time,
        end_time=end_time,
    ).__aiter__()
    pending_event = asyncio.ensure_future(anext(iterator))
    if finished_check_interval is None:
        finished_check_interval = _FINISHED_TASK_CHECK_INTERVAL
    idle_timeout = keep_alive_interval if attempt_end is None else min(keep_alive_interval, finished_check_interval)
    loop = asyncio.get_running_loop()
    last_sent = loop.time()
    last_event_at = datetime.min.replace(tzinfo=timezone.utc)
    ended_at: datetime | None = None
    end_check_failures = 0
    try:
        while True:
            done, _ = await asyncio.wait({pending_event}, timeout=idle_timeout)
            if not done:
                if attempt_end is not None:
                    now = datetime.now(timezone.utc)
                    if ended_at is None:
                        try:
                            end = await attempt_end()
                        except OperationalError as error:
                            end_check_failures += 1
                            if end_check_failures >= _END_CHECK_MAX_FAILURES:
                                capture_exception(error)
                                payload = json.dumps({"detail": "Task status is unavailable"})
                                yield f"event: error\ndata: {payload}\n\n"
                                return
                            logger.warning(
                                "Task log stream end check failed, retrying at the next check", exc_info=True
                            )
                        else:
                            end_check_failures = 0
                            if end is not None:
                                ended_at = end.ended_at or now
                    if ended_at is not None and now - max(ended_at, last_event_at) > _FINISHED_TASK_LOG_GRACE:
                        break
                if loop.time() - last_sent >= keep_alive_interval:
                    last_sent = loop.time()
                    yield ": keep-alive\n\n"
                continue
            last_sent = loop.time()
            last_event_at = datetime.now(timezone.utc)
            try:
                event = pending_event.result()
            except StopAsyncIteration:
                break
            pending_event = asyncio.ensure_future(anext(iterator))
            payload = LogEventResponse.model_validate(event, from_attributes=True).model_dump_json()
            yield f"event: log\ndata: {payload}\n\n"
    except LogProviderError as error:
        payload = json.dumps({"detail": str(error)})
        yield f"event: error\ndata: {payload}\n\n"
        return
    finally:
        pending_event.cancel()
        with suppress(asyncio.CancelledError, LogProviderError, StopAsyncIteration):
            await pending_event
    yield "event: end\ndata: {}\n\n"


@monitor_router.get(
    "/runs",
    response_model=MonitoredRunsResponse,
    description="List in-progress runs that requested monitoring, so the monitor pulls enrollment instead of being called.",
)
def get_monitored_runs(
    repository: BenchmarkRepositoryDependency,
    org: Org = Depends(get_current_org),
) -> MonitoredRunsResponse:
    runs = repository.list_in_progress(org.id)
    return MonitoredRunsResponse(
        runs=[
            MonitoredRunResponse(
                benchmark_id=run.id,
                benchmark_name=run.name,
                agent_name=run.arguments.contract.name,
                **(run.arguments.monitoring_rules.model_dump() if run.arguments.monitoring_rules else {}),
            )
            for run in runs
            if run.arguments.monitoring is not None
        ]
    )


@router.get(
    "/{benchmark_id}/monitor/tasks",
    response_model=MonitorTasksResponse,
    description="Return authoritative current attempt identities without provider details.",
)
def get_monitor_tasks(
    repository: BenchmarkRepositoryDependency,
    benchmark: RunBenchmarkDependency,
    org: Org = Depends(get_current_org),
) -> MonitorTasksResponse:
    tasks = repository.list_tasks(org.id, benchmark.id)
    return MonitorTasksResponse(
        benchmark_id=benchmark.id,
        status=benchmark.status,
        tasks=[
            MonitorTaskResponse(
                task_id=task.task_id,
                task_row_id=task.id,
                attempt_id=task_attempt_id(task.id, task.started_at),
                started_at=task.started_at,
                status=task.status,
            )
            for task in tasks
        ],
    )


@router.get(
    "/{benchmark_id}/logs",
    response_model=LogPageResponse,
    description="Return one page of logs for a run or one of its tasks.",
)
async def get_logs(
    filters: LogFiltersDependency,
    reference: LogReferenceDependency,
    runtime: RunRuntimeDependency,
    cursor: str | None = None,
    limit: int = Query(default=1_000, ge=1, le=10_000),
) -> LogPageResponse:
    try:
        page = await runtime.log_reader.fetch(reference, **filters, cursor=cursor, limit=limit)
    except LogProviderError as error:
        raise HTTPException(status_code=502, detail=str(error)) from error
    if isinstance(reference, TaskLogReference) and reference.strict_attempt and not page.available:
        raise HTTPException(status_code=503, detail="Task log source is unavailable")
    return LogPageResponse.model_validate(page, from_attributes=True)


@router.get("/{benchmark_id}/logs/stream", description="Stream a task's current log stream as server-sent events.")
async def stream_task_logs(
    filters: LogFiltersDependency,
    reference: TaskLogReferenceDependency,
    benchmark: RunBenchmarkDependency,
    runtime: RunRuntimeDependency,
    session: Session = Depends(get_session),
) -> StreamingResponse:
    if reference.strict_attempt:
        try:
            page = await runtime.log_reader.fetch(reference, **filters, limit=1)
        except LogProviderError as error:
            raise HTTPException(status_code=502, detail=str(error)) from error
        if not page.available:
            raise HTTPException(status_code=503, detail="Task log source is unavailable")
    bind = session.get_bind()
    session.close()
    org_id = benchmark.org_id

    async def attempt_end() -> _AttemptEnd | None:
        return await database_call(lambda: _attempt_end(bind, org_id, reference))

    return StreamingResponse(
        _stream_events(runtime.log_reader, reference, **filters, attempt_end=attempt_end),
        media_type="text/event-stream",
    )
