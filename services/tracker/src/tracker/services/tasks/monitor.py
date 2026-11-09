import asyncio
import traceback
from collections.abc import Coroutine
from datetime import datetime
from enum import Enum
from types import TracebackType
from typing import Any
from uuid import UUID
import logfire
import sentry_sdk
from sqlalchemy.exc import OperationalError
from sqlmodel import Session
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    Org,
    Task,
    TaskStatus,
)
from tracker.database.session import database_call, engine
from tracker.exceptions import (
    ExecutionAuthorityRevoked,
)
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.logging import get_logger
from tracker.logging.events import LogEvent
from tracker.observability import TaskOutcome, record_task_outcome
from tracker.observability.sentry import capture_exception, task_scope
from tracker.services.failures import (
    describe_failure,
)
from tracker.services.benchmarks import fetch_benchmark_row, fetch_task_row
from tracker.services.tasks.status import commit_task_error, exception_message, normalized_attempt_time

logger = get_logger(__name__)


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
    _org: Org
    _attempt_started_at: datetime
    _authority: ExecutionAuthority

    def __init__(
        self,
        coro: Coroutine[Any, Any, Any],
        org: Org,
        authority: ExecutionAuthority,
        started_at: datetime,
    ):
        self._coro = coro
        self._org = org
        self._authority = authority
        self._attempt_started_at = normalized_attempt_time(started_at)
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
        task_row: Task,
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
            except Exception as e:
                failure = describe_failure(e, exception_message(e), phase=getattr(e, "phase", None))
                error_message = f"Task error was not handled: {failure.message}\n{traceback.format_exc()}"
                logger.error(
                    error_message,
                    extra={"event": LogEvent.TASK_UNHANDLED_ERROR, "failure_category": failure.category.value},
                )
                logfire.exception("tracked_task_run failed")
                with sentry_sdk.new_scope() as scope:
                    scope.set_tag("failure_category", failure.category.value)
                    capture_exception(e)

                def commit_unhandled_error() -> bool:
                    with Session(bind=engine) as session:
                        task = fetch_task_row(task_row.id, session, self._org)
                        return commit_task_error(
                            task,
                            session,
                            error_message,
                            producer=failure.producer,
                            operation=failure.operation,
                            error_type=failure.error_type,
                            category=failure.category,
                            cause_code=failure.cause_code,
                            phase=failure.phase,
                            transient=failure.transient,
                            expected_started_at=task_row.started_at,
                            authority=self._authority,
                        )

                if await database_call(commit_unhandled_error):
                    record_task_outcome(TaskOutcome.UNHANDLED_ERROR, {"failure_category": failure.category.value})

                return {task_row.task_id: None}
            finally:
                self._status = TrackedTaskStatus.DONE


class TaskMonitor:
    _benchmark_id: UUID
    _task_tracking: dict[str, TrackedTask]
    _org: Org
    _limiter: ResizableLimiter | None
    _coordinator_done: asyncio.Event | None
    _cancellation_requested: set[str]
    _authority: ExecutionAuthority
    _TRACK_INTERVAL: int = 2

    def __init__(
        self,
        benchmark_id: UUID,
        task_tracking: dict[str, TrackedTask],
        org: Org,
        limiter: ResizableLimiter | None,
        *,
        authority: ExecutionAuthority,
        coordinator_done: asyncio.Event | None = None,
    ):
        self._benchmark_id = benchmark_id
        self._task_tracking = task_tracking
        self._org = org
        self._limiter = limiter
        self._coordinator_done = coordinator_done
        self._cancellation_requested = set()
        self._authority = authority

    def _load_state(
        self, session: Session, task_ids: list[str]
    ) -> tuple[Benchmark, dict[str, tuple[TaskStatus, datetime]]]:
        benchmark_row = fetch_benchmark_row(self._benchmark_id, session, self._org)
        task_states = {
            task_id: (TaskStatus(status), started_at)
            for task_id, status, started_at in BenchmarkRepository(session).task_states(
                self._org.id, self._benchmark_id, task_ids
            )
        }

        for task_id in task_ids:
            if task_id not in task_states:
                raise ValueError(f"Task with id {task_id} not found")

        return benchmark_row, task_states

    def _read_tick(self, task_ids: list[str]) -> tuple[bool, Benchmark | None, dict[str, tuple[TaskStatus, datetime]]]:
        with Session(bind=engine) as session:
            try:
                lock_execution_authority(session, self._authority)
            except ExecutionAuthorityRevoked:
                authority_current = False
            else:
                authority_current = True
            session.rollback()

            if task_ids:
                benchmark_row, task_states = self._load_state(session, task_ids)
                return authority_current, benchmark_row, task_states

        return authority_current, None, {}

    async def track_tasks(self) -> None:

        while self._task_tracking or (self._coordinator_done is not None and not self._coordinator_done.is_set()):
            tasks_to_check = [
                task_id
                for task_id, tracked_task in self._task_tracking.items()
                if tracked_task.status != TrackedTaskStatus.DONE
            ]
            try:
                authority_current, benchmark_row, task_states = await database_call(
                    lambda: self._read_tick(tasks_to_check)
                )
            except OperationalError:
                logger.warning("Task monitor database read failed; retrying next tick", exc_info=True)
                await asyncio.sleep(self._TRACK_INTERVAL)
                continue

            for task_id, tracked_task in list(self._task_tracking.items()):
                if tracked_task.status == TrackedTaskStatus.DONE:
                    del self._task_tracking[task_id]
                    self._cancellation_requested.discard(task_id)

            if not self._task_tracking:
                if self._coordinator_done is not None and self._coordinator_done.is_set():
                    break
                await asyncio.sleep(self._TRACK_INTERVAL)
                continue

            if benchmark_row is None:
                await asyncio.sleep(self._TRACK_INTERVAL)
                continue
            if self._limiter is not None:
                await self._limiter.resize(benchmark_row.arguments.concurrency)

            for task_id in tasks_to_check:
                tracked_task = self._task_tracking.get(task_id)
                if tracked_task is None:
                    continue
                status, started_at = task_states[task_id]
                invalid = (
                    not authority_current
                    or status == TaskStatus.STOPPED
                    or benchmark_row.status == BenchmarkStatus.ERROR
                    or normalized_attempt_time(started_at) != tracked_task.attempt_started_at
                )
                task = tracked_task.task
                if invalid and task is not None and not task.done() and task_id not in self._cancellation_requested:
                    self._cancellation_requested.add(task_id)
                    task.cancel(f"Task {task_id} has been invalidated. Run has been requested to stop")

            await asyncio.sleep(self._TRACK_INTERVAL)
