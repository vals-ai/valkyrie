from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

from benchmark_service import (
    Resources,
    Sandbox,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxSource,
)
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session

from tracker.database.session import database_call
from tracker.database.models import (
    Task,
    TaskStatus,
)
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.logging import get_logger
from tracker.repositories.advisory_lock import task_build_lock
from tracker.repositories.scheduler import SchedulerRepository
from tracker.sandbox import SandboxCreationState, delete_sandbox
from tracker.scheduler.store import queue_pool_id, queue_pool_lock_id, queued_sandbox_name

logger = get_logger(__name__)

SandboxFactory = Callable[[], AbstractAsyncContextManager[Sandbox]]

_RATE_WINDOW = timedelta(minutes=1)
_RELEASED_RESERVATION_RETENTION = 2 * _RATE_WINDOW
_ORPHAN_CLEANUP_GRACE = timedelta(minutes=5)


@dataclass(frozen=True, slots=True)
class SandboxQueueContext:
    provider: SandboxProvider = field(repr=False)
    pool_id: str
    engine: Engine = field(repr=False)
    preparation_slots: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(10), repr=False)
    admission_signals: AdmissionSignals = field(default_factory=lambda: AdmissionSignals(), repr=False)
    poll_interval_seconds: float = 1.0

    def serves_queue(self, pool_id: str) -> bool:
        return queue_pool_lock_id(pool_id) == queue_pool_lock_id(self.pool_id)

    def for_queue(self, pool_id: str) -> SandboxQueueContext:
        return replace(self, pool_id=pool_id)


def create_queue_context(
    *,
    engine: Engine,
    provider: SandboxProvider,
    poll_interval_seconds: float = 1.0,
) -> SandboxQueueContext:
    provider_pool_id = provider.admission_pool_id
    if provider_pool_id is None:
        raise ValueError("Sandbox provider does not support queued admission")

    return SandboxQueueContext(
        provider=provider,
        pool_id=queue_pool_id(provider_pool_id),
        engine=engine,
        poll_interval_seconds=poll_interval_seconds,
    )


def _start_claimed_task(
    session: Session,
    *,
    task_row_id: UUID,
    expected_started_at: datetime,
) -> bool:
    repository = SchedulerRepository(session)
    if not repository.start_building_task(task_row_id, expected_started_at):
        return False

    repository.release_reservation(task_row_id, expected_started_at)
    return True


def reset_abandoned_pool_builds(connection: Connection, context: SandboxQueueContext) -> None:
    with Session(connection) as session:
        repository = SchedulerRepository(session)
        repository.reset_abandoned_builds(context.pool_id, repository.database_now())
        repository.purge_stale_admission_candidates(context.pool_id)
        session.commit()


async def reconcile_abandoned_reservations(context: SandboxQueueContext) -> None:
    with Session(context.engine) as session:
        repository = SchedulerRepository(session)
        leak_cutoff = await database_call(repository.database_now) - _ORPHAN_CLEANUP_GRACE
        for reservation in await database_call(lambda: repository.lock_abandoned_reservations(context.pool_id)):
            name = queued_sandbox_name(reservation.task_row_id, reservation.attempt_started_at)
            if not await _cleanup_orphan_sandbox(context, name):
                if reservation.reserved_at.replace(tzinfo=UTC) > leak_cutoff:
                    continue

                logger.error(
                    "sandbox.admission.reservation_leaked",
                    extra={"sandbox_name": name, "reserved_at": reservation.reserved_at.isoformat()},
                )

            await database_call(
                lambda: repository.release_reservation(reservation.task_row_id, reservation.attempt_started_at)
            )

        def purge_and_commit() -> None:
            repository.purge_released_reservations(
                context.pool_id, repository.database_now() - _RELEASED_RESERVATION_RETENTION
            )
            session.commit()

        await database_call(purge_and_commit)


async def _cleanup_orphan_sandbox(context: SandboxQueueContext, name: str) -> bool:
    try:
        sandbox = await context.provider.get_sandbox(name)
        if sandbox.name != name:
            logger.error("sandbox.admission.recovery_identity_mismatch", extra={"sandbox_name": name})
            return False

        return await delete_sandbox(sandbox, context.provider, initiated_by="orphan_cleanup")
    except SandboxNotFoundError:
        return True
    except SandboxError:
        logger.warning("sandbox.admission.recovery_failed", extra={"sandbox_name": name}, exc_info=True)
        return False


async def _close_stack_before_cancellation(stack: AsyncExitStack) -> None:
    close_task = asyncio.create_task(stack.aclose())
    try:
        await asyncio.shield(close_task)
    except asyncio.CancelledError:
        await close_task
        raise


def _is_waiting_without_lock(engine: Engine, task_row_id: UUID, expected_started_at: datetime) -> bool:
    with Session(engine) as session:
        return SchedulerRepository(session).is_waiting(task_row_id, expected_started_at)


def _clear_admission_candidate(
    engine: Engine, task_row_id: UUID, expected_started_at: datetime, authority: ExecutionAuthority
) -> None:
    with Session(engine) as session:
        SchedulerRepository(session).clear_admission_candidate(task_row_id, expected_started_at, authority.dispatch_id)
        session.commit()


def _release_reservation(bind: Connection, task_row_id: UUID, expected_started_at: datetime) -> None:
    with Session(bind) as session:
        SchedulerRepository(session).release_reservation(task_row_id, expected_started_at)
        session.commit()


@dataclass(frozen=True, slots=True)
class ReadinessRequest:
    requested_at: datetime


class AdmissionSignals:
    def __init__(self) -> None:
        self._states: dict[UUID, tuple[datetime, TaskStatus]] = {}
        self._requests: dict[UUID, datetime] = {}
        self._consumed: dict[UUID, tuple[datetime, datetime]] = {}
        self._waiters: dict[tuple[UUID, datetime], tuple[bool, asyncio.Future[bool | ReadinessRequest]]] = {}

    def forget(self, task_row_id: UUID) -> None:
        self._states.pop(task_row_id, None)
        self._requests.pop(task_row_id, None)

    def _notice(self, task_id: UUID, attempt: datetime, ready: bool) -> bool | ReadinessRequest | None:
        state = self._states.get(task_id)
        if state is None or state[0] != attempt:
            return False
        if state[1] != TaskStatus.PENDING:
            return ready and state[1] == TaskStatus.BUILDING
        requested_at = self._requests.get(task_id)
        if not ready and requested_at is not None and self._consumed.get(task_id) != (attempt, requested_at):
            self._consumed[task_id] = (attempt, requested_at)
            return ReadinessRequest(requested_at)
        return None

    def publish(self, tasks: Sequence[Task], readiness_requests: Mapping[UUID, datetime]) -> None:
        self._states = {task.id: (task.started_at.replace(tzinfo=UTC), task.status) for task in tasks}
        self._requests = {
            task_id: requested_at.replace(tzinfo=UTC) for task_id, requested_at in readiness_requests.items()
        }
        for (task_id, attempt), (ready, future) in self._waiters.items():
            if not future.done():
                notice = self._notice(task_id, attempt, ready)
                if notice is not None:
                    future.set_result(notice)

    async def wait(self, task_row_id: UUID, expected_started_at: datetime, *, ready: bool) -> bool | ReadinessRequest:
        attempt = expected_started_at.replace(tzinfo=UTC)
        if task_row_id in self._states:
            notice = self._notice(task_row_id, attempt, ready)
            if notice is not None:
                return notice
        key = (task_row_id, attempt)
        future: asyncio.Future[bool | ReadinessRequest] = asyncio.get_running_loop().create_future()
        self._waiters[key] = (ready, future)
        try:
            return await future
        finally:
            del self._waiters[key]


async def _start_sandbox(
    *,
    stack: AsyncExitStack,
    bind: Connection,
    task_row_id: UUID,
    expected_started_at: datetime,
    authority: ExecutionAuthority,
    create: SandboxFactory,
) -> Sandbox | None:
    sandbox = await stack.enter_async_context(create())

    def commit_start() -> bool:
        with Session(bind) as session:
            try:
                lock_execution_authority(session, authority)
            except ExecutionAuthorityRevoked:
                session.rollback()
                return False
            started = _start_claimed_task(
                session,
                task_row_id=task_row_id,
                expected_started_at=expected_started_at,
            )
            if started:
                session.commit()
            else:
                session.rollback()
            return started

    try:
        started = await database_call(commit_start)
    except Exception:
        await _close_stack_before_cancellation(stack)
        raise
    if not started:
        await _close_stack_before_cancellation(stack)

        return None

    return sandbox


async def enter_queued_sandbox(
    *,
    stack: AsyncExitStack,
    context: SandboxQueueContext,
    task_row_id: UUID,
    expected_started_at: datetime,
    authority: ExecutionAuthority,
    source: SandboxSource,
    resources: Resources,
    creation_state: SandboxCreationState,
    create: SandboxFactory,
    preparation_stack: AsyncExitStack,
) -> Sandbox | None:
    prepared = False
    accepted = False
    build_lock = task_build_lock(context.engine, task_row_id)
    try:
        if not await database_call(lambda: _is_waiting_without_lock(context.engine, task_row_id, expected_started_at)):
            return None
        demand = await context.provider.reservable_demand(source, resources)

        def commit_candidate(ready: bool) -> bool:
            with Session(context.engine) as session:
                repository = SchedulerRepository(session)
                lock_execution_authority(session, authority)
                if not repository.is_waiting(task_row_id, expected_started_at):
                    return False
                repository.prepare_admission_candidate(
                    context.pool_id,
                    task_row_id,
                    expected_started_at,
                    authority.dispatch_id,
                    demand,
                    admission_ready=ready,
                )
                session.commit()
            return True

        async def publish_candidate(ready: bool) -> bool:
            context.admission_signals.forget(task_row_id)
            return await database_call(lambda: commit_candidate(ready))

        admission_ready = await context.provider.check_admission(source, resources)
        if not await publish_candidate(admission_ready):
            return None
        prepared = True
        await preparation_stack.aclose()
        while True:
            notice = await context.admission_signals.wait(task_row_id, expected_started_at, ready=admission_ready)
            if notice is False:
                return None
            if notice is True:
                break
            async with context.preparation_slots:
                requested_at = notice.requested_at

                def commit_readiness_check() -> bool:
                    with Session(context.engine) as session:
                        lock_execution_authority(session, authority)
                        checking = SchedulerRepository(session).begin_readiness_check(
                            task_row_id, expected_started_at, authority.dispatch_id, requested_at
                        )
                        session.commit()
                        return checking

                if not await database_call(commit_readiness_check):
                    continue
                admission_ready = await context.provider.check_admission(source, resources)
                if not await publish_candidate(admission_ready):
                    return None
        if not await build_lock.acquire():
            return None

        def commit_grant() -> bool:
            with Session(build_lock.connection) as session:
                lock_execution_authority(session, authority)
                if not SchedulerRepository(session).accept_grant(
                    task_row_id, expected_started_at, authority.dispatch_id
                ):
                    return False
                session.commit()
                return True

        if not await database_call(commit_grant):
            return None
        accepted = True
        return await _start_sandbox(
            stack=stack,
            bind=build_lock.connection,
            task_row_id=task_row_id,
            expected_started_at=expected_started_at,
            authority=authority,
            create=create,
        )
    except ExecutionAuthorityRevoked:
        return None
    finally:
        try:
            if prepared:
                await database_call(
                    lambda: _clear_admission_candidate(context.engine, task_row_id, expected_started_at, authority)
                )
            if accepted and not creation_state.cleanup_pending:
                await database_call(
                    lambda: _release_reservation(build_lock.connection, task_row_id, expected_started_at)
                )
        finally:
            await build_lock.release()
