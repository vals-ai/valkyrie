from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

from benchmark_service import SandboxCapacity, SandboxError, SandboxNotFoundError, SandboxProvider
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session

from tracker.aws.resolver import resolve_run_aws_runtime
from tracker.aws.services import CloudRuntimeFactory
from tracker.config import SANDBOX_BUILD_RATE_PER_MINUTE
from tracker.database.models import Benchmark, Org
from tracker.database.session import database_call, engine as tracker_engine
from tracker.local.runtime import LocalRuntimeFactory
from tracker.logging import get_logger
from tracker.repositories.advisory_lock import PostgresAdvisoryLock, task_build_lock_resource_id
from tracker.repositories.executor_release import ExecutorReleaseRepository
from tracker.repositories.scheduler import SchedulerRepository
from tracker.executor.control_queue import (
    is_reserved_queue_pool_id,
    queue_pool_id,
    queue_pool_lock_id,
    queued_sandbox_name,
)
from tracker.sandbox import delete_sandbox
from tracker.types import OrgContext

logger = get_logger(__name__)
_POLL_SECONDS = 1.0
_CAPACITY_FAILURE_LIMIT = 3
_RECOVERY_SECONDS = 30.0
_ADMISSION_BATCH_SIZE = 100
_READINESS_BATCH_SIZE = 10
_CAPACITY_CACHE_SECONDS = 2.0
_ORPHAN_CLEANUP_GRACE = timedelta(minutes=5)
_RELEASED_RESERVATION_RETENTION = timedelta(minutes=2)


@dataclass(frozen=True)
class _PoolContext:
    provider: SandboxProvider
    pool_id: str
    engine: Engine


@dataclass(frozen=True)
class _AbandonedReservation:
    task_row_id: UUID
    attempt_started_at: datetime
    executor_dispatch_id: UUID
    reserved_at: datetime
    phase_dispatch_id: UUID | None
    phase_token: UUID | None


async def _cleanup_orphan_sandbox(provider: SandboxProvider, name: str) -> bool:
    try:
        sandbox = await provider.get_sandbox(name)
        if sandbox.name != name:
            logger.error("sandbox.admission.recovery_identity_mismatch", extra={"sandbox_name": name})
            return False
        return await delete_sandbox(sandbox, provider, initiated_by="orphan_cleanup")
    except SandboxNotFoundError:
        return True
    except SandboxError:
        logger.warning("sandbox.admission.recovery_failed", extra={"sandbox_name": name}, exc_info=True)
        return False


def _pending_recovery_tasks(context: _PoolContext) -> dict[UUID, str] | None:
    with Session(context.engine) as session:
        admission = ExecutorReleaseRepository(session).shared_admission()
        if admission is None or admission.maintenance_target_sha is not None:
            return None
        pending = SchedulerRepository(session).pending_owned_ecs_task_arns(context.pool_id)
        session.rollback()
        return pending


def _prepare_abandoned_reservations(
    context: _PoolContext, pending: dict[UUID, str], stopped: set[str]
) -> tuple[list[_AbandonedReservation], datetime] | None:
    with Session(context.engine) as session:
        admission = ExecutorReleaseRepository(session).shared_admission()
        if admission is None or admission.maintenance_target_sha is not None:
            return None
        repository = SchedulerRepository(session)
        for dispatch_id, task_arn in pending.items():
            if task_arn in stopped:
                repository.handoff_dispatch_phases(dispatch_id)
        leak_cutoff = repository.database_now() - _ORPHAN_CLEANUP_GRACE
        reservations = [
            _AbandonedReservation(
                task_row_id=reservation.task_row_id,
                attempt_started_at=reservation.attempt_started_at,
                executor_dispatch_id=cast(UUID, reservation.executor_dispatch_id),
                reserved_at=reservation.reserved_at,
                phase_dispatch_id=reservation.phase_dispatch_id,
                phase_token=reservation.phase_token,
            )
            for reservation in repository.lock_abandoned_reservations(context.pool_id)
        ]
        repository.purge_released_reservations(
            context.pool_id, repository.database_now() - _RELEASED_RESERVATION_RETENTION
        )
        session.commit()
        return reservations, leak_cutoff


def _reservation_still_abandoned(
    connection: Connection, context: _PoolContext, reservation: _AbandonedReservation
) -> bool:
    with Session(connection) as session:
        admission = ExecutorReleaseRepository(session).shared_admission()
        if admission is None or admission.maintenance_target_sha is not None:
            return False
        return (
            SchedulerRepository(session).abandoned_reservation(
                context.pool_id,
                reservation.task_row_id,
                reservation.attempt_started_at,
                reservation.executor_dispatch_id,
                reservation.reserved_at,
                reservation.phase_dispatch_id,
                reservation.phase_token,
            )
            is not None
        )


def _release_abandoned_reservation(
    connection: Connection, context: _PoolContext, reservation: _AbandonedReservation
) -> None:
    with Session(connection) as session:
        admission = ExecutorReleaseRepository(session).shared_admission()
        if admission is None or admission.maintenance_target_sha is not None:
            return
        repository = SchedulerRepository(session)
        if (
            repository.abandoned_reservation(
                context.pool_id,
                reservation.task_row_id,
                reservation.attempt_started_at,
                reservation.executor_dispatch_id,
                reservation.reserved_at,
                reservation.phase_dispatch_id,
                reservation.phase_token,
            )
            is not None
        ):
            repository.release_reservation(reservation.task_row_id, reservation.attempt_started_at)
            session.commit()


async def _reconcile_abandoned_reservations(context: _PoolContext) -> None:
    pending = await database_call(lambda: _pending_recovery_tasks(context))
    if pending is None:
        return
    stopped: set[str] = set()
    if pending:
        from tracker.executor.dispatch_recovery import describe_executor_tasks

        task_arns = list(pending.values())
        for offset in range(0, len(task_arns), 100):
            response = await asyncio.to_thread(describe_executor_tasks, task_arns[offset : offset + 100])
            stopped.update(task["taskArn"] for task in response["tasks"] if task["lastStatus"] == "STOPPED")
    prepared = await database_call(lambda: _prepare_abandoned_reservations(context, pending, stopped))
    if prepared is None:
        return
    reservations, leak_cutoff = prepared
    for reservation in reservations:
        lock = PostgresAdvisoryLock(context.engine, resource_id=task_build_lock_resource_id(reservation.task_row_id))
        async with lock as acquired:
            if not acquired:
                continue
            if not await database_call(lambda: _reservation_still_abandoned(lock.connection, context, reservation)):
                continue
            name = queued_sandbox_name(reservation.task_row_id, reservation.attempt_started_at)
            cleaned = await _cleanup_orphan_sandbox(context.provider, name)
            if not cleaned:
                if reservation.reserved_at.replace(tzinfo=UTC) > leak_cutoff:
                    continue
                logger.error(
                    "sandbox.admission.reservation_leaked",
                    extra={"sandbox_name": name, "reserved_at": reservation.reserved_at.isoformat()},
                )
            await database_call(lambda: _release_abandoned_reservation(lock.connection, context, reservation))


@dataclass(frozen=True)
class CapacitySnapshot:
    capacities: dict[tuple[str, str], SandboxCapacity]
    read_at: datetime
    fetched_at: float


class CapacityCache:
    def __init__(self, provider: SandboxProvider, engine: Engine, ttl_seconds: float = _CAPACITY_CACHE_SECONDS) -> None:
        self._provider = provider
        self._engine = engine
        self._ttl = ttl_seconds
        self._snapshot: CapacitySnapshot | None = None
        self._failures = 0

    def _database_now(self) -> datetime:
        with Session(self._engine) as session:
            return SchedulerRepository(session).database_now()

    async def get(self) -> CapacitySnapshot | None:
        snapshot = self._snapshot
        if snapshot is not None and asyncio.get_running_loop().time() - snapshot.fetched_at < self._ttl:
            return snapshot
        read_at = await asyncio.to_thread(self._database_now)
        fetched_at = asyncio.get_running_loop().time()
        try:
            domains = await self._provider.get_capacity_domains()
        except SandboxError:
            self._failures += 1
            if self._failures >= _CAPACITY_FAILURE_LIMIT:
                raise
            logger.warning("sandbox.admission.capacity_failed", exc_info=True)
            return None
        if domains is None:
            return None
        self._failures = 0
        self._snapshot = CapacitySnapshot(
            {(domain.target_id, domain.sandbox_class): domain.capacity for domain in domains}, read_at, fetched_at
        )
        return self._snapshot


@dataclass(frozen=True)
class PoolReference:
    pool_id: str
    benchmark: Benchmark
    org: Org


ProviderFactory = Callable[[PoolReference], Awaitable[SandboxProvider]]


def read_pool_references(engine: Engine) -> list[PoolReference]:
    with Session(engine) as session:
        rows = SchedulerRepository(session).contributing_runs()
        references: list[PoolReference] = []
        for benchmark, org in rows:
            stored_pool_id = benchmark.arguments.queue_pool_id
            assert stored_pool_id is not None
            if is_reserved_queue_pool_id(stored_pool_id):
                references.append(PoolReference(stored_pool_id, benchmark, org))
        return references


async def open_pool_provider(reference: PoolReference) -> SandboxProvider:
    arguments = reference.benchmark.arguments
    if arguments.environment == "local":
        runtime = LocalRuntimeFactory.create_runtime(
            arguments.properties,
            reference.org.id,
            sandbox_provider=arguments.sandbox_provider,
            sandbox_provider_secret_name=arguments.sandbox_provider_secret_name,
        )
    else:
        runtime = CloudRuntimeFactory.create_runtime(
            resolve_run_aws_runtime(
                properties=arguments.properties,
                org=OrgContext(id=reference.org.id, name=reference.org.name, storage_key=reference.org.storage_key),
            ),
            sandbox_provider=arguments.sandbox_provider,
            sandbox_provider_secret_name=arguments.sandbox_provider_secret_name,
        )
    config = await runtime.get_sandbox_provider_config()
    provider = config.create_provider()
    provider_pool_id = provider.admission_pool_id
    if provider_pool_id is None or queue_pool_id(provider_pool_id) != queue_pool_lock_id(reference.pool_id):
        await provider.close()
        raise ValueError("Scheduler provider does not match the persisted admission pool")
    return provider


class PoolCoordinator:
    def __init__(
        self,
        engine: Engine,
        reference: PoolReference,
        *,
        provider_factory: ProviderFactory = open_pool_provider,
        interval_seconds: float = _POLL_SECONDS,
        rate_per_minute: int = SANDBOX_BUILD_RATE_PER_MINUTE,
    ) -> None:
        self.engine = engine
        self.reference = reference
        self.provider_factory = provider_factory
        self.interval_seconds = interval_seconds
        self.rate_per_minute = rate_per_minute

    async def run(self) -> None:
        while True:
            lock = PostgresAdvisoryLock(self.engine, resource_id=queue_pool_lock_id(self.reference.pool_id))
            async with lock as acquired:
                if acquired:
                    provider = await self.provider_factory(self.reference)
                    try:
                        context = _PoolContext(provider, self.reference.pool_id, self.engine)
                        cache = CapacityCache(provider, self.engine)
                        recovery = asyncio.create_task(self._recover(context), name=f"pool-recovery:{context.pool_id}")
                        try:
                            next_readiness = 0.0
                            while True:
                                if recovery.done():
                                    await recovery
                                now = asyncio.get_running_loop().time()
                                request_readiness = now >= next_readiness
                                await database_call(
                                    lambda: self._maintain(
                                        lock.connection, context, request_readiness=request_readiness
                                    )
                                )
                                if request_readiness:
                                    next_readiness = now + self.interval_seconds
                                granted = await self.admit(lock.connection, context, cache)
                                if granted < _ADMISSION_BATCH_SIZE:
                                    await asyncio.sleep(self.interval_seconds)
                        finally:
                            recovery.cancel()
                            await asyncio.gather(recovery, return_exceptions=True)
                    finally:
                        await provider.close()
            await asyncio.sleep(self.interval_seconds)

    def _maintain(self, connection: Connection, context: _PoolContext, *, request_readiness: bool) -> None:
        with Session(connection) as session:
            admission = ExecutorReleaseRepository(session).shared_admission()
            if admission is None or admission.maintenance_target_sha is not None:
                return
            repository = SchedulerRepository(session)
            repository.expire_unaccepted_grants(context.pool_id)
            repository.reset_abandoned_builds(context.pool_id, repository.database_now())
            repository.purge_stale_admission_candidates(context.pool_id)
            if request_readiness:
                repository.request_readiness_checks(
                    context.pool_id,
                    limit=_READINESS_BATCH_SIZE,
                    retry_delay=timedelta(seconds=self.interval_seconds),
                )
            session.commit()

    async def _recover(self, context: _PoolContext) -> None:
        while True:
            await _reconcile_abandoned_reservations(context)
            await asyncio.sleep(_RECOVERY_SECONDS)

    async def admit(self, connection: Connection, context: _PoolContext, cache: CapacityCache) -> int:
        if self.rate_per_minute and not await database_call(lambda: self._rate_available(connection, context.pool_id)):
            return 0
        snapshot = await cache.get()
        if snapshot is None:
            return 0
        return await database_call(lambda: self._commit_grants(connection, context, snapshot))

    def _rate_available(self, connection: Connection, pool_id: str) -> bool:
        with Session(connection) as session:
            repository = SchedulerRepository(session)
            used = repository.admissions_since(pool_id, repository.database_now() - timedelta(minutes=1))
            return used < self.rate_per_minute

    def _commit_grants(self, connection: Connection, context: _PoolContext, snapshot: CapacitySnapshot) -> int:
        with Session(connection) as session:
            admission = ExecutorReleaseRepository(session).shared_admission()
            if admission is None or admission.maintenance_target_sha is not None:
                return 0
            repository = SchedulerRepository(session)
            limit = _ADMISSION_BATCH_SIZE
            if self.rate_per_minute:
                used = repository.admissions_since(context.pool_id, repository.database_now() - timedelta(minutes=1))
                limit = min(limit, max(0, self.rate_per_minute - used))
            granted = repository.grant_prepared_tasks(context.pool_id, snapshot.capacities, snapshot.read_at, limit)
            session.commit()
            return granted


class SharedPoolScheduler:
    def __init__(
        self,
        engine: Engine = tracker_engine,
        *,
        provider_factory: ProviderFactory = open_pool_provider,
        interval_seconds: float = _POLL_SECONDS,
        rate_per_minute: int = SANDBOX_BUILD_RATE_PER_MINUTE,
    ) -> None:
        self.engine = engine
        self.provider_factory = provider_factory
        self.interval_seconds = interval_seconds
        self.rate_per_minute = rate_per_minute
        self._task: asyncio.Task[None] | None = None
        self._pools: dict[str, asyncio.Task[None]] = {}
        self._last_references: dict[str, UUID] = {}

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="shared-pool-scheduler")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        try:
            while True:
                try:
                    references = await asyncio.to_thread(read_pool_references, self.engine)
                    by_pool: dict[str, list[PoolReference]] = {}
                    for reference in references:
                        by_pool.setdefault(reference.pool_id, []).append(reference)
                    for pool_id, task in list(self._pools.items()):
                        if task.done() or pool_id not in by_pool:
                            task.cancel()
                            (result,) = await asyncio.shield(asyncio.gather(task, return_exceptions=True))
                            if isinstance(result, Exception):
                                logger.error(
                                    "Shared admission coordinator failed for pool %s", pool_id, exc_info=result
                                )
                            del self._pools[pool_id]
                            if pool_id not in by_pool:
                                del self._last_references[pool_id]
                    for pool_id, alternatives in by_pool.items():
                        if pool_id not in self._pools:
                            previous = self._last_references.get(pool_id)
                            previous_index = next(
                                (
                                    index
                                    for index, reference in enumerate(alternatives)
                                    if reference.benchmark.id == previous
                                ),
                                -1,
                            )
                            reference = alternatives[(previous_index + 1) % len(alternatives)]
                            coordinator = PoolCoordinator(
                                self.engine,
                                reference,
                                provider_factory=self.provider_factory,
                                interval_seconds=self.interval_seconds,
                                rate_per_minute=self.rate_per_minute,
                            )
                            self._pools[pool_id] = asyncio.create_task(
                                coordinator.run(), name=f"pool-admission:{pool_id}"
                            )
                            self._last_references[pool_id] = reference.benchmark.id
                except Exception:
                    logger.exception("Shared admission pool discovery failed")
                await asyncio.sleep(self.interval_seconds)
        finally:
            tasks = list(self._pools.values())
            for task in tasks:
                if not task.cancelling():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._pools.clear()
            self._last_references.clear()
