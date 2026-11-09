from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from benchmark_service import SandboxCapacity, SandboxError, SandboxProvider
from sqlalchemy.engine import Connection, Engine
from sqlmodel import Session

from tracker.aws.resolver import resolve_run_aws_runtime
from tracker.aws.services import CloudRuntimeFactory
from tracker.config import SANDBOX_BUILD_RATE_PER_MINUTE
from tracker.database.models import Benchmark, Org
from tracker.database.session import database_call, engine as tracker_engine
from tracker.local.runtime import LocalRuntimeFactory
from tracker.logging import get_logger
from tracker.repositories.scheduler import SchedulerRepository
from tracker.scheduler.admission import (
    SandboxQueueContext,
    reconcile_abandoned_reservations,
    reset_abandoned_pool_builds,
    create_queue_context,
)
from tracker.scheduler.store import is_reserved_queue_pool_id, queue_pool_id, queue_pool_lock, queue_pool_lock_id

logger = get_logger(__name__)
_POLL_SECONDS = 1.0
_CAPACITY_FAILURE_LIMIT = 3
_RECOVERY_SECONDS = 30.0
_ADMISSION_BATCH_SIZE = 100
_READINESS_BATCH_SIZE = 10
_CAPACITY_CACHE_SECONDS = 2.0


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
            resolve_run_aws_runtime(properties=arguments.properties, org=reference.org),
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
            lock = queue_pool_lock(self.engine, self.reference.pool_id)
            async with lock as acquired:
                if acquired:
                    provider = await self.provider_factory(self.reference)
                    try:
                        context = create_queue_context(engine=self.engine, provider=provider).for_queue(
                            self.reference.pool_id
                        )
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

    def _maintain(self, connection: Connection, context: SandboxQueueContext, *, request_readiness: bool) -> None:
        with Session(connection) as session:
            SchedulerRepository(session).expire_unaccepted_grants(context.pool_id)
            session.commit()
        reset_abandoned_pool_builds(connection, context)
        if request_readiness:
            with Session(connection) as session:
                SchedulerRepository(session).request_readiness_checks(
                    context.pool_id,
                    limit=_READINESS_BATCH_SIZE,
                    retry_delay=timedelta(seconds=self.interval_seconds),
                )
                session.commit()

    async def _recover(self, context: SandboxQueueContext) -> None:
        while True:
            await reconcile_abandoned_reservations(context)
            await asyncio.sleep(_RECOVERY_SECONDS)

    async def admit(self, connection: Connection, context: SandboxQueueContext, cache: CapacityCache) -> int:
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

    def _commit_grants(self, connection: Connection, context: SandboxQueueContext, snapshot: CapacitySnapshot) -> int:
        with Session(connection) as session:
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
