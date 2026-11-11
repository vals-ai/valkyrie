from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID

from benchmark_service import SandboxProvider
from fastapi import APIRouter, Depends, Query
from starlette.concurrency import run_in_threadpool

from tracker.auth import get_current_org
from tracker.aws.resolver import deployment_aws_runtime
from tracker.aws.secrets import SecretsManagerStore
from tracker.database.models import Org
from tracker.domain import TaskStatus
from tracker.repositories.scheduler import PoolProviderReference, SchedulerRepository, SchedulerRepositoryDependency
from tracker.logging import get_logger
from tracker.executor.control_queue import queue_pool_id, queue_pool_lock_id
from tracker.types import (
    OrgContext,
    SchedulerActiveEntryResponse,
    SchedulerActiveStatus,
    SchedulerCapacityDomainResponse,
    SchedulerOverviewResponse,
    SchedulerPoolResponse,
    SchedulerSummaryResponse,
    SchedulerWaitingEntryResponse,
)
from tracker.clients.benchmark_service import fetch_sandbox_provider_config

router = APIRouter(prefix="/scheduler")
logger = get_logger(__name__)

_CAPACITY_READ_TIMEOUT_SECONDS = 2.0
_CAPACITY_REQUEST_TIMEOUT_SECONDS = 3.0
_PROVIDER_CLOSE_TIMEOUT_SECONDS = 1.0
_CAPACITY_MAX_CONCURRENCY = 4


def read_scheduler_overview(
    *,
    repository: SchedulerRepository,
    org_id: UUID,
    now: datetime,
    waiting_limit: int,
    active_limit: int,
    waiting_offset: int = 0,
    active_offset: int = 0,
) -> SchedulerOverviewResponse:
    waiting_rows, pool_counts = repository.waiting_rows(org_id=org_id, limit=waiting_limit, offset=waiting_offset)
    active_rows, active_counts = repository.active_rows(org_id=org_id, limit=active_limit, offset=active_offset)
    waiting_entries = [
        SchedulerWaitingEntryResponse(
            benchmark_uuid=benchmark.id,
            task_uuid=task.id,
            benchmark_name=benchmark.name,
            external_task_id=task.task_id,
            started_by_email=benchmark.started_by_email,
            pool_id=pool_id,
            position=position,
            priority=priority,
            enqueued_at=task.started_at,
        )
        for task, benchmark, priority, pool_id, position in waiting_rows[:waiting_limit]
    ]
    active_entries = [
        SchedulerActiveEntryResponse(
            benchmark_uuid=benchmark.id,
            task_uuid=task.id,
            benchmark_name=benchmark.name,
            external_task_id=task.task_id,
            started_by_email=benchmark.started_by_email,
            status=SchedulerActiveStatus(task.status.value),
            started_at=task.started_at,
        )
        for task, benchmark in active_rows[:active_limit]
    ]

    return SchedulerOverviewResponse(
        observed_at=now,
        summary=SchedulerSummaryResponse(
            waiting=sum(pool_counts.values()),
            building=active_counts.get(TaskStatus.BUILDING, 0),
            in_progress=active_counts.get(TaskStatus.IN_PROGRESS, 0),
            evaluating=active_counts.get(TaskStatus.EVALUATING, 0),
        ),
        pools=[
            SchedulerPoolResponse(pool_id=pool_id, waiting=waiting) for pool_id, waiting in sorted(pool_counts.items())
        ],
        waiting_entries=waiting_entries,
        active_entries=active_entries,
        waiting_capped=len(waiting_rows) > waiting_limit,
        active_capped=len(active_rows) > active_limit,
        waiting_next_offset=waiting_offset + waiting_limit if len(waiting_rows) > waiting_limit else None,
        active_next_offset=active_offset + active_limit if len(active_rows) > active_limit else None,
    )


async def _cancel_and_drain(task: asyncio.Task[None]) -> None:
    task.cancel()
    drain = asyncio.gather(task, return_exceptions=True)
    cancellation: asyncio.CancelledError | None = None
    while not drain.done():
        try:
            await asyncio.shield(drain)
        except asyncio.CancelledError as error:
            cancellation = error
    if cancellation is not None:
        raise cancellation


async def _close_provider(provider: SandboxProvider, pool_id: str, *, deadline: float) -> None:
    close_task = asyncio.create_task(provider.close())
    close_deadline = min(deadline, asyncio.get_running_loop().time() + _PROVIDER_CLOSE_TIMEOUT_SECONDS)
    try:
        async with asyncio.timeout_at(close_deadline):
            await asyncio.shield(close_task)
    except TimeoutError:
        logger.warning("Sandbox capacity provider close timed out for pool %s (%s)", pool_id, TimeoutError.__name__)
        await _cancel_and_drain(close_task)
    except asyncio.CancelledError:
        await _cancel_and_drain(close_task)
        raise
    except Exception as error:
        logger.warning("Sandbox capacity provider close failed for pool %s (%s)", pool_id, type(error).__name__)


async def _read_provider_capacity(
    *,
    org: Org,
    pool_id: str,
    provider_type: str,
    secret_name: str,
    read_deadline: float,
    request_deadline: float,
) -> list[SchedulerCapacityDomainResponse] | None:
    provider: SandboxProvider | None = None
    try:
        if asyncio.get_running_loop().time() >= read_deadline:
            return None
        try:
            async with asyncio.timeout_at(read_deadline):
                runtime = deployment_aws_runtime(OrgContext(id=org.id, name=org.name, storage_key=org.storage_key))
                provider_config = await fetch_sandbox_provider_config(
                    secret_name,
                    SecretsManagerStore(runtime.clients),
                    provider_type,
                )
                provider = provider_config.create_provider()
                provider_pool_id = provider.admission_pool_id
                if provider_pool_id is None or queue_pool_id(provider_pool_id) != queue_pool_lock_id(pool_id):
                    raise ValueError("Sandbox capacity provider does not match the queued pool")
                domains = await provider.get_capacity_domains()
                if domains is None:
                    return None
                return [
                    SchedulerCapacityDomainResponse.model_validate(domain, from_attributes=True) for domain in domains
                ]
        finally:
            if provider is not None:
                await _close_provider(provider, pool_id, deadline=request_deadline)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        logger.warning("Sandbox capacity unavailable for pool %s (%s)", pool_id, type(error).__name__)
        return None


def _capacity_request(
    references: set[PoolProviderReference],
) -> tuple[str | None, tuple[str, str] | None]:
    provider_types = {reference.provider_type for reference in references if reference.provider_type}
    provider_type = next(iter(provider_types)) if len(provider_types) == 1 else None
    configurations = {(reference.provider_type, reference.secret_name) for reference in references}
    if len(configurations) != 1:
        return provider_type, None
    configured_provider_type, secret_name = next(iter(configurations))
    if configured_provider_type is None or not secret_name:
        return provider_type, None
    return provider_type, (configured_provider_type, secret_name)


async def _enrich_scheduler_capacity(
    overview: SchedulerOverviewResponse,
    *,
    org: Org,
    references: dict[str, set[PoolProviderReference]],
) -> SchedulerOverviewResponse:
    loop = asyncio.get_running_loop()
    started_at = loop.time()
    read_deadline = started_at + _CAPACITY_READ_TIMEOUT_SECONDS
    request_deadline = started_at + _CAPACITY_REQUEST_TIMEOUT_SECONDS
    pools: list[SchedulerPoolResponse] = []
    requests: asyncio.Queue[tuple[int, str, str]] = asyncio.Queue()
    for index, pool in enumerate(overview.pools):
        provider_type, request = _capacity_request(references.get(pool.pool_id, set()))
        pools.append(pool.model_copy(update={"provider": provider_type, "capacity_domains": None}))
        if request is not None:
            requests.put_nowait((index, request[0], request[1]))

    async def enrich() -> None:
        while True:
            try:
                index, provider_type, secret_name = requests.get_nowait()
            except asyncio.QueueEmpty:
                return
            pool = pools[index]
            capacity_domains = await _read_provider_capacity(
                org=org,
                pool_id=pool.pool_id,
                provider_type=provider_type,
                secret_name=secret_name,
                read_deadline=read_deadline,
                request_deadline=request_deadline,
            )
            pools[index] = pool.model_copy(update={"capacity_domains": capacity_domains})

    workers = [asyncio.create_task(enrich()) for _ in range(min(_CAPACITY_MAX_CONCURRENCY, requests.qsize()))]
    worker_drain = asyncio.gather(*workers, return_exceptions=True)
    cancellation: asyncio.CancelledError | None = None
    try:
        async with asyncio.timeout_at(request_deadline):
            await asyncio.shield(worker_drain)
    except TimeoutError:
        pass
    except asyncio.CancelledError as error:
        cancellation = error
    if not worker_drain.done():
        for worker in workers:
            if not worker.done():
                worker.cancel()
        while not worker_drain.done():
            try:
                await asyncio.shield(worker_drain)
            except asyncio.CancelledError as repeated_error:
                cancellation = repeated_error
    if cancellation is not None:
        raise cancellation
    return overview.model_copy(update={"pools": pools})


@router.get(
    "/overview",
    response_model=SchedulerOverviewResponse,
    response_model_exclude_unset=True,
    description="Show the organization's waiting and active runs per scheduler pool, with pool capacity when include_capacity is set.",
)
async def get_scheduler_overview(
    repository: SchedulerRepositoryDependency,
    waiting_limit: int = Query(default=100, ge=1, le=200),
    active_limit: int = Query(default=100, ge=1, le=200),
    waiting_offset: int = Query(default=0, ge=0),
    active_offset: int = Query(default=0, ge=0),
    include_capacity: bool = Query(default=False),
    org: Org = Depends(get_current_org),
) -> SchedulerOverviewResponse:
    overview = await run_in_threadpool(
        read_scheduler_overview,
        repository=repository,
        org_id=org.id,
        now=datetime.now(UTC),
        waiting_limit=waiting_limit,
        active_limit=active_limit,
        waiting_offset=waiting_offset,
        active_offset=active_offset,
    )
    if not include_capacity:
        return overview
    references = await run_in_threadpool(repository.pool_references, org_id=org.id)
    pools = {pool_id: SchedulerPoolResponse(pool_id=pool_id, waiting=0) for pool_id in references}
    pools.update({pool.pool_id: pool for pool in overview.pools})
    overview = overview.model_copy(update={"pools": [pools[pool_id] for pool_id in sorted(pools)]})
    return await _enrich_scheduler_capacity(overview, org=org, references=references)
