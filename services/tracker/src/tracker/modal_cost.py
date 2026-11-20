from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import cast

from benchmark_service import ModalProviderConfig, SandboxProviderConfig
from modal import App, Client
from modal.exception import InvalidError
from sqlalchemy import Connection, Engine
from sqlmodel import Session

from tracker.database.models import Benchmark, Org
from tracker.database.session import database_call
from tracker.domain import SandboxResource
from tracker.logging import get_logger
from tracker.repositories.cost import CostRepository
from tracker.repositories.organization import OrganizationRepository
from tracker.sandbox import MODAL_ATTEMPT_TAG

logger = get_logger(__name__)

MODAL_APP_NAME = "benchmark-service"
MODAL_RUN_TAG = "run-id"
_MODAL_RESOURCES = {"CPU": SandboxResource.CPU, "Memory": SandboxResource.MEMORY}
MODAL_STATS_MAX_SPAN = timedelta(days=7)
_HOUR = timedelta(hours=1)
MODAL_SETTLE_MARGIN = timedelta(minutes=5)
MODAL_READ_TIMEOUT_SECONDS = 60.0

ModalRunCosts = dict[tuple[str | None, SandboxResource], float]
ModalCostReader = Callable[[Org, Benchmark, datetime, datetime], Awaitable[ModalRunCosts]]

_modal_clients: dict[tuple[str, str], Client] = {}


def _as_utc(moment: datetime) -> datetime:
    return moment.astimezone(UTC) if moment.utcoffset() is not None else moment.replace(tzinfo=UTC)


def modal_usage_window(benchmark: Benchmark) -> tuple[datetime, datetime]:
    start = _as_utc(benchmark.started_at).replace(minute=0, second=0, microsecond=0)
    finished = _as_utc(cast(datetime, benchmark.finished_at))
    end = finished.replace(minute=0, second=0, microsecond=0)
    return start, end if end == finished else end + timedelta(hours=1)


async def run_provider_config(org: Org, benchmark: Benchmark) -> SandboxProviderConfig:
    from tracker.aws.resolver import deployment_aws_runtime
    from tracker.aws.services import CloudRuntimeFactory
    from tracker.local.runtime import LocalRuntimeFactory

    arguments = benchmark.arguments
    if arguments.environment == "local":
        runtime = LocalRuntimeFactory.create_runtime(
            arguments.properties,
            org.id,
            sandbox_provider=arguments.sandbox_provider,
            sandbox_provider_secret_name=arguments.sandbox_provider_secret_name,
        )
    else:
        runtime = CloudRuntimeFactory.create_runtime(
            deployment_aws_runtime(org, arguments.properties),
            sandbox_provider=arguments.sandbox_provider,
            sandbox_provider_secret_name=arguments.sandbox_provider_secret_name,
        )
    return await runtime.get_sandbox_provider_config()


async def _modal_client(config: ModalProviderConfig) -> Client:
    key = (config.MODAL_TOKEN_ID, config.MODAL_TOKEN_SECRET)
    if key not in _modal_clients:
        _modal_clients[key] = await Client.from_credentials.aio(*key)
    return _modal_clients[key]


async def read_modal_run_cost(org: Org, benchmark: Benchmark, start: datetime, end: datetime) -> ModalRunCosts:
    config = await run_provider_config(org, benchmark)
    if not isinstance(config, ModalProviderConfig):
        raise TypeError(f"Expected a Modal provider secret, got {type(config).__name__}")
    app = await App.lookup.aio(MODAL_APP_NAME, client=await _modal_client(config), create_if_missing=False)
    costs: ModalRunCosts = {}
    since = start
    while since < end:
        until = min(since + MODAL_STATS_MAX_SPAN, end)
        await _add_window_costs(app, str(benchmark.id), since, until, costs)
        since = until
    return costs


async def _add_window_costs(app: App, run_id: str, since: datetime, until: datetime, costs: ModalRunCosts) -> None:
    try:
        groups = [
            group
            async for group in app.sandboxes.stats.aio(
                since=since, until=until, tags={MODAL_RUN_TAG: run_id}, group_by_tags=[MODAL_ATTEMPT_TAG]
            )
        ]
    except InvalidError:
        if until - since <= _HOUR:
            raise
        middle = since + _HOUR * max(1, (until - since) // _HOUR // 2)
        await _add_window_costs(app, run_id, since, middle, costs)
        await _add_window_costs(app, run_id, middle, until, costs)
        return
    for group in groups:
        attempt_id = (group.tags or {}).get(MODAL_ATTEMPT_TAG)
        for name, amount in group.cost_by_resource.items():
            key = (attempt_id, _MODAL_RESOURCES.get(name, SandboxResource.GPU))
            costs[key] = costs.get(key, 0.0) + float(amount)


def _runs_to_price(bind: Engine | Connection, now: datetime) -> tuple[list[Benchmark], dict[object, Org | None]]:
    with Session(bind) as session:
        runs = [
            run
            for run in CostRepository(session).modal_runs_to_price()
            if modal_usage_window(run)[1] + MODAL_SETTLE_MARGIN <= now
        ]
        return runs, {org_id: OrganizationRepository(session).get(org_id) for org_id in {run.org_id for run in runs}}


def _write_run_cost(bind: Engine | Connection, run: Benchmark, costs: ModalRunCosts, usage_end: datetime) -> bool:
    with Session(bind) as session:
        written = CostRepository(session).replace_modal_run_costs(benchmark_id=run.id, costs=costs, usage_end=usage_end)
        session.commit()
        return written


async def sweep_modal_costs(
    bind: Engine | Connection, *, now: datetime, read_modal_cost: ModalCostReader = read_modal_run_cost
) -> int:
    try:
        return await _sweep(bind, now, read_modal_cost)
    except Exception:
        logger.exception("Modal cost sweep failed")
        return 0


async def _sweep(bind: Engine | Connection, now: datetime, read_modal_cost: ModalCostReader) -> int:
    runs, orgs = await database_call(lambda: _runs_to_price(bind, now))
    written = 0
    for run in sorted(runs, key=lambda run: _as_utc(run.started_at)):
        org = orgs[run.org_id]
        if org is None:
            continue
        start, end = modal_usage_window(run)
        try:
            async with asyncio.timeout(MODAL_READ_TIMEOUT_SECONDS):
                costs = await read_modal_cost(org, run, start, end)
        except Exception as error:
            logger.warning(
                "Modal run cost unavailable",
                extra={"org_id": str(org.id), "benchmark_id": str(run.id), "error_type": type(error).__name__},
            )
            continue
        finished = _as_utc(cast(datetime, run.finished_at))
        if await database_call(lambda: _write_run_cost(bind, run, costs, finished)):
            written += 1
    logger.info("Modal cost sweep complete", extra={"cost_rows_written": written, "runs_ready": len(runs)})
    return written
