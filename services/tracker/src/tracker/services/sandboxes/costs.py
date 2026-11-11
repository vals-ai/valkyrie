from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime, timedelta
from itertools import groupby
from uuid import UUID

from benchmark_service import SandboxNotFoundError, SandboxProvider
from benchmark_service.sandbox import priced_provider_types
from sqlalchemy import Connection, Engine
from sqlmodel import Session

from tracker.database.models import Org
from tracker.database.session import database_call
from tracker.logging import get_logger
from tracker.repositories.cost import CostRepository, UnpricedSandbox
from tracker.repositories.organization import OrganizationRepository

logger = get_logger(__name__)

SWEEP_LOOKBACK = timedelta(days=7)

ProviderOpener = Callable[[Org, str, str], AbstractAsyncContextManager[SandboxProvider]]


@asynccontextmanager
async def open_sandbox_provider(org: Org, provider_type: str, secret_name: str) -> AsyncIterator[SandboxProvider]:
    from tracker.aws.resolver import deployment_aws_runtime
    from tracker.aws.services import CloudRuntimeFactory

    runtime = CloudRuntimeFactory.create_runtime(
        deployment_aws_runtime(org), sandbox_provider=provider_type, sandbox_provider_secret_name=secret_name
    )
    config = await runtime.get_sandbox_provider_config()
    async with config.create_provider() as provider:
        yield provider


def _load(
    bind: Engine | Connection, providers: Sequence[str], since: datetime
) -> tuple[list[UnpricedSandbox], dict[UUID, Org | None]]:
    with Session(bind, expire_on_commit=False) as session:
        pending = CostRepository(session).unpriced_sandboxes(providers, since)
        orgs = {org_id: OrganizationRepository(session).get(org_id) for org_id in {row.org_id for row in pending}}
        return pending, orgs


def _write(bind: Engine | Connection, row: UnpricedSandbox, amount: float, ended_at: datetime) -> bool:
    with Session(bind) as session:
        written = CostRepository(session).add_sandbox_cost(
            benchmark_id=row.benchmark_id,
            provider=row.provider,
            sandbox_id=row.sandbox_id,
            amount_usd=amount,
            usage_end=ended_at,
        )
        session.commit()
        return written


async def _price_account(
    bind: Engine | Connection, provider: SandboxProvider, rows: list[UnpricedSandbox], now: datetime
) -> int:
    written = 0
    for row in rows:
        try:
            sandbox = await provider.get_sandbox(row.sandbox_id)
        except SandboxNotFoundError:
            logger.warning(
                "Sandbox is gone before it was priced", extra={"provider": row.provider, "sandbox_id": row.sandbox_id}
            )
            continue
        if not sandbox.has_ended:
            continue
        amount = sandbox.cost_usd(now)
        if amount is not None and await database_call(lambda row=row, amount=amount: _write(bind, row, amount, now)):
            written += 1
    return written


def _account(row: UnpricedSandbox) -> tuple[str, str, str]:
    return str(row.org_id), row.provider, row.secret_name


async def sweep_priced_sandbox_costs(
    bind: Engine | Connection,
    *,
    now: datetime,
    providers: Sequence[str] | None = None,
    open_provider: ProviderOpener = open_sandbox_provider,
) -> int:
    providers = priced_provider_types() if providers is None else providers
    try:
        pending, orgs = await database_call(lambda: _load(bind, providers, now - SWEEP_LOOKBACK))
    except Exception:
        logger.exception("Priced sandbox cost sweep failed")
        return 0
    written = 0
    for (_, provider_type, secret_name), group in groupby(sorted(pending, key=_account), key=_account):
        rows = list(group)
        if (org := orgs.get(rows[0].org_id)) is None:
            continue
        try:
            async with open_provider(org, provider_type, secret_name) as provider:
                written += await _price_account(bind, provider, rows, now)
        except Exception as error:
            logger.warning(
                "Sandbox costs unavailable",
                extra={"org_id": str(rows[0].org_id), "provider": provider_type, "error_type": type(error).__name__},
            )
    logger.info(
        "Priced sandbox cost sweep complete", extra={"cost_rows_written": written, "sandboxes_pending": len(pending)}
    )
    return written
