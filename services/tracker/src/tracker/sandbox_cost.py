from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from functools import partial
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from threading import Event, Thread
from time import monotonic
from typing import Any, TypeVar, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from benchmark_service import DaytonaProviderConfig, SandboxProviderConfig
from daytona_analytics_api_client_async import ApiClient as AnalyticsApiClient
from daytona_analytics_api_client_async import Configuration as AnalyticsConfiguration
from daytona_analytics_api_client_async import UsageApi
from daytona_analytics_api_client_async.exceptions import ApiException
from daytona_analytics_api_client_async.models.models_sandbox_usage import ModelsSandboxUsage
from daytona_api_client_async import ApiClient, ApiKeysApi, ConfigApi, Configuration
from sqlalchemy import Connection, Engine
from sqlmodel import Session

from tracker.database.models import Org
from tracker.database.session import database_call, engine
from tracker.logging import get_logger
from tracker.modal_cost import sweep_modal_costs
from tracker.repositories.cost import CostRepository, SandboxUsage
from tracker.repositories.organization import OrganizationRepository
from tracker.repositories.provider_secret import ProviderSecretRepository
from tracker.config import AUTH_REQUIRED
from tracker.repositories.advisory_lock import PostgresAdvisoryLock, sandbox_cost_sweep_lock
from tracker.outbound_security import validate_custom_service_destination, validate_service_url_syntax

logger = get_logger(__name__)

USAGE_READ_TIMEOUT_SECONDS = 60.0
USAGE_PAGE_SIZE = 1000
USAGE_SLICE_WIDTH = timedelta(hours=1)
USAGE_CALL_INTERVAL_SECONDS = 10.0
USAGE_CALL_INTERVAL_STEP_SECONDS = 0.5
USAGE_CALL_INTERVAL_FLOOR_SECONDS = 1.0
USAGE_CALL_INTERVAL_CEILING_SECONDS = 30.0
RATE_LIMIT_ATTEMPTS = 4
RATE_LIMIT_RETRY_SECONDS = 5.0
RATE_LIMIT_MAX_WAIT_SECONDS = 30.0
SWEEP_TIME_BUDGET = timedelta(minutes=10)
USAGE_SETTLE_LAG = timedelta(hours=2)
ACCOUNTING_START = datetime(2026, 11, 1, tzinfo=UTC)
DAYTONA_PROVIDER = "daytona"
_SWEEP_INTERVAL_SECONDS = 900.0
_SHUTDOWN_TIMEOUT_SECONDS = 5.0


def _as_utc(moment: datetime) -> datetime:
    return moment if moment.utcoffset() is not None else moment.replace(tzinfo=ZoneInfo("UTC"))


@dataclass(frozen=True)
class UsageAccount:
    org_id: UUID
    secret_name: str


HourUsage = dict[str, SandboxUsage | None]
HourReader = Callable[[datetime, datetime], Awaitable[HourUsage]]
UsageOpener = Callable[[Org, str], AbstractAsyncContextManager[HourReader]]


async def _account_provider_config(org: Org, secret_name: str) -> SandboxProviderConfig:
    from tracker.aws.resolver import deployment_aws_runtime
    from tracker.aws.services import CloudRuntimeFactory

    runtime = CloudRuntimeFactory.create_runtime(
        deployment_aws_runtime(org), sandbox_provider=DAYTONA_PROVIDER, sandbox_provider_secret_name=secret_name
    )
    return await runtime.get_sandbox_provider_config()


_Read = TypeVar("_Read")


@asynccontextmanager
async def _usage_api(org: Org, secret_name: str) -> AsyncIterator[tuple[UsageApi, str]]:
    config = await _account_provider_config(org, secret_name)
    if not isinstance(config, DaytonaProviderConfig):
        raise TypeError(f"Expected a Daytona provider secret, got {type(config).__name__}")
    authorization = f"Bearer {config.DAYTONA_API_KEY}"

    async with (
        ApiClient(
            Configuration(host=config.DAYTONA_API_URL), header_name="Authorization", header_value=authorization
        ) as api_client,
        asyncio.timeout(USAGE_READ_TIMEOUT_SECONDS),
    ):
        analytics_url = (await ConfigApi(api_client).config_controller_get_config()).analytics_api_url
        organization_id = config.DAYTONA_ORGANIZATION_ID
        if not organization_id:
            api_key = await ApiKeysApi(api_client).get_current_api_key()
            organization_id = api_key.additional_properties.get("organizationId")
    if not analytics_url or not organization_id:
        raise RuntimeError("Daytona did not return an analytics URL and organization for this secret")
    analytics_url = validate_service_url_syntax(analytics_url)
    validate_custom_service_destination(
        analytics_url, org_name=org.name, auth_required=AUTH_REQUIRED, restrict_vals_hosts=False
    )

    async with AnalyticsApiClient(
        AnalyticsConfiguration(host=analytics_url), header_name="Authorization", header_value=authorization
    ) as analytics_client:
        yield UsageApi(analytics_client), organization_id


async def _read_once(read: Callable[[], Awaitable[_Read]]) -> _Read:
    await _usage_call_pacer.wait()
    try:
        async with asyncio.timeout(USAGE_READ_TIMEOUT_SECONDS):
            result = await read()
    except ApiException as error:
        if getattr(error, "status", None) == 429:
            _usage_call_pacer.rate_limited()
        raise
    _usage_call_pacer.succeeded()
    return result


async def _read_paced(read: Callable[[], Awaitable[_Read]]) -> _Read:
    for attempt in range(1, RATE_LIMIT_ATTEMPTS):
        try:
            return await _read_once(read)
        except ApiException as error:
            if error.status != 429:
                raise
            await asyncio.sleep(rate_limit_delay(error, attempt))
    return await _read_once(read)


async def read_usage_page(
    usage_api: UsageApi, organization_id: str, start: datetime, end: datetime, offset: int
) -> list[ModelsSandboxUsage]:
    client: Any = cast(Any, usage_api).api_client
    request = client.param_serialize(
        method="GET",
        resource_path="/organization/{organizationId}/usage/sandbox",
        path_params={"organizationId": organization_id},
        query_params=[
            ("from", _rfc3339(start)),
            ("to", _rfc3339(end)),
            ("limit", USAGE_PAGE_SIZE),
            ("offset", offset),
        ],
        header_params={"Accept": "application/json"},
        auth_settings=[],
    )
    response = await client.call_api(*request)
    await response.read()
    page = client.response_deserialize(
        response_data=response, response_types_map={"200": "List[ModelsSandboxUsage]"}
    ).data
    return cast(list[ModelsSandboxUsage], page)


@asynccontextmanager
async def open_daytona_usage(org: Org, secret_name: str) -> AsyncGenerator[HourReader]:
    async with _usage_api(org, secret_name) as (usage_api, organization_id):

        async def read_hour(start: datetime, end: datetime) -> HourUsage:
            records: list[ModelsSandboxUsage] = []
            offset = 0
            while True:
                page = await _read_paced(partial(read_usage_page, usage_api, organization_id, start, end, offset))
                records += page
                if len(page) < USAGE_PAGE_SIZE:
                    return sum_usage({record.sandbox_id: _usage_of(record)} for record in records if record.sandbox_id)
                offset += USAGE_PAGE_SIZE

        yield read_hour


async def daytona_last_ends(
    org: Org, secret_name: str, start: datetime, end: datetime, open_usage: UsageOpener = open_daytona_usage
) -> dict[str, datetime]:
    async with open_usage(org, secret_name) as read:
        usage = await read(start, end)
    return {
        sandbox_id: found.last_end
        for sandbox_id, found in usage.items()
        if found is not None and found.last_end is not None and found.last_end < end
    }


def _usage_of(record: ModelsSandboxUsage) -> SandboxUsage | None:
    if record.total_price is None or not record.sandbox_id:
        return None
    return SandboxUsage(
        sandbox_id=record.sandbox_id, total_price=float(record.total_price), last_end=_parse_time(record.last_end)
    )


def sum_usage(parts: Iterable[HourUsage]) -> HourUsage:
    total: HourUsage = {}
    for part in parts:
        for sandbox_id, usage in part.items():
            total[sandbox_id] = _added(total[sandbox_id], usage) if sandbox_id in total else usage
    return total


def _added(earlier: SandboxUsage | None, later: SandboxUsage | None) -> SandboxUsage | None:
    if earlier is None or later is None:
        return None
    last_end = None if earlier.last_end is None or later.last_end is None else max(earlier.last_end, later.last_end)
    return SandboxUsage(
        sandbox_id=earlier.sandbox_id, total_price=earlier.total_price + later.total_price, last_end=last_end
    )


class UsageCallPacer:
    def __init__(self) -> None:
        self.interval = USAGE_CALL_INTERVAL_SECONDS
        self._last_call = float("-inf")

    async def wait(self) -> None:
        if (delay := self._last_call + self.interval - monotonic()) > 0:
            await asyncio.sleep(delay)
        self._last_call = monotonic()

    def succeeded(self) -> None:
        self.interval = max(USAGE_CALL_INTERVAL_FLOOR_SECONDS, self.interval - USAGE_CALL_INTERVAL_STEP_SECONDS)

    def rate_limited(self) -> None:
        self.interval = min(USAGE_CALL_INTERVAL_CEILING_SECONDS, self.interval * 2)


_usage_call_pacer = UsageCallPacer()


def rate_limit_delay(error: ApiException, attempt: int) -> float:
    delay = _retry_after_seconds((error.headers or {}).get("Retry-After"))
    if delay is None:
        delay = RATE_LIMIT_RETRY_SECONDS * 2 ** (attempt - 1)
    return min(max(delay, 0.0), RATE_LIMIT_MAX_WAIT_SECONDS)


def _retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return (_as_utc(retry_at) - datetime.now(ZoneInfo("UTC"))).total_seconds()


class SandboxUsageUnpriced(Exception):
    def __init__(self, sandbox_id: str) -> None:
        super().__init__(f"Daytona has not priced every usage period of sandbox {sandbox_id} yet")


def _rfc3339(moment: datetime) -> str:
    return moment.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")


def _parse_time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _hour_of(moment: datetime) -> datetime:
    return moment.replace(minute=0, second=0, microsecond=0)


@dataclass
class _HourPlan:
    listed: HourUsage
    owners: dict[str, UUID]
    lifetime: set[str]

    @property
    def priced(self) -> list[tuple[UUID, SandboxUsage]]:
        return [
            (self.owners[sandbox_id], usage)
            for sandbox_id, usage in self.listed.items()
            if usage is not None and sandbox_id in self.owners and sandbox_id not in self.lifetime
        ]

    @property
    def unmatched(self) -> list[SandboxUsage | None]:
        return [usage for sandbox_id, usage in self.listed.items() if sandbox_id not in self.owners]

    @property
    def unmatched_usd(self) -> float:
        return sum(usage.total_price for usage in self.unmatched if usage is not None)


class _CursorSweep:
    def __init__(
        self,
        bind: Engine | Connection,
        *,
        deadline: float,
        open_usage: UsageOpener,
        stack: AsyncExitStack,
    ) -> None:
        self.bind = bind
        self.deadline = deadline
        self.open_usage = open_usage
        self.stack = stack
        self.readers: dict[UsageAccount, HourReader] = {}
        self.hours_read = 0
        self.written = 0
        self.unmatched_sandboxes = 0
        self.unmatched_usd = 0.0

    async def read(self, account: UsageAccount, org: Org, hour: datetime) -> bool:
        try:
            return await self._price_hour(account, org, hour)
        except Exception as error:
            logger.warning(
                "Sandbox usage hour unavailable, keeping the cursor",
                extra={
                    "org_id": str(account.org_id),
                    "hour": hour.isoformat(),
                    "error_type": type(error).__name__,
                    "http_status": getattr(error, "status", None),
                },
            )
            return False

    async def _price_hour(self, account: UsageAccount, org: Org, hour: datetime) -> bool:
        listed = await self._listing(account, org, hour)
        plan = await database_call(lambda: self._plan(listed))
        return await database_call(lambda: self._write_and_advance(account, hour, plan))

    def _plan(self, listed: HourUsage) -> _HourPlan:
        with Session(self.bind) as session:
            costs = CostRepository(session)
            owners = costs.sandbox_owners(DAYTONA_PROVIDER, list(listed))
            lifetime = costs.priced_source_ids(DAYTONA_PROVIDER, list(owners))
        unpriced = [
            sandbox_id
            for sandbox_id, usage in listed.items()
            if usage is None and sandbox_id in owners and sandbox_id not in lifetime
        ]
        if unpriced:
            raise SandboxUsageUnpriced(unpriced[0])
        return _HourPlan(listed=listed, owners=owners, lifetime=lifetime)

    def _write_and_advance(self, account: UsageAccount, hour: datetime, plan: _HourPlan) -> bool:
        with Session(self.bind) as session:
            costs = CostRepository(session)
            written = self._write(costs, hour, plan.priced)
            if plan.unmatched:
                costs.record_unmatched_usage(account.org_id, account.secret_name, hour, plan.unmatched)
            if not costs.advance_usage_cursor(account.org_id, account.secret_name, hour, hour + USAGE_SLICE_WIDTH):
                session.rollback()
                logger.warning(
                    "Sandbox usage cursor moved by another worker",
                    extra={"org_id": str(account.org_id), "hour": hour.isoformat()},
                )
                return False
            session.commit()
        self.written += written
        self.unmatched_sandboxes += len(plan.unmatched)
        self.unmatched_usd += plan.unmatched_usd
        logger.info(
            "Sandbox usage hour priced",
            extra={
                "org_id": str(account.org_id),
                "hour": hour.isoformat(),
                "listing_rows_written": written,
                "sandboxes_with_lifetime_cost": len(plan.lifetime),
                "unmatched_sandboxes": len(plan.unmatched),
                "unmatched_usd": plan.unmatched_usd,
            },
        )
        return True

    async def _listing(self, account: UsageAccount, org: Org, hour: datetime) -> HourUsage:
        if account not in self.readers:
            self.readers[account] = await self.stack.enter_async_context(self.open_usage(org, account.secret_name))
        async with asyncio.timeout(self.deadline - monotonic()):
            usage = await self.readers[account](hour, hour + USAGE_SLICE_WIDTH)
        self.hours_read += 1
        return usage

    def _write(self, costs: CostRepository, hour: datetime, priced: list[tuple[UUID, SandboxUsage]]) -> int:
        return sum(
            costs.add_sandbox_hour_cost(benchmark_id=benchmark_id, provider=DAYTONA_PROVIDER, hour=hour, usage=usage)
            for benchmark_id, usage in priced
        )


async def sweep_sandbox_costs(
    bind: Engine | Connection,
    *,
    now: datetime,
    open_usage: UsageOpener = open_daytona_usage,
) -> int:
    deadline = monotonic() + SWEEP_TIME_BUDGET.total_seconds()
    newest_hour = _hour_of(now - USAGE_SETTLE_LAG) - USAGE_SLICE_WIDTH
    accounts, cursors, last_hours = await database_call(lambda: _load_accounts_and_cursors(bind, newest_hour))
    async with AsyncExitStack() as stack:
        sweep = _CursorSweep(
            bind,
            deadline=deadline,
            open_usage=open_usage,
            stack=stack,
        )
        await _read_ready_hours(sweep, accounts, cursors, last_hours)
    _log_sweep(sweep, cursors, last_hours)
    return sweep.written


def _load_accounts_and_cursors(
    bind: Engine | Connection, newest_hour: datetime
) -> tuple[dict[UsageAccount, Org], dict[UsageAccount, datetime], dict[UsageAccount, datetime]]:
    accounts: dict[UsageAccount, Org] = {}
    cursors: dict[UsageAccount, datetime] = {}
    last_hours: dict[UsageAccount, datetime] = {}
    with Session(bind, expire_on_commit=False) as session:
        costs = CostRepository(session)
        configured = {
            UsageAccount(org_id=secret.org_id, secret_name=secret.secret_name)
            for secret in ProviderSecretRepository(session).for_provider(DAYTONA_PROVIDER)
        }
        last_used: dict[UsageAccount, datetime | None] = {
            UsageAccount(org_id=use.org_id, secret_name=use.secret_name): use.last_finished_at
            for use in costs.sandbox_accounts_in_use(DAYTONA_PROVIDER, ACCOUNTING_START)
        }
        for account in configured | last_used.keys():
            if (org := OrganizationRepository(session).get(account.org_id)) is None:
                continue
            last_finished_at = None if account in configured else last_used[account]
            last_hour = (
                newest_hour
                if last_finished_at is None
                else min(newest_hour, _hour_of(_as_utc(last_finished_at)) + USAGE_SLICE_WIDTH)
            )
            cursor = _as_utc(costs.usage_cursor(account.org_id, account.secret_name, ACCOUNTING_START))
            if account in configured or cursor <= last_hour:
                accounts[account] = org
                cursors[account] = cursor
                last_hours[account] = last_hour
        session.commit()
    return accounts, cursors, last_hours


async def _read_ready_hours(
    sweep: _CursorSweep,
    accounts: dict[UsageAccount, Org],
    cursors: dict[UsageAccount, datetime],
    last_hours: dict[UsageAccount, datetime],
) -> None:
    stopped: set[UsageAccount] = set()
    while monotonic() < sweep.deadline:
        ready = [account for account, hour in cursors.items() if hour <= last_hours[account] and account not in stopped]
        if not ready:
            return
        account = min(ready, key=lambda account: cursors[account])
        if await sweep.read(account, accounts[account], cursors[account]):
            cursors[account] += USAGE_SLICE_WIDTH
        else:
            stopped.add(account)


def _log_sweep(
    sweep: _CursorSweep, cursors: dict[UsageAccount, datetime], last_hours: dict[UsageAccount, datetime]
) -> None:
    logger.info(
        "Sandbox cost sweep complete",
        extra={
            "cost_rows_written": sweep.written,
            "usage_hours_read": sweep.hours_read,
            "unmatched_sandboxes": sweep.unmatched_sandboxes,
            "unmatched_usd": sweep.unmatched_usd,
            "call_interval_seconds": _usage_call_pacer.interval,
            "hours_behind": max(
                (
                    int((last_hours[account] + USAGE_SLICE_WIDTH - hour) / USAGE_SLICE_WIDTH)
                    for account, hour in cursors.items()
                ),
                default=0,
            ),
        },
    )


async def sweep_sandbox_costs_as_leader(
    leader: PostgresAdvisoryLock,
    bind: Engine,
    *,
    now: datetime,
) -> int | None:
    if not await leader.still_held():
        if not await leader.acquire():
            logger.debug("Sandbox cost sweep skipped because another worker leads")
            return None
        logger.info("Sandbox cost sweep leadership acquired", extra={"pid": os.getpid()})
    return await sweep_modal_costs(bind, now=now) + await sweep_sandbox_costs(bind, now=now)


def run_sandbox_cost_loop(stop_event: Event, *, interval_seconds: float = _SWEEP_INTERVAL_SECONDS) -> None:
    leader = sandbox_cost_sweep_lock(engine)
    try:
        while not stop_event.is_set():
            try:
                asyncio.run(
                    sweep_sandbox_costs_as_leader(
                        leader,
                        engine,
                        now=datetime.now(ZoneInfo("UTC")),
                    )
                )
            except Exception:
                logger.exception("Sandbox cost sweep failed")
            stop_event.wait(interval_seconds)
    finally:
        asyncio.run(leader.release())


class AutomaticSandboxCost:
    def __init__(
        self,
        *,
        interval_seconds: float = _SWEEP_INTERVAL_SECONDS,
        shutdown_timeout_seconds: float = _SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        self._stop_event = Event()
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._thread = Thread(
            target=run_sandbox_cost_loop,
            args=(self._stop_event,),
            kwargs={"interval_seconds": interval_seconds},
            name="sandbox-cost",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(self._shutdown_timeout_seconds)
        if self._thread.is_alive():
            logger.error("Sandbox cost thread did not stop before shutdown")
