import asyncio
import math
import threading
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from benchmark_service import DaytonaProviderConfig, ImageSource, Resources
from daytona_analytics_api_client_async import ApiClient as AnalyticsApiClient
from daytona_analytics_api_client_async import Configuration as AnalyticsConfiguration
from daytona_analytics_api_client_async import UsageApi
from daytona_analytics_api_client_async.exceptions import ApiException
from daytona_analytics_api_client_async.rest import RESTResponse
from sqlalchemy import Engine
from sqlmodel import Session, col, select

import tracker.sandbox as sandbox_module
import tracker.sandbox_cost as sandbox_cost
from tests.factories import make_benchmark, make_task
from tests.utils import TEST_ORG_ID
from tracker.repositories.cost import CostRepository
from tracker.aws.runtime import AWSResources
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    LocalBenchmarkArguments,
    Org,
    OrgProviderSecret,
    RunCost,
    SandboxCostCursor,
    SandboxRecord,
    Task,
)
from tracker.local.resources import LocalResources
from tracker.repositories.advisory_lock import PostgresAdvisoryLock
from tracker.sandbox import SandboxCreationState, create_sandbox
from tracker.sandbox_cost import (
    ACCOUNTING_START,
    RATE_LIMIT_ATTEMPTS,
    RATE_LIMIT_RETRY_SECONDS,
    SWEEP_TIME_BUDGET,
    USAGE_CALL_INTERVAL_FLOOR_SECONDS,
    USAGE_CALL_INTERVAL_SECONDS,
    USAGE_CALL_INTERVAL_STEP_SECONDS,
    USAGE_PAGE_SIZE,
    USAGE_SETTLE_LAG,
    AutomaticSandboxCost,
    SandboxLink,
    HourReader,
    HourUsage,
    SandboxUsage,
    UsageOpener,
    sweep_sandbox_costs,
)

NOW = datetime(2026, 10, 21, 18, tzinfo=UTC)
START = NOW - timedelta(hours=6)
SECRET_NAME = "test-provider-secret"


@pytest.fixture(autouse=True)
def accounting_start(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sandbox_cost, "ACCOUNTING_START", START)


def _as_utc(moment: datetime) -> datetime:
    return moment if moment.utcoffset() is not None else moment.replace(tzinfo=UTC)


def _configure(session: Session, org_id: UUID = TEST_ORG_ID, secret: str = SECRET_NAME) -> None:
    session.add(OrgProviderSecret(org_id=org_id, provider="daytona", secret_name=secret))
    session.commit()


def _run(
    session: Session,
    *,
    status: BenchmarkStatus,
    org: Org | None = None,
    hours_ago: float = 6,
    secret: str | None = None,
    region: str | None = None,
    local_root: str | None = None,
) -> tuple[Benchmark, Task]:
    benchmark = make_benchmark(
        status=status, started_at=NOW - timedelta(hours=hours_ago), session=None, **({"org_id": org.id} if org else {})
    )
    if secret is not None:
        benchmark.arguments = benchmark.arguments.model_copy(update={"sandbox_provider_secret_name": secret})
    if local_root is not None:
        benchmark.arguments = LocalBenchmarkArguments(
            contract=benchmark.arguments.contract,
            concurrency=1,
            sandbox_provider="daytona",
            sandbox_provider_secret_name="test-provider-secret",
            properties=LocalResources(data_root=Path(local_root)),
        )
    if region is not None:
        resources = AWSResources(region=region, s3_bucket="bucket", log_group="logs", log_retention_days=7)
        benchmark.arguments = benchmark.arguments.model_copy(update={"properties": resources})
    task = make_task(benchmark, "task-1")
    session.add_all([benchmark, task])
    session.commit()

    return benchmark, task


def _other_org(session: Session, name: str) -> Org:
    org = Org(name=name, storage_key=f"{name}-bucket")
    session.add(org)
    session.commit()
    return org


def _link(benchmark: Benchmark) -> SandboxLink:
    return SandboxLink(benchmark_id=benchmark.id, provider="daytona")


def _save_sandbox(session: Session, benchmark: Benchmark, sandbox_id: str) -> None:
    session.add(SandboxRecord(provider="daytona", sandbox_id=sandbox_id, benchmark_id=benchmark.id))
    session.commit()


def _usage(sandbox_id: str, price: float, *, last_end: datetime | None = NOW - timedelta(hours=2)) -> SandboxUsage:
    return SandboxUsage(
        sandbox_id=sandbox_id,
        total_price=price,
        last_end=last_end,
    )


FakeHourReader = Callable[[Org, str, datetime, datetime], Awaitable[HourUsage]]


def _opener(read: FakeHourReader) -> UsageOpener:
    @asynccontextmanager
    async def open_usage(org: Org, secret: str) -> AsyncGenerator[HourReader]:
        async def read_hour(start: datetime, end: datetime) -> HourUsage:
            return await read(org, secret, start, end)

        yield read_hour

    return open_usage


def _hours(start: datetime, count: int) -> list[datetime]:
    return [start + timedelta(hours=hour) for hour in range(count)]


async def read_daytona_usage(org: Org, start: datetime, end: datetime) -> dict[str, SandboxUsage]:
    async with sandbox_cost.open_daytona_usage(org, SECRET_NAME) as read_hour:
        hours = [
            await read_hour(hour, min(hour + timedelta(hours=1), end))
            for hour in _hours(start, math.ceil((end - start) / timedelta(hours=1)))
        ]
    return {sandbox_id: found for sandbox_id, found in sandbox_cost.sum_usage(hours).items() if found is not None}


async def _open_sandbox(sandbox_id: str, link: SandboxLink, *, fail: bool) -> None:
    async with create_sandbox(
        provider=AsyncMock(),
        sandbox_name="task-1",
        source=ImageSource(image="ghcr.io/vals/swebench:latest"),
        resources=Resources(vcpu=1, memory=2, disk=3),
        creation_semaphore=asyncio.Semaphore(1),
        creation_state=SandboxCreationState(cleanup_pending=False),
        sandbox_link=link,
    ) as sandbox:
        assert sandbox.id == sandbox_id
        if fail:
            raise RuntimeError("agent setup failed")


@pytest.mark.description("The link row create_sandbox writes once the provider names the sandbox.")
class TestRecordSandbox:
    @pytest.mark.description("Errored and retried sandbox attempts are all saved.")
    async def test_saves_errored_and_retried_attempts(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.IN_PROGRESS)
        created = iter([AsyncMock(id="sandbox-attempt-1"), AsyncMock(id="sandbox-attempt-2")])

        async def fake_create(*_args: Any, **_kwargs: Any) -> AsyncMock:
            return next(created)

        monkeypatch.setattr(sandbox_module, "_create_sandbox", fake_create)
        monkeypatch.setattr(sandbox_module, "delete_sandbox", AsyncMock(return_value=True))

        with pytest.raises(RuntimeError, match="agent setup failed"):
            await _open_sandbox("sandbox-attempt-1", _link(benchmark), fail=True)
        await _open_sandbox("sandbox-attempt-2", _link(benchmark), fail=False)

        rows = database_session.exec(select(SandboxRecord).order_by(col(SandboxRecord.sandbox_id))).all()

        assert [row.sandbox_id for row in rows] == ["sandbox-attempt-1", "sandbox-attempt-2"]
        assert all(row.benchmark_id == benchmark.id for row in rows)

    @pytest.mark.description("A failed cost link write still deletes the sandbox.")
    async def test_failed_write_still_deletes_the_sandbox(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.IN_PROGRESS)
        delete = AsyncMock(return_value=True)

        async def fake_create(*_args: Any, **_kwargs: Any) -> AsyncMock:
            return AsyncMock(id="sandbox-1")

        monkeypatch.setattr(sandbox_module, "_create_sandbox", fake_create)
        monkeypatch.setattr(sandbox_module, "delete_sandbox", delete)
        monkeypatch.setattr(sandbox_cost, "engine", None)

        await _open_sandbox("sandbox-1", _link(benchmark), fail=False)

        delete.assert_awaited_once()
        assert database_session.exec(select(SandboxRecord)).all() == []

    @pytest.mark.description("Cancelling while the sandbox link is written still deletes the sandbox after the write.")
    async def test_cancel_during_the_link_write_still_deletes_the_sandbox(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.IN_PROGRESS)
        events: list[str] = []

        def record_delete(*_args: Any, **_kwargs: Any) -> bool:
            events.append("deleted")
            return True

        delete = AsyncMock(side_effect=record_delete)
        write_started = threading.Event()
        release_write = threading.Event()

        async def fake_create(*_args: Any, **_kwargs: Any) -> AsyncMock:
            return AsyncMock(id="sandbox-1")

        def blocked_write(*_args: Any, **_kwargs: Any) -> None:
            write_started.set()
            release_write.wait(5)
            events.append("written")

        monkeypatch.setattr(sandbox_module, "_create_sandbox", fake_create)
        monkeypatch.setattr(sandbox_module, "delete_sandbox", delete)
        monkeypatch.setattr(sandbox_module, "record_sandbox", blocked_write)

        opening = asyncio.create_task(_open_sandbox("sandbox-1", _link(benchmark), fail=False))
        await asyncio.to_thread(write_started.wait, 5)
        opening.cancel()
        await asyncio.sleep(0.05)
        release_write.set()
        with pytest.raises(asyncio.CancelledError):
            await opening

        assert events == ["written", "deleted"]


@pytest.mark.description("The cost rows written for one sandbox hour, and costs that are not sandboxes.")
class TestAddSandboxHourCost:
    @pytest.mark.description(
        "An hour's cost is keyed by sandbox and hour, and ends at the sandbox's last use in that hour."
    )
    def test_keys_the_row_by_sandbox_and_hour_and_ends_inside_the_hour(self, database_session: Session) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        hour = START
        costs = CostRepository(database_session)

        costs.add_sandbox_hour_cost(
            benchmark_id=benchmark.id,
            provider="daytona",
            hour=hour,
            usage=_usage("sb-1", 1, last_end=hour + timedelta(minutes=20)),
        )
        costs.add_sandbox_hour_cost(
            benchmark_id=benchmark.id,
            provider="daytona",
            hour=hour + timedelta(hours=1),
            usage=_usage("sb-1", 1, last_end=None),
        )
        database_session.commit()

        rows = database_session.exec(
            select(RunCost.source_id, RunCost.usage_end).order_by(col(RunCost.source_id))
        ).all()
        assert [(source_id, _as_utc(end)) for source_id, end in rows] == [
            (f"sb-1:{hour.isoformat()}", hour + timedelta(minutes=20)),
            (f"sb-1:{(hour + timedelta(hours=1)).isoformat()}", hour + timedelta(hours=2, microseconds=-1)),
        ]

    @pytest.mark.description(
        "The last hour of a month counts in that month, whether the sandbox ended inside it or ran on past it."
    )
    @pytest.mark.parametrize(
        "last_end",
        [None, datetime(2026, 11, 1, 0, 30, tzinfo=UTC), datetime(2026, 11, 1, tzinfo=UTC)],
        ids=["still-running", "ended-after-the-hour", "ended-at-midnight"],
    )
    def test_the_last_hour_of_a_month_counts_in_that_month(
        self, database_session: Session, last_end: datetime | None
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        costs = CostRepository(database_session)
        october_end = datetime(2026, 11, 1, tzinfo=UTC)
        costs.add_sandbox_hour_cost(
            benchmark_id=benchmark.id,
            provider="daytona",
            hour=october_end - timedelta(hours=1),
            usage=_usage("sb-1", 0.5, last_end=last_end),
        )
        database_session.commit()

        _, october = costs.by_project(TEST_ORG_ID, october_end - timedelta(days=1), october_end)
        _, november = costs.by_project(TEST_ORG_ID, october_end, october_end + timedelta(days=1))

        assert ([cost.amount_usd for cost in october], november) == ([0.5], [])

    @pytest.mark.description(
        "A spanning sandbox receives exactly one cost row per hour, even when an hour is written twice."
    )
    def test_spanning_sandbox_has_one_row_per_hour(self, database_session: Session) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        costs = CostRepository(database_session)

        written = [
            costs.add_sandbox_hour_cost(
                benchmark_id=benchmark.id, provider="daytona", hour=hour, usage=_usage("sb-1", 0.5)
            )
            for _ in range(2)
            for hour in _hours(START, 3)
        ]
        database_session.commit()

        assert written == [True] * 3 + [False] * 3
        rows = database_session.exec(select(RunCost.source_id, RunCost.amount_usd)).all()
        assert sorted(rows) == [(f"sb-1:{hour.isoformat()}", 0.5) for hour in _hours(START, 3)]

    @pytest.mark.description("A `search` cost with no `source_id` is saved.")
    def test_non_sandbox_cost_without_a_source_id_is_saved(self, database_session: Session) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        database_session.add(RunCost(benchmark_id=benchmark.id, cost_type="search", provider="tavily", amount_usd=0.01))
        database_session.commit()

        rows = database_session.exec(select(RunCost.cost_type, RunCost.source_id)).all()

        assert rows == [("search", None)]

    @pytest.mark.description("Non sandbox costs that share a source ID are both saved.")
    def test_non_sandbox_costs_sharing_a_source_id_are_both_saved(self, database_session: Session) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        database_session.add_all(
            [
                RunCost(
                    benchmark_id=benchmark.id, cost_type="search", source_id="req-1", provider="tavily", amount_usd=0.01
                )
                for _ in range(2)
            ]
        )
        database_session.commit()

        rows = database_session.exec(select(RunCost.source_id)).all()

        assert rows == ["req-1", "req-1"]


def _hour_rows(session: Session) -> dict[str, float]:
    return dict(session.exec(select(RunCost.source_id, RunCost.amount_usd)).all())


def _priced_per_sandbox(session: Session) -> dict[str, float]:
    totals: dict[str, float] = {}
    for source_id, amount in _hour_rows(session).items():
        sandbox_id = source_id.split(":", 1)[0]
        totals[sandbox_id] = totals.get(sandbox_id, 0.0) + amount
    return totals


def _cursor(session: Session, org_id: UUID = TEST_ORG_ID, secret: str = SECRET_NAME) -> datetime | None:
    session.expire_all()
    cursor = session.get(SandboxCostCursor, (org_id, secret))
    return None if cursor is None else _as_utc(cursor.next_hour)


@pytest.mark.description(
    "The sweep reads each clock hour of an account's Daytona usage once, oldest first, from a stored cursor."
)
class TestHourlyCursor:
    @pytest.mark.description("Overlapping runs share one listing read per organization hour.")
    async def test_overlapping_runs_read_each_hour_once(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        _configure(database_session)
        first, _ = _run(database_session, status=BenchmarkStatus.FINISHED, hours_ago=6)
        second, _ = _run(database_session, status=BenchmarkStatus.FINISHED, hours_ago=5)
        _save_sandbox(database_session, first, "sb-first")
        _save_sandbox(database_session, second, "sb-second")
        daytona = FakeDaytona(
            {
                "sb-first": _sandbox(START + timedelta(minutes=30), 150),
                "sb-second": _sandbox(START + timedelta(hours=1, minutes=30), 60),
            }
        )
        daytona.install(monkeypatch)

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW)

        assert daytona.listing_reads == [(hour, hour + timedelta(hours=1)) for hour in _hours(START, 4)]
        assert written == 5
        owners = dict(database_session.exec(select(RunCost.source_id, RunCost.benchmark_id)).all())
        assert owners == {
            f"sb-first:{START.isoformat()}": first.id,
            f"sb-first:{(START + timedelta(hours=1)).isoformat()}": first.id,
            f"sb-first:{(START + timedelta(hours=2)).isoformat()}": first.id,
            f"sb-second:{(START + timedelta(hours=1)).isoformat()}": second.id,
            f"sb-second:{(START + timedelta(hours=2)).isoformat()}": second.id,
        }
        assert _cursor(database_session) == START + timedelta(hours=4)

    @pytest.mark.description("A run totals all its hourly sandbox costs.")
    async def test_run_cost_sums_hourly_rows(self, monkeypatch: pytest.MonkeyPatch, database_session: Session) -> None:
        _configure(database_session)
        run, _ = _run(database_session, status=BenchmarkStatus.FINISHED, hours_ago=6)
        _save_sandbox(database_session, run, "sb-1")
        daytona = FakeDaytona({"sb-1": _sandbox(START, 180, rate=0.25 / 3600)})
        daytona.install(monkeypatch)

        await sweep_sandbox_costs(database_session.get_bind(), now=NOW)

        _, costs = CostRepository(database_session).by_project(TEST_ORG_ID, START, NOW)
        assert [cost.amount_usd for cost in costs] == [pytest.approx(0.25)] * 3
        assert sum(cost.amount_usd for cost in costs) == pytest.approx(0.75)

    @pytest.mark.description("A failed hour keeps its cursor until a successful retry, whatever the failure.")
    @pytest.mark.parametrize(
        "failure",
        [
            ApiException(status=503),
            ApiException(status=429),
            TimeoutError(),
        ],
        ids=["server-error", "rate-limited-after-retries", "timeout"],
    )
    async def test_failed_hour_keeps_cursor_and_retries(self, database_session: Session, failure: Exception) -> None:
        _configure(database_session)
        run, _ = _run(database_session, status=BenchmarkStatus.FINISHED, hours_ago=6)
        _save_sandbox(database_session, run, "sb-1")
        failing = START + timedelta(hours=1)
        reads: list[datetime] = []
        outage = [True]

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            reads.append(start)
            if start == failing and outage[0]:
                raise failure
            return {"sb-1": _usage("sb-1", 0.5, last_end=start + timedelta(minutes=30))}

        bind = database_session.get_bind()
        first = await sweep_sandbox_costs(bind, now=NOW, open_usage=_opener(read_hour))
        stuck_at = _cursor(database_session)
        outage[0] = False
        second = await sweep_sandbox_costs(bind, now=NOW, open_usage=_opener(read_hour))

        assert (first, second) == (1, 3)
        assert stuck_at == failing
        assert reads == [START, failing, failing, failing + timedelta(hours=1), failing + timedelta(hours=2)]
        assert sorted(_hour_rows(database_session)) == [f"sb-1:{hour.isoformat()}" for hour in _hours(START, 4)]

    @pytest.mark.description("A restarted worker continues from its stored cursor.")
    async def test_restart_uses_stored_cursor(self, database_session: Session) -> None:
        _configure(database_session)
        reads: list[datetime] = []

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            reads.append(start)
            return {}

        bind = database_session.get_bind()
        await sweep_sandbox_costs(
            bind, now=START + timedelta(hours=1) + USAGE_SETTLE_LAG, open_usage=_opener(read_hour)
        )
        await sweep_sandbox_costs(bind, now=NOW, open_usage=_opener(read_hour))

        assert reads == _hours(START, 4)

    @pytest.mark.description("An hour newer than the settle lag waits for a later sweep.")
    async def test_an_hour_inside_the_settle_lag_is_not_read(self, database_session: Session) -> None:
        _configure(database_session)
        reads: list[datetime] = []

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            reads.append(start)
            return {}

        await sweep_sandbox_costs(database_session.get_bind(), now=NOW, open_usage=_opener(read_hour))

        assert max(reads) + timedelta(hours=1) == NOW - USAGE_SETTLE_LAG

    @pytest.mark.description(
        "Every configured Daytona account gets its own cursor from the accounting start, and an org with none is not read."
    )
    async def test_each_configured_account_starts_at_the_accounting_start(self, database_session: Session) -> None:
        other = _other_org(database_session, "other")
        _other_org(database_session, "unconfigured")
        _configure(database_session)
        _configure(database_session, other.id, "other-secret")
        reads: list[tuple[str, datetime]] = []

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            reads.append((secret, start))
            return {}

        await sweep_sandbox_costs(database_session.get_bind(), now=NOW, open_usage=_opener(read_hour))

        assert sorted(reads) == sorted(
            (secret, hour) for secret in (SECRET_NAME, "other-secret") for hour in _hours(START, 4)
        )
        assert _cursor(database_session, other.id, "other-secret") == START + timedelta(hours=4)

    @pytest.mark.description("A run still on a replaced Daytona secret keeps that account read and priced.")
    async def test_a_run_on_a_replaced_secret_is_still_priced(self, database_session: Session) -> None:
        _configure(database_session, secret="new-secret")
        run, _ = _run(database_session, status=BenchmarkStatus.IN_PROGRESS, secret=SECRET_NAME)
        _save_sandbox(database_session, run, "sb-1")
        reads: list[tuple[str, datetime]] = []

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            reads.append((secret, start))
            return {"sb-1": _usage("sb-1", 0.5, last_end=end)} if secret == SECRET_NAME else {}

        await sweep_sandbox_costs(database_session.get_bind(), now=NOW, open_usage=_opener(read_hour))

        assert sorted(reads) == sorted(
            (secret, hour) for secret in (SECRET_NAME, "new-secret") for hour in _hours(START, 4)
        )
        assert _hour_rows(database_session) == {f"sb-1:{hour.isoformat()}": 0.5 for hour in _hours(START, 4)}

    @pytest.mark.description(
        "A deleted Daytona secret is read through the hour after its last run finished, and then no longer."
    )
    async def test_a_deleted_secret_is_read_until_the_hour_after_its_last_run(self, database_session: Session) -> None:
        run, _ = _run(database_session, status=BenchmarkStatus.FINISHED, secret=SECRET_NAME)
        run.finished_at = START + timedelta(hours=1, minutes=30)
        database_session.add(run)
        database_session.commit()
        _save_sandbox(database_session, run, "sb-1")
        reads: list[datetime] = []

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            reads.append(start)
            return {"sb-1": _usage("sb-1", 0.5, last_end=end)}

        bind = database_session.get_bind()
        await sweep_sandbox_costs(bind, now=NOW, open_usage=_opener(read_hour))
        await sweep_sandbox_costs(bind, now=NOW + timedelta(hours=1), open_usage=_opener(read_hour))

        assert reads == _hours(START, 3)
        assert _cursor(database_session) == START + timedelta(hours=3)

    @pytest.mark.description("A stopped run with no finish time is read through the hour after its last task.")
    async def test_a_stopped_run_without_a_finish_time_is_read_until_its_last_task(
        self, database_session: Session
    ) -> None:
        run, task = _run(database_session, status=BenchmarkStatus.STOPPED, secret=SECRET_NAME)
        run.finished_at = None
        task.started_at = START + timedelta(hours=2, minutes=30)
        database_session.add_all([run, task])
        database_session.commit()
        _save_sandbox(database_session, run, "sb-1")
        reads: list[datetime] = []

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            reads.append(start)
            return {"sb-1": _usage("sb-1", 0.5, last_end=end)}

        bind = database_session.get_bind()
        await sweep_sandbox_costs(bind, now=NOW, open_usage=_opener(read_hour))
        await sweep_sandbox_costs(bind, now=NOW + timedelta(hours=1), open_usage=_opener(read_hour))

        assert reads == _hours(START, 4)
        assert _cursor(database_session) == START + timedelta(hours=4)

    @pytest.mark.description(
        "An unconfigured secret whose runs have no sandbox or finished before the accounting start is not read."
    )
    async def test_an_unconfigured_secret_without_priceable_runs_is_not_read(self, database_session: Session) -> None:
        _run(database_session, status=BenchmarkStatus.IN_PROGRESS, secret="no-sandbox-secret")
        old, _ = _run(database_session, status=BenchmarkStatus.FINISHED, secret="old-secret", hours_ago=30)
        old.finished_at = START - timedelta(hours=1)
        database_session.add(old)
        database_session.commit()
        _save_sandbox(database_session, old, "sb-old")
        reads: list[str] = []

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            reads.append(secret)
            return {}

        await sweep_sandbox_costs(database_session.get_bind(), now=NOW, open_usage=_opener(read_hour))

        assert reads == []

    @pytest.mark.description("A sandbox already priced for its whole life by the older sweep gets no hourly rows.")
    async def test_a_sandbox_with_a_lifetime_row_is_not_priced_again(self, database_session: Session) -> None:
        _configure(database_session)
        run, _ = _run(database_session, status=BenchmarkStatus.FINISHED, hours_ago=6)
        _save_sandbox(database_session, run, "sb-old")
        _save_sandbox(database_session, run, "sb-new")
        database_session.add(
            RunCost(benchmark_id=run.id, cost_type="sandbox", source_id="sb-old", provider="daytona", amount_usd=2.0)
        )
        database_session.commit()

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            return {sandbox_id: _usage(sandbox_id, 0.5, last_end=end) for sandbox_id in ("sb-old", "sb-new")}

        await sweep_sandbox_costs(database_session.get_bind(), now=NOW, open_usage=_opener(read_hour))

        assert _hour_rows(database_session) == {"sb-old": 2.0} | {
            f"sb-new:{hour.isoformat()}": 0.5 for hour in _hours(START, 4)
        }

    @pytest.mark.description("A worker whose cursor another worker already moved writes nothing for that hour.")
    async def test_a_worker_that_lost_the_cursor_writes_nothing_for_that_hour(self, database_session: Session) -> None:
        _configure(database_session)
        run, _ = _run(database_session, status=BenchmarkStatus.FINISHED, hours_ago=6)
        _save_sandbox(database_session, run, "sb-1")
        bind = database_session.get_bind()

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            with Session(bind) as other_worker:
                CostRepository(other_worker).advance_usage_cursor(TEST_ORG_ID, SECRET_NAME, start, end)
                other_worker.commit()
            return {"sb-1": _usage("sb-1", 0.5, last_end=end)}

        written = await sweep_sandbox_costs(bind, now=NOW, open_usage=_opener(read_hour))

        assert written == 0
        assert _hour_rows(database_session) == {}
        assert _cursor(database_session) == START + timedelta(hours=1)


@pytest.mark.description("The Daytona reader that the hourly sweep uses.")
class TestReadDaytonaUsage:
    @pytest.mark.description("An analytics URL on an internal host is refused.")
    async def test_refuses_an_analytics_url_on_an_internal_host(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)

        async def provider_config(_org: Org, _secret: str) -> DaytonaProviderConfig:
            return DaytonaProviderConfig(
                DAYTONA_API_KEY="test-key",
                DAYTONA_API_URL="https://daytona.test/api",
                DAYTONA_TARGET="us",
                DAYTONA_ORGANIZATION_ID="org-1",
            )

        class FakeConfigApi:
            def __init__(self, _client: object) -> None:
                pass

            async def config_controller_get_config(self) -> SimpleNamespace:
                return SimpleNamespace(analytics_api_url="http://169.254.169.254/latest")

        monkeypatch.setattr(sandbox_cost, "_account_provider_config", provider_config)
        monkeypatch.setattr(sandbox_cost, "ConfigApi", FakeConfigApi)
        monkeypatch.setattr(sandbox_cost, "AUTH_REQUIRED", True)

        def not_shared(_org_name: str) -> bool:
            return False

        monkeypatch.setattr("tracker.secret_access.may_use_shared_secrets", not_shared)
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        with pytest.raises(ValueError):
            await read_daytona_usage(org, NOW - timedelta(hours=1), NOW)

    @pytest.mark.description("The Daytona org comes from the API key, and each record is parsed.")
    async def test_reads_the_org_from_the_api_key_and_parses_each_record(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        requests: list[tuple[str, datetime, datetime, int]] = []

        async def provider_config(_org: Org, _secret: str) -> DaytonaProviderConfig:
            return DaytonaProviderConfig(
                DAYTONA_API_KEY="test-key", DAYTONA_API_URL="https://daytona.test/api", DAYTONA_TARGET="us"
            )

        class FakeConfigApi:
            def __init__(self, _client: object) -> None:
                pass

            async def config_controller_get_config(self) -> SimpleNamespace:
                return SimpleNamespace(analytics_api_url="https://analytics.daytona.test")

        class FakeApiKeysApi:
            def __init__(self, _client: object) -> None:
                pass

            async def get_current_api_key(self) -> SimpleNamespace:
                return SimpleNamespace(additional_properties={"organizationId": "org-from-key"})

        async def read_page(
            _usage_api: object, organization_id: str, start: datetime, end: datetime, offset: int
        ) -> list[SimpleNamespace]:
            requests.append((organization_id, start, end, offset))
            return [
                SimpleNamespace(
                    sandbox_id="sb-1",
                    total_price=0.25,
                    first_start="2026-10-21T19:30:26Z",
                    last_end="2026-10-21T19:31:00Z",
                ),
                SimpleNamespace(sandbox_id="", total_price=9.0, first_start=None, last_end=None),
                SimpleNamespace(sandbox_id="sb-2", total_price=None, first_start=None, last_end=None),
            ]

        monkeypatch.setattr(sandbox_cost, "_account_provider_config", provider_config)
        monkeypatch.setattr(sandbox_cost, "ConfigApi", FakeConfigApi)
        monkeypatch.setattr(sandbox_cost, "ApiKeysApi", FakeApiKeysApi)
        monkeypatch.setattr(sandbox_cost, "read_usage_page", read_page)
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        usage = await read_daytona_usage(org, NOW - timedelta(hours=1), NOW)

        assert requests == [("org-from-key", NOW - timedelta(hours=1), NOW, 0)]
        assert usage == {
            "sb-1": SandboxUsage(
                sandbox_id="sb-1",
                total_price=0.25,
                last_end=datetime(2026, 10, 21, 19, 31, tzinfo=UTC),
            )
        }


class FakeDaytona:
    def __init__(self, sandboxes: dict[str, tuple[datetime, datetime, float]]) -> None:
        self.sandboxes = sandboxes
        self.rate_limited: set[datetime] = set()
        self.rate_limited_reads = 0
        self.clock: list[float] = [0.0]
        self.calls_per_second: float | None = None
        self.last_accepted = float("-inf")
        self.rate_limit_answers = 0
        self.unpriced_after: datetime | None = None
        self.failing_offsets: set[int] = set()
        self.delay_seconds = 0.001
        self.listing_reads: list[tuple[datetime, datetime]] = []
        self.offsets: list[int] = []
        self.in_flight = 0
        self.most_in_flight = 0
        self.deadline: float | None = None

    def listed(self, start: datetime, end: datetime) -> dict[str, float]:
        return {
            sandbox_id: price
            for sandbox_id in self.sandboxes
            if (price := self.price_inside(sandbox_id, start, end)) > 0
        }

    def price_inside(self, sandbox_id: str, start: datetime, end: datetime) -> float:
        first, last, rate = self.sandboxes[sandbox_id]
        return rate * max(0.0, (min(last, end) - max(first, start)).total_seconds())

    async def read_page(
        self, _usage_api: object, _organization_id: str, start: datetime, end: datetime, offset: int
    ) -> list[SimpleNamespace]:
        self.listing_reads.append((start, end))
        self.offsets.append(offset)
        self.in_flight += 1
        self.most_in_flight = max(self.most_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.delay_seconds)
            if self.deadline is not None and self.clock[0] >= self.deadline:
                raise TimeoutError
            if start in self.rate_limited or len(self.listing_reads) <= self.rate_limited_reads:
                raise ApiException(status=429)
            if offset in self.failing_offsets:
                self.failing_offsets.discard(offset)
                raise ApiException(status=503)
            if self.calls_per_second is not None:
                wait = self.last_accepted + 1 / self.calls_per_second - self.clock[0]
                if wait > 0:
                    self.rate_limit_answers += 1
                    error = ApiException(status=429)
                    error.headers = {"Retry-After": str(math.ceil(wait))}
                    raise error
                self.last_accepted = self.clock[0]
            priced = self.listed(start, end)
            ranked = sorted(priced, key=lambda sandbox_id: (-priced[sandbox_id], sandbox_id))
            return [
                SimpleNamespace(
                    sandbox_id=sandbox_id,
                    total_price=None
                    if self.unpriced_after is not None and start >= self.unpriced_after
                    else priced[sandbox_id],
                    first_start=max(self.sandboxes[sandbox_id][0], start).isoformat(),
                    last_end=min(self.sandboxes[sandbox_id][1], end).isoformat(),
                )
                for sandbox_id in ranked[offset : offset + USAGE_PAGE_SIZE]
            ]
        finally:
            self.in_flight -= 1

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def provider_config(_org: Org, _secret: str) -> DaytonaProviderConfig:
            return DaytonaProviderConfig(
                DAYTONA_API_KEY="test-key",
                DAYTONA_API_URL="https://daytona.test/api",
                DAYTONA_TARGET="us",
                DAYTONA_ORGANIZATION_ID="org-1",
            )

        class FakeConfigApi:
            def __init__(self, _client: object) -> None:
                pass

            async def config_controller_get_config(self) -> SimpleNamespace:
                return SimpleNamespace(analytics_api_url="https://analytics.daytona.test")

        monkeypatch.setattr(sandbox_cost, "_account_provider_config", provider_config)
        monkeypatch.setattr(sandbox_cost, "ConfigApi", FakeConfigApi)
        monkeypatch.setattr(sandbox_cost, "read_usage_page", self.read_page)
        monkeypatch.setattr(sandbox_cost, "RATE_LIMIT_RETRY_SECONDS", 0.0)
        monkeypatch.setattr(sandbox_cost, "USAGE_CALL_INTERVAL_SECONDS", 0.0)
        monkeypatch.setattr(sandbox_cost, "USAGE_CALL_INTERVAL_FLOOR_SECONDS", 0.0)
        monkeypatch.setattr(sandbox_cost, "_usage_call_pacer", sandbox_cost.UsageCallPacer())


def _sandbox(start: datetime, minutes: float, rate: float = 0.001) -> tuple[datetime, datetime, float]:
    return start, start + timedelta(minutes=minutes), rate


def _crowd(hour: datetime, ours: list[str]) -> dict[str, tuple[datetime, datetime, float]]:
    crowd = {f"sb-crowd-{index:04d}": _sandbox(hour + timedelta(minutes=10), 1 / 3) for index in range(USAGE_PAGE_SIZE)}
    return crowd | {sandbox_id: _sandbox(hour + timedelta(minutes=20), 5) for sandbox_id in ours}


def _crowded_run(session: Session, hour: datetime, sandbox_ids: list[str]) -> Benchmark:
    run, _ = _run(session, status=BenchmarkStatus.FINISHED, hours_ago=(NOW - hour) / timedelta(hours=1))
    run.finished_at = hour + timedelta(minutes=30)
    session.add(run)
    session.commit()
    for sandbox_id in sandbox_ids:
        _save_sandbox(session, run, sandbox_id)
    return run


def _one_hour_sweep(session: Session, monkeypatch: pytest.MonkeyPatch, sandbox_count: int) -> FakeDaytona:
    _configure(session)
    run, _ = _run(session, status=BenchmarkStatus.FINISHED, hours_ago=6)
    sandboxes = {f"sb-{index:04d}": _sandbox(START + timedelta(seconds=index), 10) for index in range(sandbox_count)}
    session.add_all(
        SandboxRecord(provider="daytona", sandbox_id=sandbox_id, benchmark_id=run.id) for sandbox_id in sandboxes
    )
    session.commit()
    daytona = FakeDaytona(sandboxes)
    daytona.install(monkeypatch)
    return daytona


ONE_HOUR_LATER = START + timedelta(hours=1) + USAGE_SETTLE_LAG


@pytest.mark.description(
    "Daytona pages each hour's listing 1,000 rows at a time, so the sweep reads pages until one comes back short."
)
class TestUsagePaging:
    @pytest.mark.description("An hour of 2,400 sandboxes is read as pages of 1,000, 1,000 and 400, and all are priced.")
    async def test_an_hour_of_2400_sandboxes_is_read_in_three_pages(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        daytona = _one_hour_sweep(database_session, monkeypatch, 2400)

        written = await sweep_sandbox_costs(database_session.get_bind(), now=ONE_HOUR_LATER)

        assert daytona.offsets == [0, 1000, 2000]
        assert written == 2400
        assert _cursor(database_session) == START + timedelta(hours=1)

    @pytest.mark.description("An hour of exactly 1,000 sandboxes asks for one more page and gets none.")
    async def test_an_hour_of_exactly_1000_asks_one_more_page(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        daytona = _one_hour_sweep(database_session, monkeypatch, USAGE_PAGE_SIZE)

        written = await sweep_sandbox_costs(database_session.get_bind(), now=ONE_HOUR_LATER)

        assert daytona.offsets == [0, 1000]
        assert written == 1000

    @pytest.mark.description(
        "A failed second page keeps the cursor, and the retry reads the hour again from its first page."
    )
    async def test_a_failed_second_page_keeps_the_cursor_and_rereads_from_page_one(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        daytona = _one_hour_sweep(database_session, monkeypatch, 2400)
        daytona.failing_offsets.add(1000)
        bind = database_session.get_bind()

        first = await sweep_sandbox_costs(bind, now=ONE_HOUR_LATER)
        stuck_at = _cursor(database_session)
        second = await sweep_sandbox_costs(bind, now=ONE_HOUR_LATER)

        assert (first, second) == (0, 2400)
        assert stuck_at == START
        assert daytona.offsets == [0, 1000, 0, 1000, 2000]
        assert _cursor(database_session) == START + timedelta(hours=1)

    @pytest.mark.description(
        "A page request carries the hour, a limit of 1,000 and its offset, the paging Daytona confirmed on 2026-11-05"
        " and the generated client does not expose yet."
    )
    async def test_a_page_request_sends_limit_and_offset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        urls: list[str] = []

        async def request(method: str, url: str, **_kwargs: Any) -> RESTResponse:
            urls.append(url)

            async def read() -> bytes:
                return b'[{"sandboxId": "sb-1", "totalPrice": 0.25}]'

            return RESTResponse(
                SimpleNamespace(status=200, reason="OK", read=read, headers={"content-type": "application/json"})
            )

        async with AnalyticsApiClient(AnalyticsConfiguration(host="https://analytics.daytona.test")) as client:
            monkeypatch.setattr(client.rest_client, "request", request)
            page = await sandbox_cost.read_usage_page(
                UsageApi(client), "org-1", START, START + timedelta(hours=1), 2000
            )

        assert urls == [
            "https://analytics.daytona.test/organization/org-1/usage/sandbox"
            "?from=2026-10-21T12%3A00%3A00Z&to=2026-10-21T13%3A00%3A00Z&limit=1000&offset=2000"
        ]
        assert [(row.sandbox_id, row.total_price) for row in page] == [("sb-1", 0.25)]


@pytest.mark.description("Hours Daytona has not priced yet, and listed sandboxes the Tracker never saved.")
class TestUnpricedAndUnmatchedHours:
    @pytest.mark.description(
        "The sweep opens every database session inside database_call, so its reads and writes stay off the event loop."
    )
    async def test_sweep_database_work_runs_through_database_call(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        _configure(database_session)
        hour = START + timedelta(hours=1)
        _crowded_run(database_session, hour, ["sb-a"])
        FakeDaytona(_crowd(hour, ["sb-a"])).install(monkeypatch)
        inside_call = [False]
        sessions: list[bool] = []

        async def recording_call(work: Callable[[], Any]) -> Any:
            inside_call[0] = True
            try:
                return work()
            finally:
                inside_call[0] = False

        def tracked_session(*args: Any, **kwargs: Any) -> Session:
            sessions.append(inside_call[0])
            return Session(*args, **kwargs)

        monkeypatch.setattr(sandbox_cost, "database_call", recording_call)
        monkeypatch.setattr(sandbox_cost, "Session", tracked_session)

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW)

        assert written == 1
        assert sessions and all(sessions)

    @pytest.mark.description("A listed sandbox Daytona has not priced yet keeps the cursor until it is priced.")
    async def test_an_unpriced_sandbox_keeps_the_cursor_until_it_is_priced(self, database_session: Session) -> None:
        _configure(database_session)
        run, _ = _run(database_session, status=BenchmarkStatus.FINISHED, hours_ago=6)
        _save_sandbox(database_session, run, "sb-1")
        priced = [False]

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            return {"sb-1": _usage("sb-1", 0.5, last_end=end) if priced[0] else None}

        bind = database_session.get_bind()
        await sweep_sandbox_costs(bind, now=NOW, open_usage=_opener(read_hour))
        waiting_at = _cursor(database_session)
        priced[0] = True
        await sweep_sandbox_costs(bind, now=NOW, open_usage=_opener(read_hour))

        assert waiting_at == START
        assert _hour_rows(database_session) == {f"sb-1:{hour.isoformat()}": 0.5 for hour in _hours(START, 4)}

    @pytest.mark.description(
        "A listing still running when the sweep budget ends is cut off, and its hour keeps the cursor."
    )
    async def test_a_listing_past_the_budget_keeps_the_cursor(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        _configure(database_session)
        monkeypatch.setattr(sandbox_cost, "SWEEP_TIME_BUDGET", timedelta(seconds=0.05))

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            await asyncio.sleep(1)
            return {}

        written = await sweep_sandbox_costs(database_session.get_bind(), now=NOW, open_usage=_opener(read_hour))

        assert written == 0
        assert _cursor(database_session) == START

    @pytest.mark.description("Usage for a sandbox the Tracker never saved is counted with its dollars and gets no row.")
    async def test_unmatched_usage_is_counted_and_not_written(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        _configure(database_session)
        run, _ = _run(database_session, status=BenchmarkStatus.FINISHED, hours_ago=6)
        _save_sandbox(database_session, run, "sb-ours")
        info = Mock()
        monkeypatch.setattr(sandbox_cost.logger, "info", info)

        async def read_hour(org: Org, secret: str, start: datetime, end: datetime) -> HourUsage:
            return {"sb-ours": _usage("sb-ours", 0.5, last_end=end), "sb-stranger": _usage("sb-stranger", 0.25)}

        await sweep_sandbox_costs(database_session.get_bind(), now=NOW, open_usage=_opener(read_hour))

        assert sorted(_hour_rows(database_session)) == [f"sb-ours:{hour.isoformat()}" for hour in _hours(START, 4)]
        hours = [call.kwargs["extra"] for call in info.call_args_list if call.args == ("Sandbox usage hour priced",)]
        assert [(extra["unmatched_sandboxes"], extra["unmatched_usd"]) for extra in hours] == [(1, 0.25)] * 4
        complete = info.call_args_list[-1].kwargs["extra"]
        assert (complete["unmatched_sandboxes"], complete["unmatched_usd"], complete["hours_behind"]) == (4, 1.0, 0)


@pytest.mark.description(
    "The sweep reads usage one clock hour at a time, and a run is priced only once every hour of it was read."
)
class TestUsageReadHourByHour:
    @pytest.mark.description(
        "A sandbox that spans two slices is priced at the sum of both and ends at its latest end time."
    )
    async def test_a_sandbox_across_two_slices_is_priced_as_their_sum_and_ends_at_its_last_end(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        start = NOW - timedelta(hours=2)
        daytona = FakeDaytona({"sb-across": _sandbox(start + timedelta(minutes=50), 20)})
        daytona.install(monkeypatch)
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        usage = await read_daytona_usage(org, start, NOW)

        assert usage == {
            "sb-across": SandboxUsage(
                sandbox_id="sb-across", total_price=pytest.approx(1.2), last_end=start + timedelta(minutes=70)
            )
        }

    @pytest.mark.description(
        "A day of 2500 sandboxes is priced from hourly listings alone, and each sandbox's hours add up to its full cost."
    )
    async def test_a_day_of_2500_sandboxes_is_fully_priced_from_hourly_pages(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        _configure(database_session)
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED, hours_ago=24)
        day = NOW - timedelta(hours=24)
        monkeypatch.setattr(sandbox_cost, "ACCOUNTING_START", day)
        sandboxes = {
            f"sb-{index:04d}": _sandbox(day + timedelta(seconds=30 * index), 45, rate=0.0001 * (index + 1))
            for index in range(2500)
        }
        database_session.add_all(
            SandboxRecord(provider="daytona", sandbox_id=sandbox_id, benchmark_id=benchmark.id)
            for sandbox_id in sandboxes
        )
        database_session.commit()
        daytona = FakeDaytona(sandboxes)
        daytona.install(monkeypatch)

        await sweep_sandbox_costs(database_session.get_bind(), now=NOW + USAGE_SETTLE_LAG + timedelta(hours=1))

        assert _priced_per_sandbox(database_session) == {
            sandbox_id: pytest.approx(rate * (last - first).total_seconds())
            for sandbox_id, (first, last, rate) in sandboxes.items()
        }

    @pytest.mark.description(
        "The request timeout covers each listing call, so a day of hourly slices still finishes while waiting for slots."
    )
    async def test_the_timeout_bounds_each_request_and_not_the_wait_for_a_slot(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        daytona = FakeDaytona({"sb-1": _sandbox(NOW - timedelta(hours=20), 10)})
        daytona.delay_seconds = 0.05
        daytona.install(monkeypatch)
        monkeypatch.setattr(sandbox_cost, "USAGE_READ_TIMEOUT_SECONDS", 0.2)
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        usage = await read_daytona_usage(org, NOW - timedelta(hours=24), NOW)

        assert len(daytona.listing_reads) == 24
        assert set(usage) == {"sb-1"}

    @pytest.mark.description("A listing call that times out raises, so its hour writes no costs.")
    async def test_a_request_past_the_timeout_writes_nothing_for_the_group(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        daytona = FakeDaytona({"sb-1": _sandbox(NOW - timedelta(minutes=50), 10)})
        daytona.delay_seconds = 0.2
        daytona.install(monkeypatch)
        monkeypatch.setattr(sandbox_cost, "USAGE_READ_TIMEOUT_SECONDS", 0.05)
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        with pytest.raises(TimeoutError):
            await read_daytona_usage(org, NOW - timedelta(hours=1), NOW)

    @pytest.mark.description("A day of hourly slices is read with one listing call in flight at a time.")
    async def test_slices_are_read_one_at_a_time(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        daytona = FakeDaytona({"sb-1": _sandbox(NOW - timedelta(hours=20), 10)})
        daytona.install(monkeypatch)
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        await read_daytona_usage(org, NOW - timedelta(hours=24), NOW)

        assert len(daytona.listing_reads) == 24
        assert daytona.most_in_flight == 1

    @pytest.mark.description("A slice that gets 429 until its last attempt is still read and priced.")
    async def test_a_slice_rate_limited_until_its_last_attempt_is_priced(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        hour = NOW - timedelta(hours=1)
        daytona = FakeDaytona({"sb-1": _sandbox(hour + timedelta(minutes=10), 10)})
        daytona.rate_limited_reads = RATE_LIMIT_ATTEMPTS - 1
        daytona.install(monkeypatch)
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        usage = await read_daytona_usage(org, hour, NOW)

        assert len(daytona.listing_reads) == RATE_LIMIT_ATTEMPTS
        assert set(usage) == {"sb-1"}

    @pytest.mark.description("A slice waiting out a 429 is retried before the next slice is read.")
    async def test_a_rate_limited_slice_keeps_its_slot_while_it_waits(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        start = NOW - timedelta(hours=2)
        daytona = FakeDaytona({"sb-1": _sandbox(start + timedelta(minutes=10), 10)})
        daytona.rate_limited_reads = 2
        daytona.install(monkeypatch)
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        await read_daytona_usage(org, start, NOW)

        assert [read_start for read_start, _ in daytona.listing_reads] == [
            start,
            start,
            start,
            start + timedelta(hours=1),
        ]


@pytest.mark.description(
    "Daytona's usage limit is shared by the whole organization, so calls are spaced before any 429."
)
class TestUsageCallPacer:
    @pytest.mark.description("Each usage call waits until the call interval has passed since the previous call.")
    async def test_each_call_waits_until_the_interval_since_the_previous_call_has_passed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clock = [100.0]
        waits: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            waits.append(seconds)
            clock[0] += seconds

        monkeypatch.setattr(sandbox_cost, "monotonic", lambda: clock[0])
        monkeypatch.setattr(sandbox_cost.asyncio, "sleep", fake_sleep)
        pacer = sandbox_cost.UsageCallPacer()

        await pacer.wait()
        clock[0] += 3
        await pacer.wait()
        clock[0] += 15
        await pacer.wait()

        assert waits == [sandbox_cost.USAGE_CALL_INTERVAL_SECONDS - 3]

    @pytest.mark.description("The call interval shrinks a step after each success and doubles after a 429.")
    def test_the_interval_shrinks_after_successes_and_doubles_after_a_429(self) -> None:
        pacer = sandbox_cost.UsageCallPacer()

        pacer.succeeded()
        pacer.succeeded()
        shrunk = pacer.interval
        pacer.rate_limited()

        assert shrunk == sandbox_cost.USAGE_CALL_INTERVAL_SECONDS - 2 * sandbox_cost.USAGE_CALL_INTERVAL_STEP_SECONDS
        assert pacer.interval == 2 * shrunk

    @pytest.mark.description("The call interval never drops below its floor or rises above its ceiling.")
    def test_the_interval_stays_between_its_floor_and_its_ceiling(self) -> None:
        pacer = sandbox_cost.UsageCallPacer()

        for _ in range(100):
            pacer.succeeded()
        floor = pacer.interval
        for _ in range(10):
            pacer.rate_limited()

        assert (floor, pacer.interval) == (
            sandbox_cost.USAGE_CALL_INTERVAL_FLOOR_SECONDS,
            sandbox_cost.USAGE_CALL_INTERVAL_CEILING_SECONDS,
        )


@pytest.mark.description("Every usage call the reader makes, across reads, waits for the shared pacer.")
class TestReaderUsesThePacer:
    @pytest.mark.description(
        "Usage calls start ten seconds apart and close in after each success, across reads as well."
    )
    async def test_calls_across_two_reads_start_ten_seconds_apart_and_close_in(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        daytona = FakeDaytona({"sb-1": _sandbox(NOW - timedelta(hours=2), 10)})
        daytona.delay_seconds = 0
        daytona.install(monkeypatch)
        clock = daytona.clock
        call_times: list[float] = []
        listing = daytona.listing_reads

        class RecordingReads(list[tuple[datetime, datetime]]):
            def append(self, read: tuple[datetime, datetime]) -> None:
                call_times.append(clock[0])
                super().append(read)

        daytona.listing_reads = RecordingReads(listing)

        async def fake_sleep(seconds: float) -> None:
            clock[0] += seconds

        monkeypatch.setattr(sandbox_cost, "monotonic", lambda: clock[0])
        monkeypatch.setattr(sandbox_cost.asyncio, "sleep", fake_sleep)
        monkeypatch.setattr(sandbox_cost, "USAGE_CALL_INTERVAL_SECONDS", 10.0)
        monkeypatch.setattr(sandbox_cost, "_usage_call_pacer", sandbox_cost.UsageCallPacer())
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        await read_daytona_usage(org, NOW - timedelta(hours=3), NOW)
        await read_daytona_usage(org, NOW - timedelta(hours=2), NOW)

        assert len(call_times) == 5
        assert [later - earlier for earlier, later in zip(call_times, call_times[1:])] == [9.5, 9.0, 8.5, 8.0]

    @pytest.mark.description("A sandbox with no price in one of its slices is left out of the usage.")
    async def test_a_sandbox_with_an_unpriced_slice_is_left_out_until_every_slice_has_a_price(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        benchmark, _ = _run(database_session, status=BenchmarkStatus.FINISHED)
        start = NOW - timedelta(hours=2)
        daytona = FakeDaytona({"sb-across": _sandbox(start + timedelta(minutes=50), 20), "sb-1": _sandbox(start, 10)})
        daytona.unpriced_after = start + timedelta(hours=1)
        daytona.install(monkeypatch)
        org = database_session.get(Org, benchmark.org_id)
        assert org is not None

        usage = await read_daytona_usage(org, start, NOW)

        assert set(usage) == {"sb-1"}


def _rate_limited(retry_after: str | None) -> ApiException:
    error = ApiException(status=429)
    error.headers = {} if retry_after is None else {"Retry-After": retry_after}
    return error


@pytest.mark.description("How long a slice waits after Daytona answers 429 before it asks again.")
class TestRateLimitDelay:
    @pytest.mark.description(
        "A 429 waits for its Retry-After, or else backs off 5, 10, then 20 seconds, never past 30."
    )
    @pytest.mark.parametrize(
        ("retry_after", "attempt", "delay"),
        [
            ("7", 1, 7.0),
            (None, 1, 5.0),
            (None, 2, 10.0),
            (None, 3, 20.0),
            (None, 4, 30.0),
            ("soon", 2, 10.0),
            ("Wed, 21 Oct 2026 07:28:00 GMT", 2, 0.0),
            ("3600", 1, 30.0),
        ],
        ids=[
            "retry-after-seconds",
            "first-backoff",
            "backoff-doubles",
            "backoff-doubles-again",
            "backoff-capped",
            "unparsed-header-backs-off",
            "past-http-date-retries-now",
            "retry-after-capped",
        ],
    )
    def test_honours_retry_after_and_otherwise_backs_off(
        self, retry_after: str | None, attempt: int, delay: float
    ) -> None:
        assert sandbox_cost.rate_limit_delay(_rate_limited(retry_after), attempt) == delay

    @pytest.mark.description("A Retry-After given as a future HTTP date waits until that moment.")
    def test_a_future_http_date_waits_until_that_moment(self) -> None:
        retry_at = format_datetime(datetime.now(UTC) + timedelta(seconds=20), usegmt=True)

        assert 18 <= sandbox_cost.rate_limit_delay(_rate_limited(retry_at), 1) <= 20


@pytest.mark.description(
    (
        "A backlog shaped like bench's, with an hour over the row cap, against a Daytona that answers 429 past a set rate.\n\n"
        "On a fake clock, every sweep writes rows within its budget, and the backlog drains."
    )
)
class TestBacklogDrainsUnderRateLimit:
    @pytest.mark.description(
        "Each sweep writes rows within its budget until every hour of the backlog is priced at full cost."
    )
    @pytest.mark.parametrize("calls_per_second", [0.05, 0.1, 2, 5, 10])
    async def test_each_sweep_prices_hours_until_the_backlog_is_empty(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session, calls_per_second: float
    ) -> None:
        first_hour = NOW - timedelta(days=6)
        runs = [
            _run(database_session, status=BenchmarkStatus.FINISHED, hours_ago=6 * 24 - 6 * index)[0]
            for index in range(24)
        ]
        sandboxes: dict[str, tuple[datetime, datetime, float]] = {}
        for hour in range(6 * 24):
            crowd = 1100 if hour == 30 else 15
            for index in range(crowd):
                sandbox_id = f"sb-{hour}-{index}"
                sandboxes[sandbox_id] = _sandbox(first_hour + timedelta(hours=hour, seconds=index * 3), 20)
                database_session.add(
                    SandboxRecord(provider="daytona", sandbox_id=sandbox_id, benchmark_id=runs[hour // 6].id)
                )
        for index, run in enumerate(runs):
            run.finished_at = first_hour + timedelta(hours=6 * index + 6)
            database_session.add(run)
        database_session.commit()
        daytona = FakeDaytona(sandboxes)
        daytona.install(monkeypatch)
        daytona.calls_per_second = calls_per_second
        daytona.delay_seconds = 0
        clock = daytona.clock

        async def fake_sleep(seconds: float) -> None:
            clock[0] += seconds

        monkeypatch.setattr(sandbox_cost, "monotonic", lambda: clock[0])
        monkeypatch.setattr(sandbox_cost.asyncio, "sleep", fake_sleep)
        monkeypatch.setattr(sandbox_cost, "USAGE_CALL_INTERVAL_SECONDS", 10.0)
        monkeypatch.setattr(sandbox_cost, "RATE_LIMIT_RETRY_SECONDS", 5.0)
        monkeypatch.setattr(sandbox_cost, "USAGE_CALL_INTERVAL_FLOOR_SECONDS", 1.0)
        monkeypatch.setattr(sandbox_cost, "_usage_call_pacer", sandbox_cost.UsageCallPacer())

        _configure(database_session)
        monkeypatch.setattr(sandbox_cost, "ACCOUNTING_START", first_hour)
        later = NOW + USAGE_SETTLE_LAG + timedelta(hours=1)
        written_per_sweep: list[int] = []
        seconds_per_sweep: list[float] = []
        while (_cursor(database_session) or first_hour) <= NOW and len(written_per_sweep) < 20:
            started = clock[0]
            written_per_sweep.append(await sweep_sandbox_costs(database_session.get_bind(), now=later))
            seconds_per_sweep.append(clock[0] - started)
            clock[0] += 900

        assert all(written > 0 for written in written_per_sweep)
        assert (daytona.rate_limit_answers > 0) == (calls_per_second < 1)
        assert (len(written_per_sweep) > 1) == (calls_per_second < 1)
        assert max(seconds_per_sweep) < SWEEP_TIME_BUDGET.total_seconds() + 15 * 60
        assert _priced_per_sandbox(database_session) == {
            sandbox_id: pytest.approx(rate * (last - first).total_seconds())
            for sandbox_id, (first, last, rate) in sandboxes.items()
        }


PEAK_HOUR = datetime(2026, 9, 25, 9, tzinfo=UTC)
PEAK_DAY = PEAK_HOUR.replace(hour=0)
PEAK_DAY_SANDBOXES_PER_HOUR = [
    640,
    520,
    470,
    430,
    580,
    1150,
    2070,
    3380,
    5140,
    6610,
    5830,
    4710,
    3860,
    3540,
    3270,
    2860,
    2450,
    1870,
    1460,
    1170,
    1060,
    870,
    760,
    413,
]
BACKLOG_END = datetime(2026, 11, 5, 9, tzinfo=UTC)
BACKLOG_SANDBOXES_PER_HOUR = 1500
BACKLOG_CALLS_PER_SECOND = 0.1


class PeakDaytona(FakeDaytona):
    def __init__(self, sandboxes_per_hour: dict[datetime, int]) -> None:
        self.by_hour = {
            hour: {
                f"sb-{hour:%m%dT%H}-{index:05d}": _sandbox(hour + timedelta(seconds=index % 2400), 5 + index % 7)
                for index in range(count)
            }
            for hour, count in sandboxes_per_hour.items()
        }
        super().__init__({sandbox_id: span for hour in self.by_hour.values() for sandbox_id, span in hour.items()})
        self.owned: set[str] = set()

    def listed(self, start: datetime, end: datetime) -> dict[str, float]:
        return {sandbox_id: self.price_inside(sandbox_id, start, end) for sandbox_id in self.by_hour.get(start, {})}

    def billed(self, hour: datetime, *, owned: set[str] | None = None) -> float:
        listed = self.listed(hour, hour + timedelta(hours=1))
        return sum(price for sandbox_id, price in listed.items() if owned is None or sandbox_id in owned)


def _peak_sweep(
    session: Session,
    monkeypatch: pytest.MonkeyPatch,
    sandboxes_per_hour: dict[datetime, int],
    *,
    owned_share: int = 1,
) -> PeakDaytona:
    _configure(session)
    run, _ = _run(session, status=BenchmarkStatus.FINISHED)
    daytona = PeakDaytona(sandboxes_per_hour)
    daytona.owned = set(list(daytona.sandboxes)[::owned_share])
    session.add_all(
        SandboxRecord(provider="daytona", sandbox_id=sandbox_id, benchmark_id=run.id) for sandbox_id in daytona.owned
    )
    session.commit()
    daytona.install(monkeypatch)
    daytona.delay_seconds = 0
    clock = daytona.clock

    async def fake_sleep(seconds: float) -> None:
        clock[0] += seconds

    monkeypatch.setattr(sandbox_cost, "monotonic", lambda: clock[0])
    monkeypatch.setattr(sandbox_cost.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(sandbox_cost, "USAGE_CALL_INTERVAL_SECONDS", USAGE_CALL_INTERVAL_SECONDS)
    monkeypatch.setattr(sandbox_cost, "USAGE_CALL_INTERVAL_FLOOR_SECONDS", USAGE_CALL_INTERVAL_FLOOR_SECONDS)
    monkeypatch.setattr(sandbox_cost, "RATE_LIMIT_RETRY_SECONDS", RATE_LIMIT_RETRY_SECONDS)
    monkeypatch.setattr(sandbox_cost, "_usage_call_pacer", sandbox_cost.UsageCallPacer())
    monkeypatch.setattr(sandbox_cost, "ACCOUNTING_START", min(sandboxes_per_hour))
    return daytona


async def _sweep_until_drained(
    session: Session, daytona: PeakDaytona, *, now: datetime, last_hour: datetime, most_sweeps: int
) -> list[int]:
    written_per_sweep: list[int] = []
    while (_cursor(session) or last_hour) <= last_hour and len(written_per_sweep) < most_sweeps:
        daytona.deadline = daytona.clock[0] + SWEEP_TIME_BUDGET.total_seconds()
        written_per_sweep.append(await sweep_sandbox_costs(session.get_bind(), now=now))
        daytona.clock[0] = daytona.deadline + 900
    return written_per_sweep


def _rows_per_hour(session: Session) -> dict[datetime, float]:
    totals: dict[datetime, float] = {}
    for source_id, amount in _hour_rows(session).items():
        hour = _as_utc(datetime.fromisoformat(source_id.split(":", 1)[1]))
        totals[hour] = totals.get(hour, 0.0) + amount
    return totals


def _default_spacing_seconds(calls: int) -> float:
    return sum(
        max(USAGE_CALL_INTERVAL_FLOOR_SECONDS, USAGE_CALL_INTERVAL_SECONDS - USAGE_CALL_INTERVAL_STEP_SECONDS * call)
        for call in range(1, calls)
    )


@pytest.mark.description(
    (
        "Peaks measured on the Daytona account: 6,610 sandboxes in the hour of 2026-09-25 09:00Z and 55,113 sandbox-hours that day.\n\n"
        "On a fake clock, the sweep reads each peak hour in pages, writes every row, and finishes inside its budget."
    )
)
class TestPeakScale:
    @pytest.mark.description(
        "The peak hour of 6,610 sandboxes costs seven listing calls, writes 6,610 rows, and moves the cursor one hour."
    )
    async def test_peak_hour_of_6610_sandboxes_is_seven_calls_and_one_cursor_step(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        daytona = _peak_sweep(database_session, monkeypatch, {PEAK_HOUR: 6610})

        written = await sweep_sandbox_costs(
            database_session.get_bind(), now=PEAK_HOUR + timedelta(hours=1) + USAGE_SETTLE_LAG
        )

        assert daytona.offsets == [0, 1000, 2000, 3000, 4000, 5000, 6000]
        assert written == 6610
        assert _cursor(database_session) == PEAK_HOUR + timedelta(hours=1)
        assert _rows_per_hour(database_session) == {PEAK_HOUR: pytest.approx(daytona.billed(PEAK_HOUR))}

    @pytest.mark.description(
        (
            "The peak day of 55,113 sandbox-hours is 67 calls, one per page of each hour, which the default spacing"
            " fits in 142.5 seconds, so one 10 minute sweep drains it.\n\n"
            "Only the 09:00Z hour and the day's total are measured. The other hours are a shape that sums to the total."
        )
    )
    async def test_peak_day_drains_in_1_sweep(self, monkeypatch: pytest.MonkeyPatch, database_session: Session) -> None:
        hours = _hours(PEAK_DAY, 24)
        daytona = _peak_sweep(database_session, monkeypatch, dict(zip(hours, PEAK_DAY_SANDBOXES_PER_HOUR, strict=True)))
        calls = sum(math.ceil(count / USAGE_PAGE_SIZE) for count in PEAK_DAY_SANDBOXES_PER_HOUR)
        sweeps = math.ceil(_default_spacing_seconds(calls) / SWEEP_TIME_BUDGET.total_seconds())

        written_per_sweep = await _sweep_until_drained(
            database_session,
            daytona,
            now=hours[-1] + timedelta(hours=1) + USAGE_SETTLE_LAG,
            last_hour=hours[-1],
            most_sweeps=5,
        )

        assert sum(PEAK_DAY_SANDBOXES_PER_HOUR) == 55113
        assert (calls, sweeps) == (67, 1)
        assert len(daytona.offsets) == calls
        assert written_per_sweep == [55113]
        assert _cursor(database_session) == hours[-1] + timedelta(hours=1)
        assert _rows_per_hour(database_session) == {hour: pytest.approx(daytona.billed(hour)) for hour in hours}

    @pytest.mark.description(
        (
            "The backlog from 2026-11-01 00:00Z to 2026-11-05 09:00Z is 105 hours of 1,500 sandboxes, so 210 calls,"
            " against a Daytona that answers 429 to calls under 10 seconds apart.\n\n"
            "The pacer climbs back from 19 seconds to 10 between 429s, about 15 seconds a call, so a 600 second sweep"
            " makes about 40 calls, 20 hours, and the backlog drains in ceil(105 / 20) = 6 sweeps.\n\n"
            "Every tenth sandbox belongs to a saved run, so the database writes stay small. Each hour's rows sum to what"
            " the fake bills for those sandboxes."
        )
    )
    async def test_backlog_under_six_calls_a_minute_drains_in_6_sweeps(
        self, monkeypatch: pytest.MonkeyPatch, database_session: Session
    ) -> None:
        hours = _hours(ACCOUNTING_START, 105)
        daytona = _peak_sweep(
            database_session, monkeypatch, dict.fromkeys(hours, BACKLOG_SANDBOXES_PER_HOUR), owned_share=10
        )
        daytona.calls_per_second = BACKLOG_CALLS_PER_SECOND

        written_per_sweep = await _sweep_until_drained(
            database_session, daytona, now=BACKLOG_END + USAGE_SETTLE_LAG, last_hour=hours[-1], most_sweeps=10
        )

        assert hours[-1] + timedelta(hours=1) == BACKLOG_END
        assert len(written_per_sweep) == 6
        assert all(written > 0 for written in written_per_sweep)
        assert daytona.rate_limit_answers > 0
        assert _cursor(database_session) == BACKLOG_END
        assert _rows_per_hour(database_session) == {
            hour: pytest.approx(daytona.billed(hour, owned=daytona.owned)) for hour in hours
        }


class FakeLeaderLock:
    def __init__(self, *, free: bool = True) -> None:
        self.free = free
        self.held = False
        self.acquires = 0
        self.releases = 0

    async def still_held(self) -> bool:
        return self.held

    async def acquire(self) -> bool:
        self.acquires += 1
        self.held = self.free
        return self.held

    async def release(self) -> None:
        self.releases += 1
        self.held = False


async def _tick(leader: FakeLeaderLock) -> int | None:
    return await sandbox_cost.sweep_sandbox_costs_as_leader(
        cast(PostgresAdvisoryLock, leader),
        cast(Engine, None),
        now=NOW,
    )


@pytest.mark.description("One tick of a worker's loop.")
class TestSweepSandboxCostsAsLeader:
    @pytest.mark.description(
        "A worker that gets the free sweep lock holds it and returns the number of costs it wrote."
    )
    async def test_a_free_lock_makes_the_worker_lead_and_sweep(self, monkeypatch: pytest.MonkeyPatch) -> None:
        leader = FakeLeaderLock()

        async def sweep(_bind: object, *, now: datetime) -> int:
            return 4

        monkeypatch.setattr(sandbox_cost, "sweep_sandbox_costs", sweep)

        assert await _tick(leader) == 4
        assert leader.held

    @pytest.mark.description("A worker that already leads sweeps without taking the lock again.")
    async def test_a_worker_that_already_leads_sweeps_without_acquiring_again(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        leader = FakeLeaderLock()
        leader.held = True

        async def sweep(_bind: object, *, now: datetime) -> int:
            return 1

        monkeypatch.setattr(sandbox_cost, "sweep_sandbox_costs", sweep)

        assert await _tick(leader) == 1
        assert leader.acquires == 0

    @pytest.mark.description("A worker that cannot get the sweep lock does not sweep.")
    async def test_a_worker_without_the_lock_skips_the_sweep(self, monkeypatch: pytest.MonkeyPatch) -> None:
        leader = FakeLeaderLock(free=False)
        sweeps: list[str] = []

        async def sweep(_bind: object, *, now: datetime) -> int:
            sweeps.append("ran")
            return 1

        monkeypatch.setattr(sandbox_cost, "sweep_sandbox_costs", sweep)

        assert await _tick(leader) is None
        assert sweeps == []


@pytest.mark.description("The Tracker thread that runs the sweep.")
class TestAutomaticSandboxCost:
    @pytest.mark.description("The cost sweep ticks every fifteen minutes.")
    def test_ticks_every_fifteen_minutes(self) -> None:
        assert sandbox_cost.run_sandbox_cost_loop.__kwdefaults__ == {"interval_seconds": 900.0}

    @pytest.mark.description("The cost loop sweeps on start, leads until shutdown, and then releases the lock.")
    def test_sweeps_on_start_leads_until_shutdown_and_then_releases(self, monkeypatch: pytest.MonkeyPatch) -> None:
        swept = threading.Event()
        leader = FakeLeaderLock()

        async def sweep(_bind: object, *, now: datetime) -> int:
            swept.set()

            return 0

        monkeypatch.setattr(sandbox_cost, "sweep_sandbox_costs", sweep)
        monkeypatch.setattr(sandbox_cost, "sandbox_cost_sweep_lock", lambda _engine: leader)
        thread = AutomaticSandboxCost(interval_seconds=60, shutdown_timeout_seconds=5)

        thread.start()
        assert swept.wait(5)
        assert leader.held
        thread.stop()

        assert not [alive for alive in threading.enumerate() if alive.name == "sandbox-cost"]
        assert not leader.held
        assert leader.releases == 1

    @pytest.mark.description(
        "After a sweep raises, the loop keeps running and keeps the lock without acquiring it again."
    )
    def test_a_failing_sweep_keeps_the_loop_and_the_lock(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ticks = threading.Semaphore(0)
        leader = FakeLeaderLock()

        async def sweep(_bind: object, *, now: datetime) -> int:
            ticks.release()
            raise RuntimeError("daytona unavailable")

        monkeypatch.setattr(sandbox_cost, "sweep_sandbox_costs", sweep)
        monkeypatch.setattr(sandbox_cost, "sandbox_cost_sweep_lock", lambda _engine: leader)
        thread = AutomaticSandboxCost(interval_seconds=0.01, shutdown_timeout_seconds=5)

        thread.start()
        assert ticks.acquire(timeout=5)
        assert ticks.acquire(timeout=5)
        assert leader.held
        thread.stop()

        assert leader.acquires == 1
