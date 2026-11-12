from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service import SandboxNotFoundError
from benchmark_service.sandbox.daytona_v2 import DaytonaV2Sandbox
from sqlmodel import Session, select

from tests.factories import make_benchmark, make_task
from tracker.services.sandboxes.costs import sweep_priced_sandbox_costs
from tracker.database.models import Benchmark, BenchmarkStatus, Org, RunCost, SandboxRecord
from tracker.repositories.benchmark import BenchmarkRepository

NOW = datetime(2026, 11, 11, 12, tzinfo=UTC)
SECRET = "PricedSecrets"
PROVIDER = "priced"
COST = 0.42
LAST_SEEN = NOW - timedelta(minutes=30)


def _sandbox(*, ended: bool) -> Mock:
    return Mock(has_ended=ended, cost_usd=Mock(return_value=COST))


def _link(
    session: Session,
    *sandbox_ids: str,
    secret: str = SECRET,
    status: BenchmarkStatus = BenchmarkStatus.IN_PROGRESS,
    started_at: datetime = NOW - timedelta(hours=1),
    finished_at: datetime | None = None,
    last_seen_at: datetime | None = LAST_SEEN,
) -> None:
    benchmark = make_benchmark(status=status, started_at=started_at, session=None)
    benchmark.arguments = benchmark.arguments.model_copy(update={"sandbox_provider_secret_name": secret})
    session.add(benchmark)
    session.commit()
    if finished_at is not None:
        benchmark.finished_at = finished_at
        session.commit()
    session.add_all(
        SandboxRecord(provider=PROVIDER, sandbox_id=sandbox_id, benchmark_id=benchmark.id, last_seen_at=last_seen_at)
        for sandbox_id in sandbox_ids
    )
    session.commit()


@pytest.mark.description(
    "The sweep prices every ended sandbox of a priced provider once, up to the last time it was seen alive."
)
async def test_prices_gone_sandboxes_once(database_session: Session) -> None:
    _link(database_session, "sb-deleted", "sb-running", "sb-missing")
    sandboxes = {"sb-deleted": _sandbox(ended=True), "sb-running": _sandbox(ended=False)}

    async def get_sandbox(sandbox_id: str) -> Mock:
        if sandbox_id not in sandboxes:
            raise SandboxNotFoundError(sandbox_id)
        return sandboxes[sandbox_id]

    @asynccontextmanager
    async def open_provider(org: Org, provider: str, secret_name: str) -> AsyncIterator[Mock]:
        assert (provider, secret_name) == (PROVIDER, SECRET)
        yield Mock(get_sandbox=AsyncMock(side_effect=get_sandbox))

    bind = database_session.get_bind()
    first = await sweep_priced_sandbox_costs(bind, now=NOW, providers=[PROVIDER], open_provider=open_provider)
    second = await sweep_priced_sandbox_costs(bind, now=NOW, providers=[PROVIDER], open_provider=open_provider)

    assert (first, second) == (1, 0)
    rows = database_session.exec(select(RunCost)).all()
    assert [(row.provider, row.source_id) for row in rows] == [(PROVIDER, "sb-deleted")]
    assert rows[0].amount_usd == pytest.approx(COST)
    assert _utc(rows[0].usage_end) == LAST_SEEN
    sandboxes["sb-deleted"].cost_usd.assert_called_with(LAST_SEEN)


def _utc(moment: datetime | None) -> datetime | None:
    return moment.replace(tzinfo=UTC) if moment is not None and moment.tzinfo is None else moment


def _last_seen(session: Session, sandbox_id: str) -> datetime | None:
    session.expire_all()
    return _utc(session.exec(select(SandboxRecord.last_seen_at).where(SandboxRecord.sandbox_id == sandbox_id)).one())


@pytest.mark.description(
    "A sandbox the sweep sees running is billed up to when it was found running, not to the sweep's start or end."
)
async def test_running_sighting_bounds_the_bill(database_session: Session) -> None:
    _link(database_session, "sb-leftover")
    sandbox = _sandbox(ended=False)

    @asynccontextmanager
    async def open_provider(org: Org, provider: str, secret_name: str) -> AsyncIterator[Mock]:
        yield Mock(get_sandbox=AsyncMock(return_value=sandbox))

    bind = database_session.get_bind()
    sighting = NOW + timedelta(minutes=15)
    assert (
        await sweep_priced_sandbox_costs(
            bind, now=NOW, providers=[PROVIDER], open_provider=open_provider, clock=lambda: sighting
        )
        == 0
    )
    assert _last_seen(database_session, "sb-leftover") == sighting

    sandbox.has_ended = True
    later = NOW + timedelta(hours=3)
    assert await sweep_priced_sandbox_costs(bind, now=later, providers=[PROVIDER], open_provider=open_provider) == 1
    sandbox.cost_usd.assert_called_once_with(sighting)
    assert _utc(database_session.exec(select(RunCost.usage_end)).one()) == sighting


@pytest.mark.description(
    "A sighting saved after a sandbox was priced raises its bill to that sighting on the next sweep."
)
async def test_later_sighting_reprices_the_bill(database_session: Session) -> None:
    _link(database_session, "sb-verifier")
    sandbox = _sandbox(ended=True)

    @asynccontextmanager
    async def open_provider(org: Org, provider: str, secret_name: str) -> AsyncIterator[Mock]:
        yield Mock(get_sandbox=AsyncMock(return_value=sandbox))

    bind = database_session.get_bind()
    assert await sweep_priced_sandbox_costs(bind, now=NOW, providers=[PROVIDER], open_provider=open_provider) == 1
    benchmark_id = database_session.exec(select(Benchmark.id)).one()
    BenchmarkRepository(database_session).record_sandbox_link(PROVIDER, "sb-verifier", benchmark_id, NOW)
    database_session.commit()
    sandbox.cost_usd.return_value = 2 * COST

    assert await sweep_priced_sandbox_costs(bind, now=NOW, providers=[PROVIDER], open_provider=open_provider) == 1
    assert await sweep_priced_sandbox_costs(bind, now=NOW, providers=[PROVIDER], open_provider=open_provider) == 0
    database_session.expire_all()
    row = database_session.exec(select(RunCost)).one()
    assert (row.amount_usd, _utc(row.usage_end)) == (pytest.approx(2 * COST), NOW)


@pytest.mark.description(
    "An ended sandbox linked before sightings were recorded is billed once, up to its run's last task activity."
)
async def test_unseen_sandbox_is_billed_to_its_runs_last_activity(database_session: Session) -> None:
    _link(database_session, "sb-unseen", last_seen_at=None)
    benchmark = database_session.exec(select(Benchmark)).one()
    last_task_end = NOW - timedelta(minutes=10)
    database_session.add_all(
        [
            make_task(
                benchmark, "early", started_at=NOW - timedelta(minutes=50), finished_at=NOW - timedelta(minutes=40)
            ),
            make_task(benchmark, "late", started_at=NOW - timedelta(minutes=30), finished_at=last_task_end),
        ]
    )
    database_session.commit()
    sandbox = _sandbox(ended=True)

    @asynccontextmanager
    async def open_provider(org: Org, provider: str, secret_name: str) -> AsyncIterator[Mock]:
        yield Mock(get_sandbox=AsyncMock(return_value=sandbox))

    bind = database_session.get_bind()
    first = await sweep_priced_sandbox_costs(bind, now=NOW, providers=[PROVIDER], open_provider=open_provider)
    second = await sweep_priced_sandbox_costs(bind, now=NOW, providers=[PROVIDER], open_provider=open_provider)

    assert (first, second) == (1, 0)
    sandbox.cost_usd.assert_called_once_with(last_task_end)
    assert _utc(database_session.exec(select(RunCost.usage_end)).one()) == last_task_end


@pytest.mark.description("Recording a sandbox link keeps the latest sighting it is given, never an earlier one.")
def test_recording_a_link_moves_last_seen_forward(database_session: Session) -> None:
    _link(database_session)
    benchmark_id = database_session.exec(select(Benchmark.id)).one()
    repository = BenchmarkRepository(database_session)
    repository.record_sandbox_link(PROVIDER, "sb-new", benchmark_id, LAST_SEEN)
    database_session.commit()
    assert _last_seen(database_session, "sb-new") == LAST_SEEN

    repository.record_sandbox_link(PROVIDER, "sb-new", benchmark_id, NOW)
    repository.record_sandbox_link(PROVIDER, "sb-new", benchmark_id, LAST_SEEN)
    database_session.commit()
    assert _last_seen(database_session, "sb-new") == NOW


@pytest.mark.description(
    "A 14 minute verifier sandbox no sweep saw running is billed up to the end of the evaluate stream that announced it."
)
async def test_short_verifier_is_billed_to_its_stream_end(database_session: Session) -> None:
    created = NOW - timedelta(minutes=20)
    _link(database_session, "sb-verifier", last_seen_at=created)
    benchmark_id = database_session.exec(select(Benchmark.id)).one()
    BenchmarkRepository(database_session).record_sandbox_link(
        PROVIDER, "sb-verifier", benchmark_id, created + timedelta(minutes=14)
    )
    database_session.commit()
    sandbox = DaytonaV2Sandbox(
        {
            "id": "sb-verifier",
            "status": "deleted",
            "cpu": 2,
            "memory_mib": 4096,
            "region_id": "us",
            "spot": False,
            "created_at": created.isoformat(),
        },
        Mock(),
    )

    @asynccontextmanager
    async def open_provider(org: Org, provider: str, secret_name: str) -> AsyncIterator[Mock]:
        yield Mock(get_sandbox=AsyncMock(return_value=sandbox))

    bind = database_session.get_bind()
    assert await sweep_priced_sandbox_costs(bind, now=NOW, providers=[PROVIDER], open_provider=open_provider) == 1
    assert database_session.exec(select(RunCost.amount_usd)).one() == pytest.approx(0.1435, abs=1e-4)


@pytest.mark.description(
    "An old run that is active or finished within the lookback still gets its sandboxes priced, "
    "but one that finished before the lookback does not."
)
async def test_old_active_runs_are_still_priced(database_session: Session) -> None:
    long_ago = NOW - timedelta(days=30)
    _link(database_session, "sb-active", started_at=long_ago)
    _link(database_session, "sb-finished", status=BenchmarkStatus.FINISHED, started_at=long_ago, finished_at=long_ago)
    _link(
        database_session,
        "sb-just-finished",
        status=BenchmarkStatus.FINISHED,
        started_at=long_ago,
        finished_at=NOW - timedelta(hours=1),
    )
    seen: list[str] = []

    async def get_sandbox(sandbox_id: str) -> Mock:
        seen.append(sandbox_id)
        return _sandbox(ended=True)

    @asynccontextmanager
    async def open_provider(org: Org, provider: str, secret_name: str) -> AsyncIterator[Mock]:
        yield Mock(get_sandbox=AsyncMock(side_effect=get_sandbox))

    bind = database_session.get_bind()
    assert await sweep_priced_sandbox_costs(bind, now=NOW, providers=[PROVIDER], open_provider=open_provider) == 2
    assert sorted(seen) == ["sb-active", "sb-just-finished"]


@pytest.mark.description("An account whose provider can't be opened doesn't stop the other accounts being priced.")
async def test_broken_account_does_not_block_others(database_session: Session) -> None:
    _link(database_session, "sb-broken", secret="BrokenSecrets")
    _link(database_session, "sb-good")

    @asynccontextmanager
    async def open_provider(org: Org, provider: str, secret_name: str) -> AsyncIterator[Mock]:
        if secret_name == "BrokenSecrets":
            raise ValueError("no storage")
        yield Mock(get_sandbox=AsyncMock(return_value=_sandbox(ended=True)))

    bind = database_session.get_bind()
    assert await sweep_priced_sandbox_costs(bind, now=NOW, providers=[PROVIDER], open_provider=open_provider) == 1
    assert [row.source_id for row in database_session.exec(select(RunCost)).all()] == ["sb-good"]


@pytest.mark.description("A link from another run cannot move a sandbox's last sighting.")
def test_another_runs_link_leaves_last_seen_alone(database_session: Session) -> None:
    _link(database_session)
    _link(database_session)
    owner, other = database_session.exec(select(Benchmark.id)).all()
    repository = BenchmarkRepository(database_session)
    repository.record_sandbox_link(PROVIDER, "sb-owned", owner, LAST_SEEN)
    repository.record_sandbox_link(PROVIDER, "sb-owned", other, NOW)
    database_session.commit()
    assert _last_seen(database_session, "sb-owned") == LAST_SEEN
