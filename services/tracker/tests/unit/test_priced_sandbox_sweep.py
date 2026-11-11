from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service import SandboxNotFoundError
from sqlmodel import Session, select

from tests.factories import make_benchmark
from tracker.services.sandboxes.costs import sweep_priced_sandbox_costs
from tracker.database.models import BenchmarkStatus, Org, RunCost, SandboxRecord

NOW = datetime(2026, 11, 11, 12, tzinfo=UTC)
SECRET = "PricedSecrets"
PROVIDER = "priced"
COST = 0.42


def _sandbox(*, ended: bool) -> Mock:
    return Mock(has_ended=ended, cost_usd=Mock(return_value=COST))


def _link(
    session: Session,
    *sandbox_ids: str,
    secret: str = SECRET,
    status: BenchmarkStatus = BenchmarkStatus.IN_PROGRESS,
    started_at: datetime = NOW - timedelta(hours=1),
    finished_at: datetime | None = None,
) -> None:
    benchmark = make_benchmark(status=status, started_at=started_at, session=None)
    benchmark.arguments = benchmark.arguments.model_copy(update={"sandbox_provider_secret_name": secret})
    session.add(benchmark)
    session.commit()
    if finished_at is not None:
        benchmark.finished_at = finished_at
        session.commit()
    session.add_all(
        SandboxRecord(provider=PROVIDER, sandbox_id=sandbox_id, benchmark_id=benchmark.id) for sandbox_id in sandbox_ids
    )
    session.commit()


@pytest.mark.description("The sweep prices every ended sandbox of a priced provider once, up to when it saw it ended.")
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
    sandboxes["sb-deleted"].cost_usd.assert_called_with(NOW)


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
