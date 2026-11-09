from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import Engine
from sqlmodel import Session, select

import tracker.modal_cost as modal_cost
import tracker.sandbox_cost as sandbox_cost
from tests.factories import make_benchmark
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    Org,
    RunCost,
    SandboxRecord,
)
from tracker.modal_cost import modal_usage_window, read_modal_run_cost, sweep_modal_costs
from tracker.repositories.advisory_lock import PostgresAdvisoryLock
from tracker.repositories.cost import MODAL_PROVIDER, CostRepository

NOW = datetime(2026, 10, 21, 18, tzinfo=UTC)


def _modal_run(
    session: Session,
    *,
    finished: datetime | None,
    status: BenchmarkStatus = BenchmarkStatus.FINISHED,
    started: datetime = NOW - timedelta(hours=6),
) -> Benchmark:
    benchmark = make_benchmark(status=status, started_at=started, session=None)
    session.add(benchmark)
    session.commit()
    benchmark.finished_at = finished
    session.add(SandboxRecord(provider=MODAL_PROVIDER, sandbox_id=f"sb-{benchmark.id}", benchmark_id=benchmark.id))
    session.commit()
    return benchmark


def _finished_dispatch(session: Session, run: Benchmark, finished_at: datetime) -> None:
    session.add(
        ExecutorDispatch(
            benchmark_id=run.id,
            kind=ExecutorDispatchKind.START,
            executor_release_id="release-1",
            executor_artifact_uri="s3://test/executor",
            executor_artifact_digest="sha256:test",
            executor_protocol_version="1",
            finished_at=finished_at,
        )
    )
    session.commit()


@pytest.mark.description("One sandbox cost row per finished Modal run, read from Modal by the run's id tag.")
class TestSweepModalCosts:
    @pytest.mark.description(
        "Only a finished run with a finish time, a Modal sandbox and no Modal cost row is selected."
    )
    def test_selects_finished_modal_runs_without_a_modal_cost_row(self, database_session: Session) -> None:
        unpriced = _modal_run(database_session, finished=NOW - timedelta(hours=2))
        priced = _modal_run(database_session, finished=NOW - timedelta(hours=2))
        _modal_run(database_session, finished=None, status=BenchmarkStatus.IN_PROGRESS)
        stopped = _modal_run(database_session, finished=NOW, status=BenchmarkStatus.STOPPED)
        stopped.finished_at = None
        daytona = make_benchmark(status=BenchmarkStatus.FINISHED, started_at=NOW, session=None)
        database_session.add(daytona)
        database_session.commit()
        database_session.add_all(
            [
                SandboxRecord(provider="daytona", sandbox_id="sb-daytona", benchmark_id=daytona.id),
                RunCost(
                    benchmark_id=priced.id,
                    cost_type="sandbox",
                    source_id=str(priced.id),
                    provider=MODAL_PROVIDER,
                    amount_usd=1.0,
                    usage_end=NOW - timedelta(hours=2),
                ),
            ]
        )
        database_session.commit()

        assert [run.id for run in CostRepository(database_session).modal_runs_to_price()] == [unpriced.id]

    @pytest.mark.description("A Modal run gets one row over its whole UTC hours, and a second sweep writes nothing.")
    async def test_writes_one_row_per_run_once(self, database_session: Session) -> None:
        run = _modal_run(database_session, finished=NOW - timedelta(hours=2, minutes=30))
        windows: list[tuple[object, datetime, datetime]] = []

        async def read_modal_cost(org: Org, benchmark: Benchmark, start: datetime, end: datetime) -> float:
            windows.append((benchmark.id, start, end))
            return 63.65

        bind = database_session.get_bind()
        first = await sweep_modal_costs(bind, now=NOW, read_modal_cost=read_modal_cost)
        second = await sweep_modal_costs(bind, now=NOW, read_modal_cost=read_modal_cost)

        assert (first, second) == (1, 0)
        assert windows == [(run.id, NOW - timedelta(hours=6), NOW - timedelta(hours=2))]
        rows = database_session.exec(select(RunCost)).all()
        assert [
            (r.cost_type, r.provider, r.source_id, r.benchmark_id, r.amount_usd, modal_cost._as_utc(r.usage_end))
            for r in rows
            if r.usage_end is not None
        ] == [("sandbox", MODAL_PROVIDER, str(run.id), run.id, 63.65, NOW - timedelta(hours=2, minutes=30))]

    @pytest.mark.description("A retried Modal run replaces its prior total and window end without adding a row.")
    async def test_reprices_a_retried_run_once(self, database_session: Session) -> None:
        run = _modal_run(database_session, finished=NOW - timedelta(hours=4, minutes=30))
        totals = iter([12.5, 18.75])

        async def read_modal_cost(_org: Org, _run: Benchmark, _start: datetime, _end: datetime) -> float:
            return next(totals)

        bind = database_session.get_bind()
        first = await sweep_modal_costs(bind, now=NOW, read_modal_cost=read_modal_cost)
        run.finished_at = NOW - timedelta(hours=2, minutes=30)
        database_session.add(run)
        database_session.commit()
        second = await sweep_modal_costs(bind, now=NOW, read_modal_cost=read_modal_cost)
        third = await sweep_modal_costs(bind, now=NOW, read_modal_cost=read_modal_cost)

        assert (first, second, third) == (1, 1, 0)

        rows = database_session.exec(select(RunCost)).all()
        assert [(row.amount_usd, modal_cost._as_utc(row.usage_end)) for row in rows if row.usage_end is not None] == [
            (18.75, NOW - timedelta(hours=2, minutes=30))
        ]

    @pytest.mark.description("A run that finishes in a month's last UTC hour keeps its cost in that month.")
    async def test_a_run_finishing_in_a_months_last_hour_stays_in_that_month(self, database_session: Session) -> None:
        finished = datetime(2026, 10, 31, 23, 20, tzinfo=UTC)
        run = _modal_run(database_session, finished=finished, started=datetime(2026, 10, 31, 20, 5, tzinfo=UTC))
        windows: list[tuple[datetime, datetime]] = []

        async def read_modal_cost(_org: Org, _run: Benchmark, start: datetime, end: datetime) -> float:
            windows.append((start, end))
            return 4.0

        await sweep_modal_costs(
            database_session.get_bind(), now=datetime(2026, 11, 1, 1, tzinfo=UTC), read_modal_cost=read_modal_cost
        )

        assert windows == [(datetime(2026, 10, 31, 20, tzinfo=UTC), datetime(2026, 11, 1, 0, tzinfo=UTC))]
        row = database_session.exec(select(RunCost).where(RunCost.benchmark_id == run.id)).one()
        assert modal_cost._as_utc(cast(datetime, row.usage_end)) == finished

    @pytest.mark.description("A stopped Modal run uses its latest completed executor dispatch as the window end.")
    async def test_prices_a_stopped_run_through_its_last_dispatch(self, database_session: Session) -> None:
        run = _modal_run(database_session, finished=None, status=BenchmarkStatus.STOPPED)
        _finished_dispatch(database_session, run, NOW - timedelta(hours=2, minutes=30))
        windows: list[tuple[datetime, datetime]] = []

        async def read_modal_cost(_org: Org, _run: Benchmark, start: datetime, end: datetime) -> float:
            windows.append((start, end))
            return 7.25

        written = await sweep_modal_costs(database_session.get_bind(), now=NOW, read_modal_cost=read_modal_cost)

        assert written == 1
        assert windows == [(NOW - timedelta(hours=6), NOW - timedelta(hours=2))]

        row = database_session.exec(select(RunCost)).one()
        assert row.usage_end is not None
        assert (row.amount_usd, modal_cost._as_utc(row.usage_end)) == (7.25, NOW - timedelta(hours=2, minutes=30))

    @pytest.mark.description("A stopped Modal run without a finish time or completed dispatch is skipped.")
    async def test_skips_a_stopped_run_without_an_end(self, database_session: Session) -> None:
        _modal_run(database_session, finished=None, status=BenchmarkStatus.STOPPED)
        read_modal_cost = AsyncMock(return_value=1.0)

        written = await sweep_modal_costs(database_session.get_bind(), now=NOW, read_modal_cost=read_modal_cost)

        assert written == 0
        read_modal_cost.assert_not_awaited()

    @pytest.mark.description("A Modal run is priced only once its last UTC hour has closed and the margin has passed.")
    async def test_waits_until_the_runs_last_hour_has_closed(self, database_session: Session) -> None:
        _modal_run(database_session, finished=NOW - timedelta(minutes=10))
        read_modal_cost = AsyncMock(return_value=1.0)
        bind = database_session.get_bind()

        written = await sweep_modal_costs(bind, now=NOW, read_modal_cost=read_modal_cost)
        later = await sweep_modal_costs(bind, now=NOW + timedelta(hours=1), read_modal_cost=read_modal_cost)

        assert (written, later) == (0, 1)
        read_modal_cost.assert_awaited_once()

    @pytest.mark.description("A failed Modal read logs one warning and the next Modal run is still priced.")
    async def test_a_failed_read_skips_that_run(
        self, database_session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        failing = _modal_run(database_session, finished=NOW - timedelta(hours=2))
        healthy = _modal_run(database_session, finished=NOW - timedelta(hours=2), started=NOW - timedelta(hours=5))
        warning = Mock()
        monkeypatch.setattr(modal_cost.logger, "warning", warning)

        async def read_modal_cost(org: Org, benchmark: Benchmark, start: datetime, end: datetime) -> float:
            if benchmark.id == failing.id:
                raise RuntimeError("modal unavailable")
            return 2.5

        written = await sweep_modal_costs(database_session.get_bind(), now=NOW, read_modal_cost=read_modal_cost)

        assert written == 1
        warning.assert_called_once()
        assert warning.call_args.kwargs["extra"]["benchmark_id"] == str(failing.id)
        assert database_session.exec(select(RunCost.source_id)).all() == [str(healthy.id)]

    @pytest.mark.description("A sweep that cannot reach the database logs the failure and writes nothing.")
    async def test_a_failing_sweep_returns_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        exception = Mock()
        monkeypatch.setattr(modal_cost.logger, "exception", exception)

        assert await sweep_modal_costs(cast(Engine, None), now=NOW) == 0
        exception.assert_called_once_with("Modal cost sweep failed")


@pytest.mark.description("The window of whole UTC hours that Modal reports a run's cost in.")
class TestModalUsageWindow:
    @pytest.mark.description("A run in another timezone still gets whole UTC hours.")
    def test_whole_utc_hours_for_another_offset(self) -> None:
        offset = timezone(timedelta(hours=5, minutes=30))
        run = SimpleNamespace(
            started_at=datetime(2026, 10, 21, 10, 0, tzinfo=offset),
            finished_at=datetime(2026, 10, 21, 12, 40, tzinfo=offset),
        )

        assert modal_usage_window(cast(Benchmark, run)) == (
            datetime(2026, 10, 21, 4, tzinfo=UTC),
            datetime(2026, 10, 21, 8, tzinfo=UTC),
        )

    @pytest.mark.description("A run that finishes exactly on the hour ends its window on that hour.")
    def test_finishing_on_the_hour(self) -> None:
        run = SimpleNamespace(started_at=NOW - timedelta(hours=6), finished_at=NOW)

        assert modal_usage_window(cast(Benchmark, run)) == (NOW - timedelta(hours=6), NOW)


@pytest.mark.description("The Modal reader that the sweep uses.")
class TestReadModalRunCost:
    @pytest.mark.description(
        "Modal is read by the run's tag in spans of at most seven days, with one client per token."
    )
    async def test_reads_the_run_tag_in_seven_day_spans_with_one_client(self, monkeypatch: pytest.MonkeyPatch) -> None:
        run = SimpleNamespace(id="3c6b5748-eef0-4c1b-8eef-15e6a44a712d")
        requests: list[dict[str, Any]] = []

        async def stats(**kwargs: Any) -> Any:
            requests.append(kwargs)
            yield SimpleNamespace(cost=1.25)

        app = SimpleNamespace(sandboxes=SimpleNamespace(stats=SimpleNamespace(aio=stats)))
        from_credentials = AsyncMock(return_value=Mock())
        monkeypatch.setattr(
            modal_cost,
            "run_provider_config",
            AsyncMock(return_value=modal_cost.ModalProviderConfig(MODAL_TOKEN_ID="id", MODAL_TOKEN_SECRET="secret")),
        )
        monkeypatch.setattr(modal_cost, "_modal_clients", {})
        monkeypatch.setattr(
            modal_cost, "Client", SimpleNamespace(from_credentials=SimpleNamespace(aio=from_credentials))
        )
        monkeypatch.setattr(modal_cost, "App", SimpleNamespace(lookup=SimpleNamespace(aio=AsyncMock(return_value=app))))
        start = NOW - timedelta(days=10)

        total = await read_modal_run_cost(Mock(), cast(Benchmark, run), start, NOW)
        await read_modal_run_cost(Mock(), cast(Benchmark, run), start, start + timedelta(days=1))

        assert total == 2.5
        from_credentials.assert_awaited_once_with("id", "secret")
        assert [(r["since"], r["until"]) for r in requests] == [
            (start, start + timedelta(days=7)),
            (start + timedelta(days=7), NOW),
            (start, start + timedelta(days=1)),
        ]
        assert all(r["tags"] == {"run-id": run.id} for r in requests)


class _Leader:
    async def still_held(self) -> bool:
        return True


@pytest.mark.description("The leader's tick runs the Modal sweep beside the Daytona sweep.")
class TestLeaderRunsBothSweeps:
    @pytest.mark.description("A leading worker runs both sweeps and returns the rows both wrote.")
    async def test_runs_both_sweeps(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sandbox_cost, "sweep_sandbox_costs", AsyncMock(return_value=4))
        monkeypatch.setattr(sandbox_cost, "sweep_modal_costs", AsyncMock(return_value=2))

        written = await sandbox_cost.sweep_sandbox_costs_as_leader(
            cast(PostgresAdvisoryLock, _Leader()), cast(Engine, None), now=NOW
        )

        assert written == 6
