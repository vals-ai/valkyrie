import asyncio
import json
import threading
import time
from collections.abc import Sequence
from uuid import UUID

import pytest
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import Benchmark, Org
from tracker.repositories.benchmark import BenchmarkRepository
import tracker.services.results as results_module
from tracker.services.results import stream_benchmark_results


@pytest.mark.description(
    "A run's result stream reads the database in a worker thread, so the event loop keeps running."
)
async def test_result_stream_reads_database_off_the_event_loop(
    postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    with Session(postgres_engine, expire_on_commit=False) as session:
        org = Org(name="event-loop", storage_key="bucket")
        session.add(org)
        session.commit()
        benchmark = make_benchmark(org_id=org.id, session=session)

    read_threads: list[int] = []
    list_with_final_evaluation = BenchmarkRepository.list_with_final_evaluation

    def slow_list_with_final_evaluation(
        repository: BenchmarkRepository, benchmark_ids: Sequence[UUID]
    ) -> Sequence[Benchmark]:
        read_threads.append(threading.get_ident())
        time.sleep(0.3)
        return list_with_final_evaluation(repository, benchmark_ids)

    monkeypatch.setattr(BenchmarkRepository, "list_with_final_evaluation", slow_list_with_final_evaluation)
    ticks = 0

    async def tick() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    ticker = asyncio.create_task(tick())
    events = stream_benchmark_results(benchmark.id, postgres_engine, "s3://bucket", org.id, storage_bucket=None)
    try:
        first = await anext(events)
    finally:
        await events.aclose()
        ticker.cancel()

    assert first.startswith("data:")
    assert read_threads and threading.get_ident() not in read_threads
    assert ticks >= 10


@pytest.mark.description(
    "Open result streams share one database read per poll, whichever runs they watch, and stop polling once closed."
)
async def test_result_streams_share_one_database_read_per_poll(
    postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    with Session(postgres_engine, expire_on_commit=False) as session:
        org = Org(name="shared-poll", storage_key="bucket")
        session.add(org)
        session.commit()
        first_run = make_benchmark(name="first-run", org_id=org.id, session=session)
        second_run = make_benchmark(name="second-run", org_id=org.id, session=session)

    checkouts = 0

    def count_checkout(*_args: object) -> None:
        nonlocal checkouts
        checkouts += 1

    monkeypatch.setattr(results_module, "RESULT_POLL_INTERVAL", 0.5)
    event.listen(postgres_engine, "checkout", count_checkout)
    watched = [first_run.id, first_run.id, second_run.id]
    streams = [
        stream_benchmark_results(run_id, postgres_engine, "s3://bucket", org.id, storage_bucket=None)
        for run_id in watched
    ]
    try:
        for stream in streams:
            await anext(stream)
        initial_checkouts = checkouts
        polled = [json.loads((await anext(stream)).removeprefix("data: ")) for stream in streams]
        poll_checkouts = checkouts - initial_checkouts
    finally:
        for stream in streams:
            await stream.aclose()
    closed_checkouts = checkouts
    await asyncio.sleep(1.5)

    assert initial_checkouts == len(watched)
    assert poll_checkouts == 1
    assert [update["benchmark_id"] for update in polled] == [str(run_id) for run_id in watched]
    assert checkouts == closed_checkouts


@pytest.mark.description(
    "A failed shared poll goes to Sentry and skips one update, and every open result stream gets the next one."
)
async def test_result_streams_survive_a_failed_poll(postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    with Session(postgres_engine, expire_on_commit=False) as session:
        org = Org(name="failed-poll", storage_key="bucket")
        session.add(org)
        session.commit()
        first_run = make_benchmark(name="first-run", org_id=org.id, session=session)
        second_run = make_benchmark(name="second-run", org_id=org.id, session=session)

    read_runs = results_module._read_runs
    reads = 0
    captured: list[BaseException] = []

    def fail_first_poll(bind: Engine, benchmark_ids: Sequence[UUID]) -> results_module._RunSnapshot:
        nonlocal reads
        reads += 1
        if reads == 3:
            raise ConnectionError("connection dropped")
        return read_runs(bind, benchmark_ids)

    monkeypatch.setattr(results_module, "RESULT_POLL_INTERVAL", 0.05)
    monkeypatch.setattr(results_module, "_read_runs", fail_first_poll)
    monkeypatch.setattr(results_module, "capture_exception", captured.append)
    streams = [
        stream_benchmark_results(run_id, postgres_engine, "s3://bucket", org.id, storage_bucket=None)
        for run_id in (first_run.id, second_run.id)
    ]
    try:
        for stream in streams:
            await anext(stream)
        updates = [await asyncio.wait_for(anext(stream), timeout=2) for stream in streams]
    finally:
        for stream in streams:
            await stream.aclose()

    assert [type(error) for error in captured] == [ConnectionError]
    assert reads >= 4
    assert all(update.startswith("data:") for update in updates)
