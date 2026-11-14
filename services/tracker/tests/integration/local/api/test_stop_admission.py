import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event, Lock
from typing import cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID

from fastapi import HTTPException, Request
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session

import main as main_module
from tests.factories import make_benchmark, make_task
from tests.utils import TEST_ORG_ID
from tracker.database.models import Benchmark, Org, Task
from tracker.domain import BenchmarkStatus, TaskStatus
from tracker.executor.release_control import get_executor_admission
from tracker.services.tasks.execute import task_attempt_id


@pytest.mark.description(
    "A force-stop waiting for admission leaves the event loop free and commits in the same session."
)
async def test_stop_admission_does_not_block_event_loop(
    control_postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock_held = Event()
    lock_requested = Event()
    release_lock = Event()
    timed_out: list[bool] = []

    def hold_admission() -> None:
        with Session(control_postgres_engine) as blocker:
            get_executor_admission(blocker, for_update=True)
            lock_held.set()
            if not release_lock.wait(timeout=2):
                timed_out.append(True)
            blocker.rollback()

    async def release_from_event_loop() -> None:
        while not lock_requested.is_set():
            await asyncio.sleep(0)
        release_lock.set()

    with Session(control_postgres_engine, expire_on_commit=False) as session:
        benchmark = make_benchmark()
        task = make_task(benchmark, "hello-a")
        session.add_all([benchmark, task])
        session.commit()
        org = session.get(Org, TEST_ORG_ID)
        assert org is not None

        def request_admission(current_session: Session, *, for_update: bool = False) -> object:
            assert current_session is session
            lock_requested.set()
            return get_executor_admission(current_session, for_update=for_update)

        monkeypatch.setattr(main_module, "get_executor_admission", request_admission)
        monkeypatch.setattr(main_module, "_force_stop_runtime", Mock())
        monkeypatch.setattr(main_module, "force_stop_sandboxes", AsyncMock())

        with ThreadPoolExecutor(max_workers=1) as executor:
            blocker = executor.submit(hold_admission)
            try:
                assert await asyncio.to_thread(lock_held.wait, 5)
                response, _ = await asyncio.gather(
                    main_module.stop_benchmark(
                        benchmark.id,
                        cast(Request, Mock()),
                        force=True,
                        task_ids=None,
                        expected_attempt_ids=None,
                        session=session,
                        org=org,
                    ),
                    release_from_event_loop(),
                )
            finally:
                release_lock.set()
                blocker.result(timeout=5)

        assert response.status == "success"
        assert timed_out == []

        with Session(control_postgres_engine) as fresh:
            stopped_run = fresh.get(Benchmark, benchmark.id)
            stopped_task = fresh.get(Task, task.id)
            assert stopped_run is not None and stopped_run.status == BenchmarkStatus.STOPPED
            assert stopped_task is not None and stopped_task.status == TaskStatus.STOPPED


@pytest.mark.description("Three force-stops commit without starving a two-worker default executor.")
def test_parallel_stops_do_not_starve_executor(
    control_postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock_held = Event()
    release_lock = Event()
    waiters_ready = Event()
    counter_lock = Lock()
    waiting = 0

    def hold_admission() -> None:
        with Session(control_postgres_engine) as blocker:
            get_executor_admission(blocker, for_update=True)
            lock_held.set()
            assert release_lock.wait(timeout=5)
            blocker.rollback()

    def request_admission(session: Session, *, for_update: bool = False) -> object:
        nonlocal waiting
        with counter_lock:
            waiting += 1
            if waiting == 2:
                waiters_ready.set()
        return get_executor_admission(session, for_update=for_update)

    monkeypatch.setattr(main_module, "get_executor_admission", request_admission)
    monkeypatch.setattr(main_module, "_force_stop_runtime", Mock())
    monkeypatch.setattr(main_module, "force_stop_sandboxes", AsyncMock())
    with Session(control_postgres_engine, expire_on_commit=False) as setup:
        benchmarks = [make_benchmark() for _ in range(3)]
        setup.add_all(benchmarks + [make_task(benchmark, "hello-a") for benchmark in benchmarks])
        setup.commit()
        run_ids = [benchmark.id for benchmark in benchmarks]

    async def run_stops() -> list[object]:
        asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=2))

        async def stop(run_id: UUID) -> object:
            with Session(control_postgres_engine, expire_on_commit=False) as session:
                session.execute(text("SET LOCAL lock_timeout = '2s'"))
                org = session.get(Org, TEST_ORG_ID)
                assert org is not None
                return await main_module.stop_benchmark(
                    run_id,
                    cast(Request, Mock()),
                    force=True,
                    task_ids=None,
                    expected_attempt_ids=None,
                    session=session,
                    org=org,
                )

        async def release_when_waiting() -> None:
            while not waiters_ready.is_set():
                await asyncio.sleep(0)
            release_lock.set()

        stops = asyncio.gather(*(stop(run_id) for run_id in run_ids), return_exceptions=True)
        try:
            await asyncio.wait_for(release_when_waiting(), timeout=3)
            return list(await asyncio.wait_for(stops, timeout=6))
        finally:
            release_lock.set()

    with ThreadPoolExecutor(max_workers=1) as executor:
        blocker = executor.submit(hold_admission)
        try:
            assert lock_held.wait(timeout=5)
            results = asyncio.run(run_stops())
        finally:
            release_lock.set()
            blocker.result(timeout=5)

    assert not [result for result in results if isinstance(result, BaseException)]
    with Session(control_postgres_engine) as fresh:
        for run_id in run_ids:
            stopped = fresh.get(Benchmark, run_id)
            assert stopped is not None and stopped.status == BenchmarkStatus.STOPPED


@pytest.mark.description("Guarded stops capture without admission locks and recheck attempts before committing.")
@pytest.mark.parametrize("replace_attempt", [False, True])
async def test_guarded_stop_rechecks_after_capture(
    control_postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch, replace_attempt: bool
) -> None:
    with Session(control_postgres_engine, expire_on_commit=False) as session:
        benchmark = make_benchmark()
        task = make_task(benchmark, "hello-a", status=TaskStatus.IN_PROGRESS)
        session.add_all([benchmark, task])
        session.commit()
        org = session.get(Org, TEST_ORG_ID)
        assert org is not None
        expected = task_attempt_id(task.id, task.started_at)
        captured = [Mock(id="original-sandbox")]

        async def capture(*_args: object, **_kwargs: object) -> list[Mock]:
            with Session(control_postgres_engine) as replacement:
                replacement.execute(text("SET LOCAL lock_timeout = '2s'"))
                get_executor_admission(replacement, for_update=True)
                current = replacement.get(Task, task.id)
                assert current is not None
                if replace_attempt:
                    current.started_at += timedelta(seconds=1)
                    replacement.add(current)
                replacement.commit()
            return captured

        stop = AsyncMock()
        monkeypatch.setattr(main_module, "_force_stop_runtime", Mock())
        monkeypatch.setattr(main_module, "capture_stop_sandboxes", capture)
        monkeypatch.setattr(main_module, "force_stop_sandboxes", stop)

        request = main_module.stop_benchmark(
            benchmark.id,
            cast(Request, Mock()),
            force=True,
            task_ids=[task.task_id],
            expected_attempt_ids={task.task_id: expected},
            session=session,
            org=org,
        )
        if replace_attempt:
            with pytest.raises(HTTPException) as error:
                await request
            assert error.value.status_code == 409
            assert error.value.detail == "Task attempt has changed"
            stop.assert_not_awaited()
        else:
            response = await request
            assert response.status == "success"
            assert stop.call_args.kwargs["sandboxes"] == captured

        session.rollback()
        with Session(control_postgres_engine) as fresh:
            current = fresh.get(Task, task.id)
            assert current is not None
            assert current.status == (TaskStatus.IN_PROGRESS if replace_attempt else TaskStatus.STOPPED)
