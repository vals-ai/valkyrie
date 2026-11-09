import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine, text

import tracker.sandbox_cost as sandbox_cost
from tracker.repositories.advisory_lock import sandbox_cost_sweep_lock

NOW = datetime(2026, 10, 23, 18, tzinfo=UTC)
TICK_SECONDS = 0.2
WAIT_SECONDS = 10.0


class Loop:
    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.thread = threading.Thread(
            target=sandbox_cost.run_sandbox_cost_loop,
            args=(self.stop_event,),
            kwargs={"interval_seconds": TICK_SECONDS},
        )
        self.thread.name = f"loop-{id(self)}"

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(WAIT_SECONDS)
        assert not self.thread.is_alive()


class Sweeps:
    def __init__(self) -> None:
        self.by_thread: list[str] = []
        self.failures_left = 0
        self._lock = threading.Lock()

    async def __call__(self, _bind: object, *, now: datetime) -> int:
        with self._lock:
            self.by_thread.append(threading.current_thread().name)
            failing = self.failures_left > 0
            self.failures_left -= 1 if failing else 0
        if failing:
            raise RuntimeError("daytona unavailable")
        return 1

    def sweepers(self) -> set[str]:
        return set(self.by_thread)


def wait_until(condition: Callable[[], bool]) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while not condition():
        assert time.monotonic() < deadline, "condition not reached"
        time.sleep(TICK_SECONDS / 5)


async def lock_is_free(engine: Engine) -> bool:
    probe = sandbox_cost_sweep_lock(engine)
    acquired = await probe.acquire()
    await probe.release()
    return acquired


@pytest.fixture
def sweeps(postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> Sweeps:
    recorder = Sweeps()
    monkeypatch.setattr(sandbox_cost, "engine", postgres_engine)
    monkeypatch.setattr(sandbox_cost, "sweep_sandbox_costs", recorder)
    return recorder


@contextmanager
def running(count: int) -> Iterator[list[Loop]]:
    loops = [Loop() for _ in range(count)]
    for loop in loops:
        loop.thread.start()
    try:
        yield loops
    finally:
        for loop in loops:
            loop.stop()


@pytest.mark.description("Exactly one of the running loops sweeps sandbox costs.")
@pytest.mark.parametrize("loops_running", [1, 2, 4])
def test_exactly_one_of_the_running_loops_sweeps(sweeps: Sweeps, loops_running: int) -> None:
    with running(loops_running):
        wait_until(lambda: len(sweeps.by_thread) >= 5)
        sweepers_while_running = sweeps.sweepers()

    assert len(sweepers_while_running) == 1


@pytest.mark.description("The lock holder sweeps sandbox costs about once per tick.")
def test_a_leader_sweeps_once_per_tick(sweeps: Sweeps) -> None:
    with running(2):
        wait_until(lambda: len(sweeps.by_thread) >= 3)
        started = time.monotonic()
        count_at_start = len(sweeps.by_thread)
        time.sleep(TICK_SECONDS * 10)
        sweeps_since = len(sweeps.by_thread) - count_at_start
        ticks_since = (time.monotonic() - started) / TICK_SECONDS

    assert ticks_since * 0.6 <= sweeps_since <= ticks_since + 1


@pytest.mark.description("A sweep that raises keeps its advisory lock, and only one worker keeps sweeping.")
async def test_a_sweep_that_raises_keeps_the_lock_and_the_leadership(postgres_engine: Engine, sweeps: Sweeps) -> None:
    sweeps.failures_left = 1

    with running(2):
        wait_until(lambda: len(sweeps.by_thread) >= 4)
        assert not await lock_is_free(postgres_engine)
        sweepers_while_running = sweeps.sweepers()

    assert len(sweepers_while_running) == 1


@pytest.mark.description("Stopping the sweeping worker releases the advisory lock.")
async def test_stopping_the_leader_frees_the_lock(postgres_engine: Engine, sweeps: Sweeps) -> None:
    with running(1) as [leader]:
        wait_until(lambda: len(sweeps.by_thread) >= 1)
        assert not await lock_is_free(postgres_engine)
        leader.stop()
        assert await lock_is_free(postgres_engine)


@pytest.mark.description("A waiting loop takes over after the leader stops.")
def test_a_waiting_loop_leads_after_the_leader_stops(sweeps: Sweeps) -> None:
    with running(2) as loops:
        wait_until(lambda: len(sweeps.by_thread) >= 2)
        (first_leader_name,) = sweeps.sweepers()
        leader = next(loop for loop in loops if loop.thread.name == first_leader_name)
        follower = next(loop for loop in loops if loop is not leader)
        leader.stop()
        wait_until(lambda: follower.thread.name in sweeps.sweepers())

    assert sweeps.sweepers() == {leader.thread.name, follower.thread.name}


def lock_holder_pids(engine: Engine) -> set[int]:
    with engine.connect() as admin:
        rows = admin.execute(text("SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted"))
        return {row[0] for row in rows}


@pytest.mark.description(
    "When the leader's Postgres connection is killed, sweeping continues with one new lock holder."
)
def test_loops_keep_exactly_one_leader_after_the_leader_connection_is_killed(
    postgres_engine: Engine, sweeps: Sweeps
) -> None:
    with running(2):
        wait_until(lambda: len(lock_holder_pids(postgres_engine)) == 1)
        (killed_pid,) = lock_holder_pids(postgres_engine)
        with postgres_engine.connect() as admin:
            admin.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": killed_pid})
            admin.commit()
        sweeps_at_kill = len(sweeps.by_thread)

        wait_until(lambda: len(sweeps.by_thread) >= sweeps_at_kill + 3)
        holders = lock_holder_pids(postgres_engine)

    assert len(holders) == 1
    assert killed_pid not in holders


@pytest.mark.description(
    "A leader whose connection died no longer holds or can reacquire the lock, and a waiting worker takes it."
)
async def test_a_leader_whose_connection_died_hands_over_to_a_waiting_worker(
    postgres_engine: Engine,
) -> None:
    old_leader = sandbox_cost_sweep_lock(postgres_engine)
    assert await old_leader.acquire()
    backend_pid = old_leader.connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
    old_leader.connection.commit()
    with postgres_engine.connect() as admin:
        admin.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": backend_pid})
        admin.commit()

    new_leader = sandbox_cost_sweep_lock(postgres_engine)
    deadline = time.monotonic() + WAIT_SECONDS
    while not await new_leader.acquire():
        assert time.monotonic() < deadline, "lock never freed"
        time.sleep(TICK_SECONDS)

    assert not await old_leader.still_held()
    assert not old_leader.held
    assert not await old_leader.acquire()
    await new_leader.release()
