import asyncio
from collections.abc import AsyncGenerator, Callable, Iterator
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest
from benchmark_service import (
    ResourceCapacity,
    Sandbox,
    SandboxCapacity,
    SandboxError,
    SandboxProvider,
    SandboxProviderConfig,
)
from benchmark_service.client import BenchmarkServiceClient
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from tests.integration.local.database.test_scheduler import (
    _ATTEMPT,
    _CAPACITY,
    _DEMAND,
    _RESOURCES,
    _SOURCE,
    MockProvider,
    _build_lock_is_free,
    _candidate_count,
    _context,
    _prepare,
    _publish_run_state,
    _recover_once,
    _reservation_count,
    _reserve,
    _revoke_dispatch,
    _run,
    _sandbox,
    _set_concurrency,
    _stop_run,
    _task,
    _update_task,
)
from tests.integration.local.database.test_scheduler import (
    executed_sql as executed_sql,
)
from tracker.database.models import (
    Benchmark,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Org,
    SandboxAdmissionCandidate,
    SandboxBuildReservation,
    Task,
    TaskStatus,
)
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.repositories import advisory_lock
from tracker.repositories.scheduler import SchedulerRepository
from tracker.runtime.services import RuntimeServices
from tracker.sandbox import SandboxCreationState
from tracker.scheduler import admission, coordinator, store
import tracker.services.runs.start as run_orchestration
import tracker.services.tasks.execute as task_execution
import tracker.services.tasks.monitor as task_monitor_module


async def _until(predicate: Callable[[], bool]) -> None:
    async with asyncio.timeout(10):
        while not predicate():
            await asyncio.sleep(0.01)


def _grant(
    session: Session,
    pool_id: str,
    *,
    capacities: dict[tuple[str, str], SandboxCapacity] | None = None,
    limit: int = 10,
    read_at: datetime | None = None,
) -> int:
    granted = SchedulerRepository(session).grant_prepared_tasks(
        pool_id,
        capacities if capacities is not None else {("region-a", "sysbox"): _CAPACITY},
        read_at if read_at is not None else SchedulerRepository(session).database_now(),
        limit,
    )
    session.commit()
    return granted


def _owner(
    context: admission.SandboxQueueContext, org: Org, benchmark: Benchmark, *, rate: int = 0
) -> coordinator.PoolCoordinator:
    async def provider_factory(_reference: coordinator.PoolReference) -> SandboxProvider:
        return context.provider

    return coordinator.PoolCoordinator(
        context.engine,
        coordinator.PoolReference(context.pool_id, benchmark, org),
        provider_factory=provider_factory,
        interval_seconds=0.01,
        rate_per_minute=rate,
    )


def _candidate(engine: Engine, task: Task) -> SandboxAdmissionCandidate:
    with Session(engine) as session:
        candidate = session.get(SandboxAdmissionCandidate, (task.id, task.started_at))
        assert candidate is not None
        return candidate


async def _worker(
    stack: AsyncExitStack,
    context: admission.SandboxQueueContext,
    task: Task,
    authority: ExecutionAuthority,
    events: list[str],
) -> Sandbox | None:
    creation_state = SandboxCreationState(cleanup_pending=False)
    async with AsyncExitStack() as preparation:
        return await admission.enter_queued_sandbox(
            stack=stack,
            context=context,
            task_row_id=task.id,
            expected_started_at=task.started_at,
            authority=authority,
            source=_SOURCE,
            resources=_RESOURCES,
            creation_state=creation_state,
            create=_sandbox(events, creation_state=creation_state),
            preparation_stack=preparation,
        )


@asynccontextmanager
async def _running(owner: coordinator.PoolCoordinator) -> AsyncGenerator[asyncio.Task[None], None]:
    task = asyncio.create_task(owner.run())
    try:
        yield task
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.description("Batch grants follow priority, then arrival order, per pool.")
def test_batch_grants_priority_fifo_and_pool_isolation(postgres_engine: Engine, postgres_session: Session) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    other_pool = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, (low,) = _run(postgres_session, pool_id, [("low", TaskStatus.PENDING, _ATTEMPT)], priority=4)
    _, _, urgent = _run(
        postgres_session,
        pool_id,
        [("first", TaskStatus.PENDING, _ATTEMPT), ("second", TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=1))],
        priority=0,
        concurrency=2,
    )
    _, _, (other,) = _run(postgres_session, other_pool, [("other", TaskStatus.PENDING, _ATTEMPT)], priority=0)
    for task in [low, *urgent]:
        _prepare(postgres_session, pool_id, task)
    _prepare(postgres_session, other_pool, other)

    assert _grant(postgres_session, pool_id, limit=1) == 1
    assert [_task(postgres_engine, task).status for task in [*urgent, low, other]] == [
        TaskStatus.BUILDING,
        TaskStatus.PENDING,
        TaskStatus.PENDING,
        TaskStatus.PENDING,
    ]
    assert _grant(postgres_session, pool_id, limit=1) == 1
    assert _task(postgres_engine, urgent[1]).status == TaskStatus.BUILDING
    assert _grant(postgres_session, other_pool) == 1
    assert _task(postgres_engine, other).status == TaskStatus.BUILDING
    assert _candidate_count(postgres_engine, pool_id) == 1


@pytest.mark.description("Batch grants keep payloads and session state together atomically.")
@pytest.mark.parametrize("rollback", [False, True])
def test_batch_grants_preserve_payloads_and_session_state_atomically(
    postgres_engine: Engine, postgres_session: Session, rollback: bool
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    tasks = []
    for index in range(3):
        _, _, (task,) = _run(
            postgres_session,
            pool_id,
            [(str(index), TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=2 - index))],
        )
        demand = _DEMAND.model_copy(update={"vcpu": index + 1, "memory": index + 1, "disk": index + 1})
        _prepare(postgres_session, pool_id, task, demand)
        tasks.append(task)

    with Session(postgres_engine) as session:
        loaded_tasks = [session.get(Task, task.id) for task in tasks]
        prepared = session.exec(select(SandboxAdmissionCandidate)).all()
        assert (
            SchedulerRepository(session).grant_prepared_tasks(
                pool_id, {("region-a", "sysbox"): _CAPACITY}, SchedulerRepository(session).database_now(), 3
            )
            == 3
        )
        assert all(task is not None and task.status == TaskStatus.BUILDING for task in loaded_tasks)
        grants = session.exec(select(SandboxBuildReservation)).all()
        by_attempt = {(grant.task_row_id, grant.attempt_started_at): grant for grant in grants}
        assert len(by_attempt) == 3
        assert len({grant.reserved_at for grant in grants}) == 1
        for candidate in prepared:
            key = candidate.task_row_id, candidate.attempt_started_at
            grant = by_attempt[key]
            assert (
                grant.executor_dispatch_id,
                grant.target_id,
                grant.sandbox_class,
                grant.requested_vcpu,
                grant.requested_memory,
                grant.requested_disk,
                grant.requested_gpu,
            ) == (
                candidate.executor_dispatch_id,
                candidate.target_id,
                candidate.sandbox_class,
                candidate.requested_vcpu,
                candidate.requested_memory,
                candidate.requested_disk,
                candidate.requested_gpu,
            )
            assert grant.accepted_at is None and grant.released_at is None
            assert session.get(SandboxAdmissionCandidate, key) is None
        if rollback:
            session.rollback()
        else:
            session.commit()

    expected = TaskStatus.PENDING if rollback else TaskStatus.BUILDING
    assert all(_task(postgres_engine, task).status == expected for task in tasks)
    assert _reservation_count(postgres_engine, pool_id) == (0 if rollback else 3)
    assert _candidate_count(postgres_engine, pool_id) == (3 if rollback else 0)


@pytest.mark.description("Batch repacking sees priority and attempt changes.")
@pytest.mark.parametrize("change", ["higher-priority-ready", "superseded-attempt"])
def test_batch_repacking_observes_priority_and_attempt_changes(
    postgres_engine: Engine, postgres_session: Session, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, (waiting,) = _run(postgres_session, pool_id, [("waiting", TaskStatus.PENDING, _ATTEMPT)], priority=4)
    _prepare(postgres_session, pool_id, waiting)
    _, _, (urgent,) = _run(postgres_session, pool_id, [("urgent", TaskStatus.PENDING, _ATTEMPT)], priority=0)
    _prepare(postgres_session, pool_id, urgent, admission_ready=False)
    original_pack = SchedulerRepository.packed_admission_candidates
    calls = 0
    new_attempt = waiting.started_at + timedelta(seconds=1)

    def pack(*args: Any, **kwargs: Any) -> Iterator[SandboxAdmissionCandidate]:
        nonlocal calls
        candidates = list(original_pack(*args, **kwargs))
        calls += 1
        if calls == 1:
            with Session(postgres_engine) as other:
                if change == "higher-priority-ready":
                    _prepare(other, pool_id, urgent)
                else:
                    _update_task(postgres_engine, waiting, TaskStatus.PENDING, new_attempt)
                    fresh = other.get(Task, waiting.id)
                    assert fresh is not None
                    _prepare(other, pool_id, fresh)
        return iter(candidates)

    with monkeypatch.context() as patch:
        patch.setattr(SchedulerRepository, "packed_admission_candidates", pack)
        assert _grant(postgres_session, pool_id, limit=1) == (0 if change == "higher-priority-ready" else 1)
    assert calls == 2
    if change == "higher-priority-ready":
        assert _task(postgres_engine, waiting).status == TaskStatus.PENDING
        assert _grant(postgres_session, pool_id, limit=1) == 1
        assert _task(postgres_engine, urgent).status == TaskStatus.BUILDING
    else:
        with Session(postgres_engine) as session:
            assert session.get(SandboxBuildReservation, (waiting.id, waiting.started_at)) is None
            assert session.get(SandboxBuildReservation, (waiting.id, new_attempt)) is not None
            assert session.get(SandboxAdmissionCandidate, (waiting.id, waiting.started_at)) is not None
            assert session.get(SandboxAdmissionCandidate, (waiting.id, new_attempt)) is None


@pytest.mark.description(
    "A grant batch moves only current attempts to `BUILDING` and leaves superseded attempts `PENDING` as candidates."
)
@pytest.mark.parametrize("superseded", [1, 3])
def test_batch_only_reserves_and_removes_candidates_for_returned_attempts(
    postgres_engine: Engine, postgres_session: Session, monkeypatch: pytest.MonkeyPatch, superseded: int
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, tasks = _run(
        postgres_session,
        pool_id,
        [(str(index), TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=index)) for index in range(3)],
        concurrency=3,
    )
    for task in tasks:
        _prepare(postgres_session, pool_id, task)
    original_pack = SchedulerRepository.packed_admission_candidates
    calls = 0

    def pack(*args: Any, **kwargs: Any) -> Iterator[SandboxAdmissionCandidate]:
        nonlocal calls
        candidates = list(original_pack(*args, **kwargs))
        calls += 1
        if calls == 2:
            for task in tasks[:superseded]:
                _update_task(postgres_engine, task, TaskStatus.PENDING, task.started_at + timedelta(seconds=10))
        return iter(candidates)

    monkeypatch.setattr(SchedulerRepository, "packed_admission_candidates", pack)
    assert _grant(postgres_session, pool_id, limit=3) == 3 - superseded
    with Session(postgres_engine) as session:
        grants = session.exec(select(SandboxBuildReservation)).all()
        remaining = session.exec(select(SandboxAdmissionCandidate)).all()
    assert {grant.task_row_id for grant in grants} == {task.id for task in tasks[superseded:]}
    assert {candidate.task_row_id for candidate in remaining} == {task.id for task in tasks[:superseded]}
    assert all(_task(postgres_engine, task).status == TaskStatus.PENDING for task in tasks[:superseded])
    assert all(_task(postgres_engine, task).status == TaskStatus.BUILDING for task in tasks[superseded:])


@pytest.mark.description("A batch cannot grant past any capacity dimension.")
@pytest.mark.parametrize("resource", ["cpu", "memory", "disk"])
def test_batch_cannot_overgrant_any_capacity_dimension(
    postgres_engine: Engine,
    postgres_session: Session,
    resource: str,
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, tasks = _run(
        postgres_session,
        pool_id,
        [(str(i), TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=i)) for i in range(2)],
        concurrency=2,
    )
    for task in tasks:
        _prepare(postgres_session, pool_id, task)
    units = {"cpu": 1, "memory": 2, "disk": 3}
    capacity = _CAPACITY.model_copy(update={resource: ResourceCapacity(total=units[resource] * 2, used=0)})
    capacities = {("region-a", "sysbox"): capacity}
    assert _grant(postgres_session, pool_id, capacities=capacities) == 1
    assert _grant(postgres_session, pool_id, capacities=capacities) == 0
    assert [_task(postgres_engine, task).status for task in tasks] == [TaskStatus.BUILDING, TaskStatus.PENDING]


@pytest.mark.description("A batch skips work that does not fit without starving work that does.")
@pytest.mark.parametrize("limit", [0, 1, 2, 20])
def test_batch_skips_nonfitting_prefix_without_starving_fitting_tail(
    postgres_engine: Engine,
    postgres_session: Session,
    limit: int,
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, tasks = _run(
        postgres_session,
        pool_id,
        [(str(i), TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=i)) for i in range(12)],
        concurrency=12,
    )
    for index, task in enumerate(tasks):
        _prepare(postgres_session, pool_id, task, _DEMAND.model_copy(update={"vcpu": 100}) if index < 10 else _DEMAND)
    expected = tasks[10 : 10 + limit]
    assert _grant(postgres_session, pool_id, limit=limit) == len(expected)
    assert {task.id for task in tasks if _task(postgres_engine, task).status == TaskStatus.BUILDING} == {
        task.id for task in expected
    }


@pytest.mark.description("An unknown priority barrier admits ready peers but not lower priority work.")
@pytest.mark.parametrize("unknown_kind", ["unprepared", "stale-rejected", "stale-nonfitting"])
@pytest.mark.parametrize("concurrency", [1, 2])
def test_unknown_priority_barrier_admits_ready_peers_but_not_lower_priority(
    postgres_engine: Engine,
    postgres_session: Session,
    unknown_kind: str,
    concurrency: int,
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, (unknown, ready) = _run(
        postgres_session,
        pool_id,
        [("unknown", TaskStatus.PENDING, _ATTEMPT), ("ready", TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=1))],
        priority=0,
        concurrency=concurrency,
    )
    _, _, (later,) = _run(postgres_session, pool_id, [("later", TaskStatus.PENDING, _ATTEMPT)], priority=1)
    if unknown_kind != "unprepared":
        _prepare(
            postgres_session,
            pool_id,
            unknown,
            _DEMAND.model_copy(update={"vcpu": 100}),
            admission_ready=unknown_kind == "stale-nonfitting",
        )
        _update_task(postgres_engine, unknown, TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1))
    _prepare(postgres_session, pool_id, ready)
    _prepare(postgres_session, pool_id, later)

    assert _grant(postgres_session, pool_id) == 1
    assert _task(postgres_engine, ready).status == TaskStatus.BUILDING
    assert _task(postgres_engine, later).status == TaskStatus.PENDING
    assert _task(postgres_engine, unknown).status == TaskStatus.PENDING


@pytest.mark.description("An unknown tail does not block lower priority work when run slots are full.")
@pytest.mark.parametrize("occupied_by", ["active", "prepared-prefix"])
def test_unknown_tail_does_not_block_lower_priority_when_run_slots_are_full(
    postgres_engine: Engine,
    postgres_session: Session,
    occupied_by: str,
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, (first, unknown) = _run(
        postgres_session,
        pool_id,
        [
            ("first", TaskStatus.IN_PROGRESS if occupied_by == "active" else TaskStatus.PENDING, _ATTEMPT),
            ("unknown", TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=1)),
        ],
        priority=0,
        concurrency=1,
    )
    if occupied_by == "prepared-prefix":
        _prepare(postgres_session, pool_id, first)
    _, _, (later,) = _run(postgres_session, pool_id, [("later", TaskStatus.PENDING, _ATTEMPT)])
    _prepare(postgres_session, pool_id, later)
    assert _grant(postgres_session, pool_id) == (1 if occupied_by == "active" else 2)
    assert _task(postgres_engine, later).status == TaskStatus.BUILDING
    assert _task(postgres_engine, unknown).status == TaskStatus.PENDING


@pytest.mark.description("An unprepared high priority tail keeps precedence after a prepared prefix.")
@pytest.mark.parametrize("vcpu", [13, 100])
def test_unprepared_high_priority_tail_retains_precedence_after_prepared_prefix(
    postgres_engine: Engine,
    postgres_session: Session,
    vcpu: int,
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, urgent = _run(
        postgres_session,
        pool_id,
        [(str(i), TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=i)) for i in range(11)],
        priority=0,
        concurrency=20,
    )
    _, _, (later,) = _run(postgres_session, pool_id, [("later", TaskStatus.PENDING, _ATTEMPT)], priority=2)
    for task in urgent[:10]:
        _prepare(postgres_session, pool_id, task)
    _prepare(postgres_session, pool_id, later)
    capacity = SandboxCapacity(
        cpu=ResourceCapacity(total=vcpu, used=0),
        memory=ResourceCapacity(total=200, used=0),
        disk=ResourceCapacity(total=300, used=0),
    )
    assert _grant(postgres_session, pool_id, capacities={("region-a", "sysbox"): capacity}, limit=20) == 10
    assert _task(postgres_engine, later).status == TaskStatus.PENDING
    _prepare(postgres_session, pool_id, urgent[-1])
    assert _grant(postgres_session, pool_id, capacities={("region-a", "sysbox"): capacity}) == (1 if vcpu == 13 else 2)
    assert _task(postgres_engine, urgent[-1]).status == TaskStatus.BUILDING
    assert _task(postgres_engine, later).status == (TaskStatus.PENDING if vcpu == 13 else TaskStatus.BUILDING)


@pytest.mark.description("High priority work known to be unavailable does not block lower priority work.")
@pytest.mark.parametrize("bypass", ["rejected", "missing-domain", "run-full", "unclaimed"])
def test_known_unavailable_high_priority_work_does_not_block_lower_priority(
    postgres_engine: Engine,
    postgres_session: Session,
    bypass: str,
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, (urgent,) = _run(
        postgres_session,
        pool_id,
        [("urgent", TaskStatus.PENDING, _ATTEMPT)],
        priority=0,
        executor=ExecutorDispatchStatus.QUEUED if bypass == "unclaimed" else ExecutorDispatchStatus.RUNNING,
        concurrency=0 if bypass == "run-full" else 1,
    )
    _, _, (later,) = _run(postgres_session, pool_id, [("later", TaskStatus.PENDING, _ATTEMPT)])
    if bypass != "unclaimed":
        _prepare(
            postgres_session,
            pool_id,
            urgent,
            _DEMAND.model_copy(update={"target_id": "region-b"}) if bypass == "missing-domain" else _DEMAND,
            admission_ready=bypass != "rejected",
        )
    _prepare(postgres_session, pool_id, later)
    assert _grant(postgres_session, pool_id) == 1
    assert _task(postgres_engine, later).status == TaskStatus.BUILDING
    assert _task(postgres_engine, urgent).status == TaskStatus.PENDING


@pytest.mark.description("A rejected candidate gets its priority back when ready.")
def test_rejected_candidate_reclaims_priority_when_ready(postgres_engine: Engine, postgres_session: Session) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, (urgent,) = _run(postgres_session, pool_id, [("urgent", TaskStatus.PENDING, _ATTEMPT)], priority=0)
    _, _, (first, second) = _run(
        postgres_session,
        pool_id,
        [("first", TaskStatus.PENDING, _ATTEMPT), ("second", TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=1))],
        concurrency=2,
    )
    _prepare(postgres_session, pool_id, urgent, admission_ready=False)
    for task in (first, second):
        _prepare(postgres_session, pool_id, task)
    assert _grant(postgres_session, pool_id, limit=1) == 1
    assert _task(postgres_engine, first).status == TaskStatus.BUILDING
    _prepare(postgres_session, pool_id, urgent)
    assert _grant(postgres_session, pool_id, limit=1) == 1
    assert _task(postgres_engine, urgent).status == TaskStatus.BUILDING
    assert _task(postgres_engine, second).status == TaskStatus.PENDING


@pytest.mark.description("A batch follows changes to run concurrency.")
def test_batch_respects_dynamic_run_concurrency(postgres_engine: Engine, postgres_session: Session) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, benchmark, tasks = _run(
        postgres_session,
        pool_id,
        [(str(i), TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=i)) for i in range(3)],
        concurrency=1,
    )
    for task in tasks:
        _prepare(postgres_session, pool_id, task)
    assert _grant(postgres_session, pool_id) == 1
    assert _grant(postgres_session, pool_id) == 0
    _set_concurrency(postgres_engine, benchmark, 2)
    assert _grant(postgres_session, pool_id) == 1
    assert [_task(postgres_engine, task).status for task in tasks] == [
        TaskStatus.BUILDING,
        TaskStatus.BUILDING,
        TaskStatus.PENDING,
    ]


@pytest.mark.description("A batch charges regions, classes, and GPU separately.")
def test_batch_charges_independent_regions_classes_and_gpu(postgres_engine: Engine, postgres_session: Session) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, tasks = _run(
        postgres_session,
        pool_id,
        [(str(i), TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=i)) for i in range(5)],
        concurrency=5,
    )
    gpu = _DEMAND.model_copy(
        update={"target_id": "earth", "sandbox_class": "gpu", "vcpu": 0, "memory": 0, "disk": 0, "gpu": 1}
    )
    demands = [
        _DEMAND,
        _DEMAND.model_copy(update={"sandbox_class": "vm"}),
        _DEMAND.model_copy(update={"target_id": "region-b"}),
        gpu,
        gpu,
    ]
    for task, demand in zip(tasks, demands, strict=True):
        _prepare(postgres_session, pool_id, task, demand)
    capacities = {
        ("region-a", "sysbox"): _CAPACITY,
        ("region-a", "vm"): _CAPACITY,
        ("region-b", "sysbox"): _CAPACITY,
        ("earth", "gpu"): SandboxCapacity(
            cpu=ResourceCapacity(total=0, used=0),
            memory=ResourceCapacity(total=0, used=0),
            disk=ResourceCapacity(total=0, used=0),
            gpu=ResourceCapacity(total=1, used=0),
        ),
    }
    assert _grant(postgres_session, pool_id, capacities=capacities) == 4
    assert _task(postgres_engine, tasks[-1]).status == TaskStatus.PENDING
    rows = postgres_session.exec(select(SandboxBuildReservation)).all()
    assert {(row.target_id, row.sandbox_class) for row in rows} == set(capacities)
    assert sum(row.requested_gpu for row in rows) == 1


@pytest.mark.description("A retained reservation blocks only live builds or the same attempt.")
@pytest.mark.parametrize("released", [False, True])
@pytest.mark.parametrize("same_attempt", [False, True])
def test_retained_reservation_blocks_only_live_builds_or_same_attempt(
    postgres_engine: Engine,
    postgres_session: Session,
    released: bool,
    same_attempt: bool,
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, (retrying, later) = _run(
        postgres_session,
        pool_id,
        [("retrying", TaskStatus.PENDING, _ATTEMPT), ("later", TaskStatus.PENDING, _ATTEMPT)],
    )
    _reserve(postgres_session, pool_id, retrying)
    if released:
        SchedulerRepository(postgres_session).release_reservation(retrying.id, retrying.started_at)
    retrying.status = TaskStatus.PENDING
    retrying.started_at = _ATTEMPT if same_attempt else _ATTEMPT + timedelta(seconds=1)
    later.started_at = retrying.started_at + timedelta(seconds=1)
    postgres_session.commit()
    _prepare(postgres_session, pool_id, retrying)
    _prepare(postgres_session, pool_id, later)

    assert _grant(postgres_session, pool_id) == 1
    blocked = not released or same_attempt
    assert _task(postgres_engine, retrying).status == (TaskStatus.PENDING if blocked else TaskStatus.BUILDING)
    assert _task(postgres_engine, later).status == (TaskStatus.BUILDING if blocked else TaskStatus.PENDING)
    assert postgres_session.get(SandboxBuildReservation, (retrying.id, _ATTEMPT)) is not None


@pytest.mark.description(
    "A retried attempt is granted once after its retained reservation is purged, and a second grant pass grants nothing."
)
def test_same_attempt_waits_for_retention_purge_then_grants_once(
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, _, (task,) = _run(postgres_session, pool_id, [("retry", TaskStatus.PENDING, _ATTEMPT)])
    _prepare(postgres_session, pool_id, task)
    now = _ATTEMPT.replace(tzinfo=UTC)
    monkeypatch.setattr(SchedulerRepository, "database_now", lambda _session: now)
    assert _grant(postgres_session, pool_id) == 1
    SchedulerRepository(postgres_session).release_reservation(task.id, task.started_at)
    postgres_session.refresh(task)
    task.status = TaskStatus.PENDING
    postgres_session.commit()
    _prepare(postgres_session, pool_id, task)
    assert _grant(postgres_session, pool_id) == 0
    assert SchedulerRepository(postgres_session).admissions_since(pool_id, now - timedelta(minutes=1)) == 1
    assert (
        SchedulerRepository(postgres_session)
        .charged_reservations_by_domain(pool_id, now - timedelta(seconds=1))[("region-a", "sysbox")]
        .count
        == 1
    )
    assert SchedulerRepository(postgres_session).charged_reservations_by_domain(pool_id, now) == {}
    now += admission._RELEASED_RESERVATION_RETENTION
    SchedulerRepository(postgres_session).purge_released_reservations(
        pool_id, now - admission._RELEASED_RESERVATION_RETENTION
    )
    assert _grant(postgres_session, pool_id) == 0
    now += timedelta(microseconds=1)
    SchedulerRepository(postgres_session).purge_released_reservations(
        pool_id, now - admission._RELEASED_RESERVATION_RETENTION
    )
    assert _grant(postgres_session, pool_id) == 1
    assert _grant(postgres_session, pool_id) == 0
    assert SchedulerRepository(postgres_session).active_reservations(pool_id).count == 1


@pytest.mark.description(
    "Grants released within the rate window still count against the budget, and capacity is read once per cache."
)
async def test_rate_budget_and_cached_capacity_include_released_grants(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    org, benchmark, tasks = _run(
        postgres_session,
        context.pool_id,
        [(str(i), TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=i)) for i in range(2)],
        concurrency=2,
    )
    for task in tasks:
        _prepare(postgres_session, context.pool_id, task)
    owner = _owner(context, org, benchmark, rate=1)
    cache = coordinator.CapacityCache(context.provider, postgres_engine)
    lock = store.queue_pool_lock(postgres_engine, context.pool_id)
    async with lock as acquired:
        assert acquired
        assert await owner.admit(lock.connection, context, cache) == 1
        with Session(postgres_engine) as session:
            assert admission._start_claimed_task(
                session, task_row_id=tasks[0].id, expected_started_at=tasks[0].started_at
            )
            session.commit()
        assert await owner.admit(lock.connection, context, cache) == 0
        with Session(postgres_engine) as session:
            reservation = session.get(SandboxBuildReservation, (tasks[0].id, tasks[0].started_at))
            assert reservation is not None
            reservation.reserved_at = SchedulerRepository(session).database_now() - timedelta(minutes=2)
            session.commit()
        assert await owner.admit(lock.connection, context, cache) == 1
    assert cast(MockProvider, context.provider).events.count("read_capacity") == 1


@pytest.mark.description("A release after a capacity read stays charged until the next snapshot.")
async def test_release_after_capacity_read_remains_charged_until_new_snapshot(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    context = _context(
        postgres_engine,
        f"daytona:{uuid4()}",
        [],
        snapshot=_CAPACITY.model_copy(update={"cpu": ResourceCapacity(total=2, used=0)}),
    )
    org, benchmark, (first, second) = _run(
        postgres_session,
        context.pool_id,
        [("first", TaskStatus.PENDING, _ATTEMPT), ("second", TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=1))],
        concurrency=2,
    )
    for task in (first, second):
        _prepare(postgres_session, context.pool_id, task)
    owner = _owner(context, org, benchmark)
    cache = coordinator.CapacityCache(context.provider, postgres_engine)
    lock = store.queue_pool_lock(postgres_engine, context.pool_id)
    async with lock as acquired:
        assert acquired
        assert await owner.admit(lock.connection, context, cache) == 1
        with Session(postgres_engine) as session:
            assert admission._start_claimed_task(session, task_row_id=first.id, expected_started_at=first.started_at)
            session.commit()
        assert await owner.admit(lock.connection, context, cache) == 0
        fresh_cache = coordinator.CapacityCache(context.provider, postgres_engine)
        assert await owner.admit(lock.connection, context, fresh_cache) == 1
    assert cast(MockProvider, context.provider).events.count("read_capacity") == 2


@pytest.mark.description("Discovery keeps each run's configuration for a shared pool.")
def test_discovery_retains_each_runs_configuration_for_a_shared_pool(
    postgres_engine: Engine, postgres_session: Session
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    benchmarks: list[Benchmark] = []
    for name in ("first", "second"):
        _, benchmark, tasks = _run(postgres_session, context.pool_id, [(name, TaskStatus.PENDING, _ATTEMPT)])
        for task in tasks:
            _prepare(postgres_session, context.pool_id, task)
        benchmarks.append(benchmark)

    references = coordinator.read_pool_references(postgres_engine)

    assert [reference.benchmark.id for reference in references] == sorted(benchmark.id for benchmark in benchmarks)
    assert {reference.pool_id for reference in references} == {context.pool_id}


@pytest.mark.description("The scheduler rotates a failed configuration without blocking other runs.")
@pytest.mark.parametrize("failure", ["setup", "capacity", "wrong-pool", "new-reference"])
async def test_scheduler_rotates_failed_configuration_without_blocking_other_runs(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, []).for_queue(
        store.reserved_queue_pool_id(store.queue_pool_id(provider_pool_id))
    )
    runs = sorted(
        [
            _run(postgres_session, context.pool_id, [(name, TaskStatus.PENDING, _ATTEMPT)])
            for name in ("first", "second")
        ],
        key=lambda run: run[1].id,
    )
    bad_org, _, (bad_task,) = runs[0]
    _, _, (good_task,) = runs[1]
    _prepare(postgres_session, context.pool_id, bad_task, admission_ready=False)
    if failure != "new-reference":
        _prepare(postgres_session, context.pool_id, good_task)

    bad_provider = MockProvider(f"daytona:{uuid4()}" if failure == "wrong-pool" else provider_pool_id, [])
    bad_close = AsyncMock()
    monkeypatch.setattr(bad_provider, "close", bad_close)
    failed_capacity = asyncio.Event()

    async def expired_capacity() -> None:
        failed_capacity.set()
        raise SandboxError("expired credentials")

    bad_capacity = AsyncMock(side_effect=expired_capacity)
    monkeypatch.setattr(bad_provider, "get_capacity_domains", bad_capacity)
    attempted_orgs: list[UUID] = []

    def runtime_factory(org_id: UUID, **_kwargs: object) -> Mock:
        attempted_orgs.append(org_id)
        if org_id == bad_org.id and failure == "setup":
            return Mock(get_sandbox_provider_config=AsyncMock(side_effect=SandboxError("invalid configuration")))
        if org_id != bad_org.id and failure != "setup":
            bad_close.assert_awaited_once()
        provider = bad_provider if org_id == bad_org.id else context.provider
        return Mock(
            get_sandbox_provider_config=AsyncMock(return_value=Mock(create_provider=Mock(return_value=provider)))
        )

    def runtime_key(*, properties: object, org: Org) -> UUID:
        return org.id

    monkeypatch.setattr(coordinator, "resolve_run_aws_runtime", runtime_key)
    monkeypatch.setattr(coordinator.CloudRuntimeFactory, "create_runtime", runtime_factory)
    scheduler = coordinator.SharedPoolScheduler(
        postgres_engine, provider_factory=coordinator.open_pool_provider, interval_seconds=0.01, rate_per_minute=0
    )
    scheduler.start()
    try:
        if failure == "new-reference":
            await asyncio.wait_for(failed_capacity.wait(), 10)
            _prepare(postgres_session, context.pool_id, good_task)
        await _until(lambda: _task(postgres_engine, good_task).status == TaskStatus.BUILDING)
        assert _task(postgres_engine, bad_task).status == TaskStatus.PENDING
        assert attempted_orgs == [run[0].id for run in runs]
        if failure == "wrong-pool":
            bad_capacity.assert_not_awaited()
    finally:
        await scheduler.stop()

    async with store.queue_pool_lock(postgres_engine, context.pool_id) as acquired:
        assert acquired


@pytest.mark.description("The scheduler resolves the configuration again when only one run supplies the pool.")
async def test_scheduler_reresolves_configuration_when_only_one_run_supplies_the_pool(
    postgres_engine: Engine, postgres_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    context = _context(postgres_engine, provider_pool_id, []).for_queue(
        store.reserved_queue_pool_id(store.queue_pool_id(provider_pool_id))
    )
    _, _, (task,) = _run(postgres_session, context.pool_id, [("recover", TaskStatus.PENDING, _ATTEMPT)])
    _prepare(postgres_session, context.pool_id, task)
    stale_provider = MockProvider(provider_pool_id, [], snapshot=SandboxError("expired credentials"))
    stale_close = AsyncMock()
    monkeypatch.setattr(stale_provider, "close", stale_close)
    resolve_config = AsyncMock(
        side_effect=[
            Mock(create_provider=Mock(return_value=stale_provider)),
            Mock(create_provider=Mock(return_value=context.provider)),
        ]
    )
    monkeypatch.setattr(coordinator, "resolve_run_aws_runtime", Mock())
    monkeypatch.setattr(
        coordinator.CloudRuntimeFactory,
        "create_runtime",
        Mock(return_value=Mock(get_sandbox_provider_config=resolve_config)),
    )
    scheduler = coordinator.SharedPoolScheduler(
        postgres_engine, provider_factory=coordinator.open_pool_provider, interval_seconds=0.01, rate_per_minute=0
    )
    scheduler.start()
    try:
        await _until(lambda: _task(postgres_engine, task).status == TaskStatus.BUILDING)
        stale_close.assert_awaited_once()
        assert resolve_config.await_count == 2
    finally:
        await scheduler.stop()


@pytest.mark.description("Two coordinators have one owner, and a saved grant survives failover.")
async def test_two_coordinators_have_one_owner_and_durable_grant_survives_failover(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events)
    org, benchmark, (task,) = _run(postgres_session, context.pool_id, [("handoff", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    first = _owner(context, org, benchmark)
    standby_provider = MockProvider(cast(MockProvider, context.provider).admission_pool_id, [])
    standby_context = admission.create_queue_context(
        engine=postgres_engine, provider=cast(SandboxProvider, standby_provider)
    ).for_queue(context.pool_id)
    second = _owner(standby_context, org, benchmark)
    contention = asyncio.Event()
    original_lock = store.queue_pool_lock

    class ObservedLock(advisory_lock.PostgresAdvisoryLock):
        async def acquire(self) -> bool:
            acquired = await super().acquire()
            if not acquired:
                contention.set()
            return acquired

    monkeypatch.setattr(
        coordinator,
        "queue_pool_lock",
        lambda engine, pool: ObservedLock(engine, resource_id=store.queue_pool_lock_id(pool)),
    )
    close_first = AsyncMock()
    monkeypatch.setattr(context.provider, "close", close_first)
    async with AsyncExitStack() as stack:
        waiting = asyncio.create_task(_worker(stack, context, task, authority, events))
        try:
            await _until(lambda: _candidate_count(postgres_engine, context.pool_id) == 1)
            async with _running(first) as leader:
                await _until(lambda: _task(postgres_engine, task).status == TaskStatus.BUILDING)
                async with _running(second):
                    await asyncio.wait_for(contention.wait(), 10)
                    assert standby_provider.events == []
                    assert "create" not in events
                    leader.cancel()
                    await asyncio.gather(leader, return_exceptions=True)
                    close_first.assert_awaited_once()
                    await _until(lambda: "read_capacity" in standby_provider.events)
                    with Session(postgres_engine) as session:
                        grant = session.get(SandboxBuildReservation, (task.id, task.started_at))
                        assert grant is not None and grant.accepted_at is None and grant.released_at is None
                    context.admission_signals.publish([_task(postgres_engine, task)], {})
                    assert await asyncio.wait_for(waiting, 10) is not None
                    assert _task(postgres_engine, task).status == TaskStatus.IN_PROGRESS
                    assert events.count("create") == 1
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
    async with original_lock(postgres_engine, context.pool_id) as acquired:
        assert acquired


@pytest.mark.description("Scheduler shutdown during idle pool retirement does not swallow cancellation.")
async def test_scheduler_shutdown_during_idle_pool_retirement_does_not_swallow_cancellation(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(
        postgres_engine,
        f"daytona:{uuid4()}",
        [],
        snapshot=_CAPACITY.model_copy(update={"cpu": ResourceCapacity(total=0, used=0)}),
    )
    _, _, (task,) = _run(postgres_session, context.pool_id, [("idle", TaskStatus.PENDING, _ATTEMPT)])
    dispatch_id = _prepare(postgres_session, context.pool_id, task)
    closing = asyncio.Event()
    finish_close = asyncio.Event()

    async def close() -> None:
        closing.set()
        await finish_close.wait()

    async def provider_factory(_reference: coordinator.PoolReference) -> SandboxProvider:
        return context.provider

    monkeypatch.setattr(context.provider, "close", close)
    scheduler = coordinator.SharedPoolScheduler(
        postgres_engine,
        provider_factory=provider_factory,
        interval_seconds=0.01,
    )
    scheduler.start()
    try:
        await _until(lambda: "read_capacity" in cast(MockProvider, context.provider).events)
        SchedulerRepository(postgres_session).clear_admission_candidate(task.id, task.started_at, dispatch_id)
        postgres_session.commit()
        await asyncio.wait_for(closing.wait(), 10)
        stopping = asyncio.create_task(scheduler.stop())
        await asyncio.sleep(0)
        finish_close.set()
        await asyncio.wait_for(stopping, 2)
        assert scheduler._task is None
        assert scheduler._pools == {}
    finally:
        finish_close.set()
        await asyncio.wait_for(scheduler.stop(), 2)


@pytest.mark.description("A worker never creates a sandbox from an invalid saved grant.")
@pytest.mark.parametrize("interruption", ["expired", "wrong-dispatch", "stopped", "superseded", "revoked"])
async def test_worker_never_creates_from_invalid_durable_grant(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
    interruption: str,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("invalid", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    async with AsyncExitStack() as stack:
        waiting = asyncio.create_task(_worker(stack, context, task, authority, events))
        try:
            await _until(lambda: _candidate_count(postgres_engine, context.pool_id) == 1)
            assert _grant(postgres_session, context.pool_id) == 1
            with Session(postgres_engine) as session:
                grant = session.get(SandboxBuildReservation, (task.id, task.started_at))
                assert grant is not None
                if interruption == "expired":
                    grant.reserved_at = SchedulerRepository(session).database_now() - store.GRANT_ACCEPTANCE_TIMEOUT
                elif interruption == "wrong-dispatch":
                    grant.executor_dispatch_id = session.exec(
                        select(ExecutorDispatch.id).where(
                            ExecutorDispatch.benchmark_id == benchmark.id,
                            ExecutorDispatch.id != authority.dispatch_id,
                        )
                    ).one()
                session.commit()
            if interruption == "stopped":
                _stop_run(postgres_engine, benchmark)
            elif interruption == "superseded":
                _update_task(postgres_engine, task, TaskStatus.PENDING, task.started_at + timedelta(seconds=1))
            elif interruption == "revoked":
                _revoke_dispatch(postgres_engine, authority)
            context.admission_signals.publish([_task(postgres_engine, task)], {})
            assert await asyncio.wait_for(waiting, 10) is None
            assert "create" not in events
            assert await _build_lock_is_free(postgres_engine, task)
            if interruption == "expired":
                pool_lock = store.queue_pool_lock(postgres_engine, context.pool_id)
                async with pool_lock as acquired:
                    assert acquired
                    with Session(pool_lock.connection) as session:
                        SchedulerRepository(session).expire_unaccepted_grants(context.pool_id)
                        session.commit()
                fresh = _task(postgres_engine, task)
                assert fresh.status == TaskStatus.PENDING
                assert fresh.started_at > task.started_at
                waiting = asyncio.create_task(_worker(stack, context, fresh, authority, events))
                await _until(lambda: _candidate_count(postgres_engine, context.pool_id) == 1)
                assert _grant(postgres_session, context.pool_id) == 1
                context.admission_signals.publish([_task(postgres_engine, fresh)], {})
                assert await asyncio.wait_for(waiting, 10) is not None
                assert events.count("create") == 1
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)


@pytest.mark.description("A late acceptance before the deadline and a duplicate signal create once.")
async def test_late_acceptance_before_deadline_and_duplicate_signal_create_once(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events)
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("late", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    async with AsyncExitStack() as stack:
        waiting = asyncio.create_task(_worker(stack, context, task, authority, events))
        try:
            await _until(lambda: _candidate_count(postgres_engine, context.pool_id) == 1)
            assert _grant(postgres_session, context.pool_id) == 1
            with Session(postgres_engine) as session:
                grant = session.get(SandboxBuildReservation, (task.id, task.started_at))
                assert grant is not None
                grant.reserved_at = SchedulerRepository(session).database_now() - store.GRANT_ACCEPTANCE_TIMEOUT / 2
                session.commit()
            await _recover_once(context)
            context.admission_signals.publish([_task(postgres_engine, task)], {})
            context.admission_signals.publish([_task(postgres_engine, task)], {})
            assert await asyncio.wait_for(waiting, 10) is not None
            with Session(postgres_engine) as session:
                grant = session.get(SandboxBuildReservation, (task.id, task.started_at))
                assert grant is not None and grant.accepted_at is not None and grant.released_at is not None
                assert not SchedulerRepository(session).accept_grant(task.id, task.started_at, authority.dispatch_id)
            assert events.count("create") == 1
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)


@pytest.mark.description("Expiry keeps the exact attempt requeue recoverable.")
@pytest.mark.parametrize("worker_holds_lock", [False, True], ids=["crash-after-commit", "worker-before-acceptance"])
async def test_expiry_keeps_exact_attempt_requeue_recoverable(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
    worker_holds_lock: bool,
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("handoff", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    _prepare(postgres_session, context.pool_id, task, dispatch_id=authority.dispatch_id)
    assert _grant(postgres_session, context.pool_id) == 1
    grant = postgres_session.get(SandboxBuildReservation, (task.id, task.started_at))
    assert grant is not None
    grant.reserved_at = SchedulerRepository(postgres_session).database_now() - store.GRANT_ACCEPTANCE_TIMEOUT
    postgres_session.commit()

    build_lock = advisory_lock.task_build_lock(postgres_engine, task.id)
    try:
        if worker_holds_lock:
            assert await build_lock.acquire()
        pool_lock = store.queue_pool_lock(postgres_engine, context.pool_id)
        async with pool_lock as acquired:
            assert acquired
            with Session(pool_lock.connection) as session:
                SchedulerRepository(session).expire_unaccepted_grants(context.pool_id)
                session.commit()
                if worker_holds_lock:
                    SchedulerRepository(session).reset_abandoned_builds(
                        context.pool_id, SchedulerRepository(session).database_now()
                    )
                    session.commit()

        if worker_holds_lock:
            assert _task(postgres_engine, task).status == TaskStatus.BUILDING
            assert _reservation_count(postgres_engine, context.pool_id) == 1
            assert [reference.pool_id for reference in coordinator.read_pool_references(postgres_engine)] == [
                context.pool_id
            ]
            with Session(build_lock.connection) as session:
                lock_execution_authority(session, authority)
                assert not SchedulerRepository(session).accept_grant(task.id, task.started_at, authority.dispatch_id)
    finally:
        await build_lock.release()

    if worker_holds_lock:
        pool_lock = store.queue_pool_lock(postgres_engine, context.pool_id)
        async with pool_lock as acquired:
            assert acquired
            with Session(pool_lock.connection) as session:
                SchedulerRepository(session).expire_unaccepted_grants(context.pool_id)
                session.commit()

    fresh = _task(postgres_engine, task)
    assert fresh.status == TaskStatus.PENDING
    assert fresh.started_at > task.started_at
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    with Session(postgres_engine) as session:
        retained = session.get(SandboxBuildReservation, (task.id, task.started_at))
        assert retained is not None and retained.released_at is not None and retained.accepted_at is None
        assert not SchedulerRepository(session).accept_grant(task.id, task.started_at, authority.dispatch_id)


@pytest.mark.description("An expiry rollback keeps both the grant and the building attempt.")
async def test_expiry_rollback_preserves_both_grant_and_building_attempt(
    postgres_engine: Engine, postgres_session: Session
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    _, _, (task,) = _run(postgres_session, context.pool_id, [("rollback", TaskStatus.PENDING, _ATTEMPT)])
    _prepare(postgres_session, context.pool_id, task)
    assert _grant(postgres_session, context.pool_id) == 1
    grant = postgres_session.get(SandboxBuildReservation, (task.id, task.started_at))
    assert grant is not None
    grant.reserved_at = SchedulerRepository(postgres_session).database_now() - store.GRANT_ACCEPTANCE_TIMEOUT
    postgres_session.commit()

    pool_lock = store.queue_pool_lock(postgres_engine, context.pool_id)
    async with pool_lock as acquired:
        assert acquired
        with Session(pool_lock.connection) as session:
            SchedulerRepository(session).expire_unaccepted_grants(context.pool_id)
            requeued = session.get(Task, task.id)
            assert requeued is not None and requeued.status == TaskStatus.PENDING
            session.rollback()

    unchanged = _task(postgres_engine, task)
    assert unchanged.status == TaskStatus.BUILDING
    assert unchanged.started_at == task.started_at
    assert _reservation_count(postgres_engine, context.pool_id) == 1
    assert [reference.pool_id for reference in coordinator.read_pool_references(postgres_engine)] == [context.pool_id]


@pytest.mark.description("Expiry keeps accepted builds and superseded attempts.")
@pytest.mark.parametrize("current_state", ["accepted", "superseded"])
async def test_expiry_preserves_accepted_builds_and_superseded_attempts(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
    current_state: str,
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("preserve", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    _prepare(postgres_session, context.pool_id, task, dispatch_id=authority.dispatch_id)
    assert _grant(postgres_session, context.pool_id) == 1
    if current_state == "accepted":
        build_lock = advisory_lock.task_build_lock(postgres_engine, task.id)
        async with build_lock as acquired:
            assert acquired
            with Session(build_lock.connection) as session:
                lock_execution_authority(session, authority)
                assert SchedulerRepository(session).accept_grant(task.id, task.started_at, authority.dispatch_id)
                session.commit()
        current_attempt = task.started_at
    else:
        current_attempt = task.started_at + timedelta(seconds=1)
        _update_task(postgres_engine, task, TaskStatus.BUILDING, current_attempt)
    grant = postgres_session.get(SandboxBuildReservation, (task.id, task.started_at))
    assert grant is not None
    grant.reserved_at = SchedulerRepository(postgres_session).database_now() - store.GRANT_ACCEPTANCE_TIMEOUT
    postgres_session.commit()

    pool_lock = store.queue_pool_lock(postgres_engine, context.pool_id)
    async with pool_lock as acquired:
        assert acquired
        with Session(pool_lock.connection) as session:
            SchedulerRepository(session).expire_unaccepted_grants(context.pool_id)
            session.commit()

    unchanged = _task(postgres_engine, task)
    assert unchanged.status == TaskStatus.BUILDING
    assert unchanged.started_at == current_attempt
    assert _reservation_count(postgres_engine, context.pool_id) == (1 if current_state == "accepted" else 0)


@pytest.mark.description("Unaccepted grants release without a provider lookup, and expiry advances the attempt.")
@pytest.mark.parametrize("interruption", ["expired", "revoked", "stopped"])
async def test_unaccepted_grants_release_without_provider_lookup_and_expiry_advances_attempt(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
    monkeypatch: pytest.MonkeyPatch,
    interruption: str,
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("expire", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    _prepare(postgres_session, context.pool_id, task, dispatch_id=authority.dispatch_id)
    assert _grant(postgres_session, context.pool_id) == 1
    lookup = AsyncMock(side_effect=AssertionError("unaccepted grants cannot own a provider sandbox"))
    monkeypatch.setattr(context.provider, "get_sandbox", lookup)
    if interruption == "expired":
        with Session(postgres_engine) as session:
            grant = session.get(SandboxBuildReservation, (task.id, task.started_at))
            assert grant is not None
            grant.reserved_at = SchedulerRepository(session).database_now() - store.GRANT_ACCEPTANCE_TIMEOUT
            session.commit()
    elif interruption == "revoked":
        _revoke_dispatch(postgres_engine, authority)
    else:
        _stop_run(postgres_engine, benchmark)
    await _recover_once(context)
    lookup.assert_not_awaited()
    assert _reservation_count(postgres_engine, context.pool_id) == 0
    if interruption != "stopped":
        fresh = _task(postgres_engine, task)
        assert fresh.status == TaskStatus.PENDING
        assert fresh.started_at > task.started_at
        with Session(postgres_engine) as session:
            assert not SchedulerRepository(session).accept_grant(task.id, task.started_at, authority.dispatch_id)


@pytest.mark.description("Slow cleanup of an accepted orphan does not block new grants.")
async def test_slow_accepted_orphan_cleanup_does_not_block_new_grants(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    org, benchmark, (orphan, waiting) = _run(
        postgres_session,
        context.pool_id,
        [("orphan", TaskStatus.PENDING, _ATTEMPT), ("waiting", TaskStatus.PENDING, _ATTEMPT + timedelta(seconds=1))],
        concurrency=2,
    )
    _reserve(postgres_session, context.pool_id, orphan)
    _prepare(postgres_session, context.pool_id, waiting)
    cleanup_started = asyncio.Event()
    finish_cleanup = asyncio.Event()

    async def lookup(name: str) -> Sandbox:
        assert name == store.queued_sandbox_name(orphan.id, orphan.started_at)
        cleanup_started.set()
        await finish_cleanup.wait()
        sandbox = Mock(id="orphan-sandbox", labels={})
        sandbox.name = name
        return cast(Sandbox, sandbox)

    monkeypatch.setattr(context.provider, "get_sandbox", lookup)
    monkeypatch.setattr(coordinator, "_RECOVERY_SECONDS", 0.01)
    delete = AsyncMock()
    monkeypatch.setattr(context.provider, "delete_sandbox", delete)
    async with _running(_owner(context, org, benchmark)):
        try:
            await asyncio.wait_for(cleanup_started.wait(), 10)
            await _until(lambda: _task(postgres_engine, waiting).status == TaskStatus.BUILDING)
            assert _reservation_count(postgres_engine, context.pool_id) == 2
            assert not await _build_lock_is_free(postgres_engine, orphan)
            delete.assert_not_awaited()
            finish_cleanup.set()
            await _until(lambda: _reservation_count(postgres_engine, context.pool_id) == 1)
            delete.assert_awaited_once_with("orphan-sandbox")
        finally:
            finish_cleanup.set()


@pytest.mark.description("Unreadable capacity defers grants until the provider recovers.")
async def test_unreadable_capacity_defers_grants_until_provider_recovers(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [], snapshot=SandboxError("unavailable"))
    org, benchmark, (task,) = _run(postgres_session, context.pool_id, [("waiting", TaskStatus.PENDING, _ATTEMPT)])
    _prepare(postgres_session, context.pool_id, task)
    owner = _owner(context, org, benchmark)
    cache = coordinator.CapacityCache(context.provider, postgres_engine)
    lock = store.queue_pool_lock(postgres_engine, context.pool_id)
    async with lock as acquired:
        assert acquired
        assert await owner.admit(lock.connection, context, cache) == 0
        assert _candidate_count(postgres_engine, context.pool_id) == 1
        assert _task(postgres_engine, task).status == TaskStatus.PENDING
        cast(MockProvider, context.provider).snapshot = _CAPACITY
        assert await owner.admit(lock.connection, context, cache) == 1
    assert _reservation_count(postgres_engine, context.pool_id) == 1


@pytest.mark.description(
    "A large prepared backlog grants only up to the batch limit and leaves the rest as candidates."
)
@pytest.mark.parametrize("backlog", [400, 1000])
@pytest.mark.parametrize("grant_limit", [3, 100])
async def test_large_prepared_backlog_is_packed_once_for_a_bounded_batch(
    postgres_engine: Engine,
    postgres_session: Session,
    backlog: int,
    grant_limit: int,
    executed_sql: list[str],
) -> None:
    capacity = SandboxCapacity(
        cpu=ResourceCapacity(total=10000, used=0),
        memory=ResourceCapacity(total=10000, used=0),
        disk=ResourceCapacity(total=10000, used=0),
    )
    context = _context(postgres_engine, f"daytona:{uuid4()}", [], snapshot=capacity)
    org, benchmark, tasks = _run(
        postgres_session,
        context.pool_id,
        [(str(i), TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=i)) for i in range(backlog)],
        concurrency=backlog,
    )
    for task in tasks:
        _prepare(postgres_session, context.pool_id, task)
    owner = _owner(context, org, benchmark, rate=grant_limit)
    cache = coordinator.CapacityCache(context.provider, postgres_engine)
    lock = store.queue_pool_lock(postgres_engine, context.pool_id)
    async with lock as acquired:
        assert acquired
        executed_sql.clear()
        assert await owner.admit(lock.connection, context, cache) == grant_limit
        assert 0 < len(executed_sql) < 30
        for statement in (
            "UPDATE task ",
            "INSERT INTO sandboxbuildreservation ",
            "DELETE FROM sandboxadmissioncandidate ",
        ):
            assert sum(sql.startswith(statement) for sql in executed_sql) == 1
    with Session(postgres_engine) as session:
        building = session.exec(select(Task.id).where(Task.status == TaskStatus.BUILDING)).all()
    assert set(building) == {task.id for task in tasks[:grant_limit]}
    assert _candidate_count(postgres_engine, context.pool_id) == backlog - grant_limit
    assert _reservation_count(postgres_engine, context.pool_id) == grant_limit


@pytest.mark.description("Packing and capacity reads scale with pools, not waiting executors.")
@pytest.mark.parametrize(("pools", "executors"), [(1, 1), (1, 12), (2, 12)])
async def test_global_packing_and_capacity_reads_scale_with_pools_not_waiting_executors(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    pools: int,
    executors: int,
    executed_sql: list[str],
) -> None:
    contexts = []
    owners = []
    for _ in range(pools):
        context = _context(
            postgres_engine,
            f"daytona:{uuid4()}",
            [],
            snapshot=_CAPACITY.model_copy(update={"cpu": ResourceCapacity(total=0, used=0)}),
        )
        runs = []
        for index in range(executors):
            org, benchmark, (task,) = _run(
                postgres_session, context.pool_id, [(str(index), TaskStatus.PENDING, _ATTEMPT)]
            )
            _prepare(postgres_session, context.pool_id, task)
            runs.append((org, benchmark))
        contexts.append(context)
        owners.append(_owner(context, runs[0][0], runs[0][1]))
    references = coordinator.read_pool_references(postgres_engine)
    assert {reference.pool_id for reference in references} == {context.pool_id for context in contexts}
    scans = 0
    original_pack = SchedulerRepository.packed_admission_candidates

    def pack(*args: Any, **kwargs: Any) -> Any:
        nonlocal scans
        scans += 1
        return original_pack(*args, **kwargs)

    monkeypatch.setattr(SchedulerRepository, "packed_admission_candidates", pack)
    for context, owner in zip(contexts, owners, strict=True):
        cache = coordinator.CapacityCache(context.provider, postgres_engine)
        lock = store.queue_pool_lock(postgres_engine, context.pool_id)
        async with lock as acquired:
            assert acquired
            for _ in range(3):
                executed_sql.clear()
                assert await owner.admit(lock.connection, context, cache) == 0
                assert 0 < len(executed_sql) <= 8
        assert _candidate_count(postgres_engine, context.pool_id) == executors
    assert scans == 3 * pools
    assert sum(cast(MockProvider, context.provider).events.count("read_capacity") for context in contexts) == pools


@pytest.mark.description("The run state refresh delivers coordinator readiness requests.")
async def test_existing_run_state_refresh_delivers_coordinator_readiness_requests(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    context = _context(postgres_engine, f"daytona:{uuid4()}", events, capacity=(False, True))
    org, benchmark, (task,) = _run(postgres_session, context.pool_id, [("probe", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)

    async def process_task(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, float]]:
        async with AsyncExitStack() as stack:
            assert await _worker(stack, context, task_row, authority, events) is not None
            _update_task(postgres_engine, task_row, TaskStatus.FINISHED, task_row.started_at)
        return {task_row.task_id: {"score": 1.0}}

    monkeypatch.setattr(run_orchestration, "engine", postgres_engine)
    monkeypatch.setattr("tracker.services.runs.finalize.engine", postgres_engine)
    monkeypatch.setattr(task_execution, "engine", postgres_engine)
    monkeypatch.setattr("tracker.services.tasks.monitor.engine", postgres_engine)
    monkeypatch.setattr(task_monitor_module.TaskMonitor, "_TRACK_INTERVAL", 0.01)
    monkeypatch.setattr(run_orchestration, "process_task", process_task)
    runner = asyncio.create_task(
        run_orchestration._run_queued_tasks(
            benchmark_id=benchmark.id,
            task_rows=[(task.task_id, task)],
            start_benchmark_request=benchmark.managed_start_benchmark_request(),
            benchmark_service=Mock(spec=BenchmarkServiceClient),
            runtime=Mock(spec=RuntimeServices),
            org=org,
            sandbox_provider_config=Mock(spec=SandboxProviderConfig),
            sandbox_provider=context.provider,
            creation_semaphore=asyncio.Semaphore(1),
            queue_context=context,
            started_by_email=None,
            record_cancellation=lambda _attempts: None,
            authority=authority,
        )
    )
    try:
        await _until(lambda: _candidate_count(postgres_engine, context.pool_id) == 1)
        assert not _candidate(postgres_engine, task).admission_ready
        assert events.count("capacity") == 1
        async with _running(_owner(context, org, benchmark)):
            await asyncio.wait_for(runner, 10)
        assert events.count("capacity") == 2
        assert events.count("create") == 1
        assert _task(postgres_engine, task).status == TaskStatus.FINISHED
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.description("Readiness requests go out only up to one shared outstanding budget per pool.")
@pytest.mark.parametrize("pools", [1, 2])
def test_readiness_requests_share_one_outstanding_budget_per_pool(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    pools: int,
) -> None:
    now = SchedulerRepository(postgres_session).database_now()
    monkeypatch.setattr(SchedulerRepository, "database_now", lambda _session: now)
    pool_tasks = {}
    for _ in range(pools):
        pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
        tasks = []
        for index in range(12):
            now += timedelta(microseconds=1)
            _, _, (task,) = _run(postgres_session, pool_id, [(str(index), TaskStatus.PENDING, _ATTEMPT)])
            _prepare(postgres_session, pool_id, task, admission_ready=False)
            tasks.append(task)
        pool_tasks[pool_id] = tasks
        assert (
            SchedulerRepository(postgres_session).request_readiness_checks(
                pool_id, limit=10, retry_delay=timedelta(seconds=1)
            )
            == 0
        )
    now += timedelta(seconds=1)
    for pool_id, tasks in pool_tasks.items():
        assert (
            SchedulerRepository(postgres_session).request_readiness_checks(
                pool_id, limit=10, retry_delay=timedelta(seconds=1)
            )
            == 10
        )
        postgres_session.commit()
        assert [task.id for task in tasks if _candidate(postgres_engine, task).readiness_requested_at is not None] == [
            task.id for task in tasks[:10]
        ]
        assert (
            SchedulerRepository(postgres_session).request_readiness_checks(
                pool_id, limit=10, retry_delay=timedelta(seconds=1)
            )
            == 0
        )
        _prepare(postgres_session, pool_id, tasks[0], admission_ready=True)
        assert (
            SchedulerRepository(postgres_session).request_readiness_checks(
                pool_id, limit=10, retry_delay=timedelta(seconds=1)
            )
            == 1
        )
        postgres_session.commit()
        assert _candidate(postgres_engine, tasks[0]).readiness_requested_at is None
        assert _candidate(postgres_engine, tasks[10]).readiness_requested_at is not None
        assert _candidate(postgres_engine, tasks[11]).readiness_requested_at is None


@pytest.mark.description("A readiness probe needs the exact live request and owner.")
@pytest.mark.parametrize(
    "interruption",
    ["wrong-attempt", "wrong-dispatch", "wrong-request", "expired", "superseded", "revoked", "stopped", "ready"],
)
def test_readiness_probe_requires_exact_live_request_and_owner(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    interruption: str,
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("probe", TaskStatus.PENDING, _ATTEMPT)])
    now = SchedulerRepository(postgres_session).database_now()
    monkeypatch.setattr(SchedulerRepository, "database_now", lambda _session: now)
    dispatch_id = _prepare(postgres_session, pool_id, task, admission_ready=False)
    assert (
        SchedulerRepository(postgres_session).request_readiness_checks(pool_id, limit=1, retry_delay=timedelta(0)) == 1
    )
    postgres_session.commit()
    requested_at = _candidate(postgres_engine, task).readiness_requested_at
    assert requested_at is not None
    attempt = task.started_at
    if interruption == "wrong-attempt":
        attempt += timedelta(seconds=1)
    elif interruption == "wrong-dispatch":
        dispatch_id = uuid4()
    elif interruption == "wrong-request":
        requested_at += timedelta(microseconds=1)
    elif interruption == "expired":
        now += store.READINESS_CHECK_TIMEOUT
    elif interruption == "superseded":
        _update_task(postgres_engine, task, TaskStatus.PENDING, task.started_at + timedelta(seconds=1))
    elif interruption == "revoked":
        _revoke_dispatch(postgres_engine, ExecutionAuthority(benchmark.id, dispatch_id))
    elif interruption == "stopped":
        _stop_run(postgres_engine, benchmark)
    elif interruption == "ready":
        _prepare(postgres_session, pool_id, task, admission_ready=True)

    assert not SchedulerRepository(postgres_session).begin_readiness_check(task.id, attempt, dispatch_id, requested_at)
    postgres_session.commit()
    assert _reservation_count(postgres_engine, pool_id) == 0


@pytest.mark.description("An expired readiness request can be reissued, and its old nonce cannot be replayed.")
def test_expired_readiness_request_can_be_reissued_and_old_nonce_cannot_be_replayed(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_id = store.reserved_queue_pool_id(f"daytona:{uuid4()}")
    _, benchmark, (task,) = _run(postgres_session, pool_id, [("probe", TaskStatus.PENDING, _ATTEMPT)])
    now = SchedulerRepository(postgres_session).database_now()
    monkeypatch.setattr(SchedulerRepository, "database_now", lambda _session: now)
    dispatch_id = _prepare(postgres_session, pool_id, task, admission_ready=False)
    assert (
        SchedulerRepository(postgres_session).request_readiness_checks(pool_id, limit=1, retry_delay=timedelta(0)) == 1
    )
    postgres_session.commit()
    old_request = _candidate(postgres_engine, task).readiness_requested_at
    assert old_request is not None

    now += store.READINESS_CHECK_TIMEOUT
    assert not SchedulerRepository(postgres_session).begin_readiness_check(
        task.id, task.started_at, dispatch_id, old_request
    )
    assert (
        SchedulerRepository(postgres_session).request_readiness_checks(pool_id, limit=1, retry_delay=timedelta(0)) == 1
    )
    postgres_session.commit()
    replacement = _candidate(postgres_engine, task).readiness_requested_at
    assert replacement is not None and replacement > old_request
    with Session(postgres_engine) as session:
        lock_execution_authority(session, ExecutionAuthority(benchmark.id, dispatch_id))
        assert not SchedulerRepository(session).begin_readiness_check(
            task.id, task.started_at, dispatch_id, old_request
        )
        assert SchedulerRepository(session).begin_readiness_check(task.id, task.started_at, dispatch_id, replacement)
        assert not SchedulerRepository(session).begin_readiness_check(
            task.id, task.started_at, dispatch_id, replacement
        )
        session.commit()
    renewed = _candidate(postgres_engine, task).readiness_requested_at
    assert renewed is not None and renewed > replacement


@pytest.mark.description("A worker renews its probe before the SDK call and waits for a new request after rejection.")
async def test_worker_renews_probe_before_sdk_call_and_waits_for_new_request_after_rejection(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    checking = asyncio.Event()
    finish_check = asyncio.Event()

    async def on_check() -> None:
        if events.count("capacity") == 2:
            checking.set()
            await finish_check.wait()

    context = _context(
        postgres_engine,
        f"daytona:{uuid4()}",
        events,
        capacity=(False, False, True),
        on_check=on_check,
    )
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("probe", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    now = SchedulerRepository(postgres_session).database_now()
    monkeypatch.setattr(SchedulerRepository, "database_now", lambda _session: now)
    async with AsyncExitStack() as stack:
        waiting = asyncio.create_task(_worker(stack, context, task, authority, events))
        try:
            await _until(lambda: _candidate_count(postgres_engine, context.pool_id) == 1)
            assert not _candidate(postgres_engine, task).admission_ready
            assert (
                SchedulerRepository(postgres_session).request_readiness_checks(
                    context.pool_id, limit=1, retry_delay=timedelta(0)
                )
                == 1
            )
            postgres_session.commit()
            request = _candidate(postgres_engine, task).readiness_requested_at
            assert request is not None
            now += timedelta(seconds=1)
            _publish_run_state(context, authority)
            await asyncio.wait_for(checking.wait(), 10)
            renewed = _candidate(postgres_engine, task).readiness_requested_at
            assert renewed is not None and renewed > request
            assert not SchedulerRepository(postgres_session).begin_readiness_check(
                task.id,
                task.started_at,
                authority.dispatch_id,
                request,
            )
            postgres_session.commit()
            assert (
                SchedulerRepository(postgres_session).request_readiness_checks(
                    context.pool_id, limit=1, retry_delay=timedelta(0)
                )
                == 0
            )
            for _ in range(3):
                _publish_run_state(context, authority)
                await asyncio.sleep(0)
            assert events.count("capacity") == 2
            assert _reservation_count(postgres_engine, context.pool_id) == 0
            finish_check.set()
            await _until(lambda: _candidate(postgres_engine, task).readiness_requested_at is None)
            candidate = _candidate(postgres_engine, task)
            assert not candidate.admission_ready
            assert candidate.readiness_checked_at > request
            for _ in range(3):
                context.admission_signals.publish([_task(postgres_engine, task)], {task.id: request})
                await asyncio.sleep(0)
            assert events.count("capacity") == 2
            assert _task(postgres_engine, task).started_at == task.started_at

            now += timedelta(seconds=1)
            assert (
                SchedulerRepository(postgres_session).request_readiness_checks(
                    context.pool_id, limit=1, retry_delay=timedelta(0)
                )
                == 1
            )
            postgres_session.commit()
            _publish_run_state(context, authority)
            await _until(lambda: _candidate(postgres_engine, task).admission_ready)
            assert events.count("capacity") == 3
            assert _grant(postgres_session, context.pool_id) == 1
            _publish_run_state(context, authority)
            assert await asyncio.wait_for(waiting, 10) is not None
            assert events.count("create") == 1
        finally:
            finish_check.set()
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)


@pytest.mark.description("A ready worker parks without capacity reads, and cancellation clears its candidate.")
async def test_ready_worker_parks_without_capacity_reads_and_cancellation_clears_candidate(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(postgres_engine, f"daytona:{uuid4()}", [])
    _, benchmark, (task,) = _run(postgres_session, context.pool_id, [("parked", TaskStatus.PENDING, _ATTEMPT)])
    authority = executor_authority(benchmark, session=postgres_session)
    capacity = AsyncMock(side_effect=AssertionError("workers must never read global capacity"))
    monkeypatch.setattr(context.provider, "get_capacity_domains", capacity)
    async with AsyncExitStack() as stack:
        waiting = asyncio.create_task(_worker(stack, context, task, authority, []))
        try:
            await _until(lambda: _candidate_count(postgres_engine, context.pool_id) == 1)
            context.admission_signals.publish([_task(postgres_engine, task)], {})
            assert not waiting.done()
            assert await _build_lock_is_free(postgres_engine, task)
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
    capacity.assert_not_awaited()
    assert _candidate_count(postgres_engine, context.pool_id) == 0
    assert _reservation_count(postgres_engine, context.pool_id) == 0
