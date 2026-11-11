import threading
import time
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tracker.database.models import ExecutorDispatch, ExecutorDispatchKind, ExecutorDispatchStatus, ExecutorRelease, Org
from tracker.database.session import get_session
from tracker.executor import dispatch_recovery
from tracker.executor.control_app import app as control_app
from tracker.executor.control_store import invocation_digest
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.repositories.scheduler import SchedulerRepository
from tracker.scheduler import coordinator
from tracker.executor.release_control import (
    create_executor_dispatch,
    get_executor_admission,
    share_executor_admission,
    pin_benchmark_to_release,
    register_release,
)
from tracker.types import AgentContractRequest


@pytest.fixture
def control_client(postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    def server_session() -> Generator[Session, None, None]:
        with Session(postgres_engine) as session:
            yield session

    monkeypatch.setitem(control_app.dependency_overrides, get_session, server_session)
    with TestClient(control_app) as client:
        yield client


def _running_dispatches(session: Session, count: int) -> list[tuple[str, str, str]]:
    org = Org(id=uuid4(), name=f"admission-lock-{uuid4()}")
    release = ExecutorRelease(
        id=f"admission-lock-{uuid4()}",
        artifact_uri="source:///admission-lock",
        artifact_digest="a" * 64,
        protocol_version="1",
    )
    session.add(org)
    session.flush()
    register_release(session, release)
    refs = []
    for index in range(count):
        benchmark = make_benchmark(
            name=f"admission-lock-{index}",
            org_id=org.id,
            contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
        )
        pin_benchmark_to_release(benchmark, release)
        dispatch = create_executor_dispatch(
            benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=["task-1"]
        )
        dispatch.status = ExecutorDispatchStatus.RUNNING
        dispatch.ecs_task_arn = f"arn:aws:ecs:us-east-1:123456789012:task/cluster/{index}"
        dispatch.invocation_key_digest = invocation_digest(f"key-{index}")
        session.add_all([benchmark, dispatch, make_task(benchmark, "task-1")])
        refs.append((str(benchmark.id), str(dispatch.id), f"key-{index}"))
    session.commit()
    return refs


def _call(client: TestClient, ref: tuple[str, str, str], operation: str) -> int:
    benchmark_id, dispatch_id, key = ref
    body: dict[str, object] = {"ref": {"benchmark_id": benchmark_id, "dispatch_id": dispatch_id}}
    if operation == "read_run_state":
        body |= {"tracked_task_ids": [], "create_missing_tasks": False}
    else:
        body |= {"task_ids": ["task-1"]}
    response = client.post(
        f"/executor-control/v1/{operation}",
        headers={"X-Executor-Dispatch-ID": dispatch_id, "Authorization": f"Bearer {key}"},
        json=body,
    )
    return response.status_code


@pytest.mark.description(
    "Control requests from different runs proceed while another control request holds the admission fence."
)
def test_ordinary_control_requests_do_not_serialize_on_the_admission_row(
    postgres_engine: Engine, postgres_session: Session, control_client: TestClient
) -> None:
    refs = _running_dispatches(postgres_session, 4)
    with Session(postgres_engine) as in_flight, ThreadPoolExecutor(max_workers=8) as pool:
        share_executor_admission(in_flight)
        calls = [
            pool.submit(_call, control_client, ref, operation)
            for ref in refs
            for operation in ("read_run_state", "monitor_tasks")
        ]
        try:
            assert [call.result(timeout=10) for call in calls] == [200] * len(calls)
        finally:
            in_flight.rollback()


@pytest.mark.description("A maintenance writer holding the admission row blocks control requests until it commits.")
def test_maintenance_writer_still_fences_control_requests(
    postgres_engine: Engine,
    postgres_session: Session,
    control_client: TestClient,
    wait_for_database_lock: Callable[[], None],
) -> None:
    (ref,) = _running_dispatches(postgres_session, 1)
    with Session(postgres_engine) as maintenance, ThreadPoolExecutor(max_workers=1) as pool:
        get_executor_admission(maintenance, for_update=True)
        call = pool.submit(_call, control_client, ref, "read_run_state")
        try:
            wait_for_database_lock()
            assert not call.done()
        finally:
            maintenance.commit()
        assert call.result(timeout=10) == 200


@pytest.mark.description("An in-flight control request blocks a maintenance writer until the request finishes.")
def test_in_flight_control_request_blocks_maintenance_writer(
    postgres_engine: Engine, wait_for_database_lock: Callable[[], None]
) -> None:
    with Session(postgres_engine) as in_flight, ThreadPoolExecutor(max_workers=1) as pool:
        share_executor_admission(in_flight)

        def write() -> None:
            with Session(postgres_engine) as maintenance:
                get_executor_admission(maintenance, for_update=True)
                maintenance.commit()

        writer = pool.submit(write)
        try:
            wait_for_database_lock()
            assert not writer.done()
        finally:
            in_flight.commit()
        writer.result(timeout=10)


@pytest.mark.description(
    "Read-only control polls return while another transaction holds the run and dispatch rows, and task creation waits."
)
def test_read_only_control_polls_do_not_wait_on_run_row_locks(
    postgres_engine: Engine,
    postgres_session: Session,
    control_client: TestClient,
    wait_for_database_lock: Callable[[], None],
) -> None:
    (ref,) = _running_dispatches(postgres_session, 1)
    benchmark_id, dispatch_id, key = ref
    with Session(postgres_engine) as writer, ThreadPoolExecutor(max_workers=3) as pool:
        BenchmarkRepository(writer).lock_by_id(UUID(benchmark_id))
        ExecutorDispatchRepository(writer).lock(UUID(dispatch_id))
        reads = [
            pool.submit(_call, control_client, ref, operation) for operation in ("read_run_state", "monitor_tasks")
        ]
        create = pool.submit(
            control_client.post,
            "/executor-control/v1/read_run_state",
            headers={"X-Executor-Dispatch-ID": dispatch_id, "Authorization": f"Bearer {key}"},
            json={
                "ref": {"benchmark_id": benchmark_id, "dispatch_id": dispatch_id},
                "tracked_task_ids": ["task-2"],
                "create_missing_tasks": True,
            },
        )
        try:
            assert [read.result(timeout=10) for read in reads] == [200, 200]
            wait_for_database_lock()
            assert not create.done()
        finally:
            writer.commit()
        assert create.result(timeout=10).status_code == 200


def _wait_for_lock_waiters(engine: Engine, count: int) -> None:
    deadline = time.monotonic() + 5
    with engine.connect() as connection:
        while time.monotonic() < deadline:
            waiting = connection.execute(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
            ).scalar_one()
            connection.rollback()
            if waiting >= count:
                return
            time.sleep(0.02)
    raise AssertionError(f"expected {count} sessions waiting on a lock")


@pytest.mark.description(
    "A control request that arrives while maintenance waits for the admission fence queues behind maintenance."
)
def test_waiting_admission_writer_is_not_overtaken_by_new_control_requests(
    postgres_engine: Engine, postgres_session: Session
) -> None:
    _running_dispatches(postgres_session, 1)

    def hold(session: Session, take: Callable[[Session], object]) -> None:
        take(session)
        session.commit()

    with (
        Session(postgres_engine) as in_flight,
        Session(postgres_engine) as writer,
        Session(postgres_engine) as late,
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        share_executor_admission(in_flight)
        writing = pool.submit(hold, writer, lambda session: get_executor_admission(session, for_update=True))
        _wait_for_lock_waiters(postgres_engine, 1)
        late_read = pool.submit(hold, late, share_executor_admission)
        try:
            _wait_for_lock_waiters(postgres_engine, 2)
            assert not writing.done() and not late_read.done()
        finally:
            in_flight.rollback()
        writing.result(timeout=10)
        late_read.result(timeout=10)


@pytest.mark.description(
    "Dispatch recovery holds no admission lock while it waits on ECS `DescribeTasks` or `RunTask`."
)
def test_dispatch_recovery_releases_admission_before_calling_ecs(
    postgres_engine: Engine, postgres_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    [(_, dispatch_id, _)] = _running_dispatches(postgres_session, 1)
    dispatch = postgres_session.get(ExecutorDispatch, UUID(dispatch_id))
    assert dispatch is not None and dispatch.ecs_task_arn is not None
    dispatch.status = ExecutorDispatchStatus.QUEUED
    stopped_task_arn = dispatch.ecs_task_arn
    postgres_session.commit()
    calls: list[str] = []

    def maintenance_can_close_admission(call: str) -> None:
        with Session(postgres_engine) as maintenance:
            maintenance.execute(text("SET LOCAL lock_timeout = '1s'"))
            get_executor_admission(maintenance, for_update=True)
            maintenance.rollback()
        calls.append(call)

    def describe(task_arns: list[str]) -> dict[str, list[dict[str, str]]]:
        maintenance_can_close_admission("describe")
        return {
            "tasks": [{"taskArn": arn, "lastStatus": "STOPPED", "stopCode": "TaskFailedToStart"} for arn in task_arns],
            "failures": [],
        }

    def relaunch(_dispatch: ExecutorDispatch) -> str:
        maintenance_can_close_admission("relaunch")
        return f"{stopped_task_arn}-replacement"

    monkeypatch.setattr(dispatch_recovery, "describe_executor_tasks", describe)
    monkeypatch.setattr(dispatch_recovery, "relaunch_dispatch", relaunch)
    with Session(postgres_engine) as session:
        share_executor_admission(session)
        assert dispatch_recovery._reconcile_stopped_tasks(session) == 1

    assert calls == ["describe", "relaunch"]


@pytest.mark.description(
    "Control requests proceed while a scheduler cycle holds the admission fence, and maintenance still waits for it."
)
def test_scheduler_cycle_shares_the_admission_fence_with_control(
    postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = threading.Event()
    finish = threading.Event()

    def held_cycle(_repository: SchedulerRepository, _pool_id: str) -> dict[UUID, str]:
        entered.set()
        assert finish.wait(10)
        return {}

    monkeypatch.setattr(SchedulerRepository, "pending_owned_ecs_task_arns", held_cycle)
    context = SimpleNamespace(engine=postgres_engine, pool_id="admission-pool")

    def take_for_maintenance(session: Session) -> None:
        get_executor_admission(session, for_update=True)
        session.rollback()

    with (
        Session(postgres_engine) as control,
        Session(postgres_engine) as maintenance,
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        cycle = pool.submit(coordinator._pending_recovery_tasks, context)
        try:
            assert entered.wait(10)
            control.execute(text("SET LOCAL lock_timeout = '2s'"))
            share_executor_admission(control)
            control.rollback()
            writing = pool.submit(take_for_maintenance, maintenance)
            _wait_for_lock_waiters(postgres_engine, 1)
            assert not writing.done()
        finally:
            finish.set()
        assert cycle.result(timeout=10) == {}
        writing.result(timeout=10)
