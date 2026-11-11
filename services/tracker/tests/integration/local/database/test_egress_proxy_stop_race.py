from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event, current_thread
from uuid import uuid4

import pytest
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import Benchmark, EgressProxyPolicy, Org, Task, TaskStatus
from tracker.egress_proxy.policy import AttemptStoppedError, PolicyStore, resolve_run_egress_proxy
from tracker.services.runs.stop import apply_stop_benchmark
from tracker.services.tasks.execute import task_attempt_id


def _run(postgres_session: Session) -> tuple[Org, Task, str]:
    org = Org(id=uuid4(), name=f"egress-stop-race-{uuid4()}")
    postgres_session.add(org)
    postgres_session.commit()
    benchmark = make_benchmark(org_id=org.id, session=postgres_session)
    task = Task(org_id=org.id, task_id="task-1", benchmark=benchmark.id, status=TaskStatus.IN_PROGRESS)
    postgres_session.add(task)
    postgres_session.commit()
    return org, task, task_attempt_id(task.id, task.started_at)


@pytest.mark.description("When the stop commits first, the egress grant refuses the policy.")
def test_stop_commits_before_grant_refuses_policy(
    postgres_engine: Engine, postgres_session: Session, wait_for_database_lock: Callable[[], None]
) -> None:
    org, task, key_id = _run(postgres_session)
    proxy = resolve_run_egress_proxy("*", key_id)
    assert proxy is not None
    expires_at = datetime.now(UTC) + timedelta(minutes=1)

    with Session(postgres_engine) as stop_session:
        benchmark = stop_session.get(Benchmark, task.benchmark)
        assert benchmark is not None
        apply_stop_benchmark(benchmark, stop_session, force=True, org=org)
        with ThreadPoolExecutor(max_workers=1) as pool:
            grant = pool.submit(PolicyStore(postgres_engine).grant, proxy, task.benchmark, task.id, expires_at)
            try:
                wait_for_database_lock()
            finally:
                stop_session.commit()
            with pytest.raises(AttemptStoppedError):
                grant.result(timeout=5)

    with Session(postgres_engine) as observer:
        assert observer.get(EgressProxyPolicy, key_id) is None


@pytest.mark.description("When the egress grant commits first, the stop revokes the policy.")
def test_grant_commits_before_stop_revokes_policy(
    postgres_engine: Engine, postgres_session: Session, wait_for_database_lock: Callable[[], None]
) -> None:
    org, task, key_id = _run(postgres_session)
    proxy = resolve_run_egress_proxy("*", key_id)
    assert proxy is not None
    expires_at = datetime.now(UTC) + timedelta(minutes=1)
    grant_locked = Event()
    release_grant = Event()
    observer_started = Event()

    def coordinate_grant_and_observer(
        _connection: object, _cursor: object, statement: str, _parameters: object, _context: object, _many: bool
    ) -> None:
        if (
            current_thread().name.startswith("policy-grant")
            and "FOR UPDATE" in statement.upper()
            and "task" in statement
        ):
            grant_locked.set()
            assert release_grant.wait(5)
        if "FROM pg_stat_activity" in statement:
            observer_started.set()

    def stop() -> None:
        assert observer_started.wait(5)
        with Session(postgres_engine) as stop_session:
            benchmark = stop_session.get(Benchmark, task.benchmark)
            assert benchmark is not None
            apply_stop_benchmark(benchmark, stop_session, force=True, org=org)
            stop_session.commit()

    event.listen(postgres_engine, "after_cursor_execute", coordinate_grant_and_observer)
    try:
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="policy-grant") as grants:
            grant = grants.submit(PolicyStore(postgres_engine).grant, proxy, task.benchmark, task.id, expires_at)
            assert grant_locked.wait(5)
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="policy-stop") as stops:
                stopped = stops.submit(stop)
                try:
                    wait_for_database_lock()
                finally:
                    release_grant.set()
                grant.result(timeout=5)
                stopped.result(timeout=5)
    finally:
        release_grant.set()
        event.remove(postgres_engine, "after_cursor_execute", coordinate_grant_and_observer)

    with Session(postgres_engine) as observer:
        assert observer.get(EgressProxyPolicy, key_id) is None
