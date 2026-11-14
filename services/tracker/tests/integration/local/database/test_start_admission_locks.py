from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tests.factories import make_benchmark
from tracker.database.models import ExecutorAdmission, ExecutorRelease, Org
from tracker.executor.dispatch_control import admit_start_dispatch
from tracker.executor.release_control import activate_release, promote_release, register_release
from tracker.exceptions import RunLimitExceededError
from tracker.services.runs import limits


def _seed(session: Session) -> tuple[UUID, str]:
    org = Org(id=uuid4(), name=f"parallel-starts-{uuid4()}")
    session.add(org)
    for index in range(2):
        release = ExecutorRelease(
            id=f"parallel-starts-{index}",
            artifact_uri=f"source:///parallel-starts-{index}",
            artifact_digest="a" * 64,
            protocol_version=SUPPORTED_PROTOCOL_VERSION,
        )
        if index == 0:
            activate_release(session, release)
        else:
            register_release(session, release)
    session.commit()
    return org.id, release.id


def _start(session: Session, org_id: UUID) -> UUID:
    benchmark = make_benchmark(org_id=org_id)
    admit_start_dispatch(session, benchmark=benchmark, dispatch_id=uuid4())
    return benchmark.id


@pytest.mark.description("Starts share admission and release rows while exclusive release writers wait for commit.")
@pytest.mark.parametrize("writer_target", ["switch", "admission-row", "release-row"])
def test_parallel_starts_fence_release_writers(
    postgres_engine: Engine,
    postgres_session: Session,
    wait_for_database_lock: Callable[[], None],
    writer_target: str,
) -> None:
    org_id, candidate_id = _seed(postgres_session)

    def start() -> None:
        with Session(postgres_engine) as session:
            session.execute(text("SET LOCAL lock_timeout = '2s'"))
            _start(session, org_id)
            session.commit()

    def write() -> None:
        with Session(postgres_engine) as session:
            if writer_target == "switch":
                promote_release(session, candidate_id)
            elif writer_target == "admission-row":
                session.exec(select(ExecutorAdmission).with_for_update()).one()
            else:
                session.exec(
                    select(ExecutorRelease).where(ExecutorRelease.id == "parallel-starts-0").with_for_update()
                ).one()
            session.commit()

    with Session(postgres_engine) as first, ThreadPoolExecutor(max_workers=2) as pool:
        _start(first, org_id)
        pool.submit(start).result(timeout=5)
        writer = pool.submit(write)
        try:
            wait_for_database_lock()
            assert not writer.done()
        finally:
            first.commit()
        writer.result(timeout=5)


@pytest.mark.description("Concurrent starts preserve the configured organization run limit without a global lock.")
def test_parallel_starts_keep_run_limit(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    wait_for_database_lock: Callable[[], None],
) -> None:
    org_id, _ = _seed(postgres_session)
    cache = limits.OrgRunLimitsCache(lambda: '{"default":1}')
    cache.refresh()
    monkeypatch.setattr(limits, "run_limits", cache)

    def start() -> None:
        with Session(postgres_engine) as session:
            run_id = _start(session, org_id)
            limits.ensure_run_capacity(session, org_id, run_id)
            session.commit()

    with Session(postgres_engine) as first, ThreadPoolExecutor(max_workers=1) as pool:
        run_id = _start(first, org_id)
        limits.ensure_run_capacity(first, org_id, run_id)
        second = pool.submit(start)
        try:
            wait_for_database_lock()
            assert not second.done()
        finally:
            first.commit()
        with pytest.raises(RunLimitExceededError):
            second.result(timeout=5)
