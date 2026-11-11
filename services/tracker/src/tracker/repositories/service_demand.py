from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import case, func, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, BenchmarkServiceWake, BenchmarkStatus, Task, TaskStatus


def active_runs(engine: Engine, statuses: Sequence[BenchmarkStatus]) -> list[tuple[str, str | None, int]]:
    with Session(engine) as session:
        rows = session.exec(
            select(Benchmark.name, Benchmark.custom_benchmark_service, func.count(col(Benchmark.id)))
            .where(col(Benchmark.status).in_(statuses))
            .group_by(col(Benchmark.name), col(Benchmark.custom_benchmark_service))
        ).all()
    return [(name, custom, count) for name, custom, count in rows]


def recent_tasks(
    engine: Engine, statuses: Sequence[TaskStatus], since: datetime
) -> list[tuple[str, str | None, int, datetime | None]]:
    with Session(engine) as session:
        rows = session.exec(
            select(
                Benchmark.name,
                Benchmark.custom_benchmark_service,
                func.sum(case((col(Task.status).in_(statuses), 1), else_=0)),
                func.max(Task.finished_at),
            )
            .join(Task, col(Task.benchmark) == col(Benchmark.id))
            .where(or_(col(Task.status).in_(statuses), col(Task.finished_at) >= since))
            .group_by(col(Benchmark.name), col(Benchmark.custom_benchmark_service))
        ).all()
    return [(name, custom, int(count or 0), finished_at) for name, custom, count, finished_at in rows]


def wakes_since(engine: Engine, since: datetime) -> list[BenchmarkServiceWake]:
    with Session(engine) as session:
        return list(session.exec(select(BenchmarkServiceWake).where(col(BenchmarkServiceWake.requested_at) >= since)))


def record_wake(engine: Engine, name: str) -> None:
    now = datetime.now(UTC)
    with Session(engine) as session:
        session.exec(
            insert(BenchmarkServiceWake)
            .values(name=name, requested_at=now)
            .on_conflict_do_update(index_elements=["name"], set_={"requested_at": now})
        )
        session.commit()


def wake_recorded(session: Session, name: str) -> bool:
    return session.get(BenchmarkServiceWake, name) is not None


def was_woken(engine: Engine, name: str) -> bool:
    with Session(engine) as session:
        return wake_recorded(session, name)
