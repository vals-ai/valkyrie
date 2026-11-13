from collections.abc import Sequence
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import Depends
from sqlalchemy import or_
from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, BenchmarkStatus, SandboxBuildReservation, Task, TaskStatus
from tracker.database.session import get_session


class SandboxBuildReservationRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def lock_active(self, task_row_id: UUID, attempt_started_at: datetime) -> SandboxBuildReservation | None:
        return self._session.exec(
            select(SandboxBuildReservation)
            .where(
                col(SandboxBuildReservation.task_row_id) == task_row_id,
                col(SandboxBuildReservation.attempt_started_at) == attempt_started_at,
                col(SandboxBuildReservation.released_at).is_(None),
            )
            .with_for_update()
        ).first()

    def lock_attempt(self, task_row_id: UUID, attempt_started_at: datetime) -> SandboxBuildReservation | None:
        return self._session.exec(
            select(SandboxBuildReservation)
            .where(
                col(SandboxBuildReservation.task_row_id) == task_row_id,
                col(SandboxBuildReservation.attempt_started_at) == attempt_started_at,
            )
            .with_for_update()
        ).first()

    def active_task_ids(self, task_row_ids: Sequence[UUID]) -> Sequence[UUID]:
        return self._session.exec(
            select(SandboxBuildReservation.task_row_id)
            .where(col(SandboxBuildReservation.task_row_id).in_(task_row_ids))
            .where(col(SandboxBuildReservation.released_at).is_(None))
        ).all()

    def save(self, reservation: SandboxBuildReservation) -> None:
        self._session.add(reservation)

    def lock_active_phase_reservations(self, dispatch_id: UUID) -> Sequence[SandboxBuildReservation]:
        return self._session.exec(
            select(SandboxBuildReservation)
            .where(col(SandboxBuildReservation.phase_dispatch_id) == dispatch_id)
            .where(col(SandboxBuildReservation.released_at).is_(None))
            .order_by(col(SandboxBuildReservation.task_row_id))
            .with_for_update()
        ).all()

    def lock_pool_orphans(self, pool_id: str) -> Sequence[tuple[Task, Benchmark, SandboxBuildReservation | None]]:
        return self._session.exec(
            select(Task, Benchmark, SandboxBuildReservation)
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .outerjoin(
                SandboxBuildReservation,
                (col(SandboxBuildReservation.task_row_id) == col(Task.id))
                & col(SandboxBuildReservation.released_at).is_(None),
            )
            .where(
                or_(
                    (col(Task.status) == TaskStatus.BUILDING) & (col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS),
                    col(SandboxBuildReservation.task_row_id).is_not(None),
                )
            )
            .where(col(Benchmark.queue_pool_id) == pool_id)
            .with_for_update(of=Task, skip_locked=True)
        ).all()


SandboxBuildReservationRepositoryDependency = Annotated[
    SandboxBuildReservationRepository, Depends(SandboxBuildReservationRepository)
]
