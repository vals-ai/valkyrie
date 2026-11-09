from collections.abc import Sequence
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import Depends
from sqlalchemy import and_, delete, func, or_, update
from sqlmodel import Session, SQLModel, col, select

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchPayload,
    ExecutorDispatchStatus,
)
from tracker.database.session import get_session

_ACTIVE_DISPATCH_STATUSES = (ExecutorDispatchStatus.QUEUED, ExecutorDispatchStatus.RUNNING)


class ExecutorDispatchRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def now(self) -> datetime:
        return self._session.exec(select(func.current_timestamp())).one().replace(tzinfo=None)

    def get(self, dispatch_id: UUID) -> ExecutorDispatch | None:
        return self._session.get(ExecutorDispatch, dispatch_id)

    def created_at(self, dispatch_id: UUID) -> datetime:
        return self._session.exec(
            select(col(ExecutorDispatch.created_at)).where(col(ExecutorDispatch.id) == dispatch_id)
        ).one()

    def latest_id(self, benchmark_id: UUID) -> UUID | None:
        return self._session.exec(
            select(ExecutorDispatch.id)
            .where(ExecutorDispatch.benchmark_id == benchmark_id)
            .order_by(col(ExecutorDispatch.created_at).desc(), col(ExecutorDispatch.id).desc())
            .limit(1)
        ).first()

    def lock(
        self,
        dispatch_id: UUID,
        *,
        benchmark_id: UUID | None = None,
        status: ExecutorDispatchStatus | None = None,
        expired_before: datetime | None = None,
    ) -> ExecutorDispatch | None:
        query = select(ExecutorDispatch).where(col(ExecutorDispatch.id) == dispatch_id)
        if benchmark_id is not None:
            query = query.where(ExecutorDispatch.benchmark_id == benchmark_id)
        if status is not None:
            query = query.where(ExecutorDispatch.status == status)
        if expired_before is not None:
            query = query.where(col(ExecutorDispatch.lease_expires_at) <= expired_before)
        return self._session.exec(query.execution_options(populate_existing=True).with_for_update()).one_or_none()

    def lock_claimable(self, dispatch_id: UUID, created_after: datetime) -> ExecutorDispatch | None:
        return self._session.exec(
            select(ExecutorDispatch)
            .join(Benchmark, col(Benchmark.id) == col(ExecutorDispatch.benchmark_id))
            .where(col(ExecutorDispatch.id) == dispatch_id)
            .where(ExecutorDispatch.status == ExecutorDispatchStatus.QUEUED)
            .where(ExecutorDispatch.created_at > created_after)
            .where(Benchmark.status == BenchmarkStatus.IN_PROGRESS)
            .where(Benchmark.current_execution_release_id == ExecutorDispatch.executor_release_id)
            .with_for_update(of=ExecutorDispatch)
        ).one_or_none()

    def take_payload(self, dispatch_id: UUID) -> ExecutorDispatchPayload:
        payload = self._session.exec(
            select(ExecutorDispatchPayload).where(ExecutorDispatchPayload.dispatch_id == dispatch_id).with_for_update()
        ).one()
        self._session.delete(payload)
        return payload

    def delete_payload(self, dispatch_id: UUID) -> None:
        self._session.exec(
            delete(ExecutorDispatchPayload).where(col(ExecutorDispatchPayload.dispatch_id) == dispatch_id)
        )

    def delete_unqueued_payloads(self) -> None:
        self._session.exec(
            delete(ExecutorDispatchPayload).where(
                col(ExecutorDispatchPayload.dispatch_id).in_(
                    select(ExecutorDispatch.id).where(col(ExecutorDispatch.status) != ExecutorDispatchStatus.QUEUED)
                )
            )
        )

    def renew_lease(self, dispatch_id: UUID, now: datetime, lease_expires_at: datetime) -> bool:
        return (
            self._session.exec(
                update(ExecutorDispatch)
                .where(col(ExecutorDispatch.id) == dispatch_id)
                .where(col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING)
                .where(col(ExecutorDispatch.lease_expires_at) > now)
                .values(lease_expires_at=lease_expires_at)
                .returning(col(ExecutorDispatch.id))
            ).first()
            is not None
        )

    def terminalize_active(
        self, benchmark_id: UUID, finished_at: datetime, *, except_dispatch_id: UUID | None = None
    ) -> list[str]:
        dispatches = (
            update(ExecutorDispatch)
            .where(col(ExecutorDispatch.benchmark_id) == benchmark_id)
            .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
        )
        if except_dispatch_id is not None:
            dispatches = dispatches.where(col(ExecutorDispatch.id) != except_dispatch_id)
        task_arns = self._session.exec(
            dispatches.values(status=ExecutorDispatchStatus.FAILED, finished_at=finished_at).returning(
                col(ExecutorDispatch.ecs_task_arn)
            )
        ).scalars()
        return [task_arn for task_arn in task_arns if task_arn is not None]

    def has_active(self, benchmark_id: UUID) -> bool:
        return (
            self._session.exec(
                select(ExecutorDispatch.id)
                .where(col(ExecutorDispatch.benchmark_id) == benchmark_id)
                .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
            ).first()
            is not None
        )

    def list_active_siblings(self, benchmark_id: UUID, except_dispatch_id: UUID) -> Sequence[ExecutorDispatch]:
        return self._session.exec(
            select(ExecutorDispatch)
            .where(col(ExecutorDispatch.benchmark_id) == benchmark_id)
            .where(col(ExecutorDispatch.id) != except_dispatch_id)
            .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
        ).all()

    def list_for_benchmark(self, benchmark_id: UUID) -> Sequence[ExecutorDispatch]:
        return self._session.exec(
            select(ExecutorDispatch)
            .where(col(ExecutorDispatch.benchmark_id) == benchmark_id)
            .order_by(col(ExecutorDispatch.created_at).desc(), col(ExecutorDispatch.id))
        ).all()

    def list_active(self) -> Sequence[ExecutorDispatch]:
        return self._session.exec(
            select(ExecutorDispatch)
            .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
            .order_by(col(ExecutorDispatch.created_at), col(ExecutorDispatch.id))
        ).all()

    def lock_active(self) -> Sequence[ExecutorDispatch]:
        return self._session.exec(
            select(ExecutorDispatch)
            .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
            .order_by(col(ExecutorDispatch.id))
            .with_for_update()
        ).all()

    def lock_queued(self, benchmark_id: UUID) -> Sequence[ExecutorDispatch]:
        return self._session.exec(
            select(ExecutorDispatch)
            .where(ExecutorDispatch.benchmark_id == benchmark_id)
            .where(ExecutorDispatch.status == ExecutorDispatchStatus.QUEUED)
            .with_for_update()
        ).all()

    def list_with_task_arn(self) -> Sequence[ExecutorDispatch]:
        return self._session.exec(
            select(ExecutorDispatch)
            .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
            .where(col(ExecutorDispatch.ecs_task_arn).is_not(None))
        ).all()

    def list_expired(self, claim_cutoff: datetime, now: datetime) -> Sequence[ExecutorDispatch]:
        return self._session.exec(
            select(ExecutorDispatch).where(
                or_(
                    and_(
                        col(ExecutorDispatch.status) == ExecutorDispatchStatus.QUEUED,
                        col(ExecutorDispatch.created_at) <= claim_cutoff,
                    ),
                    and_(
                        col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING,
                        col(ExecutorDispatch.lease_expires_at) <= now,
                    ),
                )
            )
        ).all()

    def save(self, *rows: SQLModel) -> None:
        self._session.add_all(rows)


ExecutorDispatchRepositoryDependency = Annotated[ExecutorDispatchRepository, Depends(ExecutorDispatchRepository)]
