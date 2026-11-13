from collections.abc import Sequence
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import Depends
from sqlalchemy import and_, delete, func, or_, text, update
from sqlmodel import Session, SQLModel, col, select

from tracker.database.models import ExecutorDispatch, ExecutorDispatchPayload, ExecutorDispatchStatus
from tracker.database.session import get_session

_ACTIVE_DISPATCH_STATUSES = (ExecutorDispatchStatus.QUEUED, ExecutorDispatchStatus.RUNNING)


class ExecutorDispatchRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def now(self) -> datetime:
        return self._session.exec(select(func.current_timestamp())).one().replace(tzinfo=None)

    def clock_timestamp(self) -> datetime:
        return self._session.exec(select(func.clock_timestamp())).one()

    def benchmark_id(self, dispatch_id: UUID) -> UUID | None:
        return self._session.exec(
            select(ExecutorDispatch.benchmark_id).where(col(ExecutorDispatch.id) == dispatch_id)
        ).one_or_none()

    def get(self, dispatch_id: UUID) -> ExecutorDispatch | None:
        return self._session.get(ExecutorDispatch, dispatch_id)

    def get_current(self, dispatch_id: UUID) -> ExecutorDispatch | None:
        return self._session.exec(
            select(ExecutorDispatch)
            .where(col(ExecutorDispatch.id) == dispatch_id)
            .execution_options(populate_existing=True)
        ).one_or_none()

    def set_short_lock_timeout(self) -> None:
        self._session.exec(text("SET LOCAL lock_timeout = '1s'"))

    def payload(self, dispatch_id: UUID) -> ExecutorDispatchPayload | None:
        return self._session.exec(
            select(ExecutorDispatchPayload).where(col(ExecutorDispatchPayload.dispatch_id) == dispatch_id)
        ).one_or_none()

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
        ecs_task_arn: str | None = None,
        claim_expired_before: datetime | None = None,
        shared: bool = False,
    ) -> ExecutorDispatch | None:
        query = select(ExecutorDispatch).where(col(ExecutorDispatch.id) == dispatch_id)
        if benchmark_id is not None:
            query = query.where(ExecutorDispatch.benchmark_id == benchmark_id)
        if status is not None:
            query = query.where(ExecutorDispatch.status == status)
        if expired_before is not None:
            query = query.where(col(ExecutorDispatch.lease_expires_at) <= expired_before)
        if ecs_task_arn is not None:
            query = query.where(ExecutorDispatch.ecs_task_arn == ecs_task_arn)
        if claim_expired_before is not None:
            query = query.where(col(ExecutorDispatch.claim_deadline_at) <= claim_expired_before)
        return self._session.exec(
            query.execution_options(populate_existing=True).with_for_update(read=shared)
        ).one_or_none()

    def lock_skip_locked(self, dispatch_id: UUID) -> ExecutorDispatch | None:
        return self._session.exec(
            select(ExecutorDispatch)
            .where(col(ExecutorDispatch.id) == dispatch_id)
            .execution_options(populate_existing=True)
            .with_for_update(skip_locked=True)
        ).one_or_none()

    def delete_payload(self, dispatch_id: UUID) -> None:
        self._session.exec(
            delete(ExecutorDispatchPayload).where(col(ExecutorDispatchPayload.dispatch_id) == dispatch_id)
        )

    def delete_terminal_payloads(self) -> None:
        self._session.exec(
            delete(ExecutorDispatchPayload).where(
                col(ExecutorDispatchPayload.dispatch_id).in_(
                    select(ExecutorDispatch.id).where(col(ExecutorDispatch.status).notin_(_ACTIVE_DISPATCH_STATUSES))
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

    def list_expired(self, now: datetime) -> Sequence[ExecutorDispatch]:
        return self._session.exec(
            select(ExecutorDispatch).where(
                or_(
                    and_(
                        col(ExecutorDispatch.status) == ExecutorDispatchStatus.QUEUED,
                        col(ExecutorDispatch.claim_deadline_at) <= now,
                    ),
                    and_(
                        col(ExecutorDispatch.status) == ExecutorDispatchStatus.RUNNING,
                        col(ExecutorDispatch.ecs_task_arn).is_(None),
                        col(ExecutorDispatch.lease_expires_at) <= now,
                    ),
                )
            )
        ).all()

    def record_relaunch(
        self, dispatch_id: UUID, stopped_task_arn: str, replacement_task_arn: str, reason: str | None
    ) -> bool:
        return (
            self._session.exec(
                update(ExecutorDispatch)
                .where(col(ExecutorDispatch.id) == dispatch_id)
                .where(col(ExecutorDispatch.status) == ExecutorDispatchStatus.QUEUED)
                .where(col(ExecutorDispatch.ecs_task_arn) == stopped_task_arn)
                .values(
                    ecs_task_arn=replacement_task_arn,
                    relaunch_count=col(ExecutorDispatch.relaunch_count) + 1,
                    last_relaunch_reason=reason,
                )
                .returning(col(ExecutorDispatch.id))
            ).first()
            is not None
        )

    def save(self, *rows: SQLModel) -> None:
        self._session.add_all(rows)


ExecutorDispatchRepositoryDependency = Annotated[ExecutorDispatchRepository, Depends(ExecutorDispatchRepository)]
