from collections.abc import Sequence
from typing import Annotated

from fastapi import Depends
from sqlmodel import Session, col, select

from executor_protocol import ExecutorDispatchStatus
from tracker.database.models import FINISHED_BENCHMARK_STATUSES, Benchmark, ExecutorDispatch, RunAccessKey
from tracker.database.session import get_session

_ACTIVE_DISPATCH_STATUSES = (ExecutorDispatchStatus.QUEUED, ExecutorDispatchStatus.RUNNING)


class RunAccessKeyRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def recorded_key_ids(self, key_ids: Sequence[str]) -> set[str]:
        return set(self._session.exec(select(RunAccessKey.key_id).where(col(RunAccessKey.key_id).in_(key_ids))).all())

    def lock_finished(self, limit: int) -> Sequence[RunAccessKey]:
        active_dispatch = (
            select(ExecutorDispatch.id)
            .where(col(ExecutorDispatch.benchmark_id) == col(Benchmark.id))
            .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
        )
        return self._session.exec(
            select(RunAccessKey)
            .join(Benchmark, col(Benchmark.id) == col(RunAccessKey.benchmark_id))
            .where(col(Benchmark.status).in_(FINISHED_BENCHMARK_STATUSES))
            .where(~active_dispatch.exists())
            .order_by(col(RunAccessKey.last_attempt_at).asc().nulls_first())
            .limit(limit)
            .with_for_update(of=RunAccessKey, skip_locked=True)
        ).all()

    def delete(self, key: RunAccessKey) -> None:
        self._session.delete(key)

    def save(self, key: RunAccessKey) -> None:
        self._session.add(key)


RunAccessKeyRepositoryDependency = Annotated[RunAccessKeyRepository, Depends(RunAccessKeyRepository)]
