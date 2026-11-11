from datetime import UTC, datetime
from uuid import UUID

from fastapi import Depends
from sqlalchemy import select
from sqlmodel import Session, col, delete, or_

from tracker.database.models import FINISHED_BENCHMARK_STATUSES, Benchmark, EgressProxyPolicy, Task, TaskStatus
from tracker.database.session import get_session
from tracker.egress import child_key_id


class EgressPolicyRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def grant(self, benchmark_id: UUID, task_row_id: UUID, policy: EgressProxyPolicy) -> bool:
        self._session.connection().execute(
            select(col(Benchmark.id))
            .where(col(Benchmark.id) == benchmark_id)
            .with_for_update(read=True, key_share=True)
        ).scalar_one()
        task = self._session.get(Task, task_row_id, with_for_update=True)
        if task is None or task.status == TaskStatus.STOPPED:
            return False
        self._session.add(policy)
        return True

    def revoke(self, key_id: str) -> None:
        self._session.exec(delete(EgressProxyPolicy).where(col(EgressProxyPolicy.key_id) == key_id))

    def revoke_owned(self, benchmark_id: UUID, task_row_id: UUID, key_id: str) -> None:
        task_owner = (
            select(col(Task.id)).where(col(Task.id) == task_row_id, col(Task.benchmark) == benchmark_id).exists()
        )
        self._session.exec(
            delete(EgressProxyPolicy)
            .where(col(EgressProxyPolicy.benchmark_id) == benchmark_id)
            .where(col(EgressProxyPolicy.key_id) == key_id)
            .where(task_owner)
        )

    def revoke_finished(self) -> int:
        finished_runs = select(col(Benchmark.id)).where(col(Benchmark.status).in_(FINISHED_BENCHMARK_STATUSES))
        result = self._session.exec(
            delete(EgressProxyPolicy).where(
                or_(
                    col(EgressProxyPolicy.benchmark_id).in_(finished_runs),
                    col(EgressProxyPolicy.expires_at) <= datetime.now(UTC),
                )
            )
        )
        return result.rowcount or 0

    def revoke_attempts(self, benchmark_id: UUID, key_ids: list[str] | None = None) -> None:
        statement = delete(EgressProxyPolicy).where(col(EgressProxyPolicy.benchmark_id) == benchmark_id)
        if key_ids is not None:
            statement = statement.where(
                or_(
                    col(EgressProxyPolicy.key_id).in_(key_ids),
                    *(
                        col(EgressProxyPolicy.key_id).startswith(child_key_id(key_id, ""), autoescape=True)
                        for key_id in key_ids
                    ),
                )
            )
        self._session.exec(statement)
