from collections.abc import Sequence
from typing import Annotated

from fastapi import Depends
from sqlalchemy import text
from sqlmodel import Session, col, select

from tracker.database.models import ExecutorAdmission, ExecutorRelease, ExecutorReleaseStatus
from tracker.database.session import get_session
from tracker.repositories.advisory_lock import advisory_lock_key

_ADMISSION_FENCE_KEY = advisory_lock_key("executor-admission")


class ExecutorReleaseRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def get(
        self, release_id: str, *, populate_existing: bool = False, for_update: bool = False, for_share: bool = False
    ) -> ExecutorRelease | None:
        return self._session.get(
            ExecutorRelease,
            release_id,
            populate_existing=populate_existing or for_update or for_share,
            with_for_update={"read": True} if for_share else for_update or None,
        )

    def task_definition_arn(self, release_id: str) -> str | None:
        return self._session.exec(
            select(ExecutorRelease.task_definition_arn).where(ExecutorRelease.id == release_id)
        ).one()

    def draining_ids(self) -> Sequence[str]:
        return self._session.exec(
            select(ExecutorRelease.id).where(ExecutorRelease.status == ExecutorReleaseStatus.DRAINING)
        ).all()

    def lock_draining(self) -> Sequence[ExecutorRelease]:
        return self._session.exec(
            select(ExecutorRelease)
            .where(ExecutorRelease.status == ExecutorReleaseStatus.DRAINING)
            .order_by(ExecutorRelease.id)
            .with_for_update()
        ).all()

    def admission(self, *, for_update: bool = False, for_share: bool = False) -> ExecutorAdmission | None:
        statement = (
            select(ExecutorAdmission).where(col(ExecutorAdmission.id) == 1).execution_options(populate_existing=True)
        )
        if for_update:
            self._queue_on_admission_fence("pg_advisory_xact_lock")
            statement = statement.with_for_update()
        elif for_share:
            self._queue_on_admission_fence("pg_advisory_xact_lock_shared")
            statement = statement.with_for_update(read=True)
        return self._session.exec(statement).one_or_none()

    def shared_admission(self) -> ExecutorAdmission | None:
        self._queue_on_admission_fence("pg_advisory_xact_lock_shared")
        statement = (
            select(ExecutorAdmission).where(col(ExecutorAdmission.id) == 1).execution_options(populate_existing=True)
        )
        return self._session.exec(statement).one_or_none()

    def _queue_on_admission_fence(self, function: str) -> None:
        if self._session.get_bind().dialect.name == "postgresql":
            self._session.execute(text(f"SELECT {function}(:key)"), {"key": _ADMISSION_FENCE_KEY})

    def save(self, *rows: ExecutorRelease | ExecutorAdmission) -> None:
        self._session.add_all(rows)

    def schema_revision(self) -> str:
        return self._session.exec(text("SELECT version_num FROM alembic_version")).scalar_one()

    def maintenance_owner(self) -> str | None:
        return self._session.exec(
            select(col(ExecutorAdmission.maintenance_target_sha)).where(col(ExecutorAdmission.id) == 1)
        ).one()


ExecutorReleaseRepositoryDependency = Annotated[ExecutorReleaseRepository, Depends(ExecutorReleaseRepository)]
