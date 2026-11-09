from collections.abc import Sequence
from typing import Annotated

from fastapi import Depends
from sqlmodel import Session, col, select

from tracker.database.models import ExecutorAdmission, ExecutorRelease, ExecutorReleaseStatus
from tracker.database.session import get_session


class ExecutorReleaseRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def get(
        self, release_id: str, *, populate_existing: bool = False, for_update: bool = False
    ) -> ExecutorRelease | None:
        return self._session.get(
            ExecutorRelease,
            release_id,
            populate_existing=populate_existing or for_update,
            with_for_update=for_update or None,
        )

    def task_definition_arn(self, release_id: str) -> str | None:
        return self._session.exec(
            select(ExecutorRelease.task_definition_arn).where(ExecutorRelease.id == release_id)
        ).one()

    def lock_draining(self) -> Sequence[ExecutorRelease]:
        return self._session.exec(
            select(ExecutorRelease)
            .where(ExecutorRelease.status == ExecutorReleaseStatus.DRAINING)
            .order_by(ExecutorRelease.id)
            .with_for_update()
        ).all()

    def admission(self, *, for_update: bool = False) -> ExecutorAdmission | None:
        statement = (
            select(ExecutorAdmission).where(col(ExecutorAdmission.id) == 1).execution_options(populate_existing=True)
        )
        if for_update:
            statement = statement.with_for_update()
        return self._session.exec(statement).one_or_none()

    def save(self, *rows: ExecutorRelease) -> None:
        self._session.add_all(rows)


ExecutorReleaseRepositoryDependency = Annotated[ExecutorReleaseRepository, Depends(ExecutorReleaseRepository)]
