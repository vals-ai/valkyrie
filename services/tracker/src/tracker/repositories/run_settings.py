from typing import Annotated
from uuid import UUID

from fastapi import Depends
from sqlmodel import Session, col, delete, select

from tracker.database.session import get_session
from tracker.database.models import UserBenchmarkService, UserDefaultProvider


class RunSettingsRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def get_default_provider(self, org_id: UUID, user_id: str) -> UserDefaultProvider | None:
        return self._session.get(UserDefaultProvider, (org_id, user_id))

    def list_benchmark_services(self, org_id: UUID, user_id: str) -> list[UserBenchmarkService]:
        return list(
            self._session.exec(
                select(UserBenchmarkService).where(
                    col(UserBenchmarkService.org_id) == org_id, col(UserBenchmarkService.user_id) == user_id
                )
            )
        )

    def save(self, row: UserDefaultProvider | UserBenchmarkService) -> None:
        self._session.merge(row)
        self._session.commit()

    def delete_benchmark_service(self, org_id: UUID, user_id: str, benchmark: str) -> bool:
        deleted = self._session.exec(
            delete(UserBenchmarkService).where(
                col(UserBenchmarkService.org_id) == org_id,
                col(UserBenchmarkService.user_id) == user_id,
                col(UserBenchmarkService.benchmark) == benchmark,
            )
        ).rowcount
        self._session.commit()
        return deleted > 0


RunSettingsRepositoryDependency = Annotated[RunSettingsRepository, Depends(RunSettingsRepository)]
