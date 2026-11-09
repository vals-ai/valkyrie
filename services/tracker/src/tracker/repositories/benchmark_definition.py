from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from fastapi import Depends
from sqlmodel import Session, col, select

from tracker.database.session import get_session
from tracker.database.models import BenchmarkDefinition, Dataset
from tracker.exceptions import BenchmarkHasDatasetsError, BenchmarkNameTakenError, ResourceArchivedError


def _now() -> datetime:
    return datetime.now(UTC)


class BenchmarkDefinitionRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def create(self, org_id: UUID, name: str, description: str, created_by_email: str | None) -> BenchmarkDefinition:
        benchmark = BenchmarkDefinition(
            org_id=org_id, name=name, description=description, created_by_email=created_by_email
        )
        self._session.add(benchmark)
        try:
            self._session.commit()
        except IntegrityError as error:
            self._session.rollback()
            raise BenchmarkNameTakenError(name) from error
        return benchmark

    def get(
        self, org_id: UUID, name: str, lock: Literal["share", "update"] | None = None
    ) -> BenchmarkDefinition | None:
        query = self._owned(org_id).where(col(BenchmarkDefinition.name) == name)
        if lock is not None:
            query = query.with_for_update(read=lock == "share")
        return self._session.exec(query).first()

    def list_all(self, org_id: UUID, include_archived: bool = False) -> list[BenchmarkDefinition]:
        query = self._owned(org_id).order_by(col(BenchmarkDefinition.name))
        if not include_archived:
            query = query.where(col(BenchmarkDefinition.archived_at).is_(None))
        return list(self._session.exec(query).all())

    def update_description(self, org_id: UUID, name: str, description: str) -> BenchmarkDefinition | None:
        benchmark = self.get(org_id, name, "update")
        if benchmark is None:
            return None
        if benchmark.archived_at is not None:
            raise ResourceArchivedError("Benchmark", name)
        benchmark.description = description
        benchmark.updated_at = _now()
        self._session.commit()
        return benchmark

    def archive(self, org_id: UUID, name: str) -> BenchmarkDefinition | None:
        benchmark = self.get(org_id, name, "update")
        if benchmark is None or benchmark.archived_at is not None:
            return benchmark
        active_dataset = self._session.exec(
            select(Dataset.id)
            .where(col(Dataset.benchmark_definition_id) == benchmark.id, col(Dataset.archived_at).is_(None))
            .limit(1)
        ).first()
        if active_dataset is not None:
            raise BenchmarkHasDatasetsError(name)
        benchmark.archived_at = benchmark.updated_at = _now()
        self._session.commit()
        return benchmark

    def restore(self, org_id: UUID, name: str) -> BenchmarkDefinition | None:
        benchmark = self.get(org_id, name, "update")
        if benchmark is None or benchmark.archived_at is None:
            return benchmark
        benchmark.archived_at = None
        benchmark.updated_at = _now()
        self._session.commit()
        return benchmark

    @staticmethod
    def _owned(org_id: UUID):
        return select(BenchmarkDefinition).where(col(BenchmarkDefinition.org_id) == org_id)


BenchmarkDefinitionRepositoryDependency = Annotated[
    BenchmarkDefinitionRepository, Depends(BenchmarkDefinitionRepository)
]
