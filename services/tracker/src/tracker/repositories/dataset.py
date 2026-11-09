from typing import Annotated
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from fastapi import Depends
from sqlmodel import Session, col, select

from tracker.database.session import get_session
from tracker.database.models import Dataset
from tracker.exceptions import DatasetNameTakenError, ResourceArchivedError


def _now() -> datetime:
    return datetime.now(UTC)


class DatasetRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def create(
        self, benchmark_definition_id: UUID, name: str, description: str, created_by_email: str | None
    ) -> Dataset:
        dataset = Dataset(
            benchmark_definition_id=benchmark_definition_id,
            name=name,
            description=description,
            created_by_email=created_by_email,
        )
        self._session.add(dataset)
        try:
            self._session.commit()
        except IntegrityError as error:
            self._session.rollback()
            raise DatasetNameTakenError(name) from error
        return dataset

    def get(self, benchmark_definition_id: UUID, name: str) -> Dataset | None:
        return self._session.exec(self._of(benchmark_definition_id).where(col(Dataset.name) == name)).first()

    def list_all(self, benchmark_definition_id: UUID, include_archived: bool = False) -> list[Dataset]:
        query = self._of(benchmark_definition_id).order_by(col(Dataset.name))
        if not include_archived:
            query = query.where(col(Dataset.archived_at).is_(None))
        return list(self._session.exec(query).all())

    def update_description(self, benchmark_definition_id: UUID, name: str, description: str) -> Dataset | None:
        dataset = self.get(benchmark_definition_id, name)
        if dataset is None:
            return None
        if dataset.archived_at is not None:
            raise ResourceArchivedError("Dataset", name)
        dataset.description = description
        dataset.updated_at = _now()
        self._session.commit()
        return dataset

    def archive(self, benchmark_definition_id: UUID, name: str) -> Dataset | None:
        dataset = self.get(benchmark_definition_id, name)
        if dataset is None or dataset.archived_at is not None:
            return dataset
        dataset.archived_at = dataset.updated_at = _now()
        self._session.commit()
        return dataset

    def restore(self, benchmark_definition_id: UUID, name: str) -> Dataset | None:
        dataset = self.get(benchmark_definition_id, name)
        if dataset is None or dataset.archived_at is None:
            return dataset
        dataset.archived_at = None
        dataset.updated_at = _now()
        self._session.commit()
        return dataset

    @staticmethod
    def _of(benchmark_definition_id: UUID):
        return select(Dataset).where(col(Dataset.benchmark_definition_id) == benchmark_definition_id)


DatasetRepositoryDependency = Annotated[DatasetRepository, Depends(DatasetRepository)]
