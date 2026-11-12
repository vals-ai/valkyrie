from typing import Annotated
from uuid import UUID

from fastapi import Depends
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, select

from tracker.database.models import Org
from tracker.database.session import get_session


class OrganizationRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def get(self, org_id: UUID) -> Org | None:
        return self._session.get(Org, org_id)

    def list_ids(self) -> list[UUID]:
        return list(self._session.exec(select(Org.id).order_by(col(Org.id))).all())

    def get_by_name(self, name: str) -> Org | None:
        return self._session.exec(select(Org).where(Org.name == name)).first()

    def create(self, name: str) -> None:
        self._session.add(Org(name=name))
        try:
            self._session.commit()
        except IntegrityError:
            self._session.rollback()

    def set_storage_key(self, org: Org, storage_key: str) -> None:
        org.storage_key = storage_key
        self._session.add(org)
        self._session.commit()
        self._session.refresh(org)


OrganizationRepositoryDependency = Annotated[OrganizationRepository, Depends(OrganizationRepository)]
