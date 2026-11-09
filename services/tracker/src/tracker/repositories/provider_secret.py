from typing import Annotated
from uuid import UUID

from fastapi import Depends
from sqlmodel import Session, col, delete, select

from tracker.database.session import get_session
from tracker.database.models import OrgProviderSecret


class ProviderSecretRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def list_all(self, org_id: UUID) -> dict[str, str]:
        rows = self._session.exec(select(OrgProviderSecret).where(col(OrgProviderSecret.org_id) == org_id))
        return {row.provider: row.secret_name for row in rows}

    def for_provider(self, provider: str) -> list[OrgProviderSecret]:
        return list(self._session.exec(select(OrgProviderSecret).where(col(OrgProviderSecret.provider) == provider)))

    def set(self, org_id: UUID, provider: str, secret_name: str) -> None:
        self._session.merge(OrgProviderSecret(org_id=org_id, provider=provider, secret_name=secret_name))
        self._session.commit()

    def delete(self, org_id: UUID, provider: str) -> bool:
        deleted = self._session.exec(
            delete(OrgProviderSecret).where(
                col(OrgProviderSecret.org_id) == org_id, col(OrgProviderSecret.provider) == provider
            )
        ).rowcount
        self._session.commit()
        return deleted > 0


ProviderSecretRepositoryDependency = Annotated[ProviderSecretRepository, Depends(ProviderSecretRepository)]
