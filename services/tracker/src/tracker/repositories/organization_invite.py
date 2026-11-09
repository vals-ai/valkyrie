from typing import Annotated
from datetime import UTC, datetime, timedelta

from sqlalchemy import ColumnElement
from sqlalchemy.exc import IntegrityError
from fastapi import Depends
from sqlmodel import Session, col, delete, select

from tracker.database.session import get_session
from tracker.database.models import OrganizationInvite

INVITATION_LIFETIME = timedelta(days=14)


class OrganizationInviteRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def _live(self) -> ColumnElement[bool]:
        return col(OrganizationInvite.created_at) > datetime.now(UTC) - INVITATION_LIFETIME

    def list_organizations(self, email: str | None) -> list[str]:
        return list(
            self._session.exec(
                select(OrganizationInvite.organization).where(OrganizationInvite.email == email, self._live())
            )
        )

    def list_emails(self, organization: str) -> list[str]:
        return list(
            self._session.exec(
                select(OrganizationInvite.email).where(OrganizationInvite.organization == organization, self._live())
            )
        )

    def save(self, invite: OrganizationInvite) -> None:
        self._session.merge(invite)
        try:
            self._session.commit()
        except IntegrityError:
            self._session.rollback()

    def delete(self, organization: str, email: str) -> bool:
        statement = delete(OrganizationInvite).where(
            col(OrganizationInvite.organization) == organization, col(OrganizationInvite.email) == email, self._live()
        )
        return self._session.exec(statement).rowcount > 0

    def commit(self) -> None:
        self._session.commit()


OrganizationInviteRepositoryDependency = Annotated[OrganizationInviteRepository, Depends(OrganizationInviteRepository)]
