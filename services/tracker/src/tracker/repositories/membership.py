from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from fastapi import Depends
from sqlalchemy import text
from sqlmodel import Session, col, delete

from tracker.database.models import DescopeMembership
from tracker.database.session import get_session
from tracker.repositories.advisory_lock import advisory_lock_key


def membership_lock_resource_id(project_id: str, user_id: str) -> str:
    return f"descope-membership:{project_id}:{user_id}"


@dataclass(frozen=True)
class MembershipSnapshot:
    tenant_roles: dict[str, tuple[str, ...]] | None
    fetched_at: datetime | None
    cooldown_until: datetime | None = None


class MembershipRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def get(self, project_id: str, user_id: str) -> MembershipSnapshot | None:
        row = self._session.get(DescopeMembership, (project_id, user_id), populate_existing=True)
        if row is None:
            return None
        return MembershipSnapshot(
            {tenant: tuple(roles) for tenant, roles in row.tenant_roles.items()}
            if row.tenant_roles is not None
            else None,
            row.fetched_at,
            row.cooldown_until,
        )

    def save(self, project_id: str, user_id: str, snapshot: MembershipSnapshot) -> None:
        self._session.merge(
            DescopeMembership(
                project_id=project_id,
                user_id=user_id,
                tenant_roles={tenant: list(roles) for tenant, roles in snapshot.tenant_roles.items()}
                if snapshot.tenant_roles is not None
                else None,
                fetched_at=snapshot.fetched_at,
                cooldown_until=snapshot.cooldown_until,
            )
        )
        self._session.commit()

    def invalidate(self, project_id: str, user_id: str) -> None:
        self._session.execute(
            text("SELECT pg_advisory_xact_lock(:key)"),
            {"key": advisory_lock_key(membership_lock_resource_id(project_id, user_id))},
        )
        self._session.exec(
            delete(DescopeMembership).where(
                col(DescopeMembership.project_id) == project_id,
                col(DescopeMembership.user_id) == user_id,
            )
        )
        self._session.commit()


MembershipRepositoryDependency = Annotated[MembershipRepository, Depends(MembershipRepository)]
