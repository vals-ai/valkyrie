from datetime import datetime
from enum import StrEnum
from uuid import UUID

from sqlalchemy import JSON, DateTime, Index, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class SpendType(StrEnum):
    DEVELOPMENT = "development"
    INTERNAL = "internal"
    CUSTOMER = "customer"


class Base(DeclarativeBase):
    pass


class Project(Base):
    __tablename__ = "project"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    org_id: Mapped[UUID]
    handle: Mapped[str]
    name: Mapped[str]
    spend_type: Mapped[SpendType] = mapped_column(String)
    subcategory: Mapped[str | None]
    customer: Mapped[str | None]
    extra_tags: Mapped[dict[str, str]] = mapped_column(JSON)
    gateway_request_id: Mapped[UUID | None]
    gateway_key_id: Mapped[UUID | None]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_by_email: Mapped[str | None]
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


LIVE = Project.deleted_at.is_(None)
_ = Index(
    "unique_project_handle_per_org",
    Project.org_id,
    Project.handle,
    unique=True,
    postgresql_where=LIVE,
    sqlite_where=LIVE,
)
_ = Index(
    "uq_project_org_name",
    Project.org_id,
    func.lower(func.trim(Project.name)),
    unique=True,
    postgresql_where=LIVE,
    sqlite_where=LIVE,
)
