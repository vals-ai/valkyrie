from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from sqlalchemy import JSON, DateTime, Index, Numeric, String, func
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


class LimitRequestStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"


class ProjectLimit(Base):
    __tablename__ = "project_limit"

    project_id: Mapped[UUID] = mapped_column(primary_key=True)
    limit_dollars: Mapped[Decimal] = mapped_column(Numeric())
    gateway_confirmed_limit: Mapped[Decimal | None] = mapped_column(Numeric())
    version: Mapped[int]
    reason: Mapped[str | None] = mapped_column(String)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ProjectLimitRequest(Base):
    __tablename__ = "project_limit_request"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    project_id: Mapped[UUID]
    limit_dollars: Mapped[Decimal] = mapped_column(Numeric())
    reason: Mapped[str]
    status: Mapped[LimitRequestStatus] = mapped_column(String)
    requested_by_email: Mapped[str | None]
    decided_by_email: Mapped[str | None]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


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
