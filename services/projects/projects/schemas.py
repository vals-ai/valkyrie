from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

from projects.models import SpendType


def _utc(value: datetime) -> str:
    return (value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)).isoformat()


UTCDateTime = Annotated[datetime, PlainSerializer(_utc, return_type=str)]


class ProjectCopy(BaseModel):
    id: UUID
    handle: str
    name: str
    spend_type: SpendType
    subcategory: str | None = None
    customer: str | None = None
    extra_tags: dict[str, str] = Field(default_factory=dict)
    gateway_request_id: UUID | None = None
    gateway_key_id: UUID | None = None
    created_at: datetime
    created_by_email: str | None = None
    archived_at: datetime | None = None


class ProjectRecord(ProjectCopy):
    model_config = ConfigDict(from_attributes=True)

    created_at: UTCDateTime
    archived_at: UTCDateTime | None = None


class ProjectChanges(BaseModel):
    name: str
    spend_type: SpendType
    subcategory: str | None = None
    customer: str | None = None
    extra_tags: dict[str, str] = Field(default_factory=dict)


class Archive(BaseModel):
    archived_at: datetime


class GatewayKey(BaseModel):
    gateway_key_id: UUID


class GatewayRequest(BaseModel):
    gateway_request_id: UUID


class ProjectResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    handle: str
    name: str
    spend_type: SpendType
    subcategory: str | None = None
    customer: str | None = None
    extra_tags: dict[str, str] = Field(default_factory=dict)
    created_at: UTCDateTime
    created_by_email: str | None = None
    archived_at: UTCDateTime | None = None


class ProjectsResponse(BaseModel):
    projects: list[ProjectResponse]
