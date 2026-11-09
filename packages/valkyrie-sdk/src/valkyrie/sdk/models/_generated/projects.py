from __future__ import annotations
from enum import Enum
from typing import Annotated
from pydantic import BaseModel, ConfigDict, Field
from decimal import Decimal
from uuid import UUID
from valkyrie.sdk.models._base import ResponseModel, UTCDateTime


class SpendType(str, Enum):
    """Why a run spends money."""

    DEVELOPMENT = "development"
    INTERNAL = "internal"
    CUSTOMER = "customer"


class ProjectCreateRequest(BaseModel):
    """
    A new project: its immutable handle, name, spend type and subcategory, and optional customer and extra tags.

    ``handle`` is the string runs are started with (lowercase letters, digits and hyphens, e.g. ``my-project``) and
    ``name`` is the label people read (``My Project``). The tracker accepts only the subcategories in
    ``SPEND_SUBCATEGORIES`` for the spend type and requires ``customer`` when the spend type is ``customer``.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    handle: str
    name: Annotated[str, Field(min_length=1)]
    spend_type: SpendType
    subcategory: str
    customer: str | None = None
    extra_tags: dict[str, str] = {}


class NewProjectRequest(ProjectCreateRequest):
    """A project to create with its main key's budget and reason."""

    model_config = ConfigDict(
        extra="forbid",
    )
    budget: Annotated[Decimal | None, Field(gt=0)] = None
    budget_reason: Annotated[str | None, Field(min_length=1)] = None


class ProjectResponse(ResponseModel):
    """One saved project."""

    id: UUID
    handle: str
    name: str
    spend_type: SpendType
    subcategory: str | None = None
    customer: str | None = None
    extra_tags: dict[str, str] = {}
    created_at: UTCDateTime
    created_by_email: str | None = None
    archived_at: UTCDateTime | None = None


class ProjectsResponse(ResponseModel):
    """The organization's projects, by handle."""

    projects: list[ProjectResponse]
