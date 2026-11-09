from __future__ import annotations
from typing import Annotated, Literal
from pydantic import Field
from datetime import datetime
from valkyrie.sdk.models._base import ResponseModel


class AccessKeyResponse(ResponseModel):
    """A new access key; Tracker returns `key` only when it creates the key."""

    key_id: str
    key: Annotated[str, Field(repr=False)]
    expires_at: datetime


class IdentityResponse(ResponseModel):
    """The organization and user behind a request's credential."""

    organization: str
    email: str | None
    name: str | None
    credential: Literal["login", "access_key", "none"]


class InviteResponse(ResponseModel):
    """An invitation to the current organization."""

    organization: str
    email: str


class MemberEntry(ResponseModel):
    """A member of the current organization, or someone it invited."""

    email: str | None
    name: str | None
    admin: bool
    status: Literal["active", "invited"]


class MembersResponse(ResponseModel):
    """The current organization's members and pending invitations."""

    organization: str
    members: list[MemberEntry]


class OrganizationEntry(ResponseModel):
    """One organization your login belongs to; `current` marks the one requests act for."""

    id: str
    admin: bool
    current: bool


class OrganizationResponse(ResponseModel):
    """A created organization."""

    id: str
    name: str


class OrganizationsResponse(ResponseModel):
    """Your login's email, its organizations, and the organizations that invited it."""

    email: str | None
    organizations: list[OrganizationEntry]
    invitations: list[str]
