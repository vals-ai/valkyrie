"""Tracker-backed projects that runs are started in."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import TYPE_CHECKING

from valkyrie.sdk.models import NewProjectRequest, ProjectCreateRequest, ProjectResponse, ProjectsResponse, SpendType
from valkyrie.sdk.resources._path import encode_path_segment

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


def _project_path(handle: str) -> str:
    return f"/projects/{encode_path_segment(handle, label='project handle')}"


class ProjectsResource:
    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def create(
        self,
        handle: str,
        *,
        name: str,
        spend_type: SpendType,
        subcategory: str,
        customer: str | None = None,
        extra_tags: Mapping[str, str] | None = None,
        budget: Decimal | None = None,
        budget_reason: str | None = None,
    ) -> ProjectResponse:
        """Create a project that runs can be started in; the ``handle`` is unique within the organization."""
        request = NewProjectRequest(
            handle=handle,
            name=name,
            spend_type=spend_type,
            subcategory=subcategory,
            customer=customer,
            extra_tags=dict(extra_tags or {}),
            budget=budget,
            budget_reason=budget_reason,
        )
        return await self._sdk.request_model("POST", "/projects", ProjectResponse, json=request.model_dump(mode="json"))

    async def list(self, *, include_archived: bool = False) -> ProjectsResponse:
        """List the organization's projects by handle, leaving out archived ones unless ``include_archived`` is set."""
        params = {"include_archived": True} if include_archived else None
        return await self._sdk.request_model("GET", "/projects", ProjectsResponse, params=params)

    async def update(
        self,
        handle: str,
        *,
        name: str,
        spend_type: SpendType,
        subcategory: str,
        customer: str | None = None,
        extra_tags: Mapping[str, str] | None = None,
    ) -> ProjectResponse:
        """Replace a project's shared spend metadata without changing its handle."""
        request = ProjectCreateRequest(
            handle=handle,
            name=name,
            spend_type=spend_type,
            subcategory=subcategory,
            customer=customer,
            extra_tags=dict(extra_tags or {}),
        )
        return await self._sdk.request_model(
            "PUT",
            _project_path(handle),
            ProjectResponse,
            json=request.model_dump(mode="json"),
        )

    async def delete(self, handle: str) -> ProjectResponse:
        """Delete an unused project; Tracker rejects projects with run history."""
        return await self._sdk.request_model(
            "DELETE",
            _project_path(handle),
            ProjectResponse,
        )
