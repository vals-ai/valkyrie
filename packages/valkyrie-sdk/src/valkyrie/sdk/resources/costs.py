"""Spend recorded against the organization's runs."""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from uuid import UUID

from valkyrie.sdk.models.costs import CostsByProjectResponse, ProjectModelCosts, RunCosts

if TYPE_CHECKING:
    from valkyrie.sdk.client import ValkyrieClient


class CostsResource:
    def __init__(self, client: ValkyrieClient) -> None:
        self._sdk = client

    async def by_project(self, start: date | datetime, end: date | datetime) -> CostsByProjectResponse:
        """List spend per month, vendor and project for costs that ended in the `[start, end)` window.

        A date is a UTC day, and a datetime must carry a timezone. `runs` lists every run alive during
        the window or holding a cost in it, with its project.
        """
        return await self._sdk.request_model(
            "GET",
            "/costs/by-project",
            CostsByProjectResponse,
            params={"start": start.isoformat(), "end": end.isoformat()},
        )

    async def project_model(self, project: str, start: date | datetime, end: date | datetime) -> ProjectModelCosts:
        """Return one project's model spend per UTC month for its runs placed in the `[start, end)` window.

        A run counts in the month it finished, or started if it has not finished. `unattributed_usd` is
        the project key's all-time spend outside any run or on runs in no project, and `through` is when
        the usage data ends.
        """
        return await self._sdk.request_model(
            "GET",
            "/costs/project-model",
            ProjectModelCosts,
            params={"project": project, "start": start.isoformat(), "end": end.isoformat()},
        )

    async def run(self, run_id: UUID | str) -> RunCosts:
        """Return one run's spend in total, per cost type and per provider, resource and unit.

        Model spend from the Gateway appears as the `model` cost type. When the Gateway is
        unavailable it is left out and `model_spend_available` is false.
        """
        return await self._sdk.request_model("GET", f"/benchmarks/{run_id}/costs", RunCosts)
