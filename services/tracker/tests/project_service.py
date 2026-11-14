import json
from typing import Any
from uuid import UUID

import httpx

from tracker.database.models import Project


class MockProjectRoutes:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.orgs: dict[str, str] = {}

    def seed(self, *projects: Project) -> None:
        for project in projects:
            self.rows[str(project.id)] = project.model_dump(mode="json", exclude={"org_id"})
            self.orgs[str(project.id)] = str(project.org_id)

    def projects(self) -> list[Project]:
        return [
            Project.model_validate(row | {"org_id": self.orgs[row["id"]]})
            for row in self.rows.values()
            if row.get("deleted_at") is None
        ]

    def project(self, project_id: UUID) -> Project | None:
        return next((project for project in self.projects() if project.id == project_id), None)

    def _taken(self, org: str, row: dict[str, Any], field: str) -> bool:
        value = row[field].strip().lower()
        live = [other for other in self._org_rows(org) if other.get("deleted_at") is None]
        return any(other[field].strip().lower() == value for other in live if other["id"] != row["id"])

    def _org_rows(self, org: str) -> list[dict[str, Any]]:
        return [row for row in self.rows.values() if self.orgs[row["id"]] == org]

    def handle(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.split("/")
        body: dict[str, Any] = json.loads(request.content) if request.content else {}
        if len(parts) == 4 and request.method == "GET":
            archived = request.url.params["include_archived"] == "true"
            deleted = request.url.params.get("include_deleted") == "true"
            listed = [
                row
                for row in self._org_rows(parts[2])
                if (archived or row["archived_at"] is None) and (deleted or row.get("deleted_at") is None)
            ]
            return httpx.Response(200, json={"projects": sorted(listed, key=lambda row: row["handle"])})
        if len(parts) == 4 and request.method != "POST":
            return httpx.Response(405, request=request)
        if len(parts) == 4:
            if self._taken(parts[2], body, "handle") or self._taken(parts[2], body, "name"):
                return httpx.Response(409, request=request)
            self.orgs[body["id"]] = parts[2]
            self.rows[body["id"]] = body
            return httpx.Response(201, json=self.rows[body["id"]])
        row = self.rows.get(parts[4])
        if row is None or self.orgs[parts[4]] != parts[2] or row.get("deleted_at") is not None:
            return httpx.Response(404, request=request)
        if parts[-1] == "delete":
            row["deleted_at"] = "2026-11-03T12:00:00Z"
            return httpx.Response(204)
        if request.method == "PUT":
            if self._taken(parts[2], row | body, "name"):
                return httpx.Response(409, request=request)
            row.update(body)
        elif parts[-1] == "archive":
            row["archived_at"] = row["archived_at"] or body["archived_at"]
        elif parts[-1] == "unarchive":
            row["archived_at"] = None
        elif parts[-1] in {"gateway-key-id", "gateway-request"}:
            (field,) = body
            row[field] = row[field] or body[field]
        return httpx.Response(200, json=row)
