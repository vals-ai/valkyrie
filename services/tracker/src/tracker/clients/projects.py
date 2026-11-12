from collections.abc import Sequence
from functools import cache
from uuid import UUID

import httpx

from tracker.config import project_service_configuration, project_service_reads, project_service_writes
from tracker.database.models import Project
from tracker.types import ProjectResponse, ProjectsResponse


class ProjectServiceClient:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    @classmethod
    def from_environment(cls) -> "ProjectServiceClient":
        url, token = project_service_configuration()
        return cls(httpx.Client(base_url=url, headers={"Authorization": f"Bearer {token}"}, timeout=30))

    def record(self, org_id: UUID, project_id: UUID) -> Project:
        response = self._client.get(f"/orgs/{org_id}/projects/{project_id}")
        _ = response.raise_for_status()
        return Project.model_validate(response.json() | {"org_id": org_id})

    def replace(self, org_id: UUID, projects: Sequence[Project]) -> None:
        body = {"projects": [project.model_dump(mode="json", exclude={"org_id"}) for project in projects]}
        response = self._client.put(f"/orgs/{org_id}/projects", json=body)
        _ = response.raise_for_status()

    def list_projects(self, org_id: UUID) -> list[ProjectResponse]:
        response = self._client.get(f"/orgs/{org_id}/projects", params={"include_archived": True})
        _ = response.raise_for_status()
        return ProjectsResponse.model_validate_json(response.content).projects

    def set_gateway_key(self, org_id: UUID, project_id: UUID, key_id: UUID) -> UUID:
        response = self._client.post(
            f"/orgs/{org_id}/projects/{project_id}/gateway-key-id", json={"gateway_key_id": str(key_id)}
        )
        _ = response.raise_for_status()
        return UUID(response.json()["gateway_key_id"])

    def list_rows(self, org_id: UUID, include_archived: bool) -> list[Project]:
        response = self._client.get(f"/orgs/{org_id}/projects", params={"include_archived": include_archived})
        _ = response.raise_for_status()
        return [Project.model_validate(row | {"org_id": org_id}) for row in response.json()["projects"]]

    def get(self, org_id: UUID, project_id: UUID) -> Project | None:
        response = self._client.get(f"/orgs/{org_id}/projects/{project_id}")
        if response.status_code == 404:
            return None
        _ = response.raise_for_status()
        return Project.model_validate(response.json() | {"org_id": org_id})


@cache
def project_service_writer() -> ProjectServiceClient | None:
    return ProjectServiceClient.from_environment() if project_service_writes() else None


@cache
def project_service_reader() -> ProjectServiceClient | None:
    if not project_service_reads():
        return None
    if not project_service_writes():
        raise ValueError("PROJECT_SERVICE_READS needs PROJECT_SERVICE_WRITES=true")
    return ProjectServiceClient.from_environment()
