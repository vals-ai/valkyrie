from collections.abc import Sequence
from datetime import datetime
from functools import cache
from uuid import UUID

import httpx

from tracker.config import project_service_configuration
from tracker.database.models import Project
from tracker.types import ProjectResponse, ProjectsResponse

UNAVAILABLE_STATUSES = {502, 503, 504}
PROJECT_SERVICE_TIMEOUT = httpx.Timeout(30, connect=2)


class ProjectServiceUnavailableError(Exception):
    pass


class UnavailableAwareTransport(httpx.BaseTransport):
    def __init__(self, inner: httpx.BaseTransport) -> None:
        self._inner = inner

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        try:
            response = self._inner.handle_request(request)
            _ = response.read()
        except httpx.TransportError as error:
            raise ProjectServiceUnavailableError(f"{request.method} {request.url.path}: {error!r}") from error
        if response.status_code in UNAVAILABLE_STATUSES:
            response.close()
            raise ProjectServiceUnavailableError(f"{request.method} {request.url.path}: {response.status_code}")
        return response

    def close(self) -> None:
        self._inner.close()


class ProjectServiceClient:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    @classmethod
    def from_environment(cls) -> "ProjectServiceClient":
        url, token = project_service_configuration()
        transport = UnavailableAwareTransport(httpx.HTTPTransport(retries=1))
        return cls(
            httpx.Client(
                base_url=url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=PROJECT_SERVICE_TIMEOUT,
                transport=transport,
            )
        )

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

    def set_gateway_request(self, org_id: UUID, project_id: UUID, request_id: UUID) -> UUID:
        response = self._client.post(
            f"/orgs/{org_id}/projects/{project_id}/gateway-request", json={"gateway_request_id": str(request_id)}
        )
        _ = response.raise_for_status()
        return UUID(response.json()["gateway_request_id"])

    def create(self, project: Project) -> Project | None:
        response = self._client.post(
            f"/orgs/{project.org_id}/projects", json=project.model_dump(mode="json", exclude={"org_id"})
        )
        if response.status_code == 409:
            return None
        _ = response.raise_for_status()
        return Project.model_validate(response.json() | {"org_id": project.org_id})

    def save(self, project: Project) -> None:
        fields = {"name", "spend_type", "subcategory", "customer", "extra_tags"}
        path = f"/orgs/{project.org_id}/projects/{project.id}"
        _ = self._client.put(path, json=project.model_dump(mode="json", include=fields)).raise_for_status()

    def set_archived(self, project: Project, archived_at: datetime | None) -> None:
        path = f"/orgs/{project.org_id}/projects/{project.id}"
        if archived_at is None:
            _ = self._client.post(f"{path}/unarchive").raise_for_status()
        else:
            _ = self._client.post(f"{path}/archive", json={"archived_at": archived_at.isoformat()}).raise_for_status()

    def delete(self, project: Project) -> None:
        response = self._client.post(f"/orgs/{project.org_id}/projects/{project.id}/delete")
        if response.status_code != 404:
            _ = response.raise_for_status()

    def list_rows(self, org_id: UUID, include_archived: bool, include_deleted: bool = False) -> list[Project]:
        params = {"include_archived": include_archived, "include_deleted": include_deleted}
        response = self._client.get(f"/orgs/{org_id}/projects", params=params)
        _ = response.raise_for_status()
        return [Project.model_validate(row | {"org_id": org_id}) for row in response.json()["projects"]]

    def get(self, org_id: UUID, project_id: UUID) -> Project | None:
        response = self._client.get(f"/orgs/{org_id}/projects/{project_id}")
        if response.status_code == 404:
            return None
        _ = response.raise_for_status()
        return Project.model_validate(response.json() | {"org_id": org_id})


@cache
def project_service() -> ProjectServiceClient:
    return ProjectServiceClient.from_environment()
