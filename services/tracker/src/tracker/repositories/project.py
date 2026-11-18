from collections.abc import Iterator
from itertools import count
from typing import Annotated, Any
from uuid import UUID

import httpx
from fastapi import Depends
from sqlalchemy import text
from sqlmodel import Session, col, select

from tracker.clients.projects import ProjectServiceUnavailableError, project_service
from tracker.database.session import get_session
from tracker.database.models import Benchmark, Project, RunProject, SpendType
from tracker.logging import get_logger
from tracker.exceptions import (
    ProjectHandleTakenError,
    ProjectLimitRequestDecidedError,
    ProjectLimitRequestNotFoundError,
    ProjectNameTakenError,
    ProjectNotFoundError,
)
from tracker.repositories.advisory_lock import advisory_lock_key
from tracker.types import ProjectLimit


DEFAULT_PROJECT_HANDLE = "default"
DEFAULT_PROJECT_NAME = "Default"
logger = get_logger(__name__)


def _default_names() -> Iterator[str]:
    yield DEFAULT_PROJECT_NAME
    yield f"{DEFAULT_PROJECT_NAME} ({DEFAULT_PROJECT_HANDLE})"
    for number in count(2):
        yield f"{DEFAULT_PROJECT_NAME} ({DEFAULT_PROJECT_HANDLE} {number})"


class ProjectRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session
        self._service = project_service()

    def create(self, project: Project) -> Project:
        self._reject_taken_name(project.org_id, project.name, project.id)
        created = self._service.create(project)
        if created is None:
            self._reject_taken_name(project.org_id, project.name, project.id)
            raise ProjectHandleTakenError(project.handle)
        return created

    def get_or_create(self, project: Project) -> Project:
        existing = self.get(project.org_id, project.handle)
        if existing is not None:
            return existing
        existing = self._service.create(project) or self.get(project.org_id, project.handle)
        if existing is None:
            self._reject_taken_name(project.org_id, project.name, project.id)
            raise ProjectHandleTakenError(project.handle)
        return existing

    def get_or_create_default(self, org_id: UUID, created_by_email: str | None) -> Project:
        existing = self.get(org_id, DEFAULT_PROJECT_HANDLE)
        if existing is not None:
            return existing
        name = next(name for name in _default_names() if self.get_by_name(org_id, name) is None)
        return self.get_or_create(
            Project(
                org_id=org_id,
                handle=DEFAULT_PROJECT_HANDLE,
                name=name,
                spend_type=SpendType.DEVELOPMENT,
                subcategory="new_benchmark_generation",
                created_by_email=created_by_email,
            )
        )

    def update(self, project: Project) -> Project:
        self._reject_taken_name(project.org_id, project.name, project.id)
        try:
            self._service.save(project)
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 409:
                self._reject_taken_name(project.org_id, project.name, project.id)
            raise
        return project

    def _reject_taken_name(self, org_id: UUID, name: str, project_id: UUID) -> None:
        holder = self.get_by_name(org_id, name)
        if holder is not None and holder.id != project_id:
            raise ProjectNameTakenError(holder.name, holder.handle)

    def get_by_name(self, org_id: UUID, name: str) -> Project | None:
        wanted = name.strip().lower()
        rows = self._service.list_rows(org_id, include_archived=True)
        return next((row for row in rows if row.name.strip().lower() == wanted), None)

    def save(self, project: Project) -> Project:
        self._service.set_archived(project, project.archived_at)
        return project

    def delete(self, project: Project) -> bool:
        self._lock_links(project.id, shared=False)
        if self._session.exec(select(RunProject).where(col(RunProject.project_id) == project.id).limit(1)).first():
            return False
        self._service.delete(project)
        return True

    def get(self, org_id: UUID, handle: str) -> Project | None:
        rows = self._service.list_rows(org_id, include_archived=True)
        return next((row for row in rows if row.handle == handle), None)

    def resolve(self, org_id: UUID, handle: str) -> Project:
        project = self.get(org_id, handle)
        if project is None:
            raise ProjectNotFoundError(handle)
        return project

    def get_by_id(self, org_id: UUID, project_id: UUID) -> Project | None:
        return self._service.get(org_id, project_id)

    def _lock_links(self, project_id: UUID, *, shared: bool) -> None:
        if self._session.get_bind().dialect.name == "postgresql":
            lock = "pg_advisory_xact_lock_shared" if shared else "pg_advisory_xact_lock"
            self._session.connection().execute(
                text(f"SELECT {lock}(:key)"), {"key": advisory_lock_key(f"project-links:{project_id}")}
            )

    def lock_gateway_key(self, project_id: UUID) -> None:
        if self._session.get_bind().dialect.name == "postgresql":
            self._session.connection().execute(
                text("SELECT pg_advisory_xact_lock(:key)"),
                {"key": advisory_lock_key(f"project-gateway-key:{project_id}")},
            )

    def set_gateway_request(self, project: Project, request_id: UUID) -> None:
        try:
            project.gateway_request_id = self._service.set_gateway_request(project.org_id, project.id, request_id)
        except httpx.HTTPError as error:
            raise ProjectServiceUnavailableError() from error

    def set_limit(self, project: Project, limit: ProjectLimit) -> None:
        try:
            self._service.set_limit(project.org_id, project.id, limit)
        except httpx.HTTPError as error:
            raise ProjectServiceUnavailableError() from error

    def limit_requests(self, project: Project, method: str, path: str = "", body: object = None) -> Any:
        response = self._service.limit_requests(project, method, path, body)
        if response.status_code == 404:
            raise ProjectLimitRequestNotFoundError(response.json()["detail"])
        if response.status_code == 409:
            raise ProjectLimitRequestDecidedError(response.json()["detail"])
        try:
            return response.raise_for_status().json()
        except httpx.HTTPError as error:
            raise ProjectServiceUnavailableError() from error

    def limit_request_queue(self, status: str | None) -> Any:
        try:
            return self._service.limit_request_queue(status).raise_for_status().json()
        except httpx.HTTPError as error:
            raise ProjectServiceUnavailableError() from error

    def find(self, org_id: UUID, handle: str) -> Project | None:
        rows = self._service.list_rows(org_id, include_archived=True)
        return next((self._service.get(org_id, row.id) for row in rows if row.handle == handle), None)

    def get_for_run(self, benchmark_id: UUID, *, strict: bool = False) -> Project | None:
        link = self._session.exec(
            select(Benchmark.org_id, RunProject.project_id)
            .join(Benchmark, col(Benchmark.id) == col(RunProject.benchmark_id))
            .where(col(RunProject.benchmark_id) == benchmark_id)
            .order_by(col(RunProject.id).desc())
            .limit(1)
        ).first()
        if link is None:
            return None
        project = self._service.get(*link)
        if project is None and strict:
            raise LookupError(f"Project {link[1]} is not in the project service")
        return project

    def get_for_link(self, org_id: UUID, project_id: UUID) -> Project:
        self._lock_links(project_id, shared=True)
        project = self._service.get(org_id, project_id)
        if project is None:
            raise LookupError(f"Project {project_id} is not in the project service")
        return project

    def list_all(
        self,
        org_id: UUID,
        include_archived: bool = False,
        include_deleted: bool = False,
        *,
        timeout: httpx.Timeout | None = None,
    ) -> list[Project]:
        return self._service.list_rows(org_id, include_archived, include_deleted, timeout)


ProjectRepositoryDependency = Annotated[ProjectRepository, Depends(ProjectRepository)]
