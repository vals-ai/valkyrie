from collections.abc import Iterator
from itertools import count
from typing import Annotated, Literal
from uuid import UUID

import httpx
from fastapi import Depends
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, func, select

from tracker.clients.projects import project_service_reader, project_service_writer
from tracker.database.session import get_session
from tracker.database.models import Benchmark, Org, Project, RunProject, SpendType
from tracker.exceptions import ProjectHandleTakenError, ProjectNameTakenError, ProjectServiceUnavailableError


DEFAULT_PROJECT_HANDLE = "default"
DEFAULT_PROJECT_NAME = "Default"


def _default_names() -> Iterator[str]:
    yield DEFAULT_PROJECT_NAME
    yield f"{DEFAULT_PROJECT_NAME} ({DEFAULT_PROJECT_HANDLE})"
    for number in count(2):
        yield f"{DEFAULT_PROJECT_NAME} ({DEFAULT_PROJECT_HANDLE} {number})"


class ProjectRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session
        self._writer = project_service_writer()
        self._reader = project_service_reader()

    def _commit(self, org_id: UUID) -> None:
        if self._writer is not None:
            try:
                self._session.flush()
                _ = self._session.exec(
                    select(Org.id).where(col(Org.id) == org_id).with_for_update(key_share=True)
                ).one()
                for row in list(self._session.identity_map.values()):
                    if isinstance(row, Project):
                        self._session.expire(row)
                self._writer.replace(org_id, self._rows(org_id, include_archived=True))
            except httpx.HTTPError as error:
                self._session.rollback()
                raise ProjectServiceUnavailableError() from error
        self._session.commit()

    def create(self, project: Project) -> Project:
        org_id, name, handle = project.org_id, project.name, project.handle
        self._reject_taken_name(org_id, name, project.id)
        self._session.add(project)
        try:
            self._commit(project.org_id)
        except IntegrityError as error:
            self._session.rollback()
            self._reject_taken_name(org_id, name, project.id)
            raise ProjectHandleTakenError(handle) from error
        return project

    def get_or_create(self, project: Project) -> Project:
        existing = self.get(project.org_id, project.handle)
        if existing is not None:
            return existing
        self._session.add(project)
        try:
            self._commit(project.org_id)
        except IntegrityError:
            self._session.rollback()
            existing = self.get(project.org_id, project.handle)
            if existing is None:
                raise
            return existing
        self._session.refresh(project)
        return project

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
        org_id, name, project_id = project.org_id, project.name, project.id
        self._reject_taken_name(org_id, name, project_id)
        self._session.add(project)
        try:
            self._commit(project.org_id)
        except IntegrityError:
            self._session.rollback()
            self._reject_taken_name(org_id, name, project_id)
            raise
        return project

    def _reject_taken_name(self, org_id: UUID, name: str, project_id: UUID) -> None:
        holder = self.get_by_name(org_id, name)
        if holder is not None and holder.id != project_id:
            raise ProjectNameTakenError(holder.name, holder.handle)

    def get_by_name(self, org_id: UUID, name: str) -> Project | None:
        query = select(Project).where(
            col(Project.org_id) == org_id, func.lower(func.btrim(Project.name)) == func.lower(func.btrim(name))
        )
        with self._session.no_autoflush:
            return self._session.exec(query).first()

    def save(self, project: Project) -> Project:
        self._session.add(project)
        self._commit(project.org_id)
        return project

    def delete(self, project: Project) -> bool:
        if self._session.exec(select(RunProject).where(col(RunProject.project_id) == project.id).limit(1)).first():
            return False
        self._session.delete(project)
        try:
            self._commit(project.org_id)
        except IntegrityError:
            self._session.rollback()
            return False
        return True

    def get(self, org_id: UUID, handle: str, lock: Literal["share", "update"] | None = None) -> Project | None:
        query = select(Project).where(col(Project.org_id) == org_id, col(Project.handle) == handle)
        if lock is not None:
            query = query.with_for_update(read=lock == "share")
        return self._session.exec(query).first()

    def get_by_id(
        self, org_id: UUID, project_id: UUID, lock: Literal["share", "update"] | None = None
    ) -> Project | None:
        query = select(Project).where(col(Project.org_id) == org_id, col(Project.id) == project_id)
        if lock is not None:
            query = query.with_for_update(read=lock == "share")
        if self._reader is None:
            return self._session.exec(query).first()
        if lock is not None:
            _ = self._session.exec(query).first()
        return self._reader.get(org_id, project_id)

    def set_gateway_key(self, project: Project, key_id: UUID) -> UUID:
        if self._reader is not None:
            key_id = self._reader.set_gateway_key(project.org_id, project.id, key_id)
        row = self._session.exec(select(Project).where(col(Project.id) == project.id)).one()
        row.gateway_key_id = key_id
        _ = self.save(row)
        return key_id

    def find(self, org_id: UUID, handle: str) -> Project | None:
        if self._reader is None:
            return self.get(org_id, handle, "share")
        rows = self._reader.list_rows(org_id, include_archived=True)
        return next((self._reader.get(org_id, row.id) for row in rows if row.handle == handle), None)

    @property
    def reads_from_service(self) -> bool:
        return self._reader is not None

    def get_for_run(self, benchmark_id: UUID, *, strict: bool = False) -> Project | None:
        if self._reader is not None:
            link = self._session.exec(
                select(Benchmark.org_id, RunProject.project_id)
                .join(Benchmark, col(Benchmark.id) == col(RunProject.benchmark_id))
                .where(col(RunProject.benchmark_id) == benchmark_id)
                .order_by(col(RunProject.id).desc())
                .limit(1)
            ).first()
            if link is None:
                return None
            project = self._reader.get(*link)
            if project is None and strict:
                raise LookupError(f"Project {link[1]} is not in the project service")
            return project
        return self._session.exec(
            select(Project)
            .join(RunProject, col(RunProject.project_id) == col(Project.id))
            .where(col(RunProject.benchmark_id) == benchmark_id)
            .order_by(col(RunProject.id).desc())
            .limit(1)
        ).first()

    def get_for_link(self, org_id: UUID, project_id: UUID) -> Project:
        if self._reader is not None:
            project = self._reader.get(org_id, project_id)
            if project is None:
                raise LookupError(f"Project {project_id} is not in the project service")
            return project
        return self._session.exec(select(Project).where(col(Project.id) == project_id).with_for_update(read=True)).one()

    def list_all(self, org_id: UUID, include_archived: bool = False) -> list[Project]:
        if self._reader is not None:
            return self._reader.list_rows(org_id, include_archived)
        return self._rows(org_id, include_archived)

    def _rows(self, org_id: UUID, include_archived: bool) -> list[Project]:
        query = select(Project).where(col(Project.org_id) == org_id).order_by(col(Project.handle))
        if not include_archived:
            query = query.where(col(Project.archived_at).is_(None))
        return list(self._session.exec(query).all())

    def list_every(self) -> list[Project]:
        query = select(Project).order_by(col(Project.org_id), col(Project.handle))
        return list(self._session.exec(query).all())


ProjectRepositoryDependency = Annotated[ProjectRepository, Depends(ProjectRepository)]
