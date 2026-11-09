from typing import Annotated, Literal
from uuid import UUID

from fastapi import Depends
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, func, select

from tracker.database.session import get_session
from tracker.database.models import Project, RunProject
from tracker.exceptions import ProjectHandleTakenError, ProjectNameTakenError


class ProjectRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def create(self, project: Project) -> Project:
        org_id, name, handle = project.org_id, project.name, project.handle
        self._reject_taken_name(org_id, name, project.id)
        self._session.add(project)
        try:
            self._session.commit()
        except IntegrityError as error:
            self._session.rollback()
            self._reject_taken_name(org_id, name, project.id)
            raise ProjectHandleTakenError(handle) from error
        return project

    def update(self, project: Project) -> Project:
        org_id, name, project_id = project.org_id, project.name, project.id
        self._reject_taken_name(org_id, name, project_id)
        self._session.add(project)
        try:
            self._session.commit()
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
        self._session.commit()
        return project

    def delete(self, project: Project) -> bool:
        if self._session.exec(select(RunProject).where(col(RunProject.project_id) == project.id).limit(1)).first():
            return False
        self._session.delete(project)
        try:
            self._session.commit()
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
        return self._session.exec(query).first()

    def get_for_run(self, benchmark_id: UUID) -> Project | None:
        return self._session.exec(
            select(Project)
            .join(RunProject, col(RunProject.project_id) == col(Project.id))
            .where(col(RunProject.benchmark_id) == benchmark_id)
            .order_by(col(RunProject.id).desc())
            .limit(1)
        ).first()

    def lock(self, project_id: UUID) -> Project:
        return self._session.exec(select(Project).where(col(Project.id) == project_id).with_for_update(read=True)).one()

    def list_all(self, org_id: UUID, include_archived: bool = False) -> list[Project]:
        query = select(Project).where(col(Project.org_id) == org_id).order_by(col(Project.handle))
        if not include_archived:
            query = query.where(col(Project.archived_at).is_(None))
        return list(self._session.exec(query).all())


ProjectRepositoryDependency = Annotated[ProjectRepository, Depends(ProjectRepository)]
