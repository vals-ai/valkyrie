from collections.abc import Iterator
from datetime import UTC, datetime
from hashlib import sha256
from itertools import count
import json
from typing import Annotated
from uuid import UUID

import httpx
from fastapi import Depends
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, select

from tracker.clients.projects import ProjectServiceUnavailableError, project_service
from tracker.database.session import get_session
from tracker.database.models import Benchmark, Project, ProjectBackfill, ProjectCutover, RunProject, SpendType
from tracker.logging import get_logger
from tracker.exceptions import ProjectHandleTakenError, ProjectMigrationConflictError, ProjectNameTakenError
from tracker.repositories.advisory_lock import advisory_lock_key


DEFAULT_PROJECT_HANDLE = "default"
DEFAULT_PROJECT_NAME = "Default"
logger = get_logger(__name__)
PROJECT_COPY_FIELDS = {
    "id",
    "org_id",
    "handle",
    "name",
    "spend_type",
    "subcategory",
    "customer",
    "extra_tags",
    "gateway_request_id",
    "gateway_key_id",
    "created_at",
    "created_by_email",
    "archived_at",
}


def _project_values(project: Project) -> dict[str, object]:
    values = project.model_dump(mode="json", include=PROJECT_COPY_FIELDS)
    for field in ("created_at", "archived_at"):
        value = getattr(project, field)
        if isinstance(value, datetime):
            values[field] = (value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)).isoformat()

    return values


def project_hash(project: Project) -> str:
    return sha256(json.dumps(_project_values(project), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


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

    def set_gateway_key(self, project: Project, key_id: UUID) -> UUID:
        try:
            return self._service.set_gateway_key(project.org_id, project.id, key_id)
        except httpx.HTTPError as error:
            raise ProjectServiceUnavailableError() from error

    def set_gateway_request(self, project: Project, request_id: UUID) -> None:
        try:
            project.gateway_request_id = self._service.set_gateway_request(project.org_id, project.id, request_id)
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

    def list_all(self, org_id: UUID, include_archived: bool = False, include_deleted: bool = False) -> list[Project]:
        return self._service.list_rows(org_id, include_archived, include_deleted)

    def list_every(self) -> list[Project]:
        query = select(Project).order_by(col(Project.org_id), col(Project.handle))
        return list(self._session.exec(query).all())

    def backfill(self, *, reconcile: bool = False) -> tuple[int, int]:
        existing: dict[UUID, set[UUID]] = {}
        baselines = {row.project_id: row for row in self._session.exec(select(ProjectBackfill)).all()}
        projects = self.list_every()
        source_ids = {project.id for project in projects}
        copied = 0
        skipped = 0
        for project in projects:
            source_hash = project_hash(project)
            concurrent_insert = False
            if project.org_id not in existing:
                existing[project.org_id] = {
                    row.id
                    for row in self._service.list_rows(project.org_id, include_archived=True, include_deleted=True)
                }
            if project.id not in existing[project.org_id]:
                if project.id not in baselines:
                    baselines[project.id] = self._record_baseline(project, source_hash)
                if self._service.create(project) is not None:
                    existing[project.org_id].add(project.id)
                    baselines[project.id] = self._record_baseline(project, source_hash, refresh=True, confirmed=True)
                    copied += 1
                    continue
                concurrent_insert = True
                existing[project.org_id] = {
                    row.id
                    for row in self._service.list_rows(project.org_id, include_archived=True, include_deleted=True)
                }
                if project.id not in existing[project.org_id]:
                    if reconcile:
                        self._migration_conflict(
                            project.id, "conflicts with a different project in the project service"
                        )
                    logger.warning(
                        "Project %s conflicts with a different service project, skipping startup copy", project.id
                    )
                    skipped += 1
                    continue
            baseline = baselines.get(project.id)
            if (
                baseline is not None
                and baseline.confirmed
                and not concurrent_insert
                and (not reconcile or source_hash == baseline.source_hash)
            ):
                skipped += 1
                continue
            current = self._service.get(project.org_id, project.id)
            if baseline is None:
                if current is not None and project_hash(current) == source_hash:
                    baselines[project.id] = self._record_baseline(project, source_hash, confirmed=True)
                else:
                    logger.warning(
                        "Project %s has pre-existing service divergence and no backfill baseline", project.id
                    )
                skipped += 1
                continue
            if current is not None and project_hash(current) == source_hash:
                if concurrent_insert or not baseline.confirmed:
                    skipped += 1
                baselines[project.id] = self._record_baseline(project, source_hash, refresh=True, confirmed=True)
                logger.info("Recorded already-applied legacy edit for project %s", project.id)
                continue
            if not reconcile:
                logger.warning("Project %s has service divergence and retains its backfill baseline", project.id)
                skipped += 1
                continue
            if current is None:
                fields = ["deleted_at"]
            else:
                source = _project_values(project)
                service = _project_values(current)
                fields = sorted(field for field in PROJECT_COPY_FIELDS if source[field] != service[field])
            self._migration_conflict(
                project.id, f"requires manual reconciliation of a legacy edit; differing fields: {', '.join(fields)}"
            )
        if reconcile:
            for project_id, baseline in baselines.items():
                if project_id in source_ids:
                    continue
                current = self._service.get(baseline.org_id, project_id)
                if current is None:
                    skipped += 1
                    continue
                self._migration_conflict(
                    project_id, "requires manual reconciliation of a legacy deletion; differing fields: deleted_at"
                )
        return copied, skipped

    def finish_startup_backfill(self) -> None:
        project_id = self._session.exec(
            select(Project.id)
            .outerjoin(ProjectBackfill, col(Project.id) == col(ProjectBackfill.project_id))
            .where(col(ProjectBackfill.confirmed).is_not(True))
            .limit(1)
        ).first()
        if project_id is None:
            return
        if self._session.get(ProjectCutover, 1) is not None:
            logger.warning("Project %s remains unconfirmed after completed project cutover", project_id)
            return
        self._migration_conflict(project_id, "has no confirmed project service backfill baseline")

    def complete_cutover(self) -> None:
        if self._session.get(ProjectCutover, 1) is not None:
            return
        self._session.add(ProjectCutover())
        try:
            self._session.commit()
        except IntegrityError:
            self._session.rollback()
            if self._session.get(ProjectCutover, 1) is None:
                raise

    def _record_baseline(
        self, project: Project, source_hash: str, *, refresh: bool = False, confirmed: bool = False
    ) -> ProjectBackfill:
        baseline = self._session.get(ProjectBackfill, project.id)
        if baseline is not None and not refresh:
            return baseline
        if baseline is None:
            baseline = ProjectBackfill(
                project_id=project.id, org_id=project.org_id, source_hash=source_hash, confirmed=confirmed
            )
        else:
            baseline.source_hash = source_hash
            baseline.confirmed = confirmed
            baseline.copied_at = datetime.now(UTC)
        self._session.add(baseline)
        try:
            self._session.commit()
        except IntegrityError:
            self._session.rollback()
            saved = self._session.get(ProjectBackfill, project.id)
            if saved is None:
                raise
            return saved
        return baseline

    def _migration_conflict(self, project_id: UUID, reason: str) -> None:
        message = f"Project {project_id} {reason}"
        logger.error(message)
        raise ProjectMigrationConflictError(message)


ProjectRepositoryDependency = Annotated[ProjectRepository, Depends(ProjectRepository)]
