from collections.abc import Mapping, Sequence
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends
from sqlmodel import Session, col, func, select

from tracker.database.models import (
    Dataset,
    DatasetVersion,
    DatasetVersionTask,
    DatasetTag,
)
from tracker.database.session import get_session
from tracker.services.datasets.digest import version_digest
from tracker.services.datasets.metric_script import METRIC_FILE
from tracker.services.datasets.version_ref import LATEST, VersionRef
from tracker.exceptions import AmbiguousDigestError, TagExistsError
from tracker.types import VersionManifest


def _info(manifest: VersionManifest | None) -> dict[str, Any] | None:
    if manifest is None:
        return None
    return manifest.model_dump(mode="json", exclude={"tasks", "files"})


def _source_name(manifest: VersionManifest | None, task_id: str) -> str | None:
    return None if manifest is None else manifest.tasks[task_id].name


class DatasetVersionRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def publish(
        self,
        dataset_id: UUID,
        tasks: Mapping[str, tuple[str, int]],
        tags: Sequence[str],
        created_by_email: str | None,
        manifest: VersionManifest | None = None,
        metric_digest: str | None = None,
    ) -> tuple[DatasetVersion, bool]:
        self._lock(dataset_id)
        digest = version_digest(
            {task_id: task_digest for task_id, (task_digest, _) in tasks.items()},
            {} if metric_digest is None else {METRIC_FILE: metric_digest},
        )
        version = self._session.exec(
            select(DatasetVersion).where(
                col(DatasetVersion.dataset_id) == dataset_id, col(DatasetVersion.digest) == digest
            )
        ).first()
        created = version is None
        if version is None:
            latest = self._session.exec(
                select(func.max(DatasetVersion.revision)).where(col(DatasetVersion.dataset_id) == dataset_id)
            ).one()
            version = DatasetVersion(
                dataset_id=dataset_id,
                revision=(latest or 0) + 1,
                digest=digest,
                task_count=len(tasks),
                metric_digest=metric_digest,
                manifest=_info(manifest),
                created_by_email=created_by_email,
            )
            self._session.add(version)
            self._session.flush()
            self._session.add_all(
                DatasetVersionTask(
                    version_id=version.id,
                    task_id=task_id,
                    digest=task_digest,
                    size_bytes=size,
                    source_name=_source_name(manifest, task_id),
                )
                for task_id, (task_digest, size) in tasks.items()
            )
        elif manifest is not None:
            version.manifest = _info(manifest)
            for row in self.tasks(version.id):
                row.source_name = _source_name(manifest, row.task_id)
        for tag in dict.fromkeys([LATEST, *tags]):
            self._point(dataset_id, tag, version.id)
        self._session.commit()
        return version, created

    def get(self, dataset_id: UUID, ref: VersionRef) -> DatasetVersion | None:
        query = select(DatasetVersion).where(col(DatasetVersion.dataset_id) == dataset_id)
        if ref.kind == "tag":
            query = query.join(DatasetTag, col(DatasetTag.version_id) == col(DatasetVersion.id)).where(
                col(DatasetTag.dataset_id) == dataset_id, col(DatasetTag.tag) == ref.value
            )
            return self._session.exec(query).first()
        if ref.kind == "revision":
            return self._session.exec(query.where(col(DatasetVersion.revision) == int(ref.value))).first()
        matches = self._session.exec(query.where(col(DatasetVersion.digest).startswith(ref.value)).limit(2)).all()
        if len(matches) > 1:
            raise AmbiguousDigestError(ref.value)
        return matches[0] if matches else None

    def list_all(self, dataset_id: UUID) -> list[DatasetVersion]:
        query = (
            select(DatasetVersion)
            .where(col(DatasetVersion.dataset_id) == dataset_id)
            .order_by(col(DatasetVersion.revision).desc())
        )
        return list(self._session.exec(query).all())

    def tags_of(self, version_ids: Sequence[UUID]) -> dict[UUID, list[str]]:
        tags: dict[UUID, list[str]] = {version_id: [] for version_id in version_ids}
        if not version_ids:
            return tags
        rows = self._session.exec(
            select(DatasetTag).where(col(DatasetTag.version_id).in_(version_ids)).order_by(col(DatasetTag.tag))
        ).all()
        for row in rows:
            tags[row.version_id].append(row.tag)
        return tags

    def tasks(self, version_id: UUID) -> list[DatasetVersionTask]:
        query = (
            select(DatasetVersionTask)
            .where(col(DatasetVersionTask.version_id) == version_id)
            .order_by(col(DatasetVersionTask.task_id))
        )
        return list(self._session.exec(query).all())

    def tasks_page(self, version_id: UUID, after: str | None, limit: int) -> list[DatasetVersionTask]:
        query = select(DatasetVersionTask).where(col(DatasetVersionTask.version_id) == version_id)
        if after is not None:
            query = query.where(col(DatasetVersionTask.task_id) > after)
        return list(self._session.exec(query.order_by(col(DatasetVersionTask.task_id)).limit(limit)).all())

    def task(self, version_id: UUID, task_id: str) -> DatasetVersionTask | None:
        return self._session.get(DatasetVersionTask, (version_id, task_id))

    def metric_digest(self, dataset_id: UUID, revision: int) -> str | None:
        version = self.get(dataset_id, VersionRef("revision", str(revision)))
        return None if version is None else version.metric_digest

    def task_digests(self, dataset_id: UUID, revision: int) -> dict[str, str] | None:
        version = self.get(dataset_id, VersionRef("revision", str(revision)))
        if version is None:
            return None
        return {task.task_id: task.digest for task in self.tasks(version.id)}

    def point_tag(self, dataset_id: UUID, tag: str, version_id: UUID, force: bool = False) -> None:
        self._lock(dataset_id)
        self._point(dataset_id, tag, version_id, force)
        self._session.commit()

    def remove_tag(self, dataset_id: UUID, tag: str) -> DatasetVersion | None:
        self._lock(dataset_id)
        row = self._session.get(DatasetTag, (dataset_id, tag))
        if row is None:
            return None
        version = self._session.get(DatasetVersion, row.version_id)
        self._session.delete(row)
        self._session.commit()
        return version

    def _lock(self, dataset_id: UUID) -> None:
        self._session.exec(select(Dataset).where(col(Dataset.id) == dataset_id).with_for_update()).one()

    def _point(self, dataset_id: UUID, tag: str, version_id: UUID, force: bool = False) -> None:
        row = self._session.get(DatasetTag, (dataset_id, tag))
        if row is None:
            self._session.add(DatasetTag(dataset_id=dataset_id, tag=tag, version_id=version_id))
            return
        if row.version_id != version_id and tag != LATEST and not force:
            raise TagExistsError(tag)
        row.version_id = version_id


DatasetVersionRepositoryDependency = Annotated[DatasetVersionRepository, Depends(DatasetVersionRepository)]
