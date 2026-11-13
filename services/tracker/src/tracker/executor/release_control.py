import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlmodel import Session

from executor_protocol import (
    DEFAULT_EXECUTOR_DISPATCH_CLAIM_TIMEOUT_SECONDS,
    SUPPORTED_PROTOCOL_VERSIONS,
    validate_executor_digest,
)
from tracker.database.models import Benchmark, ExecutorAdmission, ExecutorDispatch, ExecutorRelease
from tracker.domain import BenchmarkStatus, ExecutorDispatchKind, ExecutorReleaseStatus
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.repositories.executor_dispatch import ExecutorDispatchRepository
from tracker.repositories.executor_release import ExecutorReleaseRepository

_ACTIVE_BENCHMARK_STATUSES = (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)
_ARTIFACT_RETENTION_DAYS = 30
_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActiveExecutorReleaseWork:
    dispatches_by_release: dict[str, list[ExecutorDispatch]]
    executions_by_release: dict[str, list[Benchmark]]
    unattributed_executions: list[Benchmark]

    @property
    def counts_by_release(self) -> dict[str, int]:
        release_ids = self.dispatches_by_release.keys() | self.executions_by_release.keys()
        return {
            release_id: len(self.dispatches_by_release.get(release_id, []))
            + len(self.executions_by_release.get(release_id, []))
            for release_id in release_ids
        }


class ReleaseControlError(ValueError):
    pass


class MaintenanceModeError(ReleaseControlError):
    pass


def register_release(session: Session, release: ExecutorRelease) -> ExecutorRelease:
    repository = ExecutorReleaseRepository(session)
    if repository.get(release.id) is not None:
        raise ReleaseControlError(f"Executor release {release.id!r} already exists")
    if release.status != ExecutorReleaseStatus.CANDIDATE:
        raise ReleaseControlError("New executor releases must start as candidates")
    _validate_release_manifest(release)

    repository.save(release)
    session.flush()
    _logger.info(
        "executor_release_lifecycle",
        extra={"event": "registered", "release_id": release.id, "status": release.status.value},
    )
    return release


def activate_release(session: Session, candidate: ExecutorRelease) -> ExecutorRelease:
    _validate_release_manifest(candidate)

    get_executor_admission(session, for_update=True)
    release = ExecutorReleaseRepository(session).get(candidate.id)
    if release is None:
        release = register_release(session, candidate)
    elif (
        release.artifact_uri != candidate.artifact_uri
        or release.artifact_digest != candidate.artifact_digest
        or release.protocol_version != candidate.protocol_version
        or release.task_definition_arn != candidate.task_definition_arn
    ):
        raise ReleaseControlError(f"Executor release {candidate.id!r} already has a different immutable identity")
    if release.status in (ExecutorReleaseStatus.DRAINING, ExecutorReleaseStatus.RETIRED):
        raise ReleaseControlError(f"Executor release {release.id!r} cannot be activated from {release.status.value}")

    activated = promote_release(session, release.id)
    admission = get_executor_admission(session)
    if admission.release_id != activated.id or activated.status != ExecutorReleaseStatus.ACTIVE:
        raise ReleaseControlError(f"Executor release {activated.id!r} did not become active")
    return activated


def pin_benchmark_to_release(benchmark: Benchmark, release: ExecutorRelease) -> None:
    if benchmark.executor_release_id is not None:
        raise ReleaseControlError(f"Benchmark {benchmark.id} already has executor release ownership")

    benchmark.executor_release_id = release.id
    benchmark.current_execution_release_id = release.id
    benchmark.executor_artifact_uri = release.artifact_uri
    benchmark.executor_artifact_digest = release.artifact_digest
    benchmark.executor_protocol_version = release.protocol_version


def create_executor_dispatch(
    benchmark_id: UUID,
    release: ExecutorRelease,
    kind: ExecutorDispatchKind,
    *,
    dispatch_id: UUID,
    task_ids: list[str] | None = None,
) -> ExecutorDispatch:
    created_at = datetime.now(UTC)
    return ExecutorDispatch(
        id=dispatch_id,
        benchmark_id=benchmark_id,
        kind=kind,
        executor_release_id=release.id,
        executor_artifact_uri=release.artifact_uri,
        executor_artifact_digest=release.artifact_digest,
        executor_protocol_version=release.protocol_version,
        created_at=created_at,
        claim_deadline_at=created_at + timedelta(seconds=DEFAULT_EXECUTOR_DISPATCH_CLAIM_TIMEOUT_SECONDS),
        assigned_task_ids=list(task_ids) if task_ids is not None else None,
    )


def active_executor_release_work(session: Session) -> ActiveExecutorReleaseWork:
    active_dispatches = ExecutorDispatchRepository(session).list_active()
    dispatches_by_release: dict[str, list[ExecutorDispatch]] = {}
    active_dispatch_owners: set[tuple[UUID, str]] = set()
    for dispatch in active_dispatches:
        dispatches_by_release.setdefault(dispatch.executor_release_id, []).append(dispatch)
        active_dispatch_owners.add((dispatch.benchmark_id, dispatch.executor_release_id))

    executions_by_release: dict[str, list[Benchmark]] = {}
    unattributed_executions: list[Benchmark] = []
    active_benchmarks = BenchmarkRepository(session).list_by_status(_ACTIVE_BENCHMARK_STATUSES)
    for benchmark in active_benchmarks:
        release_id = benchmark.current_execution_release_id
        if release_id is None:
            unattributed_executions.append(benchmark)
        elif (benchmark.id, release_id) not in active_dispatch_owners:
            executions_by_release.setdefault(release_id, []).append(benchmark)

    return ActiveExecutorReleaseWork(
        dispatches_by_release=dispatches_by_release,
        executions_by_release=executions_by_release,
        unattributed_executions=unattributed_executions,
    )


def _get_open_executor_admission(session: Session, *, for_update: bool) -> ExecutorAdmission:
    admission = ExecutorReleaseRepository(session).admission(for_update=for_update)
    if admission is None:
        raise ReleaseControlError("No active executor release is configured")
    if admission.maintenance_target_sha is not None:
        raise MaintenanceModeError("Executor maintenance is in progress")
    return admission


def lock_executor_admission(session: Session) -> ExecutorAdmission:
    return _get_open_executor_admission(session, for_update=True)


def select_active_release(session: Session, *, for_update: bool = False) -> ExecutorRelease:
    admission = _get_open_executor_admission(session, for_update=for_update)
    if admission.release_id is None:
        raise ReleaseControlError("No active executor release is configured")

    release = ExecutorReleaseRepository(session).get(admission.release_id)
    if release is None or release.status != ExecutorReleaseStatus.ACTIVE:
        raise ReleaseControlError("No active executor release is configured")
    return release


def resolve_current_execution_release(
    session: Session,
    benchmark: Benchmark,
    *,
    for_update: bool = False,
) -> ExecutorRelease:
    release_id = benchmark.current_execution_release_id
    if release_id is None:
        raise ReleaseControlError(f"Benchmark {benchmark.id} has no current executor release")

    release = ExecutorReleaseRepository(session).get(release_id, for_update=for_update)
    if release is None or release.status not in (ExecutorReleaseStatus.ACTIVE, ExecutorReleaseStatus.DRAINING):
        raise ReleaseControlError(f"Current executor release {release_id!r} is unavailable")
    return release


def promote_release(session: Session, release_id: str) -> ExecutorRelease:
    admission = get_executor_admission(session, for_update=True)
    release = _get_release(session, release_id, populate_existing=True, for_update=True)

    if admission.release_id == release_id:
        if release.status != ExecutorReleaseStatus.ACTIVE:
            raise ReleaseControlError("Admission points to a release that is not active")
        return release
    if release.status != ExecutorReleaseStatus.CANDIDATE:
        raise ReleaseControlError(f"Executor release {release_id!r} cannot be promoted from {release.status.value}")

    if admission.release_id is not None:
        previous = _get_release(session, admission.release_id, populate_existing=True)
        if previous.status == ExecutorReleaseStatus.ACTIVE:
            previous.status = ExecutorReleaseStatus.DRAINING
            previous.draining_at = previous.draining_at or datetime.now(UTC)
            _logger.info(
                "executor_release_lifecycle",
                extra={"event": "draining", "release_id": previous.id, "status": previous.status.value},
            )

    release.status = ExecutorReleaseStatus.ACTIVE
    release.activated_at = release.activated_at or datetime.now(UTC)
    release.draining_at = None
    release.retired_at = None

    admission.release_id = release_id
    admission.updated_at = datetime.now(UTC)
    session.flush()
    _logger.info(
        "executor_release_lifecycle",
        extra={"event": "promoted", "release_id": release.id, "status": release.status.value},
    )
    return release


def has_retirable_releases(session: Session) -> bool:
    draining_ids = set(ExecutorReleaseRepository(session).draining_ids())
    if not draining_ids:
        return False
    active_work = active_executor_release_work(session)
    return not active_work.unattributed_executions and bool(draining_ids - active_work.counts_by_release.keys())


def retire_drained_releases(session: Session) -> list[str]:
    admission = get_executor_admission(session, for_update=True)
    draining_releases = ExecutorReleaseRepository(session).lock_draining()
    if admission.release_id in {release.id for release in draining_releases}:
        raise ReleaseControlError("The active admission target cannot be draining")

    active_work = active_executor_release_work(session)
    if active_work.unattributed_executions:
        return []

    busy_release_ids = active_work.counts_by_release
    retired_at = datetime.now(UTC)
    retired_ids: list[str] = []
    for release in draining_releases:
        if release.id in busy_release_ids:
            continue
        release.status = ExecutorReleaseStatus.RETIRED
        release.retired_at = retired_at
        release.artifact_retention_until = retired_at + timedelta(days=_ARTIFACT_RETENTION_DAYS)
        retired_ids.append(release.id)
        _logger.info(
            "executor_release_lifecycle",
            extra={
                "event": "retired",
                "release_id": release.id,
                "status": release.status.value,
                "artifact_retention_until": release.artifact_retention_until.isoformat(),
            },
        )
    session.flush()
    return retired_ids


def artifact_deletion_allowed(session: Session, release_id: str, *, now: datetime | None = None) -> bool:
    release = _get_release(session, release_id)
    if release.status != ExecutorReleaseStatus.RETIRED or release.artifact_retention_until is None:
        return False
    retention_until = release.artifact_retention_until
    if retention_until.tzinfo is None:
        retention_until = retention_until.replace(tzinfo=UTC)
    active_work = active_executor_release_work(session)
    return (
        (now or datetime.now(UTC)) >= retention_until
        and not active_work.unattributed_executions
        and release.id not in active_work.counts_by_release
    )


def get_executor_admission(session: Session, *, for_update: bool = False) -> ExecutorAdmission:
    admission = ExecutorReleaseRepository(session).admission(for_update=for_update)
    if admission is None:
        raise ReleaseControlError("Executor admission state is not initialized")
    return admission


def share_executor_admission(session: Session) -> ExecutorAdmission:
    admission = ExecutorReleaseRepository(session).shared_admission()
    if admission is None:
        raise ReleaseControlError("Executor admission state is not initialized")
    return admission


def _validate_release_manifest(release: ExecutorRelease) -> None:
    try:
        release.artifact_digest = validate_executor_digest(release.artifact_digest)
    except ValueError as error:
        raise ReleaseControlError(str(error)) from error
    if release.protocol_version not in SUPPORTED_PROTOCOL_VERSIONS:
        raise ReleaseControlError(f"Unsupported executor protocol version: {release.protocol_version}")
    if release.task_definition_arn is None and not release.artifact_uri.startswith("source:///"):
        raise ReleaseControlError("Executor artifact URI must use source:///")


def _get_release(
    session: Session,
    release_id: str,
    *,
    populate_existing: bool = False,
    for_update: bool = False,
) -> ExecutorRelease:
    release = ExecutorReleaseRepository(session).get(
        release_id, populate_existing=populate_existing, for_update=for_update
    )
    if release is None:
        raise ReleaseControlError(f"Executor release {release_id!r} does not exist")
    return release
