from uuid import UUID, uuid4

import pytest
from sqlmodel import Session, select

from executor_protocol import ExecutorDispatchStatus
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorRelease,
    ExecutorReleaseStatus,
    Org,
    Task,
    TaskStatus,
)
from tracker.executor.maintenance_control import MaintenanceOwnershipError, begin_maintenance, finish_maintenance
from tracker.executor.release_control import MaintenanceModeError, select_active_release
from tracker.types import AgentContractRequest, AWSBenchmarkArguments


def _release(release_id: str = "maintenance-release") -> ExecutorRelease:
    return ExecutorRelease(
        id=release_id,
        artifact_uri=f"s3://artifacts/{release_id}.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        status=ExecutorReleaseStatus.ACTIVE,
    )


def _benchmark(org_id: UUID, name: str) -> Benchmark:
    return Benchmark(
        id=uuid4(),
        org_id=org_id,
        name=name,
        status=BenchmarkStatus.IN_PROGRESS,
        arguments=AWSBenchmarkArguments(
            contract=AgentContractRequest(name=f"{name}-agent", install_cmd="true", run_cmd="true"),
            concurrency=1,
        ),
        current_execution_release_id="maintenance-release",
    )


@pytest.mark.description("Beginning maintenance stops all work and closes admission.")
def test_begin_maintenance_globally_stops_work_and_closes_admission(control_postgres_session: Session) -> None:
    release = _release()
    control_postgres_session.add(release)
    admission = control_postgres_session.get(ExecutorAdmission, 1)
    assert admission is not None
    admission.release_id = release.id
    control_postgres_session.add(admission)

    second_org = Org(id=uuid4(), name="maintenance-second-org")
    control_postgres_session.add(second_org)
    default_org = control_postgres_session.exec(select(Org).where(Org.name == "default")).one()
    benchmarks = [
        _benchmark(default_org.id, "first"),
        _benchmark(second_org.id, "second"),
    ]
    for benchmark in benchmarks:
        control_postgres_session.add(benchmark)
        control_postgres_session.add(
            Task(
                org_id=benchmark.org_id,
                benchmark=benchmark.id,
                task_id=f"{benchmark.name}-task",
                status=TaskStatus.IN_PROGRESS,
            )
        )
        control_postgres_session.add(
            ExecutorDispatch(
                benchmark_id=benchmark.id,
                kind=ExecutorDispatchKind.START,
                status=ExecutorDispatchStatus.RUNNING,
                executor_release_id=release.id,
                executor_artifact_uri=release.artifact_uri,
                executor_artifact_digest=release.artifact_digest,
                executor_protocol_version=release.protocol_version,
            )
        )
    control_postgres_session.commit()

    summary = begin_maintenance(control_postgres_session, target_sha="a" * 40)
    control_postgres_session.commit()

    assert summary.benchmarks == 2
    assert summary.tasks == 2
    assert summary.dispatches == 2
    assert all(benchmark.status == BenchmarkStatus.STOPPED for benchmark in benchmarks)
    assert all(benchmark.finished_at is not None for benchmark in benchmarks)
    assert all(task.status == TaskStatus.STOPPED for task in control_postgres_session.exec(select(Task)).all())
    assert all(
        dispatch.status == ExecutorDispatchStatus.FAILED
        for dispatch in control_postgres_session.exec(select(ExecutorDispatch)).all()
    )
    with pytest.raises(MaintenanceModeError, match="maintenance is in progress"):
        select_active_release(control_postgres_session, for_update=True)


@pytest.mark.description("Finishing maintenance needs the current target.")
def test_finish_maintenance_requires_current_target(control_postgres_session: Session) -> None:
    admission = control_postgres_session.get(ExecutorAdmission, 1)
    assert admission is not None
    admission.maintenance_target_sha = "a" * 40
    control_postgres_session.add(admission)
    control_postgres_session.commit()

    with pytest.raises(MaintenanceOwnershipError, match="does not own"):
        finish_maintenance(control_postgres_session, target_sha="b" * 40)
    control_postgres_session.rollback()

    finish_maintenance(control_postgres_session, target_sha="a" * 40)
    control_postgres_session.commit()

    admission = control_postgres_session.get(ExecutorAdmission, 1)
    assert admission is not None
    assert admission.maintenance_target_sha is None


@pytest.mark.description("Beginning maintenance again for the same target changes nothing.")
def test_begin_maintenance_is_idempotent_for_same_target(control_postgres_session: Session) -> None:
    first = begin_maintenance(control_postgres_session, target_sha="a" * 40)
    control_postgres_session.commit()
    second = begin_maintenance(control_postgres_session, target_sha="a" * 40)

    assert first == second
    admission = control_postgres_session.get(ExecutorAdmission, 1)
    assert admission is not None
    assert admission.maintenance_target_sha == "a" * 40


@pytest.mark.description("Beginning maintenance for a different target is rejected.")
def test_begin_maintenance_rejects_a_different_target(control_postgres_session: Session) -> None:
    begin_maintenance(control_postgres_session, target_sha="a" * 40)
    control_postgres_session.commit()

    with pytest.raises(MaintenanceOwnershipError, match="Another executor maintenance owner"):
        begin_maintenance(control_postgres_session, target_sha="b" * 40)
    control_postgres_session.rollback()

    admission = control_postgres_session.get(ExecutorAdmission, 1)
    assert admission is not None
    assert admission.maintenance_target_sha == "a" * 40
