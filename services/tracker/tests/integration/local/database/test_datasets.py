import threading
from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError
from sqlmodel import Session

from tracker.database.models import Org
from tracker.exceptions import BenchmarkNameTakenError, DatasetNameTakenError
from tracker.repositories import benchmark_definition
from tracker.repositories.benchmark_definition import BenchmarkDefinitionRepository
from tracker.repositories.dataset import DatasetRepository


@pytest.mark.description("Benchmark names are unique within an org, and archived benchmarks keep their names.")
def test_benchmark_names_are_unique_per_org_and_archived_ones_keep_theirs(postgres_session: Session) -> None:
    first_org = Org(name="first-org")
    second_org = Org(name="second-org")
    postgres_session.add_all([first_org, second_org])
    postgres_session.commit()
    repository = BenchmarkDefinitionRepository(postgres_session)

    repository.create(first_org.id, "shared", "", None)
    repository.create(second_org.id, "shared", "", None)

    with pytest.raises(BenchmarkNameTakenError):
        repository.create(first_org.id, "shared", "", None)

    assert repository.archive(first_org.id, "shared") is not None

    with pytest.raises(BenchmarkNameTakenError):
        repository.create(first_org.id, "shared", "", None)

    assert repository.list_all(first_org.id) == []
    assert [benchmark.name for benchmark in repository.list_all(first_org.id, include_archived=True)] == ["shared"]
    assert [benchmark.name for benchmark in repository.list_all(second_org.id)] == ["shared"]


@pytest.mark.description("Dataset names are unique within a benchmark, and archived datasets keep their names.")
def test_dataset_names_are_unique_per_benchmark_and_archived_ones_keep_theirs(postgres_session: Session) -> None:
    org = Org(name="datasets-org")
    postgres_session.add(org)
    postgres_session.commit()
    benchmarks = BenchmarkDefinitionRepository(postgres_session)
    first = benchmarks.create(org.id, "first", "", None)
    second = benchmarks.create(org.id, "second", "", None)
    repository = DatasetRepository(postgres_session)

    repository.create(first.id, "shared", "", None)
    repository.create(second.id, "shared", "", None)

    with pytest.raises(DatasetNameTakenError):
        repository.create(first.id, "shared", "", None)

    assert repository.archive(first.id, "shared") is not None

    with pytest.raises(DatasetNameTakenError):
        repository.create(first.id, "shared", "", None)

    assert repository.list_all(first.id) == []
    assert [dataset.name for dataset in repository.list_all(first.id, include_archived=True)] == ["shared"]
    assert [dataset.name for dataset in repository.list_all(second.id)] == ["shared"]


@pytest.mark.description("Archiving a benchmark locks it before checking for datasets.")
def test_archiving_locks_the_benchmark_before_it_checks_for_datasets(
    postgres_engine: Engine, postgres_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    org = Org(name="locks-org")
    postgres_session.add(org)
    postgres_session.commit()
    BenchmarkDefinitionRepository(postgres_session).create(org.id, "bench", "", None)
    checked, release = threading.Event(), threading.Event()

    def pause_between_the_check_and_the_update() -> datetime:
        checked.set()
        release.wait(10)
        return datetime.now(UTC)

    monkeypatch.setattr(benchmark_definition, "_now", pause_between_the_check_and_the_update)

    def archive() -> None:
        with Session(postgres_engine) as archiver:
            BenchmarkDefinitionRepository(archiver).archive(org.id, "bench")

    archiver_thread = threading.Thread(target=archive)
    archiver_thread.start()
    assert checked.wait(10)

    try:
        with Session(postgres_engine) as writer:
            writer.connection().exec_driver_sql("SET LOCAL lock_timeout = '200ms'")

            with pytest.raises(OperationalError, match="lock timeout"):
                BenchmarkDefinitionRepository(writer).get(org.id, "bench", "share")
    finally:
        release.set()
        archiver_thread.join()
