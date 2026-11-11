from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

import pytest
from sqlalchemy import Engine
from sqlmodel import Session

from tracker.database.models import DatasetVersion, Org
from tracker.services.datasets.version_ref import VersionRef
from tracker.exceptions import AmbiguousDigestError, TagExistsError
from tracker.repositories.benchmark_definition import BenchmarkDefinitionRepository
from tracker.repositories.dataset import DatasetRepository
from tracker.repositories.dataset_version import DatasetVersionRepository

PUBLISHERS = 8


def _digest(number: int) -> str:
    return f"sha256:{number:064x}"


@pytest.fixture
def dataset_id(postgres_session: Session) -> UUID:
    org = Org(name="versions-org")
    postgres_session.add(org)
    postgres_session.commit()
    benchmark = BenchmarkDefinitionRepository(postgres_session).create(org.id, "bench", "", None)
    return DatasetRepository(postgres_session).create(benchmark.id, "verified", "", None).id


def _publish(engine: Engine, dataset_id: UUID, number: int) -> tuple[int, bool]:
    with Session(engine) as session:
        version, created = DatasetVersionRepository(session).publish(
            dataset_id, {"task-a": (_digest(number), 10)}, [], None
        )
        return version.revision, created


@pytest.mark.description("Concurrent publishes of different tasks get distinct consecutive revisions.")
def test_concurrent_publishes_of_different_tasks_get_distinct_consecutive_revisions(
    postgres_engine: Engine, dataset_id: UUID
) -> None:
    with ThreadPoolExecutor(PUBLISHERS) as pool:
        published = list(pool.map(lambda number: _publish(postgres_engine, dataset_id, number), range(PUBLISHERS)))

    assert sorted(revision for revision, _ in published) == list(range(1, PUBLISHERS + 1))
    assert all(created for _, created in published)


@pytest.mark.description("Concurrent publishes of the same tasks make one version.")
def test_concurrent_publishes_of_the_same_tasks_make_one_version(postgres_engine: Engine, dataset_id: UUID) -> None:
    with ThreadPoolExecutor(PUBLISHERS) as pool:
        published = list(pool.map(lambda _: _publish(postgres_engine, dataset_id, 1), range(PUBLISHERS)))

    assert {revision for revision, _ in published} == {1}
    assert sum(created for _, created in published) == 1


@pytest.mark.description(
    "Concurrent publishes and forced tag moves of one tag never deadlock, and a publish can only fail on a tag conflict."
)
def test_concurrent_publishes_and_tag_moves_of_one_tag_only_conflict_without_force(
    postgres_engine: Engine, dataset_id: UUID
) -> None:
    with Session(postgres_engine) as session:
        first, _ = DatasetVersionRepository(session).publish(dataset_id, {"task-a": (_digest(0), 10)}, [], None)
        first_id = first.id

    def publish_with_the_tag(number: int) -> None:
        with Session(postgres_engine) as session:
            DatasetVersionRepository(session).publish(dataset_id, {"task-a": (_digest(number), 10)}, ["rc"], None)

    def move_the_tag(_: int) -> None:
        with Session(postgres_engine) as session:
            DatasetVersionRepository(session).point_tag(dataset_id, "rc", first_id, force=True)

    def remove_the_tag(_: int) -> None:
        with Session(postgres_engine) as session:
            DatasetVersionRepository(session).remove_tag(dataset_id, "rc")

    for round_number in range(4):
        with ThreadPoolExecutor(PUBLISHERS) as pool:
            publishes = [pool.submit(publish_with_the_tag, 100 * round_number + number) for number in range(1, 3)]
            others = [*(pool.submit(move_the_tag, number) for number in range(3)), pool.submit(remove_the_tag, 0)]
            for future in others:
                future.result()
            for future in publishes:
                error = future.exception()
                assert error is None or isinstance(error, TagExistsError)


@pytest.mark.description("A digest prefix shared by two versions is ambiguous.")
def test_a_digest_prefix_shared_by_two_versions_is_ambiguous(postgres_session: Session, dataset_id: UUID) -> None:
    postgres_session.add_all(
        DatasetVersion(dataset_id=dataset_id, revision=number, digest=_digest(number), task_count=1)
        for number in (1, 2)
    )
    postgres_session.commit()
    versions = DatasetVersionRepository(postgres_session)

    with pytest.raises(AmbiguousDigestError):
        versions.get(dataset_id, VersionRef("digest", "sha256:00000000"))

    found = versions.get(dataset_id, VersionRef("digest", _digest(2)))
    assert found is not None
    assert found.revision == 2
