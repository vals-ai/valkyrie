import asyncio
from concurrent.futures import ThreadPoolExecutor
from collections.abc import AsyncIterator, Callable, Iterator
from datetime import UTC, datetime, timedelta
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import re
from uuid import UUID, uuid4
from zipfile import ZIP_STORED

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Engine, event, text
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from main import app
from tracker.auth import RequestIdentity, get_current_identity, get_current_org, get_current_starter
from tracker.database.models import BenchmarkDefinition, Dataset, DatasetUpload, Org
from tracker.database.session import get_session
from tracker.domain import DatasetUploadStatus
from tracker.exceptions import DatasetUploadConflictError, ResourceArchivedError
from tracker.repositories.benchmark_definition import BenchmarkDefinitionRepository
from tracker.repositories.dataset import DatasetRepository
from tracker.repositories.dataset_upload import DatasetUploadRepository
from tracker.repositories.dataset_version import DatasetVersionRepository
from tracker.types import DatasetUploadResponse, VersionCreateRequest
from tracker.local.storage import FilesystemObjectStore
from tracker.local import config as local_config
from tracker.local.resources import LocalResources
from tracker.runtime.artifacts import dataset_blob_key, dataset_file_key, dataset_upload_key
import tracker.dataset_worker as dataset_worker
from tracker.dataset_worker import clean_uploads, run_claimed_upload, run_worker
from tests.unit.datasets.support import TASK, archive_digest_of, zip_bytes

_OWNER = "user:author"
_SHA256 = "a" * 64
_TASK_DIGEST = "sha256:" + "b" * 64
_REQUEST = VersionCreateRequest(tasks={"task-a": _TASK_DIGEST}, tags=["reviewed"])
_STORED_SIZES = {_TASK_DIGEST: 100}


@pytest.fixture
def dataset_id(postgres_session: Session) -> UUID:
    org = Org(name="uploads-org")
    postgres_session.add(org)
    postgres_session.commit()
    benchmark = BenchmarkDefinitionRepository(postgres_session).create(org.id, "bench", "", None)
    return DatasetRepository(postgres_session).create(benchmark.id, "validation", "", None).id


@pytest.fixture
def upload_identity(postgres_session: Session, dataset_id: UUID) -> RequestIdentity:
    dataset = postgres_session.get(Dataset, dataset_id)
    assert dataset is not None
    benchmark = postgres_session.get(BenchmarkDefinition, dataset.benchmark_definition_id)
    assert benchmark is not None
    org = postgres_session.get(Org, benchmark.org_id)
    assert org is not None
    postgres_session.commit()

    return RequestIdentity(org=org, user_id="author")


@pytest.fixture
async def upload_client(
    postgres_engine: Engine,
    upload_identity: RequestIdentity,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[AsyncClient, FilesystemObjectStore]]:
    def request_session() -> Iterator[Session]:
        with Session(postgres_engine, expire_on_commit=False) as session:
            yield session

    monkeypatch.setitem(app.dependency_overrides, get_session, request_session)
    monkeypatch.setitem(app.dependency_overrides, get_current_identity, lambda: upload_identity)
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: upload_identity.org)
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: upload_identity)
    monkeypatch.setattr(local_config, "resources", LocalResources(data_root=tmp_path))
    root = tmp_path / "orgs" / str(upload_identity.org.id)
    objects = FilesystemObjectStore(root / "objects", root / "staging")
    client = AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost")
    try:
        yield client, objects
    finally:
        await client.aclose()


@pytest.fixture
async def upload_worker(
    postgres_engine: Engine,
    upload_client: tuple[AsyncClient, FilesystemObjectStore],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[None]:
    configuration = tmp_path / "worker.json"
    configuration.write_text(json.dumps({"data_root": str(tmp_path)}))
    monkeypatch.setenv("DATABASE_URL", postgres_engine.url.render_as_string(hide_password=False))
    monkeypatch.setattr(dataset_worker, "_POLL_SECONDS", 0.05)
    worker = asyncio.create_task(run_worker(postgres_engine, configuration))
    try:
        yield
    finally:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass


async def _wait_status(client: AsyncClient, path: str, status: DatasetUploadStatus) -> None:
    async with asyncio.timeout(60):
        while True:
            response = await client.get(path)
            assert response.status_code == 200, response.text
            upload = DatasetUploadResponse.model_validate(response.json())
            if upload.status == status:
                return
            assert upload.status not in (DatasetUploadStatus.FAILED, DatasetUploadStatus.EXPIRED), upload.error
            await asyncio.sleep(0.05)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[datetime]:
    moments = [datetime(2026, 11, 16, tzinfo=UTC)]

    def now() -> datetime:
        return moments[0]

    monkeypatch.setattr("tracker.repositories.dataset_upload._now", now)
    return moments


def _create(repository: DatasetUploadRepository, dataset_id: UUID) -> DatasetUpload:
    return repository.create_upload(dataset_id, uuid4(), _OWNER, _SHA256, 100, "author@example.test")


def _ready(
    repository: DatasetUploadRepository, dataset_id: UUID, request: VersionCreateRequest = _REQUEST
) -> DatasetUpload:
    upload = _create(repository, dataset_id)
    repository.queue_upload(dataset_id, upload.id, _OWNER)
    claimed = repository.claim_upload()
    assert claimed is not None and claimed.lease_id is not None
    return repository.complete_upload(claimed.id, claimed.lease_id, request, {_TASK_DIGEST: 100})


@pytest.mark.description("Upload jobs retain their identity and reviewed version across sessions and worker restarts.")
class TestDatasetUploads:
    @pytest.mark.description(
        "Confirmation publishes reviewed tasks with their current stored sizes. Missing content can be restored before retrying."
    )
    @pytest.mark.parametrize("preexisting", [False, True])
    @pytest.mark.usefixtures("upload_worker")
    async def test_archive_to_published_version(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        dataset_id: UUID,
        preexisting: bool,
        upload_client: tuple[AsyncClient, FilesystemObjectStore],
    ) -> None:
        repository = DatasetUploadRepository(postgres_session)
        client, objects = upload_client
        base = "/v1/benchmarks/bench/datasets/validation"
        existing = zip_bytes(TASK, compression=ZIP_STORED)
        if preexisting:
            await objects.put_bytes(dataset_blob_key(str(dataset_id), archive_digest_of(existing)), existing)
        content = zip_bytes(
            {f"{task_id}/{name}": value for task_id in ("task-a", "task-b") for name, value in TASK.items()}
        )
        upload_id = uuid4()
        upload_path = f"{base}/uploads/{upload_id}"
        intent = {"archive_sha256": hashlib.sha256(content).hexdigest(), "archive_size": len(content)}
        created = await client.put(upload_path, json=intent)
        assert created.status_code == 200, created.text
        assert (await client.put(upload_path, json=intent)).json() == created.json()
        assert (await client.post(f"{upload_path}/form")).status_code == 501
        assert (await client.post(f"{upload_path}/complete")).status_code == 404
        assert (await client.get(f"{upload_path}/tasks")).status_code == 409
        await objects.put_bytes(dataset_upload_key(str(dataset_id), str(upload_id)), content)
        queued = await client.post(f"{upload_path}/complete")
        assert queued.status_code == 200, queued.text
        assert queued.json()["status"] == "QUEUED"
        await _wait_status(client, upload_path, DatasetUploadStatus.READY)
        postgres_session.expire_all()
        ready = repository.get_upload(dataset_id, upload_id, _OWNER)
        assert ready is not None and ready.review_digest is not None
        assert ready.status == DatasetUploadStatus.READY
        assert DatasetVersionRepository(postgres_session).list_all(dataset_id) == []
        async with asyncio.timeout(10):
            while await objects.exists(dataset_upload_key(str(dataset_id), str(upload_id))):
                await asyncio.sleep(0.05)
        reviewed = await client.get(upload_path)
        assert reviewed.status_code == 200
        assert reviewed.headers["cache-control"] == "private, no-store"
        assert reviewed.json()["task_count"] == 2
        assert reviewed.json()["review_digest"] == ready.review_digest
        task_page = await client.get(f"{upload_path}/tasks", params={"limit": 1})
        assert task_page.status_code == 200
        assert task_page.json()["tasks"][0]["task_id"] == "task-a"
        assert task_page.json()["next_cursor"] == "task-a"
        second_page = (await client.get(f"{upload_path}/tasks", params={"limit": 1, "cursor": "task-a"})).json()
        assert second_page["tasks"][0]["task_id"] == "task-b"
        assert second_page["next_cursor"] is None
        assert (await client.post(f"{upload_path}/form")).status_code == 409
        assert (
            await client.post(f"{upload_path}/publish", json={"review_digest": "sha256:" + "0" * 64})
        ).status_code == 409
        confirmation = {"review_digest": ready.review_digest}
        if preexisting:
            key = dataset_blob_key(str(dataset_id), archive_digest_of(existing))
            assert await objects.get_bytes(key) == existing
            await objects.delete(key)

            assert (await client.post(f"{upload_path}/publish", json=confirmation)).status_code == 200
            await _wait_status(client, upload_path, DatasetUploadStatus.READY)
            recovered = (await client.get(upload_path)).json()
            assert "not stored" in recovered["error"]
            assert recovered["published_version_digest"] is None
            assert DatasetVersionRepository(postgres_session).list_all(dataset_id) == []

            await objects.put_bytes(key, zip_bytes(TASK))

        confirmed = await client.post(f"{upload_path}/publish", json=confirmation)
        assert confirmed.status_code == 200, confirmed.text
        await _wait_status(client, upload_path, DatasetUploadStatus.SUCCEEDED)
        postgres_session.expire_all()
        published = repository.get_upload(dataset_id, upload_id, _OWNER)
        assert published is not None and published.published_version_id is not None
        assert published.status == DatasetUploadStatus.SUCCEEDED
        assert (await client.get(upload_path)).json()["published_version_digest"] == ready.review_digest
        assert (await client.get(f"{base}/versions/{ready.review_digest}")).status_code == 200
        tasks = DatasetVersionRepository(postgres_session).tasks(published.published_version_id)
        assert [(task.task_id, task.digest) for task in tasks] == [
            (task_id, archive_digest_of(zip_bytes(TASK))) for task_id in ("task-a", "task-b")
        ]
        stored = await objects.get_bytes(dataset_blob_key(str(dataset_id), tasks[0].digest))
        assert archive_digest_of(stored) == tasks[0].digest
        assert len(stored) == tasks[0].size_bytes

    @pytest.mark.description(
        "Upload status, history, and archive cleanup read the summary without loading the task map."
    )
    async def test_summary_queries(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        dataset_id: UUID,
        upload_client: tuple[AsyncClient, FilesystemObjectStore],
    ) -> None:
        client, _ = upload_client
        metric_digest = "sha256:" + "c" * 64
        request = VersionCreateRequest(
            tasks={"one": _TASK_DIGEST, "two": _TASK_DIGEST}, tags=["reviewed"], metric_digest=metric_digest
        )
        repository = DatasetUploadRepository(postgres_session)
        upload_id = _ready(repository, dataset_id, request).id
        base = "/v1/benchmarks/bench/datasets/validation/uploads"
        review_reads: list[str] = []

        def record_query(_conn: object, _cursor: object, statement: str, *_args: object) -> None:
            if re.search(r"\bdataset_upload\.review\b", statement):
                review_reads.append(statement)

        event.listen(postgres_engine, "before_cursor_execute", record_query)
        try:
            status = await client.get(f"{base}/{upload_id}")
            history = await client.get(base)
            assert status.status_code == history.status_code == 200
            assert history.json()["uploads"] == [status.json()]
            assert status.json()["task_count"] == 2
            assert status.json()["total_task_bytes"] == 200
            assert status.json()["metric_digest"] == metric_digest
            assert status.json()["tags"] == ["reviewed"]
            assert [row.id for row in repository.claim_upload_cleanup()] == [upload_id]
            repository.finish_upload_cleanup(upload_id)
            assert review_reads == []

            tasks = await client.get(f"{base}/{upload_id}/tasks")
            assert tasks.status_code == 200
            assert [task["task_id"] for task in tasks.json()["tasks"]] == ["one", "two"]
            assert len(review_reads) == 1
        finally:
            event.remove(postgres_engine, "before_cursor_execute", record_query)

    @pytest.mark.description("Upload routes hide another caller's or organization's uploads before storage access.")
    @pytest.mark.parametrize("scope", ["user", "organization"])
    async def test_http_scope(
        self,
        upload_client: tuple[AsyncClient, FilesystemObjectStore],
        upload_identity: RequestIdentity,
        monkeypatch: pytest.MonkeyPatch,
        scope: str,
    ) -> None:
        client, _ = upload_client
        base = "/v1/benchmarks/bench/datasets/validation/uploads"
        path = f"{base}/{uuid4()}"
        assert (await client.put(path, json={"archive_sha256": _SHA256, "archive_size": 100})).status_code == 200
        other = replace(upload_identity, user_id="other")
        if scope == "organization":
            other = replace(other, org=Org(id=uuid4(), name="other"))
        monkeypatch.setitem(app.dependency_overrides, get_current_identity, lambda: other)
        monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: other.org)
        monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: other)

        assert (await client.get(path)).status_code == 404
        assert (await client.get(f"{path}/tasks")).status_code == 404
        for action in ("form", "complete", "cancel"):
            assert (await client.post(f"{path}/{action}")).status_code == 404
        assert (await client.post(f"{path}/publish", json={"review_digest": _TASK_DIGEST})).status_code == 404
        listing = await client.get(base)
        if scope == "organization":
            assert listing.status_code == 404
        else:
            assert listing.json() == {"uploads": []}

    @pytest.mark.description(
        "A replaced archive or an unsupported metric fails validation before task or metric blobs are stored."
    )
    @pytest.mark.parametrize("rejection", ["checksum", "Custom scoring"])
    async def test_worker_rejects_archive(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        dataset_id: UUID,
        rejection: str,
        upload_client: tuple[AsyncClient, FilesystemObjectStore],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _, objects = upload_client
        configuration = tmp_path / "worker.json"
        configuration.write_text(json.dumps({"data_root": str(tmp_path)}))
        monkeypatch.setenv("DATABASE_URL", postgres_engine.url.render_as_string(hide_password=False))
        repository = DatasetUploadRepository(postgres_session)
        monkeypatch.setenv("METRIC_RUNNER_FUNCTION", "")
        metric = b"def score(rewards):\n    return sum(rewards)\n"
        content = zip_bytes({"metric.py": metric, **{f"task-a/{name}": value for name, value in TASK.items()}})
        checksum = _SHA256 if rejection == "checksum" else hashlib.sha256(content).hexdigest()
        upload_id = repository.create_upload(dataset_id, uuid4(), _OWNER, checksum, len(content), None).id
        key = dataset_upload_key(str(dataset_id), str(upload_id))
        await objects.put_bytes(key, content)
        repository.queue_upload(dataset_id, upload_id, _OWNER)
        claimed = repository.claim_upload()
        assert claimed is not None

        await run_claimed_upload(postgres_engine, claimed, configuration)
        postgres_session.expire_all()
        failed = repository.get_upload(dataset_id, upload_id, _OWNER)
        assert failed is not None and failed.status == DatasetUploadStatus.FAILED
        assert failed.error is not None and rejection in failed.error
        assert not await objects.exists(dataset_blob_key(str(dataset_id), archive_digest_of(zip_bytes(TASK))))
        assert not await objects.exists(
            dataset_file_key(str(dataset_id), "sha256:" + hashlib.sha256(metric).hexdigest())
        )
        assert DatasetVersionRepository(postgres_session).list_all(dataset_id) == []
        await clean_uploads(postgres_engine)
        assert not await objects.exists(key)
        postgres_session.expire_all()
        assert failed.archive_cleanup_at is None
        assert repository.claim_upload() is None

    @pytest.mark.description("A child that fails during startup retries its upload at most three times.")
    async def test_worker_startup_failure_is_bounded(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        dataset_id: UUID,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATABASE_URL", postgres_engine.url.render_as_string(hide_password=False))
        repository = DatasetUploadRepository(postgres_session)
        upload_id = _create(repository, dataset_id).id
        repository.queue_upload(dataset_id, upload_id, _OWNER)
        for attempt in range(1, 4):
            claimed = repository.claim_upload()
            assert claimed is not None and claimed.attempts == attempt
            await run_claimed_upload(postgres_engine, claimed, tmp_path / "missing-config.yaml")
            postgres_session.expire_all()
        failed = repository.get_upload(dataset_id, upload_id, _OWNER)
        assert failed is not None and failed.status == DatasetUploadStatus.FAILED
        assert failed.error == "The archive worker stopped unexpectedly"
        assert repository.claim_upload() is None

    @pytest.mark.description(
        "Cleanup skips active processing and recovers an abandoned cleanup claim after one minute."
    )
    def test_cleanup_claim_recovery(self, postgres_session: Session, dataset_id: UUID, clock: list[datetime]) -> None:
        repository = DatasetUploadRepository(postgres_session)
        upload_id = _create(repository, dataset_id).id
        assert repository.claim_upload_cleanup() == []
        repository.queue_upload(dataset_id, upload_id, _OWNER)
        assert repository.claim_upload_cleanup() == []
        assert repository.claim_upload() is not None
        assert repository.claim_upload_cleanup() == []
        repository.cancel_upload(dataset_id, upload_id, _OWNER)
        assert [row.id for row in repository.claim_upload_cleanup()] == [upload_id]
        assert repository.claim_upload_cleanup() == []
        clock[0] += timedelta(seconds=61)
        assert [row.id for row in repository.claim_upload_cleanup()] == [upload_id]
        repository.finish_upload_cleanup(upload_id)
        clock[0] += timedelta(seconds=61)
        assert repository.claim_upload_cleanup() == []

    @pytest.mark.description(
        "A recovered upload publishes only its reviewed content and repeated confirmation creates no new version."
    )
    def test_recovery_and_publication(self, postgres_engine: Engine, dataset_id: UUID) -> None:
        with Session(postgres_engine) as session:
            repository = DatasetUploadRepository(session)
            upload = _create(repository, dataset_id)
            upload_id = upload.id
            repeated = repository.create_upload(dataset_id, upload_id, _OWNER, _SHA256, 100, "renamed@example.test")
            assert repeated.id == upload_id
            assert repeated.created_by_email == "author@example.test"
            repository.queue_upload(dataset_id, upload_id, _OWNER)

        with Session(postgres_engine) as session:
            repository = DatasetUploadRepository(session)
            recovered = repository.get_upload(dataset_id, upload_id, _OWNER)
            assert recovered is not None and recovered.status == DatasetUploadStatus.QUEUED
            claimed = repository.claim_upload()
            assert claimed is not None and claimed.lease_id is not None
            repository.complete_upload(upload_id, claimed.lease_id, _REQUEST, {_TASK_DIGEST: 100}, ["Review this task"])
            assert repository.claim_upload() is None

        with Session(postgres_engine) as session:
            repository = DatasetUploadRepository(session)
            recovered = repository.get_upload(dataset_id, upload_id, _OWNER)
            assert recovered is not None and recovered.review_digest is not None
            assert recovered.warnings == ["Review this task"]
            with pytest.raises(DatasetUploadConflictError, match="does not match"):
                repository.request_publication(dataset_id, upload_id, _OWNER, "sha256:" + "0" * 64)
            session.rollback()
            digest = recovered.review_digest
            assert digest is not None
            repository.request_publication(dataset_id, upload_id, _OWNER, digest)
            repository.request_publication(dataset_id, upload_id, _OWNER, digest)
            claimed = repository.claim_upload()
            assert claimed is not None and claimed.lease_id is not None
            assert claimed.status == DatasetUploadStatus.PUBLISHING
            version = repository.publish_upload(upload_id, claimed.lease_id, _REQUEST, _STORED_SIZES)
            assert version.digest == digest
            assert version.task_count == 1
            assert version.created_by_email == "author@example.test"
            assert (
                repository.request_publication(dataset_id, upload_id, _OWNER, digest).published_version_id == version.id
            )
            assert repository.claim_upload() is None
            versions = DatasetVersionRepository(session)
            assert len(versions.list_all(dataset_id)) == 1
            assert versions.tags_of([version.id])[version.id] == ["latest", "reviewed"]

            newer, _ = versions.publish(dataset_id, {"task-b": (_TASK_DIGEST, 100)}, [], None)
            repository.request_publication(dataset_id, upload_id, _OWNER, digest)
            assert repository.claim_upload() is None
            assert versions.tags_of([version.id, newer.id]) == {version.id: ["reviewed"], newer.id: ["latest"]}

    @pytest.mark.description("An upload cannot be read, cancelled, or reused by another owner or dataset.")
    def test_scope_and_immutable_intent(self, postgres_session: Session, dataset_id: UUID) -> None:
        repository = DatasetUploadRepository(postgres_session)
        upload_id = _create(repository, dataset_id).id
        assert repository.get_upload(dataset_id, upload_id, "user:other") is None
        assert repository.get_upload(uuid4(), upload_id, _OWNER) is None
        assert repository.list_uploads(dataset_id, "user:other") == []
        with pytest.raises(DatasetUploadConflictError, match="not found"):
            repository.cancel_upload(dataset_id, upload_id, "user:other")
        postgres_session.rollback()
        with pytest.raises(DatasetUploadConflictError, match="another archive"):
            repository.create_upload(dataset_id, upload_id, _OWNER, "c" * 64, 100, None)
        postgres_session.rollback()
        assert repository.list_uploads(dataset_id, _OWNER)[0].archive_sha256 == _SHA256

    @pytest.mark.description(
        "An expired worker cannot finish or renew a claim after another worker recovers the upload."
    )
    def test_stale_worker(self, postgres_engine: Engine, dataset_id: UUID, clock: list[datetime]) -> None:
        with Session(postgres_engine, expire_on_commit=False) as first, Session(postgres_engine) as second:
            original = DatasetUploadRepository(first)
            recovered = DatasetUploadRepository(second)
            upload_id = _create(original, dataset_id).id
            original.queue_upload(dataset_id, upload_id, _OWNER)
            claimed = original.claim_upload()
            assert claimed is not None and claimed.lease_id is not None
            old_lease = claimed.lease_id
            assert recovered.claim_upload() is None
            clock[0] += timedelta(seconds=121)
            new_claim = recovered.claim_upload()
            assert new_claim is not None and new_claim.lease_id is not None
            assert new_claim.lease_id != old_lease
            assert new_claim.attempts == 2
            with pytest.raises(DatasetUploadConflictError, match="no longer owns"):
                original.complete_upload(upload_id, old_lease, _REQUEST, {_TASK_DIGEST: 100})
            first.rollback()
            assert not original.renew_upload(upload_id, old_lease)
            recovered.complete_upload(upload_id, new_claim.lease_id, _REQUEST, {_TASK_DIGEST: 100})
            first.expire_all()
            ready = original.get_upload(dataset_id, upload_id, _OWNER)
            assert ready is not None and ready.status == DatasetUploadStatus.READY

    @pytest.mark.description("Heartbeat renewal retains a claim, but three abandoned claims end processing.")
    def test_heartbeat_and_retry_limit(
        self, postgres_session: Session, dataset_id: UUID, clock: list[datetime]
    ) -> None:
        repository = DatasetUploadRepository(postgres_session)
        upload_id = _create(repository, dataset_id).id
        repository.queue_upload(dataset_id, upload_id, _OWNER)
        claimed = repository.claim_upload()
        assert claimed is not None and claimed.lease_id is not None
        lease_id = claimed.lease_id
        clock[0] += timedelta(seconds=100)
        assert repository.renew_upload(upload_id, lease_id)
        clock[0] += timedelta(seconds=30)
        assert repository.claim_upload() is None
        for attempt in (2, 3):
            clock[0] += timedelta(seconds=121)
            claimed = repository.claim_upload()
            assert claimed is not None and claimed.attempts == attempt
        clock[0] += timedelta(seconds=121)
        assert repository.claim_upload() is None
        assert repository.expire_uploads() == 1
        failed = repository.get_upload(dataset_id, upload_id, _OWNER)
        assert failed is not None and failed.status == DatasetUploadStatus.FAILED

    @pytest.mark.description("Regular heartbeats cannot extend archive processing past its total time limit.")
    def test_processing_deadline(self, postgres_session: Session, dataset_id: UUID, clock: list[datetime]) -> None:
        repository = DatasetUploadRepository(postgres_session)
        upload_id = _create(repository, dataset_id).id
        repository.queue_upload(dataset_id, upload_id, _OWNER)
        claimed = repository.claim_upload()
        assert claimed is not None and claimed.lease_id is not None
        lease_id = claimed.lease_id
        for _ in range(17):
            clock[0] += timedelta(seconds=100)
            assert repository.renew_upload(upload_id, lease_id)
        clock[0] += timedelta(seconds=100)
        assert not repository.renew_upload(upload_id, lease_id)
        assert repository.expire_uploads() == 1
        assert repository.claim_upload() is None

    @pytest.mark.description("Cancellation and expiry prevent workers from publishing unfinished uploads.")
    def test_cancellation_and_expiry(self, postgres_session: Session, dataset_id: UUID, clock: list[datetime]) -> None:
        repository = DatasetUploadRepository(postgres_session)
        upload_id = _create(repository, dataset_id).id
        repository.queue_upload(dataset_id, upload_id, _OWNER)
        claimed = repository.claim_upload()
        assert claimed is not None and claimed.lease_id is not None
        lease_id = claimed.lease_id
        assert repository.cancel_upload(dataset_id, upload_id, _OWNER).status == DatasetUploadStatus.CANCELLED
        assert not repository.renew_upload(upload_id, lease_id)
        ready_id = _ready(repository, dataset_id).id
        clock[0] += timedelta(hours=24, seconds=1)
        assert repository.expire_uploads() == 1
        expired = repository.get_upload(dataset_id, ready_id, _OWNER)
        assert expired is not None and expired.review_digest is not None
        with pytest.raises(DatasetUploadConflictError, match="not ready"):
            repository.request_publication(dataset_id, ready_id, _OWNER, expired.review_digest)
        postgres_session.rollback()
        assert repository.claim_upload() is None

    @pytest.mark.description(
        "A locked publication can outlast its lease, but reaching the processing deadline or upload expiry rolls it back."
    )
    @pytest.mark.parametrize("limit", ["lease", "processing_deadline", "expires_at"])
    def test_publication_time_limits(
        self,
        postgres_session: Session,
        postgres_engine: Engine,
        dataset_id: UUID,
        clock: list[datetime],
        wait_for_database_lock: Callable[[], None],
        limit: str,
    ) -> None:
        repository = DatasetUploadRepository(postgres_session)
        ready = _ready(repository, dataset_id)
        assert ready.review_digest is not None
        upload_id, digest = ready.id, ready.review_digest
        repository.request_publication(dataset_id, upload_id, _OWNER, digest)
        claimed = repository.claim_upload()
        assert claimed is not None and claimed.lease_id is not None
        lease_id = claimed.lease_id
        if limit == "expires_at":
            claimed.expires_at = clock[0] + timedelta(minutes=3)
            postgres_session.commit()

        def publish() -> str:
            with Session(postgres_engine) as session:
                return (
                    DatasetUploadRepository(session).publish_upload(upload_id, lease_id, _REQUEST, _STORED_SIZES).digest
                )

        with ThreadPoolExecutor(1) as workers:
            with postgres_session.begin():
                postgres_session.connection().execute(
                    text("SELECT id FROM dataset WHERE id = :id FOR UPDATE"), {"id": dataset_id}
                )
                publication = workers.submit(publish)
                wait_for_database_lock()
                clock[0] += timedelta(minutes=30 if limit == "processing_deadline" else 3)
                with Session(postgres_engine) as other_worker:
                    assert DatasetUploadRepository(other_worker).claim_upload() is None
            if limit == "lease":
                assert publication.result(timeout=10) == digest
            else:
                with pytest.raises(DatasetUploadConflictError, match="deadline"):
                    publication.result(timeout=10)
        postgres_session.expire_all()
        saved = repository.get_upload(dataset_id, upload_id, _OWNER)
        assert saved is not None
        if limit == "lease":
            assert saved.status == DatasetUploadStatus.SUCCEEDED and saved.published_version_id is not None
        else:
            assert saved.status == DatasetUploadStatus.PUBLISHING and saved.published_version_id is None
            assert DatasetVersionRepository(postgres_session).list_all(dataset_id) == []

    @pytest.mark.description("A database failure while recording success rolls back the version and its tags.")
    def test_publication_transaction(self, postgres_session: Session, dataset_id: UUID) -> None:
        repository = DatasetUploadRepository(postgres_session)
        ready = _ready(repository, dataset_id)
        assert ready.review_digest is not None
        upload_id, digest = ready.id, ready.review_digest
        repository.request_publication(dataset_id, upload_id, _OWNER, digest)
        claimed = repository.claim_upload()
        assert claimed is not None and claimed.lease_id is not None
        lease_id = claimed.lease_id
        postgres_session.connection().execute(
            text("ALTER TABLE dataset_upload ADD CONSTRAINT reject_success CHECK (status <> 'SUCCEEDED')")
        )
        postgres_session.commit()
        with pytest.raises(IntegrityError):
            repository.publish_upload(upload_id, lease_id, _REQUEST, _STORED_SIZES)
        postgres_session.rollback()
        assert DatasetVersionRepository(postgres_session).list_all(dataset_id) == []
        recovered = repository.get_upload(dataset_id, upload_id, _OWNER)
        assert recovered is not None and recovered.status == DatasetUploadStatus.PUBLISHING
        postgres_session.connection().execute(text("ALTER TABLE dataset_upload DROP CONSTRAINT reject_success"))
        postgres_session.commit()
        version = repository.publish_upload(upload_id, lease_id, _REQUEST, _STORED_SIZES)
        assert version.revision == 1
        assert version.digest == digest

    @pytest.mark.description("Workers claim queued uploads once even when several workers poll together.")
    def test_concurrent_claims(self, postgres_engine: Engine, postgres_session: Session, dataset_id: UUID) -> None:
        repository = DatasetUploadRepository(postgres_session)
        expected: set[UUID] = set()
        for _ in range(3):
            upload_id = _create(repository, dataset_id).id
            repository.queue_upload(dataset_id, upload_id, _OWNER)
            expected.add(upload_id)
        with pytest.raises(DatasetUploadConflictError, match="three active"):
            _create(repository, dataset_id)
        postgres_session.rollback()

        def claim(_number: int) -> UUID | None:
            with Session(postgres_engine) as session:
                upload = DatasetUploadRepository(session).claim_upload()
                return None if upload is None else upload.id

        with ThreadPoolExecutor(4) as workers:
            actual = list(workers.map(claim, range(4)))
        assert actual.count(None) == 1
        assert {upload_id for upload_id in actual if upload_id is not None} == expected

    @pytest.mark.description("Archiving a dataset after review prevents publication and leaves a recoverable review.")
    def test_archive_after_review(self, postgres_session: Session, dataset_id: UUID) -> None:
        repository = DatasetUploadRepository(postgres_session)
        ready = _ready(repository, dataset_id)
        assert ready.review_digest is not None
        upload_id = ready.id
        repository.request_publication(dataset_id, upload_id, _OWNER, ready.review_digest)
        claimed = repository.claim_upload()
        assert claimed is not None and claimed.lease_id is not None
        lease_id = claimed.lease_id
        postgres_session.connection().execute(
            text("UPDATE dataset SET archived_at = NOW() WHERE id = :id"), {"id": dataset_id}
        )
        postgres_session.commit()
        with pytest.raises(ResourceArchivedError):
            repository.publish_upload(upload_id, lease_id, _REQUEST, _STORED_SIZES)
        postgres_session.rollback()
        repository.fail_upload(upload_id, lease_id, "Restore the dataset before publication")
        recovered = repository.get_upload(dataset_id, upload_id, _OWNER)
        assert recovered is not None and recovered.status == DatasetUploadStatus.READY
        assert DatasetVersionRepository(postgres_session).list_all(dataset_id) == []
