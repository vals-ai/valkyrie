from typing import Any
import errno
import io
import os
import json
import zipfile
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

import httpx
import pytest

from valkyrie.sdk import ValkyrieAPIError, ValkyrieClient, ValkyrieRunError
from valkyrie.sdk.errors import ValkyrieDatasetPushError
from valkyrie.sdk.models import (
    AgentContractRequest,
    AgentReleaseSelection,
    BenchmarkDefinitionResponse,
    BenchmarkDefinitionsResponse,
    DatasetResponse,
    DatasetsResponse,
    HostedDataset,
)
from valkyrie.sdk.resources.datasets import folder_run_name
from valkyrie.sdk.dataset_manifest import ManifestError
from valkyrie.sdk.metric_script import MetricScriptError, metric_digest
from valkyrie.sdk.task_archive import TaskFolderError
from valkyrie.sdk.task_content import archive_digest, task_digest

_BENCHMARK = {
    "name": "swe-bench",
    "description": "SWE-bench tasks",
    "created_by_email": "ada@example.com",
    "created_at": "2026-10-24T04:20:24.675682+00:00",
    "updated_at": "2026-10-24T04:25:00+00:00",
}
_DATASET = {**_BENCHMARK, "name": "verified", "benchmark": "swe-bench"}


@pytest.mark.description(
    "Benchmark resource methods call `/v1/benchmarks` with the name and description and return typed models."
)
async def test_benchmark_requests_use_the_benchmark_routes_and_return_typed_models(
    make_client: Callable[..., ValkyrieClient],
) -> None:
    requests: list[tuple[str, str, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.raw_path.decode(), request.content))
        if (request.method, request.url.path) == ("GET", "/v1/benchmarks"):
            return httpx.Response(200, json={"benchmarks": [_BENCHMARK]})
        return httpx.Response(200, json=_BENCHMARK)

    async with make_client(handler) as client:
        created = await client.benchmark_definitions.create("swe-bench", description="SWE-bench tasks")
        listed = await client.benchmark_definitions.list()
        updated = await client.benchmark_definitions.update("swe-bench", description="SWE-bench family")
        removed = await client.benchmark_definitions.remove("swe-bench")

    expected = BenchmarkDefinitionResponse.model_validate(_BENCHMARK)
    assert created == updated == removed == expected
    assert listed == BenchmarkDefinitionsResponse(benchmarks=[expected])
    assert [(method, path) for method, path, _ in requests] == [
        ("POST", "/v1/benchmarks"),
        ("GET", "/v1/benchmarks"),
        ("PATCH", "/v1/benchmarks/swe-bench"),
        ("DELETE", "/v1/benchmarks/swe-bench"),
    ]
    assert json.loads(requests[0][2]) == {"name": "swe-bench", "description": "SWE-bench tasks"}
    assert json.loads(requests[2][2]) == {"description": "SWE-bench family"}


@pytest.mark.description("Dataset methods call routes under their benchmark and return typed models.")
async def test_dataset_requests_nest_under_their_benchmark_and_return_typed_models(
    make_client: Callable[..., ValkyrieClient],
) -> None:
    requests: list[tuple[str, str, bytes]] = []
    base = "/v1/benchmarks/swe-bench/datasets"

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.raw_path.decode(), request.content))
        if (request.method, request.url.path) == ("GET", base):
            return httpx.Response(200, json={"datasets": [_DATASET]})
        return httpx.Response(200, json=_DATASET)

    async with make_client(handler) as client:
        created = await client.datasets.create("swe-bench", "verified", description="SWE-bench tasks")
        listed = await client.datasets.list("swe-bench")
        fetched = await client.datasets.get("swe-bench", "verified")
        updated = await client.datasets.update("swe-bench", "verified", description="Human-verified")
        removed = await client.datasets.remove("swe-bench", "verified")

    expected = DatasetResponse.model_validate(_DATASET)
    assert created == fetched == updated == removed == expected
    assert expected.benchmark == "swe-bench"
    assert listed == DatasetsResponse(datasets=[expected])
    assert [(method, path) for method, path, _ in requests] == [
        ("POST", base),
        ("GET", base),
        ("GET", f"{base}/verified"),
        ("PATCH", f"{base}/verified"),
        ("DELETE", f"{base}/verified"),
    ]
    assert json.loads(requests[0][2]) == {"name": "verified", "description": "SWE-bench tasks"}
    assert json.loads(requests[3][2]) == {"description": "Human-verified"}


@pytest.mark.description(
    "Archived resources are listed only with `include_archived`, and restore POSTs to the resource's `restore` route."
)
async def test_archived_resources_list_on_request_and_restore_through_their_routes(
    make_client: Callable[..., ValkyrieClient],
) -> None:
    archived = {"archived_at": "2026-10-25T01:00:00+00:00"}
    requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.raw_path.decode()))
        if request.method == "GET" and request.url.path.endswith("/datasets"):
            return httpx.Response(200, json={"datasets": [{**_DATASET, **archived}]})
        if request.method == "GET" and request.url.path == "/v1/benchmarks":
            return httpx.Response(200, json={"benchmarks": [{**_BENCHMARK, **archived}]})
        return httpx.Response(200, json=_DATASET if "/datasets/" in request.url.path else _BENCHMARK)

    async with make_client(handler) as client:
        benchmarks = await client.benchmark_definitions.list(include_archived=True)
        benchmark = await client.benchmark_definitions.get("swe-bench")
        restored_benchmark = await client.benchmark_definitions.restore("swe-bench")
        datasets = await client.datasets.list("swe-bench", include_archived=True)
        restored_dataset = await client.datasets.restore("swe-bench", "verified")

    assert benchmarks.benchmarks[0].archived_at is not None
    assert datasets.datasets[0].archived_at is not None
    assert benchmark.archived_at is restored_benchmark.archived_at is restored_dataset.archived_at is None
    assert requests == [
        ("GET", "/v1/benchmarks?include_archived=true"),
        ("GET", "/v1/benchmarks/swe-bench"),
        ("POST", "/v1/benchmarks/swe-bench/restore"),
        ("GET", "/v1/benchmarks/swe-bench/datasets?include_archived=true"),
        ("POST", "/v1/benchmarks/swe-bench/datasets/verified/restore"),
    ]


@pytest.mark.description("Dataset methods raise for an invalid benchmark or dataset name and send nothing.")
@pytest.mark.parametrize("bad_name", ["..", "../agents", "Upper", "a b", "x" * 65, ""])
async def test_invalid_names_are_rejected_before_any_request(
    make_client: Callable[..., ValkyrieClient], bad_name: str
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_DATASET)

    async with make_client(handler) as client:
        for operation in (
            client.benchmark_definitions.create,
            client.benchmark_definitions.remove,
            client.datasets.list,
        ):
            with pytest.raises(ValueError, match="Invalid benchmark name"):
                await operation(bad_name)
        with pytest.raises(ValueError, match="Invalid benchmark name"):
            await client.benchmark_definitions.update(bad_name, description="x")
        for operation in (client.datasets.create, client.datasets.get, client.datasets.remove):
            with pytest.raises(ValueError, match="Invalid benchmark name"):
                await operation(bad_name, "verified")
            with pytest.raises(ValueError, match="Invalid dataset name"):
                await operation("swe-bench", bad_name)
        with pytest.raises(ValueError, match="Invalid dataset name"):
            await client.datasets.update("swe-bench", bad_name, description="x")

    assert requests == []


_DIGEST = "sha256:" + "ab" * 32
_TASK = {"task_id": "django-11099", "digest": _DIGEST, "size": 7}
_DATASET_URL = "/v1/benchmarks/swe-bench/datasets/verified"
_VERSION = {
    "benchmark": "swe-bench",
    "dataset": "verified",
    "revision": 2,
    "digest": "sha256:" + "cd" * 32,
    "task_count": 1,
    "tags": ["latest", "stable"],
    "created_by_email": "ada@example.com",
    "created_at": "2026-10-24T04:20:24+00:00",
}
_DETAIL = {**_VERSION}


def _with_storage(storage: Callable[[httpx.Request], httpx.Response]) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "dl.test":
            return storage(request)
        return httpx.Response(200, json={**_TASK, "download_url": "https://dl.test/task", "expires_in": 300})

    return handler


@pytest.mark.description("`tasks` reads every page of a version's tasks.")
async def test_tasks_reads_every_page_of_a_versions_tasks(make_client: Callable[..., ValkyrieClient]) -> None:
    pages: dict[str | None, dict[str, Any]] = {
        None: {"tasks": [_TASK], "next_cursor": "django-11099"},
        "django-11099": {"tasks": [{**_TASK, "task_id": "z-1"}], "next_cursor": None},
    }
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"{_DATASET_URL}/versions/stable/tasks"
        seen.append(request.url.params.get("cursor"))
        return httpx.Response(200, json=pages[seen[-1]])

    async with make_client(handler) as client:
        tasks = await client.datasets.versions.tasks("swe-bench", "verified", "stable")

    assert [task.task_id for task in tasks] == ["django-11099", "z-1"]
    assert seen == [None, "django-11099"]


@pytest.mark.description("Version requests nest under their dataset and return typed models.")
async def test_version_requests_nest_under_their_dataset_and_return_typed_models(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    archive = tmp_path / "task.zip"
    archive.write_bytes(b"PKarchive")
    requests: list[tuple[str, str, str | None, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(
            (request.method, request.url.raw_path.decode(), request.headers.get("content-type"), request.content)
        )
        path = request.url.path
        if path.endswith("/blobs/missing"):
            return httpx.Response(200, json={"missing": [_DIGEST]})
        if "/blobs/" in path:
            return httpx.Response(201, json={"digest": _DIGEST, "size": 9})
        if path.endswith("/versions") and request.method == "GET":
            return httpx.Response(200, json={"versions": [_VERSION]})
        if "/tasks/" in path:
            return httpx.Response(200, json={**_TASK, "download_url": "https://dl.test/task", "expires_in": 300})
        if "/tags/" in path:
            return httpx.Response(200, json=_VERSION)
        return httpx.Response(200, json=_DETAIL)

    async with make_client(handler) as client:
        datasets = client.datasets
        versions = datasets.versions
        missing = await datasets._missing("swe-bench", "verified", [_DIGEST])
        stored = await datasets._put_blob("swe-bench", "verified", _DIGEST, archive)
        published = await datasets._publish("swe-bench", "verified", {"django-11099": _DIGEST}, tags=["stable"])
        listed = await versions.list("swe-bench", "verified")
        latest = await versions.get("swe-bench", "verified")
        by_digest = await versions.get("swe-bench", "verified", _VERSION["digest"])
        task = await versions.get_task("swe-bench", "verified", "django-11099", "stable")
        tagged = await versions.tag("swe-bench", "verified", "stable", "2")
        untagged = await versions.untag("swe-bench", "verified", "stable")

    assert missing.missing == [_DIGEST]
    assert (stored.digest, stored.size) == (_DIGEST, 9)
    assert published == latest == by_digest
    assert listed.versions[0].tags == ["latest", "stable"]
    assert task.download_url == "https://dl.test/task"
    assert tagged == untagged == listed.versions[0]
    assert [(method, path) for method, path, _, _ in requests] == [
        ("POST", f"{_DATASET_URL}/blobs/missing"),
        ("PUT", f"{_DATASET_URL}/blobs/{_DIGEST}"),
        ("POST", f"{_DATASET_URL}/versions"),
        ("GET", f"{_DATASET_URL}/versions"),
        ("GET", f"{_DATASET_URL}/versions/latest"),
        ("GET", f"{_DATASET_URL}/versions/{_VERSION['digest']}"),
        ("GET", f"{_DATASET_URL}/versions/stable/tasks/django-11099"),
        ("PUT", f"{_DATASET_URL}/tags/stable"),
        ("DELETE", f"{_DATASET_URL}/tags/stable"),
    ]
    assert json.loads(requests[0][3]) == {"digests": [_DIGEST]}
    assert requests[1][2:] == ("application/zip", b"PKarchive")
    assert json.loads(requests[2][3]) == {"tasks": {"django-11099": _DIGEST}, "tags": ["stable"]}
    assert json.loads(requests[7][3]) == {"ref": "2", "force": False}


@pytest.mark.description(
    "Task download fetches the archive URL without Tracker credentials and leaves only the saved file."
)
async def test_download_saves_the_archive_without_forwarding_tracker_credentials(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    def storage(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "dl.test"
        assert "x-api-key" not in request.headers

        return httpx.Response(200, content=b"archive")

    target = tmp_path / "saved.zip"
    async with make_client(_with_storage(storage)) as client:
        saved = await client.datasets.versions.download("swe-bench", "verified", "django-11099", target)

        with pytest.raises(FileExistsError):
            await client.datasets.versions.download("swe-bench", "verified", "django-11099", target)

    assert saved.read_bytes() == b"archive"
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.description("A symlink at the download target is replaced and the file it pointed to is unchanged.")
async def test_download_never_writes_through_a_planted_symlink(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    def storage(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"archive")

    victim = tmp_path / "victim.txt"
    victim.write_text("keep")
    target = tmp_path / "saved.tar.gz"
    (tmp_path / "saved.tar.gz.part").symlink_to(victim)

    async with make_client(_with_storage(storage)) as client:
        await client.datasets.versions.download("swe-bench", "verified", "django-11099", target)

    assert victim.read_text() == "keep"
    assert target.read_bytes() == b"archive"


@pytest.mark.description("A file created at the target during the transfer is not overwritten by a second copy.")
async def test_download_keeps_a_file_created_while_it_transfers(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    target = tmp_path / "saved.zip"

    def storage(_request: httpx.Request) -> httpx.Response:
        target.write_text("another process")
        return httpx.Response(200, content=b"archive")

    async with make_client(_with_storage(storage)) as client:
        with pytest.raises(FileExistsError, match="use overwrite"):
            await client.datasets.versions.download("swe-bench", "verified", "django-11099", target)
        assert target.read_text() == "another process"
        await client.datasets.versions.download("swe-bench", "verified", "django-11099", target, overwrite=True)

    assert target.read_bytes() == b"archive"
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.description(
    "Without hard-link support the download saves the archive and keeps a file created at the target mid-transfer."
)
async def test_download_publishes_without_hard_links(
    make_client: Callable[..., ValkyrieClient], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    first = tmp_path / "first.zip"
    raced = tmp_path / "raced.zip"

    def storage(_request: httpx.Request) -> httpx.Response:
        if first.exists():
            raced.write_text("another process")
        return httpx.Response(200, content=b"archive")

    def no_hard_links(_source: Path, _target: Path) -> None:
        raise PermissionError(errno.EPERM, "Operation not permitted")

    monkeypatch.setattr(os, "link", no_hard_links)

    async with make_client(_with_storage(storage)) as client:
        await client.datasets.versions.download("swe-bench", "verified", "django-11099", first)
        with pytest.raises(FileExistsError, match="use overwrite"):
            await client.datasets.versions.download("swe-bench", "verified", "django-11099", raced)

    assert first.read_bytes() == b"archive"
    assert raced.read_text() == "another process"
    assert sorted(tmp_path.iterdir()) == [first, raced]


@pytest.mark.description(
    "Without hard-link support a failed rename removes the placeholder so a retry can save the archive."
)
async def test_download_removes_the_placeholder_when_the_rename_fails(
    make_client: Callable[..., ValkyrieClient], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def storage(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"archive")

    def no_hard_links(_source: Path, _target: Path) -> None:
        raise PermissionError(errno.EPERM, "Operation not permitted")

    def busy(_source: Path, _target: Path) -> None:
        raise PermissionError(errno.EACCES, "Permission denied")

    monkeypatch.setattr(os, "link", no_hard_links)
    replace = os.replace
    monkeypatch.setattr(os, "replace", busy)

    target = tmp_path / "saved.zip"
    async with make_client(_with_storage(storage)) as client:
        with pytest.raises(PermissionError):
            await client.datasets.versions.download("swe-bench", "verified", "django-11099", target)
        assert list(tmp_path.iterdir()) == []
        monkeypatch.setattr(os, "replace", replace)
        await client.datasets.versions.download("swe-bench", "verified", "django-11099", target)

    assert target.read_bytes() == b"archive"


@pytest.mark.description("Download creates missing parent folders of the target before saving the archive.")
async def test_download_creates_missing_parent_folders(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    def storage(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"archive")

    target = tmp_path / "tasks" / "verified" / "saved.zip"
    async with make_client(_with_storage(storage)) as client:
        await client.datasets.versions.download("swe-bench", "verified", "django-11099", target)

    assert target.read_bytes() == b"archive"
    assert list(target.parent.iterdir()) == [target]


@pytest.mark.description(
    "A storage error status raises `ValkyrieAPIError` with that status and leaves no file in the folder."
)
async def test_a_failed_download_leaves_no_partial_file(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    def storage(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    async with make_client(_with_storage(storage)) as client:
        with pytest.raises(ValkyrieAPIError) as raised:
            await client.datasets.versions.download("swe-bench", "verified", "django-11099", tmp_path / "saved.tar.gz")

    assert raised.value.status_code == 500

    assert list(tmp_path.iterdir()) == []


@pytest.mark.description("Task methods raise for an invalid task ID and send nothing.")
@pytest.mark.parametrize("bad_id", ["..", "a/b", "-lead", "has space", "x" * 129, ""])
async def test_invalid_task_ids_are_rejected_before_any_request(
    make_client: Callable[..., ValkyrieClient], bad_id: str
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_DETAIL)

    async with make_client(handler) as client:
        with pytest.raises(ValueError, match="Invalid task ID"):
            await client.datasets.versions.get_task("swe-bench", "verified", bad_id)
        with pytest.raises(ValueError, match="Invalid task ID"):
            await client.datasets._publish("swe-bench", "verified", {bad_id: _DIGEST})

    assert requests == []


@pytest.mark.description("Invalid digests are rejected before any request.")
@pytest.mark.parametrize("bad_digest", ["", "sha256:abc", "0" * 64, "sha256:" + "G" * 64, "sha256:" + "AB" * 32])
async def test_invalid_digests_are_rejected_before_any_request(
    make_client: Callable[..., ValkyrieClient], bad_digest: str, tmp_path: Path
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_DETAIL)

    async with make_client(handler) as client:
        datasets = client.datasets
        with pytest.raises(ValueError, match="Invalid digest"):
            await datasets._missing("swe-bench", "verified", [bad_digest])
        with pytest.raises(ValueError, match="Invalid digest"):
            await datasets._publish("swe-bench", "verified", {"task-a": bad_digest})
        with pytest.raises(ValueError, match="Invalid digest"):
            await datasets._put_blob("swe-bench", "verified", bad_digest, tmp_path / "missing.zip")

    assert requests == []


def _write_tasks(root: Path, *names: str) -> Path:
    for name in names:
        files = {"task.toml": "", "instruction.md": f"Do {name}.", "tests/test.sh": "#!/bin/bash\n"}
        for relative, text in files.items():
            (root / name / relative).parent.mkdir(parents=True, exist_ok=True)
            (root / name / relative).write_text(text)
    return root


class _Tracker:
    def __init__(self, *, dataset: dict[str, object] | None = None, refuse: str | None = None) -> None:
        self.dataset = dataset or _DATASET
        self.refuse = refuse
        self.blobs: dict[str, bytes] = {}
        self.files: dict[str, bytes] = {}
        self.published: list[dict[str, object]] = []
        self.requests: list[tuple[str, str]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.requests.append((request.method, path))
        if request.method == "GET":
            return httpx.Response(200, json=self.dataset)
        if "/files/" in path:
            digest = path.rsplit("/", 1)[1]
            self.files[digest] = request.content
            return httpx.Response(201, json={"digest": digest, "size": len(request.content)})
        if path.endswith("/blobs/missing"):
            digests = json.loads(request.content)["digests"]
            return httpx.Response(200, json={"missing": [digest for digest in digests if digest not in self.blobs]})
        if "/blobs/" in path:
            digest = path.rsplit("/", 1)[1]
            if self.refuse and self.refuse in str(zipfile.ZipFile(io.BytesIO(request.content)).read("instruction.md")):
                return httpx.Response(403, json={"detail": "denied"})
            self.blobs[digest] = request.content
            return httpx.Response(201, json={"digest": digest, "size": len(request.content)})
        body = json.loads(request.content)
        self.published.append(body)
        return httpx.Response(201, json={**_VERSION, "task_count": len(body["tasks"])})


@pytest.mark.description("Push uploads each task as a zip of its files and publishes their digests.")
async def test_push_uploads_each_task_as_a_zip_of_its_files_and_publishes_their_digests(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    tracker = _Tracker()
    reported: list[str] = []
    folder = _write_tasks(tmp_path / "tasks", "task-b", "task-a")

    async with make_client(tracker) as client:
        version = await client.datasets.push(
            "swe-bench", "verified", folder, tags=["stable"], on_stored=lambda task_id, _blob: reported.append(task_id)
        )

    (published,) = tracker.published
    assert sorted(published["tasks"]) == ["task-a", "task-b"]
    assert published["tags"] == ["stable"]
    assert version.task_count == 2
    assert sorted(reported) == ["task-a", "task-b"]
    assert sorted(tracker.blobs) == sorted(published["tasks"].values())
    for digest, content in tracker.blobs.items():
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            assert archive.namelist() == ["instruction.md", "task.toml", "tests/test.sh"]
            assert archive_digest(archive) == digest


@pytest.mark.description("Push uploads only the tasks the dataset does not store.")
async def test_push_uploads_only_the_tasks_the_dataset_does_not_store(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    tracker = _Tracker()
    folder = _write_tasks(tmp_path / "tasks", "task-a", "task-b")
    reported: list[str] = []

    async with make_client(tracker) as client:
        await client.datasets.push("swe-bench", "verified", folder)
        stored_before = dict(tracker.blobs)
        await client.datasets.push(
            "swe-bench", "verified", folder, on_stored=lambda task_id, _: reported.append(task_id)
        )

        (folder / "task-b" / "instruction.md").write_text("Changed.")
        await client.datasets.push(
            "swe-bench", "verified", folder, on_stored=lambda task_id, _: reported.append(task_id)
        )

    assert reported == ["task-b"]
    assert len(tracker.blobs) == len(stored_before) + 1
    assert len(tracker.published) == 3


@pytest.mark.description("Push reports the entries of a task folder it does not store.")
async def test_push_reports_the_entries_of_a_task_folder_it_does_not_store(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    tracker = _Tracker()
    folder = _write_tasks(tmp_path / "tasks", "task-a", "task-b")
    (folder / "task-a" / "data").mkdir()
    (folder / "task-a" / "data" / "set.csv").write_text("1")
    (folder / "task-a" / "notes.txt").write_text("n")
    left: list[tuple[str, list[str]]] = []

    async with make_client(tracker) as client:
        await client.datasets.push("swe-bench", "verified", folder, on_left_out=lambda *entry: left.append(entry))

    assert left == [("task-a", ["data", "notes.txt"])]


@pytest.mark.description("Push uploads identical tasks once.")
async def test_push_uploads_identical_tasks_once(make_client: Callable[..., ValkyrieClient], tmp_path: Path) -> None:
    tracker = _Tracker()
    folder = _write_tasks(tmp_path / "tasks", "task-a", "task-b")
    (folder / "task-b" / "instruction.md").write_text("Do task-a.")

    async with make_client(tracker) as client:
        await client.datasets.push("swe-bench", "verified", folder)

    assert len(tracker.blobs) == 1
    (published,) = tracker.published
    assert len(set(published["tasks"].values())) == 1 and sorted(published["tasks"]) == ["task-a", "task-b"]


@pytest.mark.description(
    "Push creates the dataset when it is missing, and its benchmark only when that is missing too."
)
@pytest.mark.parametrize(
    ("benchmark_exists", "expected_requests", "expected_created"),
    [
        (
            False,
            [
                ("GET", "/v1/benchmarks/swe-bench/datasets/verified"),
                ("GET", "/v1/benchmarks/swe-bench"),
                ("POST", "/v1/benchmarks"),
                ("POST", "/v1/benchmarks/swe-bench/datasets"),
            ],
            ["benchmark swe-bench", "dataset swe-bench/verified"],
        ),
        (
            True,
            [
                ("GET", "/v1/benchmarks/swe-bench/datasets/verified"),
                ("GET", "/v1/benchmarks/swe-bench"),
                ("POST", "/v1/benchmarks/swe-bench/datasets"),
            ],
            ["dataset swe-bench/verified"],
        ),
    ],
    ids=["benchmark-missing", "benchmark-exists"],
)
async def test_push_creates_what_is_missing(
    make_client: Callable[..., ValkyrieClient],
    tmp_path: Path,
    benchmark_exists: bool,
    expected_requests: list[tuple[str, str]],
    expected_created: list[str],
) -> None:
    requests: list[tuple[str, str]] = []
    tracker = _Tracker()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            requests.append((request.method, request.url.path))
            if benchmark_exists and request.url.path == "/v1/benchmarks/swe-bench":
                return httpx.Response(200, json=_BENCHMARK)
            return httpx.Response(404, json={"detail": "not found"})
        if request.url.path in {"/v1/benchmarks", "/v1/benchmarks/swe-bench/datasets"}:
            requests.append((request.method, request.url.path))
            return httpx.Response(201, json=_DATASET if "datasets" in request.url.path else _BENCHMARK)
        return tracker(request)

    created: list[str] = []
    async with make_client(handler) as client:
        await client.datasets.push(
            "swe-bench", "verified", _write_tasks(tmp_path / "tasks", "task-a"), on_created=created.append
        )

    assert requests == expected_requests
    assert created == expected_created
    assert len(tracker.published) == 1


@pytest.mark.description("Push stops when the target cannot be read.")
@pytest.mark.parametrize("forbidden", ["/v1/benchmarks/swe-bench/datasets/verified", "/v1/benchmarks/swe-bench"])
async def test_push_stops_when_the_target_cannot_be_read(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path, forbidden: str
) -> None:
    requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path))
        if request.url.path == forbidden:
            return httpx.Response(403, json={"detail": "Not allowed"})
        return httpx.Response(404, json={"detail": "not found"})

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieAPIError) as refused:
            await client.datasets.push("swe-bench", "verified", _write_tasks(tmp_path / "tasks", "task-a"))

    assert refused.value.status_code == 403
    assert all(method == "GET" for method, _ in requests)


@pytest.mark.description("Push goes on when another push creates the benchmark and dataset first.")
async def test_push_goes_on_when_another_push_creates_the_benchmark_and_dataset_first(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    gets = {"dataset": 0}
    created: list[str] = []
    tracker = _Tracker()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path in {"/v1/benchmarks", "/v1/benchmarks/swe-bench/datasets"}:
            return httpx.Response(409, json={"detail": "already exists"})
        if request.method == "GET" and request.url.path.endswith("/datasets/verified"):
            gets["dataset"] += 1
            return httpx.Response(404 if gets["dataset"] == 1 else 200, json={"detail": "not found"} | _DATASET)
        if request.method == "GET":
            return httpx.Response(404, json={"detail": "not found"})
        return tracker(request)

    async with make_client(handler) as client:
        version = await client.datasets.push(
            "swe-bench", "verified", _write_tasks(tmp_path / "tasks", "task-a"), on_created=created.append
        )

    assert version.task_count == 1
    assert created == []


@pytest.mark.description("Push refuses an archived dataset before sending any task.")
async def test_push_refuses_an_archived_dataset_before_sending_any_task(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    tracker = _Tracker(dataset={**_DATASET, "archived_at": "2026-10-25T01:00:00+00:00"})

    async with make_client(tracker) as client:
        with pytest.raises(ValueError, match="is archived; restore it first"):
            await client.datasets.push("swe-bench", "verified", _write_tasks(tmp_path / "tasks", "task-a"))

    assert [method for method, _ in tracker.requests] == ["GET"]


@pytest.mark.description("Push publishes nothing and reports the tasks that could not be uploaded.")
async def test_push_publishes_nothing_and_reports_the_tasks_that_could_not_be_uploaded(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    tracker = _Tracker(refuse="Refuse me")
    folder = _write_tasks(tmp_path / "tasks", "task-a", "task-b")
    (folder / "task-b" / "instruction.md").write_text("Refuse me")

    async with make_client(tracker) as client:
        with pytest.raises(ValkyrieDatasetPushError) as error:
            await client.datasets.push("swe-bench", "verified", folder)

    assert error.value.stored == ["task-a"]
    assert list(error.value.failed) == ["task-b"]
    assert "denied" in error.value.failed["task-b"]
    assert tracker.published == []


@pytest.mark.description("Push reports a reply it cannot read as a failed task.")
async def test_push_reports_a_reply_it_cannot_read_as_a_failed_task(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    tracker = _Tracker()
    folder = _write_tasks(tmp_path / "tasks", "task-a", "task-b", "task-c")
    (folder / "task-b" / "instruction.md").write_text("Another.")
    (folder / "task-c" / "instruction.md").write_text("A third.")

    def handler(request: httpx.Request) -> httpx.Response:
        if "/blobs/" in request.url.path and not request.url.path.endswith("missing"):
            with zipfile.ZipFile(io.BytesIO(request.content)) as archive:
                if archive.read("instruction.md") == b"Another.":
                    return httpx.Response(200, content=b"<html>not json</html>")
        return tracker(request)

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieDatasetPushError) as error:
            await client.datasets.push("swe-bench", "verified", folder)

    assert sorted(error.value.stored) == ["task-a", "task-c"]
    assert list(error.value.failed) == ["task-b"]


@pytest.mark.description("Push refuses a broken task before sending anything.")
async def test_push_refuses_a_broken_task_before_sending_anything(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    tracker = _Tracker()
    root = _write_tasks(tmp_path / "tasks", "task-a", "task-b")
    (root / "task-b" / "instruction.md").unlink()

    async with make_client(tracker) as client:
        with pytest.raises(TaskFolderError, match="task-b: instruction.md is missing"):
            await client.datasets.push("swe-bench", "verified", root)

    assert tracker.requests == []


_STARTED = {
    "benchmark_name": "swe-bench",
    "agent_name": "agent",
    "benchmark_id": "00000000-0000-0000-0000-000000000001",
    "concurrency": 2,
    "started_at": "2026-07-15T12:00:00+00:00",
    "task_count": 1,
    "cloudwatch_url": "https://cloudwatch.example/run",
    "s3_bucket_url": "s3://runs/run",
}


@pytest.mark.description("Start run sends the dataset and names the run after its benchmark.")
@pytest.mark.parametrize("exact_release", [False, True])
@pytest.mark.parametrize("admission_id", [None, UUID("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa")])
async def test_start_run_sends_the_dataset_and_names_the_run_after_its_benchmark(
    make_client: Callable[..., ValkyrieClient],
    exact_release: bool,
    admission_id: UUID | None,
) -> None:
    bodies: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json=_STARTED)

    async with make_client(handler) as client:
        await client.datasets.start_run(
            AgentContractRequest(
                name="agent",
                agent_release=AgentReleaseSelection(reference="R1", archive_sha256="a" * 64) if exact_release else None,
            ),
            HostedDataset(benchmark="swe-bench", dataset="verified"),
            admission_id=admission_id,
            concurrency=2,
            task_ids=["task-a"],
            provider="docker",
        )

    (body,) = bodies
    if admission_id is not None:
        assert body["admission_id"] == str(admission_id)
    elif exact_release:
        assert UUID(body["admission_id"]).version == 4
    else:
        assert body.get("admission_id") is None
    assert body["benchmark_name"] == "swe-bench"
    assert body["dataset_ref"] == {"kind": "hosted", "benchmark": "swe-bench", "dataset": "verified", "ref": "latest"}
    assert (body["concurrency"], body["task_ids"], body["sandbox_provider"]) == (2, ["task-a"], "docker")
    assert "dataset" not in body and "dataset_version" not in body


@pytest.mark.description("Start run refuses a benchmark that is not the datasets.")
async def test_start_run_refuses_a_benchmark_that_is_not_the_datasets(
    make_client: Callable[..., ValkyrieClient],
) -> None:
    async with make_client(lambda request: httpx.Response(200, json=_STARTED)) as client:
        with pytest.raises(ValkyrieRunError, match="benchmark must be 'swe-bench'"):
            await client.datasets.start_run(
                "agent", HostedDataset(benchmark="swe-bench", dataset="verified"), benchmark="other"
            )


@pytest.mark.description("A folder that cannot name a run asks for a name.")
def test_a_folder_that_cannot_name_a_run_asks_for_a_name() -> None:
    with pytest.raises(ValkyrieRunError, match="pass a benchmark name"):
        folder_run_name("/tasks/___")


def _write_manifest_dataset(root: Path, *names: str, dataset: str = 'name = "acme/verified"') -> Path:
    entries = ""
    for name in names:
        folder = root / name
        (folder / "tests").mkdir(parents=True, exist_ok=True)
        (folder / "task.toml").write_text(f'[task]\nname = "acme/{name}"\n')
        (folder / "instruction.md").write_text(f"Do {name}.")
        (folder / "tests" / "test.sh").write_text("#!/bin/bash\n")
        entries += f'[[tasks]]\nname = "acme/{name}"\ndigest = "{task_digest(folder)}"\n'
    (root / "dataset.toml").write_text(f"[dataset]\n{dataset}\n\n{entries}")
    return root


@pytest.mark.description("Push with a manifest publishes exactly the listed tasks with the manifest.")
async def test_push_with_a_manifest_publishes_exactly_the_listed_tasks_with_the_manifest(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    tracker = _Tracker()
    root = _write_manifest_dataset(
        tmp_path / "tasks", "task-a", "task-b", dataset='name = "acme/verified"\nversion = "1.0"'
    )
    _write_tasks(root, "unlisted")
    (root / "unlisted" / "task.toml").write_text('[task]\nname = "acme/unlisted"\n')
    warnings: list[str] = []

    async with make_client(tracker) as client:
        await client.datasets.push("swe-bench", "verified", root, tags=["stable"], on_warning=warnings.append)

    (published,) = tracker.published
    assert sorted(published["tasks"]) == ["task-a", "task-b"]
    assert published["tags"] == ["stable", "1.0"]
    manifest = published["manifest"]
    assert manifest["name"] == "acme/verified" and manifest["version"] == "1.0"
    assert {task_id: task["name"] for task_id, task in manifest["tasks"].items()} == {
        "task-a": "acme/task-a",
        "task-b": "acme/task-b",
    }
    assert [task["digest"] for task in manifest["tasks"].values()] == [
        published["tasks"][task_id] for task_id in manifest["tasks"]
    ]
    assert any("unlisted" in warning and "left out" in warning for warning in warnings)
    assert any("acme/verified" in warning and "swe-bench/verified" in warning for warning in warnings)


@pytest.mark.description("Push with a manifest refuses a changed task before creating or uploading anything.")
async def test_push_with_a_manifest_refuses_a_changed_task_before_creating_or_uploading_anything(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    tracker = _Tracker()
    root = _write_manifest_dataset(tmp_path / "tasks", "task-a")
    (root / "task-a" / "instruction.md").write_text("Changed.")

    async with make_client(tracker) as client:
        with pytest.raises(ManifestError, match="Task task-a \\(acme/task-a\\) changed since dataset.toml"):
            await client.datasets.push("swe-bench", "verified", root)

    assert tracker.requests == []


@pytest.mark.description("Push with a manifest sets the description only when the dataset has none.")
async def test_push_with_a_manifest_sets_the_description_only_when_the_dataset_has_none(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    updates: list[dict[str, object]] = []

    def tracker_for(description: str) -> Callable[[httpx.Request], httpx.Response]:
        tracker = _Tracker(dataset={**_DATASET, "description": description})

        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "PATCH":
                updates.append(json.loads(request.content))
                return httpx.Response(200, json=_DATASET)
            return tracker(request)

        return handler

    root = _write_manifest_dataset(
        tmp_path / "tasks", "task-a", dataset='name = "swe-bench/verified"\ndescription = "From the manifest"'
    )

    async with make_client(tracker_for("")) as client:
        await client.datasets.push("swe-bench", "verified", root)
    async with make_client(tracker_for("Set by hand")) as client:
        await client.datasets.push("swe-bench", "verified", root)

    assert updates == [{"description": "From the manifest"}]


@pytest.mark.description("Push refuses a manifest that pins a file other than metric py.")
async def test_push_refuses_a_manifest_that_pins_a_file_other_than_metric_py(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    tracker = _Tracker()
    root = _write_manifest_dataset(tmp_path / "tasks", "task-a")
    (root / "dataset.toml").write_text((root / "dataset.toml").read_text() + '\n[[files]]\npath = "other.py"\n')

    async with make_client(tracker) as client:
        with pytest.raises(ManifestError, match="only metric.py is supported"):
            await client.datasets.push("swe-bench", "verified", root)

    assert tracker.requests == []


@pytest.mark.description("Push stores the metric script before the tasks and publishes it as a file.")
async def test_push_stores_the_metric_script_before_the_tasks_and_publishes_it_as_a_file(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    script = b"import sys\nprint('metric')\n"
    folder = _write_tasks(tmp_path / "tasks", "task-a")
    (folder / "metric.py").write_bytes(script)
    tracker = _Tracker()

    async with make_client(tracker) as client:
        version = await client.datasets.push("swe-bench", "verified", folder)

    digest = metric_digest(script)
    assert tracker.files == {digest: script}
    mutations = [path for method, path in tracker.requests if method != "GET"]
    assert mutations[0].endswith(f"/files/{digest}")
    assert tracker.published[0]["metric_digest"] == digest
    assert version.task_count == 1


@pytest.mark.description("Push without a metric script publishes no metric digest.")
async def test_push_without_a_metric_script_publishes_no_metric_digest(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    tracker = _Tracker()

    async with make_client(tracker) as client:
        await client.datasets.push("swe-bench", "verified", _write_tasks(tmp_path / "tasks", "task-a"))

    assert "metric_digest" not in tracker.published[0]


@pytest.mark.description("Push refuses a metric script the tracker would refuse before sending anything.")
async def test_push_refuses_a_metric_script_the_tracker_would_refuse_before_sending_anything(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    folder = _write_tasks(tmp_path / "tasks", "task-a")
    (folder / "metric.py").write_text("def broken(:\n")
    tracker = _Tracker()

    async with make_client(tracker) as client:
        with pytest.raises(MetricScriptError, match="not valid Python"):
            await client.datasets.push("swe-bench", "verified", folder)

    assert tracker.requests == []


@pytest.mark.description("A manifest that pins metric py is checked against the folders script.")
async def test_a_manifest_that_pins_metric_py_is_checked_against_the_folders_script(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    root = _write_manifest_dataset(tmp_path / "tasks", "task-a")
    (root / "metric.py").write_text("print(1)\n")
    digest = metric_digest(b"print(1)\n")
    (root / "dataset.toml").write_text(
        (root / "dataset.toml").read_text() + f'\n[[files]]\npath = "metric.py"\ndigest = "{digest}"\n'
    )
    tracker = _Tracker()

    async with make_client(tracker) as client:
        await client.datasets.push("swe-bench", "verified", root)
        (root / "metric.py").write_text("print(2)\n")
        with pytest.raises(ManifestError, match="metric.py changed since dataset.toml was written"):
            await client.datasets.push("swe-bench", "verified", root)

    assert tracker.published[0]["metric_digest"] == digest
    assert len(tracker.published) == 1
