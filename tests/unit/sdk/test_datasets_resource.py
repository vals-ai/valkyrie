import errno
import json
import os
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from valkyrie.sdk import ValkyrieAPIError, ValkyrieClient, ValkyrieStreamError
from valkyrie.sdk.models import (
    BenchmarkDefinitionResponse,
    BenchmarkDefinitionsResponse,
    DatasetResponse,
    DatasetsResponse,
    DatasetTaskResponse,
)

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


_TASK = {"task_id": "django-11099", "size": 7, "last_modified": "2026-10-24T04:20:24+00:00"}
_TASKS = "/v1/benchmarks/swe-bench/datasets/verified/tasks"


def _with_storage(storage: Callable[[httpx.Request], httpx.Response]) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "dl.test":
            return storage(request)
        return httpx.Response(200, json={**_TASK, "download_url": "https://dl.test/task", "expires_in": 300})

    return handler


@pytest.mark.description(
    "Task methods call routes under their dataset, and `put` uploads the archive as `application/zip`."
)
async def test_task_requests_nest_under_their_dataset_and_return_typed_models(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    archive = tmp_path / "task.zip"
    archive.write_bytes(b"PKarchive")
    requests: list[tuple[str, str, str | None, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(
            (request.method, request.url.raw_path.decode(), request.headers.get("content-type"), request.content)
        )
        if request.method == "GET" and request.url.path == _TASKS:
            return httpx.Response(200, json={"tasks": [_TASK], "next_cursor": "after"})
        if request.method == "GET":
            return httpx.Response(200, json={**_TASK, "download_url": "https://dl.test/task", "expires_in": 300})
        return httpx.Response(200, json=_TASK)

    async with make_client(handler) as client:
        stored = await client.datasets.tasks.put("swe-bench", "verified", "django-11099", archive)
        listed = await client.datasets.tasks.list("swe-bench", "verified", cursor="before", limit=2)
        fetched = await client.datasets.tasks.get("swe-bench", "verified", "django-11099")
        removed = await client.datasets.tasks.remove("swe-bench", "verified", "django-11099")

    expected = DatasetTaskResponse.model_validate(_TASK)
    assert stored == removed == expected
    assert listed.tasks == [expected] and listed.next_cursor == "after"
    assert fetched.download_url == "https://dl.test/task"
    assert [(method, path) for method, path, _, _ in requests] == [
        ("PUT", f"{_TASKS}/django-11099"),
        ("GET", f"{_TASKS}?limit=2&cursor=before"),
        ("GET", f"{_TASKS}/django-11099"),
        ("DELETE", f"{_TASKS}/django-11099"),
    ]
    assert requests[0][2:] == ("application/zip", b"PKarchive")


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
        saved = await client.datasets.tasks.download("swe-bench", "verified", "django-11099", target)

        with pytest.raises(FileExistsError):
            await client.datasets.tasks.download("swe-bench", "verified", "django-11099", target)

    assert saved.read_bytes() == b"archive"
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.description("`list_all` keeps passing the next cursor until the last page and returns every task.")
async def test_list_all_follows_every_page(make_client: Callable[..., ValkyrieClient]) -> None:
    pages = {None: ([_TASK, {**_TASK, "task_id": "b"}], "after-b"), "after-b": ([{**_TASK, "task_id": "c"}], None)}
    cursors: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        cursors.append(cursor)
        tasks, next_cursor = pages[cursor]
        return httpx.Response(200, json={"tasks": tasks, "next_cursor": next_cursor})

    async with make_client(handler) as client:
        everything = await client.datasets.tasks.list_all("swe-bench", "verified")

    assert [task.task_id for task in everything.tasks] == ["django-11099", "b", "c"]
    assert cursors == [None, "after-b"]


@pytest.mark.description("`list_all` raises when the Tracker returns a cursor it already followed, instead of looping.")
async def test_list_all_rejects_a_repeated_cursor(make_client: Callable[..., ValkyrieClient]) -> None:
    pages = {None: "a", "a": "b", "b": "a"}
    cursors: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        cursors.append(cursor)
        assert len(cursors) <= len(pages), "list_all kept following a repeated cursor"
        return httpx.Response(200, json={"tasks": [_TASK], "next_cursor": pages[cursor]})

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieStreamError, match="repeated"):
            await client.datasets.tasks.list_all("swe-bench", "verified")

    assert cursors == [None, "a", "b"]


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
        await client.datasets.tasks.download("swe-bench", "verified", "django-11099", target)

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
            await client.datasets.tasks.download("swe-bench", "verified", "django-11099", target)
        assert target.read_text() == "another process"
        await client.datasets.tasks.download("swe-bench", "verified", "django-11099", target, overwrite=True)

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
        await client.datasets.tasks.download("swe-bench", "verified", "django-11099", first)
        with pytest.raises(FileExistsError, match="use overwrite"):
            await client.datasets.tasks.download("swe-bench", "verified", "django-11099", raced)

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
            await client.datasets.tasks.download("swe-bench", "verified", "django-11099", target)
        assert list(tmp_path.iterdir()) == []
        monkeypatch.setattr(os, "replace", replace)
        await client.datasets.tasks.download("swe-bench", "verified", "django-11099", target)

    assert target.read_bytes() == b"archive"


@pytest.mark.description("Download creates missing parent folders of the target before saving the archive.")
async def test_download_creates_missing_parent_folders(
    make_client: Callable[..., ValkyrieClient], tmp_path: Path
) -> None:
    def storage(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"archive")

    target = tmp_path / "tasks" / "verified" / "saved.zip"
    async with make_client(_with_storage(storage)) as client:
        await client.datasets.tasks.download("swe-bench", "verified", "django-11099", target)

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
            await client.datasets.tasks.download("swe-bench", "verified", "django-11099", tmp_path / "saved.tar.gz")

    assert raised.value.status_code == 500

    assert list(tmp_path.iterdir()) == []


@pytest.mark.description("Task methods raise for an invalid task ID and send nothing.")
@pytest.mark.parametrize("bad_id", ["..", "a/b", "-lead", "has space", "x" * 129, ""])
async def test_invalid_task_ids_are_rejected_before_any_request(
    make_client: Callable[..., ValkyrieClient], bad_id: str, tmp_path: Path
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_TASK)

    async with make_client(handler) as client:
        for operation in (client.datasets.tasks.get, client.datasets.tasks.remove):
            with pytest.raises(ValueError, match="Invalid task ID"):
                await operation("swe-bench", "verified", bad_id)
        with pytest.raises(ValueError, match="Invalid task ID"):
            await client.datasets.tasks.put("swe-bench", "verified", bad_id, tmp_path / "missing.tar.gz")

    assert requests == []
