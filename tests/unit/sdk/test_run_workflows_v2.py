from __future__ import annotations

import asyncio
import io
import threading
import json
import tarfile
from pathlib import Path
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import ValidationError

from valkyrie.sdk import ValkyrieAPIError, ValkyrieStreamError, ValkyrieTransportError
from valkyrie.sdk.models import AWSBenchmarkArguments, LocalBenchmarkArguments

_STOP_RUN_ID = UUID("11111111-1111-4111-8111-111111111111")


class ChunkStream(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"first-"
        yield b"second"


@pytest.mark.description("`runs.metadata` returns the run's arguments as the model for its environment.")
async def test_metadata_returns_typed_run_metadata(make_client) -> None:
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == f"/fetch-benchmark-metadata/{run_id}"
        return httpx.Response(
            200,
            json={
                "benchmark_id": str(run_id),
                "benchmark_name": "swebench",
                "benchmark_arguments": {
                    "environment": "aws",
                    "contract": {"name": "sweagent", "model": "anthropic/claude-sonnet-4-6"},
                    "concurrency": 5,
                    "task_ids": None,
                    "slice_str": None,
                    "lambda_function": None,
                    "dataset": "default",
                    "sandbox_provider": "daytona",
                    "sandbox_provider_secret_name": None,
                },
                "started_by_email": "developer@vals.ai",
            },
        )

    async with make_client(handler) as client:
        result = await client.runs.metadata(run_id)

    assert result.benchmark_id == run_id
    assert result.storage_bucket is None
    assert isinstance(result.benchmark_arguments, AWSBenchmarkArguments)


@pytest.mark.description("`runs.metadata` raises when the resources do not fit the run's environment.")
@pytest.mark.parametrize(
    ("environment", "properties", "valid"),
    [
        ("local", {"data_root": "/tmp/valkyrie"}, True),
        ("aws", {"data_root": "/tmp/valkyrie"}, False),
        ("local", None, False),
        (
            "local",
            {"region": "us-east-1", "s3_bucket": "runs", "log_group": "runs", "log_retention_days": 365},
            False,
        ),
    ],
)
async def test_metadata_validates_environment_resources(make_client, environment, properties, valid) -> None:
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "benchmark_id": str(run_id),
                "benchmark_name": "swebench",
                "benchmark_arguments": {
                    "contract": {"name": "sweagent"},
                    "concurrency": 1,
                    "environment": environment,
                    "properties": properties,
                },
            },
        )

    async with make_client(handler) as client:
        if valid:
            result = await client.runs.metadata(run_id)
            assert isinstance(result.benchmark_arguments, LocalBenchmarkArguments)
            assert result.benchmark_arguments.properties.data_root == Path("/tmp/valkyrie")
        else:
            with pytest.raises(ValidationError):
                await client.runs.metadata(run_id)


@pytest.mark.description("`runs.results_exist` returns whether results are in S3 as a typed model.")
async def test_results_exist_returns_typed_s3_state(make_client) -> None:
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/check-results-exist"
        assert request.url.params["benchmark_id"] == str(run_id)
        return httpx.Response(200, json={"exists": True})

    async with make_client(handler) as client:
        result = await client.runs.results_exist(run_id)

    assert result.exists is True


@pytest.mark.description("A stop request sends a task selection only when task IDs are given.")
@pytest.mark.parametrize(
    ("task_ids", "expected_body"),
    [
        (None, None),
        (["task-1", "task-2"], {"task_ids": ["task-1", "task-2"]}),
    ],
)
async def test_stop_sends_optional_task_scope(
    make_client,
    task_ids: list[str] | None,
    expected_body: dict[str, list[str]] | None,
) -> None:

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        assert (request.url.path, request.url.params["force"], body) == (
            f"/stop-benchmark/{_STOP_RUN_ID}",
            "true",
            expected_body,
        )

        return httpx.Response(200, json={"status": "success"})

    async with make_client(handler) as client:
        result = await client.runs.stop(_STOP_RUN_ID, force=True, task_ids=task_ids)

    assert result.status == "success"


@pytest.mark.description("Analyze turns cached JSON into a done event.")
async def test_analyze_normalizes_cached_json_to_a_done_event(make_client) -> None:
    run_id = uuid4()
    captured_body: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == f"/analyze-benchmark/{run_id}"
        captured_body.update(json.loads(request.content))
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            json={"status": "done", "reading_plan_url": "https://analysis.test/plan"},
        )

    async with make_client(handler) as client:
        events = [event async for event in client.runs.analyze(run_id)]

    assert captured_body == {"no_cache": False, "lambda_function": None}
    assert [(event.event, event.data) for event in events] == [
        ("done", {"status": "done", "reading_plan_url": "https://analysis.test/plan"})
    ]


@pytest.mark.description("`runs.analyze` yields each progress event, ending with the reading plan URL.")
async def test_analyze_parses_sse_progress_events(make_client) -> None:
    run_id = uuid4()

    def handler(_request: httpx.Request) -> httpx.Response:
        content = (
            'event: started\ndata: {"lambda_function": "analyze-fn"}\n\n'
            "event: heartbeat\ndata: {}\n\n"
            'event: done\ndata: {"reading_plan_url":\n'
            'data: "https://analysis.test/plan"}\n\n'
        )
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=content)

    async with make_client(handler) as client:
        events = [
            event
            async for event in client.runs.analyze(
                run_id,
                no_cache=True,
                lambda_function="analyze-fn",
            )
        ]

    assert [event.event for event in events] == ["started", "heartbeat", "done"]
    assert events[-1].data == {"reading_plan_url": "https://analysis.test/plan"}


@pytest.mark.description("Analyze stops after the final done event.")
async def test_analyze_stops_after_the_terminal_done_event(make_client) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=(
                'event: done\ndata: {"reading_plan_url": "https://analysis.test/plan"}\n\n'
                "event: heartbeat\ndata: {}\n\n"
            ),
        )

    async with make_client(handler) as client:
        events = [event async for event in client.runs.analyze(uuid4(), lambda_function="analyze-fn")]

    assert [event.event for event in events] == ["done"]


@pytest.mark.description("`runs.analyze` raises when the stream sends an error event.")
async def test_analyze_raises_for_an_sse_error_event(make_client) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content='event: error\ndata: {"message": "lambda failed"}\n\n',
        )

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieStreamError, match="lambda failed"):
            _ = [event async for event in client.runs.analyze(uuid4(), lambda_function="analyze-fn")]


@pytest.mark.description("`runs.analyze` raises an error status before reading any events.")
async def test_analyze_raises_api_errors_before_streaming(make_client) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"detail": "run must be FINISHED"})

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieAPIError, match="run must be FINISHED"):
            _ = [event async for event in client.runs.analyze(uuid4())]


@pytest.mark.description("Output streaming yields archive chunks and sends repeated task filters.")
async def test_stream_outputs_yields_archive_chunks_and_repeated_task_filters(make_client) -> None:
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == f"/fetch-run-outputs/{run_id}"
        assert request.url.params.get_list("task_ids") == ["task-1", "task-2"]
        return httpx.Response(200, headers={"content-type": "application/x-tar"}, stream=ChunkStream())

    async with make_client(handler) as client:
        chunks = [
            chunk
            async for chunk in client.runs.stream_outputs(
                run_id,
                task_ids=["task-1", "task-2"],
            )
        ]

    assert chunks == [b"first-", b"second"]


@pytest.mark.description("Output streaming raises API errors before streaming.")
async def test_stream_outputs_raises_api_errors_before_streaming(make_client) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "No outputs found"})

    async with make_client(handler) as client:
        with pytest.raises(ValkyrieAPIError, match="No outputs found"):
            _ = [chunk async for chunk in client.runs.stream_outputs(uuid4())]


@pytest.mark.description("Updating concurrency sends the new value and returns the saved one.")
async def test_update_concurrency(make_client) -> None:
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PATCH"
        assert request.url.path == f"/benchmarks/{run_id}/concurrency"
        assert json.loads(request.content) == {"concurrency": 7}
        return httpx.Response(200, json={"benchmark_id": str(run_id), "status": "IN_PROGRESS", "concurrency": 7})

    async with make_client(handler) as client:
        result = await client.runs.update_concurrency(run_id, concurrency=7)
        for invalid in (0, -1, True):
            with pytest.raises(ValueError):
                await client.runs.update_concurrency(run_id, concurrency=invalid)
    assert result.benchmark_id == run_id
    assert result.concurrency == 7


@pytest.mark.description("Shed sends its options and returns the typed result.")
async def test_shed(make_client) -> None:
    run_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == f"/benchmarks/{run_id}/shed"
        assert json.loads(request.content) == {"concurrency": 3, "dry_run": True}
        return httpx.Response(200, json={"benchmark_id": str(run_id), "concurrency": 3, "task_ids": ["task_newest"]})

    async with make_client(handler) as client:
        result = await client.runs.shed(run_id, concurrency=3, dry_run=True)
        for invalid in (0, -1, True):
            with pytest.raises(ValueError):
                await client.runs.shed(run_id, concurrency=invalid)
    assert result.concurrency == 3
    assert result.task_ids == ["task_newest"]


@pytest.mark.description("`runs.iter` raises when the Tracker returns the same cursor twice, instead of looping.")
async def test_run_iteration_rejects_repeated_cursor(make_client) -> None:
    from valkyrie.sdk import FetchBenchmarksRequest

    async with make_client(
        lambda request: httpx.Response(200, json={"benchmarks": [], "next_cursor": "same"})
    ) as client:
        with pytest.raises(ValkyrieStreamError, match="repeated"):
            _ = [run async for run in client.runs.iter()]
        with pytest.raises(ValueError, match="offset"):
            _ = [run async for run in client.runs.iter(FetchBenchmarksRequest(offset=1))]


@pytest.mark.description("Output download extracts nested archives.")
@pytest.mark.parametrize("unsafe", [False, True])
@pytest.mark.parametrize("task_id", ["task", "group/task"])
async def test_download_outputs_extracts_nested_archives(
    make_client, tmp_path: Path, unsafe: bool, task_id: str
) -> None:
    nested = io.BytesIO()
    with tarfile.open(fileobj=nested, mode="w:gz") as archive:
        member = tarfile.TarInfo("../escape" if unsafe else "result.txt")
        member.size = 6
        archive.addfile(member, io.BytesIO(b"result"))
    outer = io.BytesIO()
    with tarfile.open(fileobj=outer, mode="w") as archive:
        member = tarfile.TarInfo(f"{task_id}/agent_output.tar.gz")
        member.size = len(nested.getvalue())
        archive.addfile(member, io.BytesIO(nested.getvalue()))
        artifact = tarfile.TarInfo(f"{task_id}/model.tar.gz")
        artifact.size = 6
        archive.addfile(artifact, io.BytesIO(b"opaque"))

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params.get_list("task_ids") == [task_id]
        return httpx.Response(200, content=outer.getvalue())

    destination = tmp_path / "outputs"
    async with make_client(handler) as client:
        if unsafe:
            with pytest.raises(ValueError, match="Unsafe"):
                await client.runs.download_outputs(uuid4(), destination, task_ids=[task_id])
            assert not destination.exists()
            assert not (tmp_path / "escape").exists()
        else:
            result = await client.runs.download_outputs(uuid4(), destination, task_ids=[task_id])
            assert result == destination
            assert (destination / f"{task_id}/agent_output/result.txt").read_text() == "result"
            assert (destination / f"{task_id}/model.tar.gz").read_bytes() == b"opaque"
            with pytest.raises(FileExistsError):
                await client.runs.download_outputs(uuid4(), destination)
        for limits in ({"max_archive_bytes": 1}, {"max_expanded_bytes": 1}, {"max_entries": 1}):
            with pytest.raises(ValueError, match="exceed"):
                await client.runs.download_outputs(uuid4(), tmp_path / "limited", task_ids=[task_id], **limits)
            assert not (tmp_path / "limited").exists()


@pytest.mark.description("Artifact download validates paths and leaves out credentials.")
@pytest.mark.parametrize("path", ["task/result.json", "task:one/result.json", "../escape", "task/../../escape"])
async def test_artifact_download_validates_paths_and_omits_credentials(make_client, tmp_path: Path, path: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "download.test":
            assert not any(name.lower().startswith("x-") for name in request.headers)
            assert "authorization" not in request.headers
            return httpx.Response(200, content=b"{}")
        assert request.headers["X-Api-Key"] == "vals-key"
        if request.url.path.endswith("download-url"):
            return httpx.Response(
                200, json={"path": path, "download_url": "https://download.test/file", "expires_in": 300, "size": 2}
            )
        return httpx.Response(200, json={"artifacts": [{"path": path, "size": 2}]})

    destination = tmp_path / "outputs"
    async with make_client(handler) as client:
        if ".." in path:
            with pytest.raises(ValueError, match="relative"):
                await client.artifacts.download(uuid4(), destination)
            assert not destination.exists()
        else:
            result = await client.artifacts.download(uuid4(), destination, path=path.split("/")[0])
            assert (result / path).read_bytes() == b"{}"
            with pytest.raises(FileExistsError):
                await client.artifacts.download(uuid4(), destination)
            with pytest.raises(ValueError, match="limits"):
                await client.artifacts.download(uuid4(), tmp_path / "limited", max_bytes=1)
            assert not (tmp_path / "limited").exists()


@pytest.mark.description("A task moving to a later page must not hide the remaining tasks.")
async def test_task_iteration_continues_when_tasks_move_between_pages(make_client) -> None:
    first, second, third = uuid4(), uuid4(), uuid4()
    offsets = []

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        offsets.append(offset)
        ids = [first, second] if offset == 0 else [first, third]
        return httpx.Response(
            200,
            json={
                "tasks": [
                    {
                        "id": str(task_id),
                        "task_id": str(task_id),
                        "status": "PENDING",
                        "started_at": "2026-01-01T00:00:00Z",
                        "finished_at": None,
                    }
                    for task_id in ids
                ],
                "total_count": 4,
            },
        )

    from valkyrie.sdk import FetchTasksRequest

    async with make_client(handler) as client:
        tasks = [task async for task in client.benchmarks.iter_tasks(uuid4(), FetchTasksRequest(limit=2))]

    assert [task.id for task in tasks] == [first, second, first, third]
    assert offsets == [0, 2]


@pytest.mark.description("Cancellation must not close a worker's input or leave published output behind.")
async def test_cancelled_output_extraction_finishes_before_cleanup(make_client, monkeypatch, tmp_path: Path) -> None:
    from valkyrie.sdk.resources import runs

    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    destination = tmp_path / "outputs"

    def extract(stream, output_dir, **limits):
        started.set()
        assert release.wait(5)
        assert stream.read() == b"archive"
        output_dir.mkdir()
        (output_dir / "result").write_text("complete")
        finished.set()
        return output_dir

    monkeypatch.setattr(runs, "extract_output_archive", extract)
    async with make_client(lambda request: httpx.Response(200, content=b"archive")) as client:
        download = asyncio.create_task(client.runs.download_outputs(uuid4(), destination))
        try:
            assert await asyncio.to_thread(started.wait, 5)
            download.cancel()
            await asyncio.sleep(0)
            assert not download.done()
            download.cancel()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await download

    assert finished.is_set()
    assert not destination.exists()


@pytest.mark.description("A cancelled output download still ends cancelled when the extraction it waits for fails.")
async def test_cancelled_output_download_stays_cancelled_when_extraction_fails(make_client, monkeypatch) -> None:
    from valkyrie.sdk.resources import runs

    started = threading.Event()
    release = threading.Event()

    def extract(stream, output_dir, **limits):
        started.set()
        assert release.wait(5)
        raise ValueError("Run outputs exceed extraction limits")

    monkeypatch.setattr(runs, "extract_output_archive", extract)
    async with make_client(lambda request: httpx.Response(200, content=b"archive")) as client:
        download = asyncio.create_task(client.runs.download_outputs(uuid4(), Path("unused")))
        try:
            assert await asyncio.to_thread(started.wait, 5)
            download.cancel()
            await asyncio.sleep(0)
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await download


@pytest.mark.description(
    "A download cancelled while it publishes its output ends cancelled once the output is in place."
)
async def test_output_download_cancelled_during_publish_stays_cancelled(make_client, monkeypatch, tmp_path) -> None:
    from valkyrie.sdk.resources import runs

    started = threading.Event()
    release = threading.Event()
    destination = tmp_path / "outputs"
    rename = Path.rename

    def extract(stream, output_dir, **limits):
        output_dir.mkdir()
        return output_dir

    def publish(self, target):
        if Path(target) == destination:
            started.set()
            assert release.wait(5)
        return rename(self, target)

    monkeypatch.setattr(runs, "extract_output_archive", extract)
    monkeypatch.setattr(Path, "rename", publish)
    async with make_client(lambda request: httpx.Response(200, content=b"archive")) as client:
        download = asyncio.create_task(client.runs.download_outputs(uuid4(), destination))
        try:
            assert await asyncio.to_thread(started.wait, 5)
            download.cancel()
            await asyncio.sleep(0)
            assert not download.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await download

    assert destination.is_dir()


@pytest.mark.description(
    "Extracting run output into an existing folder adds new files and leaves existing ones unchanged."
)
@pytest.mark.parametrize("collision", [False, True])
async def test_output_archive_merges_existing_directory_without_overwriting(make_client, tmp_path, collision):
    nested = io.BytesIO()
    with tarfile.open(fileobj=nested, mode="w:gz") as archive:
        member = tarfile.TarInfo("result.txt")
        member.size = 6
        archive.addfile(member, io.BytesIO(b"result"))
    outer = io.BytesIO()
    with tarfile.open(fileobj=outer, mode="w") as archive:
        for name, content in {
            "task/agent_output.tar.gz": nested.getvalue(),
            "task/agent_output/result.txt" if collision else "task/agent_output/summary.json": b"{}",
        }.items():
            member = tarfile.TarInfo(name)
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    destination = tmp_path / "outputs"
    async with make_client(lambda request: httpx.Response(200, content=outer.getvalue())) as client:
        if collision:
            with pytest.raises(ValueError, match="collides"):
                await client.runs.download_outputs(uuid4(), destination)
            assert not destination.exists()
        else:
            await client.runs.download_outputs(uuid4(), destination)
            assert (destination / "task/agent_output/result.txt").read_text() == "result"
            assert (destination / "task/agent_output/summary.json").read_text() == "{}"


@pytest.mark.description("Output files extracted from mode-zero entries stay readable and writable by their owner.")
async def test_output_archive_keeps_mode_zero_files_readable(make_client, tmp_path: Path) -> None:
    nested = io.BytesIO()
    with tarfile.open(fileobj=nested, mode="w:gz") as archive:
        member = tarfile.TarInfo("result.txt")
        member.size = 6
        member.mode = 0
        archive.addfile(member, io.BytesIO(b"result"))
    outer = io.BytesIO()
    with tarfile.open(fileobj=outer, mode="w") as archive:
        for name, content in {"task/agent_output.tar.gz": nested.getvalue(), "task/script.sh": b"run"}.items():
            member = tarfile.TarInfo(name)
            member.size = len(content)
            member.mode = 0 if name.endswith(".gz") else 0o755
            archive.addfile(member, io.BytesIO(content))
    destination = tmp_path / "outputs"
    async with make_client(lambda request: httpx.Response(200, content=outer.getvalue())) as client:
        await client.runs.download_outputs(uuid4(), destination)
    result = destination / "task/agent_output/result.txt"
    assert result.read_text() == "result"
    assert result.stat().st_mode & 0o777 == 0o600
    assert (destination / "task/script.sh").stat().st_mode & 0o777 == 0o755


@pytest.mark.description("Output archive members whose paths collide as file and directory raise `ValueError`.")
@pytest.mark.parametrize(
    "members",
    [
        [("task", tarfile.REGTYPE), ("task/result.txt", tarfile.REGTYPE)],
        [("task/result.txt", tarfile.REGTYPE), ("task", tarfile.REGTYPE)],
        [("task/agent_output", tarfile.REGTYPE), ("task/agent_output.tar.gz", tarfile.REGTYPE)],
        [("task", tarfile.REGTYPE), ("task/sub", tarfile.DIRTYPE)],
        [("task/agent_output.tar.gz", tarfile.DIRTYPE)],
    ],
)
async def test_output_archive_rejects_file_directory_collisions(
    make_client, tmp_path: Path, members: list[tuple[str, bytes]]
) -> None:
    nested = io.BytesIO()
    with tarfile.open(fileobj=nested, mode="w:gz"):
        pass
    outer = io.BytesIO()
    with tarfile.open(fileobj=outer, mode="w") as archive:
        for name, kind in members:
            member = tarfile.TarInfo(name)
            member.type = kind
            content = nested.getvalue() if name.endswith(".tar.gz") else b"x"
            if kind == tarfile.REGTYPE:
                member.size = len(content)
            archive.addfile(member, io.BytesIO(content) if kind == tarfile.REGTYPE else None)
    destination = tmp_path / "outputs"
    async with make_client(lambda request: httpx.Response(200, content=outer.getvalue())) as client:
        with pytest.raises(ValueError, match="collides"):
            await client.runs.download_outputs(uuid4(), destination)
    assert not destination.exists()


@pytest.mark.description("A cancelled output download leaves other callers' destinations alone.")
async def test_cancelled_output_download_preserves_other_callers_destination(make_client, monkeypatch, tmp_path):
    from valkyrie.sdk.resources import runs

    started = threading.Event()
    release = threading.Event()
    destination = tmp_path / "outputs"

    def extract(stream, output_dir, **limits):
        started.set()
        assert release.wait(5)
        output_dir.mkdir()
        (output_dir / "downloaded.txt").write_text("downloaded")
        destination.mkdir(exist_ok=True)
        (destination / "unrelated.txt").write_text("another caller")
        return output_dir

    monkeypatch.setattr(runs, "extract_output_archive", extract)
    async with make_client(lambda request: httpx.Response(200, content=b"archive")) as client:
        download = asyncio.create_task(client.runs.download_outputs(uuid4(), destination))
        try:
            assert await asyncio.to_thread(started.wait, 5)
            download.cancel()
            await asyncio.sleep(0)
            assert not download.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await download

    assert (destination / "unrelated.txt").read_text() == "another caller"
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.description("Artifact download on Windows rejects colons before writing.")
async def test_windows_artifact_download_rejects_colons_before_writing(make_client, monkeypatch, tmp_path):
    from valkyrie.sdk.resources import artifacts

    monkeypatch.setattr(artifacts.sys, "platform", "win32")
    async with make_client(
        lambda request: httpx.Response(200, json={"artifacts": [{"path": "task:one/result.json", "size": 2}]})
    ) as client:
        with pytest.raises(ValueError, match="Windows filenames"):
            await client.artifacts.download(uuid4(), tmp_path / "outputs")
    assert not (tmp_path / "outputs").exists()


@pytest.mark.description("Keep Tracker credentials on its origin and reject authenticated redirects.")
@pytest.mark.parametrize(
    "url",
    ["https://tracker.test/file", "http://tracker.test/file", "https://tracker.test:8443/file"],
)
async def test_artifact_download_authenticates_only_tracker_origin(make_client, tmp_path, url):
    redirect = False

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/file":
            assert str(request.url) == url
            if url == "https://tracker.test/file":
                assert request.headers["x-api-key"] == "vals-key"
            else:
                assert not any(name.startswith("x-") for name in request.headers)
                assert "authorization" not in request.headers
            if redirect:
                return httpx.Response(302, headers={"location": "https://other.test/file"})
            return httpx.Response(200, content=b"{}")
        if request.url.path.endswith("download-url"):
            return httpx.Response(200, json={"path": "result", "download_url": url, "expires_in": 300, "size": 2})
        return httpx.Response(200, json={"artifacts": [{"path": "result", "size": 2}]})

    async with make_client(handler) as client:
        result = await client.artifacts.download(uuid4(), tmp_path / "outputs")
        assert (result / "result").read_bytes() == b"{}"
        if url == "https://tracker.test/file":
            redirect = True
            with pytest.raises(ValkyrieTransportError):
                await client.artifacts.download(uuid4(), tmp_path / "redirect")


@pytest.mark.description("Rescore asks the tracker to run the datasets metric script again.")
async def test_rescore_asks_the_tracker_to_run_the_datasets_metric_script_again(make_client) -> None:
    run_id = UUID("123e4567-e89b-12d3-a456-426614174000")
    seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.url.path))
        return httpx.Response(
            200,
            json={"benchmark_id": str(run_id), "final_score": 75.0, "properties": {"scorer": {"cached": False}}},
        )

    async with make_client(handler) as client:
        rescored = await client.runs.rescore(run_id)

    assert seen == [("POST", f"/rescore-benchmark/{run_id}")]
    assert (rescored.benchmark_id, rescored.final_score) == (run_id, 75.0)
    assert rescored.properties == {"scorer": {"cached": False}}


@pytest.mark.description("Artifact download with attempts=True reads only the artifacts kept from replaced attempts.")
async def test_artifact_download_reads_kept_attempts(make_client, tmp_path: Path) -> None:
    path = "task/attempt-1/vals_format/result.json"
    requested: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "download.test":
            return httpx.Response(200, content=b"{}")
        requested.append(request)
        if request.url.path.endswith("download-url"):
            return httpx.Response(
                200, json={"path": path, "download_url": "https://download.test/file", "expires_in": 300, "size": 2}
            )
        return httpx.Response(200, json={"artifacts": [{"path": path, "size": 2}]})

    async with make_client(handler) as client:
        result = await client.artifacts.download(uuid4(), tmp_path / "attempts", path="task", attempts=True)

    assert (result / path).read_bytes() == b"{}"
    assert [request.url.params.get("attempts") for request in requested] == ["true", "true"]
