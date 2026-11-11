import asyncio
import json
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from valkyrie.sdk import ValkyrieClient, github_download
from valkyrie.sdk.dataset_install import download_dataset
from valkyrie.sdk.errors import ValkyrieInputError, ValkyrieTransportError

_DATASET = {
    "name": "verified",
    "benchmark": "swe-bench",
    "description": "",
    "created_at": "2026-10-24T04:20:24+00:00",
    "updated_at": "2026-10-24T04:20:24+00:00",
}
_TASK_FILES = {"task.toml": "", "instruction.md": "Do it.", "tests/test.sh": "#!/bin/bash\n"}


def _write_task(folder: Path) -> None:
    for relative, text in _TASK_FILES.items():
        (folder / relative).parent.mkdir(parents=True, exist_ok=True)
        (folder / relative).write_text(text)


def _serve(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]) -> None:
    async def handle(_transport: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
        return handler(request)

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle)


class MockProcess:
    def __init__(self, returncode: int, output: bytes) -> None:
        self.returncode = returncode
        self._output = output

    async def communicate(self) -> tuple[bytes, None]:
        return self._output, None


class TestHuggingFace:
    @pytest.mark.description(
        "A Hugging Face subfolder at a slash-named revision is listed across every page, saved relative to the "
        "subfolder, and its scripts stay executable."
    )
    async def test_downloads_every_page_of_a_subfolder(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("HF_TOKEN", "hf-secret")
        seen: list[httpx.Request] = []
        tree = "/api/datasets/acme/tasks/tree/refs%2Fpr%2F1/sets/verified"
        pages = {
            "": [{"type": "directory", "path": "sets/verified/task-a"}, *_entries("task-a", ["task.toml"])],
            "next": _entries("task-a", ["instruction.md", "tests/test.sh"]),
        }

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            path = request.url.raw_path.decode().partition("?")[0]
            if path == tree:
                cursor = request.url.params.get("cursor", "")
                link = (
                    {} if cursor else {"link": f'<https://huggingface.co{tree}?recursive=true&cursor=next>; rel="next"'}
                )
                return httpx.Response(200, json=pages[cursor], headers=link)
            relative = path.removeprefix("/datasets/acme/tasks/resolve/refs%2Fpr%2F1/sets/verified/task-a/")
            return httpx.Response(200, content=_TASK_FILES[relative].encode())

        _serve(monkeypatch, handler)
        async with download_dataset(
            "hugging_face", "https://huggingface.co/datasets/acme/tasks/tree/refs%2Fpr%2F1/sets/verified"
        ) as folder:
            saved = {
                path.relative_to(folder).as_posix(): path.read_text() for path in folder.rglob("*") if path.is_file()
            }
            executable = {path.name for path in folder.rglob("*") if path.is_file() and path.stat().st_mode & 0o111}

        assert saved == {f"task-a/{relative}": text for relative, text in _TASK_FILES.items()}
        assert executable == {"test.sh"}
        assert all(request.headers["authorization"] == "Bearer hf-secret" for request in seen)

    @pytest.mark.description("A Hugging Face file listed outside the dataset folder is refused.")
    async def test_refuses_a_file_that_escapes_the_folder(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _serve(monkeypatch, lambda _request: httpx.Response(200, json=[{"type": "file", "path": "../escape"}]))

        with pytest.raises(ValueError, match="escapes"):
            async with download_dataset("hugging_face", "acme/tasks"):
                pytest.fail("escaping file accepted")

    @pytest.mark.description("A Hugging Face dataset that is missing or private without a token is an input error.")
    async def test_missing_dataset_is_an_input_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _serve(monkeypatch, lambda _request: httpx.Response(401))

        with pytest.raises(ValkyrieInputError, match="not found or not readable"):
            async with download_dataset("hugging_face", "acme/private"):
                pytest.fail("missing dataset accepted")

    @pytest.mark.description("Malformed Hugging Face sources are refused before any request.")
    @pytest.mark.parametrize(
        "source",
        [
            "acme",
            "https://gitlab.com/acme/tasks",
            "acme/tasks/tree/main/../escape",
            "../tasks",
            "acme/tasks/tree/..%2F..",
            "acme/tasks/tree/main//x",
        ],
    )
    async def test_refuses_malformed_sources(self, monkeypatch: pytest.MonkeyPatch, source: str) -> None:
        _serve(monkeypatch, lambda _request: pytest.fail("malformed source reached Hugging Face"))

        with pytest.raises(ValkyrieInputError):
            async with download_dataset("hugging_face", source):
                pytest.fail("malformed source accepted")


def _entries(task: str, files: list[str]) -> list[dict[str, str]]:
    return [{"type": "file", "path": f"sets/verified/{task}/{relative}"} for relative in files]


class TestHarbor:
    @pytest.mark.description(
        "Harbor runs a current release through `uvx` and yields the dataset folder it exports so its `metric.py` is pushed."
    )
    async def test_exports_through_uvx_and_yields_the_dataset_folder(self, monkeypatch: pytest.MonkeyPatch) -> None:
        commands: list[tuple[str, ...]] = []

        async def run(*command: str, **_options: object) -> MockProcess:
            commands.append(command)
            dataset = Path(command[-1]) / "terminal-bench"
            _write_task(dataset / "task-a")
            (dataset / "metric.py").write_text("")
            return MockProcess(0, b"")

        monkeypatch.setattr("valkyrie.sdk.dataset_install.shutil.which", lambda name: f"/usr/bin/{name}")
        monkeypatch.setattr(asyncio, "create_subprocess_exec", run)
        async with download_dataset("harbor", "terminal-bench/terminal-bench@2.0") as folder:
            entries = sorted(path.name for path in folder.iterdir())

        ((*harbor, output),) = commands
        assert harbor == [
            "uvx",
            "--from",
            "harbor>=0.25",
            "harbor",
            "download",
            "terminal-bench/terminal-bench@2.0",
            "--export",
            "--output-dir",
        ]
        assert folder == Path(output) / "terminal-bench"
        assert entries == ["metric.py", "task-a"]

    @pytest.mark.description("A failed Harbor download is an input error that carries Harbor's output.")
    async def test_failed_download_reports_harbor_output(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def run(*_command: str, **_options: object) -> MockProcess:
            return MockProcess(1, b"Dataset not found\n")

        monkeypatch.setattr(asyncio, "create_subprocess_exec", run)

        with pytest.raises(ValkyrieInputError, match="Dataset not found"):
            async with download_dataset("harbor", "acme/missing"):
                pytest.fail("failed download yielded a folder")

    @pytest.mark.description("A Harbor name that could be read as an option or a path is refused before Harbor runs.")
    @pytest.mark.parametrize("name", ["--help", "acme/../escape", "acme/tasks\n"])
    async def test_refuses_malformed_names(self, monkeypatch: pytest.MonkeyPatch, name: str) -> None:
        async def run(*_command: str, **_options: object) -> MockProcess:
            pytest.fail("malformed name reached Harbor")

        monkeypatch.setattr(asyncio, "create_subprocess_exec", run)

        with pytest.raises(ValkyrieInputError, match="Invalid Harbor dataset"):
            async with download_dataset("harbor", name):
                pytest.fail("malformed name accepted")


@pytest.mark.description("An unknown location is refused before anything is downloaded.")
async def test_unknown_location_is_refused() -> None:
    with pytest.raises(ValkyrieInputError, match="Invalid dataset location"):
        async with download_dataset("gitlab", "acme/tasks"):  # pyright: ignore[reportArgumentType]
            pytest.fail("unknown location accepted")


@pytest.mark.description("`datasets.install` clones a GitHub `owner/name` shorthand and publishes the tasks it holds.")
async def test_install_clones_github_shorthand_and_publishes(
    make_client: Callable[..., ValkyrieClient], monkeypatch: pytest.MonkeyPatch
) -> None:
    clones: list[str] = []
    published: list[dict[str, object]] = []

    async def git(_repository: Path | None, *arguments: str) -> str:
        if arguments[0] == "clone":
            clones.append(arguments[-2])
            _write_task(Path(arguments[-1]) / "tasks" / "task-a")
        return ""

    def tracker(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=_DATASET)
        if request.url.path.endswith("/blobs/missing"):
            return httpx.Response(200, json={"missing": json.loads(request.content)["digests"]})
        digest = request.url.path.rsplit("/", 1)[1]
        if "/blobs/" in request.url.path:
            return httpx.Response(201, json={"digest": digest, "size": len(request.content)})
        published.append(json.loads(request.content))
        return httpx.Response(
            201,
            json={
                "benchmark": "swe-bench",
                "dataset": "verified",
                "revision": 1,
                "digest": "sha256:" + "ab" * 32,
                "task_count": 1,
                "tags": ["latest"],
                "created_at": "2026-10-24T04:20:24+00:00",
                "tasks": [],
            },
        )

    monkeypatch.setattr(github_download, "_run_git_command", git)
    async with make_client(tracker) as client:
        version = await client.datasets.install("swe-bench", "verified", "acme/tasks", location="github")

    assert clones == ["https://github.com/acme/tasks"]
    assert [list(body["tasks"]) for body in published] == [["task-a"]]
    assert version.revision == 1


@pytest.mark.description("A failed Hugging Face file cancels the other downloads before the error is raised.")
async def test_failed_hugging_face_file_cancels_the_other_downloads(monkeypatch: pytest.MonkeyPatch) -> None:
    started = asyncio.Event()
    finished: list[str] = []

    async def handle(_transport: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/api/"):
            return httpx.Response(200, json=[{"type": "file", "path": "slow"}, {"type": "file", "path": "broken"}])
        if request.url.path.endswith("/broken"):
            await started.wait()
            return httpx.Response(500)
        started.set()
        await asyncio.sleep(1)
        finished.append(request.url.path)
        return httpx.Response(200, content=b"late")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle)

    with pytest.raises(ValkyrieTransportError):
        async with download_dataset("hugging_face", "acme/tasks"):
            pytest.fail("failed download yielded a folder")

    assert asyncio.all_tasks() == {asyncio.current_task()}
    assert finished == []


@pytest.mark.description("`datasets.install` refuses an invalid dataset name or tag before downloading anything.")
@pytest.mark.parametrize(("dataset", "tags"), [("Bad Name", ()), ("verified", ("12",))])
async def test_install_validates_before_downloading(
    make_client: Callable[..., ValkyrieClient], monkeypatch: pytest.MonkeyPatch, dataset: str, tags: tuple[str, ...]
) -> None:
    def fetch(*_arguments: object) -> None:
        pytest.fail("download started before validation")

    monkeypatch.setattr("valkyrie.sdk.resources.datasets.download_dataset", fetch)
    async with make_client(lambda _request: pytest.fail("request sent before validation")) as client:
        with pytest.raises(ValkyrieInputError):
            await client.datasets.install("swe-bench", dataset, "acme/tasks", location="github", tags=tags)
