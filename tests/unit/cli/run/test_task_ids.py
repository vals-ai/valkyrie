import io
from pathlib import Path

import click
import pytest

from valkyrie.cli.run import task_ids as task_ids_module
from valkyrie.cli.run.task_ids import read_task_ids_source


class FakeResponse(io.BytesIO):
    def __init__(self, body: bytes, missing_bytes: int) -> None:
        super().__init__(body)
        self.length = missing_bytes


def serve_body(monkeypatch: pytest.MonkeyPatch, body: bytes, missing_bytes: int = 0) -> list[float | None]:
    timeouts: list[float | None] = []

    def urlopen(url: str, timeout: float | None = None) -> FakeResponse:
        timeouts.append(timeout)
        return FakeResponse(body, missing_bytes)

    monkeypatch.setattr(task_ids_module.urllib.request, "urlopen", urlopen)
    return timeouts


class TestReadTaskIdsSource:
    @pytest.mark.description("A remote task ID file is fetched with a timeout and parsed one ID per line.")
    def test_remote_file_uses_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        timeouts = serve_body(monkeypatch, b"task-a\ntask-b\n")

        assert read_task_ids_source("https://example.com/ids.txt") == ["task-a", "task-b"]
        assert timeouts == [task_ids_module._REMOTE_TIMEOUT_SECONDS]

    @pytest.mark.description("A remote task ID file over the size cap fails with a CLI error instead of loading it.")
    def test_remote_file_over_cap_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        serve_body(monkeypatch, b"t\n" * (task_ids_module._REMOTE_MAX_BYTES // 2 + 1), missing_bytes=1)

        with pytest.raises(click.ClickException, match="exceed 16 MiB"):
            read_task_ids_source("https://example.com/ids.txt")

    @pytest.mark.description(
        "A remote task ID file that ends before its `Content-Length` fails instead of parsing a partial list."
    )
    def test_truncated_remote_file_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        serve_body(monkeypatch, b"task-a\ntask-b\ntask-c", missing_bytes=80)

        with pytest.raises(click.ClickException, match="were truncated"):
            read_task_ids_source("https://example.com/ids.txt")

    @pytest.mark.description(
        (
            "A task ID file that is not UTF-8 fails with a CLI error instead of a decoding traceback.\n"
            "\n"
            "Test cases:\n"
            "- A remote file.\n"
            "- A local file."
        )
    )
    def test_non_utf8_file_is_cli_error(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        serve_body(monkeypatch, b"task-a\n\xff\n")
        local_file = tmp_path / "ids.txt"
        local_file.write_bytes(b"task-a\n\xff\n")

        for source in ("https://example.com/ids.txt", str(local_file)):
            with pytest.raises(click.ClickException, match="not valid UTF-8"):
                read_task_ids_source(source)
