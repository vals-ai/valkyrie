import io
import json
import tarfile
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner
from valkyrie.sdk.resources.artifacts import ArtifactsResource

from valkyrie.cli.run.outputs import files as artifacts, extract_run_outputs, download as outputs

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import make_fetch_metadata, ok

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


@pytest.mark.description("Selected artifact paths match whole task IDs, including slashes, but not adjacent names.")
@pytest.mark.parametrize(
    ("task_id", "path", "matched"),
    [
        ("task", "task/result.txt", True),
        ("group/task", "group/task/result.txt", True),
        ("group/task", "group/task", True),
        ("group/task", "group/task-other/result.txt", False),
        ("group/task", "group/other/result.txt", False),
    ],
)
def test_selected_artifact_paths_match_task_id_boundaries(
    cli_runner: CliRunner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    task_id: str,
    path: str,
    matched: bool,
) -> None:
    downloads: list[str] = []

    async def download(
        self: ArtifactsResource, run_id: UUID, output_dir: str | Path, *, path: str = "", attempts: bool = False
    ) -> Path:
        downloads.append(path)
        destination = Path(output_dir)
        destination.mkdir()
        (destination / "result.txt").write_text("result")
        return destination

    monkeypatch.setattr(ArtifactsResource, "download", download)
    output_dir = tmp_path / "selected"

    result = cli_runner.invoke(outputs, [str(_RUN_ID), path, "--task-ids", task_id, "--output", str(output_dir)])

    assert result.exit_code == (0 if matched else 1), result.output
    assert downloads == ([path] if matched else [])
    assert output_dir.exists() is matched
    if matched:
        assert (output_dir / "result.txt").read_text() == "result"
    else:
        assert "No artifacts found" in result.output


def _tar(files: dict[str, bytes], mode: str = "w") -> bytes:
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode=mode) as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))

    return archive.getvalue()


@pytest.mark.description("The default directory names the benchmark, agent and run. Selected task IDs reach Tracker.")
@pytest.mark.parametrize("json_output", [False, True])
def test_outputs_extract_into_a_named_directory_and_unpack_task_archives(
    tracker: MockTracker, cli_runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, json_output: bool
) -> None:
    nested = _tar({"nested.txt": b"nested contents"}, mode="w:gz")
    tracker.reply("GET", f"/fetch-benchmark-metadata/{_RUN_ID}", ok(make_fetch_metadata(_RUN_ID)))
    tracker.reply(
        "GET",
        f"/fetch-run-outputs/{_RUN_ID}",
        httpx.Response(200, content=_tar({"task-a/output.txt": b"run output", "task-a/artifacts.tar.gz": nested})),
    )
    monkeypatch.chdir(tmp_path)

    result = cli_runner.invoke(
        outputs, [str(_RUN_ID), "--task-ids", "task-a, task-b", *(["--json"] if json_output else [])]
    )

    assert result.exit_code == 0, result.output
    assert tracker.requested("GET", f"/fetch-run-outputs/{_RUN_ID}")[0].url.params.get_list("task_ids") == [
        "task-a",
        "task-b",
    ]

    output_dir = tmp_path / f"swebench_mini_sweagent_{_RUN_ID}"
    if json_output:
        assert json.loads(result.stdout) == {"run_id": str(_RUN_ID), "path": output_dir.name}
        assert "Extracting archives" in result.stderr
        assert "Unpacking 1 nested" in result.stderr
    else:
        assert f"Run outputs extracted to: {output_dir.name}" in result.output
    assert (output_dir / "task-a" / "output.txt").read_bytes() == b"run output"
    assert (output_dir / "task-a" / "artifacts" / "nested.txt").read_bytes() == b"nested contents"
    assert not (output_dir / "task-a" / "artifacts.tar.gz").exists()


@pytest.mark.description("Output extraction rejects paths that escape the archive.")
@pytest.mark.parametrize("nested", [False, True], ids=["outer-archive", "nested-archive"])
def test_extract_run_outputs_rejects_archive_traversal(nested: bool, tmp_path: Path) -> None:
    if nested:
        archive = _tar({"task/artifacts.tar.gz": _tar({"../../escaped.txt": b"escaped"}, mode="w:gz")})
        escaped_path = tmp_path / "output" / "escaped.txt"
    else:
        archive = _tar({"../escaped.txt": b"escaped"})
        escaped_path = tmp_path / "escaped.txt"

    with pytest.raises(tarfile.FilterError):
        extract_run_outputs(io.BytesIO(archive), tmp_path / "output")

    assert not escaped_path.exists()


@pytest.mark.description("Run artifacts with --attempts lists the artifacts kept from attempts replaced by a retry.")
def test_artifacts_attempts_lists_kept_attempt_artifacts(tracker: MockTracker, cli_runner: CliRunner) -> None:
    kept = "task-a/attempt-1/vals_format/result.json"
    tracker.reply(
        "GET", f"/benchmarks/{_RUN_ID}/artifacts", httpx.Response(200, json={"artifacts": [{"path": kept, "size": 2}]})
    )

    result = cli_runner.invoke(artifacts, [str(_RUN_ID), "task-a", "--attempts"])

    assert result.exit_code == 0, result.output
    assert kept in result.output
    assert tracker.requested("GET", f"/benchmarks/{_RUN_ID}/artifacts")[0].url.params["attempts"] == "true"
