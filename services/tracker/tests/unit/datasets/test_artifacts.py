import io
import os
import tarfile
from collections.abc import AsyncIterable
from pathlib import Path
from typing import cast

import pytest
from benchmark_service.sandbox.types import ExecResult, Sandbox

from tests.unit.datasets.support import ShellSandbox
from tracker.services.datasets import artifacts
from tracker.services.datasets.artifacts import (
    discard_staged,
    run_collect_hooks,
    stage_artifacts,
    upload_artifacts,
)
from tracker.services.datasets.config import ArtifactConfig, CollectConfig


@pytest.fixture
def sandbox(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Sandbox:
    monkeypatch.setattr(artifacts, "STAGING_ROOT", str(tmp_path))
    return cast(Sandbox, ShellSandbox())


def member_names(archive: bytes) -> list[str]:
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        return sorted(member.name for member in tar.getmembers() if member.isfile())


def staging_folders(tmp_path: Path) -> list[Path]:
    return list(tmp_path.glob("valkyrie-artifacts-*"))


@pytest.mark.description("Hooks snapshot state before artifacts are read, and never fail the task.")
class TestRunCollectHooks:
    @pytest.mark.description("A hook runs in the tasks working directory.")
    async def test_a_hook_runs_in_the_tasks_working_directory(self, sandbox: Sandbox, tmp_path: Path) -> None:
        messages: list[str] = []

        await run_collect_hooks(
            {"main": sandbox}, [CollectConfig(command="echo dump > snapshot.txt")], messages.append, str(tmp_path)
        )

        assert (tmp_path / "snapshot.txt").read_text() == "dump\n"
        assert messages == []

    @pytest.mark.description("A failing hook is reported and the next still runs.")
    async def test_a_failing_hook_is_reported_and_the_next_still_runs(self, sandbox: Sandbox, tmp_path: Path) -> None:
        messages: list[str] = []
        hooks = [CollectConfig(command="exit 7"), CollectConfig(command=f"touch {tmp_path}/second")]

        await run_collect_hooks({"main": sandbox}, hooks, messages.append, str(tmp_path))

        assert messages == ["The collect hook 'exit 7' exited with code 7"]
        assert (tmp_path / "second").exists()

    @pytest.mark.description("Only a hook on main runs in the tasks working directory.")
    async def test_only_a_hook_on_main_runs_in_the_tasks_working_directory(self) -> None:
        class RecordingSandbox:
            def __init__(self) -> None:
                self.cwds: list[str | None] = []

            async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
                self.cwds.append(cwd)
                return ExecResult(exit_code=0, output="")

        main, sidecar = RecordingSandbox(), RecordingSandbox()
        hooks = [CollectConfig(command="a"), CollectConfig(command="b", service="db")]

        await run_collect_hooks(
            {"main": cast(Sandbox, main), "db": cast(Sandbox, sidecar)}, hooks, lambda _message: None, "/app"
        )

        assert (main.cwds, sidecar.cwds) == (["/app"], [None])

    @pytest.mark.description("A hook that raises is reported instead of raised.")
    async def test_a_hook_that_raises_is_reported_instead_of_raised(self) -> None:
        class BrokenSandbox:
            async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
                raise TimeoutError("timed out")

        messages: list[str] = []

        await run_collect_hooks(
            {"main": cast(Sandbox, BrokenSandbox())}, [CollectConfig(command="slow")], messages.append, "/"
        )

        assert messages == ["The collect hook 'slow' failed: timed out"]


@pytest.mark.description("Each artifact becomes one tar.gz named for its destination.")
class TestStageAndUpload:
    @pytest.mark.description("A folder and a file are archived with exclusions and uploaded.")
    async def test_a_folder_and_a_file_are_archived_with_exclusions_and_uploaded(
        self, sandbox: Sandbox, tmp_path: Path
    ) -> None:
        out = tmp_path / "out"
        (out / "nested").mkdir(parents=True)
        (out / "result.txt").write_text("42")
        (out / "nested" / "cache.tmp").write_text("junk")
        (out / "nested" / "kept.txt").write_text("kept")
        (tmp_path / "work").mkdir()
        (tmp_path / "work" / "log.txt").write_text("log")
        uploaded: dict[str, bytes] = {}

        async def sink(task_id: str, name: str, chunks: AsyncIterable[bytes]) -> None:
            uploaded[f"{task_id}:{name}"] = b"".join([chunk async for chunk in chunks])

        staged, skipped = await stage_artifacts(
            {"main": sandbox},
            [ArtifactConfig(source=str(out), exclude=["*.tmp"]), ArtifactConfig(source="log.txt", destination="logs")],
            str(tmp_path / "work"),
        )
        await upload_artifacts("task-1", {"main": sandbox}, staged, sink, lambda _message: None)
        await discard_staged({"main": sandbox}, staged)

        folder = f"task-1:artifacts/{str(out).strip('/')}.tar.gz"
        assert skipped == []
        assert [artifact.source for artifact in staged] == [str(out), str(tmp_path / "work" / "log.txt")]
        assert sorted(uploaded) == sorted([folder, "task-1:artifacts/logs.tar.gz"])
        assert member_names(uploaded[folder]) == ["out/nested/kept.txt", "out/result.txt"]
        assert member_names(uploaded["task-1:artifacts/logs.tar.gz"]) == ["log.txt"]
        assert staging_folders(tmp_path) == []

    @pytest.mark.description("Staging uses a private folder with an unpredictable name.")
    async def test_staging_uses_a_private_folder_with_an_unpredictable_name(
        self, sandbox: Sandbox, tmp_path: Path
    ) -> None:
        (tmp_path / "one.txt").write_text("1")

        staged, _ = await stage_artifacts({"main": sandbox}, [ArtifactConfig(source="one.txt")], str(tmp_path))
        (folder,) = staging_folders(tmp_path)

        assert os.stat(folder).st_mode & 0o777 == 0o700
        assert len(folder.name) > len("valkyrie-artifacts-") + 16
        assert staged[0].archive_path.startswith(str(folder))

    @pytest.mark.description("A source that cannot be archived is skipped with its reason.")
    @pytest.mark.parametrize(
        ("setup", "source", "reason", "absent"),
        [
            ("nothing", "/no/such/path", "nothing was written there", True),
            ("link", "link-to-out", "it is a symbolic link, which isn't followed", False),
        ],
    )
    async def test_a_source_that_cannot_be_archived_is_skipped_with_its_reason(
        self, sandbox: Sandbox, tmp_path: Path, setup: str, source: str, reason: str, absent: bool
    ) -> None:
        (tmp_path / "real").mkdir()
        (tmp_path / "link-to-out").symlink_to(tmp_path / "real")

        staged, skipped = await stage_artifacts({"main": sandbox}, [ArtifactConfig(source=source)], str(tmp_path))

        assert staged == []
        assert [(item.reason, item.absent) for item in skipped] == [(reason, absent)]
        assert staging_folders(tmp_path) == []

    @pytest.mark.description(
        "A folder with too many files after exclusions is skipped but excluded files do not count."
    )
    async def test_a_folder_with_too_many_files_after_exclusions_is_skipped_but_excluded_files_do_not_count(
        self, sandbox: Sandbox, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(artifacts, "MAX_ARTIFACT_FILES", 5)
        crowded = tmp_path / "crowded"
        (crowded / "node_modules").mkdir(parents=True)
        for index in range(20):
            (crowded / "node_modules" / f"dep{index}").write_text("x")
        (crowded / "kept.txt").write_text("kept")
        too_many = tmp_path / "too-many"
        too_many.mkdir()
        for index in range(8):
            (too_many / f"file{index}").write_text("x")

        staged, skipped = await stage_artifacts(
            {"main": sandbox},
            [ArtifactConfig(source=str(crowded), exclude=["node_modules"]), ArtifactConfig(source=str(too_many))],
            str(tmp_path),
        )

        assert [artifact.source for artifact in staged] == [str(crowded)]
        assert [item.reason for item in skipped] == ["it holds more than 5 files"]

    @pytest.mark.description("An archive over the size limit is stopped and skipped.")
    async def test_an_archive_over_the_size_limit_is_stopped_and_skipped(
        self, sandbox: Sandbox, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(artifacts, "MAX_ARTIFACT_BYTES", 2048)
        (tmp_path / "big.bin").write_bytes(os.urandom(200_000))

        staged, skipped = await stage_artifacts({"main": sandbox}, [ArtifactConfig(source="big.bin")], str(tmp_path))

        assert staged == []
        assert len(skipped) == 1
        assert staging_folders(tmp_path) == []

    @pytest.mark.description("A source spelled with dots or a trailing slash is archived as the folder.")
    @pytest.mark.parametrize("source", ["out/.", "out/", "./out"])
    async def test_a_source_spelled_with_dots_or_a_trailing_slash_is_archived_as_the_folder(
        self, sandbox: Sandbox, tmp_path: Path, source: str
    ) -> None:
        (tmp_path / "out").mkdir()
        (tmp_path / "out" / "result.txt").write_text("42")

        staged, skipped = await stage_artifacts({"main": sandbox}, [ArtifactConfig(source=source)], str(tmp_path))
        archive = b"".join([chunk async for chunk in sandbox.stream_download(staged[0].archive_path)])

        assert skipped == []
        assert staged[0].source == str(tmp_path / "out")
        assert member_names(archive) == ["out/result.txt"]

    @pytest.mark.description("A source that starts with a dash is a name and not an option.")
    async def test_a_source_that_starts_with_a_dash_is_a_name_and_not_an_option(
        self, sandbox: Sandbox, tmp_path: Path
    ) -> None:
        (tmp_path / "--version").write_text("not an option")

        staged, skipped = await stage_artifacts({"main": sandbox}, [ArtifactConfig(source="--version")], str(tmp_path))

        assert skipped == []
        assert [artifact.name for artifact in staged] == ["--version.tar.gz"]

    @pytest.mark.description("Output with warnings before the count is still counted.")
    async def test_output_with_warnings_before_the_count_is_still_counted(self, tmp_path: Path) -> None:
        class NoisySandbox:
            async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
                if command.endswith("| wc -l"):
                    return ExecResult(exit_code=0, output="tar: Removing leading `/' from member names\n3\n")
                return ExecResult(exit_code=1 if command.startswith("test -L") else 0, output="")

        staged, skipped = await stage_artifacts(
            {"main": cast(Sandbox, NoisySandbox())}, [ArtifactConfig(source="/a/b")], "/"
        )

        assert skipped == []
        assert len(staged) == 1

    @pytest.mark.description("An error while archiving skips that artifact and keeps going.")
    async def test_an_error_while_archiving_skips_that_artifact_and_keeps_going(self, tmp_path: Path) -> None:
        class FlakySandbox:
            async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
                if "/a/b" in command:
                    raise TimeoutError("provider timed out")
                return ExecResult(exit_code=0, output="")

        staged, skipped = await stage_artifacts(
            {"main": cast(Sandbox, FlakySandbox())}, [ArtifactConfig(source="/a/b")], "/"
        )

        assert staged == []
        assert [item.reason for item in skipped] == ["TimeoutError: provider timed out"]

    @pytest.mark.description("An upload that fails is reported and the next artifact is still saved.")
    async def test_an_upload_that_fails_is_reported_and_the_next_artifact_is_still_saved(
        self, sandbox: Sandbox, tmp_path: Path
    ) -> None:
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "b.txt").write_text("b")
        staged, _ = await stage_artifacts(
            {"main": sandbox}, [ArtifactConfig(source="a.txt"), ArtifactConfig(source="b.txt")], str(tmp_path)
        )
        saved: list[str] = []
        messages: list[str] = []

        async def sink(task_id: str, name: str, chunks: AsyncIterable[bytes]) -> None:
            if name.endswith("a.txt.tar.gz"):
                raise OSError("store unavailable")
            saved.append(name)

        await upload_artifacts("task-1", {"main": sandbox}, staged, sink, messages.append)

        assert saved == ["artifacts/b.txt.tar.gz"]
        assert messages == [f"Artifact {tmp_path / 'a.txt'} could not be saved: store unavailable"]
