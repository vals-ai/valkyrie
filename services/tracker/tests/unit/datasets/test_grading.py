import shlex
from pathlib import Path

import pytest
from benchmark_service.sandbox.types import ExecResult

from tracker.services.datasets.config import TaskConfig
from tracker.services.datasets.grading import (
    REWARD_JSON_PATH,
    REWARD_TEXT_PATH,
    TEST_SCRIPT_PATH,
    VERIFIER_OUTPUT_PATH,
    VerifierResultError,
    grade_task,
    upload_dir,
    verifier_command,
)
from tests.unit.datasets.support import FakeSandbox

CONFIG = TaskConfig.parse("[verifier]\ntimeout_sec = 30\nenv = { MODE = 'test', NAME = \"it's\" }")


def write_tests_folder(tmp_path: Path) -> Path:
    folder = tmp_path / "tests"
    (folder / "cases").mkdir(parents=True)
    (folder / "test.sh").write_text("#!/bin/bash\n")
    (folder / "cases" / "one.txt").write_text("1")
    (folder / "cases" / "two.txt").write_text("2")
    return folder


@pytest.mark.description("Putting the tests where the grader expects them.")
class TestUploadDir:
    @pytest.mark.description("Every file is uploaded after its directories exist.")
    async def test_every_file_is_uploaded_after_its_directories_exist(self, tmp_path: Path) -> None:
        sandbox = FakeSandbox()

        await upload_dir(sandbox, write_tests_folder(tmp_path), "/tests")

        assert sandbox.files == {
            "/tests/cases/one.txt": b"1",
            "/tests/cases/two.txt": b"2",
            "/tests/test.sh": b"#!/bin/bash\n",
        }
        assert sandbox.commands == ["mkdir -p /tests /tests/cases"]

    @pytest.mark.description("Files that are executable on disk stay executable once they are uploaded.")
    async def test_files_that_are_executable_on_disk_stay_executable_once_they_are_uploaded(
        self, tmp_path: Path
    ) -> None:
        class OrderedSandbox(FakeSandbox):
            def __init__(self) -> None:
                super().__init__()
                self.order: list[str] = []

            async def upload_file(self, remote_path: str, content: bytes) -> None:
                self.order.append("upload")
                await super().upload_file(remote_path, content)

            async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
                self.order.append(command.split()[0])
                return await super().exec(command, cwd=cwd, timeout=timeout)

        folder = write_tests_folder(tmp_path)
        for name, mode in {"run.sh": 0o755, "cases/tool": 0o700, "cases/group": 0o610}.items():
            (folder / name).write_text("x")
            (folder / name).chmod(mode)
        sandbox = OrderedSandbox()

        await upload_dir(sandbox, folder, "/tests")

        assert sandbox.order == ["mkdir", *["upload"] * 6, "chmod"]
        assert sandbox.commands[-1] == "chmod +x /tests/cases/group /tests/cases/tool /tests/run.sh"

    @pytest.mark.description("Many directories are created in batches.")
    async def test_many_directories_are_created_in_batches(self, tmp_path: Path) -> None:
        for number in range(120):
            (tmp_path / f"d{number:03}").mkdir()
            (tmp_path / f"d{number:03}" / "f").write_text("x")
        sandbox = FakeSandbox()

        await upload_dir(sandbox, tmp_path, "/tests")

        assert len(sandbox.commands) == 3
        assert len(sandbox.files) == 120


@pytest.mark.description("The shell line that runs the grader.")
class TestVerifierCommand:
    @pytest.mark.description("The script runs under bash with its output saved.")
    def test_the_script_runs_under_bash_with_its_output_saved(self) -> None:
        command = verifier_command({})

        assert command.startswith("bash -c ")
        assert TEST_SCRIPT_PATH in command
        assert VERIFIER_OUTPUT_PATH in command

    @pytest.mark.description("A user runs the script under that account.")
    def test_a_user_runs_the_script_under_that_account(self) -> None:
        command = verifier_command({"MODE": "test"}, "grader")

        assert "getent passwd grader" in command
        assert TEST_SCRIPT_PATH in command and "MODE=test" in command

    @pytest.mark.description("Environment values are quoted.")
    def test_environment_values_are_quoted(self) -> None:
        command = verifier_command({"NAME": "it's; rm -rf /"})

        assert command.startswith("env ")
        assert shlex.split(command)[1] == "NAME=it's; rm -rf /"


@pytest.mark.description("The result of a grading run.")
class TestGradeTask:
    @pytest.mark.description("A verifier user gets the tests and log directory it needs.")
    async def test_a_verifier_user_gets_the_tests_and_log_directory_it_needs(self, tmp_path: Path) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})
        config = TaskConfig.parse("[verifier]\nuser = 'grader'")

        await grade_task("task-1", config, {}, write_tests_folder(tmp_path), sandbox)

        assert "chmod -R a+rX /tests; chown -R grader /logs/verifier" in sandbox.commands
        assert any("getent passwd grader" in command for command in sandbox.commands)

    @pytest.mark.description("A reward file becomes the result.")
    async def test_a_reward_file_becomes_the_result(self, tmp_path: Path) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1\n", VERIFIER_OUTPUT_PATH: b"all passed"})
        messages: list[str] = []

        result = await grade_task(
            "task-1", CONFIG, CONFIG.verifier.env, write_tests_folder(tmp_path), sandbox, messages.append
        )

        assert result["rewards"] == {"reward": 1.0}
        assert result["verifier_result"] == {"rewards": {"reward": 1.0}}
        assert result["metadata"] == {
            "reward_source": REWARD_TEXT_PATH,
            "verifier_exit_code": 0,
            "verifier_output": "all passed",
        }
        assert messages == ["Running the verifier for task-1", 'Verifier rewards for task-1: {"reward": 1.0}']
        assert sandbox.timeouts == [30]
        assert f"chmod +x {TEST_SCRIPT_PATH}" in sandbox.commands

    @pytest.mark.description("A json reward file is preferred and keeps every key.")
    async def test_a_json_reward_file_is_preferred_and_keeps_every_key(self, tmp_path: Path) -> None:
        sandbox = FakeSandbox(writes={REWARD_JSON_PATH: b'{"reward": 0.5, "tests_passed": 4}', REWARD_TEXT_PATH: b"1"})

        result = await grade_task("task-1", CONFIG, CONFIG.verifier.env, write_tests_folder(tmp_path), sandbox)

        assert result["rewards"] == {"reward": 0.5, "tests_passed": 4.0}
        assert result["metadata"]["reward_source"] == REWARD_JSON_PATH

    @pytest.mark.description("A grader that fails but wrote a reward still scores.")
    async def test_a_grader_that_fails_but_wrote_a_reward_still_scores(self, tmp_path: Path) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"0"}, exit_code=1)

        result = await grade_task("task-1", CONFIG, CONFIG.verifier.env, write_tests_folder(tmp_path), sandbox)

        assert result["rewards"] == {"reward": 0.0}
        assert result["metadata"]["verifier_exit_code"] == 1

    @pytest.mark.description("The end of a long output is what is saved.")
    async def test_the_end_of_a_long_output_is_what_is_saved(self, tmp_path: Path) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1", VERIFIER_OUTPUT_PATH: b"a" * 30_000 + b"the end"})

        result = await grade_task("task-1", CONFIG, CONFIG.verifier.env, write_tests_folder(tmp_path), sandbox)

        saved = result["metadata"]["verifier_output"]
        assert len(saved) == 20_000
        assert saved.endswith("the end")

    @pytest.mark.description("The verifier folder is emptied and made private before the grader runs.")
    async def test_the_verifier_folder_is_emptied_and_made_private_before_the_grader_runs(self, tmp_path: Path) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"1"})

        await grade_task("task-1", CONFIG, CONFIG.verifier.env, write_tests_folder(tmp_path), sandbox)

        reset = sandbox.commands.index("find /logs/verifier -mindepth 1 -delete; chmod 755 /logs/verifier")
        assert reset < next(index for index, command in enumerate(sandbox.commands) if "bash -c" in command)

    @pytest.mark.description("A reward left before the grader ran is not scored.")
    async def test_a_reward_left_before_the_grader_ran_is_not_scored(self, tmp_path: Path) -> None:
        sandbox = FakeSandbox(files={REWARD_TEXT_PATH: b"1"})

        with pytest.raises(VerifierResultError, match="wrote no reward file"):
            await grade_task("task-1", CONFIG, CONFIG.verifier.env, write_tests_folder(tmp_path), sandbox)

    @pytest.mark.description("No reward file after a failed run reports the redacted tail of its output.")
    async def test_no_reward_file_after_a_failed_run_reports_the_redacted_tail_of_its_output(
        self, tmp_path: Path
    ) -> None:
        sandbox = FakeSandbox(
            writes={VERIFIER_OUTPUT_PATH: b"x" * 30_000 + b"Traceback: boom token=s3cret"}, exit_code=2
        )

        with pytest.raises(VerifierResultError) as error:
            await grade_task(
                "task-1",
                CONFIG,
                CONFIG.verifier.env,
                write_tests_folder(tmp_path),
                sandbox,
                redact=lambda text: text.replace("s3cret", "[REDACTED]"),
            )

        message = str(error.value)
        assert message.startswith("The verifier failed with exit code 2: ")
        assert message.endswith("Traceback: boom token=[REDACTED]")
        assert len(message) < 20_100

    @pytest.mark.description("A provider failure while reading the reward is not a verifier error.")
    async def test_a_provider_failure_while_reading_the_reward_is_not_a_verifier_error(self, tmp_path: Path) -> None:
        class BrokenDownload(FakeSandbox):
            async def download_file(self, remote_path: str) -> bytes:
                raise TypeError("provider bug")

        sandbox = BrokenDownload(writes={REWARD_TEXT_PATH: b"1"})

        with pytest.raises(TypeError, match="provider bug"):
            await grade_task("task-1", CONFIG, CONFIG.verifier.env, write_tests_folder(tmp_path), sandbox)

    @pytest.mark.description("A long reward that is not a number is cut down in the error.")
    async def test_a_long_reward_that_is_not_a_number_is_cut_down_in_the_error(self, tmp_path: Path) -> None:
        sandbox = FakeSandbox(writes={REWARD_TEXT_PATH: b"z" * 100_000})

        with pytest.raises(VerifierResultError) as error:
            await grade_task("task-1", CONFIG, CONFIG.verifier.env, write_tests_folder(tmp_path), sandbox)

        assert len(str(error.value)) == 20_000

    @pytest.mark.description("A secret at the edge of the size limit is redacted whole.")
    async def test_a_secret_at_the_edge_of_the_size_limit_is_redacted_whole(self, tmp_path: Path) -> None:
        sandbox = FakeSandbox(writes={VERIFIER_OUTPUT_PATH: b"s3cret" + b"y" * 19_997}, exit_code=2)

        with pytest.raises(VerifierResultError) as error:
            await grade_task(
                "task-1",
                CONFIG,
                CONFIG.verifier.env,
                write_tests_folder(tmp_path),
                sandbox,
                redact=lambda text: text.replace("s3cret", "[REDACTED]"),
            )

        assert "ret" not in str(error.value).removeprefix("The verifier failed with exit code 2: ")

    @pytest.mark.description("A reward that cannot be read as a number is a redacted error.")
    @pytest.mark.parametrize(
        ("writes", "message"),
        [
            ({REWARD_JSON_PATH: b'{"reward": null}'}, "Expected a numeric reward"),
            ({REWARD_JSON_PATH: b'{"reward": ' + b"9" * 400 + b"}"}, "too large"),
            ({REWARD_TEXT_PATH: b"Traceback token=s3cret"}, r"token=\[REDACTED\]"),
            ({REWARD_JSON_PATH: b"[" * 200_000 + b"]" * 200_000}, "must hold an object|nesting|recursion"),
        ],
        ids=["null", "huge integer", "text", "deep nesting"],
    )
    async def test_a_reward_that_cannot_be_read_as_a_number_is_a_redacted_error(
        self, tmp_path: Path, writes: dict[str, bytes], message: str
    ) -> None:
        with pytest.raises(VerifierResultError, match=message):
            await grade_task(
                "task-1",
                CONFIG,
                CONFIG.verifier.env,
                write_tests_folder(tmp_path),
                FakeSandbox(writes=writes),
                redact=lambda text: text.replace("s3cret", "[REDACTED]"),
            )

    @pytest.mark.description(
        "A reward file that is missing, empty, not finite or not an object is an error, not a zero."
    )
    @pytest.mark.parametrize(
        ("writes", "complaint"),
        [
            ({}, "wrote no reward file"),
            ({REWARD_TEXT_PATH: b""}, "reward file is empty"),
            ({REWARD_JSON_PATH: b""}, "reward file is empty"),
            ({REWARD_TEXT_PATH: b"nan"}, "not a finite number"),
            ({REWARD_JSON_PATH: b'{"reward": Infinity}'}, "not a finite number"),
            ({REWARD_JSON_PATH: b"[1, 2]"}, "must hold an object"),
        ],
        ids=["missing", "empty-text", "empty-json", "nan-text", "infinity-json", "not-an-object"],
    )
    async def test_a_bad_reward_file_is_an_error_not_a_zero(
        self, tmp_path: Path, writes: dict[str, bytes], complaint: str
    ) -> None:
        with pytest.raises(VerifierResultError, match=complaint):
            await grade_task(
                "task-1", CONFIG, CONFIG.verifier.env, write_tests_folder(tmp_path), FakeSandbox(writes=writes)
            )
