import json
import posixpath
import shlex
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from benchmark_service import SandboxNotFoundError
from benchmark_service.sandbox.types import ExecResult

from tracker.services.datasets.config import TaskConfig, is_table
from tracker.services.datasets.rewards import RewardMap, normalize_rewards
from tracker.services.datasets.user import run_as

TESTS_DIR = "/tests"
VERIFIER_DIR = "/logs/verifier"
AGENT_LOG_DIR = "/logs/agent"
ARTIFACTS_DIR = "/logs/artifacts"
LOG_DIRS = (AGENT_LOG_DIR, VERIFIER_DIR, ARTIFACTS_DIR)
OPEN_LOG_DIRS = f"mkdir -p {' '.join(LOG_DIRS)} && chmod 777 {' '.join(LOG_DIRS)}"
RESET_STEP_LOG_DIRS = f"find {AGENT_LOG_DIR} {ARTIFACTS_DIR} -mindepth 1 -delete"
TEST_SCRIPT_PATH = f"{TESTS_DIR}/test.sh"
REWARD_TEXT_PATH = f"{VERIFIER_DIR}/reward.txt"
REWARD_JSON_PATH = f"{VERIFIER_DIR}/reward.json"
VERIFIER_OUTPUT_PATH = f"{VERIFIER_DIR}/test-stdout.txt"
SAVED_OUTPUT_CHARS = 20_000
DIRECTORIES_PER_COMMAND = 50


class VerifierResultError(Exception):
    pass


class GradingSandbox(Protocol):
    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult: ...

    async def upload_file(self, remote_path: str, content: bytes) -> None: ...

    async def download_file(self, remote_path: str) -> bytes: ...


async def exec_in_batches(sandbox: GradingSandbox, command: str, arguments: list[str]) -> None:
    for start in range(0, len(arguments), DIRECTORIES_PER_COMMAND):
        batch = arguments[start : start + DIRECTORIES_PER_COMMAND]
        await sandbox.exec(f"{command} " + " ".join(shlex.quote(argument) for argument in batch))


async def make_directories(sandbox: GradingSandbox, directories: list[str]) -> None:
    await exec_in_batches(sandbox, "mkdir -p", directories)


async def upload_dir(sandbox: GradingSandbox, source: Path, target: str) -> None:
    files = sorted(path for path in source.rglob("*") if path.is_file())
    destinations = {path: f"{target.rstrip('/')}/{path.relative_to(source).as_posix()}" for path in files}
    directories = sorted({target, *(posixpath.dirname(destination) for destination in destinations.values())})
    await make_directories(sandbox, directories)
    for path, destination in destinations.items():
        await sandbox.upload_file(destination, path.read_bytes())
    executable = [destination for path, destination in destinations.items() if path.stat().st_mode & 0o111]
    await exec_in_batches(sandbox, "chmod +x", executable)


async def try_download(sandbox: GradingSandbox, remote_path: str) -> bytes | None:
    quoted = shlex.quote(remote_path)
    probe = await sandbox.exec(
        f"if [ -s {quoted} ]; then echo full; elif [ -e {quoted} ]; then echo empty; else echo missing; fi"
    )
    state = (probe.output.split() or [""])[-1]
    if state == "missing":
        return None
    if state == "empty":
        return b""
    try:
        return await sandbox.download_file(remote_path)
    except (FileNotFoundError, SandboxNotFoundError):
        return None


def parse_rewards(content: bytes, path: str, redact: Callable[[str], str]) -> RewardMap:
    try:
        if not content.strip():
            raise ValueError(f"The reward file is empty: {path}")
        if path == REWARD_TEXT_PATH:
            return normalize_rewards({"reward": content.decode().strip()})
        rewards = json.loads(content.decode())
        if not is_table(rewards):
            raise ValueError(f"The reward file must hold an object of numbers: {path}")
        return normalize_rewards(rewards)
    except (ValueError, TypeError, OverflowError, RecursionError) as error:
        raise VerifierResultError(redact(str(error))[:SAVED_OUTPUT_CHARS]) from None


async def download_rewards(sandbox: GradingSandbox, redact: Callable[[str], str]) -> tuple[RewardMap, str]:
    for path in (REWARD_JSON_PATH, REWARD_TEXT_PATH):
        content = await try_download(sandbox, path)
        if content is not None:
            return parse_rewards(content, path, redact), path
    raise FileNotFoundError("The verifier wrote no reward file")


def verifier_command(env: dict[str, str], user: str | None = None) -> str:
    script = f"({shlex.quote(TEST_SCRIPT_PATH)}) > {shlex.quote(VERIFIER_OUTPUT_PATH)} 2>&1"
    assignments = "".join(f"{shlex.quote(f'{name}={value}')} " for name, value in env.items())
    prefix = f"env {assignments}" if assignments else ""
    command = f"{prefix}bash -c {shlex.quote(script)}"
    return command if user is None else run_as(user, command)


def keep(text: str) -> str:
    return text


async def grade_task(
    task_id: str,
    config: TaskConfig,
    env: dict[str, str],
    tests_dir: Path,
    sandbox: GradingSandbox,
    on_message: Callable[[str], None] | None = None,
    *,
    redact: Callable[[str], str] = keep,
    upload_tests: bool = True,
    reset_tests: bool = False,
) -> dict[str, Any]:
    def say(message: str) -> None:
        if on_message is not None:
            on_message(message)

    grader = config.verifier.environment if config.verifier.is_separate else None
    cwd = (grader or config.environment).workdir or config.environment.workdir

    if upload_tests:
        if reset_tests:
            await sandbox.exec(f"rm -rf {shlex.quote(TESTS_DIR)}")
        await upload_dir(sandbox, tests_dir, TESTS_DIR)
    await sandbox.exec(f"chmod +x {shlex.quote(TEST_SCRIPT_PATH)}")
    await make_directories(sandbox, [VERIFIER_DIR, *([cwd] if cwd else [])])
    await sandbox.exec(f"find {shlex.quote(VERIFIER_DIR)} -mindepth 1 -delete; chmod 755 {shlex.quote(VERIFIER_DIR)}")
    if config.verifier.user is not None:
        await sandbox.exec(
            f"chmod -R a+rX {shlex.quote(TESTS_DIR)}; chown -R {shlex.quote(config.verifier.user)} {shlex.quote(VERIFIER_DIR)}"
        )

    say(f"Running the verifier for {task_id}")
    result = await sandbox.exec(
        verifier_command(env, config.verifier.user), cwd=cwd, timeout=config.verifier.timeout_sec
    )
    try:
        rewards, source = await download_rewards(sandbox, redact)
    except FileNotFoundError as error:
        if result.exit_code == 0:
            raise VerifierResultError(str(error)) from None
        output = await try_download(sandbox, VERIFIER_OUTPUT_PATH) or result.output.encode()
        shown = redact(output.decode(errors="replace"))[-SAVED_OUTPUT_CHARS:]
        raise VerifierResultError(f"The verifier failed with exit code {result.exit_code}: {shown}") from None

    output = await try_download(sandbox, VERIFIER_OUTPUT_PATH)
    say(f"Verifier rewards for {task_id}: {json.dumps(rewards, sort_keys=True)}")
    return {
        "task_id": task_id,
        "rewards": rewards,
        "verifier_result": {"rewards": rewards},
        "metadata": {
            "reward_source": source,
            "verifier_exit_code": result.exit_code,
            "verifier_output": redact(output.decode(errors="replace"))[-SAVED_OUTPUT_CHARS:]
            if output is not None
            else None,
        },
    }
