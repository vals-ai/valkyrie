import json
import math
import os
import shutil
import signal
import subprocess
import tempfile
import time
from collections.abc import Mapping
from pathlib import Path
from typing import cast

CONTRACT = 1
MAX_SCRIPT_BYTES = 256 * 1024
MAX_REWARDS_BYTES = 5 * 1024 * 1024
MAX_OUTPUT_BYTES = 64 * 1024
STDERR_TAIL_BYTES = 64 * 1024
SCRIPT_TIMEOUT_SECONDS = 300
WHEEL_DIRECTORY = Path("/opt/wheels")


class MetricError(Exception):
    pass


def script_env(work: Path) -> dict[str, str]:
    env = {
        "PATH": os.environ["PATH"],
        "HOME": str(work),
        "TMPDIR": str(work),
        "LANG": "C.UTF-8",
        "UV_CACHE_DIR": "/tmp/uv-cache",
        "UV_PYTHON_DOWNLOADS": "never",
        "UV_NO_PROGRESS": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    if WHEEL_DIRECTORY.is_dir():
        env["UV_FIND_LINKS"] = str(WHEEL_DIRECTORY)
    return env


def tail(path: Path, limit: int) -> str:
    size = path.stat().st_size
    with path.open("rb") as stream:
        stream.seek(max(0, size - limit))
        return stream.read().decode(errors="replace")


def kill_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def read_metrics(path: Path) -> dict[str, float]:
    if not path.is_file():
        raise MetricError("The script wrote no output file")
    if path.stat().st_size > MAX_OUTPUT_BYTES:
        raise MetricError(f"The output is larger than {MAX_OUTPUT_BYTES} bytes")
    try:
        loaded = cast(object, json.loads(path.read_text()))
    except ValueError as error:
        raise MetricError(f"The output is not JSON: {error}") from error
    if not isinstance(loaded, dict) or not loaded:
        raise MetricError("The output must be a non-empty JSON object")
    metrics: dict[str, float] = {}
    for key, value in cast(dict[object, object], loaded).items():
        if (
            not isinstance(key, str)
            or isinstance(value, bool)
            or not isinstance(value, int | float)
            or not math.isfinite(value)
        ):
            raise MetricError(f"The value for {key!r} is not a finite number")
        metrics[key] = float(value)
    return metrics


def input_lines(event: Mapping[str, object]) -> tuple[str, str]:
    if event.get("contract") != CONTRACT:
        raise MetricError(f"The event contract {event.get('contract')!r} is not {CONTRACT}")
    script = event.get("script")
    rewards = event.get("rewards")
    if not isinstance(script, str) or not isinstance(rewards, list):
        raise MetricError("The event needs a script and a list of rewards")
    if len(script.encode()) > MAX_SCRIPT_BYTES:
        raise MetricError(f"The script is larger than {MAX_SCRIPT_BYTES} bytes")
    lines: list[str] = []
    for reward in cast(list[object], rewards):
        if reward is not None and not isinstance(reward, dict):
            raise MetricError("Each reward must be an object or null")
        lines.append(json.dumps(reward) + "\n")
    joined = "".join(lines)
    if len(joined.encode()) > MAX_REWARDS_BYTES:
        raise MetricError(f"The rewards are larger than {MAX_REWARDS_BYTES} bytes")
    return script, joined


def run(event: Mapping[str, object]) -> dict[str, object]:
    script, rewards = input_lines(event)
    work = Path(tempfile.mkdtemp(prefix="metric-"))
    try:
        (work / "metric.py").write_text(script)
        (work / "input.jsonl").write_text(rewards)
        started = time.monotonic()
        with (work / "stderr.txt").open("wb") as err:
            try:
                process = subprocess.Popen(
                    ["uv", "run", "--script", "metric.py", "-i", "input.jsonl", "-o", "output.json"],
                    cwd=work,
                    env=script_env(work),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=err,
                    start_new_session=True,
                )
            except OSError as error:
                raise MetricError(f"uv could not be started to run metric.py: {error}") from error
            try:
                process.wait(timeout=SCRIPT_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired as error:
                raise MetricError(f"The script ran longer than {SCRIPT_TIMEOUT_SECONDS} seconds") from error
            finally:
                kill_group(process)
        duration = time.monotonic() - started
        if process.returncode != 0:
            stderr_tail = tail(work / "stderr.txt", STDERR_TAIL_BYTES)
            raise MetricError(f"The script exited with status {process.returncode}: {stderr_tail[-2000:]}")
        return {"metrics": read_metrics(work / "output.json"), "duration": duration}
    finally:
        shutil.rmtree(work, ignore_errors=True)


def handler(event: Mapping[str, object], context: object) -> dict[str, object]:
    try:
        return run(event)
    except MetricError as error:
        return {"error": str(error)}
