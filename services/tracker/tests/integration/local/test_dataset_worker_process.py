import asyncio
import os
import sys
from pathlib import Path

import pytest

from tracker.dataset_worker import supervise_process


@pytest.mark.description(
    "The dataset worker stops and reaps child processes when work expires or the worker shuts down."
)
class TestDatasetWorkerProcess:
    @pytest.mark.description(
        "A supervised child creates temporary files under its assigned directory and exits on timeout or cancellation."
    )
    @pytest.mark.parametrize("cancel", [False, True])
    async def test_child_is_reaped(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancel: bool) -> None:
        scratch = tmp_path / "worker"
        scratch.mkdir()
        monkeypatch.setenv("TMPDIR", str(tmp_path))
        script = """
import os
import tempfile
import time
from pathlib import Path

with tempfile.TemporaryDirectory(prefix="task-") as task_dir:
    (Path(task_dir) / str(os.getpid())).touch()
    time.sleep(60)
"""
        task = asyncio.create_task(supervise_process([sys.executable, "-c", script], 60 if cancel else 5, str(scratch)))
        try:
            async with asyncio.timeout(10):
                while (pid_file := next(tmp_path.rglob("task-*/*"), None)) is None:
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)

            assert pid_file.parent.parent == scratch
            if cancel:
                task.cancel()
            with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
                await task
            with pytest.raises(ProcessLookupError):
                os.kill(int(pid_file.name), 0)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
