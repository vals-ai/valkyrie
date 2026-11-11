import asyncio
import io
import os
import shlex
import zipfile
from collections.abc import AsyncIterator, Mapping, Sequence
from pathlib import Path
from typing import Any
from uuid import UUID

from benchmark_service import SandboxNotFoundError
from benchmark_service.sandbox.types import ExecResult

from fastapi.testclient import TestClient

from tracker.services.datasets.compose import DOCKERD_RUNNING
from tracker.services.datasets.digest import archive_digest as digest_of_archive
from tracker.services.datasets.grading import REWARD_TEXT_PATH
from tracker.runtime.artifacts import dataset_blob_key
from tracker.runtime.storage import ObjectStore

TASK = {
    "task.toml": b"schema_version = '1.4'",
    "instruction.md": b"Do it.",
    "environment/Dockerfile": b"FROM ubuntu",
    "environment/data/big.bin": b"x" * 500,
    "tests/test.sh": b"#!/bin/bash",
    "tests/cases/one.txt": b"1",
    "solution/solve.sh": b"#!/bin/bash",
}


def zip_bytes(files: Mapping[str, bytes | str], compression: int = zipfile.ZIP_DEFLATED) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression) as writer:
        for name, content in files.items():
            writer.writestr(name, content)
    return buffer.getvalue()


def archive_digest_of(archive: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(archive)) as reader:
        return digest_of_archive(reader)


async def store_archive(store: ObjectStore, dataset_id: UUID, archive: bytes) -> str:
    digest = archive_digest_of(archive)
    await store.put_bytes(dataset_blob_key(str(dataset_id), digest), archive)
    return digest


def publish_version(
    client: TestClient,
    base: str,
    archives: Mapping[str, bytes],
    tags: Sequence[str] = (),
    manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    tasks: dict[str, str] = {}
    for task_id, archive in archives.items():
        digest = archive_digest_of(archive)
        stored = client.put(f"{base}/blobs/{digest}", content=archive, headers={"content-type": "application/zip"})
        assert stored.status_code in {200, 201}, stored.text
        tasks[task_id] = digest
    body: dict[str, Any] = {"tasks": tasks, "tags": list(tags)}
    if manifest is not None:
        body["manifest"] = {
            **manifest,
            "tasks": {task_id: {"name": f"org/{task_id}", "digest": digest} for task_id, digest in tasks.items()},
        }
    published = client.post(f"{base}/versions", json=body)
    assert published.status_code in {200, 201}, published.text
    return published.json()


class FakeSandbox:
    def __init__(
        self,
        *,
        files: dict[str, bytes] | None = None,
        writes: dict[str, bytes] | None = None,
        exit_code: int = 0,
        hide: tuple[str, ...] = (),
        id: str = "sandbox-1",
        artifacts_present: bool = False,
        process_cwd: str = "/root",
    ) -> None:
        self.id = id
        self.process_cwd = process_cwd
        self.artifacts_present = artifacts_present
        self.labels: dict[str, str] = {}
        self.files: dict[str, bytes] = dict(files or {})
        self.commands: list[str] = []
        self.timeouts: list[float | None] = []
        self.cwds: list[str | None] = []
        self.writes = writes or {}
        self.exit_code = exit_code
        self.hide = hide

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        self.commands.append(command)
        if command.startswith("if [ -s"):
            path = shlex.split(command)[3]
            if path in self.hide or path not in self.files:
                return ExecResult(exit_code=0, output="missing")
            return ExecResult(exit_code=0, output="full" if self.files[path] else "empty")
        if command.endswith("| wc -l"):
            return ExecResult(exit_code=0, output="1\n")
        if command == "readlink /proc/1/cwd":
            return ExecResult(exit_code=0, output=f"{self.process_cwd}\n")
        if command.startswith("test -L"):
            return ExecResult(exit_code=1)
        if command.startswith("rm -f "):
            for path in shlex.split(command)[2:]:
                self.files.pop(path, None)
        if command.startswith("find /logs/verifier -mindepth 1 -delete"):
            for path in [path for path in self.files if path.startswith("/logs/verifier/")]:
                del self.files[path]
        if command == "find /logs/artifacts -mindepth 1 -print -quit":
            return ExecResult(exit_code=0, output="/logs/artifacts/note.txt\n" if self.artifacts_present else "")
        if "bash -c" in command:
            self.timeouts.append(timeout)
            self.cwds.append(cwd)
            self.files.update(self.writes)
            return ExecResult(exit_code=self.exit_code, output="stdout of the run")
        return ExecResult(exit_code=0)

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        self.files[remote_path] = content

    async def download_file(self, remote_path: str) -> bytes:
        if remote_path not in self.files:
            raise SandboxNotFoundError(remote_path)
        return self.files[remote_path]

    async def stream_download(self, remote_path: str) -> AsyncIterator[bytes]:
        yield await self.download_file(remote_path)


class ShellSandbox:
    id = "shell"
    name = "shell"

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        process = await asyncio.create_subprocess_exec(
            "bash",
            "-c",
            command,
            cwd=cwd,
            env={**os.environ, "COPYFILE_DISABLE": "1"},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await asyncio.wait_for(process.communicate(), timeout)
        return ExecResult(exit_code=process.returncode or 0, output=output.decode())

    async def stream_download(self, remote_path: str) -> AsyncIterator[bytes]:
        yield await asyncio.to_thread(Path(remote_path).read_bytes)

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        await asyncio.to_thread(Path(remote_path).write_bytes, content)


class OuterSandbox(FakeSandbox):
    def __init__(
        self,
        *,
        failing: str | None = None,
        output: str = "boom",
        docker_ready: bool = True,
        daemon_running: bool = False,
    ) -> None:
        super().__init__()
        self.daemon_running = daemon_running
        self.labels: dict[str, str] = {}
        self.created_at = None
        self.output = output
        self.failing = failing
        self.docker_ready = docker_ready
        self.timeouts_by_command: dict[str, float | None] = {}

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        self.commands.append(command)
        self.timeouts_by_command[command] = timeout
        if command == DOCKERD_RUNNING:
            return ExecResult(exit_code=0 if self.daemon_running else 1, output="")
        if command == "docker info":
            return ExecResult(exit_code=0 if self.docker_ready else 1, output="")
        if self.failing is not None and self.failing in command:
            return ExecResult(exit_code=1, output=self.output)
        return ExecResult(exit_code=0, output="")

    async def download_file(self, remote_path: str) -> bytes:
        if remote_path.endswith("-reward.json"):
            raise SandboxNotFoundError(remote_path)
        if remote_path.endswith("-reward.txt"):
            return self.writes[REWARD_TEXT_PATH]
        return await super().download_file(remote_path)
