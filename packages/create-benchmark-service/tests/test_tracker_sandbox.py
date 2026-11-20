import asyncio
import json
from collections.abc import AsyncGenerator, Mapping
from typing import Any

import pytest

from benchmark_service.sandbox import (
    ExecResult,
    ImageSource,
    Resources,
    Sandbox,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
    VolumeMount,
)
from benchmark_service.sandbox.tracker import SandboxCall, TrackerChannel, TrackerSandboxHandler, TrackerSandboxProvider


class MemorySandbox(Sandbox):
    def __init__(self, sandbox_id: str) -> None:
        self._id = sandbox_id
        self.files: dict[str, bytes] = {}
        self.released = asyncio.Event()

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._id

    @property
    def state(self) -> str:
        return "started"

    async def exec(self, command: str, *, cwd: str | None = None, timeout: float | None = None) -> ExecResult:
        if command == "release":
            self.released.set()
        return ExecResult(exit_code=0, output=f"{cwd}:{command}")

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        if command == "fail":
            yield "partial"
            raise SandboxCommandError(2)
        if command == "wait":
            yield "ready"
            await self.released.wait()
            yield "released"
            return
        yield f"{command}:"
        yield f"{dict(env_vars or {})}"

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        self.files[remote_path] = content

    async def download_file(self, remote_path: str) -> bytes:
        return self.files[remote_path]


class MemoryProvider(SandboxProvider):
    def __init__(self) -> None:
        self.sandboxes = {"task": MemorySandbox("task"), "other-run": MemorySandbox("other-run")}

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        sandbox = MemorySandbox(request.name)
        self.sandboxes[sandbox.id] = sandbox
        return sandbox

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        if instance_id not in self.sandboxes:
            raise SandboxNotFoundError(instance_id)
        return self.sandboxes[instance_id]

    async def delete_sandbox(self, instance_id: str) -> None:
        self.sandboxes.pop(instance_id, None)

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        for sandbox in self.sandboxes.values():
            yield sandbox


MAX_MESSAGE_SIZE = 10 * 1024 * 1024


def connect(handler: TrackerSandboxHandler) -> TrackerChannel:
    replies: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    calls: set[asyncio.Task[None]] = set()

    async def serve(call: SandboxCall) -> None:
        async for reply in handler.handle(call):
            assert len(reply.model_dump_json()) < MAX_MESSAGE_SIZE
            replies.put_nowait(reply.model_dump())

    async def send(message: dict[str, Any]) -> None:
        assert message["type"] == "sandbox_call"
        assert len(json.dumps(message)) < MAX_MESSAGE_SIZE
        task = asyncio.create_task(serve(SandboxCall.model_validate(message["data"])))
        calls.add(task)
        task.add_done_callback(calls.discard)

    return TrackerChannel(send, replies.get)


async def test_service_reaches_tracker_sandbox() -> None:
    provider = MemoryProvider()
    remote = TrackerSandboxProvider(connect(TrackerSandboxHandler(lambda: provider, "task")))
    sandbox = await remote.get_sandbox("task")

    await sandbox.upload_file("/workspace/answer.txt", b"\x00answer")
    assert await sandbox.download_file("/workspace/answer.txt") == b"\x00answer"
    assert await sandbox.exec("ls", cwd="/workspace") == ExecResult(exit_code=0, output="/workspace:ls")
    assert [chunk async for chunk in sandbox.command("env", env_vars={"A": "1"})] == ["env:", "{'A': '1'}"]

    output: list[str] = []
    with pytest.raises(SandboxCommandError) as failed:
        async for chunk in sandbox.command("fail"):
            output.append(chunk)
    assert output == ["partial"]
    assert failed.value.exit_code == 2
    with pytest.raises(SandboxError, match="KeyError"):
        await sandbox.download_file("/missing")


async def test_sandbox_calls_progress_while_a_command_streams() -> None:
    sandbox = await TrackerSandboxProvider(connect(TrackerSandboxHandler(MemoryProvider, "task"))).get_sandbox("task")
    command = sandbox.command("wait")

    assert await anext(command) == "ready"
    await asyncio.wait_for(sandbox.exec("release"), timeout=5)
    assert [chunk async for chunk in command] == ["released"]


async def test_files_larger_than_a_websocket_message_round_trip() -> None:
    sandbox = await TrackerSandboxProvider(connect(TrackerSandboxHandler(MemoryProvider, "task"))).get_sandbox("task")
    content = bytes(range(256)) * (36 * 1024)

    await sandbox.upload_file("/workspace/large.bin", content)
    assert await sandbox.download_file("/workspace/large.bin") == content
    await sandbox.upload_file("/workspace/empty.bin", b"")
    assert await sandbox.download_file("/workspace/empty.bin") == b""


async def test_service_reaches_only_its_own_sandboxes() -> None:
    provider = MemoryProvider()
    remote = TrackerSandboxProvider(connect(TrackerSandboxHandler(lambda: provider, "task")))

    with pytest.raises(SandboxError, match="does not belong to this request"):
        await (await remote.get_sandbox("other-run")).exec("ls")
    with pytest.raises(SandboxError, match="does not belong to this request"):
        await remote.delete_sandbox("other-run")
    with pytest.raises(SandboxError, match="does not belong to this request"):
        _ = [chunk async for chunk in (await remote.get_sandbox("other-run")).command("env")]
    assert "other-run" in provider.sandboxes


GRADER_REQUEST = SandboxCreateRequest(
    source=ImageSource(image="python:3.12"),
    resources=Resources(vcpu=1, memory=1, disk=1),
    name="grader",
    labels={},
    env_vars={},
    auto_stop_interval=0,
    create_timeout=60,
)


@pytest.mark.parametrize(
    "update",
    [{"sandbox_secrets": {"TOKEN": "tracker-secret"}}, {"volumes": [VolumeMount(name="shared", mount_path="/data")]}],
    ids=["secrets", "volumes"],
)
async def test_service_cannot_request_provider_secrets_or_volumes(update: dict[str, Any]) -> None:
    provider = MemoryProvider()
    remote = TrackerSandboxProvider(connect(TrackerSandboxHandler(lambda: provider, "task")))

    with pytest.raises(SandboxError, match="provider secrets or volumes"):
        await remote.create_sandbox(GRADER_REQUEST.model_copy(update=update))
    assert "grader" not in provider.sandboxes


async def test_tracker_deletes_sandboxes_the_service_left_behind() -> None:
    provider = MemoryProvider()
    handler = TrackerSandboxHandler(lambda: provider, "task")
    remote = TrackerSandboxProvider(connect(handler))

    grader = await remote.create_sandbox(GRADER_REQUEST)
    assert await grader.exec("ls", cwd="/") == ExecResult(exit_code=0, output="/:ls")
    await remote.create_sandbox(GRADER_REQUEST.model_copy(update={"name": "leftover"}))
    await remote.delete_sandbox("grader")
    await remote.delete_sandbox("grader")
    await remote.delete_sandbox("task")
    await remote.delete_sandbox("task")
    await handler.close()

    assert set(provider.sandboxes) == {"other-run"}
