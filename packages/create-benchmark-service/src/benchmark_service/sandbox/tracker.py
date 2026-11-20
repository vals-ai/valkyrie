"""Sandboxes owned by the Tracker and reached over the benchmark service WebSocket."""

from __future__ import annotations

import asyncio
import base64
import itertools
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from contextlib import suppress
from functools import cached_property
from typing import Any, Literal

from pydantic import BaseModel, Field

from benchmark_service.sandbox.types import (
    ExecResult,
    Sandbox,
    SandboxCommandError,
    SandboxCreateRequest,
    SandboxError,
    SandboxNotFoundError,
    SandboxProvider,
    SandboxQuery,
)

_CHUNK_SIZE = 4 * 1024 * 1024

SandboxMethod = Literal["exec", "command", "upload_file", "download_file", "create_sandbox", "delete_sandbox"]


class SandboxCall(BaseModel):
    id: int
    method: SandboxMethod
    sandbox_id: str
    args: dict[str, Any] = Field(default_factory=dict)


class SandboxReply(BaseModel):
    type: Literal["sandbox_reply"] = "sandbox_reply"
    id: int
    result: Any = None
    error: str | None = None
    not_found: bool = False
    exit_code: int | None = None
    done: bool = True


class TrackerChannel:
    def __init__(
        self,
        send: Callable[[dict[str, Any]], Awaitable[None]],
        receive: Callable[[], Awaitable[Any]],
    ) -> None:
        self._send = send
        self._receive = receive
        self._ids = itertools.count()
        self._pending: dict[int, asyncio.Queue[SandboxReply | Exception]] = {}
        self._reader: asyncio.Task[None] | None = None

    async def _read(self) -> None:
        try:
            while self._pending:
                reply = SandboxReply.model_validate(await self._receive())
                if (queue := self._pending.get(reply.id)) is not None:
                    queue.put_nowait(reply)
        except Exception as exc:
            for queue in self._pending.values():
                queue.put_nowait(exc)

    async def stream(self, method: SandboxMethod, sandbox_id: str, **args: Any) -> AsyncGenerator[Any, None]:
        """Yield each partial result of the call, then its final result."""
        call = SandboxCall(id=next(self._ids), method=method, sandbox_id=sandbox_id, args=args)
        queue: asyncio.Queue[SandboxReply | Exception] = asyncio.Queue()
        self._pending[call.id] = queue
        try:
            if self._reader is None or self._reader.done():
                self._reader = asyncio.create_task(self._read())
            await self._send({"type": "sandbox_call", "data": call.model_dump()})
            while True:
                reply = await queue.get()
                if isinstance(reply, Exception):
                    raise reply
                if reply.done:
                    break
                yield reply.result
        finally:
            del self._pending[call.id]
            if not self._pending and self._reader is not None:
                self._reader.cancel()
                self._reader = None
        if reply.exit_code is not None:
            raise SandboxCommandError(reply.exit_code)
        if reply.not_found:
            raise SandboxNotFoundError(reply.error)
        if reply.error is not None:
            raise SandboxError(reply.error)
        yield reply.result

    async def call(self, method: SandboxMethod, sandbox_id: str, **args: Any) -> Any:
        result = None
        async for result in self.stream(method, sandbox_id, **args):
            pass
        return result


class TrackerSandbox(Sandbox):
    def __init__(self, channel: TrackerChannel, sandbox_id: str) -> None:
        self._channel = channel
        self._id = sandbox_id

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
        result = await self._channel.call("exec", self._id, command=command, cwd=cwd, timeout=timeout)
        return ExecResult.model_validate(result)

    async def command(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float | None = None,
        env_vars: Mapping[str, str] | None = None,
    ) -> AsyncGenerator[str, None]:
        async for chunk in self._channel.stream(
            "command", self._id, command=command, cwd=cwd, timeout=timeout, env_vars=dict(env_vars or {})
        ):
            if chunk is not None:
                yield chunk

    async def upload_file(self, remote_path: str, content: bytes) -> None:
        upload_id = uuid.uuid4().hex
        starts = range(0, max(len(content), 1), _CHUNK_SIZE)
        for start in starts:
            await self._channel.call(
                "upload_file",
                self._id,
                remote_path=remote_path,
                content=base64.b64encode(content[start : start + _CHUNK_SIZE]).decode(),
                upload_id=upload_id,
                last=start == starts[-1],
            )

    async def download_file(self, remote_path: str) -> bytes:
        chunks = self._channel.stream("download_file", self._id, remote_path=remote_path)
        return b"".join([base64.b64decode(chunk) async for chunk in chunks if chunk is not None])


class TrackerSandboxProvider(SandboxProvider):
    def __init__(self, channel: TrackerChannel) -> None:
        self._channel = channel

    async def create_sandbox(self, request: SandboxCreateRequest) -> Sandbox:
        sandbox_id = await self._channel.call("create_sandbox", "", **request.model_dump(mode="json"))
        return TrackerSandbox(self._channel, sandbox_id)

    async def get_sandbox(self, instance_id: str) -> Sandbox:
        return TrackerSandbox(self._channel, instance_id)

    async def delete_sandbox(self, instance_id: str) -> None:
        await self._channel.call("delete_sandbox", instance_id)

    async def list_sandboxes(self, query: SandboxQuery) -> AsyncGenerator[Sandbox, None]:
        raise SandboxError("Listing sandboxes is not available through the Tracker")
        yield


class TrackerSandboxHandler:
    """Serves sandbox calls from a benchmark service with the Tracker's own provider.

    The service may only reach the request's own sandbox and the sandboxes it created.
    """

    def __init__(self, resolve_provider: Callable[[], SandboxProvider], instance_id: str) -> None:
        self._resolve_provider = resolve_provider
        self._owned = {instance_id}
        self._created: set[str] = set()
        self._uploads: dict[str, bytearray] = {}

    @cached_property
    def _provider(self) -> SandboxProvider:
        return self._resolve_provider()

    async def handle(self, call: SandboxCall) -> AsyncGenerator[SandboxReply, None]:
        try:
            if call.method in ("command", "download_file"):
                async for chunk in self._stream(call):
                    yield SandboxReply(id=call.id, result=chunk, done=False)
                result = None
            else:
                result = await self._dispatch(call)
        except SandboxCommandError as exc:
            yield SandboxReply(id=call.id, error=str(exc), exit_code=exc.exit_code)
        except SandboxNotFoundError as exc:
            yield SandboxReply(id=call.id, error=str(exc), not_found=True)
        except Exception as exc:
            yield SandboxReply(id=call.id, error=f"{type(exc).__name__}: {exc}")
        else:
            yield SandboxReply(id=call.id, result=result)

    def _check_owned(self, sandbox_id: str) -> None:
        if sandbox_id not in self._owned:
            raise SandboxError(f"Sandbox {sandbox_id!r} does not belong to this request")

    async def _sandbox(self, sandbox_id: str) -> Sandbox:
        self._check_owned(sandbox_id)
        return await self._provider.get_sandbox(sandbox_id)

    async def _stream(self, call: SandboxCall) -> AsyncGenerator[str, None]:
        sandbox = await self._sandbox(call.sandbox_id)
        if call.method == "command":
            async for chunk in sandbox.command(**call.args):
                yield chunk
            return
        content = await sandbox.download_file(call.args["remote_path"])
        for start in range(0, len(content), _CHUNK_SIZE):
            yield base64.b64encode(content[start : start + _CHUNK_SIZE]).decode()

    async def _dispatch(self, call: SandboxCall) -> Any:
        args = call.args
        match call.method:
            case "create_sandbox":
                request = SandboxCreateRequest.model_validate(args)
                if request.sandbox_secrets or request.volumes:
                    raise SandboxError("Services cannot attach provider secrets or volumes to Tracker sandboxes")
                sandbox = await self._provider.create_sandbox(request)
                self._owned.add(sandbox.id)
                self._created.add(sandbox.id)
                return sandbox.id
            case "delete_sandbox":
                self._check_owned(call.sandbox_id)
                await self._provider.delete_sandbox(call.sandbox_id)
                self._created.discard(call.sandbox_id)
                return None
            case _:
                pass
        sandbox = await self._sandbox(call.sandbox_id)
        match call.method:
            case "exec":
                return (await sandbox.exec(args["command"], cwd=args["cwd"], timeout=args["timeout"])).model_dump()
            case _:
                upload = self._uploads.setdefault(args["upload_id"], bytearray())
                upload += base64.b64decode(args["content"])
                if args["last"]:
                    del self._uploads[args["upload_id"]]
                    await sandbox.upload_file(args["remote_path"], bytes(upload))
                return None

    async def close(self) -> None:
        for sandbox_id in self._created:
            with suppress(Exception):
                await self._provider.delete_sandbox(sandbox_id)
        self._created.clear()
