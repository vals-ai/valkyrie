"""Version selection through the real HTTP and WebSocket client transports."""

import asyncio
import json
from collections.abc import AsyncGenerator, AsyncIterator
from functools import partial
from typing import Any

import httpx
import pytest
import websockets
from fastapi.testclient import TestClient
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.server import serve

import benchmark_service.client as client_module
from benchmark_service import BenchmarkServiceApp, BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.schemas import (
    DATASET_VERSION_HEADER,
    EvaluateResponseRequest,
    StreamChunk,
    StreamErrorChunk,
    StreamResultChunk,
)
from conftest import StubBenchmark


class LegacyContinuingBenchmark(StubBenchmark):
    async def stream_evaluate_response(
        self, request: EvaluateResponseRequest, dataset: str | None = None
    ) -> AsyncGenerator[StreamChunk, None]:
        yield StreamErrorChunk(type="error", data="Recoverable warning")
        yield StreamResultChunk(type="result", data={"resolved": True})


def test_legacy_service_requires_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    with TestClient(BenchmarkServiceApp(StubBenchmark)) as client:
        assert client.get("/verify-task-ids").json()["task_ids"] == ["task-1", "task-2", "task-3"]
        assert client.get("/verify-task-ids", headers={DATASET_VERSION_HEADER: "v1.0"}).status_code == 400
        assert client.post("/resolve-dataset", json={"dataset": "default", "version": None}).status_code == 400
        with client.websocket_connect("/ws/evaluate-response") as ws:
            ws.send_json({"task_id": "task-1", "response": "2"})
            assert ws.receive_json() == {"type": "result", "data": {"resolved": True}}
        with client.websocket_connect("/ws/evaluate-response", headers={DATASET_VERSION_HEADER: "v1.0"}) as ws:
            ws.send_json({"task_id": "task-1", "response": "2"})
            assert ws.receive_json() == {
                "type": "error",
                "data": "Dataset version selection is not supported",
                "status_code": 400,
            }


def test_legacy_unpinned_stream_continues_after_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AUTH_REQUIRED", raising=False)
    with TestClient(BenchmarkServiceApp(LegacyContinuingBenchmark)) as client:
        with client.websocket_connect("/ws/evaluate-response") as ws:
            ws.send_json({"task_id": "task-1", "response": "2"})
            assert ws.receive_json() == {"type": "error", "data": "Recoverable warning"}
            assert ws.receive_json() == {"type": "result", "data": {"resolved": True}}


@pytest.mark.parametrize("echo", [None, "v1.1", "v1.0, v1.0"])
async def test_client_rejects_http_without_exact_acknowledgement(echo: str | None) -> None:
    async with BenchmarkServiceClient("http://test", {}, dataset_version="v1.0") as client:

        async def response(request: httpx.Request) -> httpx.Response:
            assert request.headers[DATASET_VERSION_HEADER] == "v1.0"
            return httpx.Response(
                200, json={"task_ids": []}, headers={} if echo is None else {DATASET_VERSION_HEADER: echo}
            )

        original = client._http_client  # pyright: ignore[reportPrivateUsage]
        client._http_client = httpx.AsyncClient(  # pyright: ignore[reportPrivateUsage]
            headers=original.headers, event_hooks=original.event_hooks, transport=httpx.MockTransport(response)
        )
        await original.aclose()
        with pytest.raises(BenchmarkServiceError, match="acknowledge"):
            await client.verify_task_ids(None, None)


@pytest.mark.parametrize(
    "messages",
    [
        [{"type": "result", "data": 1}],
        [{"type": "eval_resume_state", "data": {}}],
        [{"type": "dataset_version", "data": {"id": "v1.1", "label": None}}],
        [{"type": "dataset_version", "data": {"id": "v1.0", "label": None}}] * 2,
    ],
)
async def test_client_rejects_invalid_stream_before_callbacks(messages: list[dict[str, Any]]) -> None:
    async def respond(ws: websockets.ServerConnection) -> None:
        await ws.recv()
        for message in messages:
            await ws.send(json.dumps(message))

    async with serve(respond, "127.0.0.1", 0) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        async with BenchmarkServiceClient(f"http://127.0.0.1:{port}", {}, dataset_version="v1.0") as client:
            checkpoints: list[dict[str, Any]] = []
            with pytest.raises(BenchmarkServiceError, match="acknowledge"):
                await client.resume_evaluation("task-1", {}, on_eval_resume_state=checkpoints.append)
            assert not checkpoints


@pytest.mark.parametrize("callback_fails", [False, True], ids=["success", "callback-error"])
async def test_client_awaits_checkpoint_before_terminal_result(
    callback_fails: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = {"artifact_prefix": "s3://bucket/run"}
    announced: list[str] = []
    checkpoint_started = asyncio.Event()
    release_checkpoint = asyncio.Event()
    checkpoint_completed = asyncio.Event()

    class CheckpointConnection(ClientConnection):
        async def __aiter__(self) -> AsyncIterator[str | bytes]:
            async for message in super().__aiter__():
                yield message
                assert checkpoint_completed.is_set(), "received again before checkpoint completed"

    monkeypatch.setattr(
        client_module,
        "_NoRedirectConnect",
        partial(client_module._NoRedirectConnect, create_connection=CheckpointConnection),  # pyright: ignore[reportPrivateUsage]
    )

    async def respond(ws: websockets.ServerConnection) -> None:
        request = json.loads(await ws.recv())
        assert request["announce_sandboxes"] is True
        await ws.send(json.dumps({"type": "eval_resume_state", "data": state}))
        await ws.send(json.dumps({"type": "sandbox_created", "data": {"provider": "daytona", "sandbox_id": "sb-1"}}))
        await ws.send(json.dumps({"type": "result", "data": {"score": 1.0}}))

    async def save_checkpoint(received: dict[str, Any]) -> None:
        assert received == state
        checkpoint_started.set()
        await release_checkpoint.wait()
        if callback_fails:
            raise RuntimeError("checkpoint persistence failed")
        checkpoint_completed.set()

    async with serve(respond, "127.0.0.1", 0) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        async with BenchmarkServiceClient(f"http://127.0.0.1:{port}", {}) as client:
            evaluation = asyncio.create_task(
                client.resume_evaluation(
                    "task-1",
                    {},
                    on_eval_resume_state=save_checkpoint,
                    on_sandbox_created=lambda data: announced.append(data.sandbox_id),
                )
            )
            try:
                await asyncio.wait_for(checkpoint_started.wait(), timeout=5)
            finally:
                release_checkpoint.set()
                await asyncio.gather(evaluation, return_exceptions=True)
            if callback_fails:
                with pytest.raises(RuntimeError, match="checkpoint persistence failed"):
                    await evaluation
                assert announced == []
            else:
                assert await evaluation == {"score": 1.0}
                assert announced == ["sb-1"]
