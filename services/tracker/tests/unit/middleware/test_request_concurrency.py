import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from tracker.middleware.request_concurrency import RequestConcurrencyLimitMiddleware


def _app(release: asyncio.Event, started: list[str]) -> FastAPI:
    app = FastAPI()
    app.add_middleware(RequestConcurrencyLimitMiddleware, limit=1)

    async def work(name: str) -> dict[str, str]:
        started.append(name)
        await release.wait()
        return {"name": name}

    async def health() -> dict[str, str]:
        return {"status": "ok"}

    async def stream() -> StreamingResponse:
        async def body() -> AsyncIterator[bytes]:
            started.append("stream")
            yield b"open"
            await release.wait()

        return StreamingResponse(body())

    app.add_api_route("/work/{name}", work, methods=["GET"])

    async def quick() -> dict[str, str]:
        return {"status": "done"}

    app.add_api_route("/stream", stream, methods=["GET"])
    app.add_api_route("/quick", quick, methods=["GET"])
    app.add_api_route("/health", health, methods=["GET"])
    return app


async def _scenario() -> tuple[list[str], list[str], int, list[int]]:
    release = asyncio.Event()
    started: list[str] = []
    transport = httpx.ASGITransport(app=_app(release, started))
    async with httpx.AsyncClient(transport=transport, base_url="http://tracker") as client:
        first = asyncio.create_task(client.get("/work/first"))
        second = asyncio.create_task(client.get("/work/second"))
        await asyncio.sleep(0.2)
        while_held = list(started)
        health = await client.get("/health")
        release.set()
        responses = await asyncio.gather(first, second)
    return while_held, started, health.status_code, [response.status_code for response in responses]


@pytest.mark.description(
    "A request past the limit waits for a slot while `/health` still answers, and both finish once it frees."
)
def test_requests_over_the_limit_wait_and_health_is_not_limited() -> None:
    while_held, started, health_status, statuses = asyncio.run(_scenario())

    assert len(while_held) == 1
    assert sorted(started) == ["first", "second"]
    assert health_status == 200
    assert statuses == [200, 200]


async def _stream_scenario() -> tuple[list[str], int]:
    release = asyncio.Event()
    started: list[str] = []
    transport = httpx.ASGITransport(app=_app(release, started))
    async with httpx.AsyncClient(transport=transport, base_url="http://tracker") as client:
        stream = asyncio.create_task(client.get("/stream"))
        await asyncio.sleep(0.2)
        while_streaming = list(started)
        quick = await asyncio.wait_for(client.get("/quick"), timeout=5)
        release.set()
        await stream
    return while_streaming, quick.status_code


@pytest.mark.description("An open streamed response gives up its slot once headers are sent, so the next request runs.")
def test_streamed_response_frees_its_slot_after_headers() -> None:
    while_streaming, status = asyncio.run(_stream_scenario())

    assert while_streaming == ["stream"]
    assert status == 200
